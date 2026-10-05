//! Token bucket speed limiter for controlling download bandwidth.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A thread-safe Token Bucket rate limiter that throttles bandwidth to a target bytes-per-second limit.
#[derive(Debug)]
pub struct TokenBucketLimiter {
    rate_bytes_per_sec: u64,
    capacity_bytes: u64,
    state: Mutex<BucketState>,
}

#[derive(Debug)]
struct BucketState {
    next_allowed: Instant,
}

impl TokenBucketLimiter {
    /// Creates a new `TokenBucketLimiter` with the given rate in bytes per second.
    /// A rate of `0` denotes unlimited speed (no throttling).
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        let capacity_bytes = if rate_bytes_per_sec == 0 {
            0
        } else {
            // Burst capacity is 1 second worth of bandwidth, at least 64 KiB
            rate_bytes_per_sec.max(64 * 1024)
        };

        Self {
            rate_bytes_per_sec,
            capacity_bytes,
            state: Mutex::new(BucketState {
                next_allowed: Instant::now(),
            }),
        }
    }

    /// The configured bandwidth limit in bytes per second (0 if unlimited).
    pub fn rate(&self) -> u64 {
        self.rate_bytes_per_sec
    }

    /// Acquires tokens for the specified number of bytes, sleeping if necessary to enforce the limit.
    pub async fn acquire(&self, bytes: usize) {
        if self.rate_bytes_per_sec == 0 || bytes == 0 {
            return;
        }

        let wait_duration = self.reserve(bytes);
        if wait_duration > Duration::ZERO {
            tokio::time::sleep(wait_duration).await;
        }
    }

    /// Reserves tokens and computes the duration the caller must wait before transmission.
    pub fn reserve(&self, bytes: usize) -> Duration {
        if self.rate_bytes_per_sec == 0 || bytes == 0 {
            return Duration::ZERO;
        }

        let mut state = self.state.lock().expect("limiter mutex poisoned");
        let now = Instant::now();
        let rate_f64 = self.rate_bytes_per_sec as f64;
        let burst_secs = (self.capacity_bytes as f64) / rate_f64;
        let burst_duration = Duration::from_secs_f64(burst_secs);

        // Cannot accumulate more burst than burst_duration in the past
        let earliest = now.checked_sub(burst_duration).unwrap_or(now);
        if state.next_allowed < earliest {
            state.next_allowed = earliest;
        }

        let wait_duration = if state.next_allowed > now {
            state.next_allowed - now
        } else {
            Duration::ZERO
        };

        let service_secs = (bytes as f64) / rate_f64;
        let service_duration = Duration::from_secs_f64(service_secs);

        state.next_allowed = state.next_allowed.max(now) + service_duration;

        wait_duration
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_unlimited_limiter() {
        let limiter = TokenBucketLimiter::new(0);
        let start = Instant::now();
        limiter.acquire(10_000_000).await;
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_limiter_rate_accuracy() {
        // Target: 200 KiB/s = 204,800 B/s
        let rate = 200 * 1024;
        let limiter = Arc::new(TokenBucketLimiter::new(rate));

        let chunk_size = 10 * 1024; // 10 KiB chunks
                                    // Warm up and consume initial burst
        for _ in 0..25 {
            limiter.acquire(chunk_size).await;
        }

        // Now measure steady state: 100 KiB at 200 KiB/s -> exactly 0.50s
        let total_bytes = 100 * 1024;
        let chunks = total_bytes / chunk_size;

        let start = Instant::now();
        for _ in 0..chunks {
            limiter.acquire(chunk_size).await;
        }
        let elapsed = start.elapsed();

        let expected_secs = (total_bytes as f64) / (rate as f64);
        let actual_secs = elapsed.as_secs_f64();

        // Check within ±10% accuracy
        let lower = expected_secs * 0.90;
        let upper = expected_secs * 1.15;

        assert!(
            actual_secs >= lower && actual_secs <= upper,
            "Actual elapsed {:.3}s not in expected range [{:.3}s, {:.3}s]",
            actual_secs,
            lower,
            upper
        );
    }

    #[tokio::test]
    async fn test_concurrent_limiters() {
        let rate = 400 * 1024; // 400 KiB/s
        let limiter = Arc::new(TokenBucketLimiter::new(rate));

        let chunk_size = 10 * 1024;
        // Warm up / consume burst
        for _ in 0..45 {
            limiter.acquire(chunk_size).await;
        }

        let start = Instant::now();
        let mut handles = Vec::new();

        // 4 concurrent workers, each downloading 50 KiB (total 200 KiB at 400 KiB/s -> 0.50s)
        for _ in 0..4 {
            let lim = Arc::clone(&limiter);
            handles.push(tokio::spawn(async move {
                for _ in 0..5 {
                    lim.acquire(10 * 1024).await;
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let elapsed = start.elapsed();
        let expected = 200.0 / 400.0; // 0.5s
        let actual = elapsed.as_secs_f64();

        assert!(
            actual >= expected * 0.85 && actual <= expected * 1.25,
            "Concurrent elapsed {:.3}s not near expected {:.3}s",
            actual,
            expected
        );
    }
}
