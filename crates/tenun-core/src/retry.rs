//! Network error classification, retry policy, backoff with jitter, and stall detection.

use reqwest::header::{self, HeaderMap};
use reqwest::StatusCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Classification of errors encountered during download operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    /// Transient error that can be retried, optionally with a suggested wait duration from Retry-After header.
    Retryable {
        /// Suggested delay before retry, parsed from `Retry-After`.
        retry_after: Option<Duration>,
    },
    /// Non-recoverable error (e.g. 404 Not Found, 410 Gone, invalid byte ranges).
    Fatal,
    /// Resource expired or requires credentials/URL refresh (e.g. 401 Unauthorized, 403 Forbidden).
    NeedsRefresh,
}

/// Classifies an HTTP status code and response headers into an [`ErrorKind`].
pub fn classify_response(status: StatusCode, headers: &HeaderMap) -> ErrorKind {
    let retry_after = parse_retry_after(headers);

    if status == StatusCode::TOO_MANY_REQUESTS {
        return ErrorKind::Retryable { retry_after };
    }

    if status.is_server_error() {
        return ErrorKind::Retryable { retry_after };
    }

    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return ErrorKind::NeedsRefresh;
    }

    if status.is_client_error() {
        return ErrorKind::Fatal;
    }

    ErrorKind::Fatal
}

/// Parses the `Retry-After` header value (either seconds or HTTP-date).
pub fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let val = headers.get(header::RETRY_AFTER)?.to_str().ok()?;
    parse_retry_after_str(val)
}

/// Parses a `Retry-After` string value (either seconds or HTTP-date).
pub fn parse_retry_after_str(val: &str) -> Option<Duration> {
    let val = val.trim();
    if let Ok(secs) = val.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }

    if let Ok(time) = httpdate::parse_http_date(val) {
        if let Ok(duration) = time.duration_since(std::time::SystemTime::now()) {
            return Some(duration);
        } else {
            return Some(Duration::ZERO);
        }
    }

    None
}

/// Classifies a `reqwest::Error` into an [`ErrorKind`].
pub fn classify_reqwest_error(err: &reqwest::Error) -> ErrorKind {
    if let Some(status) = err.status() {
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            return ErrorKind::Retryable { retry_after: None };
        }
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return ErrorKind::NeedsRefresh;
        }
        if status.is_client_error() {
            return ErrorKind::Fatal;
        }
    }

    if err.is_timeout() || err.is_connect() || err.is_request() || err.is_body() {
        return ErrorKind::Retryable { retry_after: None };
    }

    ErrorKind::Retryable { retry_after: None }
}

/// Retry policy configuration for download operations.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts per segment or operation.
    pub max_retries: usize,
    /// Base delay for exponential backoff (e.g. 500ms).
    pub base_delay: Duration,
    /// Maximum ceiling for backoff delay (e.g. 30s).
    pub max_delay: Duration,
    /// Stall timeout: max time allowed without receiving any chunk bytes (e.g. 30s).
    pub stall_timeout: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            stall_timeout: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Computes the backoff duration for a given attempt (1-indexed), applying exponential backoff,
    /// respecting `Retry-After` if provided, and adding pseudo-random jitter.
    pub fn compute_delay(&self, attempt: usize, retry_after: Option<Duration>) -> Duration {
        if let Some(ra) = retry_after {
            return ra.min(self.max_delay);
        }

        let exponent = (attempt.saturating_sub(1)).min(16) as u32;
        let factor = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let base_nanos = self.base_delay.as_nanos().min(u128::MAX / 2) as u64;
        let raw_nanos = base_nanos.saturating_mul(factor);
        let capped_nanos = raw_nanos.min(self.max_delay.as_nanos() as u64);

        // Add uniform random jitter up to 25% of capped_nanos
        let jitter_range = (capped_nanos / 4).max(1);
        let jitter = pseudo_random_u64() % jitter_range;
        Duration::from_nanos(capped_nanos.saturating_add(jitter)).min(self.max_delay)
    }
}

static PRNG_SEED: AtomicU64 = AtomicU64::new(0);

fn pseudo_random_u64() -> u64 {
    let mut s = PRNG_SEED.load(Ordering::Relaxed);
    if s == 0 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(123456789);
        s = nanos ^ 0x517cc1b727220a95;
    }
    s = s.wrapping_add(0x9e3779b97f4a7c15);
    PRNG_SEED.store(s, Ordering::Relaxed);
    let mut z = s;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_status_codes() {
        let headers = HeaderMap::new();

        // 429 Too Many Requests -> Retryable
        assert_eq!(
            classify_response(StatusCode::TOO_MANY_REQUESTS, &headers),
            ErrorKind::Retryable { retry_after: None }
        );

        // 5xx Server Errors -> Retryable
        assert_eq!(
            classify_response(StatusCode::INTERNAL_SERVER_ERROR, &headers),
            ErrorKind::Retryable { retry_after: None }
        );
        assert_eq!(
            classify_response(StatusCode::BAD_GATEWAY, &headers),
            ErrorKind::Retryable { retry_after: None }
        );
        assert_eq!(
            classify_response(StatusCode::SERVICE_UNAVAILABLE, &headers),
            ErrorKind::Retryable { retry_after: None }
        );
        assert_eq!(
            classify_response(StatusCode::GATEWAY_TIMEOUT, &headers),
            ErrorKind::Retryable { retry_after: None }
        );

        // 401 & 403 -> NeedsRefresh
        assert_eq!(
            classify_response(StatusCode::UNAUTHORIZED, &headers),
            ErrorKind::NeedsRefresh
        );
        assert_eq!(
            classify_response(StatusCode::FORBIDDEN, &headers),
            ErrorKind::NeedsRefresh
        );

        // 404 & 410 -> Fatal
        assert_eq!(
            classify_response(StatusCode::NOT_FOUND, &headers),
            ErrorKind::Fatal
        );
        assert_eq!(
            classify_response(StatusCode::GONE, &headers),
            ErrorKind::Fatal
        );
    }

    #[test]
    fn test_retry_after_header_parsing() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RETRY_AFTER, "120".parse().unwrap());

        assert_eq!(
            classify_response(StatusCode::TOO_MANY_REQUESTS, &headers),
            ErrorKind::Retryable {
                retry_after: Some(Duration::from_secs(120))
            }
        );
    }

    #[test]
    fn test_compute_delay_backoff_and_jitter() {
        let policy = RetryPolicy {
            max_retries: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
            stall_timeout: Duration::from_secs(10),
        };

        // Attempt 1: ~100ms + up to 25ms jitter
        let d1 = policy.compute_delay(1, None);
        assert!(d1 >= Duration::from_millis(100) && d1 <= Duration::from_millis(125));

        // Attempt 2: ~200ms + up to 50ms jitter
        let d2 = policy.compute_delay(2, None);
        assert!(d2 >= Duration::from_millis(200) && d2 <= Duration::from_millis(250));

        // Attempt 3: ~400ms + up to 100ms jitter
        let d3 = policy.compute_delay(3, None);
        assert!(d3 >= Duration::from_millis(400) && d3 <= Duration::from_millis(500));

        // High attempt capped by max_delay (2s)
        let d_high = policy.compute_delay(10, None);
        assert_eq!(d_high, Duration::from_secs(2));

        // Respect explicit retry_after
        let d_ra = policy.compute_delay(1, Some(Duration::from_secs(1)));
        assert_eq!(d_ra, Duration::from_secs(1));
    }
}
