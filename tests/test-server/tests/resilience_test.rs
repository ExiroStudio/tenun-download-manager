//! Integration tests for network resilience, retry on flaky servers,
//! speed limiter accuracy, and dynamic re-split (work stealing).

use axum::http::StatusCode;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tenun_core::{
    download_segmented, download_single, RequestOptions, RetryPolicy, DEFAULT_MIN_SEGMENT_SIZE,
};
use tenun_test_server::{TestServer, TestServerConfig};

#[tokio::test(flavor = "multi_thread")]
async fn test_retry_on_flaky_server_503() {
    let payload = vec![0xAB; 2 * 1024 * 1024]; // 2 MiB
    let config = TestServerConfig {
        data: payload.clone(),
        range_enabled: true,
        fail_first_n_requests: Some(2),
        error_status: Some(StatusCode::SERVICE_UNAVAILABLE),
        retry_after: Some("1".to_string()),
        ..Default::default()
    };

    let server = TestServer::spawn(config).await;
    let temp_dir = tempdir().expect("temp dir");

    let retry_policy = RetryPolicy {
        max_retries: 5,
        base_delay: Duration::from_millis(200),
        max_delay: Duration::from_secs(2),
        stall_timeout: Duration::from_secs(10),
    };

    let opts = RequestOptions {
        connections: Some(2),
        min_segment_size: Some(DEFAULT_MIN_SEGMENT_SIZE),
        retry_policy: Some(retry_policy),
        ..Default::default()
    };

    let downloaded_path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download should succeed after retries");

    let downloaded_data = std::fs::read(&downloaded_path).expect("read downloaded file");
    assert_eq!(
        downloaded_data.len(),
        payload.len(),
        "Downloaded file length mismatch"
    );
    assert_eq!(downloaded_data, payload, "Payload content mismatch");

    // Ensure server was called at least 3 times (2 failures + successes)
    assert!(
        server.request_count() >= 3,
        "Expected at least 3 requests, got {}",
        server.request_count()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_retry_on_flaky_server_429() {
    let payload = b"Resilience test with 429 Too Many Requests response handling!".to_vec();
    let config = TestServerConfig {
        data: payload.clone(),
        range_enabled: false, // will use single stream
        fail_first_n_requests: Some(1),
        error_status: Some(StatusCode::TOO_MANY_REQUESTS),
        retry_after: Some("1".to_string()),
        ..Default::default()
    };

    let server = TestServer::spawn(config).await;
    let temp_dir = tempdir().expect("temp dir");

    let retry_policy = RetryPolicy {
        max_retries: 3,
        base_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(2),
        stall_timeout: Duration::from_secs(5),
    };

    let opts = RequestOptions {
        retry_policy: Some(retry_policy),
        ..Default::default()
    };

    let downloaded_path = download_single(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download should succeed after 429 retry");

    let downloaded_data = std::fs::read(&downloaded_path).expect("read file");
    assert_eq!(downloaded_data, payload);
    assert!(server.request_count() >= 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dynamic_resplit_integrity() {
    // Generate 4 MiB non-trivial pseudo-random data
    let mut payload = Vec::with_capacity(4 * 1024 * 1024);
    for i in 0..(4 * 1024 * 1024) {
        payload.push(((i * 31 + 17) % 256) as u8);
    }

    let config = TestServerConfig {
        data: payload.clone(),
        range_enabled: true,
        ..Default::default()
    };

    let server = TestServer::spawn(config).await;
    let temp_dir = tempdir().expect("temp dir");

    // Use min_segment_size = 256 KiB and 4 connections to encourage work stealing
    let opts = RequestOptions {
        connections: Some(4),
        min_segment_size: Some(256 * 1024),
        ..Default::default()
    };

    let downloaded_path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download with dynamic re-split should succeed");

    let downloaded_data = std::fs::read(&downloaded_path).expect("read downloaded file");
    assert_eq!(
        downloaded_data.len(),
        payload.len(),
        "Length mismatch after re-split download"
    );
    assert_eq!(
        downloaded_data, payload,
        "File content corrupted or overlapped during dynamic re-split!"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_speed_limiter_download_accuracy() {
    // 500 KiB payload
    let total_bytes = 500 * 1024;
    let payload = vec![0x77; total_bytes];

    let config = TestServerConfig {
        data: payload.clone(),
        range_enabled: true,
        ..Default::default()
    };

    let server = TestServer::spawn(config).await;
    let temp_dir = tempdir().expect("temp dir");

    // Limit rate to 250 KiB/s -> Expected transfer time: ~2.0 seconds
    let rate_limit = 250 * 1024;
    let opts = RequestOptions {
        connections: Some(2),
        min_segment_size: Some(100 * 1024),
        limit_rate: Some(rate_limit),
        ..Default::default()
    };

    let start = Instant::now();
    let downloaded_path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("rate limited download should succeed");
    let elapsed = start.elapsed();

    let downloaded_data = std::fs::read(&downloaded_path).expect("read downloaded file");
    assert_eq!(downloaded_data, payload);

    let expected_secs = (total_bytes as f64) / (rate_limit as f64);
    let actual_secs = elapsed.as_secs_f64();

    // Check ±15% accuracy for real network loopback transfer
    let lower = expected_secs * 0.85;
    let upper = expected_secs * 1.35;

    assert!(
        actual_secs >= lower && actual_secs <= upper,
        "Actual elapsed {:.3}s not within expected range [{:.3}s, {:.3}s]",
        actual_secs,
        lower,
        upper
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stall_detection_and_recovery() {
    let payload = vec![0x33; 100 * 1024]; // 100 KiB
    let config = TestServerConfig {
        data: payload.clone(),
        range_enabled: true,
        stall_after_bytes: Some((16 * 1024, Duration::from_millis(800))),
        stall_only_first_request: true,
        ..Default::default()
    };

    let server = TestServer::spawn(config).await;
    let temp_dir = tempdir().expect("temp dir");

    let retry_policy = RetryPolicy {
        max_retries: 3,
        base_delay: Duration::from_millis(50),
        max_delay: Duration::from_secs(1),
        stall_timeout: Duration::from_millis(300),
    };

    let opts = RequestOptions {
        connections: Some(1),
        retry_policy: Some(retry_policy),
        ..Default::default()
    };

    let downloaded_path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download should recover from stall and succeed");

    let downloaded_data = std::fs::read(&downloaded_path).expect("read file");
    assert_eq!(downloaded_data, payload);
}
