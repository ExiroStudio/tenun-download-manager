//! Integration tests for segmented downloading, parallel speedup, and fallbacks.

use std::fs;
use std::time::Instant;
use tempfile::tempdir;
use tenun_core::{download_segmented, RequestOptions};
use tenun_test_server::{TestServer, TestServerConfig};

#[tokio::test(flavor = "multi_thread")]
async fn test_segmented_speedup_over_single_connection() {
    // 512 KiB total payload
    let total_size: usize = 512 * 1024;
    let test_data: Vec<u8> = (0..total_size).map(|i| (i % 251) as u8).collect();

    // Rate limit server to 150 KiB/s per connection stream
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        speed_limit_bytes_per_sec: Some(150 * 1024),
        content_disposition: Some("attachment; filename=\"speedup_test.bin\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");

    // 1. Download with 1 connection
    let single_opts = RequestOptions {
        connections: Some(1),
        min_segment_size: Some(64 * 1024),
        ..Default::default()
    };

    let start_single = Instant::now();
    let single_path = download_segmented(
        &server.download_url(),
        &temp_dir.path().join("single"),
        &single_opts,
        None,
    )
    .await
    .expect("single connection download");
    let single_duration = start_single.elapsed();

    assert_eq!(fs::read(&single_path).expect("read single"), test_data);

    // 2. Download with 8 connections in parallel
    let multi_opts = RequestOptions {
        connections: Some(8),
        min_segment_size: Some(64 * 1024), // 512 KiB / 64 KiB = 8 segments
        ..Default::default()
    };

    let start_multi = Instant::now();
    let multi_path = download_segmented(
        &server.download_url(),
        &temp_dir.path().join("multi"),
        &multi_opts,
        None,
    )
    .await
    .expect("8 connections download");
    let multi_duration = start_multi.elapsed();

    assert_eq!(fs::read(&multi_path).expect("read multi"), test_data);

    // Assert that 8 connections is significantly faster than 1 connection
    println!(
        "Benchmark: 1 connection = {:.2?}, 8 connections = {:.2?} (Speedup: {:.2}x)",
        single_duration,
        multi_duration,
        single_duration.as_secs_f64() / multi_duration.as_secs_f64()
    );

    assert!(
        multi_duration < single_duration,
        "8 connections ({:?}) should be clearly faster than 1 connection ({:?})",
        multi_duration,
        single_duration
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segmented_integrity_large_file() {
    // 2 MiB payload
    let total_size: usize = 2 * 1024 * 1024;
    let test_data: Vec<u8> = (0..total_size).map(|i| (i % 255) as u8).collect();

    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        content_disposition: Some("attachment; filename=\"large_segmented.dat\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions {
        connections: Some(8),
        min_segment_size: Some(256 * 1024),
        ..Default::default()
    };

    let path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download large segmented file");

    let downloaded = fs::read(&path).expect("read large downloaded file");
    assert_eq!(downloaded.len(), test_data.len());
    assert_eq!(downloaded, test_data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segmented_fallback_when_server_returns_200() {
    let test_data = b"Server ignores Range and forces 200 OK".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        force_200_for_range: true, // Forces 200 OK on Range requests
        content_disposition: Some("attachment; filename=\"force_200.txt\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions {
        connections: Some(4),
        min_segment_size: Some(8),
        ..Default::default()
    };

    let path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("fallback to single stream should succeed");

    assert_eq!(fs::read(&path).expect("read file"), test_data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segmented_fallback_when_range_disabled() {
    let test_data = b"Server explicitly does not support range".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        range_enabled: false,
        content_disposition: Some("attachment; filename=\"no_range_fallback.txt\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions {
        connections: Some(8),
        min_segment_size: Some(4),
        ..Default::default()
    };

    let path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("range disabled fallback should succeed");

    assert_eq!(fs::read(&path).expect("read file"), test_data);
}
