//! Integration tests for probing and single-stream downloading against the mock test server.

use std::fs;
use tempfile::tempdir;
use tenun_core::{download_single, probe, RequestOptions};
use tenun_test_server::{TestServer, TestServerConfig};

#[tokio::test(flavor = "multi_thread")]
async fn test_download_single_range_server() {
    let test_data: Vec<u8> = (0..500_000).map(|i| (i % 256) as u8).collect();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        content_disposition: Some("attachment; filename=\"test_file.bin\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions::default();

    let downloaded_path = download_single(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download should succeed");

    assert!(downloaded_path.exists());
    let downloaded_data = fs::read(&downloaded_path).expect("read downloaded file");
    assert_eq!(downloaded_data, test_data);

    // Verify against curl output asynchronously if curl is present
    let curl_path = temp_dir.path().join("curl_out.bin");
    if let Ok(status) = tokio::process::Command::new("curl")
        .args([
            "-s",
            "-o",
            curl_path.to_str().unwrap(),
            &server.download_url(),
        ])
        .status()
        .await
    {
        if status.success() {
            let curl_data = fs::read(&curl_path).expect("read curl file");
            assert_eq!(downloaded_data, curl_data);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_single_no_range_server() {
    let test_data = b"Arbitrary payload from server without range support".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        range_enabled: false,
        content_disposition: Some("attachment; filename=\"no_range.txt\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions::default();

    let downloaded_path = download_single(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download should succeed");

    assert!(downloaded_path.exists());
    let downloaded_data = fs::read(&downloaded_path).expect("read downloaded file");
    assert_eq!(downloaded_data, test_data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_single_zero_byte_file() {
    let test_data: Vec<u8> = Vec::new();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data,
        content_disposition: Some("attachment; filename=\"empty.txt\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions::default();

    let downloaded_path = download_single(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download should succeed");

    assert!(downloaded_path.exists());
    let downloaded_data = fs::read(&downloaded_path).expect("read downloaded file");
    assert!(downloaded_data.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_content_disposition_rfc5987() {
    let test_data = b"Testing RFC 5987 UTF-8 filename".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        content_disposition: Some(
            "attachment; filename=\"fallback.dat\"; filename*=UTF-8''l%27%c3%a9t%c3%a9%202026.dat"
                .to_string(),
        ),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions::default();

    let downloaded_path = download_single(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download should succeed");

    assert_eq!(
        downloaded_path.file_name().unwrap().to_str().unwrap(),
        "l'été 2026.dat"
    );
    let downloaded_data = fs::read(&downloaded_path).expect("read file");
    assert_eq!(downloaded_data, test_data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_filename_clash_resolution() {
    let test_data = b"Clash test content".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        content_disposition: Some("attachment; filename=\"clash_target.dat\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions::default();

    let first = download_single(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("first download");
    let second = download_single(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("second download");

    assert_eq!(first, temp_dir.path().join("clash_target.dat"));
    assert_eq!(second, temp_dir.path().join("clash_target (1).dat"));
    assert_eq!(fs::read(&first).unwrap(), test_data);
    assert_eq!(fs::read(&second).unwrap(), test_data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_probe_metadata() {
    let test_data = b"Probing resource payload".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        etag: Some("\"probe-etag-v1\"".to_string()),
        last_modified: Some("Mon, 05 Oct 2026 15:00:00 GMT".to_string()),
        content_disposition: Some("attachment; filename=\"report.pdf\"".to_string()),
        ..Default::default()
    })
    .await;

    let opts = RequestOptions::default();
    let client = reqwest::Client::new();
    let info = probe(&client, &server.download_url(), &opts)
        .await
        .expect("probe should succeed");

    assert_eq!(info.filename, "report.pdf");
    assert_eq!(info.total_bytes, Some(test_data.len() as u64));
    assert!(info.range_supported);
    assert_eq!(info.etag.as_deref(), Some("\"probe-etag-v1\""));
    assert_eq!(
        info.last_modified.as_deref(),
        Some("Mon, 05 Oct 2026 15:00:00 GMT")
    );
}
