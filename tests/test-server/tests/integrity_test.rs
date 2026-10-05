//! Integration tests for data integrity verification, HTTP checksum headers, and deduplication clash resolution.

use base64::prelude::*;
use md5::Md5;
use sha2::{Digest, Sha256};
use std::fs;
use tempfile::tempdir;
use tenun_core::{
    compute_file_checksum, download_segmented, download_single, ChecksumAlgorithm, ClashResolution,
    ExpectedChecksum, RequestOptions, TenunError,
};
use tenun_test_server::{TestServer, TestServerConfig};

#[tokio::test(flavor = "multi_thread")]
async fn test_digest_header_automatic_verification() {
    let test_data: Vec<u8> = (0..200_000).map(|i| ((i * 7) % 256) as u8).collect();
    let mut hasher = Sha256::new();
    hasher.update(&test_data);
    let raw_sha256 = hasher.finalize();
    let b64_sha256 = BASE64_STANDARD.encode(raw_sha256);

    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        content_disposition: Some("attachment; filename=\"digest_file.dat\"".to_string()),
        digest: Some(format!("SHA-256={b64_sha256}")),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions::default();

    let downloaded_path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download with Digest header should succeed and verify");

    assert!(downloaded_path.exists());
    let downloaded_data = fs::read(&downloaded_path).expect("read downloaded file");
    assert_eq!(downloaded_data, test_data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_content_md5_header_automatic_verification() {
    let test_data: Vec<u8> = (0..200_000).map(|i| ((i * 13) % 256) as u8).collect();
    let mut hasher = Md5::new();
    hasher.update(&test_data);
    let raw_md5 = hasher.finalize();
    let b64_md5 = BASE64_STANDARD.encode(raw_md5);

    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        content_disposition: Some("attachment; filename=\"md5_file.bin\"".to_string()),
        content_md5: Some(b64_md5),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions::default();

    let downloaded_path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download with Content-MD5 header should succeed");

    assert!(downloaded_path.exists());
    let downloaded_data = fs::read(&downloaded_path).expect("read downloaded file");
    assert_eq!(downloaded_data, test_data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_etag_md5_automatic_verification() {
    let test_data: Vec<u8> = (0..200_000).map(|i| ((i * 17) % 256) as u8).collect();
    let mut hasher = Md5::new();
    hasher.update(&test_data);
    let md5_hex = hex::encode(hasher.finalize());

    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        content_disposition: Some("attachment; filename=\"etag_file.bin\"".to_string()),
        etag: Some(format!("\"{md5_hex}\"")),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions::default();

    let downloaded_path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download with standard ETag MD5 should succeed");

    assert!(downloaded_path.exists());
    let downloaded_data = fs::read(&downloaded_path).expect("read downloaded file");
    assert_eq!(downloaded_data, test_data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_integrity_mismatch_fails_and_preserves_part() {
    let test_data = b"Real server payload for integrity mismatch test".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data,
        content_disposition: Some("attachment; filename=\"mismatch.txt\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let fake_sha256 = "0".repeat(64);
    let opts = RequestOptions {
        expected_checksum: Some(ExpectedChecksum::new(
            ChecksumAlgorithm::Sha256,
            &fake_sha256,
        )),
        ..Default::default()
    };

    let result = download_single(&server.download_url(), temp_dir.path(), &opts, None).await;

    match result {
        Err(TenunError::IntegrityMismatch(msg)) => {
            assert!(msg.contains("Checksum mismatch"));
        }
        other => panic!("Expected IntegrityMismatch, got: {other:?}"),
    }

    let final_path = temp_dir.path().join("mismatch.txt");
    let part_path = temp_dir.path().join("mismatch.txt.part");

    assert!(
        !final_path.exists(),
        "Final target must not be created on mismatch"
    );
    assert!(
        part_path.exists(),
        "Part file must be preserved for forensic analysis"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_deduplication_skips_download_when_checksum_matches() {
    let test_data = b"Deduplication test data payload that already exists locally".to_vec();
    let temp_dir = tempdir().expect("create temp dir");
    let existing_file = temp_dir.path().join("dedup_file.bin");
    fs::write(&existing_file, &test_data).expect("write existing file");

    let actual_sha256 =
        compute_file_checksum(&existing_file, ChecksumAlgorithm::Sha256).expect("compute sha256");

    let server = TestServer::spawn(TestServerConfig {
        data: test_data,
        content_disposition: Some("attachment; filename=\"dedup_file.bin\"".to_string()),
        ..Default::default()
    })
    .await;

    let opts = RequestOptions {
        clash_resolution: ClashResolution::Deduplicate,
        expected_checksum: Some(ExpectedChecksum::new(
            ChecksumAlgorithm::Sha256,
            &actual_sha256,
        )),
        ..Default::default()
    };

    let initial_requests = server.request_count();
    let result = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("deduplication should succeed immediately");

    assert_eq!(result, existing_file);
    // Only probe was performed, no range segment downloads were triggered!
    assert!(server.request_count() <= initial_requests + 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_deduplication_mismatch_falls_back_to_rename() {
    let old_data = b"Old existing local file content".to_vec();
    let new_data = b"New fresh remote file content from server".to_vec();

    let temp_dir = tempdir().expect("create temp dir");
    let existing_file = temp_dir.path().join("doc.txt");
    fs::write(&existing_file, &old_data).expect("write old file");

    let mut hasher = Sha256::new();
    hasher.update(&new_data);
    let new_sha256 = hex::encode(hasher.finalize());

    let server = TestServer::spawn(TestServerConfig {
        data: new_data.clone(),
        content_disposition: Some("attachment; filename=\"doc.txt\"".to_string()),
        ..Default::default()
    })
    .await;

    let opts = RequestOptions {
        clash_resolution: ClashResolution::Deduplicate,
        expected_checksum: Some(ExpectedChecksum::new(
            ChecksumAlgorithm::Sha256,
            &new_sha256,
        )),
        ..Default::default()
    };

    let downloaded_path = download_segmented(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("download should fall back to rename and succeed");

    assert_eq!(downloaded_path, temp_dir.path().join("doc (1).txt"));
    assert_eq!(fs::read(&existing_file).expect("read old"), old_data);
    assert_eq!(fs::read(&downloaded_path).expect("read new"), new_data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clash_resolution_overwrite() {
    let old_data = b"Stale local file content to be overwritten".to_vec();
    let new_data = b"Brand new file content replacing old".to_vec();

    let temp_dir = tempdir().expect("create temp dir");
    let target_file = temp_dir.path().join("output.bin");
    fs::write(&target_file, &old_data).expect("write initial file");

    let server = TestServer::spawn(TestServerConfig {
        data: new_data.clone(),
        content_disposition: Some("attachment; filename=\"output.bin\"".to_string()),
        ..Default::default()
    })
    .await;

    let opts = RequestOptions {
        clash_resolution: ClashResolution::Overwrite,
        ..Default::default()
    };

    let downloaded_path = download_single(&server.download_url(), temp_dir.path(), &opts, None)
        .await
        .expect("overwrite download should succeed");

    assert_eq!(downloaded_path, target_file);
    assert_eq!(
        fs::read(&target_file).expect("read overwritten file"),
        new_data
    );
}
