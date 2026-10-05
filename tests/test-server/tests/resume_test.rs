//! Integration tests for pause, atomic state checkpointing, crash recovery, and If-Range validation.

use std::fs;
use std::time::Duration;
use tempfile::tempdir;
use tenun_core::{
    download_segmented_with_cancel, resume_download, CancellationToken, RequestOptions, TenunError,
};
use tenun_test_server::{TestServer, TestServerConfig};

#[tokio::test(flavor = "multi_thread")]
async fn test_pause_and_resume_success() {
    let total_size: usize = 1024 * 1024; // 1 MiB
    let test_data: Vec<u8> = (0..total_size).map(|i| (i % 251) as u8).collect();

    // Throttled server to allow controlled pause mid-download
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        speed_limit_bytes_per_sec: Some(250 * 1024), // 250 KiB/s
        etag: Some("\"etag-resumable-v1\"".to_string()),
        content_disposition: Some("attachment; filename=\"resume_test.bin\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions {
        connections: Some(4),
        min_segment_size: Some(64 * 1024),
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();

    // Spawn download in background task
    let download_handle = {
        let url = server.download_url();
        let dest = temp_dir.path().to_path_buf();
        let token = cancel_token.clone();
        tokio::spawn(async move {
            download_segmented_with_cancel(&url, &dest, &opts, None, token).await
        })
    };

    // Allow download to run for 250ms, then pause cleanly
    tokio::time::sleep(Duration::from_millis(250)).await;
    cancel_token.cancel();

    let result = download_handle.await.expect("task join");
    match result {
        Err(TenunError::DownloadPaused) => {
            println!("Download successfully paused as expected.");
        }
        other => panic!("Expected DownloadPaused, got: {:?}", other),
    }

    let state_file = temp_dir.path().join("resume_test.bin.tenun");
    let part_file = temp_dir.path().join("resume_test.bin.part");
    assert!(state_file.exists(), "State file must exist after pause");
    assert!(part_file.exists(), "Part file must exist after pause");

    // Verify state file is valid JSON and has progress recorded
    let state = tenun_core::load_state(&state_file).expect("load state");
    let downloaded_at_pause = state.downloaded_bytes();
    assert!(
        downloaded_at_pause > 0,
        "Should have downloaded some bytes before pause"
    );
    assert!(
        downloaded_at_pause < (total_size as u64),
        "Should not be fully finished at pause"
    );

    // Resume the download
    let resume_token = CancellationToken::new();
    let final_path = resume_download(&state_file, None, resume_token)
        .await
        .expect("resume download should succeed");

    assert!(final_path.exists());
    assert!(
        !state_file.exists(),
        "State file should be cleaned up after completion"
    );
    assert!(
        !part_file.exists(),
        "Part file should be cleaned up after completion"
    );

    let final_data = fs::read(&final_path).expect("read final file");
    assert_eq!(final_data.len(), test_data.len());
    assert_eq!(
        final_data, test_data,
        "Resumed file must be byte-for-byte identical"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_resume_rejects_modified_remote_file() {
    let total_size: usize = 512 * 1024;
    let original_data: Vec<u8> = (0..total_size).map(|i| (i % 241) as u8).collect();

    let server = TestServer::spawn(TestServerConfig {
        data: original_data.clone(),
        speed_limit_bytes_per_sec: Some(200 * 1024),
        etag: Some("\"etag-original\"".to_string()),
        content_disposition: Some("attachment; filename=\"etag_test.bin\"".to_string()),
        ..Default::default()
    })
    .await;

    let temp_dir = tempdir().expect("create temp dir");
    let opts = RequestOptions {
        connections: Some(4),
        min_segment_size: Some(64 * 1024),
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();

    let download_handle = {
        let url = server.download_url();
        let dest = temp_dir.path().to_path_buf();
        let token = cancel_token.clone();
        tokio::spawn(async move {
            download_segmented_with_cancel(&url, &dest, &opts, None, token).await
        })
    };

    tokio::time::sleep(Duration::from_millis(250)).await;
    cancel_token.cancel();

    let _ = download_handle.await;

    let state_file = temp_dir.path().join("etag_test.bin.tenun");
    assert!(state_file.exists());

    // Server modifies file and updates ETag
    let new_data: Vec<u8> = (0..total_size).map(|i| ((i + 13) % 241) as u8).collect();
    server.modify_config(|cfg| {
        cfg.data = new_data;
        cfg.etag = Some("\"etag-modified-v2\"".to_string());
    });

    // Attempting to resume should detect validator change and reject
    let resume_result = resume_download(&state_file, None, CancellationToken::new()).await;

    match resume_result {
        Err(TenunError::RemoteFileChanged(msg)) => {
            println!("Correctly rejected resume with error: {msg}");
        }
        other => panic!("Expected RemoteFileChanged error, got: {:?}", other),
    }
}
