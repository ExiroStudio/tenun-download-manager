//! Integration test for DownloadManager concurrency, SQLite persistence, and crash recovery.

use std::collections::HashSet;
use std::fs;
use std::time::Duration;
use tempfile::tempdir;
use tenun_core::{
    AddDownloadRequest, Database, DownloadManager, DownloadManagerConfig, DownloadStatus,
    FileCategory, ManagerEvent, RequestOptions,
};
use tenun_test_server::{generate_file_content, TestServer, TestServerConfig};
use tokio::time::{sleep, timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manager_queue_concurrency_20_downloads_and_crash_recovery() {
    let server = TestServer::spawn(TestServerConfig::default()).await;
    let temp_dir = tempdir().expect("create temp dir");
    let db_path = temp_dir.path().join("tenun_queue.db");

    let total_downloads = 20usize;
    let max_concurrent = 3usize;

    // Phase 1: Initialize DownloadManager and queue all 20 downloads
    let db = Database::open(&db_path).expect("open database");
    let config = DownloadManagerConfig {
        max_concurrent_downloads: max_concurrent,
        default_download_dir: temp_dir.path().to_path_buf(),
        global_speed_limit: None,
    };

    let manager = DownloadManager::new(db.clone(), config.clone()).expect("create manager");
    let mut subscriber = manager.subscribe();

    for i in 1..=total_downloads {
        let filename = format!("file_{i}.bin");
        let url = server.file_url(&filename);
        let req = AddDownloadRequest {
            url,
            output_dir: Some(temp_dir.path().to_path_buf()),
            filename: Some(filename),
            options: RequestOptions {
                connections: Some(4),
                ..Default::default()
            },
            priority: (total_downloads - i) as i32,
            category: Some(FileCategory::General),
            start_now: true,
        };
        let id = manager.add(req).await.expect("add download");
        assert_eq!(id, i as u64);
    }

    // Monitor progress until at least 5 downloads complete
    let mut completed_before_restart = HashSet::new();
    let collect_phase1 = async {
        while let Ok(event) = subscriber.recv().await {
            if let ManagerEvent::Finished { id, .. } = event {
                completed_before_restart.insert(id);
                if completed_before_restart.len() >= 5 {
                    break;
                }
            }
        }
    };

    timeout(Duration::from_secs(30), collect_phase1)
        .await
        .expect("phase 1 should complete at least 5 downloads within 30s");

    assert!(
        completed_before_restart.len() >= 5,
        "Should have completed at least 5 items before restart"
    );

    // Phase 2: Simulate crash and shutdown
    manager.shutdown().await;
    drop(manager);
    sleep(Duration::from_millis(200)).await;

    // Verify recovery on new database connection
    let db_reopened = Database::open(&db_path).expect("reopen database");
    let recovered_count = db_reopened
        .recover_interrupted_downloads()
        .expect("recover");
    println!("Recovered {recovered_count} interrupted downloads");

    // Phase 3: Start new DownloadManager on same database and resume all unfinished
    let manager_restarted =
        DownloadManager::new(db_reopened.clone(), config.clone()).expect("restart manager");
    let mut sub_restarted = manager_restarted.subscribe();

    let all_records = db_reopened.list(None, None).expect("list records");
    for record in all_records {
        if record.status.is_resumable() && record.status != DownloadStatus::Completed {
            manager_restarted
                .resume(record.id)
                .await
                .expect("resume download");
        }
    }

    // Monitor until all 20 downloads finish
    let mut all_completed = HashSet::new();
    // Pre-populate already completed in DB
    for record in db_reopened.list(None, None).expect("list records") {
        if record.status == DownloadStatus::Completed {
            all_completed.insert(record.id);
        }
    }

    let collect_phase2 = async {
        while all_completed.len() < total_downloads {
            match sub_restarted.recv().await {
                Ok(ManagerEvent::Finished { id, .. }) => {
                    all_completed.insert(id);
                }
                Ok(ManagerEvent::Failed { id, error }) => {
                    panic!("Download {id} failed unexpectedly: {error}");
                }
                _ => {}
            }
        }
    };

    timeout(Duration::from_secs(45), collect_phase2)
        .await
        .expect("all 20 downloads should finish within timeout");

    assert_eq!(all_completed.len(), total_downloads);

    // Phase 4: Verify 100% byte-for-byte data integrity for all 20 files
    let final_records = db_reopened.list(None, None).expect("final list");
    assert_eq!(final_records.len(), total_downloads);

    for r in &final_records {
        assert_eq!(
            r.status,
            DownloadStatus::Completed,
            "Record {} should be completed",
            r.id
        );
        let file_path = temp_dir.path().join(&r.filename);
        assert!(
            file_path.exists(),
            "Downloaded file {} must exist",
            file_path.display()
        );

        let metadata = fs::metadata(&file_path).expect("file metadata");
        assert_eq!(
            metadata.len(),
            65_536,
            "File length must match exact expected size"
        );

        let actual_bytes = fs::read(&file_path).expect("read file bytes");
        let expected_bytes = generate_file_content(&r.filename, 65_536);
        assert_eq!(
            actual_bytes, expected_bytes,
            "File {} data integrity mismatch",
            r.filename
        );

        // Verify sidecar state and part files were cleaned up
        let part_file = temp_dir.path().join(format!("{}.part", r.filename));
        let state_file = temp_dir.path().join(format!("{}.tenun", r.filename));
        assert!(
            !part_file.exists(),
            "Part file {} should be cleaned up",
            part_file.display()
        );
        assert!(
            !state_file.exists(),
            "State file {} should be cleaned up",
            state_file.display()
        );
    }

    manager_restarted.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_manager_priority_ordering() {
    let server = TestServer::spawn(TestServerConfig::default()).await;
    let temp_dir = tempdir().expect("create temp dir");
    let db_path = temp_dir.path().join("tenun_priority.db");

    let db = Database::open(&db_path).expect("open database");
    let config = DownloadManagerConfig {
        max_concurrent_downloads: 1, // Strict serial execution
        default_download_dir: temp_dir.path().to_path_buf(),
        global_speed_limit: None,
    };

    let manager = DownloadManager::new(db.clone(), config).expect("create manager");
    let mut subscriber = manager.subscribe();

    // 1. First start a blocking download that takes up the single slot
    let blocker_req = AddDownloadRequest {
        url: server.file_url("blocker.bin"),
        output_dir: Some(temp_dir.path().to_path_buf()),
        filename: Some("blocker.bin".to_string()),
        options: RequestOptions::default(),
        priority: 0,
        category: None,
        start_now: true,
    };
    let blocker_id = manager.add(blocker_req).await.expect("add blocker");

    // Wait until blocker is downloading
    loop {
        if let Ok(ManagerEvent::StatusChanged { id, status }) = subscriber.recv().await {
            if id == blocker_id && status == DownloadStatus::Downloading {
                break;
            }
        }
    }

    // 2. While blocker is running, add low priority first, then high priority
    let low_req = AddDownloadRequest {
        url: server.file_url("low.bin"),
        output_dir: Some(temp_dir.path().to_path_buf()),
        filename: Some("low.bin".to_string()),
        options: RequestOptions::default(),
        priority: 1,
        category: None,
        start_now: true, // will stay Queued because blocker holds the slot
    };
    let low_id = manager.add(low_req).await.expect("add low");

    let high_req = AddDownloadRequest {
        url: server.file_url("high.bin"),
        output_dir: Some(temp_dir.path().to_path_buf()),
        filename: Some("high.bin".to_string()),
        options: RequestOptions::default(),
        priority: 10,
        category: None,
        start_now: true, // will stay Queued because blocker holds the slot
    };
    let high_id = manager.add(high_req).await.expect("add high");

    // 3. Collect completion order
    let mut order = Vec::new();
    let collect_order = async {
        while let Ok(event) = subscriber.recv().await {
            if let ManagerEvent::Finished { id, .. } = event {
                order.push(id);
                if order.len() == 3 {
                    break;
                }
            }
        }
    };

    timeout(Duration::from_secs(15), collect_order)
        .await
        .expect("downloads finish");

    // Blocker finishes first, then high priority (10 > 1), then low priority
    assert_eq!(order, vec![blocker_id, high_id, low_id]);

    manager.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_manager_cli_subcommands_lifecycle() {
    let temp_dir = tempdir().expect("create temp dir");
    let data_home = temp_dir.path().join("data_home");
    std::fs::create_dir_all(&data_home).expect("create data home");

    let bin_path = std::env::current_exe()
        .expect("test exe")
        .parent()
        .expect("deps dir")
        .parent()
        .expect("target/debug dir")
        .join(if cfg!(windows) { "tenun.exe" } else { "tenun" });

    // 1. Add download via CLI
    let add_out = tokio::process::Command::new(&bin_path)
        .env("XDG_DATA_HOME", &data_home)
        .args([
            "add",
            "https://example.com/software/tool.zip",
            "--priority",
            "7",
            "--category",
            "compressed",
        ])
        .output()
        .await
        .expect("run tenun add");

    assert!(add_out.status.success());
    let add_str = String::from_utf8_lossy(&add_out.stdout);
    assert!(add_str.contains("Added download #1 (tool.zip)"));
    assert!(add_str.contains("Category: Compressed"));
    assert!(add_str.contains("Status:   Paused"));
    assert!(add_str.contains("Priority: 7"));

    // 2. List downloads via CLI
    let list_out = tokio::process::Command::new(&bin_path)
        .env("XDG_DATA_HOME", &data_home)
        .args(["list"])
        .output()
        .await
        .expect("run tenun list");

    assert!(list_out.status.success());
    let list_str = String::from_utf8_lossy(&list_out.stdout);
    assert!(list_str.contains("tool.zip"));
    assert!(list_str.contains("Paused"));
    assert!(list_str.contains("Compressed"));

    // 3. Resume download via CLI
    let resume_out = tokio::process::Command::new(&bin_path)
        .env("XDG_DATA_HOME", &data_home)
        .args(["resume", "1"])
        .output()
        .await
        .expect("run tenun resume");

    assert!(resume_out.status.success());
    let resume_str = String::from_utf8_lossy(&resume_out.stdout);
    assert!(resume_str.contains("Download #1 (tool.zip) resumed and queued."));

    // 4. Verify status updated to Queued via CLI list
    let list_queued_out = tokio::process::Command::new(&bin_path)
        .env("XDG_DATA_HOME", &data_home)
        .args(["list", "--status", "Queued"])
        .output()
        .await
        .expect("run tenun list --status Queued");

    assert!(list_queued_out.status.success());
    let list_queued_str = String::from_utf8_lossy(&list_queued_out.stdout);
    assert!(list_queued_str.contains("tool.zip"));
    assert!(list_queued_str.contains("Queued"));

    // 5. Pause download via CLI
    let pause_out = tokio::process::Command::new(&bin_path)
        .env("XDG_DATA_HOME", &data_home)
        .args(["pause", "1"])
        .output()
        .await
        .expect("run tenun pause");

    assert!(pause_out.status.success());
    let pause_str = String::from_utf8_lossy(&pause_out.stdout);
    assert!(pause_str.contains("Download #1 (tool.zip) paused."));

    // 6. Cancel download via CLI
    let cancel_out = tokio::process::Command::new(&bin_path)
        .env("XDG_DATA_HOME", &data_home)
        .args(["cancel", "1"])
        .output()
        .await
        .expect("run tenun cancel");

    assert!(cancel_out.status.success());
    let cancel_str = String::from_utf8_lossy(&cancel_out.stdout);
    assert!(cancel_str.contains("Download #1 (tool.zip) cancelled."));

    // 7. Remove download via CLI
    let remove_out = tokio::process::Command::new(&bin_path)
        .env("XDG_DATA_HOME", &data_home)
        .args(["remove", "1"])
        .output()
        .await
        .expect("run tenun remove");

    assert!(remove_out.status.success());
    let remove_str = String::from_utf8_lossy(&remove_out.stdout);
    assert!(remove_str.contains("Download #1 (tool.zip) removed from database."));

    // 8. List should now be empty
    let list_empty_out = tokio::process::Command::new(&bin_path)
        .env("XDG_DATA_HOME", &data_home)
        .args(["list"])
        .output()
        .await
        .expect("run tenun list after remove");

    assert!(list_empty_out.status.success());
    let list_empty_str = String::from_utf8_lossy(&list_empty_out.stdout);
    assert!(list_empty_str.contains("No downloads found matching criteria."));
}
