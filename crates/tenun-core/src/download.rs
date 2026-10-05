//! Downloading engines: single-stream and high-speed multi-segment downloads.

use crate::error::{Result, TenunError};
use crate::platform::{atomic_rename_file, sync_file, PositionalWrite};
use crate::probe::{apply_options, build_client, probe, RequestOptions};
use crate::segment::{calculate_segments, DEFAULT_CONNECTIONS, DEFAULT_MIN_SEGMENT_SIZE};
use reqwest::header::{self, HeaderValue};
use reqwest::StatusCode;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinSet;
use tracing::{info, warn};

/// Buffer size per positional write operation (256 KiB).
pub const WRITE_BUFFER_CAPACITY: usize = 256 * 1024;

/// Events emitted during the download lifecycle.
#[derive(Debug, Clone)]
pub enum DownloadEvent {
    /// Download initialized with file metadata.
    Started {
        /// Target filename.
        filename: String,
        /// Total file size, if known.
        total_bytes: Option<u64>,
    },
    /// Periodic progress update.
    Progress {
        /// Total bytes downloaded so far across all segments.
        downloaded_bytes: u64,
        /// Total expected bytes.
        total_bytes: Option<u64>,
        /// Download speed in bytes per second.
        speed_bytes_per_sec: f64,
    },
    /// Download completed successfully and verified.
    Finished {
        /// Path to the final output file.
        path: PathBuf,
        /// Total bytes written.
        total_bytes: u64,
    },
}

/// Resolves a unique filename in `dest_dir` to prevent overwriting existing files,
/// appending ` (1)`, ` (2)`, etc. when a clash occurs.
pub fn resolve_unique_filename(dest_dir: &Path, filename: &str) -> PathBuf {
    let initial_path = dest_dir.join(filename);
    if !initial_path.exists() {
        return initial_path;
    }

    let (stem, ext) = match filename.rsplit_once('.') {
        Some((s, e)) => (s, Some(e)),
        None => (filename, None),
    };

    let mut counter = 1;
    loop {
        let candidate_name = match ext {
            Some(e) => format!("{stem} ({counter}).{e}"),
            None => format!("{stem} ({counter})"),
        };
        let candidate_path = dest_dir.join(candidate_name);
        if !candidate_path.exists() {
            return candidate_path;
        }
        counter += 1;
    }
}

/// Performs a single-stream download to `<filename>.part` and renames to the final target upon completion.
pub async fn download_single(
    url: &str,
    dest_dir: &Path,
    opts: &RequestOptions,
    event_tx: Option<UnboundedSender<DownloadEvent>>,
) -> Result<PathBuf> {
    let client = build_client(opts)?;

    info!("Probing remote resource: {url}");
    let remote_info = probe(&client, url, opts).await?;
    info!(
        "Probe completed: filename='{}', size={:?}",
        remote_info.filename, remote_info.total_bytes
    );

    let final_path = resolve_unique_filename(dest_dir, &remote_info.filename);
    let final_filename = final_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(&remote_info.filename)
        .to_string();

    let part_path = dest_dir.join(format!("{final_filename}.part"));

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Started {
            filename: final_filename.clone(),
            total_bytes: remote_info.total_bytes,
        });
    }

    let mut req = client.get(&remote_info.final_url);
    req = apply_options(req, opts)?;

    let mut response = req
        .send()
        .await
        .map_err(|e| TenunError::Network(e.to_string()))?;

    if !response.status().is_success() {
        return Err(TenunError::Network(format!(
            "Server returned error status {}",
            response.status()
        )));
    }

    let mut downloaded: u64 = 0;
    let mut last_tick = Instant::now();
    let mut last_downloaded = 0u64;

    {
        let mut file = File::create(&part_path).map_err(TenunError::Io)?;

        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| TenunError::Network(e.to_string()))?
        {
            file.write_all(&chunk).map_err(TenunError::Io)?;
            downloaded += chunk.len() as u64;

            let elapsed = last_tick.elapsed();
            if elapsed >= Duration::from_millis(200) {
                let bytes_in_interval = downloaded - last_downloaded;
                let speed = (bytes_in_interval as f64) / elapsed.as_secs_f64();

                if let Some(ref tx) = event_tx {
                    let _ = tx.send(DownloadEvent::Progress {
                        downloaded_bytes: downloaded,
                        total_bytes: remote_info.total_bytes,
                        speed_bytes_per_sec: speed,
                    });
                }

                last_tick = Instant::now();
                last_downloaded = downloaded;
            }
        }

        file.flush().map_err(TenunError::Io)?;
        sync_file(&file).map_err(TenunError::Io)?;
    }

    atomic_rename_file(&part_path, &final_path)?;

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Finished {
            path: final_path.clone(),
            total_bytes: downloaded,
        });
    }

    Ok(final_path)
}

/// Performs a parallel segmented download using multiple HTTP Range requests.
///
/// Features:
/// - Preallocates disk file via `set_len`.
/// - Each worker writes to its offset using `PositionalWrite`.
/// - Validates `206 Partial Content` and exact `Content-Range`.
/// - If server returns `200 OK` for Range or does not support Range, falls back to `download_single`.
/// - Emits real-time progress, moving average speed, and ETA.
pub async fn download_segmented(
    url: &str,
    dest_dir: &Path,
    opts: &RequestOptions,
    event_tx: Option<UnboundedSender<DownloadEvent>>,
) -> Result<PathBuf> {
    let client = build_client(opts)?;

    info!("Probing remote resource for segmented download: {url}");
    let remote_info = probe(&client, url, opts).await?;

    let total_size = match remote_info.total_bytes {
        Some(s) if s > 0 && remote_info.range_supported => s,
        _ => {
            info!("Server does not support Range or size unknown; falling back to single stream");
            return download_single(url, dest_dir, opts, event_tx).await;
        }
    };

    let conn_count = opts.connections.unwrap_or(DEFAULT_CONNECTIONS);
    let min_seg = opts.min_segment_size.unwrap_or(DEFAULT_MIN_SEGMENT_SIZE);
    let segments = calculate_segments(total_size, conn_count, min_seg);

    if segments.len() <= 1 {
        info!("File size requires only 1 segment; using single stream");
        return download_single(url, dest_dir, opts, event_tx).await;
    }

    let final_path = resolve_unique_filename(dest_dir, &remote_info.filename);
    let final_filename = final_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(&remote_info.filename)
        .to_string();
    let part_path = dest_dir.join(format!("{final_filename}.part"));

    info!(
        "Preallocating {} bytes for {} ({} segments)",
        total_size,
        part_path.display(),
        segments.len()
    );

    let base_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&part_path)
        .map_err(TenunError::Io)?;

    base_file.set_len(total_size).map_err(TenunError::Io)?;
    sync_file(&base_file).map_err(TenunError::Io)?;

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Started {
            filename: final_filename.clone(),
            total_bytes: Some(total_size),
        });
    }

    // Atomic progress tracking
    let segment_counters: Arc<Vec<Arc<AtomicU64>>> = Arc::new(
        segments
            .iter()
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect(),
    );

    let is_running = Arc::new(AtomicBool::new(true));

    // Background progress aggregation task
    let progress_join_handle = {
        let counters = Arc::clone(&segment_counters);
        let tx = event_tx.clone();
        let running = Arc::clone(&is_running);

        tokio::spawn(async move {
            let mut last_tick = Instant::now();
            let mut last_downloaded = 0u64;

            while running.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(200)).await;

                let downloaded: u64 = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
                let elapsed = last_tick.elapsed();

                if elapsed.as_secs_f64() > 0.0 {
                    let bytes_in_interval = downloaded.saturating_sub(last_downloaded);
                    let speed = (bytes_in_interval as f64) / elapsed.as_secs_f64();

                    if let Some(ref sender) = tx {
                        let _ = sender.send(DownloadEvent::Progress {
                            downloaded_bytes: downloaded,
                            total_bytes: Some(total_size),
                            speed_bytes_per_sec: speed,
                        });
                    }

                    last_tick = Instant::now();
                    last_downloaded = downloaded;
                }
            }
        })
    };

    // Spawn download worker tasks
    let mut workers = JoinSet::new();

    for (idx, seg) in segments.iter().cloned().enumerate() {
        let worker_client = client.clone();
        let worker_file = base_file.try_clone().map_err(TenunError::Io)?;
        let worker_opts = opts.clone();
        let final_url = remote_info.final_url.clone();
        let counter = Arc::clone(&segment_counters[idx]);

        workers.spawn(async move {
            let range_val = format!("bytes={}-{}", seg.start, seg.end);
            let mut req = worker_client
                .get(&final_url)
                .header(header::RANGE, HeaderValue::from_str(&range_val).unwrap());
            req = apply_options(req, &worker_opts)?;

            let mut response = req
                .send()
                .await
                .map_err(|e| TenunError::Network(e.to_string()))?;

            let status = response.status();

            // Check for server 200 OK fallback
            if status == StatusCode::OK {
                warn!(
                    "Server replied 200 OK for Range request on segment {}",
                    seg.id
                );
                return Err(TenunError::RangeNotSupported);
            }

            if status != StatusCode::PARTIAL_CONTENT {
                return Err(TenunError::Network(format!(
                    "Segment {} failed with HTTP status {}",
                    seg.id, status
                )));
            }

            // Validate Content-Range header
            if let Some(cr) = response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
            {
                let expected_prefix = format!("bytes {}-", seg.start);
                if !cr.starts_with(&expected_prefix) {
                    return Err(TenunError::InvalidRange(format!(
                        "Content-Range '{}' does not match expected prefix '{}'",
                        cr, expected_prefix
                    )));
                }
            }

            let mut write_buf = Vec::with_capacity(WRITE_BUFFER_CAPACITY);
            let mut buf_offset = seg.start;
            let mut seg_downloaded = 0u64;

            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|e| TenunError::Network(e.to_string()))?
            {
                write_buf.extend_from_slice(&chunk);

                if write_buf.len() >= WRITE_BUFFER_CAPACITY {
                    worker_file
                        .write_all_at(&write_buf, buf_offset)
                        .map_err(TenunError::Io)?;
                    buf_offset += write_buf.len() as u64;
                    seg_downloaded += write_buf.len() as u64;
                    counter.store(seg_downloaded, Ordering::Relaxed);
                    write_buf.clear();
                }
            }

            if !write_buf.is_empty() {
                worker_file
                    .write_all_at(&write_buf, buf_offset)
                    .map_err(TenunError::Io)?;
                seg_downloaded += write_buf.len() as u64;
                counter.store(seg_downloaded, Ordering::Relaxed);
                write_buf.clear();
            }

            sync_file(&worker_file).map_err(TenunError::Io)?;
            Ok(())
        });
    }

    // Wait for all workers to complete
    let mut fallback_needed = false;

    while let Some(res) = workers.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(TenunError::RangeNotSupported)) => {
                fallback_needed = true;
                workers.abort_all();
                break;
            }
            Ok(Err(e)) => {
                is_running.store(false, Ordering::Relaxed);
                let _ = progress_join_handle.await;
                return Err(e);
            }
            Err(join_err) => {
                is_running.store(false, Ordering::Relaxed);
                let _ = progress_join_handle.await;
                return Err(TenunError::Platform(format!(
                    "Worker task panicked: {join_err}"
                )));
            }
        }
    }

    is_running.store(false, Ordering::Relaxed);
    let _ = progress_join_handle.await;

    // Drop base file handle to ensure file unlocks on Windows
    drop(base_file);

    if fallback_needed {
        info!("Server returned 200 OK for Range request; switching to single-stream download");
        let _ = std::fs::remove_file(&part_path);
        return download_single(url, dest_dir, opts, event_tx).await;
    }

    atomic_rename_file(&part_path, &final_path)?;

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Finished {
            path: final_path.clone(),
            total_bytes: total_size,
        });
    }

    Ok(final_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_resolve_unique_filename_no_clash() {
        let temp = tempdir().expect("temp dir");
        let path = resolve_unique_filename(temp.path(), "sample.txt");
        assert_eq!(path, temp.path().join("sample.txt"));
    }

    #[test]
    fn test_resolve_unique_filename_with_clashes() {
        let temp = tempdir().expect("temp dir");
        let original = temp.path().join("file.zip");
        std::fs::write(&original, b"").expect("write original");

        let clash1 = resolve_unique_filename(temp.path(), "file.zip");
        assert_eq!(clash1, temp.path().join("file (1).zip"));
        std::fs::write(&clash1, b"").expect("write clash1");

        let clash2 = resolve_unique_filename(temp.path(), "file.zip");
        assert_eq!(clash2, temp.path().join("file (2).zip"));
    }
}
