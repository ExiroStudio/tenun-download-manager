//! Downloading engines: single-stream, multi-segment, and crash-resilient resumable downloads.

use crate::error::{Result, TenunError};
use crate::platform::{atomic_rename_file, sync_file, PositionalWrite};
use crate::probe::{apply_options, build_client, probe, RequestOptions};
use crate::segment::{calculate_segments, DEFAULT_CONNECTIONS, DEFAULT_MIN_SEGMENT_SIZE};
use crate::state::{load_state, save_state_atomic, DownloadState};
use reqwest::header::{self, HeaderValue};
use reqwest::StatusCode;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Buffer size per positional write operation (256 KiB).
pub const WRITE_BUFFER_CAPACITY: usize = 256 * 1024;
/// Periodic checkpoint interval (2 seconds).
pub const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(2);

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
    std::fs::create_dir_all(dest_dir).map_err(TenunError::Io)?;
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

/// Performs a parallel segmented download with atomic state checkpointing (`<filename>.tenun`).
pub async fn download_segmented(
    url: &str,
    dest_dir: &Path,
    opts: &RequestOptions,
    event_tx: Option<UnboundedSender<DownloadEvent>>,
) -> Result<PathBuf> {
    download_segmented_with_cancel(url, dest_dir, opts, event_tx, CancellationToken::new()).await
}

/// Performs a parallel segmented download with an explicit `CancellationToken` for pause/cancel.
pub async fn download_segmented_with_cancel(
    url: &str,
    dest_dir: &Path,
    opts: &RequestOptions,
    event_tx: Option<UnboundedSender<DownloadEvent>>,
    cancel_token: CancellationToken,
) -> Result<PathBuf> {
    std::fs::create_dir_all(dest_dir).map_err(TenunError::Io)?;
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
    let state_path = dest_dir.join(format!("{final_filename}.tenun"));

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

    // Create and save initial state file
    let mut initial_state =
        DownloadState::from_remote_info(&remote_info, &final_filename, &segments, opts);
    save_state_atomic(&state_path, &initial_state)?;

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Started {
            filename: final_filename.clone(),
            total_bytes: Some(total_size),
        });
    }

    // Atomic progress tracking per segment
    let segment_counters: Arc<Vec<Arc<AtomicU64>>> = Arc::new(
        segments
            .iter()
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect(),
    );

    let is_running = Arc::new(AtomicBool::new(true));

    // Periodic checkpoint & progress aggregation task
    let checkpoint_join_handle = {
        let counters = Arc::clone(&segment_counters);
        let tx = event_tx.clone();
        let running = Arc::clone(&is_running);
        let s_path = state_path.clone();
        let mut state = initial_state.clone();

        tokio::spawn(async move {
            let mut last_tick = Instant::now();
            let mut last_checkpoint = Instant::now();
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

                // Checkpoint every CHECKPOINT_INTERVAL
                if last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
                    for (i, c) in counters.iter().enumerate() {
                        state.segments[i].downloaded = c.load(Ordering::Relaxed);
                    }
                    state.updated_at = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let _ = save_state_atomic(&s_path, &state);
                    last_checkpoint = Instant::now();
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
        let token = cancel_token.clone();

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

            let mut write_buf = Vec::with_capacity(WRITE_BUFFER_CAPACITY);
            let mut buf_offset = seg.start;
            let mut seg_downloaded = 0u64;

            while !token.is_cancelled() {
                match response.chunk().await {
                    Ok(Some(chunk)) => {
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
                    Ok(None) => break, // Reached end of segment stream
                    Err(e) => return Err(TenunError::Network(e.to_string())),
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

            // Sync bytes flushed to disk before segment worker finishes or exits on cancel
            sync_file(&worker_file).map_err(TenunError::Io)?;

            if token.is_cancelled() {
                Err(TenunError::DownloadPaused)
            } else {
                Ok(())
            }
        });
    }

    // Wait for all workers to complete or handle pause/error
    let mut fallback_needed = false;
    let mut paused = false;

    while let Some(res) = workers.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(TenunError::DownloadPaused)) => {
                paused = true;
                workers.abort_all();
            }
            Ok(Err(TenunError::RangeNotSupported)) => {
                fallback_needed = true;
                workers.abort_all();
                break;
            }
            Ok(Err(e)) => {
                is_running.store(false, Ordering::Relaxed);
                let _ = checkpoint_join_handle.await;
                return Err(e);
            }
            Err(join_err) => {
                if paused || fallback_needed || join_err.is_cancelled() {
                    continue;
                }
                is_running.store(false, Ordering::Relaxed);
                let _ = checkpoint_join_handle.await;
                return Err(TenunError::Platform(format!(
                    "Worker task panicked: {join_err}"
                )));
            }
        }
    }

    is_running.store(false, Ordering::Relaxed);
    let _ = checkpoint_join_handle.await;

    // Drop base file handle to ensure file unlocks on Windows
    drop(base_file);

    // Save final state before exiting
    for (i, c) in segment_counters.iter().enumerate() {
        initial_state.segments[i].downloaded = c.load(Ordering::Relaxed);
    }
    initial_state.updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    save_state_atomic(&state_path, &initial_state)?;

    if paused {
        info!(
            "Download successfully paused at checkpoint: {}",
            state_path.display()
        );
        return Err(TenunError::DownloadPaused);
    }

    if fallback_needed {
        info!("Server returned 200 OK for Range request; switching to single-stream download");
        let _ = std::fs::remove_file(&part_path);
        let _ = std::fs::remove_file(&state_path);
        return download_single(url, dest_dir, opts, event_tx).await;
    }

    // Finished: rename .part to final and remove sidecar .tenun state file
    atomic_rename_file(&part_path, &final_path)?;
    let _ = std::fs::remove_file(&state_path);

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Finished {
            path: final_path.clone(),
            total_bytes: total_size,
        });
    }

    Ok(final_path)
}

/// Resumes an interrupted or paused download from a `.tenun` sidecar state file.
///
/// Features:
/// - Validates `.part` file existence and size.
/// - Validates remote file using `If-Range` header.
/// - Resumes each incomplete segment from its saved `downloaded` offset.
/// - Refuses silent corruption if remote file changed on the server.
pub async fn resume_download(
    state_path: &Path,
    event_tx: Option<UnboundedSender<DownloadEvent>>,
    cancel_token: CancellationToken,
) -> Result<PathBuf> {
    let mut state = load_state(state_path)?;
    let dest_dir = state_path.parent().unwrap_or_else(|| Path::new("."));

    let part_path = dest_dir.join(format!("{}.part", state.filename));
    let final_path = dest_dir.join(&state.filename);

    if !part_path.exists() {
        return Err(TenunError::CorruptState(format!(
            "Missing part file: {}",
            part_path.display()
        )));
    }

    let actual_part_len = part_path.metadata().map_err(TenunError::Io)?.len();
    if actual_part_len != state.total_bytes {
        return Err(TenunError::CorruptState(format!(
            "Part file length mismatch: {} bytes vs expected {} bytes",
            actual_part_len, state.total_bytes
        )));
    }

    let opts = state.options.to_request_options();
    let client = build_client(&opts)?;

    // Validate remote file with If-Range
    info!(
        "Validating remote resource with If-Range: {}",
        state.final_url
    );
    let mut validate_req = client
        .get(&state.final_url)
        .header(header::RANGE, "bytes=0-0");
    validate_req = apply_options(validate_req, &opts)?;

    if let Some(ref etag) = state.etag {
        validate_req = validate_req.header(header::IF_RANGE, etag);
    } else if let Some(ref lm) = state.last_modified {
        validate_req = validate_req.header(header::IF_RANGE, lm);
    }

    let validate_res = validate_req
        .send()
        .await
        .map_err(|e| TenunError::Network(e.to_string()))?;

    let val_status = validate_res.status();

    if val_status == StatusCode::OK {
        // Server returned 200 OK: the If-Range condition failed, meaning file changed!
        return Err(TenunError::RemoteFileChanged(
            "Server returned 200 OK for If-Range; remote resource was modified".to_string(),
        ));
    }

    if val_status != StatusCode::PARTIAL_CONTENT {
        return Err(TenunError::Network(format!(
            "Server returned error {} during resume validation",
            val_status
        )));
    }

    // Double-check ETag if present
    if let Some(ref old_etag) = state.etag {
        if let Some(new_etag) = validate_res
            .headers()
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
        {
            if new_etag != old_etag {
                return Err(TenunError::RemoteFileChanged(format!(
                    "ETag changed from '{old_etag}' to '{new_etag}'"
                )));
            }
        }
    }

    info!(
        "Remote validator verified. Resuming download from {}/{} bytes",
        state.downloaded_bytes(),
        state.total_bytes
    );

    let base_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&part_path)
        .map_err(TenunError::Io)?;

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Started {
            filename: state.filename.clone(),
            total_bytes: Some(state.total_bytes),
        });
    }

    // Segment counters initialized with already downloaded bytes
    let segment_counters: Arc<Vec<Arc<AtomicU64>>> = Arc::new(
        state
            .segments
            .iter()
            .map(|s| Arc::new(AtomicU64::new(s.downloaded)))
            .collect(),
    );

    let is_running = Arc::new(AtomicBool::new(true));

    // Periodic checkpoint task
    let checkpoint_join_handle = {
        let counters = Arc::clone(&segment_counters);
        let tx = event_tx.clone();
        let running = Arc::clone(&is_running);
        let s_path = state_path.to_path_buf();
        let total_size = state.total_bytes;
        let mut save_state = state.clone();

        tokio::spawn(async move {
            let mut last_tick = Instant::now();
            let mut last_checkpoint = Instant::now();
            let mut last_downloaded = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();

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

                if last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
                    for (i, c) in counters.iter().enumerate() {
                        save_state.segments[i].downloaded = c.load(Ordering::Relaxed);
                    }
                    save_state.updated_at = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let _ = save_state_atomic(&s_path, &save_state);
                    last_checkpoint = Instant::now();
                }
            }
        })
    };

    let mut workers = JoinSet::new();

    for (idx, seg) in state.segments.iter().cloned().enumerate() {
        let total_seg_len = seg.end - seg.start + 1;
        if seg.downloaded >= total_seg_len {
            // Segment already 100% finished
            continue;
        }

        let worker_client = client.clone();
        let worker_file = base_file.try_clone().map_err(TenunError::Io)?;
        let worker_opts = opts.clone();
        let final_url = state.final_url.clone();
        let counter = Arc::clone(&segment_counters[idx]);
        let token = cancel_token.clone();
        let resume_start = seg.start + seg.downloaded;
        let etag_opt = state.etag.clone();

        workers.spawn(async move {
            let range_val = format!("bytes={}-{}", resume_start, seg.end);
            let mut req = worker_client
                .get(&final_url)
                .header(header::RANGE, HeaderValue::from_str(&range_val).unwrap());

            if let Some(ref etag) = etag_opt {
                req = req.header(header::IF_RANGE, HeaderValue::from_str(etag).unwrap());
            }

            req = apply_options(req, &worker_opts)?;

            let mut response = req
                .send()
                .await
                .map_err(|e| TenunError::Network(e.to_string()))?;

            let status = response.status();

            if status != StatusCode::PARTIAL_CONTENT {
                return Err(TenunError::Network(format!(
                    "Segment {} failed to resume with HTTP status {}",
                    seg.id, status
                )));
            }

            let mut write_buf = Vec::with_capacity(WRITE_BUFFER_CAPACITY);
            let mut buf_offset = resume_start;
            let mut seg_downloaded = seg.downloaded;

            while !token.is_cancelled() {
                match response.chunk().await {
                    Ok(Some(chunk)) => {
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
                    Ok(None) => break,
                    Err(e) => return Err(TenunError::Network(e.to_string())),
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

            if token.is_cancelled() {
                Err(TenunError::DownloadPaused)
            } else {
                Ok(())
            }
        });
    }

    let mut paused = false;

    while let Some(res) = workers.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(TenunError::DownloadPaused)) => {
                paused = true;
                workers.abort_all();
            }
            Ok(Err(e)) => {
                is_running.store(false, Ordering::Relaxed);
                let _ = checkpoint_join_handle.await;
                return Err(e);
            }
            Err(join_err) => {
                if paused || join_err.is_cancelled() {
                    continue;
                }
                is_running.store(false, Ordering::Relaxed);
                let _ = checkpoint_join_handle.await;
                return Err(TenunError::Platform(format!(
                    "Resume worker panicked: {join_err}"
                )));
            }
        }
    }

    is_running.store(false, Ordering::Relaxed);
    let _ = checkpoint_join_handle.await;

    drop(base_file);

    for (i, c) in segment_counters.iter().enumerate() {
        state.segments[i].downloaded = c.load(Ordering::Relaxed);
    }
    state.updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    save_state_atomic(state_path, &state)?;

    if paused {
        info!("Download paused at checkpoint: {}", state_path.display());
        return Err(TenunError::DownloadPaused);
    }

    atomic_rename_file(&part_path, &final_path)?;
    let _ = std::fs::remove_file(state_path);

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Finished {
            path: final_path.clone(),
            total_bytes: state.total_bytes,
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
