//! Downloading engines: single-stream, multi-segment, and crash-resilient resumable downloads.

use crate::error::{Result, TenunError};
use crate::limiter::TokenBucketLimiter;
use crate::platform::{atomic_rename_file, sync_file, PositionalWrite};
use crate::probe::{apply_options, build_client, probe, RequestOptions};
use crate::retry::{classify_reqwest_error, classify_response, ErrorKind, RetryPolicy};
use crate::segment::{
    calculate_segments, DynamicSegment, Segment, SegmentManager, DEFAULT_CONNECTIONS,
    DEFAULT_MIN_SEGMENT_SIZE,
};
use crate::state::{load_state, save_state_atomic, DownloadState, SegmentState};
use reqwest::header::{self, HeaderValue};
use reqwest::StatusCode;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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

    let limiter = opts.limit_rate.map(TokenBucketLimiter::new);
    let policy = opts.retry_policy.clone().unwrap_or_default();

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

        loop {
            let chunk_res = tokio::time::timeout(policy.stall_timeout, response.chunk()).await;
            match chunk_res {
                Ok(Ok(Some(chunk))) => {
                    if let Some(ref lim) = limiter {
                        lim.acquire(chunk.len()).await;
                    }

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
                Ok(Ok(None)) => break,
                Ok(Err(e)) => return Err(TenunError::Network(e.to_string())),
                Err(_) => {
                    return Err(TenunError::Network(format!(
                        "Download stalled: no data received for {:?}",
                        policy.stall_timeout
                    )));
                }
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
    drop(base_file);

    let initial_state =
        DownloadState::from_remote_info(&remote_info, &final_filename, &segments, opts);
    save_state_atomic(&state_path, &initial_state)?;

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Started {
            filename: final_filename.clone(),
            total_bytes: Some(total_size),
        });
    }

    let segment_manager = SegmentManager::new(&segments);

    download_multi_segment(
        part_path,
        state_path,
        final_path,
        total_size,
        remote_info.final_url,
        remote_info.etag,
        segment_manager,
        opts.clone(),
        client,
        initial_state,
        event_tx,
        cancel_token,
        true,
        url,
        dest_dir,
    )
    .await
}

/// Resumes an interrupted or paused download from a `.tenun` sidecar state file.
pub async fn resume_download(
    state_path: &Path,
    event_tx: Option<UnboundedSender<DownloadEvent>>,
    cancel_token: CancellationToken,
) -> Result<PathBuf> {
    resume_download_with_options(state_path, event_tx, cancel_token, None).await
}

/// Resumes an interrupted or paused download with optional request option overrides.
pub async fn resume_download_with_options(
    state_path: &Path,
    event_tx: Option<UnboundedSender<DownloadEvent>>,
    cancel_token: CancellationToken,
    opts_override: Option<&RequestOptions>,
) -> Result<PathBuf> {
    let state = load_state(state_path)?;
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

    let mut opts = state.options.to_request_options();
    if let Some(over) = opts_override {
        if over.limit_rate.is_some() {
            opts.limit_rate = over.limit_rate;
        }
        if over.retry_policy.is_some() {
            opts.retry_policy = over.retry_policy.clone();
        }
        if over.user_agent.is_some() {
            opts.user_agent = over.user_agent.clone();
        }
    }

    let client = build_client(&opts)?;

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

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Started {
            filename: state.filename.clone(),
            total_bytes: Some(state.total_bytes),
        });
    }

    let initial_segments: Vec<Segment> = state.segments.iter().map(Segment::from).collect();
    let segment_manager = SegmentManager::new(&initial_segments);

    download_multi_segment(
        part_path,
        state_path.to_path_buf(),
        final_path,
        state.total_bytes,
        state.final_url.clone(),
        state.etag.clone(),
        segment_manager,
        opts,
        client,
        state,
        event_tx,
        cancel_token,
        false,
        "",
        dest_dir,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn download_multi_segment(
    part_path: PathBuf,
    state_path: PathBuf,
    final_path: PathBuf,
    total_size: u64,
    final_url: String,
    etag: Option<String>,
    segment_manager: SegmentManager,
    opts: RequestOptions,
    client: reqwest::Client,
    mut initial_state: DownloadState,
    event_tx: Option<UnboundedSender<DownloadEvent>>,
    cancel_token: CancellationToken,
    fallback_on_200: bool,
    original_url: &str,
    dest_dir: &Path,
) -> Result<PathBuf> {
    let base_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&part_path)
        .map_err(TenunError::Io)?;

    let is_running = Arc::new(AtomicBool::new(true));
    let limiter = opts
        .limit_rate
        .map(|r| Arc::new(TokenBucketLimiter::new(r)));
    let policy = opts.retry_policy.clone().unwrap_or_default();
    let min_split = opts.min_segment_size.unwrap_or(DEFAULT_MIN_SEGMENT_SIZE);

    // Periodic checkpoint task
    let checkpoint_join_handle = {
        let seg_mgr = segment_manager.clone();
        let tx = event_tx.clone();
        let running = Arc::clone(&is_running);
        let s_path = state_path.clone();
        let mut save_state = initial_state.clone();

        tokio::spawn(async move {
            let mut last_tick = Instant::now();
            let mut last_checkpoint = Instant::now();
            let mut last_downloaded = seg_mgr
                .segments()
                .iter()
                .map(|s| s.current_downloaded())
                .sum::<u64>();

            while running.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(200)).await;

                let downloaded: u64 = seg_mgr
                    .segments()
                    .iter()
                    .map(|s| s.current_downloaded())
                    .sum();
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
                    let current_segments = seg_mgr.snapshot();
                    save_state.segments = current_segments
                        .into_iter()
                        .map(SegmentState::from)
                        .collect();
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

    for dyn_seg in segment_manager.segments() {
        let worker_client = client.clone();
        let worker_file = base_file.try_clone().map_err(TenunError::Io)?;
        let worker_opts = opts.clone();
        let f_url = final_url.clone();
        let token = cancel_token.clone();
        let seg_mgr = segment_manager.clone();
        let w_limiter = limiter.clone();
        let w_policy = policy.clone();
        let w_etag = etag.clone();

        workers.spawn(async move {
            run_worker(
                dyn_seg,
                worker_file,
                worker_client,
                worker_opts,
                f_url,
                token,
                seg_mgr,
                w_limiter,
                w_policy,
                min_split,
                w_etag,
            )
            .await
        });
    }

    let mut paused = false;
    let mut fallback_needed = false;

    while let Some(res) = workers.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(TenunError::DownloadPaused)) => {
                paused = true;
                workers.abort_all();
            }
            Ok(Err(TenunError::RangeNotSupported)) if fallback_on_200 => {
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
                    "Download worker panicked: {join_err}"
                )));
            }
        }
    }

    is_running.store(false, Ordering::Relaxed);
    let _ = checkpoint_join_handle.await;

    drop(base_file);

    // Save final state
    let final_segments = segment_manager.snapshot();
    initial_state.segments = final_segments.into_iter().map(SegmentState::from).collect();
    initial_state.updated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    save_state_atomic(&state_path, &initial_state)?;

    if paused {
        info!("Download paused at checkpoint: {}", state_path.display());
        return Err(TenunError::DownloadPaused);
    }

    if fallback_needed {
        info!("Server returned 200 OK for Range request; switching to single-stream download");
        let _ = std::fs::remove_file(&part_path);
        let _ = std::fs::remove_file(&state_path);
        return download_single(original_url, dest_dir, &opts, event_tx).await;
    }

    // Atomic rename part to final
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

#[allow(clippy::too_many_arguments)]
async fn run_worker(
    mut cur_seg: Arc<DynamicSegment>,
    worker_file: File,
    client: reqwest::Client,
    opts: RequestOptions,
    final_url: String,
    token: CancellationToken,
    segment_manager: SegmentManager,
    limiter: Option<Arc<TokenBucketLimiter>>,
    policy: RetryPolicy,
    min_split_size: u64,
    etag: Option<String>,
) -> Result<()> {
    'worker_loop: loop {
        if token.is_cancelled() {
            return Err(TenunError::DownloadPaused);
        }

        // If current segment is already finished, attempt to steal work
        if cur_seg.remaining() == 0 {
            if let Some(stolen) = segment_manager.find_and_split_largest(min_split_size) {
                cur_seg = stolen;
                continue 'worker_loop;
            } else {
                return Ok(());
            }
        }

        let mut attempt = 0;

        'segment_attempt: loop {
            if token.is_cancelled() {
                return Err(TenunError::DownloadPaused);
            }

            if cur_seg.remaining() == 0 {
                break 'segment_attempt;
            }

            attempt += 1;
            let start_offset = cur_seg.current_offset();
            let end_offset = cur_seg.current_end();

            if start_offset > end_offset {
                break 'segment_attempt;
            }

            let range_val = format!("bytes={start_offset}-{end_offset}");
            let mut req = client
                .get(&final_url)
                .header(header::RANGE, HeaderValue::from_str(&range_val).unwrap());

            if let Some(ref et) = etag {
                if let Ok(hv) = HeaderValue::from_str(et) {
                    req = req.header(header::IF_RANGE, hv);
                }
            }

            let req = apply_options(req, &opts)?;

            let res_result = req.send().await;
            let mut response = match res_result {
                Ok(res) => res,
                Err(err) => {
                    let kind = classify_reqwest_error(&err);
                    match kind {
                        ErrorKind::Retryable { retry_after } => {
                            if attempt > policy.max_retries {
                                return Err(TenunError::Network(format!(
                                    "Segment {} exceeded max retries: {err}",
                                    cur_seg.id
                                )));
                            }
                            let delay = policy.compute_delay(attempt, retry_after);
                            tokio::time::sleep(delay).await;
                            continue 'segment_attempt;
                        }
                        ErrorKind::NeedsRefresh => {
                            return Err(TenunError::NeedsRefresh(err.to_string()));
                        }
                        ErrorKind::Fatal => {
                            return Err(TenunError::Network(err.to_string()));
                        }
                    }
                }
            };

            let status = response.status();
            if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                let kind = classify_response(status, response.headers());
                if let ErrorKind::Retryable { retry_after } = kind {
                    if attempt > policy.max_retries {
                        return Err(TenunError::Network(format!(
                            "Segment {} exceeded max retries on HTTP status {status}",
                            cur_seg.id
                        )));
                    }
                    let delay = policy.compute_delay(attempt, retry_after);
                    tokio::time::sleep(delay).await;
                    continue 'segment_attempt;
                }
            }

            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                return Err(TenunError::NeedsRefresh(format!("HTTP status {status}")));
            }

            if status == StatusCode::OK {
                warn!(
                    "Server replied 200 OK for Range request on segment {}",
                    cur_seg.id
                );
                return Err(TenunError::RangeNotSupported);
            }

            if status != StatusCode::PARTIAL_CONTENT {
                return Err(TenunError::Network(format!(
                    "Segment {} failed with HTTP status {status}",
                    cur_seg.id
                )));
            }

            // Stream chunks
            'stream_loop: loop {
                if token.is_cancelled() {
                    return Err(TenunError::DownloadPaused);
                }

                // Stall detection
                let chunk_result =
                    tokio::time::timeout(policy.stall_timeout, response.chunk()).await;

                let chunk = match chunk_result {
                    Ok(Ok(Some(c))) => c,
                    Ok(Ok(None)) => {
                        // End of response body
                        break 'stream_loop;
                    }
                    Ok(Err(err)) => {
                        warn!("Network chunk error on segment {}: {err}", cur_seg.id);
                        let kind = classify_reqwest_error(&err);
                        match kind {
                            ErrorKind::Retryable { retry_after } => {
                                if attempt > policy.max_retries {
                                    return Err(TenunError::Network(format!(
                                        "Segment {} failed chunk stream: {err}",
                                        cur_seg.id
                                    )));
                                }
                                let delay = policy.compute_delay(attempt, retry_after);
                                tokio::time::sleep(delay).await;
                                continue 'segment_attempt;
                            }
                            ErrorKind::NeedsRefresh => {
                                return Err(TenunError::NeedsRefresh(err.to_string()));
                            }
                            ErrorKind::Fatal => {
                                return Err(TenunError::Network(err.to_string()));
                            }
                        }
                    }
                    Err(_timeout) => {
                        warn!(
                            "Stall detected on segment {} (no data for {:?})",
                            cur_seg.id, policy.stall_timeout
                        );
                        if attempt > policy.max_retries {
                            return Err(TenunError::Network(format!(
                                "Segment {} stalled repeatedly",
                                cur_seg.id
                            )));
                        }
                        let delay = policy.compute_delay(attempt, None);
                        tokio::time::sleep(delay).await;
                        continue 'segment_attempt;
                    }
                };

                // Rate limiter throttle
                if let Some(ref lim) = limiter {
                    lim.acquire(chunk.len()).await;
                }

                // Write chunk
                let (write_offset, to_write, is_end) = cur_seg.reserve_chunk(chunk.len());
                if to_write > 0 {
                    if let Err(io_err) = worker_file.write_all_at(&chunk[..to_write], write_offset)
                    {
                        cur_seg.rollback_chunk(to_write);
                        return Err(TenunError::Io(io_err));
                    }
                    cur_seg.commit_chunk(to_write);
                }

                if is_end {
                    break 'segment_attempt;
                }
            }

            if cur_seg.remaining() == 0 {
                break 'segment_attempt;
            }
        }

        sync_file(&worker_file).map_err(TenunError::Io)?;

        // Try to steal work
        if let Some(stolen) = segment_manager.find_and_split_largest(min_split_size) {
            cur_seg = stolen;
        } else {
            return Ok(());
        }
    }
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
