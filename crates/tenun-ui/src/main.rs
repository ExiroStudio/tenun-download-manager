#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
//! Tenun Desktop UI entrypoint.
//!
//! Provides the primary desktop interface powered by Slint, managing background
//! downloads via the `tenun_core::DownloadManager` actor running on a dedicated Tokio runtime.

use interprocess::local_socket::{
    prelude::*, GenericFilePath, GenericNamespaced, ListenerOptions, Stream,
};
use notify_rust::Notification;
use slint::{ComponentHandle, ModelRc, SharedString, VecModel};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tenun_core::{
    AddDownloadRequest, ChecksumAlgorithm, DownloadManager, DownloadStatus, ExpectedChecksum,
    ManagerEvent, RequestOptions,
};

slint::include_modules!();

const SOCKET_NAME: &str = "tenun-download-manager.sock";

/// In-memory state of an individual download record tracked by the UI.
#[derive(Debug, Clone)]
pub struct DownloadItemState {
    pub id: u64,
    pub uuid: String,
    pub filename: String,
    pub url: String,
    pub dest_dir: String,
    pub total_bytes: Option<u64>,
    pub downloaded_bytes: u64,
    pub speed_bps: f64,
    pub status: DownloadStatus,
    pub error_message: Option<String>,
}

/// Global shared state between Tokio event stream and the Slint UI thread.
pub struct AppState {
    pub downloads: Vec<DownloadItemState>,
    pub active_tab: i32,
    pub dirty: bool,
}

/// Formats a byte size into human-readable units (B, KB, MB, GB).
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Formats download transfer rate into human-readable units per second.
pub fn format_speed(bytes_per_sec: f64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * KB;
    const GB: f64 = 1024.0 * MB;

    if bytes_per_sec >= GB {
        format!("{:.1} GB/s", bytes_per_sec / GB)
    } else if bytes_per_sec >= MB {
        format!("{:.1} MB/s", bytes_per_sec / MB)
    } else if bytes_per_sec >= KB {
        format!("{:.1} KB/s", bytes_per_sec / KB)
    } else {
        format!("{:.0} B/s", bytes_per_sec)
    }
}

/// Formats estimated remaining time into mm:ss or hh:mm:ss.
pub fn format_eta(remaining_bytes: u64, speed_bps: f64) -> String {
    if speed_bps <= 10.0 {
        return "--:--".to_string();
    }
    let seconds = (remaining_bytes as f64 / speed_bps).ceil() as u64;
    if seconds >= 3600 {
        format!(
            "{:02}:{:02}:{:02}",
            seconds / 3600,
            (seconds % 3600) / 60,
            seconds % 60
        )
    } else {
        format!("{:02}:{:02}", seconds / 60, seconds % 60)
    }
}

/// Parses user speed limit strings (e.g. "5 MB/s", "500 KB", "2048").
pub fn parse_speed_limit(input: &str) -> Option<u64> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }
    let lower = input.to_lowercase();
    let (num_str, mult) = if let Some(s) = lower
        .strip_suffix("gb/s")
        .or_else(|| lower.strip_suffix("gb"))
    {
        (s.trim(), 1024 * 1024 * 1024)
    } else if let Some(s) = lower
        .strip_suffix("mb/s")
        .or_else(|| lower.strip_suffix("mb"))
    {
        (s.trim(), 1024 * 1024)
    } else if let Some(s) = lower
        .strip_suffix("kb/s")
        .or_else(|| lower.strip_suffix("kb"))
    {
        (s.trim(), 1024)
    } else if let Some(s) = lower
        .strip_suffix("b/s")
        .or_else(|| lower.strip_suffix("b"))
    {
        (s.trim(), 1)
    } else {
        (lower.as_str(), 1024 * 1024)
    };

    if let Ok(val) = num_str.parse::<f64>() {
        if val > 0.0 {
            Some((val * mult as f64) as u64)
        } else {
            None
        }
    } else {
        None
    }
}

/// Parses checksum input strings into `ExpectedChecksum`.
pub fn parse_checksum_input(input: &str) -> Option<ExpectedChecksum> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }
    if let Some((algo, hex)) = input.split_once(':') {
        let algo = match algo.to_lowercase().as_str() {
            "sha256" | "sha-256" => ChecksumAlgorithm::Sha256,
            "sha1" | "sha-1" => ChecksumAlgorithm::Sha1,
            "md5" => ChecksumAlgorithm::Md5,
            _ => return None,
        };
        Some(ExpectedChecksum::new(algo, hex.trim()))
    } else {
        let hex = input.trim();
        let algo = match hex.len() {
            64 => ChecksumAlgorithm::Sha256,
            40 => ChecksumAlgorithm::Sha1,
            32 => ChecksumAlgorithm::Md5,
            _ => return None,
        };
        Some(ExpectedChecksum::new(algo, hex))
    }
}

/// Maps an internal `DownloadItemState` into Slint's `DownloadItemData`.
pub fn map_item_to_ui(item: &DownloadItemState) -> DownloadItemData {
    let is_downloading = item.status.is_active();
    let is_paused = item.status == DownloadStatus::Paused;
    let is_completed = item.status == DownloadStatus::Completed;
    let is_failed = item.status == DownloadStatus::Failed
        || item.status == DownloadStatus::NeedsRefresh
        || item.status == DownloadStatus::Cancelled;

    let progress_pct = match (item.total_bytes, is_completed) {
        (_, true) => 1.0,
        (Some(total), false) if total > 0 => {
            (item.downloaded_bytes as f32 / total as f32).clamp(0.0, 1.0)
        }
        _ => 0.0,
    };

    let progress_text = match (item.total_bytes, is_completed) {
        (_, true) => "100%".to_string(),
        (Some(total), false) if total > 0 => {
            let pct = (item.downloaded_bytes as f64 / total as f64 * 100.0).clamp(0.0, 100.0);
            format!("{pct:.1}%")
        }
        _ => format_bytes(item.downloaded_bytes),
    };

    let info_text = match item.status {
        DownloadStatus::Downloading => {
            if let Some(total) = item.total_bytes {
                let remaining = total.saturating_sub(item.downloaded_bytes);
                let eta = format_eta(remaining, item.speed_bps);
                format!(
                    "{} / {} · {} · ETA {}",
                    format_bytes(item.downloaded_bytes),
                    format_bytes(total),
                    format_speed(item.speed_bps),
                    eta
                )
            } else {
                format!(
                    "{} · {}",
                    format_bytes(item.downloaded_bytes),
                    format_speed(item.speed_bps)
                )
            }
        }
        DownloadStatus::Completed => {
            format!("{} · Completed", format_bytes(item.downloaded_bytes))
        }
        DownloadStatus::Paused => {
            if let Some(total) = item.total_bytes {
                format!(
                    "{} / {} · Paused",
                    format_bytes(item.downloaded_bytes),
                    format_bytes(total)
                )
            } else {
                format!("{} · Paused", format_bytes(item.downloaded_bytes))
            }
        }
        DownloadStatus::Failed => format!(
            "Failed: {}",
            item.error_message.as_deref().unwrap_or("Unknown error")
        ),
        DownloadStatus::Queued => "Queued".to_string(),
        DownloadStatus::Probing => "Connecting...".to_string(),
        DownloadStatus::Verifying => "Verifying integrity...".to_string(),
        DownloadStatus::Cancelled => "Cancelled".to_string(),
        DownloadStatus::NeedsRefresh => "Download link expired".to_string(),
    };

    DownloadItemData {
        id: item.id as i32,
        filename: SharedString::from(&item.filename),
        url: SharedString::from(&item.url),
        dest_folder: SharedString::from(&item.dest_dir),
        error_msg: SharedString::from(item.error_message.as_deref().unwrap_or("")),
        progress_pct,
        progress_text: SharedString::from(progress_text),
        info_text: SharedString::from(info_text),
        status_str: SharedString::from(item.status.as_str()),
        is_downloading,
        is_paused,
        is_completed,
        is_failed,
    }
}

/// Refreshes UI properties and the item model based on active filters and state.
pub fn refresh_ui_view(ui: &MainWindow, model: &VecModel<DownloadItemData>, state: &mut AppState) {
    let count_all = state.downloads.len() as i32;
    let count_downloading = state
        .downloads
        .iter()
        .filter(|d| d.status.is_active() || d.status == DownloadStatus::Queued)
        .count() as i32;
    let count_completed = state
        .downloads
        .iter()
        .filter(|d| d.status == DownloadStatus::Completed)
        .count() as i32;

    ui.set_count_all(count_all);
    ui.set_count_downloading(count_downloading);
    ui.set_count_completed(count_completed);

    let active_count = state
        .downloads
        .iter()
        .filter(|d| d.status.is_active())
        .count();
    let total_speed: f64 = state
        .downloads
        .iter()
        .filter(|d| d.status.is_active())
        .map(|d| d.speed_bps)
        .sum();
    ui.set_footer_text(SharedString::from(format!(
        "{active_count} active · {}",
        format_speed(total_speed)
    )));

    let filtered_items: Vec<DownloadItemData> = state
        .downloads
        .iter()
        .filter(|d| match state.active_tab {
            1 => d.status.is_active() || d.status == DownloadStatus::Queued,
            2 => d.status == DownloadStatus::Completed,
            _ => true,
        })
        .map(map_item_to_ui)
        .collect();

    model.set_vec(filtered_items);

    if ui.get_show_detail_dialog() || ui.get_show_action_menu() {
        let sel_id = ui.get_selected_id() as u64;
        if let Some(item) = state.downloads.iter().find(|d| d.id == sel_id) {
            ui.set_selected_item(map_item_to_ui(item));
        }
    }

    state.dirty = false;
}

/// Dispatches a system notification when a download finishes.
pub fn send_desktop_notification(filename: &str) {
    if let Err(e) = Notification::new()
        .summary("Download Complete")
        .body(&format!("{filename} has finished downloading."))
        .appname("Tenun")
        .show()
    {
        tracing::debug!("Notification omitted or unavailable: {e}");
    }
}

/// Forwards command-line URL to an existing instance if running.
pub fn try_forward_to_existing_instance(url: Option<&str>) -> bool {
    let name_res = if GenericNamespaced::is_supported() {
        SOCKET_NAME.to_ns_name::<GenericNamespaced>()
    } else {
        format!("/tmp/{SOCKET_NAME}").to_fs_name::<GenericFilePath>()
    };

    let name = match name_res {
        Ok(n) => n,
        Err(_) => return false,
    };

    if let Ok(mut stream) = Stream::connect(name) {
        let msg = url.unwrap_or("");
        let _ = writeln!(stream, "{msg}");
        let _ = stream.flush();
        return true;
    }
    false
}

/// Listens for IPC connections from secondary instances and brings URL to Add dialog.
pub fn start_single_instance_listener(main_window_weak: slint::Weak<MainWindow>) {
    std::thread::spawn(move || {
        let name_res = if GenericNamespaced::is_supported() {
            SOCKET_NAME.to_ns_name::<GenericNamespaced>()
        } else {
            format!("/tmp/{SOCKET_NAME}").to_fs_name::<GenericFilePath>()
        };

        let name = match name_res {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!("Failed to create single instance socket name: {e}");
                return;
            }
        };

        let listener = match ListenerOptions::new()
            .name(name)
            .try_overwrite(true)
            .create_sync()
        {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("Single instance listener failed to bind: {e}");
                return;
            }
        };

        for conn in listener.incoming().flatten() {
            let mut reader = BufReader::new(conn);
            let mut line = String::new();
            if reader.read_line(&mut line).is_ok() {
                let trimmed = line.trim().to_string();
                if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
                    let weak = main_window_weak.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(ui) = weak.upgrade() {
                            ui.invoke_request_add_url(SharedString::from(trimmed));
                        }
                    });
                }
            }
        }
    });
}

fn main() -> Result<(), slint::PlatformError> {
    tenun_core::init_logging("info");

    let args: Vec<String> = std::env::args().collect();
    let cli_url = args
        .get(1)
        .filter(|s| s.starts_with("http://") || s.starts_with("https://"))
        .cloned();

    // Single instance IPC check
    if try_forward_to_existing_instance(cli_url.as_deref()) {
        tracing::info!("Sent URL to active Tenun instance. Exiting secondary process.");
        return Ok(());
    }

    // Initialize UI with software renderer fallback if needed
    let main_window = match MainWindow::new() {
        Ok(win) => win,
        Err(e) => {
            tracing::warn!(
                "Default UI renderer failed ({e}). Attempting software renderer fallback..."
            );
            std::env::set_var("SLINT_BACKEND", "software");
            MainWindow::new()?
        }
    };

    // Initialize Tokio runtime in a dedicated background OS thread
    let (rt_tx, rt_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize background Tokio runtime");
        let handle = rt.handle().clone();
        let _ = rt_tx.send(handle);
        rt.block_on(async {
            // Keep runtime alive indefinitely
            let (_dummy_tx, mut dummy_rx) = tokio::sync::mpsc::channel::<()>(1);
            let _ = dummy_rx.recv().await;
        });
    });

    let rt_handle = rt_rx
        .recv()
        .expect("Failed to receive Tokio runtime handle");

    // Initialize DownloadManager with default database within Tokio runtime context
    let _rt_guard = rt_handle.enter();
    let manager = DownloadManager::init_default().expect("Failed to initialize DownloadManager");

    // Load initial downloads from SQLite
    let initial_records = manager.db().list(None, None).unwrap_or_default();
    let initial_downloads: Vec<DownloadItemState> = initial_records
        .into_iter()
        .map(|rec| DownloadItemState {
            id: rec.id,
            uuid: rec.uuid,
            filename: rec.filename,
            url: rec.url,
            dest_dir: rec.dest_dir,
            total_bytes: rec.total_bytes,
            downloaded_bytes: rec.downloaded_bytes,
            speed_bps: 0.0,
            status: rec.status,
            error_message: rec.error_message,
        })
        .collect();

    let app_state = Arc::new(Mutex::new(AppState {
        downloads: initial_downloads,
        active_tab: 0,
        dirty: true,
    }));

    // Setup Download Items Model
    let downloads_model = std::rc::Rc::new(VecModel::<DownloadItemData>::default());
    main_window.set_downloads(ModelRc::from(downloads_model.clone()));

    // Set default folders & settings
    let default_folder = tenun_core::AppDirectories::detect()
        .map(|d| d.download_dir().to_string_lossy().to_string())
        .unwrap_or_else(|_| "./downloads".to_string());
    main_window.set_add_folder(SharedString::from(&default_folder));
    main_window.set_settings_folder(SharedString::from(&default_folder));
    main_window.set_settings_concurrent(SharedString::from("3"));

    // Event listener: Subscribe to DownloadManager broadcast events
    {
        let state_arc = app_state.clone();
        let db = manager.db().clone();
        let mut event_rx = manager.subscribe();

        rt_handle.spawn(async move {
            while let Ok(event) = event_rx.recv().await {
                let mut state = state_arc.lock().unwrap();
                match event {
                    ManagerEvent::Added {
                        id,
                        uuid,
                        filename,
                        url,
                    } => {
                        let dest_dir = db
                            .get(id)
                            .ok()
                            .flatten()
                            .map(|r| r.dest_dir)
                            .unwrap_or_default();

                        if !state.downloads.iter().any(|d| d.id == id) {
                            state.downloads.insert(
                                0,
                                DownloadItemState {
                                    id,
                                    uuid,
                                    filename,
                                    url,
                                    dest_dir,
                                    total_bytes: None,
                                    downloaded_bytes: 0,
                                    speed_bps: 0.0,
                                    status: DownloadStatus::Queued,
                                    error_message: None,
                                },
                            );
                        }
                        state.dirty = true;
                    }
                    ManagerEvent::StatusChanged { id, status } => {
                        if let Some(item) = state.downloads.iter_mut().find(|d| d.id == id) {
                            item.status = status;
                            if !status.is_active() {
                                item.speed_bps = 0.0;
                            }
                            state.dirty = true;
                        }
                    }
                    ManagerEvent::Progress {
                        id,
                        downloaded_bytes,
                        total_bytes,
                        speed_bytes_per_sec,
                    } => {
                        if let Some(item) = state.downloads.iter_mut().find(|d| d.id == id) {
                            item.downloaded_bytes = downloaded_bytes;
                            if total_bytes.is_some() {
                                item.total_bytes = total_bytes;
                            }
                            item.speed_bps = speed_bytes_per_sec;
                            if item.status == DownloadStatus::Queued
                                || item.status == DownloadStatus::Probing
                            {
                                item.status = DownloadStatus::Downloading;
                            }
                            state.dirty = true;
                        }
                    }
                    ManagerEvent::Finished {
                        id,
                        path,
                        total_bytes,
                    } => {
                        let filename = path
                            .file_name()
                            .map(|f| f.to_string_lossy().to_string())
                            .unwrap_or_default();
                        if let Some(item) = state.downloads.iter_mut().find(|d| d.id == id) {
                            item.downloaded_bytes = total_bytes;
                            item.total_bytes = Some(total_bytes);
                            item.status = DownloadStatus::Completed;
                            item.speed_bps = 0.0;
                            state.dirty = true;
                        }
                        send_desktop_notification(&filename);
                    }
                    ManagerEvent::Failed { id, error } => {
                        if let Some(item) = state.downloads.iter_mut().find(|d| d.id == id) {
                            item.status = DownloadStatus::Failed;
                            item.error_message = Some(error);
                            item.speed_bps = 0.0;
                            state.dirty = true;
                        }
                    }
                    ManagerEvent::Paused { id } => {
                        if let Some(item) = state.downloads.iter_mut().find(|d| d.id == id) {
                            item.status = DownloadStatus::Paused;
                            item.speed_bps = 0.0;
                            state.dirty = true;
                        }
                    }
                    ManagerEvent::Cancelled { id } => {
                        if let Some(item) = state.downloads.iter_mut().find(|d| d.id == id) {
                            item.status = DownloadStatus::Cancelled;
                            item.speed_bps = 0.0;
                            state.dirty = true;
                        }
                    }
                    ManagerEvent::Removed { id } => {
                        state.downloads.retain(|d| d.id != id);
                        state.dirty = true;
                    }
                }
            }
        });
    }

    // 10 Hz UI throttled refresh timer (100 ms)
    let refresh_timer = slint::Timer::default();
    refresh_timer.start(slint::TimerMode::Repeated, Duration::from_millis(100), {
        let main_window_weak = main_window.as_weak();
        let model = downloads_model.clone();
        let state_arc = app_state.clone();
        move || {
            let mut state = state_arc.lock().unwrap();
            if state.dirty {
                if let Some(ui) = main_window_weak.upgrade() {
                    refresh_ui_view(&ui, &model, &mut state);
                }
            }
        }
    });

    // Initial view rendering
    {
        let mut state = app_state.lock().unwrap();
        refresh_ui_view(&main_window, &downloads_model, &mut state);
    }

    // Clipboard suggestion polling timer (1.5s)
    let clipboard_timer = slint::Timer::default();
    clipboard_timer.start(slint::TimerMode::Repeated, Duration::from_millis(1500), {
        let main_window_weak = main_window.as_weak();
        let mut last_clipboard = String::new();
        move || {
            if let Ok(mut clip) = arboard::Clipboard::new() {
                if let Ok(text) = clip.get_text() {
                    let text = text.trim();
                    if (text.starts_with("http://") || text.starts_with("https://"))
                        && text != last_clipboard
                    {
                        last_clipboard = text.to_string();
                        if let Some(ui) = main_window_weak.upgrade() {
                            ui.set_clipboard_url(SharedString::from(text));
                        }
                    }
                }
            }
        }
    });

    // Single instance IPC server
    start_single_instance_listener(main_window.as_weak());

    // UI Callbacks Hookup

    // 1. Request Add URL (from AddBar or Suggestion Chip)
    {
        let main_window_weak = main_window.as_weak();
        let rt = rt_handle.clone();
        main_window.on_request_add_url(move |url| {
            let url_str = url.to_string().trim().to_string();
            if url_str.is_empty() {
                return;
            }

            let filename_fallback = url_str
                .split('?')
                .next()
                .unwrap_or(&url_str)
                .rsplit('/')
                .next()
                .filter(|s| !s.is_empty())
                .unwrap_or("download")
                .to_string();

            if let Some(ui) = main_window_weak.upgrade() {
                ui.set_add_url(SharedString::from(&url_str));
                ui.set_add_filename(SharedString::from(&filename_fallback));
                ui.set_add_info(SharedString::from("Connecting..."));
                ui.set_show_add_dialog(true);
                ui.set_clipboard_url(SharedString::from(""));
            }

            let weak = main_window_weak.clone();
            rt.spawn(async move {
                let opts = RequestOptions::default();
                let client = tenun_core::build_client(&opts).unwrap_or_default();
                match tenun_core::probe(&client, &url_str, &opts).await {
                    Ok(info) => {
                        let filename = info.filename.clone();
                        let size_str = info
                            .total_bytes
                            .map(format_bytes)
                            .unwrap_or_else(|| "Unknown size".to_string());
                        let resume_str = if info.range_supported {
                            "Resumable"
                        } else {
                            "Non-resumable"
                        };
                        let info_display = format!("{size_str} · {resume_str}");
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(ui) = weak.upgrade() {
                                ui.set_add_filename(SharedString::from(filename));
                                ui.set_add_info(SharedString::from(info_display));
                            }
                        });
                    }
                    Err(e) => {
                        let err_msg = format!("Probe error: {e}");
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(ui) = weak.upgrade() {
                                ui.set_add_info(SharedString::from(err_msg));
                            }
                        });
                    }
                }
            });
        });
    }

    // 2. Probe URL (when user edits URL inside AddDialog)
    {
        let main_window_weak = main_window.as_weak();
        let rt = rt_handle.clone();
        main_window.on_probe_url(move |url| {
            let url_str = url.to_string().trim().to_string();
            if !url_str.starts_with("http://") && !url_str.starts_with("https://") {
                return;
            }

            if let Some(ui) = main_window_weak.upgrade() {
                ui.set_add_info(SharedString::from("Connecting..."));
            }

            let weak = main_window_weak.clone();
            rt.spawn(async move {
                let opts = RequestOptions::default();
                let client = tenun_core::build_client(&opts).unwrap_or_default();
                if let Ok(info) = tenun_core::probe(&client, &url_str, &opts).await {
                    let filename = info.filename.clone();
                    let size_str = info
                        .total_bytes
                        .map(format_bytes)
                        .unwrap_or_else(|| "Unknown size".to_string());
                    let resume_str = if info.range_supported {
                        "Resumable"
                    } else {
                        "Non-resumable"
                    };
                    let info_display = format!("{size_str} · {resume_str}");
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(ui) = weak.upgrade() {
                            ui.set_add_filename(SharedString::from(filename));
                            ui.set_add_info(SharedString::from(info_display));
                        }
                    });
                }
            });
        });
    }

    // 3. Submit Add Download
    {
        let mgr = manager.clone();
        let rt = rt_handle.clone();
        main_window.on_submit_add_download(
            move |url, folder, custom_name, conn_str, speed_str, checksum_str| {
                let url_str = url.to_string().trim().to_string();
                if url_str.is_empty() {
                    return;
                }

                let mut req = AddDownloadRequest::new(url_str);
                let folder_str = folder.to_string().trim().to_string();
                if !folder_str.is_empty() {
                    req.output_dir = Some(PathBuf::from(folder_str));
                }

                let name_str = custom_name.to_string().trim().to_string();
                if !name_str.is_empty() {
                    req.filename = Some(name_str);
                }

                if let Ok(conns) = conn_str.trim().parse::<usize>() {
                    req.options.connections = Some(conns);
                }

                if let Some(limit) = parse_speed_limit(&speed_str) {
                    req.options.limit_rate = Some(limit);
                }

                let check_str = checksum_str.to_string().trim().to_string();
                if !check_str.is_empty() {
                    if let Some(expected) = parse_checksum_input(&check_str) {
                        req.options.expected_checksum = Some(expected);
                    }
                }

                let mgr_c = mgr.clone();
                rt.spawn(async move {
                    if let Err(e) = mgr_c.add(req).await {
                        tracing::error!("Failed to submit download: {e}");
                    }
                });
            },
        );
    }

    // 4. Pause Download
    {
        let mgr = manager.clone();
        let rt = rt_handle.clone();
        main_window.on_pause_download(move |id| {
            let mgr_c = mgr.clone();
            rt.spawn(async move {
                let _ = mgr_c.pause(id as u64).await;
            });
        });
    }

    // 5. Resume Download
    {
        let mgr = manager.clone();
        let rt = rt_handle.clone();
        main_window.on_resume_download(move |id| {
            let mgr_c = mgr.clone();
            rt.spawn(async move {
                let _ = mgr_c.resume(id as u64).await;
            });
        });
    }

    // 6. Retry Download
    {
        let mgr = manager.clone();
        let rt = rt_handle.clone();
        main_window.on_retry_download(move |id| {
            let mgr_c = mgr.clone();
            rt.spawn(async move {
                let _ = mgr_c.resume(id as u64).await;
            });
        });
    }

    // 7. Delete Download
    {
        let mgr = manager.clone();
        let rt = rt_handle.clone();
        main_window.on_delete_download(move |id, del_file| {
            let mgr_c = mgr.clone();
            rt.spawn(async move {
                let _ = mgr_c.remove(id as u64, del_file).await;
            });
        });
    }

    // 8. Open File
    {
        let db = manager.db().clone();
        let rt = rt_handle.clone();
        main_window.on_open_download(move |id| {
            let db_c = db.clone();
            rt.spawn(async move {
                if let Ok(Some(rec)) = db_c.get(id as u64) {
                    let path = PathBuf::from(&rec.dest_dir).join(&rec.filename);
                    if let Err(e) = open::that(&path) {
                        tracing::warn!("Failed to open downloaded file {}: {e}", path.display());
                    }
                }
            });
        });
    }

    // 9. Open Destination Folder
    {
        let db = manager.db().clone();
        let rt = rt_handle.clone();
        main_window.on_open_folder(move |id| {
            let db_c = db.clone();
            rt.spawn(async move {
                if let Ok(Some(rec)) = db_c.get(id as u64) {
                    let dest = PathBuf::from(&rec.dest_dir);
                    if let Err(e) = open::that(&dest) {
                        tracing::warn!("Failed to open destination folder {}: {e}", dest.display());
                    }
                }
            });
        });
    }

    // 10. Copy Link
    main_window.on_copy_link(move |url| {
        let text = url.to_string();
        if let Ok(mut clip) = arboard::Clipboard::new() {
            let _ = clip.set_text(text);
        }
    });

    // 11. Folder Picker (Add Dialog)
    {
        let weak = main_window.as_weak();
        main_window.on_pick_add_folder(move || {
            if let Some(folder) = rfd::FileDialog::new().pick_folder() {
                let folder_str = folder.to_string_lossy().to_string();
                if let Some(ui) = weak.upgrade() {
                    ui.set_add_folder(SharedString::from(folder_str));
                }
            }
        });
    }

    // 12. Folder Picker (Settings Dialog)
    {
        let weak = main_window.as_weak();
        main_window.on_pick_settings_folder(move || {
            if let Some(folder) = rfd::FileDialog::new().pick_folder() {
                let folder_str = folder.to_string_lossy().to_string();
                if let Some(ui) = weak.upgrade() {
                    ui.set_settings_folder(SharedString::from(folder_str));
                }
            }
        });
    }

    // 13. Save Settings
    {
        let mgr = manager.clone();
        let rt = rt_handle.clone();
        let weak = main_window.as_weak();
        main_window.on_save_settings(move |folder, concurrent_str, speed_str| {
            let folder_str = folder.to_string();
            if !folder_str.is_empty() {
                if let Some(ui) = weak.upgrade() {
                    ui.set_add_folder(SharedString::from(&folder_str));
                    ui.set_settings_folder(SharedString::from(&folder_str));
                }
            }

            if let Ok(concur) = concurrent_str.trim().parse::<usize>() {
                let mgr_c = mgr.clone();
                rt.spawn(async move {
                    let _ = mgr_c.set_max_concurrent(concur).await;
                });
            }

            let speed_opt = parse_speed_limit(&speed_str);
            let mgr_c = mgr.clone();
            rt.spawn(async move {
                let _ = mgr_c.set_global_speed_limit(speed_opt).await;
            });
        });
    }

    // 14. Tab Changed
    {
        let state_arc = app_state.clone();
        let model = downloads_model.clone();
        let weak = main_window.as_weak();
        main_window.on_tab_changed(move |idx| {
            let mut state = state_arc.lock().unwrap();
            state.active_tab = idx;
            state.dirty = true;
            if let Some(ui) = weak.upgrade() {
                refresh_ui_view(&ui, &model, &mut state);
            }
        });
    }

    // If initial CLI URL was provided to primary instance, request add immediately
    if let Some(url) = cli_url {
        main_window.invoke_request_add_url(SharedString::from(url));
    }

    // In non-interactive or headless environments, avoid blocking window loop
    if std::env::var("CI").is_err() && std::env::var("TENUN_HEADLESS").is_err() {
        main_window.run()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1024 * 1024 * 5), "5.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024 * 2), "2.0 GB");
    }

    #[test]
    fn test_format_speed() {
        assert_eq!(format_speed(0.0), "0 B/s");
        assert_eq!(format_speed(1024.0), "1.0 KB/s");
        assert_eq!(format_speed(1024.0 * 1024.0 * 3.5), "3.5 MB/s");
        assert_eq!(format_speed(1024.0 * 1024.0 * 1024.0 * 1.2), "1.2 GB/s");
    }

    #[test]
    fn test_format_eta() {
        assert_eq!(format_eta(1000, 0.0), "--:--");
        assert_eq!(format_eta(1000, 5.0), "--:--");
        assert_eq!(format_eta(1000, 100.0), "00:10");
        assert_eq!(format_eta(3600, 1.0), "--:--");
        assert_eq!(format_eta(3665 * 100, 100.0), "01:01:05");
    }

    #[test]
    fn test_parse_speed_limit() {
        assert_eq!(parse_speed_limit(""), None);
        assert_eq!(parse_speed_limit("   "), None);
        assert_eq!(parse_speed_limit("5 MB/s"), Some(5 * 1024 * 1024));
        assert_eq!(parse_speed_limit("500 KB"), Some(500 * 1024));
        assert_eq!(parse_speed_limit("1 GB/s"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_speed_limit("2048 B"), Some(2048));
    }

    #[test]
    fn test_parse_checksum_input() {
        assert!(parse_checksum_input("").is_none());
        let res = parse_checksum_input("sha256:abcdef").unwrap();
        assert_eq!(res.algorithm, ChecksumAlgorithm::Sha256);
        assert_eq!(res.hex_digest, "abcdef");

        let sha256_full = "a".repeat(64);
        let res2 = parse_checksum_input(&sha256_full).unwrap();
        assert_eq!(res2.algorithm, ChecksumAlgorithm::Sha256);

        let md5_full = "b".repeat(32);
        let res3 = parse_checksum_input(&md5_full).unwrap();
        assert_eq!(res3.algorithm, ChecksumAlgorithm::Md5);
    }

    #[test]
    fn test_map_item_to_ui() {
        let item = DownloadItemState {
            id: 1,
            uuid: "test-uuid".to_string(),
            filename: "file.iso".to_string(),
            url: "https://example.com/file.iso".to_string(),
            dest_dir: "/tmp".to_string(),
            total_bytes: Some(1000),
            downloaded_bytes: 500,
            speed_bps: 100.0,
            status: DownloadStatus::Downloading,
            error_message: None,
        };

        let ui_data = map_item_to_ui(&item);
        assert_eq!(ui_data.id, 1);
        assert_eq!(ui_data.filename, "file.iso");
        assert_eq!(ui_data.progress_pct, 0.5);
        assert_eq!(ui_data.progress_text, "50.0%");
        assert!(ui_data.is_downloading);
        assert!(!ui_data.is_completed);
    }

    #[test]
    fn test_ui_component_initialization() {
        let win = MainWindow::new().unwrap();
        win.on_request_add_url(|_| {});
        win.invoke_request_add_url("https://example.com".into());
        let model = std::rc::Rc::new(VecModel::<DownloadItemData>::default());
        model.set_vec(vec![]);
    }
}
