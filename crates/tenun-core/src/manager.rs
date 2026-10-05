//! Central actor-based DownloadManager managing parallel concurrency, queue priority,
//! persistence via SQLite, and broadcast event streaming.

use crate::category::FileCategory;
use crate::config::AppDirectories;
use crate::db::{Database, DownloadRecord, DownloadStatus, NewDownloadRecord};
use crate::download::{
    download_segmented_with_cancel, resume_download_with_options, DownloadEvent,
};
use crate::error::{Result, TenunError};
use crate::limiter::TokenBucketLimiter;
use crate::probe::RequestOptions;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

/// Capacity of the global broadcast channel for download manager events.
const EVENT_BROADCAST_CAPACITY: usize = 512;

/// Events broadcast by the `DownloadManager` to UI, CLI, and observers.
#[derive(Debug, Clone)]
pub enum ManagerEvent {
    /// New download registered in manager.
    Added {
        id: u64,
        uuid: String,
        filename: String,
        url: String,
    },
    /// Status transition for a download item.
    StatusChanged { id: u64, status: DownloadStatus },
    /// Real-time progress update.
    Progress {
        id: u64,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
        speed_bytes_per_sec: f64,
    },
    /// Download successfully completed and verified.
    Finished {
        id: u64,
        path: PathBuf,
        total_bytes: u64,
    },
    /// Download failed with an error message.
    Failed { id: u64, error: String },
    /// Download paused cleanly.
    Paused { id: u64 },
    /// Download cancelled.
    Cancelled { id: u64 },
    /// Download record removed from manager.
    Removed { id: u64 },
}

/// Request parameters for adding a new download.
#[derive(Debug, Clone)]
pub struct AddDownloadRequest {
    /// Target remote URL.
    pub url: String,
    /// Destination directory (defaults to configured download dir).
    pub output_dir: Option<PathBuf>,
    /// Optional custom output filename.
    pub filename: Option<String>,
    /// HTTP request options (headers, limits, checksum, etc.).
    pub options: RequestOptions,
    /// Priority in queue (higher value runs earlier, default: 0).
    pub priority: i32,
    /// Optional file category override.
    pub category: Option<FileCategory>,
    /// Whether to start downloading immediately if slot is available (default: true).
    pub start_now: bool,
}

impl AddDownloadRequest {
    /// Creates a basic request with default options.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            output_dir: None,
            filename: None,
            options: RequestOptions::default(),
            priority: 0,
            category: None,
            start_now: true,
        }
    }
}

/// Configuration settings for `DownloadManager`.
#[derive(Debug, Clone)]
pub struct DownloadManagerConfig {
    /// Maximum concurrent downloads (default: 3).
    pub max_concurrent_downloads: usize,
    /// Global bandwidth rate limit in bytes per second.
    pub global_speed_limit: Option<u64>,
    /// Default download directory.
    pub default_download_dir: PathBuf,
}

impl Default for DownloadManagerConfig {
    fn default() -> Self {
        let default_dir = AppDirectories::detect()
            .map(|d| d.download_dir().to_path_buf())
            .unwrap_or_else(|_| PathBuf::from("./downloads"));

        Self {
            max_concurrent_downloads: 3,
            global_speed_limit: None,
            default_download_dir: default_dir,
        }
    }
}

/// Commands sent to the background actor managing state and queue.
enum ManagerCommand {
    Add {
        req: Box<AddDownloadRequest>,
        reply: oneshot::Sender<Result<u64>>,
    },
    Pause {
        id: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    Resume {
        id: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    Cancel {
        id: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    Remove {
        id: u64,
        delete_file: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    SetMaxConcurrent {
        limit: usize,
        reply: oneshot::Sender<()>,
    },
    SetGlobalSpeedLimit {
        limit: Option<u64>,
        reply: oneshot::Sender<()>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Internal notifications from child download worker tasks.
enum WorkerMessage {
    Progress {
        id: u64,
        downloaded: u64,
        total: Option<u64>,
        speed: f64,
    },
    Finished {
        id: u64,
        path: PathBuf,
        total_bytes: u64,
    },
    Failed {
        id: u64,
        error: String,
        needs_refresh: bool,
    },
    Paused {
        id: u64,
    },
}

/// Active task tracked in memory by the actor.
struct ActiveTask {
    cancel_token: CancellationToken,
    _join_handle: JoinHandle<()>,
}

/// Cloneable handle to the central DownloadManager.
#[derive(Clone)]
pub struct DownloadManager {
    cmd_tx: mpsc::Sender<ManagerCommand>,
    event_bus: broadcast::Sender<ManagerEvent>,
    db: Database,
}

impl DownloadManager {
    /// Initializes and starts a new `DownloadManager` with SQLite persistence.
    pub fn new(db: Database, config: DownloadManagerConfig) -> Result<Self> {
        // Recover any unfinished active downloads on startup
        let recovered = db.recover_interrupted_downloads()?;
        if recovered > 0 {
            info!("Recovered {recovered} active downloads from previous session to 'Paused'");
        }

        let (cmd_tx, cmd_rx) = mpsc::channel::<ManagerCommand>(128);
        let (event_bus, _) = broadcast::channel::<ManagerEvent>(EVENT_BROADCAST_CAPACITY);

        let actor = ManagerActor::new(cmd_rx, event_bus.clone(), db.clone(), config);
        tokio::spawn(actor.run());

        Ok(Self {
            cmd_tx,
            event_bus,
            db,
        })
    }

    /// Initializes a manager using the default SQLite path in application data directory.
    pub fn init_default() -> Result<Self> {
        let app_dirs = AppDirectories::detect()?;
        let db_path = app_dirs.data_dir().join("tenun.db");
        let db = Database::open(&db_path)?;
        Self::new(db, DownloadManagerConfig::default())
    }

    /// Subscribes to real-time manager events (status, progress, completion).
    pub fn subscribe(&self) -> broadcast::Receiver<ManagerEvent> {
        self.event_bus.subscribe()
    }

    /// Access to direct database queries.
    pub fn db(&self) -> &Database {
        &self.db
    }

    /// Adds a new download item to the manager.
    pub async fn add(&self, req: AddDownloadRequest) -> Result<u64> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(ManagerCommand::Add {
                req: Box::new(req),
                reply,
            })
            .await
            .map_err(|_| TenunError::Platform("DownloadManager actor is shut down".to_string()))?;
        rx.await
            .map_err(|_| TenunError::Platform("Actor dropped response channel".to_string()))?
    }

    /// Pauses an active or queued download.
    pub async fn pause(&self, id: u64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(ManagerCommand::Pause { id, reply })
            .await
            .map_err(|_| TenunError::Platform("DownloadManager actor is shut down".to_string()))?;
        rx.await
            .map_err(|_| TenunError::Platform("Actor dropped response channel".to_string()))?
    }

    /// Resumes a paused, failed, or queued download.
    pub async fn resume(&self, id: u64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(ManagerCommand::Resume { id, reply })
            .await
            .map_err(|_| TenunError::Platform("DownloadManager actor is shut down".to_string()))?;
        rx.await
            .map_err(|_| TenunError::Platform("Actor dropped response channel".to_string()))?
    }

    /// Cancels and aborts a download.
    pub async fn cancel(&self, id: u64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(ManagerCommand::Cancel { id, reply })
            .await
            .map_err(|_| TenunError::Platform("DownloadManager actor is shut down".to_string()))?;
        rx.await
            .map_err(|_| TenunError::Platform("Actor dropped response channel".to_string()))?
    }

    /// Removes a download from the manager and database, optionally deleting files on disk.
    pub async fn remove(&self, id: u64, delete_file: bool) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(ManagerCommand::Remove {
                id,
                delete_file,
                reply,
            })
            .await
            .map_err(|_| TenunError::Platform("DownloadManager actor is shut down".to_string()))?;
        rx.await
            .map_err(|_| TenunError::Platform("Actor dropped response channel".to_string()))?
    }

    /// Updates the maximum concurrent downloads limit.
    pub async fn set_max_concurrent(&self, limit: usize) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(ManagerCommand::SetMaxConcurrent { limit, reply })
            .await
            .map_err(|_| TenunError::Platform("DownloadManager actor is shut down".to_string()))?;
        let _ = rx.await;
        Ok(())
    }

    /// Updates the global speed limit.
    pub async fn set_global_speed_limit(&self, limit: Option<u64>) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(ManagerCommand::SetGlobalSpeedLimit { limit, reply })
            .await
            .map_err(|_| TenunError::Platform("DownloadManager actor is shut down".to_string()))?;
        let _ = rx.await;
        Ok(())
    }

    /// Fetches all downloads matching optional filters from SQLite.
    pub fn list(
        &self,
        status: Option<DownloadStatus>,
        category: Option<FileCategory>,
    ) -> Result<Vec<DownloadRecord>> {
        self.db.list(status, category)
    }

    /// Fetches a download record by numeric ID.
    pub fn get(&self, id: u64) -> Result<Option<DownloadRecord>> {
        self.db.get(id)
    }

    /// Gracefully shuts down the manager actor.
    pub async fn shutdown(&self) {
        let (reply, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(ManagerCommand::Shutdown { reply })
            .await
            .is_ok()
        {
            let _ = rx.await;
        }
    }
}

/// The internal actor loop that coordinates tasks and enforces concurrency.
struct ManagerActor {
    cmd_rx: mpsc::Receiver<ManagerCommand>,
    event_bus: broadcast::Sender<ManagerEvent>,
    db: Database,
    config: DownloadManagerConfig,
    active_tasks: HashMap<u64, ActiveTask>,
    worker_tx: mpsc::Sender<WorkerMessage>,
    worker_rx: mpsc::Receiver<WorkerMessage>,
    global_limiter: Option<Arc<TokenBucketLimiter>>,
}

impl ManagerActor {
    fn new(
        cmd_rx: mpsc::Receiver<ManagerCommand>,
        event_bus: broadcast::Sender<ManagerEvent>,
        db: Database,
        config: DownloadManagerConfig,
    ) -> Self {
        let (worker_tx, worker_rx) = mpsc::channel::<WorkerMessage>(256);
        let global_limiter = config
            .global_speed_limit
            .map(|r| Arc::new(TokenBucketLimiter::new(r)));

        Self {
            cmd_rx,
            event_bus,
            db,
            config,
            active_tasks: HashMap::new(),
            worker_tx,
            worker_rx,
            global_limiter,
        }
    }

    async fn run(mut self) {
        // Initial queue schedule check
        self.check_queue();

        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        // Discard first immediate tick
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.sync_active_tasks_and_queue();
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        ManagerCommand::Add { req, reply } => {
                            let res = self.handle_add(*req);
                            let _ = reply.send(res);
                            self.check_queue();
                        }
                        ManagerCommand::Pause { id, reply } => {
                            let res = self.handle_pause(id);
                            let _ = reply.send(res);
                            self.check_queue();
                        }
                        ManagerCommand::Resume { id, reply } => {
                            let res = self.handle_resume(id);
                            let _ = reply.send(res);
                            self.check_queue();
                        }
                        ManagerCommand::Cancel { id, reply } => {
                            let res = self.handle_cancel(id);
                            let _ = reply.send(res);
                            self.check_queue();
                        }
                        ManagerCommand::Remove { id, delete_file, reply } => {
                            let res = self.handle_remove(id, delete_file);
                            let _ = reply.send(res);
                            self.check_queue();
                        }
                        ManagerCommand::SetMaxConcurrent { limit, reply } => {
                            self.config.max_concurrent_downloads = limit.max(1);
                            let _ = reply.send(());
                            self.check_queue();
                        }
                        ManagerCommand::SetGlobalSpeedLimit { limit, reply } => {
                            self.config.global_speed_limit = limit;
                            self.global_limiter =
                                limit.map(|r| Arc::new(TokenBucketLimiter::new(r)));
                            let _ = reply.send(());
                        }
                        ManagerCommand::Shutdown { reply } => {
                            for (_, task) in self.active_tasks.drain() {
                                task.cancel_token.cancel();
                            }
                            let _ = reply.send(());
                            break;
                        }
                    }
                }
                Some(msg) = self.worker_rx.recv() => {
                    match msg {
                        WorkerMessage::Progress { id, downloaded, total, speed } => {
                            // Update progress in DB throttled (db update is done here)
                            let _ = self.db.update_progress(id, downloaded, total);
                            let _ = self.event_bus.send(ManagerEvent::Progress {
                                id,
                                downloaded_bytes: downloaded,
                                total_bytes: total,
                                speed_bytes_per_sec: speed,
                            });
                        }
                        WorkerMessage::Finished { id, path, total_bytes } => {
                            self.active_tasks.remove(&id);
                            let _ = self.db.mark_completed(id, total_bytes);
                            let _ = self.event_bus.send(ManagerEvent::Finished {
                                id,
                                path,
                                total_bytes,
                            });
                            self.check_queue();
                        }
                        WorkerMessage::Failed { id, error, needs_refresh } => {
                            self.active_tasks.remove(&id);
                            let status = if needs_refresh {
                                DownloadStatus::NeedsRefresh
                            } else {
                                DownloadStatus::Failed
                            };
                            let _ = self.db.update_status(id, status, Some(&error));
                            let _ = self.event_bus.send(ManagerEvent::Failed { id, error });
                            self.check_queue();
                        }
                        WorkerMessage::Paused { id } => {
                            self.active_tasks.remove(&id);
                            let _ = self.db.update_status(id, DownloadStatus::Paused, None);
                            let _ = self.event_bus.send(ManagerEvent::Paused { id });
                            self.check_queue();
                        }
                    }
                }
                else => break,
            }
        }
    }

    fn sync_active_tasks_and_queue(&mut self) {
        let mut cancelled_ids = Vec::new();
        for (&id, task) in &self.active_tasks {
            if let Ok(Some(rec)) = self.db.get(id) {
                if rec.status == DownloadStatus::Paused || rec.status == DownloadStatus::Cancelled {
                    task.cancel_token.cancel();
                    cancelled_ids.push(id);
                }
            }
        }
        for id in cancelled_ids {
            self.active_tasks.remove(&id);
        }
        self.check_queue();
    }

    fn handle_add(&mut self, req: AddDownloadRequest) -> Result<u64> {
        // Derive initial filename from URL if not specified
        let filename = req.filename.unwrap_or_else(|| {
            req.url
                .rsplit('/')
                .next()
                .unwrap_or("download")
                .split('?')
                .next()
                .unwrap_or("download")
                .to_string()
        });

        let category = req
            .category
            .unwrap_or_else(|| FileCategory::from_filename_or_mime(&filename, None));

        let dest_dir = req.output_dir.unwrap_or_else(|| {
            self.config
                .default_download_dir
                .join(category.subfolder_name())
        });

        let status = if req.start_now {
            DownloadStatus::Queued
        } else {
            DownloadStatus::Paused
        };

        let new_rec = NewDownloadRecord {
            uuid: Uuid::new_v4().to_string(),
            url: req.url.clone(),
            filename: filename.clone(),
            dest_dir: dest_dir.to_string_lossy().to_string(),
            total_bytes: None,
            status,
            category,
            priority: req.priority,
            options: req.options,
        };

        let id = self.db.insert(&new_rec)?;

        let _ = self.event_bus.send(ManagerEvent::Added {
            id,
            uuid: new_rec.uuid,
            filename,
            url: req.url,
        });

        Ok(id)
    }

    fn handle_pause(&mut self, id: u64) -> Result<()> {
        if let Some(task) = self.active_tasks.remove(&id) {
            task.cancel_token.cancel();
        }
        self.db.update_status(id, DownloadStatus::Paused, None)?;
        let _ = self.event_bus.send(ManagerEvent::StatusChanged {
            id,
            status: DownloadStatus::Paused,
        });
        Ok(())
    }

    fn handle_resume(&mut self, id: u64) -> Result<()> {
        let rec = self
            .db
            .get(id)?
            .ok_or_else(|| TenunError::Config(format!("Download item {id} not found")))?;

        if rec.status == DownloadStatus::Downloading || rec.status == DownloadStatus::Probing {
            return Ok(());
        }

        self.db.update_status(id, DownloadStatus::Queued, None)?;
        let _ = self.event_bus.send(ManagerEvent::StatusChanged {
            id,
            status: DownloadStatus::Queued,
        });
        Ok(())
    }

    fn handle_cancel(&mut self, id: u64) -> Result<()> {
        if let Some(task) = self.active_tasks.remove(&id) {
            task.cancel_token.cancel();
        }
        self.db.update_status(id, DownloadStatus::Cancelled, None)?;
        let _ = self.event_bus.send(ManagerEvent::Cancelled { id });
        Ok(())
    }

    fn handle_remove(&mut self, id: u64, delete_file: bool) -> Result<()> {
        if let Some(task) = self.active_tasks.remove(&id) {
            task.cancel_token.cancel();
        }

        if delete_file {
            if let Ok(Some(rec)) = self.db.get(id) {
                let dest = Path::new(&rec.dest_dir);
                let target = dest.join(&rec.filename);
                let part = dest.join(format!("{}.part", rec.filename));
                let state = dest.join(format!("{}.tenun", rec.filename));

                let _ = std::fs::remove_file(target);
                let _ = std::fs::remove_file(part);
                let _ = std::fs::remove_file(state);
            }
        }

        self.db.delete(id)?;
        let _ = self.event_bus.send(ManagerEvent::Removed { id });
        Ok(())
    }

    /// Checks the queue and spawns active download workers up to `max_concurrent_downloads`.
    fn check_queue(&mut self) {
        while self.active_tasks.len() < self.config.max_concurrent_downloads {
            match self.db.get_next_queued() {
                Ok(Some(record)) => {
                    self.spawn_download_task(record);
                }
                _ => break,
            }
        }
    }

    fn spawn_download_task(&mut self, record: DownloadRecord) {
        let id = record.id;
        let token = CancellationToken::new();
        let child_token = token.clone();
        let worker_tx = self.worker_tx.clone();

        // Transition status to Downloading
        let _ = self.db.update_status(id, DownloadStatus::Downloading, None);
        let _ = self.event_bus.send(ManagerEvent::StatusChanged {
            id,
            status: DownloadStatus::Downloading,
        });

        let global_speed_limit = self.config.global_speed_limit;
        let join_handle = tokio::spawn(async move {
            run_download_job(record, child_token, worker_tx, global_speed_limit).await;
        });

        self.active_tasks.insert(
            id,
            ActiveTask {
                cancel_token: token,
                _join_handle: join_handle,
            },
        );
    }
}

/// Executes an individual download job and routes progress/events back to manager.
async fn run_download_job(
    record: DownloadRecord,
    token: CancellationToken,
    worker_tx: mpsc::Sender<WorkerMessage>,
    global_speed_limit: Option<u64>,
) {
    let id = record.id;
    let dest_dir = PathBuf::from(&record.dest_dir);
    if let Err(e) = std::fs::create_dir_all(&dest_dir) {
        let _ = worker_tx
            .send(WorkerMessage::Failed {
                id,
                error: format!(
                    "Failed to create download directory {}: {}",
                    dest_dir.display(),
                    e
                ),
                needs_refresh: false,
            })
            .await;
        return;
    }
    let state_file = dest_dir.join(format!("{}.tenun", record.filename));

    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<DownloadEvent>();

    let w_tx = worker_tx.clone();
    let forwarder = tokio::spawn(async move {
        let mut last_progress = Instant::now();
        while let Some(event) = ev_rx.recv().await {
            match event {
                DownloadEvent::Started { .. } => {}
                DownloadEvent::Progress {
                    downloaded_bytes,
                    total_bytes,
                    speed_bytes_per_sec,
                } => {
                    if last_progress.elapsed() >= Duration::from_millis(250) {
                        let _ = w_tx
                            .send(WorkerMessage::Progress {
                                id,
                                downloaded: downloaded_bytes,
                                total: total_bytes,
                                speed: speed_bytes_per_sec,
                            })
                            .await;
                        last_progress = Instant::now();
                    }
                }
                DownloadEvent::Finished { path, total_bytes } => {
                    let _ = w_tx
                        .send(WorkerMessage::Finished {
                            id,
                            path,
                            total_bytes,
                        })
                        .await;
                }
            }
        }
    });

    let mut opts = record.options();
    if opts.limit_rate.is_none() {
        opts.limit_rate = global_speed_limit;
    }

    let result = if state_file.exists() {
        debug!("Resuming existing download from {}", state_file.display());
        resume_download_with_options(&state_file, Some(ev_tx), token.clone(), Some(&opts)).await
    } else {
        download_segmented_with_cancel(&record.url, &dest_dir, &opts, Some(ev_tx), token.clone())
            .await
    };

    let _ = forwarder.await;

    match result {
        Ok(path) => {
            let total = path
                .metadata()
                .map(|m| m.len())
                .unwrap_or(record.downloaded_bytes);
            let _ = worker_tx
                .send(WorkerMessage::Finished {
                    id,
                    path,
                    total_bytes: total,
                })
                .await;
        }
        Err(TenunError::DownloadPaused) => {
            let _ = worker_tx.send(WorkerMessage::Paused { id }).await;
        }
        Err(TenunError::NeedsRefresh(e)) => {
            let _ = worker_tx
                .send(WorkerMessage::Failed {
                    id,
                    error: e,
                    needs_refresh: true,
                })
                .await;
        }
        Err(e) => {
            let _ = worker_tx
                .send(WorkerMessage::Failed {
                    id,
                    error: e.to_string(),
                    needs_refresh: false,
                })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_manager_lifecycle_in_memory() {
        let db = Database::in_memory().expect("db");
        let manager = DownloadManager::new(
            db,
            DownloadManagerConfig {
                max_concurrent_downloads: 2,
                ..Default::default()
            },
        )
        .expect("manager");

        let mut subscriber = manager.subscribe();

        let req = AddDownloadRequest {
            url: "https://example.com/test.iso".to_string(),
            output_dir: Some(PathBuf::from("/tmp")),
            filename: Some("test.iso".to_string()),
            options: RequestOptions::default(),
            priority: 1,
            category: Some(FileCategory::Compressed),
            start_now: false, // add as Paused
        };

        let id = manager.add(req).await.expect("add");
        assert!(id > 0);

        // Receive added event
        if let Ok(ManagerEvent::Added {
            id: ev_id,
            filename,
            ..
        }) = subscriber.recv().await
        {
            assert_eq!(ev_id, id);
            assert_eq!(filename, "test.iso");
        } else {
            panic!("Expected Added event");
        }

        // Check list
        let list = manager.list(None, None).expect("list");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].status, DownloadStatus::Paused);

        // Resume triggers queue: status becomes Downloading (slot available)
        manager.resume(id).await.expect("resume");
        let rec = manager.get(id).expect("get").expect("found");
        assert!(rec.status == DownloadStatus::Downloading || rec.status == DownloadStatus::Queued);

        // Pause
        manager.pause(id).await.expect("pause");
        let rec_paused = manager.get(id).expect("get").expect("found");
        assert_eq!(rec_paused.status, DownloadStatus::Paused);

        // Cancel
        manager.cancel(id).await.expect("cancel");
        let rec2 = manager.get(id).expect("get").expect("found");
        assert_eq!(rec2.status, DownloadStatus::Cancelled);

        // Remove
        manager.remove(id, false).await.expect("remove");
        assert!(manager.get(id).expect("get").is_none());

        manager.shutdown().await;
    }
}
