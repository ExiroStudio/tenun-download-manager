//! Tenun CLI application.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use tenun_core::{
    compute_file_checksum, download_segmented_with_cancel, init_logging, parse_checksum_file,
    resume_download_with_options, verify_file_checksum, AppDirectories, CancellationToken,
    ChecksumAlgorithm, ClashResolution, Database, DownloadEvent, DownloadManager,
    DownloadManagerConfig, DownloadStatus, ExpectedChecksum, FileCategory, ManagerEvent,
    NewDownloadRecord, RequestOptions, TenunError,
};
use tokio::sync::mpsc;
use tracing::info;

/// Parses human-friendly bandwidth strings like `500K`, `1M`, `2MB`, `1048576`.
fn parse_rate_limit(s: &str) -> std::result::Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Rate limit string cannot be empty".to_string());
    }

    let upper = s.to_ascii_uppercase();
    if upper.ends_with("KIB") || upper.ends_with("KB") {
        let num_str = upper.trim_end_matches("KIB").trim_end_matches("KB").trim();
        let num: f64 = num_str
            .parse()
            .map_err(|e| format!("Invalid rate number: {e}"))?;
        Ok((num * 1024.0) as u64)
    } else if upper.ends_with('K') {
        let num_str = upper.trim_end_matches('K').trim();
        let num: f64 = num_str
            .parse()
            .map_err(|e| format!("Invalid rate number: {e}"))?;
        Ok((num * 1024.0) as u64)
    } else if upper.ends_with("MIB") || upper.ends_with("MB") {
        let num_str = upper.trim_end_matches("MIB").trim_end_matches("MB").trim();
        let num: f64 = num_str
            .parse()
            .map_err(|e| format!("Invalid rate number: {e}"))?;
        Ok((num * 1024.0 * 1024.0) as u64)
    } else if upper.ends_with('M') {
        let num_str = upper.trim_end_matches('M').trim();
        let num: f64 = num_str
            .parse()
            .map_err(|e| format!("Invalid rate number: {e}"))?;
        Ok((num * 1024.0 * 1024.0) as u64)
    } else if upper.ends_with("GIB") || upper.ends_with("GB") {
        let num_str = upper.trim_end_matches("GIB").trim_end_matches("GB").trim();
        let num: f64 = num_str
            .parse()
            .map_err(|e| format!("Invalid rate number: {e}"))?;
        Ok((num * 1024.0 * 1024.0 * 1024.0) as u64)
    } else if upper.ends_with('G') {
        let num_str = upper.trim_end_matches('G').trim();
        let num: f64 = num_str
            .parse()
            .map_err(|e| format!("Invalid rate number: {e}"))?;
        Ok((num * 1024.0 * 1024.0 * 1024.0) as u64)
    } else if upper.ends_with('B') {
        let num_str = upper.trim_end_matches('B').trim();
        let num: u64 = num_str
            .parse()
            .map_err(|e| format!("Invalid rate number: {e}"))?;
        Ok(num)
    } else {
        let num: u64 = upper
            .parse()
            .map_err(|e| format!("Invalid rate number: {e}"))?;
        Ok(num)
    }
}

/// Formats a byte count into a human-readable string (B, KiB, MiB, GiB).
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.2} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Formats a transfer speed in bytes per second into human-readable rate.
fn format_speed(bytes_per_sec: f64) -> String {
    if bytes_per_sec >= (1024.0 * 1024.0) {
        format!("{:.2} MB/s", bytes_per_sec / (1024.0 * 1024.0))
    } else if bytes_per_sec >= 1024.0 {
        format!("{:.2} KB/s", bytes_per_sec / 1024.0)
    } else {
        format!("{:.0} B/s", bytes_per_sec)
    }
}

/// Opens the application SQLite database, creating data directories if necessary.
fn get_database() -> Result<Database> {
    let app_dirs = AppDirectories::detect().context("Failed to detect app directories")?;
    let db_dir = app_dirs.data_dir();
    std::fs::create_dir_all(db_dir)
        .with_context(|| format!("Failed to create {}", db_dir.display()))?;
    let db_path = db_dir.join("tenun.db");
    Database::open(&db_path).context("Failed to open SQLite database")
}

/// Conflict resolution strategy when destination file already exists.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Default)]
enum ClashArg {
    /// Appends incrementing number (file (1).ext)
    #[default]
    Rename,
    /// Overwrites existing file
    Overwrite,
    /// Skips downloading if existing file has identical checksum
    Deduplicate,
}

impl From<ClashArg> for ClashResolution {
    fn from(arg: ClashArg) -> Self {
        match arg {
            ClashArg::Rename => ClashResolution::Rename,
            ClashArg::Overwrite => ClashResolution::Overwrite,
            ClashArg::Deduplicate => ClashResolution::Deduplicate,
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "tenun", version, about = "Fast and resilient download manager")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Download a file from a URL using multi-connection segmented download
    Get {
        /// Target URL to download
        url: String,

        /// Output directory where the file will be saved
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Number of parallel connections to use (default: 8, max: 32)
        #[arg(short, long, default_value_t = 8)]
        connections: usize,

        /// Custom User-Agent header
        #[arg(long)]
        user_agent: Option<String>,

        /// Referer header
        #[arg(long)]
        referer: Option<String>,

        /// Cookie header
        #[arg(long)]
        cookie: Option<String>,

        /// Maximum bandwidth rate limit (e.g. 500K, 1M, 2MB, 1048576)
        #[arg(long, value_parser = parse_rate_limit)]
        limit_rate: Option<u64>,

        /// Expected SHA-256 hash for integrity verification
        #[arg(long)]
        sha256: Option<String>,

        /// Expected SHA-1 hash for integrity verification
        #[arg(long)]
        sha1: Option<String>,

        /// Expected MD5 hash for integrity verification
        #[arg(long)]
        md5: Option<String>,

        /// Collision resolution strategy when file already exists
        #[arg(long, value_enum, default_value_t = ClashArg::Rename)]
        clash: ClashArg,
    },

    /// Add a download to the queue
    Add {
        /// Target URL to download
        url: String,

        /// Destination directory
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Number of parallel connections (1-32)
        #[arg(short, long)]
        connections: Option<usize>,

        /// Priority in queue (higher runs earlier, default: 0)
        #[arg(short, long, default_value_t = 0)]
        priority: i32,

        /// Category override (Documents, Compressed, Video, Audio, Images, Programs, General)
        #[arg(long)]
        category: Option<String>,

        /// Start downloading immediately
        #[arg(long)]
        start_now: bool,
    },

    /// List downloads in the manager queue
    List {
        /// Filter by status (Queued, Probing, Downloading, Paused, Verifying, Completed, Failed, Cancelled, NeedsRefresh)
        #[arg(short, long)]
        status: Option<String>,

        /// Filter by category (Documents, Compressed, Video, Audio, Images, Programs, General)
        #[arg(short, long)]
        category: Option<String>,
    },

    /// Pause an active or queued download
    Pause {
        /// ID of download to pause
        id: u64,
    },

    /// Resume a download by ID or from a .tenun state file
    Resume {
        /// Download ID in queue or path to .tenun state file
        target: String,

        /// Maximum bandwidth rate limit override (e.g. 500K, 1M, 2MB)
        #[arg(long, value_parser = parse_rate_limit)]
        limit_rate: Option<u64>,
    },

    /// Cancel a download
    Cancel {
        /// ID of download to cancel
        id: u64,
    },

    /// Remove a download from manager and database
    Remove {
        /// ID of download to remove
        id: u64,

        /// Also delete downloaded or partial files from disk
        #[arg(long)]
        delete_file: bool,
    },

    /// Run the download manager actor daemon processing the queue
    Run {
        /// Maximum concurrent downloads (default: 3)
        #[arg(short, long, default_value_t = 3)]
        concurrent: usize,

        /// Global bandwidth rate limit (e.g. 500K, 1M, 2MB)
        #[arg(long, value_parser = parse_rate_limit)]
        limit_rate: Option<u64>,
    },

    /// Verify the cryptographic checksum of a file
    Verify {
        /// Path to the file to verify
        file: PathBuf,

        /// Expected SHA-256 hash (64 hex characters)
        #[arg(long)]
        sha256: Option<String>,

        /// Expected SHA-1 hash (40 hex characters)
        #[arg(long)]
        sha1: Option<String>,

        /// Expected MD5 hash (32 hex characters)
        #[arg(long)]
        md5: Option<String>,

        /// Path to a checksum file (e.g. SHA256SUMS or MD5SUMS)
        #[arg(short = 'c', long)]
        checksum_file: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging("warn");

    let cancel_token = CancellationToken::new();

    // Graceful Ctrl+C handler
    {
        let token = cancel_token.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!("\nPausing download and saving checkpoint to .tenun state file...");
                token.cancel();
            }
        });
    }

    match cli.command {
        Commands::Get {
            url,
            output,
            connections,
            user_agent,
            referer,
            cookie,
            limit_rate,
            sha256,
            sha1,
            md5,
            clash,
        } => {
            let dest_dir = match output {
                Some(p) => p,
                None => match AppDirectories::detect() {
                    Ok(dirs) => dirs.download_dir().to_path_buf(),
                    Err(_) => std::env::current_dir().context("Failed to get current directory")?,
                },
            };

            std::fs::create_dir_all(&dest_dir).with_context(|| {
                format!("Failed to create output directory: {}", dest_dir.display())
            })?;

            let conn_clamped = connections.clamp(1, 32);

            let expected_checksum = if let Some(ref h) = sha256 {
                Some(ExpectedChecksum::new(ChecksumAlgorithm::Sha256, h))
            } else if let Some(ref h) = sha1 {
                Some(ExpectedChecksum::new(ChecksumAlgorithm::Sha1, h))
            } else {
                md5.as_ref()
                    .map(|h| ExpectedChecksum::new(ChecksumAlgorithm::Md5, h))
            };

            let opts = RequestOptions {
                connections: Some(conn_clamped),
                user_agent,
                referer,
                cookie,
                limit_rate,
                expected_checksum,
                clash_resolution: clash.into(),
                ..Default::default()
            };

            let (tx, mut rx) = mpsc::unbounded_channel::<DownloadEvent>();

            let progress_task = tokio::spawn(async move {
                let mut pb: Option<ProgressBar> = None;

                while let Some(event) = rx.recv().await {
                    match event {
                        DownloadEvent::Started {
                            filename,
                            total_bytes,
                        } => {
                            let bar = match total_bytes {
                                Some(total) => {
                                    let b = ProgressBar::new(total);
                                    let style = ProgressStyle::default_bar()
                                        .template("{msg}\n[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) {bytes_per_sec}")
                                        .unwrap_or_else(|_| ProgressStyle::default_bar())
                                        .progress_chars("#>-");
                                    b.set_style(style);
                                    b
                                }
                                None => {
                                    let b = ProgressBar::new_spinner();
                                    let style = ProgressStyle::default_spinner()
                                        .template(
                                            "{msg}\n[{elapsed_precise}] {bytes} ({bytes_per_sec})",
                                        )
                                        .unwrap_or_else(|_| ProgressStyle::default_spinner());
                                    b.set_style(style);
                                    b
                                }
                            };
                            bar.set_message(format!(
                                "Downloading {filename} (connections: {conn_clamped})"
                            ));
                            pb = Some(bar);
                        }
                        DownloadEvent::Progress {
                            downloaded_bytes, ..
                        } => {
                            if let Some(ref b) = pb {
                                b.set_position(downloaded_bytes);
                            }
                        }
                        DownloadEvent::Finished { path, total_bytes } => {
                            if let Some(b) = pb.take() {
                                b.finish_with_message(format!(
                                    "Saved {} ({} bytes)",
                                    path.display(),
                                    total_bytes
                                ));
                            }
                        }
                    }
                }
            });

            info!("Starting download for {url}");
            let res =
                download_segmented_with_cancel(&url, &dest_dir, &opts, Some(tx), cancel_token)
                    .await;
            let _ = progress_task.await;

            match res {
                Ok(path) => println!("Download complete: {}", path.display()),
                Err(TenunError::DownloadPaused) => {
                    println!("Download paused cleanly. You can resume later using 'tenun resume'.");
                }
                Err(e) => return Err(e.into()),
            }
        }

        Commands::Add {
            url,
            output,
            connections,
            priority,
            category,
            start_now,
        } => {
            let app_dirs = AppDirectories::detect().context("Failed to detect app directories")?;
            let db = get_database()?;

            let filename = url
                .rsplit('/')
                .next()
                .unwrap_or("download")
                .split('?')
                .next()
                .unwrap_or("download")
                .to_string();

            let cat = if let Some(cat_str) = category {
                cat_str
                    .parse::<FileCategory>()
                    .map_err(|e| anyhow::anyhow!(e))?
            } else {
                FileCategory::from_filename_or_mime(&filename, None)
            };

            let dest_dir =
                output.unwrap_or_else(|| app_dirs.download_dir().join(cat.subfolder_name()));
            std::fs::create_dir_all(&dest_dir).with_context(|| {
                format!(
                    "Failed to create download directory: {}",
                    dest_dir.display()
                )
            })?;

            let conn_clamped = connections.map(|c| c.clamp(1, 32));
            let opts = RequestOptions {
                connections: conn_clamped,
                ..Default::default()
            };

            let status = if start_now {
                DownloadStatus::Queued
            } else {
                DownloadStatus::Paused
            };

            let new_rec = NewDownloadRecord::new(
                url.clone(),
                filename.clone(),
                dest_dir.to_string_lossy().to_string(),
                cat,
                status,
                priority,
                opts,
            );

            let id = db.insert(&new_rec)?;
            println!("Added download #{id} ({filename})");
            println!("  URL:      {url}");
            println!("  Category: {cat}");
            println!("  Folder:   {}", dest_dir.display());
            println!("  Status:   {status}");
            println!("  Priority: {priority}");
            if !start_now {
                println!(
                    "\nDownload staged as Paused. Run 'tenun resume {id}' or run 'tenun run' to start."
                );
            } else {
                println!(
                    "\nDownload queued. Run 'tenun run' to process downloads if manager is not already running."
                );
            }
        }

        Commands::List { status, category } => {
            let db = get_database()?;

            let status_filter = if let Some(s) = status {
                Some(DownloadStatus::parse(&s).ok_or_else(|| {
                    anyhow::anyhow!("Unknown status '{}'. Valid: Queued, Probing, Downloading, Paused, Verifying, Completed, Failed, Cancelled, NeedsRefresh", s)
                })?)
            } else {
                None
            };

            let category_filter = if let Some(c) = category {
                Some(c.parse::<FileCategory>().map_err(|e| anyhow::anyhow!(e))?)
            } else {
                None
            };

            let records = db.list(status_filter, category_filter)?;
            if records.is_empty() {
                println!("No downloads found matching criteria.");
                return Ok(());
            }

            println!(
                "{:<4} {:<13} {:<12} {:<24} {:<10} FILENAME",
                "ID", "STATUS", "CATEGORY", "PROGRESS", "PRIORITY"
            );
            println!("{}", "-".repeat(80));

            for r in records {
                let progress_str = match r.total_bytes {
                    Some(total) if total > 0 => {
                        let pct = (r.downloaded_bytes as f64 / total as f64) * 100.0;
                        format!(
                            "{}/{} ({:.1}%)",
                            format_bytes(r.downloaded_bytes),
                            format_bytes(total),
                            pct
                        )
                    }
                    Some(total) => {
                        format!(
                            "{}/{}",
                            format_bytes(r.downloaded_bytes),
                            format_bytes(total)
                        )
                    }
                    None => format!("{}/?", format_bytes(r.downloaded_bytes)),
                };

                println!(
                    "{:<4} {:<13} {:<12} {:<24} {:<10} {}",
                    r.id,
                    r.status.as_str(),
                    r.category.to_string(),
                    progress_str,
                    r.priority,
                    r.filename
                );
            }
        }

        Commands::Pause { id } => {
            let db = get_database()?;
            let rec = db
                .get(id)?
                .ok_or_else(|| anyhow::anyhow!("Download #{id} not found"))?;
            db.update_status(id, DownloadStatus::Paused, None)?;
            println!("Download #{id} ({}) paused.", rec.filename);
        }

        Commands::Resume { target, limit_rate } => {
            if let Ok(id) = target.parse::<u64>() {
                let db = get_database()?;
                let rec = db
                    .get(id)?
                    .ok_or_else(|| anyhow::anyhow!("Download #{id} not found"))?;
                if rec.status == DownloadStatus::Completed {
                    println!("Download #{id} ({}) is already completed.", rec.filename);
                    return Ok(());
                }
                db.update_status(id, DownloadStatus::Queued, None)?;
                println!("Download #{id} ({}) resumed and queued.", rec.filename);
                println!("Run 'tenun run' to process downloads if manager is not already running.");
            } else {
                let state_file = PathBuf::from(&target);
                if !state_file.exists() {
                    anyhow::bail!(
                        "State file '{}' does not exist (or invalid download ID)",
                        target
                    );
                }

                let (tx, mut rx) = mpsc::unbounded_channel::<DownloadEvent>();

                let progress_task = tokio::spawn(async move {
                    let mut pb: Option<ProgressBar> = None;

                    while let Some(event) = rx.recv().await {
                        match event {
                            DownloadEvent::Started {
                                filename,
                                total_bytes,
                            } => {
                                let bar = match total_bytes {
                                    Some(total) => {
                                        let b = ProgressBar::new(total);
                                        let style = ProgressStyle::default_bar()
                                            .template("{msg}\n[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) {bytes_per_sec}")
                                            .unwrap_or_else(|_| ProgressStyle::default_bar())
                                            .progress_chars("#>-");
                                        b.set_style(style);
                                        b
                                    }
                                    None => {
                                        let b = ProgressBar::new_spinner();
                                        let style = ProgressStyle::default_spinner()
                                            .template(
                                                "{msg}\n[{elapsed_precise}] {bytes} ({bytes_per_sec})",
                                            )
                                            .unwrap_or_else(|_| ProgressStyle::default_spinner());
                                        b.set_style(style);
                                        b
                                    }
                                };
                                bar.set_message(format!("Resuming {filename}"));
                                pb = Some(bar);
                            }
                            DownloadEvent::Progress {
                                downloaded_bytes, ..
                            } => {
                                if let Some(ref b) = pb {
                                    b.set_position(downloaded_bytes);
                                }
                            }
                            DownloadEvent::Finished { path, total_bytes } => {
                                if let Some(b) = pb.take() {
                                    b.finish_with_message(format!(
                                        "Saved {} ({} bytes)",
                                        path.display(),
                                        total_bytes
                                    ));
                                }
                            }
                        }
                    }
                });

                info!("Resuming download from state: {}", state_file.display());
                let opts_override = limit_rate.map(|r| RequestOptions {
                    limit_rate: Some(r),
                    ..Default::default()
                });

                let res = resume_download_with_options(
                    &state_file,
                    Some(tx),
                    cancel_token,
                    opts_override.as_ref(),
                )
                .await;
                let _ = progress_task.await;

                match res {
                    Ok(path) => println!("Download resumed and completed: {}", path.display()),
                    Err(TenunError::DownloadPaused) => {
                        println!(
                            "Download paused cleanly. You can resume again using 'tenun resume'."
                        );
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }

        Commands::Cancel { id } => {
            let db = get_database()?;
            let rec = db
                .get(id)?
                .ok_or_else(|| anyhow::anyhow!("Download #{id} not found"))?;
            db.update_status(id, DownloadStatus::Cancelled, None)?;
            println!("Download #{id} ({}) cancelled.", rec.filename);
        }

        Commands::Remove { id, delete_file } => {
            let db = get_database()?;
            let rec = db
                .get(id)?
                .ok_or_else(|| anyhow::anyhow!("Download #{id} not found"))?;
            if delete_file {
                let dest = std::path::Path::new(&rec.dest_dir);
                let target = dest.join(&rec.filename);
                let part = dest.join(format!("{}.part", rec.filename));
                let state = dest.join(format!("{}.tenun", rec.filename));

                let _ = std::fs::remove_file(target);
                let _ = std::fs::remove_file(part);
                let _ = std::fs::remove_file(state);
            }
            db.delete(id)?;
            if delete_file {
                println!(
                    "Download #{id} ({}) deleted from database and disk.",
                    rec.filename
                );
            } else {
                println!("Download #{id} ({}) removed from database.", rec.filename);
            }
        }

        Commands::Run {
            concurrent,
            limit_rate,
        } => {
            let app_dirs = AppDirectories::detect().context("Failed to detect app directories")?;
            let db = get_database()?;
            let config = DownloadManagerConfig {
                max_concurrent_downloads: concurrent.max(1),
                global_speed_limit: limit_rate,
                default_download_dir: app_dirs.download_dir().to_path_buf(),
            };

            let manager = DownloadManager::new(db, config)?;
            let mut subscriber = manager.subscribe();

            println!("Tenun Download Manager running.");
            println!("  Max concurrent:     {}", concurrent.max(1));
            if let Some(limit) = limit_rate {
                println!("  Global speed limit: {}", format_bytes(limit));
            }
            println!("Press Ctrl+C to stop.");
            println!("{}", "-".repeat(60));

            let mut last_progress_print = std::collections::HashMap::new();

            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        println!("\nStopping Download Manager...");
                        manager.shutdown().await;
                        println!("Download Manager stopped.");
                        break;
                    }
                    Ok(event) = subscriber.recv() => {
                        match event {
                            ManagerEvent::Added { id, filename, .. } => {
                                println!("[+] Added #{id}: {filename}");
                            }
                            ManagerEvent::StatusChanged { id, status } => {
                                println!("[*] #{id} -> {status}");
                            }
                            ManagerEvent::Progress {
                                id,
                                downloaded_bytes,
                                total_bytes,
                                speed_bytes_per_sec,
                            } => {
                                let now = std::time::Instant::now();
                                let should_print = match last_progress_print.get(&id) {
                                    Some(&last) => now.duration_since(last) >= std::time::Duration::from_secs(1),
                                    None => true,
                                };
                                if should_print {
                                    last_progress_print.insert(id, now);
                                    let total_str = total_bytes.map(format_bytes).unwrap_or_else(|| "?".to_string());
                                    let speed_str = format_speed(speed_bytes_per_sec);
                                    let pct = total_bytes
                                        .map(|t| if t > 0 { (downloaded_bytes as f64 / t as f64) * 100.0 } else { 0.0 })
                                        .unwrap_or(0.0);
                                    println!(
                                        "[>] #{id} {:>6.1}% | {} / {} ({})",
                                        pct,
                                        format_bytes(downloaded_bytes),
                                        total_str,
                                        speed_str
                                    );
                                }
                            }
                            ManagerEvent::Finished { id, path, total_bytes } => {
                                println!(
                                    "[✓] #{id} Finished: {} ({})",
                                    path.display(),
                                    format_bytes(total_bytes)
                                );
                            }
                            ManagerEvent::Failed { id, error } => {
                                eprintln!("[✗] #{id} Failed: {error}");
                            }
                            ManagerEvent::Paused { id } => {
                                println!("[-] #{id} Paused");
                            }
                            ManagerEvent::Cancelled { id } => {
                                println!("[-] #{id} Cancelled");
                            }
                            ManagerEvent::Removed { id } => {
                                println!("[-] #{id} Removed");
                            }
                        }
                    }
                }
            }
        }

        Commands::Verify {
            file,
            sha256,
            sha1,
            md5,
            checksum_file,
        } => {
            if !file.exists() {
                anyhow::bail!("File '{}' does not exist", file.display());
            }

            let expected = if let Some(ref h) = sha256 {
                ExpectedChecksum::new(ChecksumAlgorithm::Sha256, h)
            } else if let Some(ref h) = sha1 {
                ExpectedChecksum::new(ChecksumAlgorithm::Sha1, h)
            } else if let Some(ref h) = md5 {
                ExpectedChecksum::new(ChecksumAlgorithm::Md5, h)
            } else if let Some(ref cf) = checksum_file {
                if !cf.exists() {
                    anyhow::bail!("Checksum file '{}' does not exist", cf.display());
                }
                let content = std::fs::read_to_string(cf)
                    .with_context(|| format!("Failed to read checksum file: {}", cf.display()))?;
                let filename = file
                    .file_name()
                    .and_then(|n| n.to_str())
                    .context("Invalid file path")?;
                parse_checksum_file(&content, filename).with_context(|| {
                    format!(
                        "File entry for '{}' not found in checksum file '{}'",
                        filename,
                        cf.display()
                    )
                })?
            } else {
                anyhow::bail!(
                    "No checksum specified. Provide --sha256, --sha1, --md5, or --checksum-file <PATH>"
                );
            };

            let matches = verify_file_checksum(&file, &expected)
                .with_context(|| format!("Failed to compute checksum for '{}'", file.display()))?;

            if matches {
                println!(
                    "OK: {} checksum matches ({})",
                    expected.algorithm, expected.hex_digest
                );
            } else {
                let actual = compute_file_checksum(&file, expected.algorithm)
                    .unwrap_or_else(|_| "unknown".to_string());
                anyhow::bail!(
                    "FAILED: Checksum mismatch for '{}'\nExpected ({}): {}\nActual   ({}): {}",
                    file.display(),
                    expected.algorithm,
                    expected.hex_digest,
                    expected.algorithm,
                    actual
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rate_limit() {
        assert_eq!(parse_rate_limit("500K").unwrap(), 500 * 1024);
        assert_eq!(parse_rate_limit("500KB").unwrap(), 500 * 1024);
        assert_eq!(parse_rate_limit("500kib").unwrap(), 500 * 1024);
        assert_eq!(parse_rate_limit("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_rate_limit("2MB").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_rate_limit("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_rate_limit("1048576").unwrap(), 1048576);
        assert_eq!(parse_rate_limit("1024B").unwrap(), 1024);
        assert!(parse_rate_limit("").is_err());
        assert!(parse_rate_limit("invalid").is_err());
    }

    #[test]
    fn test_cli_clash_resolution_conversion() {
        assert_eq!(
            ClashResolution::from(ClashArg::Rename),
            ClashResolution::Rename
        );
        assert_eq!(
            ClashResolution::from(ClashArg::Overwrite),
            ClashResolution::Overwrite
        );
        assert_eq!(
            ClashResolution::from(ClashArg::Deduplicate),
            ClashResolution::Deduplicate
        );
    }

    #[test]
    fn test_cli_get_with_checksum_and_clash() {
        let args = [
            "tenun",
            "get",
            "https://example.com/file.iso",
            "--sha256",
            "c4ca4238a0b923820dcc509a6f75849bc4ca4238a0b923820dcc509a6f75849b",
            "--clash",
            "deduplicate",
        ];
        let parsed = Cli::try_parse_from(args).expect("valid parse");
        match parsed.command {
            Commands::Get {
                sha256, clash, url, ..
            } => {
                assert_eq!(url, "https://example.com/file.iso");
                assert_eq!(
                    sha256.as_deref(),
                    Some("c4ca4238a0b923820dcc509a6f75849bc4ca4238a0b923820dcc509a6f75849b")
                );
                assert_eq!(clash, ClashArg::Deduplicate);
            }
            _ => panic!("Expected Commands::Get"),
        }
    }

    #[test]
    fn test_cli_verify_parsing() {
        let args = [
            "tenun",
            "verify",
            "sample.tar.gz",
            "--sha256",
            "66f44d8b9487c699faaa4687d60f5e7146522cbbcf074e6a88b502cf3c95963b",
        ];
        let parsed = Cli::try_parse_from(args).expect("valid parse");
        match parsed.command {
            Commands::Verify {
                file,
                sha256,
                sha1,
                md5,
                checksum_file,
            } => {
                assert_eq!(file, PathBuf::from("sample.tar.gz"));
                assert_eq!(
                    sha256.as_deref(),
                    Some("66f44d8b9487c699faaa4687d60f5e7146522cbbcf074e6a88b502cf3c95963b")
                );
                assert!(sha1.is_none());
                assert!(md5.is_none());
                assert!(checksum_file.is_none());
            }
            _ => panic!("Expected Commands::Verify"),
        }
    }

    #[test]
    fn test_cli_add_parsing() {
        let args = [
            "tenun",
            "add",
            "https://example.com/archive.zip",
            "--output",
            "/tmp/downloads",
            "--connections",
            "16",
            "--priority",
            "5",
            "--category",
            "compressed",
            "--start-now",
        ];
        let parsed = Cli::try_parse_from(args).expect("valid parse");
        match parsed.command {
            Commands::Add {
                url,
                output,
                connections,
                priority,
                category,
                start_now,
            } => {
                assert_eq!(url, "https://example.com/archive.zip");
                assert_eq!(output, Some(PathBuf::from("/tmp/downloads")));
                assert_eq!(connections, Some(16));
                assert_eq!(priority, 5);
                assert_eq!(category.as_deref(), Some("compressed"));
                assert!(start_now);
            }
            _ => panic!("Expected Commands::Add"),
        }
    }

    #[test]
    fn test_cli_list_parsing() {
        let args = [
            "tenun",
            "list",
            "--status",
            "Downloading",
            "--category",
            "video",
        ];
        let parsed = Cli::try_parse_from(args).expect("valid parse");
        match parsed.command {
            Commands::List { status, category } => {
                assert_eq!(status.as_deref(), Some("Downloading"));
                assert_eq!(category.as_deref(), Some("video"));
            }
            _ => panic!("Expected Commands::List"),
        }
    }

    #[test]
    fn test_cli_pause_resume_cancel_remove_parsing() {
        // Pause
        let p = Cli::try_parse_from(["tenun", "pause", "42"]).expect("parse pause");
        match p.command {
            Commands::Pause { id } => assert_eq!(id, 42),
            _ => panic!("Expected Pause"),
        }

        // Resume ID
        let r1 = Cli::try_parse_from(["tenun", "resume", "42"]).expect("parse resume ID");
        match r1.command {
            Commands::Resume { target, limit_rate } => {
                assert_eq!(target, "42");
                assert!(limit_rate.is_none());
            }
            _ => panic!("Expected Resume"),
        }

        // Resume state file with rate
        let r2 = Cli::try_parse_from(["tenun", "resume", "file.iso.tenun", "--limit-rate", "1M"])
            .expect("parse resume state");
        match r2.command {
            Commands::Resume { target, limit_rate } => {
                assert_eq!(target, "file.iso.tenun");
                assert_eq!(limit_rate, Some(1024 * 1024));
            }
            _ => panic!("Expected Resume"),
        }

        // Cancel
        let c = Cli::try_parse_from(["tenun", "cancel", "42"]).expect("parse cancel");
        match c.command {
            Commands::Cancel { id } => assert_eq!(id, 42),
            _ => panic!("Expected Cancel"),
        }

        // Remove
        let rm =
            Cli::try_parse_from(["tenun", "remove", "42", "--delete-file"]).expect("parse remove");
        match rm.command {
            Commands::Remove { id, delete_file } => {
                assert_eq!(id, 42);
                assert!(delete_file);
            }
            _ => panic!("Expected Remove"),
        }
    }

    #[test]
    fn test_cli_run_parsing() {
        let args = ["tenun", "run", "--concurrent", "5", "--limit-rate", "2MB"];
        let parsed = Cli::try_parse_from(args).expect("valid parse");
        match parsed.command {
            Commands::Run {
                concurrent,
                limit_rate,
            } => {
                assert_eq!(concurrent, 5);
                assert_eq!(limit_rate, Some(2 * 1024 * 1024));
            }
            _ => panic!("Expected Commands::Run"),
        }
    }

    #[test]
    fn test_format_bytes_and_speed() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(2048), "2.00 KiB");
        assert_eq!(format_bytes(5 * 1024 * 1024), "5.00 MiB");
        assert_eq!(format_bytes(3 * 1024 * 1024 * 1024), "3.00 GiB");

        assert_eq!(format_speed(500.0), "500 B/s");
        assert_eq!(format_speed(1024.0 * 2.5), "2.50 KB/s");
        assert_eq!(format_speed(1024.0 * 1024.0 * 3.75), "3.75 MB/s");
    }
}
