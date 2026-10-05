//! Tenun CLI application.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use tenun_core::{download_single, init_logging, AppDirectories, DownloadEvent, RequestOptions};
use tokio::sync::mpsc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "tenun", version, about = "Fast and resilient download manager")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Download a file from a URL using single stream (segmented download in Phase 2)
    Get {
        /// Target URL to download
        url: String,

        /// Output directory where the file will be saved
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Custom User-Agent header
        #[arg(long)]
        user_agent: Option<String>,

        /// Referer header
        #[arg(long)]
        referer: Option<String>,

        /// Cookie header
        #[arg(long)]
        cookie: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging("warn");

    match cli.command {
        Commands::Get {
            url,
            output,
            user_agent,
            referer,
            cookie,
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

            let opts = RequestOptions {
                user_agent,
                referer,
                cookie,
                ..Default::default()
            };

            let (tx, mut rx) = mpsc::unbounded_channel::<DownloadEvent>();

            // Spawn progress bar handler in background
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
                            bar.set_message(format!("Downloading {filename}"));
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
            let final_path = download_single(&url, &dest_dir, &opts, Some(tx)).await?;
            let _ = progress_task.await;

            println!("Download complete: {}", final_path.display());
        }
    }

    Ok(())
}
