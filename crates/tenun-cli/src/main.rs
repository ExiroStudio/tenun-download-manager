//! Tenun CLI application.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use tenun_core::{
    download_segmented_with_cancel, init_logging, resume_download_with_options, AppDirectories,
    CancellationToken, DownloadEvent, RequestOptions, TenunError,
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
    },

    /// Resume an interrupted or paused download from a .tenun state file
    Resume {
        /// Path to the .tenun state file
        state_file: PathBuf,

        /// Maximum bandwidth rate limit override (e.g. 500K, 1M, 2MB)
        #[arg(long, value_parser = parse_rate_limit)]
        limit_rate: Option<u64>,
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

            let opts = RequestOptions {
                connections: Some(conn_clamped),
                user_agent,
                referer,
                cookie,
                limit_rate,
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

        Commands::Resume {
            state_file,
            limit_rate,
        } => {
            if !state_file.exists() {
                anyhow::bail!("State file '{}' does not exist", state_file.display());
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
                    println!("Download paused cleanly. You can resume again using 'tenun resume'.");
                }
                Err(e) => return Err(e.into()),
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
}
