//! Tenun CLI application.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use tenun_core::{
    compute_file_checksum, download_segmented_with_cancel, init_logging, parse_checksum_file,
    resume_download_with_options, verify_file_checksum, AppDirectories, CancellationToken,
    ChecksumAlgorithm, ClashResolution, DownloadEvent, ExpectedChecksum, RequestOptions,
    TenunError,
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

    /// Resume an interrupted or paused download from a .tenun state file
    Resume {
        /// Path to the .tenun state file
        state_file: PathBuf,

        /// Maximum bandwidth rate limit override (e.g. 500K, 1M, 2MB)
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
}
