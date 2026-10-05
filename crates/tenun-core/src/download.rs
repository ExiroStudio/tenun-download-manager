//! Single-stream downloading and event reporting module.

use crate::error::{Result, TenunError};
use crate::platform::{atomic_rename_file, sync_file};
use crate::probe::{apply_options, build_client, probe, RequestOptions};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::sync::mpsc::UnboundedSender;
use tracing::info;

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
        /// Total bytes downloaded so far.
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

    // Perform full single-stream GET request
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

    // Write to part file
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
            if elapsed >= std::time::Duration::from_millis(200) {
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

        // Flush and fsync before closing handle
        file.flush().map_err(TenunError::Io)?;
        sync_file(&file).map_err(TenunError::Io)?;
    } // File handle is explicitly closed here, allowing safe rename on Windows

    // Atomic rename from .part to final
    atomic_rename_file(&part_path, &final_path)?;

    if let Some(ref tx) = event_tx {
        let _ = tx.send(DownloadEvent::Finished {
            path: final_path.clone(),
            total_bytes: downloaded,
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
