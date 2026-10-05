//! Persistent sidecar state management and atomic checkpointing (`.tenun`).

use crate::error::{Result, TenunError};
use crate::platform::{atomic_rename_file, sync_file};
use crate::probe::{RemoteInfo, RequestOptions};
use crate::segment::Segment;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Current state schema version.
pub const STATE_SCHEMA_VERSION: u32 = 1;

/// Structured sidecar state written to `<filename>.tenun`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownloadState {
    /// Schema format version.
    pub version: u32,
    /// Original URL.
    pub url: String,
    /// Final URL after redirects.
    pub final_url: String,
    /// Output filename.
    pub filename: String,
    /// Total expected size of the file in bytes.
    pub total_bytes: u64,
    /// Remote ETag validator, if available.
    pub etag: Option<String>,
    /// Remote Last-Modified validator, if available.
    pub last_modified: Option<String>,
    /// State of each individual segment.
    pub segments: Vec<SegmentState>,
    /// Safe request options (excluding raw secrets).
    pub options: PersistedOptions,
    /// Timestamp when download started (Unix timestamp seconds).
    pub created_at: u64,
    /// Timestamp of last written checkpoint.
    pub updated_at: u64,
}

/// State and progress of a single download segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentState {
    /// Segment identifier.
    pub id: usize,
    /// Start byte offset (inclusive).
    pub start: u64,
    /// End byte offset (inclusive).
    pub end: u64,
    /// Number of bytes successfully flushed and synced to disk.
    pub downloaded: u64,
}

impl From<&Segment> for SegmentState {
    fn from(s: &Segment) -> Self {
        Self {
            id: s.id,
            start: s.start,
            end: s.end,
            downloaded: s.downloaded,
        }
    }
}

impl From<Segment> for SegmentState {
    fn from(s: Segment) -> Self {
        Self::from(&s)
    }
}

impl From<&SegmentState> for Segment {
    fn from(s: &SegmentState) -> Self {
        Self {
            id: s.id,
            start: s.start,
            end: s.end,
            downloaded: s.downloaded,
        }
    }
}

/// Request options safely persisted to disk without leaking raw auth credentials.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PersistedOptions {
    /// Number of parallel connections used.
    pub connections: usize,
    /// Custom User-Agent string.
    pub user_agent: Option<String>,
    /// Referer header.
    pub referer: Option<String>,
    /// Custom non-sensitive headers.
    pub custom_headers: Vec<(String, String)>,
    /// Bandwidth rate limit in bytes per second.
    #[serde(default)]
    pub limit_rate: Option<u64>,
}

impl PersistedOptions {
    /// Extracts safe options from `RequestOptions`, stripping sensitive authentication headers.
    pub fn from_request_options(opts: &RequestOptions, connections: usize) -> Self {
        let safe_headers = opts
            .custom_headers
            .iter()
            .filter(|(k, _)| {
                let lower = k.to_ascii_lowercase();
                lower != "authorization" && lower != "proxy-authorization" && lower != "cookie"
            })
            .cloned()
            .collect();

        Self {
            connections,
            user_agent: opts.user_agent.clone(),
            referer: opts.referer.clone(),
            custom_headers: safe_headers,
            limit_rate: opts.limit_rate,
        }
    }

    /// Converts persisted options back to `RequestOptions`.
    pub fn to_request_options(&self) -> RequestOptions {
        RequestOptions {
            custom_headers: self.custom_headers.clone(),
            user_agent: self.user_agent.clone(),
            referer: self.referer.clone(),
            connections: Some(self.connections),
            limit_rate: self.limit_rate,
            ..Default::default()
        }
    }
}

impl DownloadState {
    /// Creates a new `DownloadState` from remote discovery info and segments.
    pub fn from_remote_info(
        info: &RemoteInfo,
        target_filename: &str,
        segments: &[Segment],
        opts: &RequestOptions,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let segment_states = segments
            .iter()
            .map(|s| SegmentState {
                id: s.id,
                start: s.start,
                end: s.end,
                downloaded: s.downloaded,
            })
            .collect();

        Self {
            version: STATE_SCHEMA_VERSION,
            url: info.original_url.clone(),
            final_url: info.final_url.clone(),
            filename: target_filename.to_string(),
            total_bytes: info.total_bytes.unwrap_or(0),
            etag: info.etag.clone(),
            last_modified: info.last_modified.clone(),
            segments: segment_states,
            options: PersistedOptions::from_request_options(opts, segments.len()),
            created_at: now,
            updated_at: now,
        }
    }

    /// Total bytes downloaded across all segments.
    pub fn downloaded_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.downloaded).sum()
    }

    /// Returns true if all segments are 100% downloaded.
    pub fn is_complete(&self) -> bool {
        self.segments
            .iter()
            .all(|s| s.downloaded >= (s.end - s.start + 1))
    }
}

/// Atomically saves the state to disk using a write-to-temp -> fsync -> rename pattern.
pub fn save_state_atomic(state_path: &Path, state: &DownloadState) -> Result<()> {
    let tmp_path = state_path.with_extension("tenun.tmp");
    let json_bytes = serde_json::to_vec_pretty(state)
        .map_err(|e| TenunError::CorruptState(format!("Serialization error: {e}")))?;

    {
        let mut file = File::create(&tmp_path).map_err(TenunError::Io)?;
        file.write_all(&json_bytes).map_err(TenunError::Io)?;
        file.flush().map_err(TenunError::Io)?;
        sync_file(&file).map_err(TenunError::Io)?;
    }

    atomic_rename_file(&tmp_path, state_path)?;
    Ok(())
}

/// Loads and validates a `.tenun` state file from disk.
pub fn load_state(state_path: &Path) -> Result<DownloadState> {
    if !state_path.exists() {
        return Err(TenunError::CorruptState(format!(
            "State file '{}' does not exist",
            state_path.display()
        )));
    }

    let contents = std::fs::read_to_string(state_path).map_err(TenunError::Io)?;
    let state: DownloadState = serde_json::from_str(&contents)
        .map_err(|e| TenunError::CorruptState(format!("Invalid state JSON: {e}")))?;

    if state.version != STATE_SCHEMA_VERSION {
        return Err(TenunError::CorruptState(format!(
            "Unsupported state schema version: {} (expected {})",
            state.version, STATE_SCHEMA_VERSION
        )));
    }

    if state.total_bytes == 0 || state.segments.is_empty() {
        return Err(TenunError::CorruptState(
            "State file contains empty payload or zero segments".to_string(),
        ));
    }

    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::calculate_segments;
    use tempfile::tempdir;

    #[test]
    fn test_atomic_save_and_load_state() {
        let temp = tempdir().expect("create temp dir");
        let state_path = temp.path().join("sample.iso.tenun");

        let segs = calculate_segments(10_000_000, 4, 1024 * 1024);
        let opts = RequestOptions::default();

        let info = RemoteInfo {
            original_url: "https://example.com/sample.iso".to_string(),
            final_url: "https://example.com/sample.iso".to_string(),
            filename: "sample.iso".to_string(),
            total_bytes: Some(10_000_000),
            range_supported: true,
            etag: Some("\"etag-12345\"".to_string()),
            last_modified: Some("Mon, 05 Oct 2026 12:00:00 GMT".to_string()),
            content_type: None,
        };

        let state = DownloadState::from_remote_info(&info, "sample.iso", &segs, &opts);

        save_state_atomic(&state_path, &state).expect("save state");
        assert!(state_path.exists());

        let loaded = load_state(&state_path).expect("load state");
        assert_eq!(loaded, state);
        assert_eq!(loaded.segments.len(), 4);
        assert_eq!(loaded.total_bytes, 10_000_000);
        assert_eq!(loaded.etag.as_deref(), Some("\"etag-12345\""));
    }

    #[test]
    fn test_persisted_options_excludes_sensitive_headers() {
        let mut opts = RequestOptions::default();
        opts.custom_headers.push((
            "Authorization".to_string(),
            "Bearer secret_token".to_string(),
        ));
        opts.custom_headers
            .push(("Cookie".to_string(), "session=abc123secret".to_string()));
        opts.custom_headers
            .push(("X-Custom-Client".to_string(), "TenunTest".to_string()));

        let persisted = PersistedOptions::from_request_options(&opts, 4);
        assert_eq!(persisted.custom_headers.len(), 1);
        assert_eq!(persisted.custom_headers[0].0, "X-Custom-Client");
    }
}
