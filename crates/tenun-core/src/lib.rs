//! # Tenun Core
//!
//! `tenun-core` is the core download engine for the Tenun Download Manager.
//! It provides robust, resilient, and multi-platform downloading capabilities
//! including probing, segmented downloads, resuming, queuing, and data integrity verification.

pub mod config;
pub mod download;
pub mod error;
pub mod logging;
pub mod platform;
pub mod probe;
pub mod retry;
pub mod segment;
pub mod state;

pub use config::AppDirectories;
pub use download::{
    download_segmented, download_segmented_with_cancel, download_single, resolve_unique_filename,
    resume_download, DownloadEvent,
};
pub use error::{Result, TenunError};
pub use logging::init_logging;
pub use platform::{atomic_rename_file, sanitize_filename, sync_file, PositionalWrite};
pub use probe::{apply_options, build_client, probe, RemoteInfo, RequestOptions};
pub use retry::{
    classify_reqwest_error, classify_response, parse_retry_after, parse_retry_after_str, ErrorKind,
    RetryPolicy,
};
pub use segment::{
    calculate_segments, Segment, DEFAULT_CONNECTIONS, DEFAULT_MIN_SEGMENT_SIZE, MAX_CONNECTIONS,
};
pub use state::{load_state, save_state_atomic, DownloadState, SegmentState, STATE_SCHEMA_VERSION};
pub use tokio_util::sync::CancellationToken;
