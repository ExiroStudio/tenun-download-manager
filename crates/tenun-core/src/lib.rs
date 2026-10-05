//! # Tenun Core
//!
//! `tenun-core` is the core download engine for the Tenun Download Manager.
//! It provides robust, resilient, and multi-platform downloading capabilities
//! including probing, segmented downloads, resuming, queuing, and data integrity verification.

pub mod category;
pub mod checksum;
pub mod config;
pub mod db;
pub mod download;
pub mod error;
pub mod limiter;
pub mod logging;
pub mod platform;
pub mod probe;
pub mod retry;
pub mod segment;
pub mod state;

pub use category::FileCategory;
pub use checksum::{
    compute_file_checksum, extract_checksum_from_headers, parse_checksum_file,
    parse_content_md5_header, parse_digest_header, parse_etag_checksum, verify_file_checksum,
    ChecksumAlgorithm, ClashResolution, ExpectedChecksum,
};
pub use config::AppDirectories;
pub use db::{Database, DownloadRecord, DownloadStatus, NewDownloadRecord};
pub use download::{
    download_segmented, download_segmented_with_cancel, download_single, resolve_destination,
    resolve_unique_filename, resume_download, resume_download_with_options, DownloadEvent,
    ResolvedDestination,
};
pub use error::{Result, TenunError};
pub use limiter::TokenBucketLimiter;
pub use logging::init_logging;
pub use platform::{
    atomic_rename_file, available_disk_space, ensure_disk_space, sanitize_filename, sync_file,
    PositionalWrite,
};
pub use probe::{apply_options, build_client, probe, RemoteInfo, RequestOptions};
pub use retry::{
    classify_reqwest_error, classify_response, parse_retry_after, parse_retry_after_str, ErrorKind,
    RetryPolicy,
};
pub use segment::{
    calculate_segments, DynamicSegment, Segment, SegmentManager, DEFAULT_CONNECTIONS,
    DEFAULT_MIN_SEGMENT_SIZE, MAX_CONNECTIONS,
};
pub use state::{load_state, save_state_atomic, DownloadState, SegmentState, STATE_SCHEMA_VERSION};
pub use tokio_util::sync::CancellationToken;
