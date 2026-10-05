//! Error definitions for Tenun Core.

use thiserror::Error;

/// The primary error type for Tenun Core operations.
#[derive(Debug, Error)]
pub enum TenunError {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A configuration or directory resolution error occurred.
    #[error("Configuration error: {0}")]
    Config(String),

    /// A platform-specific operation failed.
    #[error("Platform error: {0}")]
    Platform(String),

    /// Network error occurred during operation.
    #[error("Network error: {0}")]
    Network(String),

    /// Remote server does not support HTTP Range requests.
    #[error("HTTP Range is not supported by the remote server")]
    RangeNotSupported,

    /// An invalid byte range was encountered.
    #[error("Invalid byte range: {0}")]
    InvalidRange(String),

    /// Data integrity verification failed.
    #[error("Data integrity check failed: {0}")]
    IntegrityMismatch(String),

    /// File state was corrupted or incompatible.
    #[error("Corrupted state: {0}")]
    CorruptState(String),

    /// Remote file changed on the server (ETag or Last-Modified mismatch), preventing safe resume.
    #[error("Remote file changed on server: {0}")]
    RemoteFileChanged(String),

    /// Download operation was paused via cancellation token.
    #[error("Download was paused")]
    DownloadPaused,
}

/// Convenience alias for Results returning [`TenunError`].
pub type Result<T> = std::result::Result<T, TenunError>;
