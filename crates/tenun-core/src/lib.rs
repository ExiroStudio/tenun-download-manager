//! # Tenun Core
//!
//! `tenun-core` is the core download engine for the Tenun Download Manager.
//! It provides robust, resilient, and multi-platform downloading capabilities
//! including probing, segmented downloads, resuming, queuing, and data integrity verification.

pub mod config;
pub mod error;
pub mod logging;
pub mod platform;

pub use config::AppDirectories;
pub use error::{Result, TenunError};
pub use logging::init_logging;
pub use platform::{atomic_rename_file, sanitize_filename, sync_file, PositionalWrite};
