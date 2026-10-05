//! Application directories and configuration paths.

use crate::error::{Result, TenunError};
use directories::{ProjectDirs, UserDirs};
use std::path::{Path, PathBuf};

const QUALIFIER: &str = "org";
const ORGANIZATION: &str = "Tenun";
const APPLICATION: &str = "Tenun";
const PORTABLE_MARKER: &str = "tenun.portable";

/// Resolves standard directories across Linux and Windows,
/// with automatic fallback to portable mode if `tenun.portable` is present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppDirectories {
    config_dir: PathBuf,
    data_dir: PathBuf,
    cache_dir: PathBuf,
    download_dir: PathBuf,
    is_portable: bool,
}

impl AppDirectories {
    /// Detects application directories according to standard OS conventions
    /// or adjacent portable mode.
    pub fn detect() -> Result<Self> {
        let exe_path = std::env::current_exe().ok();
        let exe_dir = exe_path.as_deref().and_then(Path::parent);
        Self::detect_with_exe_dir(exe_dir)
    }

    /// Internal resolver that accepts an optional executable directory for testability.
    pub fn detect_with_exe_dir(exe_dir: Option<&Path>) -> Result<Self> {
        if let Some(dir) = exe_dir {
            let marker = dir.join(PORTABLE_MARKER);
            if marker.exists() {
                let portable_root = dir.join("tenun_data");
                return Ok(Self {
                    config_dir: portable_root.join("config"),
                    data_dir: portable_root.join("data"),
                    cache_dir: portable_root.join("cache"),
                    download_dir: portable_root.join("downloads"),
                    is_portable: true,
                });
            }
        }

        let proj_dirs = ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
            .ok_or_else(|| TenunError::Config("Failed to determine project directories".into()))?;

        let user_dirs = UserDirs::new();
        let download_dir = user_dirs
            .and_then(|u| u.download_dir().map(Path::to_path_buf))
            .unwrap_or_else(|| proj_dirs.data_local_dir().join("downloads"));

        Ok(Self {
            config_dir: proj_dirs.config_dir().to_path_buf(),
            data_dir: proj_dirs.data_local_dir().to_path_buf(),
            cache_dir: proj_dirs.cache_dir().to_path_buf(),
            download_dir,
            is_portable: false,
        })
    }

    /// Returns the configuration directory path.
    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    /// Returns the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the cache directory path.
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Returns the default user download directory path.
    pub fn download_dir(&self) -> &Path {
        &self.download_dir
    }

    /// Returns true if running in portable mode.
    pub fn is_portable(&self) -> bool {
        self.is_portable
    }

    /// Ensures that all base application directories exist.
    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.config_dir)?;
        std::fs::create_dir_all(&self.data_dir)?;
        std::fs::create_dir_all(&self.cache_dir)?;
        std::fs::create_dir_all(&self.download_dir)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_detect_standard_directories() {
        let dirs = AppDirectories::detect_with_exe_dir(None).expect("standard dirs detected");
        assert!(!dirs.is_portable());
        assert!(!dirs.config_dir().as_os_str().is_empty());
        assert!(!dirs.data_dir().as_os_str().is_empty());
        assert!(!dirs.download_dir().as_os_str().is_empty());
    }

    #[test]
    fn test_portable_mode_detection() {
        let temp = tempdir().expect("temp dir created");
        let marker = temp.path().join(PORTABLE_MARKER);
        std::fs::write(&marker, b"").expect("marker written");

        let dirs =
            AppDirectories::detect_with_exe_dir(Some(temp.path())).expect("portable dirs detected");
        assert!(dirs.is_portable());
        assert_eq!(
            dirs.config_dir(),
            temp.path().join("tenun_data").join("config")
        );
        assert_eq!(dirs.data_dir(), temp.path().join("tenun_data").join("data"));
    }
}
