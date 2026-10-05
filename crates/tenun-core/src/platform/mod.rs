//! Platform abstraction module for cross-platform file I/O and OS integration.

use crate::error::{Result, TenunError};
use std::fs::File;
use std::io;
use std::path::Path;
use std::time::Duration;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

/// Trait providing positional writing to an open file without altering its shared cursor.
pub trait PositionalWrite {
    /// Writes a buffer at the specified byte offset.
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize>;

    /// Repeatedly writes bytes from `buf` at `offset` until the entire buffer is written.
    fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match self.write_at(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer at offset",
                    ))
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
pub use unix::available_disk_space;
#[cfg(windows)]
pub use windows::available_disk_space;

/// Checks if the partition containing `path` has at least `required_bytes` free space.
/// Returns `Ok(())` if sufficient or if space could not be strictly determined,
/// and `Err(TenunError::InsufficientDiskSpace)` if available space is known to be less than required.
pub fn ensure_disk_space(path: &Path, required_bytes: u64) -> Result<()> {
    if required_bytes == 0 {
        return Ok(());
    }
    // Path might not exist yet, find the closest existing ancestor directory
    let mut check_path = path;
    while !check_path.exists() {
        match check_path.parent() {
            Some(parent) => check_path = parent,
            None => break,
        }
    }
    match available_disk_space(check_path) {
        Ok(available) => {
            if available < required_bytes {
                return Err(TenunError::InsufficientDiskSpace {
                    required: required_bytes,
                    available,
                });
            }
            Ok(())
        }
        Err(e) => {
            tracing::warn!(
                "Could not determine available disk space for {}: {}",
                check_path.display(),
                e
            );
            Ok(())
        }
    }
}

/// Flushes data and metadata of a file to disk (`fsync`).
pub fn sync_file(file: &File) -> io::Result<()> {
    file.sync_data()
}

/// Atomically renames a file from `from` to `to`.
///
/// On Windows, open handles or transient antivirus file locks may cause `rename`
/// to temporarily fail. This function retries briefly before returning an error.
pub fn atomic_rename_file(from: &Path, to: &Path) -> Result<()> {
    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 10;
    const RETRY_DELAY: Duration = Duration::from_millis(50);

    loop {
        match std::fs::rename(from, to) {
            Ok(()) => return Ok(()),
            Err(e) => {
                attempts += 1;
                if attempts >= MAX_ATTEMPTS {
                    return Err(TenunError::Io(e));
                }
                std::thread::sleep(RETRY_DELAY);
            }
        }
    }
}

/// Sanitizes an untrusted filename across Linux and Windows platforms.
///
/// Handles:
/// - Forbidden characters: `< > : " / \ | ? *` and ASCII control chars (0x00..=0x1f).
/// - Windows reserved device names (CON, PRN, AUX, NUL, COM1..9, LPT1..9).
/// - Trailing dots and spaces disallowed by Windows.
/// - Maximum filename length restriction (255 chars).
/// - Empty filename fallback to "download".
pub fn sanitize_filename(name: &str) -> String {
    // 1. Filter out control characters and illegal filesystem characters.
    let mut sanitized: String = name
        .chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            c if (c as u32) < 32 => '_',
            c => c,
        })
        .collect();

    // 2. Remove leading/trailing whitespaces and trailing dots.
    sanitized = sanitized.trim().trim_end_matches('.').trim().to_string();

    // 3. Fallback if empty.
    if sanitized.is_empty() {
        sanitized = "download".to_string();
    }

    // 4. Check for Windows reserved names.
    let stem = match sanitized.split_once('.') {
        Some((prefix, _)) => prefix,
        None => &sanitized,
    };

    let reserved = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];

    if reserved.iter().any(|r| stem.eq_ignore_ascii_case(r)) {
        sanitized = format!("_{sanitized}");
    }

    // 5. Truncate to maximum 255 characters safely at UTF-8 char boundary.
    if sanitized.len() > 255 {
        let mut boundary = 255;
        while !sanitized.is_char_boundary(boundary) {
            boundary -= 1;
        }
        sanitized.truncate(boundary);
    }

    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::NamedTempFile;

    #[test]
    fn test_positional_write() {
        let mut file = NamedTempFile::new().expect("temp file created");
        let handle = file.reopen().expect("file reopened");

        // Write "world" at offset 6, then "hello " at offset 0
        handle.write_all_at(b"world", 6).expect("write at 6");
        handle.write_all_at(b"hello ", 0).expect("write at 0");

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).expect("read file");
        assert_eq!(&buf[0..11], b"hello world");
    }

    #[test]
    fn test_sanitize_filename_illegal_chars() {
        assert_eq!(
            sanitize_filename("foo:bar?baz*qux.txt"),
            "foo_bar_baz_qux.txt"
        );
        assert_eq!(
            sanitize_filename("hello/world\\file.zip"),
            "hello_world_file.zip"
        );
        assert_eq!(sanitize_filename("a<b|c>d\"e.tar"), "a_b_c_d_e.tar");
    }

    #[test]
    fn test_sanitize_filename_reserved_windows_names() {
        assert_eq!(sanitize_filename("CON.txt"), "_CON.txt");
        assert_eq!(sanitize_filename("aux"), "_aux");
        assert_eq!(sanitize_filename("prn.tar.gz"), "_prn.tar.gz");
        assert_eq!(sanitize_filename("com1.dat"), "_com1.dat");
        assert_eq!(sanitize_filename("Nul"), "_Nul");
    }

    #[test]
    fn test_sanitize_filename_trailing_dots_and_spaces() {
        assert_eq!(sanitize_filename("file...   "), "file");
        assert_eq!(sanitize_filename("  test.pdf. "), "test.pdf");
        assert_eq!(sanitize_filename("..."), "download");
    }

    #[test]
    fn test_sanitize_filename_empty_fallback() {
        assert_eq!(sanitize_filename(""), "download");
        assert_eq!(sanitize_filename("   "), "download");
    }

    #[test]
    fn test_sanitize_filename_length_boundary() {
        let long_name = "a".repeat(300);
        let sanitized = sanitize_filename(&long_name);
        assert_eq!(sanitized.len(), 255);
    }

    #[test]
    fn test_disk_space_checks() {
        let temp = tempfile::tempdir().expect("temp dir");
        let free = available_disk_space(temp.path()).expect("available space");
        assert!(free > 0, "available disk space should be positive");

        // Small requirement should pass
        assert!(ensure_disk_space(temp.path(), 1024).is_ok());

        // Absurd requirement (100 Petabytes) should fail
        let huge = 100 * 1024 * 1024 * 1024 * 1024 * 1024;
        let res = ensure_disk_space(temp.path(), huge);
        match res {
            Err(TenunError::InsufficientDiskSpace {
                required,
                available,
            }) => {
                assert_eq!(required, huge);
                assert_eq!(available, free);
            }
            _ => panic!("Expected InsufficientDiskSpace error"),
        }
    }
}
