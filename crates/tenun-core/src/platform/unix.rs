//! Unix-specific platform implementations.

use super::PositionalWrite;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

impl PositionalWrite for File {
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        FileExt::write_at(self, buf, offset)
    }
}

/// Computes available free disk space (in bytes) on the partition containing `path`.
pub fn available_disk_space(path: &std::path::Path) -> io::Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
            let free = (stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64);
            Ok(free)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}
