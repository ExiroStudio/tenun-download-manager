//! Windows-specific platform implementations.

use super::PositionalWrite;
use std::fs::File;
use std::io;
use std::os::windows::fs::FileExt;

impl PositionalWrite for File {
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        FileExt::seek_write(self, buf, offset)
    }
}

/// Computes available free disk space (in bytes) on the partition containing `path`.
pub fn available_disk_space(path: &std::path::Path) -> io::Result<u64> {
    use std::os::windows::ffi::OsStrExt;

    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();

    extern "system" {
        fn GetDiskFreeSpaceExW(
            lpDirectoryName: *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes: *mut u64,
            lpTotalNumberOfFreeBytes: *mut u64,
        ) -> i32;
    }

    let mut free_available: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_free: u64 = 0;

    let res = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut free_available,
            &mut total_bytes,
            &mut total_free,
        )
    };

    if res != 0 {
        Ok(free_available)
    } else {
        Err(io::Error::last_os_error())
    }
}
