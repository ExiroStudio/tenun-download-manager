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
