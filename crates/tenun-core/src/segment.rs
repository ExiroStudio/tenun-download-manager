//! Download segmentation logic and byte range partitioning.

use serde::{Deserialize, Serialize};

/// Default minimum segment size: 1 MiB (1,048,576 bytes).
pub const DEFAULT_MIN_SEGMENT_SIZE: u64 = 1024 * 1024;
/// Default number of parallel connections.
pub const DEFAULT_CONNECTIONS: usize = 8;
/// Maximum allowed parallel connections.
pub const MAX_CONNECTIONS: usize = 32;

/// A contiguous slice of bytes assigned to a worker connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    /// Unique identifier for this segment (0-indexed).
    pub id: usize,
    /// Starting byte offset (inclusive).
    pub start: u64,
    /// Ending byte offset (inclusive).
    pub end: u64,
    /// Number of bytes successfully downloaded and written to disk so far.
    pub downloaded: u64,
}

impl Segment {
    /// Total expected length of this segment in bytes.
    pub fn total_bytes(&self) -> u64 {
        if self.start > self.end {
            0
        } else {
            self.end - self.start + 1
        }
    }

    /// Remaining bytes to download for this segment.
    pub fn remaining_bytes(&self) -> u64 {
        self.total_bytes().saturating_sub(self.downloaded)
    }

    /// Current read/write offset on disk (`start + downloaded`).
    pub fn current_offset(&self) -> u64 {
        self.start + self.downloaded
    }

    /// Checks if this segment is completely downloaded.
    pub fn is_complete(&self) -> bool {
        self.downloaded >= self.total_bytes()
    }
}

/// Calculates non-overlapping, gapless segments covering the exact interval `[0, total_size)`.
///
/// Number of segments is computed as:
/// `min(clamp(requested_connections, 1, 32), max(1, total_size / min_segment_size))`.
pub fn calculate_segments(
    total_size: u64,
    requested_connections: usize,
    min_segment_size: u64,
) -> Vec<Segment> {
    if total_size == 0 {
        return Vec::new();
    }

    let min_seg = if min_segment_size == 0 {
        1
    } else {
        min_segment_size
    };

    let conn = requested_connections.clamp(1, MAX_CONNECTIONS);
    let max_by_size = total_size / min_seg;
    let num_segments = (conn as u64).min(max_by_size).max(1) as usize;

    let base_chunk = total_size / (num_segments as u64);
    let remainder = total_size % (num_segments as u64);

    let mut segments = Vec::with_capacity(num_segments);
    let mut current_offset: u64 = 0;

    for id in 0..num_segments {
        let chunk_size = base_chunk + if (id as u64) < remainder { 1 } else { 0 };
        let start = current_offset;
        let end = start + chunk_size - 1;
        current_offset = end + 1;

        segments.push(Segment {
            id,
            start,
            end,
            downloaded: 0,
        });
    }

    segments
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_empty_file_segments() {
        let segs = calculate_segments(0, 8, DEFAULT_MIN_SEGMENT_SIZE);
        assert!(segs.is_empty());
    }

    #[test]
    fn test_small_file_single_segment() {
        let size = 500_000; // < 1 MiB
        let segs = calculate_segments(size, 8, DEFAULT_MIN_SEGMENT_SIZE);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].start, 0);
        assert_eq!(segs[0].end, size - 1);
        assert_eq!(segs[0].total_bytes(), size);
    }

    #[test]
    fn test_exact_split() {
        let size = 8 * DEFAULT_MIN_SEGMENT_SIZE;
        let segs = calculate_segments(size, 8, DEFAULT_MIN_SEGMENT_SIZE);
        assert_eq!(segs.len(), 8);
        assert_eq!(segs[0].start, 0);
        assert_eq!(segs[7].end, size - 1);
        for seg in &segs {
            assert_eq!(seg.total_bytes(), DEFAULT_MIN_SEGMENT_SIZE);
        }
    }

    proptest! {
        #[test]
        fn test_segment_invariants(
            total_size in 1u64..100_000_000u64,
            connections in 1usize..64usize,
            min_segment_size in 1u64..10_000_000u64,
        ) {
            let segs = calculate_segments(total_size, connections, min_segment_size);

            prop_assert!(!segs.is_empty(), "Segments must not be empty for size > 0");
            prop_assert!(segs.len() <= MAX_CONNECTIONS, "Segments count must be <= MAX_CONNECTIONS");

            // Invariant 1: First segment starts at 0
            prop_assert_eq!(segs[0].start, 0, "First segment must start at 0");

            // Invariant 2: Last segment ends at total_size - 1
            prop_assert_eq!(segs.last().unwrap().end, total_size - 1, "Last segment must end at total_size - 1");

            let mut accumulated_bytes = 0u64;

            for i in 0..segs.len() {
                // Invariant 3: Valid bounds
                prop_assert!(segs[i].start <= segs[i].end, "Segment start must be <= end");
                prop_assert_eq!(segs[i].id, i, "Segment ID must match index");

                accumulated_bytes += segs[i].total_bytes();

                // Invariant 4: Gapless and non-overlapping contiguous boundaries
                if i + 1 < segs.len() {
                    prop_assert_eq!(
                        segs[i].end + 1,
                        segs[i + 1].start,
                        "Contiguous segments must have no gap and no overlap at index {}",
                        i
                    );
                }
            }

            // Invariant 5: Exact coverage of total_size
            prop_assert_eq!(accumulated_bytes, total_size, "Sum of segment lengths must equal total_size");
        }
    }
}
