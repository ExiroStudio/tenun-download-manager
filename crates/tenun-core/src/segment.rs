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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// A thread-safe dynamic segment supporting progress reporting, synchronized boundary
/// adjustments, and dynamic re-splitting (work stealing).
#[derive(Debug)]
pub struct DynamicSegment {
    /// Unique identifier for this segment.
    pub id: usize,
    /// Starting byte offset (inclusive).
    pub start: u64,
    /// Atomic counter for downloaded bytes (for lockless progress polling).
    pub downloaded_atomic: Arc<AtomicU64>,
    /// Atomic counter for current end boundary (inclusive).
    pub end_atomic: Arc<AtomicU64>,
    /// Flag indicating whether a worker is actively processing this segment.
    pub active: Arc<AtomicBool>,
    /// Synchronized inner state for boundary updates and write chunk reservations.
    inner: Arc<Mutex<SegmentInner>>,
}

#[derive(Debug)]
struct SegmentInner {
    end: u64,
    downloaded: u64,
    in_flight: u64,
}

impl DynamicSegment {
    /// Creates a new `DynamicSegment` from explicit parameters.
    pub fn new(id: usize, start: u64, end: u64, downloaded: u64) -> Self {
        Self {
            id,
            start,
            downloaded_atomic: Arc::new(AtomicU64::new(downloaded)),
            end_atomic: Arc::new(AtomicU64::new(end)),
            active: Arc::new(AtomicBool::new(false)),
            inner: Arc::new(Mutex::new(SegmentInner {
                end,
                downloaded,
                in_flight: 0,
            })),
        }
    }

    /// Converts a static [`Segment`] into a [`DynamicSegment`].
    pub fn from_segment(seg: &Segment) -> Self {
        Self::new(seg.id, seg.start, seg.end, seg.downloaded)
    }

    /// Converts the current state of this dynamic segment into a static [`Segment`].
    pub fn to_segment(&self) -> Segment {
        let inner = self.inner.lock().expect("lock poisoned");
        Segment {
            id: self.id,
            start: self.start,
            end: inner.end,
            downloaded: inner.downloaded,
        }
    }

    /// Returns the current end boundary offset.
    pub fn current_end(&self) -> u64 {
        self.end_atomic.load(Ordering::SeqCst)
    }

    /// Returns the current bytes downloaded.
    pub fn current_downloaded(&self) -> u64 {
        self.downloaded_atomic.load(Ordering::SeqCst)
    }

    /// Returns the current write offset on disk (`start + downloaded`).
    pub fn current_offset(&self) -> u64 {
        self.start + self.current_downloaded()
    }

    /// Remaining bytes yet to be downloaded or in flight.
    pub fn remaining(&self) -> u64 {
        let inner = self.inner.lock().expect("lock poisoned");
        let active_end = self.start + inner.downloaded + inner.in_flight;
        if active_end > inner.end {
            0
        } else {
            inner.end - active_end + 1
        }
    }

    /// Reserves a slice of bytes to write.
    /// Returns `(write_offset, bytes_to_write, reached_end)`.
    pub fn reserve_chunk(&self, chunk_len: usize) -> (u64, usize, bool) {
        let mut inner = self.inner.lock().expect("lock poisoned");
        let cur_offset = self.start + inner.downloaded + inner.in_flight;
        if cur_offset > inner.end {
            return (cur_offset, 0, true);
        }

        let remaining = (inner.end - cur_offset + 1) as usize;
        let to_write = chunk_len.min(remaining);
        inner.in_flight += to_write as u64;
        let reached_end = cur_offset + (to_write as u64) > inner.end;
        (cur_offset, to_write, reached_end)
    }

    /// Commits a previously reserved chunk after successful write to disk.
    pub fn commit_chunk(&self, bytes_written: usize) {
        let mut inner = self.inner.lock().expect("lock poisoned");
        inner.downloaded += bytes_written as u64;
        inner.in_flight = inner.in_flight.saturating_sub(bytes_written as u64);
        self.downloaded_atomic
            .store(inner.downloaded, Ordering::SeqCst);
    }

    /// Rolls back an in-flight chunk reservation (e.g. on network failure before disk write).
    pub fn rollback_chunk(&self, bytes: usize) {
        let mut inner = self.inner.lock().expect("lock poisoned");
        inner.in_flight = inner.in_flight.saturating_sub(bytes as u64);
    }

    /// Attempts to split the unfinished tail of this segment into a new dynamic segment.
    ///
    /// If remaining bytes >= `2 * min_split_size`, this segment's end boundary is atomically
    /// shrunk to `mid - 1`, and a new dynamic segment covering `[mid, old_end]` is returned.
    pub fn try_split(&self, new_id: usize, min_split_size: u64) -> Option<DynamicSegment> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        let active_offset = self.start + inner.downloaded + inner.in_flight;
        if inner.end <= active_offset {
            return None;
        }

        let remaining = inner.end - active_offset + 1;
        if remaining < 2 * min_split_size {
            return None;
        }

        let mid = active_offset + remaining / 2;
        let old_end = inner.end;
        inner.end = mid - 1;
        self.end_atomic.store(mid - 1, Ordering::SeqCst);

        Some(DynamicSegment::new(new_id, mid, old_end, 0))
    }
}

/// Coordinates dynamic segments and facilitates work stealing for idle workers.
#[derive(Debug, Clone)]
pub struct SegmentManager {
    segments: Arc<RwLock<Vec<Arc<DynamicSegment>>>>,
    next_id: Arc<AtomicU64>,
}

impl SegmentManager {
    /// Creates a manager from static segments.
    pub fn new(initial: &[Segment]) -> Self {
        let dyn_segs: Vec<Arc<DynamicSegment>> = initial
            .iter()
            .map(|s| Arc::new(DynamicSegment::from_segment(s)))
            .collect();
        let max_id = initial.iter().map(|s| s.id).max().unwrap_or(0);

        Self {
            segments: Arc::new(RwLock::new(dyn_segs)),
            next_id: Arc::new(AtomicU64::new((max_id + 1) as u64)),
        }
    }

    /// Returns a list of current dynamic segments.
    pub fn segments(&self) -> Vec<Arc<DynamicSegment>> {
        self.segments.read().expect("lock poisoned").clone()
    }

    /// Produces a snapshot of all segments in their current state.
    pub fn snapshot(&self) -> Vec<Segment> {
        let segs = self.segments.read().expect("lock poisoned");
        segs.iter().map(|s| s.to_segment()).collect()
    }

    /// Finds the active segment with the largest remaining bytes and splits it.
    /// Returns the newly created segment if splitting succeeded.
    pub fn find_and_split_largest(&self, min_split_size: u64) -> Option<Arc<DynamicSegment>> {
        let segs = self.segments();

        // Find candidate with largest remaining bytes
        let mut candidate: Option<(Arc<DynamicSegment>, u64)> = None;
        for seg in &segs {
            let rem = seg.remaining();
            if rem >= 2 * min_split_size {
                match candidate {
                    Some((_, max_rem)) if rem <= max_rem => {}
                    _ => candidate = Some((Arc::clone(seg), rem)),
                }
            }
        }

        let (target_seg, _) = candidate?;
        let new_id = self.next_id.fetch_add(1, Ordering::SeqCst) as usize;

        if let Some(new_seg) = target_seg.try_split(new_id, min_split_size) {
            let new_seg_arc = Arc::new(new_seg);
            let mut list = self.segments.write().expect("lock poisoned");
            list.push(Arc::clone(&new_seg_arc));
            Some(new_seg_arc)
        } else {
            None
        }
    }
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

    #[test]
    fn test_dynamic_segment_split_success() {
        let seg = Segment {
            id: 0,
            start: 0,
            end: 999_999,
            downloaded: 0,
        };
        let dyn_seg = DynamicSegment::from_segment(&seg);
        assert_eq!(dyn_seg.remaining(), 1_000_000);

        // Split with min_size = 200_000
        let new_seg = dyn_seg
            .try_split(1, 200_000)
            .expect("should split successfully");

        // dyn_seg end should be shrank to 499_999
        assert_eq!(dyn_seg.current_end(), 499_999);
        assert_eq!(dyn_seg.start, 0);

        // new_seg should be 500_000..999_999
        assert_eq!(new_seg.id, 1);
        assert_eq!(new_seg.start, 500_000);
        assert_eq!(new_seg.current_end(), 999_999);
        assert_eq!(new_seg.current_downloaded(), 0);

        // Gapless invariant
        assert_eq!(dyn_seg.current_end() + 1, new_seg.start);
    }

    #[test]
    fn test_dynamic_segment_split_with_in_flight() {
        let seg = Segment {
            id: 0,
            start: 0,
            end: 999_999,
            downloaded: 100_000,
        };
        let dyn_seg = DynamicSegment::from_segment(&seg);

        // Reserve 50,000 bytes in flight
        let (offset, len, is_end) = dyn_seg.reserve_chunk(50_000);
        assert_eq!(offset, 100_000);
        assert_eq!(len, 50_000);
        assert!(!is_end);

        // Active offset is now 150_000. Remaining: 850,000.
        // Mid = 150_000 + 425_000 = 575_000.
        let new_seg = dyn_seg
            .try_split(1, 100_000)
            .expect("should split with in-flight");
        assert_eq!(dyn_seg.current_end(), 574_999);
        assert_eq!(new_seg.start, 575_000);
        assert_eq!(new_seg.current_end(), 999_999);

        // Commit in-flight chunk
        dyn_seg.commit_chunk(50_000);
        assert_eq!(dyn_seg.current_downloaded(), 150_000);
    }

    #[test]
    fn test_segment_manager_work_stealing() {
        let initial = calculate_segments(10_000_000, 2, 1_000_000);
        let manager = SegmentManager::new(&initial);

        // Segment 0 downloads all its bytes
        let seg0 = &manager.segments()[0];
        let total0 = seg0.current_end() - seg0.start + 1;
        let (_, len, _) = seg0.reserve_chunk(total0 as usize);
        seg0.commit_chunk(len);

        // Steal work from remaining segment
        let stolen = manager
            .find_and_split_largest(500_000)
            .expect("should steal work");
        assert!(stolen.start > 0);
        assert!(stolen.current_end() == 9_999_999);

        // Verify total coverage of all snapshot segments
        let snap = manager.snapshot();
        assert_eq!(snap.len(), 3);
        let mut total_coverage = 0u64;
        for s in &snap {
            total_coverage += s.total_bytes();
        }
        assert_eq!(total_coverage, 10_000_000);
    }
}
