// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::sync::Arc;

use crate::newindex::codecs::segment_infos;
use crate::newindex::segment::FlushedSegment;
use crate::store::SharedDirectory;

/// Collects flushed segments and writes the `segments_N` commit point.
///
/// Holds the list of segments that make up the index and the generation
/// counter that increments on each commit. Writing is atomic: the file
/// is written to a temp name and renamed.
// LOCKED
#[derive(Default)]
pub struct SegmentInfos {
    /// Flushed segments to include in the next commit.
    segments: Vec<FlushedSegment>,
    /// Generation counter — the `N` in `segments_N`.
    generation: u64,
}

impl SegmentInfos {
    /// Creates an empty instance at generation 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a flushed segment to be included in the next commit.
    pub fn add(&mut self, segment: FlushedSegment) {
        self.segments.push(segment);
    }

    /// Writes `segments_N` to the directory and increments the generation.
    ///
    /// The file is written to a pending name, synced, and atomically renamed.
    pub fn commit(&mut self, directory: &Arc<SharedDirectory>) -> io::Result<String> {
        self.generation += 1;
        segment_infos::write(directory, &self.segments, self.generation)
    }

    /// Returns the current generation number.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the segments in this commit.
    pub fn segments(&self) -> &[FlushedSegment] {
        &self.segments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::newindex::segment::SegmentId;
    use crate::store::MemoryDirectory;

    fn test_directory() -> Arc<SharedDirectory> {
        Arc::new(SharedDirectory::new(Box::new(MemoryDirectory::new())))
    }

    fn make_segment(name: &str, doc_count: i32) -> FlushedSegment {
        FlushedSegment {
            segment_id: SegmentId {
                name: name.to_string(),
                id: [0xABu8; 16],
            },
            doc_count,
            file_names: vec![format!("{name}.fdt")],
        }
    }

    #[test]
    fn commit_writes_segments_file() {
        let dir = test_directory();
        let mut si = SegmentInfos::new();
        si.add(make_segment("_0", 3));

        let name = si.commit(&dir).unwrap();
        assert_eq!(name, "segments_1");
        assert_eq!(si.generation(), 1);

        let data = dir.lock().unwrap().read_file(&name).unwrap();
        // Header magic
        assert_eq!(&data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);
    }

    #[test]
    fn commit_increments_generation() {
        let dir = test_directory();
        let mut si = SegmentInfos::new();
        si.add(make_segment("_0", 1));

        si.commit(&dir).unwrap();
        assert_eq!(si.generation(), 1);

        si.add(make_segment("_1", 2));
        let name = si.commit(&dir).unwrap();
        assert_eq!(name, "segments_2");
        assert_eq!(si.generation(), 2);
    }
}
