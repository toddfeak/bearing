// SPDX-License-Identifier: Apache-2.0

use std::io;

use crate::newindex::directory::Directory;
use crate::newindex::segment::FlushedSegment;

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
    /// The file is written to a temp name and atomically renamed.
    pub fn commit(&mut self, _directory: &dyn Directory) -> io::Result<String> {
        self.generation += 1;
        // let file_name = format!("segments_{}", self.generation);
        // TODO: write segment list to temp file, sync, rename to file_name
        todo!()
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
