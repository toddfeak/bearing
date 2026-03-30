// SPDX-License-Identifier: Apache-2.0

/// Shared accumulation space for field consumers during indexing.
///
/// Owned by the [`SegmentWorker`](super::segment_worker::SegmentWorker)
/// and passed as `&mut` to each [`FieldConsumer`](super::consumer::FieldConsumer)
/// in sequence. Only one consumer borrows the pools at a time —
/// no concurrent access within a worker thread.
///
/// Consumers that need to share data (e.g., postings and term vectors
/// sharing a byte pool) do so through fields on this struct.
#[derive(Debug, Default)]
pub struct Pools {
    // TODO: ByteBlockPool, IntBlockPool, etc.
}

impl Pools {
    /// Creates an empty pool set.
    pub fn new() -> Self {
        Self::default()
    }
}
