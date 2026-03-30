// SPDX-License-Identifier: Apache-2.0

/// Shared state accumulated across all documents in a segment.
///
/// Owned by the [`SegmentWorker`](super::segment_worker::SegmentWorker)
/// and passed as `&mut` to each [`FieldConsumer`](super::consumer::FieldConsumer)
/// in sequence. Only one consumer borrows at a time — no concurrent
/// access within a worker thread.
///
/// This struct serves two purposes:
///
/// 1. **Shared data pools** — memory pools (byte blocks, int blocks, etc.)
///    that multiple consumers read from and write to. For example, postings
///    and term vectors may share a byte pool for term storage.
///
/// 2. **Cross-consumer metadata** — information that one consumer produces
///    and another consumes. For example, a consumer that discovers field
///    properties during document processing (like the presence of payloads)
///    records that here so the field infos consumer can include it when
///    writing the segment's field metadata.
#[derive(Debug, Default)]
pub struct SegmentAccumulator {
    // TODO: ByteBlockPool, IntBlockPool, field metadata, etc.
}

impl SegmentAccumulator {
    /// Creates an empty accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}
