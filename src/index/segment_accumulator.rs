// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

/// Shared state accumulated across all documents in a segment.
///
/// Owned by the [`SegmentWorker`](crate::newindex::segment_worker::SegmentWorker)
/// and passed as `&mut` to each [`FieldConsumer`](crate::newindex::consumer::FieldConsumer)
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
///    and another consumes. For example, norms computed by
///    [`NormsConsumer`](crate::newindex::norms_consumer::NormsConsumer) are stored
///    here so that the postings consumer can build `NormsLookup` at flush
///    time for competitive impact encoding.
#[derive(Debug, Default, mem_dbg::MemSize)]
pub struct SegmentAccumulator {
    /// field_id → per-field norms data
    norms: HashMap<u32, PerFieldNormsData>,
    /// Total documents processed in this segment.
    doc_count: i32,
}

/// Per-field norms accumulated during document processing.
///
/// Written by `NormsConsumer` in `finish_field`, read by both
/// `NormsConsumer` (at flush for .nvm/.nvd writing) and
/// `PostingsConsumer` (at flush for competitive impact encoding).
#[derive(Debug, mem_dbg::MemSize)]
pub struct PerFieldNormsData {
    /// Field name (for debug logging and codec writing).
    pub field_name: String,
    /// Doc IDs that have a norm value for this field.
    pub docs: Vec<i32>,
    /// Norm values, parallel with `docs`.
    pub values: Vec<i64>,
}

impl SegmentAccumulator {
    /// Creates an empty accumulator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a norm value for a field in a document.
    ///
    /// Called by `NormsConsumer::finish_field` after computing the norm
    /// from the token count.
    pub fn record_norm(&mut self, field_id: u32, field_name: &str, doc_id: i32, norm: i64) {
        let entry = self
            .norms
            .entry(field_id)
            .or_insert_with(|| PerFieldNormsData {
                field_name: field_name.to_string(),
                docs: Vec::new(),
                values: Vec::new(),
            });
        entry.docs.push(doc_id);
        entry.values.push(norm);
    }

    /// Returns the accumulated norms data for all fields.
    pub fn norms(&self) -> &HashMap<u32, PerFieldNormsData> {
        &self.norms
    }

    /// Increments the document count by one.
    pub fn increment_doc_count(&mut self) {
        self.doc_count += 1;
    }

    /// Returns the total number of documents processed.
    pub fn doc_count(&self) -> i32 {
        self.doc_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;
    use mem_dbg::{MemSize, SizeFlags};

    #[test]
    fn mem_size_empty_is_small() {
        let acc = SegmentAccumulator::new();
        assert_lt!(acc.mem_size(SizeFlags::CAPACITY), 200);
    }

    #[test]
    fn mem_size_grows_with_norms() {
        let mut acc = SegmentAccumulator::new();
        for doc_id in 0..100 {
            acc.record_norm(0, "body", doc_id, 42);
        }
        assert_gt!(acc.mem_size(SizeFlags::CAPACITY), 0);
    }
}
