// SPDX-License-Identifier: Apache-2.0

use std::io;

use crate::newindex::analyzer::Analyzer;
use crate::newindex::consumer::FieldConsumer;

use crate::newindex::document::Document;
use crate::newindex::field_info_registry::FieldInfoRegistry;
use crate::newindex::segment::{FlushedSegment, SegmentId};

/// Per-thread worker that accumulates documents into a single segment.
///
/// Owns all per-segment state. Processes documents sequentially — no
/// concurrency within a worker. Shared resources (pools, codec writers)
/// are passed as `&mut` to each processing step in sequence.
///
/// A worker is disposable — it is consumed by `flush()` and the
/// coordinator creates a fresh one for the next segment.
///
/// # Lifecycle
///
/// ## Construction
/// - Created by the coordinator with a segment name/ID and directory access.
/// - Allocates empty pools and a fresh FieldInfoRegistry.
/// - Creates the set of field consumers (postings, stored fields, norms, etc.).
/// - No files are opened yet.
///
/// ## Per-document processing (`add_document`)
///
/// ### 1. Start document
/// - Call `start_document(doc_id)` on each field consumer.
/// - Some field consumers (stored fields) may need the doc ID.
/// - No files opened here unless this is the first document in the segment.
///
/// ### 2. Field iteration
/// - Iterate the document's fields sequentially.
/// - For each field:
///     - Register the field in FieldInfoRegistry (get_or_register).
///     - Call `add_field(field)` on each field consumer. The consumer decides
///       whether this field is relevant and processes it or ignores it.
///     - If the field is tokenized: ask each field consumer `wants_tokens(field)`
///       to build a filtered list. Run the analyzer to produce a token
///       stream from the field's value (string or Reader). For each token,
///       call `add_token(field, token)` on only the field consumers that opted in.
///     - Each field consumer borrows &mut pools for the duration of that
///       field, then releases. No overlapping borrows.
///
/// ### 3. Finish document
/// - Call `finish_document(doc_id)` on each field consumer.
/// - Some field consumers write data at this point:
///   - Term vectors: flush per-document TV data to codec, reset TV pools.
///   - Stored fields: finalize the document's stored data.
/// - Update document count.
///
/// ### 4. Check thresholds
/// - After each document, check RAM usage and document count against
///   config thresholds.
/// - If a threshold is exceeded, the worker signals the coordinator
///   that it needs to flush, or the coordinator detects it.
///
/// ## Segment flush (`flush`)
/// - Call `flush()` on each field consumer to write accumulated data to
///   codec files via the directory.
/// - Codec writers borrow &pools (immutable) to read accumulated data.
/// - After writing, report segment metadata back to the coordinator.
/// - The worker is consumed — all state is dropped.
///
/// ## Reset (via disposal)
/// - There is no in-place reset. Flushing consumes the worker: pools,
///   field consumers, the registry, and all accumulated state are dropped.
/// - The index coordinator creates a fresh `SegmentWorker` with new pools,
///   a new `FieldInfoRegistry`, and new field consumer instances.
/// - This avoids the class of bugs where stale state leaks across
///   segments (e.g., pool data from a previous segment corrupting
///   the next one).
///
/// # Ownership summary
///
/// The worker owns:
/// - Byte/int pools (shared across field consumers within a document)
/// - FieldInfoRegistry (per-segment field metadata)
/// - Field consumers (each manages its own per-field accumulators)
///
/// The worker borrows:
/// - Directory (from coordinator, for creating output files)
/// - Config (thresholds for flush decisions)
// LOCKED
pub struct SegmentWorker {
    segment_id: SegmentId,
    registry: FieldInfoRegistry,
    field_consumers: Vec<Box<dyn FieldConsumer>>,
    analyzer: Box<dyn Analyzer>,
    doc_count: i32,
    /// Reusable buffer for token text, avoids per-token allocation.
    token_buf: String,
    // TODO: pools
}

impl SegmentWorker {
    /// Creates a new worker for a specific segment.
    pub fn new(
        segment_id: SegmentId,
        field_consumers: Vec<Box<dyn FieldConsumer>>,
        analyzer: Box<dyn Analyzer>,
    ) -> Self {
        Self {
            segment_id,
            registry: FieldInfoRegistry::new(),
            field_consumers,
            analyzer,
            doc_count: 0,
            token_buf: String::new(),
        }
    }

    /// Processes a single document through the indexing pipeline.
    pub fn add_document(&mut self, doc: Document) -> io::Result<()> {
        let doc_id = self.doc_count;

        // 1. Start document — notify all field consumers
        for consumer in &mut self.field_consumers {
            consumer.start_document(doc_id)?;
        }

        // 2. Field iteration
        for field in doc.fields() {
            let field_id = self
                .registry
                .get_or_register(field.name(), field.field_type())?;

            // 2a. Every consumer sees the field metadata
            for consumer in &mut self.field_consumers {
                consumer.add_field(field_id, field)?;
            }

            // 2b. Tokenized fields: run the analyzer once, stream tokens
            //     to only the field consumers that want them.
            // TODO: check field.field_type().tokenized()
            {
                // Build filtered index list of field consumers that want tokens
                let interested: Vec<usize> = self
                    .field_consumers
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.wants_tokens(field_id, field))
                    .map(|(i, _)| i)
                    .collect();

                if !interested.is_empty() {
                    // TODO: get reader from field value (string or Reader)
                    let mut reader: &[u8] = b"";
                    let mut token_buf = std::mem::take(&mut self.token_buf);

                    self.analyzer.reset();
                    while let Some(token) = self.analyzer.next_token(&mut reader, &mut token_buf)? {
                        for &i in &interested {
                            self.field_consumers[i].add_token(field_id, field, &token)?;
                        }
                    }

                    self.token_buf = token_buf;
                }
            }
        }

        // 3. Finish document — notify all field consumers
        for consumer in &mut self.field_consumers {
            consumer.finish_document(doc_id)?;
        }

        self.doc_count += 1;

        Ok(())
    }

    /// Returns true if this worker has hit a threshold and should flush.
    ///
    /// Called by the thread loop after each document. Doc count is
    /// checked directly. RAM-based flushing is coordinated externally.
    pub fn should_flush(&self, max_buffered_docs: i32) -> bool {
        // Doc count threshold (-1 means disabled)
        if max_buffered_docs > 0 && self.doc_count >= max_buffered_docs {
            return true;
        }

        // RAM-based threshold:
        // - After each document, the worker reports its RAM usage to a
        //   shared AtomicUsize on the coordinator.
        // - The coordinator tracks total RAM across all workers.
        // - If total RAM exceeds the configured limit, the coordinator
        //   signals the worker with the most RAM to flush.
        // - If total RAM exceeds 2x the limit, add_document stalls
        //   (bounded channel backpressure) until flushes bring it down.
        // - RAM measurement uses mem_dbg on consumer accumulators.
        // TODO: implement RAM-based flush signaling

        false
    }

    /// Returns the estimated RAM bytes used by this worker's accumulators.
    pub fn ram_bytes_used(&self) -> usize {
        // TODO: sum across field consumers and pools
        0
    }

    /// Flushes all accumulated data as a segment to the directory.
    /// Consumes the worker — the coordinator creates a new one for
    /// the next segment.
    pub fn flush(mut self) -> io::Result<FlushedSegment> {
        let mut file_names = Vec::new();
        for consumer in &mut self.field_consumers {
            file_names.extend(consumer.flush()?);
        }
        Ok(FlushedSegment {
            segment_id: self.segment_id,
            doc_count: self.doc_count,
            file_names,
        })
    }
}
