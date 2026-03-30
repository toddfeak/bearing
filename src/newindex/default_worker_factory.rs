// SPDX-License-Identifier: Apache-2.0

//! Default [`WorkerFactory`] implementation.

use std::sync::Arc;

use crate::newindex::coordinator::WorkerFactory;
use crate::newindex::field_infos_consumer::FieldInfosConsumer;
use crate::newindex::segment::SegmentId;
use crate::newindex::segment_context::SegmentContext;
use crate::newindex::segment_worker::SegmentWorker;
use crate::newindex::standard_analyzer::StandardAnalyzer;
use crate::newindex::stored_fields_consumer::StoredFieldsConsumer;
use crate::store::SharedDirectory;

/// Creates workers with the standard set of field consumers.
pub struct DefaultWorkerFactory {
    directory: Arc<SharedDirectory>,
}

impl std::fmt::Debug for DefaultWorkerFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultWorkerFactory")
            .finish_non_exhaustive()
    }
}

impl DefaultWorkerFactory {
    /// Creates a new factory backed by the given directory.
    pub fn new(directory: Arc<SharedDirectory>) -> Self {
        Self { directory }
    }
}

impl WorkerFactory for DefaultWorkerFactory {
    fn create_worker(&self, segment_id: SegmentId) -> (SegmentWorker, SegmentContext) {
        let context = SegmentContext {
            directory: Arc::clone(&self.directory),
            segment_name: segment_id.name.clone(),
            segment_id: segment_id.id,
        };

        // Order matters: consumers are flushed in sequence and some depend
        // on files written by earlier consumers. FieldInfosConsumer must
        // remain last — it writes .fnm after all other consumers have
        // finalized field metadata.
        //
        // Expected final order:
        //   NormsConsumer        — norms must flush before postings (postings reads norms)
        //   DocValuesConsumer    — doc values
        //   PointsConsumer       — BKD tree
        //   StoredFieldsConsumer — stored fields
        //   TermVectorsConsumer  — term vectors (shares term pool with postings)
        //   PostingsConsumer     — postings + terms
        //   FieldInfosConsumer   — field infos (.fnm, must be last)
        let consumers: Vec<Box<dyn crate::newindex::consumer::FieldConsumer>> = vec![
            Box::new(StoredFieldsConsumer::new()),
            Box::new(FieldInfosConsumer::new()),
        ];

        let worker = SegmentWorker::new(segment_id, consumers, Box::new(StandardAnalyzer));

        (worker, context)
    }
}
