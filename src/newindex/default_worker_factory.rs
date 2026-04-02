// SPDX-License-Identifier: Apache-2.0

//! Default [`WorkerFactory`] implementation.

use std::fmt;
use std::sync::Arc;

use crate::index::segment::SegmentId;
use crate::index::segment_context::SegmentContext;
use crate::newindex::consumer::FieldConsumer;
use crate::newindex::coordinator::WorkerFactory;
use crate::newindex::doc_values_consumer::DocValuesConsumer;
use crate::newindex::field_infos_consumer::FieldInfosConsumer;
use crate::newindex::norms_consumer::NormsConsumer;
use crate::newindex::points_consumer::PointsConsumer;
use crate::newindex::postings_consumer::PostingsConsumer;
use crate::newindex::segment_worker::SegmentWorker;
use crate::newindex::standard_analyzer::StandardAnalyzer;
use crate::newindex::stored_fields_consumer::StoredFieldsConsumer;
use crate::newindex::term_vectors_consumer::TermVectorsConsumer;
use crate::store::SharedDirectory;

/// Creates workers with the standard set of field consumers.
pub struct DefaultWorkerFactory {
    directory: Arc<SharedDirectory>,
}

impl fmt::Debug for DefaultWorkerFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

        // Order matters: norms before postings, FieldInfosConsumer last.
        let consumers: Vec<Box<dyn FieldConsumer>> = vec![
            Box::new(NormsConsumer::new()),
            Box::new(DocValuesConsumer::new()),
            Box::new(PointsConsumer::new()),
            Box::new(StoredFieldsConsumer::new()),
            Box::new(TermVectorsConsumer::new()),
            Box::new(PostingsConsumer::new()),
            Box::new(FieldInfosConsumer::new()),
        ];

        let worker =
            SegmentWorker::new(segment_id, consumers, Box::new(StandardAnalyzer::default()));

        (worker, context)
    }
}
