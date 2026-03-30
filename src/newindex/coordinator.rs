// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::sync::Arc;

use crate::newindex::channel::{self, Sender};
use crate::newindex::config::IndexWriterConfig;
use crate::newindex::document::Document;
use crate::newindex::id_generator::IdGenerator;
use crate::newindex::index_file_names::radix_fmt;
use crate::newindex::segment::{FlushedSegment, SegmentId};
use crate::newindex::segment_context::SegmentContext;
use crate::newindex::segment_infos::SegmentInfos;
use crate::newindex::segment_worker::SegmentWorker;
use crate::store::SharedDirectory;

/// Creates [`SegmentWorker`] instances for worker threads.
///
/// Called once per thread at startup, and again after each mid-stream
/// flush to create a replacement worker. Implementations encapsulate
/// knowledge of which consumers and analyzer to use.
///
/// The order of consumers in the worker matters — consumers are
/// flushed in order, and some consumers depend on files written by
/// earlier consumers during flush. Implementations must ensure the
/// consumer list is correctly ordered to satisfy these dependencies.
// LOCKED
pub trait WorkerFactory: Send + Sync {
    /// Creates a new `SegmentWorker` and its flush-time `SegmentContext`
    /// for the given segment identity.
    fn create_worker(&self, segment_id: SegmentId) -> (SegmentWorker, SegmentContext);
}

/// Manages the worker thread pool and document processing pipeline.
///
/// Owns the bounded channel and worker threads. Documents are sent to
/// workers via the channel. Workers process documents independently,
/// flushing segments when buffer thresholds are reached.
///
/// # Worker lifecycle
///
/// Each thread owns a `SegmentWorker`. The thread loop is:
///
/// ```text
/// worker = create SegmentWorker
/// loop {
///     doc = receive from channel       // blocks if empty, exits if closed
///     worker.add_document(doc)?        // error → thread exits with Err
///     if worker.should_flush(config) {
///         flushed = worker.flush()?    // consumes worker
///         report flushed to coordinator
///         worker = create SegmentWorker
///     }
/// }
/// // channel closed — flush remaining buffered data
/// worker.flush()?
/// ```
///
/// Workers are disposable — flushing consumes the worker and drops
/// all accumulated state (pools, consumers, registry). The thread
/// then creates a brand new worker for the next segment. There is
/// no in-place reset — disposal prevents stale state from leaking
/// across segments.
///
/// # Error handling
///
/// If a worker hits an error (I/O failure, codec error), the thread
/// exits with that error. On `shutdown()`, the coordinator joins all
/// threads and returns the first error encountered. The caller
/// receives it from `commit()`. No poison flags or error channels —
/// just `Result` propagation through the thread join.
// LOCKED
pub struct IndexCoordinator {
    sender: Sender,
    workers: Vec<std::thread::JoinHandle<io::Result<Vec<FlushedSegment>>>>,
    /// Counter for assigning unique segment names. Incremented each
    /// time a new SegmentWorker is created. Only accessed by the
    /// coordinator — no locking needed.
    next_segment_num: u64,
    /// Generates random bytes for segment IDs.
    id_generator: Box<dyn IdGenerator>,
    /// Shared directory for writing segment infos at commit time.
    directory: Arc<SharedDirectory>,
    /// Tracks committed segments and writes `segments_N`.
    segment_infos: SegmentInfos,
}

impl IndexCoordinator {
    /// Creates a new coordinator, spawning worker threads.
    pub fn new(
        config: &IndexWriterConfig,
        mut id_generator: Box<dyn IdGenerator>,
        directory: Arc<SharedDirectory>,
        worker_factory: Arc<dyn WorkerFactory>,
    ) -> Self {
        let queue_capacity = config.num_threads * 2;
        let (sender, receiver) = channel::bounded(queue_capacity);
        let max_buffered_docs = config.max_buffered_docs;

        let mut workers = Vec::with_capacity(config.num_threads);
        let mut next_segment_num: u64 = 0;

        for _ in 0..config.num_threads {
            let rx = receiver.clone();
            let factory = Arc::clone(&worker_factory);
            let segment_id = SegmentId {
                name: format!("_{}", radix_fmt(next_segment_num)),
                id: id_generator.next_id(),
            };
            next_segment_num += 1;

            let handle = std::thread::spawn(move || -> io::Result<Vec<FlushedSegment>> {
                let mut segments = Vec::new();
                let (mut worker, context) = factory.create_worker(segment_id);

                while let Some(doc) = rx.recv() {
                    worker.add_document(doc)?;

                    if worker.should_flush(max_buffered_docs) {
                        segments.push(worker.flush(&context)?);
                        // TODO: create replacement worker with new segment ID
                        // For now, this only works when should_flush stays false
                        // (single flush at shutdown).
                        unreachable!("mid-stream flush not yet supported");
                    }
                }

                // Channel closed — flush remaining buffered data
                let flushed = worker.flush(&context)?;
                if flushed.doc_count > 0 {
                    segments.push(flushed);
                }

                Ok(segments)
            });
            workers.push(handle);
        }

        Self {
            sender,
            workers,
            next_segment_num,
            id_generator,
            directory,
            segment_infos: SegmentInfos::new(),
        }
    }

    /// Assigns a unique segment identity for a new SegmentWorker.
    #[expect(dead_code)]
    fn next_segment_id(&mut self) -> SegmentId {
        let name = format!("_{}", radix_fmt(self.next_segment_num));
        self.next_segment_num += 1;
        SegmentId {
            name,
            id: self.id_generator.next_id(),
        }
    }

    /// Sends a document to the worker pool. Blocks if the queue is full.
    pub fn add_document(&self, doc: Document) -> io::Result<()> {
        self.sender.send(doc)
    }

    /// Shuts down the coordinator: closes the channel, waits for all
    /// workers to drain remaining documents and flush their final segments,
    /// then writes the `segments_N` commit point.
    ///
    /// Returns all flushed segments, or the first error from any worker.
    pub fn shutdown(mut self) -> io::Result<Vec<FlushedSegment>> {
        // Dropping the sender closes the channel, signaling workers to
        // exit their recv loop. Each worker flushes its remaining data
        // before the thread exits.
        drop(self.sender);

        let mut all_segments = Vec::new();
        for handle in self.workers {
            match handle.join() {
                Ok(result) => all_segments.extend(result?),
                Err(_) => return Err(io::Error::other("worker thread panicked")),
            }
        }

        // TODO: compound file packaging would go here

        // Write the commit point
        if !all_segments.is_empty() {
            for segment in &all_segments {
                self.segment_infos.add(segment.clone());
            }
            self.segment_infos.commit(&self.directory)?;
        }

        Ok(all_segments)
    }
}
