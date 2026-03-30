// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::sync::{Arc, Mutex};

use crate::newindex::channel::{self, Receiver, Sender};
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

/// Thread-safe delegate for creating [`SegmentWorker`] instances.
///
/// Owns the segment counter and ID generator behind a [`Mutex`], and
/// holds a reference to the [`WorkerFactory`]. Worker threads call
/// [`create_worker`](Self::create_worker) to get their initial worker
/// and to obtain replacements after mid-stream flushes.
///
/// The [`Mutex`] is only taken when a worker is created — once per
/// segment, not per document. Lock hold time is nanoseconds (one
/// counter increment + 16 random bytes).
struct WorkerSource {
    state: Mutex<WorkerSourceState>,
    factory: Arc<dyn WorkerFactory>,
}

struct WorkerSourceState {
    next_segment_num: u64,
    id_generator: Box<dyn IdGenerator>,
}

impl WorkerSource {
    fn new(id_generator: Box<dyn IdGenerator>, factory: Arc<dyn WorkerFactory>) -> Self {
        Self {
            state: Mutex::new(WorkerSourceState {
                next_segment_num: 0,
                id_generator,
            }),
            factory,
        }
    }

    /// Mints a unique [`SegmentId`] and creates a new worker + context.
    fn create_worker(&self) -> (SegmentWorker, SegmentContext) {
        let segment_id = {
            let mut state = self.state.lock().unwrap();
            let name = format!("_{}", radix_fmt(state.next_segment_num));
            state.next_segment_num += 1;
            SegmentId {
                name,
                id: state.id_generator.next_id(),
            }
        };
        self.factory.create_worker(segment_id)
    }
}

/// Worker thread loop: pulls documents from the channel, processes
/// them through the worker, and flushes segments when thresholds are
/// reached. Returns all flushed segments when the channel closes.
fn worker_thread_loop(
    doc_rx: Receiver,
    worker_source: Arc<WorkerSource>,
    max_buffered_docs: i32,
) -> io::Result<Vec<FlushedSegment>> {
    let mut segments = Vec::new();
    let (mut worker, mut context) = worker_source.create_worker();

    while let Some(doc) = doc_rx.recv() {
        worker.add_document(doc)?;

        if worker.should_flush(max_buffered_docs) {
            segments.push(worker.flush(&context)?);
            let (new_worker, new_context) = worker_source.create_worker();
            worker = new_worker;
            context = new_context;
        }
    }

    // Channel closed — flush remaining buffered data
    let flushed = worker.flush(&context)?;
    if flushed.doc_count > 0 {
        segments.push(flushed);
    }

    Ok(segments)
}

/// Manages the worker thread pool and document processing pipeline.
///
/// Owns the bounded channel and worker threads. Documents are sent to
/// workers via the channel. Workers process documents independently,
/// flushing segments when buffer thresholds are reached.
///
/// # Worker lifecycle
///
/// Each thread pulls a worker from the shared [`WorkerSource`].
/// The thread loop is:
///
/// ```text
/// worker = worker_source.create_worker()
/// loop {
///     doc = receive from channel       // blocks if empty, exits if closed
///     worker.add_document(doc)?        // error → thread exits with Err
///     if worker.should_flush(config) {
///         flushed = worker.flush()?    // consumes worker
///         collect flushed segment
///         worker = worker_source.create_worker()
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
    /// Shared directory for writing segment infos at commit time.
    directory: Arc<SharedDirectory>,
    /// Tracks committed segments and writes `segments_N`.
    segment_infos: SegmentInfos,
}

impl IndexCoordinator {
    /// Creates a new coordinator, spawning worker threads.
    pub fn new(
        config: &IndexWriterConfig,
        id_generator: Box<dyn IdGenerator>,
        directory: Arc<SharedDirectory>,
        worker_factory: Arc<dyn WorkerFactory>,
    ) -> Self {
        let queue_capacity = config.num_threads * 2;
        let (sender, receiver) = channel::bounded(queue_capacity);
        let max_buffered_docs = config.max_buffered_docs;
        let worker_source = Arc::new(WorkerSource::new(id_generator, worker_factory));

        let mut workers = Vec::with_capacity(config.num_threads);

        for _ in 0..config.num_threads {
            let rx = receiver.clone();
            let source = Arc::clone(&worker_source);

            let handle =
                std::thread::spawn(move || worker_thread_loop(rx, source, max_buffered_docs));
            workers.push(handle);
        }

        Self {
            sender,
            workers,
            directory,
            segment_infos: SegmentInfos::new(),
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
