// SPDX-License-Identifier: Apache-2.0

use std::io;

use crate::newindex::channel::{self, Sender};
use crate::newindex::config::IndexWriterConfig;
use crate::newindex::document::Document;
use crate::newindex::id_generator::IdGenerator;
use crate::newindex::segment::SegmentId;

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
    workers: Vec<std::thread::JoinHandle<io::Result<()>>>,
    /// Counter for assigning unique segment names. Incremented each
    /// time a new SegmentWorker is created. Only accessed by the
    /// coordinator — no locking needed.
    next_segment_num: u64,
    /// Generates random bytes for segment IDs.
    id_generator: Box<dyn IdGenerator>,
}

impl IndexCoordinator {
    /// Creates a new coordinator, spawning worker threads.
    pub fn new(config: &IndexWriterConfig) -> Self {
        let queue_capacity = config.num_threads * 2;
        let (sender, receiver) = channel::bounded(queue_capacity);

        let mut workers = Vec::with_capacity(config.num_threads);
        for _ in 0..config.num_threads {
            // Each worker thread owns a Receiver and a SegmentWorker.
            // It loops on recv(), processing documents until the
            // channel closes, flushing and replacing the worker when
            // thresholds are hit.
            let handle = std::thread::spawn(move || -> io::Result<()> {
                // TODO: create SegmentWorker, loop recv/process/flush
                Ok(())
            });
            workers.push(handle);
        }

        Self {
            sender,
            workers,
            next_segment_num: 0,
            id_generator: todo!("inject via constructor"),
        }
    }

    /// Assigns a unique segment identity for a new SegmentWorker.
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
    /// workers to drain remaining documents and flush their final segments.
    ///
    /// Returns the first error from any worker thread, or Ok if all
    /// workers completed successfully.
    pub fn shutdown(self) -> io::Result<()> {
        // Dropping the sender closes the channel, signaling workers to
        // exit their recv loop. Each worker flushes its remaining data
        // before the thread exits.
        // (Explicit drop is intentional — must happen before join.
        // Clippy warns because the placeholder Sender has no Drop impl.)
        let _sender = self.sender;
        for handle in self.workers {
            match handle.join() {
                Ok(result) => result?,
                Err(_) => return Err(io::Error::other("worker thread panicked")),
            }
        }
        Ok(())
    }
}

/// Formats a number as a base-36 string.
fn radix_fmt(_n: u64) -> String {
    todo!()
}
