// SPDX-License-Identifier: Apache-2.0

//! Thread-safe index writer with multi-segment, multi-threaded document ingestion.

// Multi-segment, multi-threaded, no merging, no deletes

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use log::debug;

use crate::analysis::Analyzer;
use crate::analysis::standard::StandardAnalyzer;
use crate::codecs::lucene90;
use crate::codecs::lucene94;
use crate::codecs::lucene99;
use crate::codecs::lucene103;
use crate::document::Document;
use crate::index::flush_control::FlushControl;
use crate::index::flush_policy::{FlushByRamOrCountsPolicy, FlushPolicy};
use crate::index::index_writer_config::IndexWriterConfig;
use crate::index::indexing_chain::IndexingChain;
use crate::index::segment_worker::SegmentWorker;
use crate::index::segment_worker_pool::SegmentWorkerPool;
use crate::index::{SegmentCommitInfo, index_file_names, segment_infos};
use crate::store::{Directory, SegmentFile, SharedDirectory};

/// A thread-safe IndexWriter supporting multiple segments and concurrent
/// document ingestion.
///
/// `IndexWriter` is cheaply cloneable (wraps `Arc<SharedState>`). Multiple
/// threads can call `add_document` concurrently — each thread obtains its
/// own `SegmentWorker` from a pool. When the flush policy triggers, the
/// worker is flushed to a segment. On `commit()`, all remaining workers
/// are flushed and a `segments_N` commit point is written.
///
/// Segment files are written to the writer's [`Directory`] at flush time,
/// not buffered until commit. This keeps memory usage bounded to the active
/// worker buffers plus one segment's worth of codec output at a time.
///
/// Usage (single-threaded, in-memory):
/// ```ignore
/// let writer = IndexWriter::new();
/// writer.add_document(doc)?;
/// let result = writer.commit()?;
/// let files = result.into_segment_files()?;
/// ```
///
/// Usage (multi-threaded, filesystem):
/// ```ignore
/// let dir = Box::new(FSDirectory::open(path)?);
/// let writer = IndexWriter::with_config_and_directory(config, dir);
/// thread::scope(|s| {
///     for chunk in doc_chunks {
///         let w = writer.clone();
///         s.spawn(move || {
///             for doc in chunk {
///                 w.add_document(doc).unwrap();
///             }
///         });
///     }
/// });
/// let result = writer.commit()?;
/// // Files are already on disk — just inspect file_names()
/// ```
pub struct IndexWriter {
    shared: Arc<SharedState>,
}

struct SharedState {
    worker_pool: SegmentWorkerPool,
    flush_control: FlushControl,
    analyzer: Arc<dyn Analyzer>,
    config: IndexWriterConfig,
    flush_policy: Box<dyn FlushPolicy>,
    /// Directory where segment files are written at flush time.
    directory: Arc<SharedDirectory>,
    /// Flushed but uncommitted segments (metadata only — file data is in directory).
    pending_segments: Mutex<Vec<FlushedSegment>>,
    /// Commit generation (incremented on each commit).
    generation: Mutex<i64>,
    /// Total number of documents added across all workers.
    total_docs: AtomicI32,
}

impl Clone for IndexWriter {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl IndexWriter {
    /// Creates a new IndexWriter with default config and an in-memory directory.
    pub fn new() -> Self {
        Self::with_config(IndexWriterConfig::new())
    }

    /// Creates a new IndexWriter with the given configuration and an in-memory directory.
    pub fn with_config(config: IndexWriterConfig) -> Self {
        Self::with_config_and_directory(
            config,
            Box::new(crate::store::memory::MemoryDirectory::new()),
        )
    }

    /// Creates a new IndexWriter with default config and the given directory.
    pub fn with_directory(dir: Box<dyn Directory>) -> Self {
        Self::with_config_and_directory(IndexWriterConfig::new(), dir)
    }

    /// Creates a new IndexWriter with the given configuration and directory.
    ///
    /// Segment files are written to `dir` at flush time. At commit, only the
    /// `segments_N` file is added and all files are synced.
    pub fn with_config_and_directory(config: IndexWriterConfig, dir: Box<dyn Directory>) -> Self {
        Self {
            shared: Arc::new(SharedState {
                worker_pool: SegmentWorkerPool::new(),
                flush_control: FlushControl::new(),
                analyzer: Arc::new(StandardAnalyzer::new()),
                config,
                flush_policy: Box::new(FlushByRamOrCountsPolicy),
                directory: Arc::new(Mutex::new(dir)),
                pending_segments: Mutex::new(Vec::new()),
                generation: Mutex::new(0),
                total_docs: AtomicI32::new(0),
            }),
        }
    }

    /// Adds a document to the index, consuming it.
    ///
    /// Obtains a worker from the pool, processes the document, checks the
    /// flush policy, and either returns the worker to the pool or flushes it.
    ///
    /// Thread-safe: multiple threads can call this concurrently.
    pub fn add_document(&self, doc: Document) -> io::Result<()> {
        // Wait if too many concurrent flushes
        self.shared.flush_control.wait_if_stalled();

        // Obtain a worker (creates new one or reuses free one)
        let mut worker = self.shared.worker_pool.obtain(&self.shared.directory);

        // Process document (no locks held during this CPU-intensive work)
        worker.add_document(doc, self.shared.analyzer.as_ref())?;
        self.shared.total_docs.fetch_add(1, Ordering::Relaxed);

        // Log per-component breakdown after each doc
        worker.log_ram_breakdown(&format!("after_doc:{}", worker.segment_name()));

        // Check flush policy
        let ram_used = worker.ram_bytes_used();
        if self
            .shared
            .flush_policy
            .should_flush(worker.num_docs(), ram_used, &self.shared.config)
        {
            debug!(
                "flush trigger: segment {} with {} docs, RAM {} bytes ({:.2} MB)",
                worker.segment_name(),
                worker.num_docs(),
                ram_used,
                ram_used as f64 / 1024.0 / 1024.0,
            );
            // Update global field numbers before flushing
            self.shared
                .worker_pool
                .update_field_numbers(worker.field_number_mappings());

            // Flush this worker
            self.flush_worker(worker)?;
        } else {
            // Return worker to pool for reuse
            self.shared.worker_pool.release(worker);
        }

        Ok(())
    }

    /// Flushes a worker, writing segment files to the directory and adding
    /// metadata to pending.
    fn flush_worker(&self, worker: SegmentWorker) -> io::Result<()> {
        let segment_name = worker.segment_name().to_string();
        let num_docs = worker.num_docs();

        let flushed = worker.flush(self.shared.config.use_compound_file())?;

        debug!(
            "flush: flushed segment {} with {} docs",
            segment_name, num_docs
        );

        self.shared.pending_segments.lock().unwrap().push(flushed);

        self.shared.flush_control.flush_completed();
        Ok(())
    }

    /// Returns the total number of documents added across all segments.
    pub fn num_docs(&self) -> i32 {
        self.shared.total_docs.load(Ordering::Relaxed)
    }

    /// Commits the index, producing a [`CommitResult`].
    ///
    /// Drains remaining workers from the pool, flushes any with documents,
    /// then writes a `segments_N` commit point listing all segments.
    ///
    /// Must not be called concurrently with `add_document`.
    ///
    /// Use [`CommitResult::into_segment_files`] to materialize all files in memory
    /// (requires [`MemoryDirectory`](crate::store::MemoryDirectory)),
    /// or [`CommitResult::file_names`] to get the list of written file names.
    pub fn commit(&self) -> io::Result<CommitResult> {
        // Drain any pending workers from flush control
        for worker in self.shared.flush_control.drain_pending() {
            if worker.num_docs() > 0 {
                self.shared
                    .worker_pool
                    .update_field_numbers(worker.field_number_mappings());
                let flushed = worker.flush(self.shared.config.use_compound_file())?;
                self.shared.pending_segments.lock().unwrap().push(flushed);
            }
        }

        // Drain free workers from pool and flush any with documents
        for worker in self.shared.worker_pool.drain_free() {
            if worker.num_docs() > 0 {
                self.shared
                    .worker_pool
                    .update_field_numbers(worker.field_number_mappings());
                let flushed = worker.flush(self.shared.config.use_compound_file())?;
                self.shared.pending_segments.lock().unwrap().push(flushed);
            }
        }

        // Drain pending segments so we don't hold the lock during I/O
        let pending: Vec<FlushedSegment> = {
            let mut lock = self.shared.pending_segments.lock().unwrap();
            lock.drain(..).collect()
        };

        let mut generation = self.shared.generation.lock().unwrap();
        *generation += 1;

        // Handle empty commit (no documents at all)
        if pending.is_empty() {
            let user_data = HashMap::new();
            let seg_file = segment_infos::write(
                &[],
                *generation,
                *generation,
                self.shared.worker_pool.segment_counter() as i64,
                &user_data,
            )?;
            debug!("commit: wrote {} (empty)", seg_file.name);
            let mut dir = self.shared.directory.lock().unwrap();
            dir.write_file(&seg_file.name, &seg_file.data)?;
            dir.sync(&[&seg_file.name])?;
            dir.sync_meta_data()?;
            return Ok(CommitResult {
                directory: Arc::clone(&self.shared.directory),
                file_names: vec![seg_file.name],
            });
        }

        // Collect all segment commit infos for the segments_N file
        let sci_refs: Vec<&SegmentCommitInfo> =
            pending.iter().map(|fs| &fs.segment_commit_info).collect();

        let user_data = HashMap::new();
        let seg_file = segment_infos::write(
            &sci_refs,
            *generation,
            *generation,
            self.shared.worker_pool.segment_counter() as i64,
            &user_data,
        )?;
        debug!("commit: wrote {}", seg_file.name);
        drop(generation);

        // Build complete file list: segments_N first, then per-segment files
        let mut file_names = Vec::new();
        file_names.push(seg_file.name.clone());
        for seg in &pending {
            file_names.extend(seg.file_names.iter().cloned());
        }

        // Write segments_N to directory and sync all files
        {
            let mut dir = self.shared.directory.lock().unwrap();
            dir.write_file(&seg_file.name, &seg_file.data)?;
            let name_refs: Vec<&str> = file_names.iter().map(|s| s.as_str()).collect();
            dir.sync(&name_refs)?;
            dir.sync_meta_data()?;
        }

        debug!("commit: {} segments", pending.len());

        Ok(CommitResult {
            directory: Arc::clone(&self.shared.directory),
            file_names,
        })
    }
}

impl Default for IndexWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// All state needed to write a segment to disk.
pub(crate) struct SegmentWriteState<'a> {
    pub(crate) segment_commit_info: SegmentCommitInfo,
    pub(crate) field_infos: crate::index::FieldInfos,
    pub(crate) chain: &'a IndexingChain,
}

/// Metadata for a flushed segment. File data has already been written to the
/// directory — this struct holds only the segment info and the list of file
/// names for sync at commit time.
pub(crate) struct FlushedSegment {
    pub(crate) segment_commit_info: SegmentCommitInfo,
    pub(crate) file_names: Vec<String>,
}

/// Result of [`IndexWriter::commit`]. Provides access to committed files:
///
/// - [`file_names`](CommitResult::file_names): returns the list of written file names.
/// - [`into_segment_files`](CommitResult::into_segment_files): reads files from the
///   writer's directory into memory.
/// - [`write_to_directory`](CommitResult::write_to_directory): copies files to a different
///   [`Directory`].
pub struct CommitResult {
    directory: Arc<SharedDirectory>,
    file_names: Vec<String>,
}

impl CommitResult {
    /// Returns the list of file names written during commit.
    pub fn file_names(&self) -> &[String] {
        &self.file_names
    }

    /// Materializes all files in memory by reading from the writer's directory.
    /// Returns a flat list: `[segments_N, _0.si, _0.cfs, _0.cfe, ...]`.
    pub fn into_segment_files(self) -> io::Result<Vec<SegmentFile>> {
        let dir = self.directory.lock().unwrap();
        let mut files = Vec::with_capacity(self.file_names.len());
        for name in &self.file_names {
            let data = dir.read_file(name)?;
            files.push(SegmentFile {
                name: name.clone(),
                data,
            });
        }
        Ok(files)
    }

    /// Copies all committed files to the given [`Directory`], syncs, and returns
    /// the file names.
    pub fn write_to_directory(
        self,
        dir: &mut dyn crate::store::Directory,
    ) -> io::Result<Vec<String>> {
        let source = self.directory.lock().unwrap();
        for name in &self.file_names {
            let data = source.read_file(name)?;
            dir.write_file(name, &data)?;
        }
        drop(source);
        let name_refs: Vec<&str> = self.file_names.iter().map(|s| s.as_str()).collect();
        dir.sync(&name_refs)?;
        dir.sync_meta_data()?;
        Ok(self.file_names)
    }
}

/// Runs the full codec pipeline for one segment, writes files to the directory,
/// and returns metadata-only [`FlushedSegment`].
pub(crate) fn flush_segment_to_files(
    state: &SegmentWriteState<'_>,
    directory: &SharedDirectory,
    use_compound_file: bool,
    tv_file_names: Option<Vec<String>>,
) -> io::Result<FlushedSegment> {
    let si = &state.segment_commit_info.info;
    let field_infos = &state.field_infos;
    let chain = state.chain;

    let mut all_file_names: Vec<String> = Vec::new();

    // 1. Field infos (.fnm)
    let fnm_name = lucene94::field_infos_format::write(directory, si, "", field_infos)?;
    debug!("flush: wrote {}", fnm_name);
    all_file_names.push(fnm_name);

    // 2. Postings (.doc, .pos, .tim, .tip, .tmd, .psm) — PerField suffix "Lucene103_0"
    let postings_names = lucene103::postings_format::write(
        directory,
        si,
        "Lucene103_0",
        field_infos,
        chain.per_field(),
    )?;
    for name in &postings_names {
        debug!("flush: wrote {}", name);
    }
    all_file_names.extend(postings_names);

    // 3. Norms (.nvm, .nvd)
    let norms_names = lucene90::norms::write(
        directory,
        &si.name,
        "",
        &si.id,
        field_infos,
        chain.per_field(),
        chain.num_docs(),
    )?;
    for name in &norms_names {
        debug!("flush: wrote {}", name);
    }
    all_file_names.extend(norms_names);

    // 4. Doc values (.dvm, .dvd) — PerField suffix "Lucene90_0"
    let dv_names = lucene90::doc_values::write(
        directory,
        &si.name,
        "Lucene90_0",
        &si.id,
        field_infos,
        chain.per_field(),
        chain.num_docs(),
    )?;
    for name in &dv_names {
        debug!("flush: wrote {}", name);
    }
    all_file_names.extend(dv_names);

    // 5. Points (.kdd, .kdi, .kdm)
    let pts_names = lucene90::points::write(
        directory,
        &si.name,
        "",
        &si.id,
        field_infos,
        chain.per_field(),
        chain.num_docs(),
    )?;
    for name in &pts_names {
        debug!("flush: wrote {}", name);
    }
    all_file_names.extend(pts_names);

    // 6. Stored fields (.fdt, .fdx, .fdm)
    let sf_names = lucene90::stored_fields::write(
        directory,
        &si.name,
        "",
        &si.id,
        chain.stored_docs(),
        chain.num_docs(),
    )?;
    for name in &sf_names {
        debug!("flush: wrote {}", name);
    }
    all_file_names.extend(sf_names);

    // 7. Term vectors (.tvd, .tvx, .tvm)
    let tv_names = if let Some(names) = tv_file_names {
        // TV files already written by streaming chunk writer
        names
    } else {
        lucene90::term_vectors::write(
            directory,
            &si.name,
            "",
            &si.id,
            chain.term_vector_docs(),
            chain.num_docs(),
        )?
    };
    for name in &tv_names {
        debug!("flush: wrote {}", name);
    }
    all_file_names.extend(tv_names);

    debug!("flush: {} per-segment files", all_file_names.len());

    // All codec files are already written to directory (auto-persisted on drop).
    // Now write compound files or .si as needed.
    let file_names = if use_compound_file {
        write_compound_segment(si, &all_file_names, directory)?
    } else {
        write_non_compound_segment(si, &all_file_names, directory)?
    };

    Ok(FlushedSegment {
        segment_commit_info: state.segment_commit_info.clone(),
        file_names,
    })
}

/// Builds compound files (.cfs/.cfe) from files already in the directory,
/// then writes .si + .cfs + .cfe and deletes the originals.
fn write_compound_segment(
    si: &crate::index::SegmentInfo,
    sub_file_names: &[String],
    directory: &SharedDirectory,
) -> io::Result<Vec<String>> {
    let si_name = index_file_names::segment_file_name(&si.name, "", "si");
    let cfs_name = index_file_names::segment_file_name(&si.name, "", "cfs");
    let cfe_name = index_file_names::segment_file_name(&si.name, "", "cfe");
    let si_files = vec![si_name.clone(), cfs_name.clone(), cfe_name.clone()];

    lucene99::segment_info_format::write(directory, si, &si_files)?;
    debug!("flush: wrote {}", si_name);
    {
        let mut dir = directory.lock().unwrap();

        // Read sub-files into SegmentFile structs for compound writer
        let sub_files: Vec<SegmentFile> = sub_file_names
            .iter()
            .map(|name| {
                let data = dir.read_file(name)?;
                Ok(SegmentFile {
                    name: name.clone(),
                    data,
                })
            })
            .collect::<io::Result<Vec<_>>>()?;

        // Build compound file
        let mut cfs_out = crate::store::memory::MemoryIndexOutput::new(cfs_name.clone());
        let cfe = lucene90::compound::write_to(&si.name, &si.id, &sub_files, &mut cfs_out)?;
        dir.write_file(&cfs_name, cfs_out.bytes())?;
        dir.write_file(&cfe.name, &cfe.data)?;

        // Delete original sub-files
        for name in sub_file_names {
            dir.delete_file(name)?;
        }
    }

    Ok(vec![si_name, cfs_name, cfe_name])
}

/// Writes .si directly to the directory (non-compound mode).
/// All sub-files are already in the directory.
fn write_non_compound_segment(
    si: &crate::index::SegmentInfo,
    sub_file_names: &[String],
    directory: &SharedDirectory,
) -> io::Result<Vec<String>> {
    let si_name = index_file_names::segment_file_name(&si.name, "", "si");
    let mut si_files = Vec::with_capacity(1 + sub_file_names.len());
    si_files.push(si_name.clone());
    si_files.extend_from_slice(sub_file_names);

    lucene99::segment_info_format::write(directory, si, &si_files)?;
    debug!("flush: wrote {}", si_name);

    // Return all file names (si + sub-files)
    let names = si_files;

    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::codec_util::{CODEC_MAGIC, FOOTER_LENGTH, FOOTER_MAGIC};
    use crate::document;

    #[test]
    fn test_index_writer_add_documents() {
        let writer = IndexWriter::new();

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/foo.txt"));
        doc.add(document::long_field("modified", 1000));
        doc.add(document::text_field("contents", "hello world"));
        writer.add_document(doc).unwrap();

        let mut doc2 = Document::new();
        doc2.add(document::keyword_field("path", "/bar.txt"));
        doc2.add(document::long_field("modified", 2000));
        doc2.add(document::text_field("contents", "goodbye world"));
        writer.add_document(doc2).unwrap();

        assert_eq!(writer.num_docs(), 2);
    }

    #[test]
    fn test_commit_produces_stored_fields_mode_attribute() {
        let writer = IndexWriter::new();

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/foo.txt"));
        doc.add(document::long_field("modified", 1000));
        doc.add(document::text_field("contents", "hello world"));
        writer.add_document(doc).unwrap();

        let files = writer.commit().unwrap().into_segment_files().unwrap();

        // The .si file must contain the stored fields mode attribute
        let si_data = &files.iter().find(|f| f.name.ends_with(".si")).unwrap().data;
        let si_str = String::from_utf8_lossy(si_data);
        assert!(
            si_str.contains("Lucene90StoredFieldsFormat.mode"),
            "segment must have Lucene90StoredFieldsFormat.mode attribute"
        );
        assert!(
            si_str.contains("BEST_SPEED"),
            "segment must have BEST_SPEED value"
        );
    }

    // --- commit() integration tests ---

    fn make_three_doc_writer() -> IndexWriter {
        let writer = IndexWriter::new();
        for (path, modified, contents) in [
            ("/a.txt", 100, "the quick brown fox"),
            ("/b.txt", 200, "the lazy dog"),
            ("/c.txt", 300, "quick fox jumps"),
        ] {
            let mut doc = Document::new();
            doc.add(document::keyword_field("path", path));
            doc.add(document::long_field("modified", modified));
            doc.add(document::text_field("contents", contents));
            writer.add_document(doc).unwrap();
        }
        writer
    }

    #[test]
    fn test_commit_produces_segment_files() {
        let writer = make_three_doc_writer();
        let files = writer.commit().unwrap().into_segment_files().unwrap();

        let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
        // segments_1 + _0.si + _0.cfs + _0.cfe = 4
        assert_len_eq_x!(&names, 4);
        assert_eq!(names[0], "segments_1");
        assert_contains!(names, &"_0.si");
        assert_contains!(names, &"_0.cfs");
        assert_contains!(names, &"_0.cfe");
    }

    #[test]
    fn test_commit_files_have_valid_headers_footers() {
        let writer = make_three_doc_writer();
        let files = writer.commit().unwrap().into_segment_files().unwrap();

        for f in &files {
            assert!(
                f.data.len() >= FOOTER_LENGTH + 4,
                "file {} too small: {} bytes",
                f.name,
                f.data.len()
            );

            // Verify codec header magic
            let magic = i32::from_be_bytes(f.data[0..4].try_into().unwrap());
            assert_eq!(
                magic, CODEC_MAGIC,
                "file {} has wrong header magic: 0x{magic:08x}",
                f.name
            );

            // Verify footer magic
            let footer_start = f.data.len() - FOOTER_LENGTH;
            let footer_magic =
                i32::from_be_bytes(f.data[footer_start..footer_start + 4].try_into().unwrap());
            assert_eq!(
                footer_magic, FOOTER_MAGIC,
                "file {} has wrong footer magic: 0x{footer_magic:08x}",
                f.name
            );
        }
    }

    #[test]
    fn test_commit_single_doc() {
        let writer = IndexWriter::new();
        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/only.txt"));
        doc.add(document::long_field("modified", 42));
        doc.add(document::text_field("contents", "just one document"));
        writer.add_document(doc).unwrap();

        let files = writer.commit().unwrap().into_segment_files().unwrap();
        // segments_1 + _0.si + _0.cfs + _0.cfe = 4
        assert_len_eq_x!(&files, 4);

        let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names[0], "segments_1");
        assert_contains!(names, &"_0.si");
        assert_contains!(names, &"_0.cfs");
        assert_contains!(names, &"_0.cfe");
    }

    #[test]
    fn test_commit_si_contains_stored_fields_mode() {
        let writer = make_three_doc_writer();
        let files = writer.commit().unwrap().into_segment_files().unwrap();

        let si_data = &files.iter().find(|f| f.name == "_0.si").unwrap().data;

        // The .si file must contain the serialized attribute key and value
        let si_str = String::from_utf8_lossy(si_data);
        assert!(
            si_str.contains("Lucene90StoredFieldsFormat.mode"),
            ".si file must contain Lucene90StoredFieldsFormat.mode attribute"
        );
        assert!(
            si_str.contains("BEST_SPEED"),
            ".si file must contain BEST_SPEED value"
        );
    }

    #[test]
    fn test_commit_file_sizes_reasonable() {
        let writer = make_three_doc_writer();
        let files = writer.commit().unwrap().into_segment_files().unwrap();

        for f in &files {
            // Every file should be at least header + footer
            assert!(
                f.data.len() > FOOTER_LENGTH,
                "file {} is suspiciously small: {} bytes",
                f.name,
                f.data.len()
            );
            // No file in a 3-doc index should be enormous
            assert!(
                f.data.len() < 100_000,
                "file {} is suspiciously large: {} bytes",
                f.name,
                f.data.len()
            );
        }

        // .cfs should be the largest (contains all per-segment data)
        let cfs_size = files[2].data.len();
        let cfe_size = files[3].data.len();
        assert!(
            cfs_size > cfe_size,
            ".cfs ({cfs_size}) should be larger than .cfe ({cfe_size})"
        );
    }

    // --- Multi-segment tests ---

    #[test]
    fn test_multi_segment_flush_by_doc_count() {
        // 5 docs with max_buffered_docs=2 should produce 3 segments:
        // _0 (2 docs), _1 (2 docs), _2 (1 doc)
        let config = IndexWriterConfig::new().set_max_buffered_docs(2);
        let writer = IndexWriter::with_config(config);

        for i in 0..5 {
            let mut doc = Document::new();
            doc.add(document::keyword_field("path", &format!("/{i}.txt")));
            doc.add(document::long_field("modified", i as i64 * 100));
            doc.add(document::text_field("contents", &format!("doc number {i}")));
            writer.add_document(doc).unwrap();
        }

        let files = writer.commit().unwrap().into_segment_files().unwrap();

        // segments_1 + 3 segments x (si + cfs + cfe) = 10
        assert_len_eq_x!(&files, 10);
        assert_eq!(files[0].name, "segments_1");

        let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
        assert_contains!(names, &"_0.si");
        assert_contains!(names, &"_0.cfs");
        assert_contains!(names, &"_0.cfe");
        assert_contains!(names, &"_1.si");
        assert_contains!(names, &"_1.cfs");
        assert_contains!(names, &"_1.cfe");
        assert_contains!(names, &"_2.si");
        assert_contains!(names, &"_2.cfs");
        assert_contains!(names, &"_2.cfe");

        assert_eq!(writer.num_docs(), 5);
    }

    #[test]
    fn test_single_segment_when_flush_disabled() {
        // Explicitly disable both doc-count and RAM flushing
        let config = IndexWriterConfig::new()
            .set_max_buffered_docs(-1)
            .set_ram_buffer_size_mb(0.0);
        let writer = IndexWriter::with_config(config);
        for i in 0..10 {
            let mut doc = Document::new();
            doc.add(document::keyword_field("path", &format!("/{i}.txt")));
            doc.add(document::long_field("modified", i as i64));
            doc.add(document::text_field("contents", "hello"));
            writer.add_document(doc).unwrap();
        }

        let files = writer.commit().unwrap().into_segment_files().unwrap();

        // segments_1 + _0.si + _0.cfs + _0.cfe = 4
        assert_len_eq_x!(&files, 4);
        assert_eq!(files[0].name, "segments_1");
        let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
        assert_contains!(names, &"_0.si");
    }

    #[test]
    fn test_segment_names_increment() {
        // Segment names use radix-36 counter
        let config = IndexWriterConfig::new().set_max_buffered_docs(1);
        let writer = IndexWriter::with_config(config);

        for i in 0..3 {
            let mut doc = Document::new();
            doc.add(document::keyword_field("path", &format!("/{i}.txt")));
            doc.add(document::long_field("modified", i as i64));
            doc.add(document::text_field("contents", "test"));
            writer.add_document(doc).unwrap();
        }

        let files = writer.commit().unwrap().into_segment_files().unwrap();

        // 3 segments: _0, _1, _2
        let si_names: Vec<&str> = files
            .iter()
            .filter(|f| f.name.ends_with(".si"))
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(si_names, &["_0.si", "_1.si", "_2.si"]);
    }

    #[test]
    fn test_commit_files_have_valid_headers_footers_multi_segment() {
        let config = IndexWriterConfig::new().set_max_buffered_docs(2);
        let writer = IndexWriter::with_config(config);

        for i in 0..5 {
            let mut doc = Document::new();
            doc.add(document::keyword_field("path", &format!("/{i}.txt")));
            doc.add(document::long_field("modified", i as i64));
            doc.add(document::text_field("contents", "test content"));
            writer.add_document(doc).unwrap();
        }

        let files = writer.commit().unwrap().into_segment_files().unwrap();

        for f in &files {
            assert!(
                f.data.len() >= FOOTER_LENGTH + 4,
                "file {} too small: {} bytes",
                f.name,
                f.data.len()
            );

            let magic = i32::from_be_bytes(f.data[0..4].try_into().unwrap());
            assert_eq!(
                magic, CODEC_MAGIC,
                "file {} has wrong header magic: 0x{magic:08x}",
                f.name
            );

            let footer_start = f.data.len() - FOOTER_LENGTH;
            let footer_magic =
                i32::from_be_bytes(f.data[footer_start..footer_start + 4].try_into().unwrap());
            assert_eq!(
                footer_magic, FOOTER_MAGIC,
                "file {} has wrong footer magic: 0x{footer_magic:08x}",
                f.name
            );
        }
    }

    #[test]
    fn test_empty_commit() {
        // Commit with 0 docs produces only segments_1
        let writer = IndexWriter::new();
        let files = writer.commit().unwrap().into_segment_files().unwrap();

        assert_len_eq_x!(&files, 1);
        assert_eq!(files[0].name, "segments_1");
    }

    // --- Thread safety tests ---

    #[test]
    fn test_concurrent_add_document() {
        // 4 threads x 25 docs = 100 total docs
        let config = IndexWriterConfig::new().set_max_buffered_docs(10);
        let writer = IndexWriter::with_config(config);

        std::thread::scope(|s| {
            for thread_id in 0..4 {
                let w = writer.clone();
                s.spawn(move || {
                    for i in 0..25 {
                        let mut doc = Document::new();
                        doc.add(document::keyword_field(
                            "path",
                            &format!("/t{thread_id}_{i}.txt"),
                        ));
                        doc.add(document::long_field(
                            "modified",
                            (thread_id * 100 + i) as i64,
                        ));
                        doc.add(document::text_field(
                            "contents",
                            &format!("thread {thread_id} doc {i}"),
                        ));
                        w.add_document(doc).unwrap();
                    }
                });
            }
        });

        assert_eq!(writer.num_docs(), 100);

        let files = writer.commit().unwrap().into_segment_files().unwrap();

        // Should have segments_1 + some number of segments (at least 1)
        assert_ge!(files.len(), 4, "expected at least segments_1 + 1 segment");
        assert_eq!(files[0].name, "segments_1");

        // Count total segments
        let num_segments = files.iter().filter(|f| f.name.ends_with(".si")).count();
        assert_ge!(num_segments, 1);
    }

    #[test]
    fn test_concurrent_produces_valid_headers_footers() {
        // Multi-threaded output must have valid codec headers/footers
        let config = IndexWriterConfig::new().set_max_buffered_docs(5);
        let writer = IndexWriter::with_config(config);

        std::thread::scope(|s| {
            for t in 0..2 {
                let w = writer.clone();
                s.spawn(move || {
                    for i in 0..10 {
                        let mut doc = Document::new();
                        doc.add(document::keyword_field("path", &format!("/t{t}_{i}.txt")));
                        doc.add(document::long_field("modified", i as i64));
                        doc.add(document::text_field("contents", "test"));
                        w.add_document(doc).unwrap();
                    }
                });
            }
        });

        let files = writer.commit().unwrap().into_segment_files().unwrap();

        for f in &files {
            assert!(
                f.data.len() >= FOOTER_LENGTH + 4,
                "file {} too small: {} bytes",
                f.name,
                f.data.len()
            );

            let magic = i32::from_be_bytes(f.data[0..4].try_into().unwrap());
            assert_eq!(magic, CODEC_MAGIC, "file {} has wrong header magic", f.name);

            let footer_start = f.data.len() - FOOTER_LENGTH;
            let footer_magic =
                i32::from_be_bytes(f.data[footer_start..footer_start + 4].try_into().unwrap());
            assert_eq!(
                footer_magic, FOOTER_MAGIC,
                "file {} has wrong footer magic",
                f.name
            );
        }
    }

    #[test]
    fn test_stall_control_no_deadlock() {
        // max_buffered_docs=1 with many docs doesn't deadlock
        let config = IndexWriterConfig::new().set_max_buffered_docs(1);
        let writer = IndexWriter::with_config(config);

        std::thread::scope(|s| {
            for t in 0..4 {
                let w = writer.clone();
                s.spawn(move || {
                    for i in 0..10 {
                        let mut doc = Document::new();
                        doc.add(document::keyword_field("path", &format!("/t{t}_{i}.txt")));
                        doc.add(document::long_field("modified", i as i64));
                        doc.add(document::text_field("contents", "stall test"));
                        w.add_document(doc).unwrap();
                    }
                });
            }
        });

        assert_eq!(writer.num_docs(), 40);
        let files = writer.commit().unwrap().into_segment_files().unwrap();
        assert_ge!(files.len(), 2); // at least segments_1 + some segment
    }

    // --- RAM-based flush tests ---

    /// Helper: creates a document with a large "contents" field to consume
    /// meaningful amounts of RAM.
    fn make_large_doc(i: usize) -> Document {
        let mut doc = Document::new();
        doc.add(document::keyword_field("path", &format!("/{i}.txt")));
        doc.add(document::long_field("modified", i as i64));
        // ~1 KB of text per doc
        let text = format!("document {i} ").repeat(80);
        doc.add(document::text_field("contents", &text));
        doc
    }

    #[test]
    fn test_ram_based_flush_creates_multiple_segments() {
        // Use a very small RAM buffer (1 KB) so a few docs trigger a flush
        let config = IndexWriterConfig::new().set_ram_buffer_size_mb(0.001); // ~1 KB
        let writer = IndexWriter::with_config(config);

        for i in 0..20 {
            writer.add_document(make_large_doc(i)).unwrap();
        }

        let files = writer.commit().unwrap().into_segment_files().unwrap();
        let num_segments = files.iter().filter(|f| f.name.ends_with(".si")).count();
        assert!(
            num_segments > 1,
            "expected multiple segments with small RAM buffer, got {num_segments}"
        );
    }

    #[test]
    fn test_ram_based_flush_default_config() {
        // Default config (16 MB RAM buffer) with a small number of docs
        // should produce a single segment (not enough data to trigger flush)
        let writer = IndexWriter::new();
        for i in 0..10 {
            let mut doc = Document::new();
            doc.add(document::keyword_field("path", &format!("/{i}.txt")));
            doc.add(document::long_field("modified", i as i64));
            doc.add(document::text_field("contents", "small doc"));
            writer.add_document(doc).unwrap();
        }

        let files = writer.commit().unwrap().into_segment_files().unwrap();
        let num_segments = files.iter().filter(|f| f.name.ends_with(".si")).count();
        assert_eq!(
            num_segments, 1,
            "small data with 16 MB buffer should be 1 segment"
        );
    }

    #[test]
    fn test_ram_flush_disabled() {
        // ram_buffer_size_mb = 0.0 and no doc count limit → single segment
        let config = IndexWriterConfig::new().set_ram_buffer_size_mb(0.0);
        let writer = IndexWriter::with_config(config);

        for i in 0..50 {
            writer.add_document(make_large_doc(i)).unwrap();
        }

        let files = writer.commit().unwrap().into_segment_files().unwrap();
        let num_segments = files.iter().filter(|f| f.name.ends_with(".si")).count();
        assert_eq!(num_segments, 1, "disabled flush should produce 1 segment");
    }

    // --- write_to_directory tests ---

    #[test]
    fn test_commit_write_to_directory() {
        use crate::store::memory::MemoryDirectory;

        let writer = make_three_doc_writer();
        let commit = writer.commit().unwrap();

        let mut dir = MemoryDirectory::new();
        let written = commit.write_to_directory(&mut dir).unwrap();

        // segments_1 + _0.si + _0.cfs + _0.cfe = 4
        assert_len_eq_x!(&written, 4);
        assert_eq!(written[0], "segments_1");
        assert_contains!(written, &"_0.si".to_string());
        assert_contains!(written, &"_0.cfs".to_string());
        assert_contains!(written, &"_0.cfe".to_string());

        // All files should exist in the directory
        let files = dir.list_all().unwrap();
        assert_len_eq_x!(&files, 4);
        for name in &written {
            assert!(
                dir.file_length(name).unwrap() > 0,
                "file {name} should be non-empty"
            );
        }
    }

    #[test]
    fn test_commit_write_to_directory_multi_segment() {
        use crate::store::memory::MemoryDirectory;

        // 5 docs with max_buffered_docs=2 → 3 segments
        let config = IndexWriterConfig::new().set_max_buffered_docs(2);
        let writer = IndexWriter::with_config(config);

        for i in 0..5 {
            let mut doc = Document::new();
            doc.add(document::keyword_field("path", &format!("/{i}.txt")));
            doc.add(document::long_field("modified", i as i64 * 100));
            doc.add(document::text_field("contents", &format!("doc number {i}")));
            writer.add_document(doc).unwrap();
        }

        let commit = writer.commit().unwrap();

        let mut dir = MemoryDirectory::new();
        let written = commit.write_to_directory(&mut dir).unwrap();

        // segments_1 + 3 segments x (si + cfs + cfe) = 10
        assert_len_eq_x!(&written, 10);
        assert_eq!(written[0], "segments_1");

        let files = dir.list_all().unwrap();
        assert_len_eq_x!(&files, 10);

        // Verify all segment files are present
        let si_count = written.iter().filter(|n| n.ends_with(".si")).count();
        assert_eq!(si_count, 3);
    }

    // --- Non-compound mode tests ---

    #[test]
    fn test_non_compound_mode() {
        let config = IndexWriterConfig::new().set_use_compound_file(false);
        let writer = IndexWriter::with_config(config);

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/test.txt"));
        doc.add(document::long_field("modified", 1000));
        doc.add(document::text_field("contents", "hello world"));
        writer.add_document(doc).unwrap();

        let result = writer.commit().unwrap();
        let file_names = result.file_names().to_vec();

        // Should not have .cfs/.cfe files
        assert!(
            !file_names.iter().any(|n| n.ends_with(".cfs")),
            "non-compound should not produce .cfs"
        );
        assert!(
            !file_names.iter().any(|n| n.ends_with(".cfe")),
            "non-compound should not produce .cfe"
        );

        // Should have individual sub-files
        assert!(file_names.iter().any(|n| n.ends_with(".fnm")));
        assert!(file_names.iter().any(|n| n.ends_with(".si")));
        assert!(file_names.iter().any(|n| n.starts_with("segments_")));

        // All files should have valid headers/footers
        let files = result.into_segment_files().unwrap();
        for f in &files {
            assert!(
                f.data.len() >= FOOTER_LENGTH + 4,
                "file {} too small: {} bytes",
                f.name,
                f.data.len()
            );
            let magic = i32::from_be_bytes(f.data[0..4].try_into().unwrap());
            assert_eq!(magic, CODEC_MAGIC, "file {} has wrong header magic", f.name);
        }
    }

    #[test]
    fn test_non_compound_multi_segment() {
        let config = IndexWriterConfig::new()
            .set_use_compound_file(false)
            .set_max_buffered_docs(2);
        let writer = IndexWriter::with_config(config);

        for i in 0..5 {
            let mut doc = Document::new();
            doc.add(document::keyword_field("path", &format!("/{i}.txt")));
            doc.add(document::long_field("modified", i as i64));
            doc.add(document::text_field("contents", &format!("doc {i}")));
            writer.add_document(doc).unwrap();
        }

        let result = writer.commit().unwrap();
        let file_names = result.file_names();

        // 3 segments, each with individual files
        let si_count = file_names.iter().filter(|n| n.ends_with(".si")).count();
        assert_eq!(si_count, 3, "expected 3 segments");

        // No compound files
        assert!(!file_names.iter().any(|n| n.ends_with(".cfs")));
        assert!(!file_names.iter().any(|n| n.ends_with(".cfe")));
    }

    // --- with_directory constructor tests ---

    #[test]
    fn test_with_directory() {
        use crate::store::memory::MemoryDirectory;

        let dir = Box::new(MemoryDirectory::new());
        let writer = IndexWriter::with_directory(dir);

        let mut doc = Document::new();
        doc.add(document::keyword_field("path", "/test.txt"));
        doc.add(document::text_field("contents", "hello"));
        writer.add_document(doc).unwrap();

        let result = writer.commit().unwrap();
        // segments_1 + _0.si + _0.cfs + _0.cfe = 4
        assert_len_eq_x!(&result.file_names(), 4);
    }

    #[test]
    fn test_with_config_and_directory() {
        use crate::store::memory::MemoryDirectory;

        let config = IndexWriterConfig::new().set_max_buffered_docs(1);
        let dir = Box::new(MemoryDirectory::new());
        let writer = IndexWriter::with_config_and_directory(config, dir);

        for i in 0..3 {
            let mut doc = Document::new();
            doc.add(document::keyword_field("path", &format!("/{i}.txt")));
            doc.add(document::text_field("contents", "test"));
            writer.add_document(doc).unwrap();
        }

        let result = writer.commit().unwrap();
        // segments_1 + 3 segments x (si + cfs + cfe) = 10
        assert_len_eq_x!(&result.file_names(), 10);
    }
}
