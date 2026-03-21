// SPDX-License-Identifier: Apache-2.0
//! Postings list writer that encodes doc IDs, frequencies, positions, and offsets.

use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::codecs::competitive_impact::{CompetitiveImpactAccumulator, Impact, NormsLookup};
use crate::document::IndexOptions;
use crate::encoding::zigzag;
use crate::index::FieldInfo;
use crate::index::index_file_names::segment_file_name;
use crate::store::{DataOutput, IndexOutput, SharedDirectory, VecOutput};

use super::for_util::{self, BLOCK_SIZE};
use super::postings_format::{
    self, DOC_CODEC, DOC_EXTENSION, IntBlockTermState, META_CODEC, META_EXTENSION, POS_CODEC,
    POS_EXTENSION, VERSION_CURRENT,
};

/// Buffers position deltas and PFOR-encodes them in blocks of 128.
/// Extracts the repeated pattern from singleton, VInt, and block encoding paths.
struct PositionEncoder {
    buffer: [i32; BLOCK_SIZE],
    buffer_upto: usize,
}

impl PositionEncoder {
    fn new() -> Self {
        Self {
            buffer: [0i32; BLOCK_SIZE],
            buffer_upto: 0,
        }
    }

    fn buffer_upto(&self) -> usize {
        self.buffer_upto
    }

    /// Buffer a position delta, flushing a PFOR block when full.
    fn add_position(&mut self, pos_delta: i32, pos_out: &mut dyn DataOutput) -> io::Result<()> {
        self.buffer[self.buffer_upto] = pos_delta;
        self.buffer_upto += 1;
        if self.buffer_upto == BLOCK_SIZE {
            let mut longs = [0i64; BLOCK_SIZE];
            for (i, &val) in self.buffer.iter().enumerate().take(BLOCK_SIZE) {
                longs[i] = val as i64;
            }
            for_util::pfor_encode(&mut longs, pos_out)?;
            self.buffer_upto = 0;
        }
        Ok(())
    }

    /// Write VInt tail for remaining buffered deltas and compute last_pos_block_offset.
    fn finish(
        &self,
        pos_out: &mut dyn IndexOutput,
        total_term_freq: i64,
        pos_start_fp: i64,
    ) -> io::Result<i64> {
        let last_pos_block_offset = if total_term_freq > BLOCK_SIZE as i64 {
            pos_out.file_pointer() as i64 - pos_start_fp
        } else {
            -1
        };
        for &delta in &self.buffer[..self.buffer_upto] {
            pos_out.write_vint(delta)?;
        }
        Ok(last_pos_block_offset)
    }
}

/// Groups buffers and per-term state for block encoding's flush_doc_block.
/// Reduces the 14-parameter free function to a 5-parameter method.
struct BlockFlushState {
    level0_buf: Vec<u8>,
    level1_buf: Vec<u8>,
    scratch_buf: Vec<u8>,
    /// Reusable bitset buffer for doc ID encoding (max BLOCK_SIZE/2 = 64 longs).
    bitset_buf: [u64; BLOCK_SIZE / 2],
    level0_last_doc_id: i32,
    level0_last_pos_fp: i64,
    write_freqs: bool,
    write_positions: bool,
    max_num_impacts_at_level0: i32,
    max_impact_num_bytes_at_level0: i32,
    max_num_impacts_at_level1: i32,
    max_impact_num_bytes_at_level1: i32,
    /// Accumulates competitive (freq, norm) pairs for the current level 0 block.
    level0_accumulator: CompetitiveImpactAccumulator,
    /// Accumulates competitive impacts across level 0 blocks for level 1 skip data.
    level1_accumulator: CompetitiveImpactAccumulator,
}

impl BlockFlushState {
    fn new(pos_start_fp: i64, write_freqs: bool, write_positions: bool) -> Self {
        Self {
            level0_buf: Vec::new(),
            level1_buf: Vec::new(),
            scratch_buf: Vec::new(),
            bitset_buf: [0u64; BLOCK_SIZE / 2],
            level0_last_doc_id: -1,
            level0_last_pos_fp: pos_start_fp,
            write_freqs,
            write_positions,
            max_num_impacts_at_level0: 0,
            max_impact_num_bytes_at_level0: 0,
            max_num_impacts_at_level1: 0,
            max_impact_num_bytes_at_level1: 0,
            level0_accumulator: CompetitiveImpactAccumulator::new(),
            level1_accumulator: CompetitiveImpactAccumulator::new(),
        }
    }

    /// Flush one full doc block (128 docs) with skip data.
    fn flush_doc_block(
        &mut self,
        doc_delta_buffer: &[i32; BLOCK_SIZE],
        freq_buffer: &mut [i32; BLOCK_SIZE],
        last_doc_id: i32,
        pos_buffer_upto: usize,
        pos_fp: i64,
    ) -> io::Result<()> {
        self.level0_buf.clear();
        self.scratch_buf.clear();

        // 1. Write impact data (only when we have freqs)
        if self.write_freqs {
            let impacts = self.level0_accumulator.get_competitive_freq_norm_pairs();

            let impact_start = self.scratch_buf.len();
            let impact_bytes_len = write_impacts(&impacts, &mut self.scratch_buf)?;

            if impacts.len() as i32 > self.max_num_impacts_at_level0 {
                self.max_num_impacts_at_level0 = impacts.len() as i32;
            }
            if impact_bytes_len as i32 > self.max_impact_num_bytes_at_level0 {
                self.max_impact_num_bytes_at_level0 = impact_bytes_len as i32;
            }

            {
                let mut enc = VecOutput(&mut self.level0_buf);
                enc.write_vlong(impact_bytes_len as i64)?;
            }
            let impact_end = impact_start + impact_bytes_len;
            self.level0_buf
                .extend_from_slice(&self.scratch_buf[impact_start..impact_end]);
            self.scratch_buf.clear();

            if self.write_positions {
                let mut enc = VecOutput(&mut self.level0_buf);
                enc.write_vlong(pos_fp - self.level0_last_pos_fp)?;
                enc.write_byte(pos_buffer_upto as u8)?;
            }
        }

        let num_skip_bytes_before_doc = self.level0_buf.len();

        // 2. Doc encoding: decide between consecutive, FOR, or bitset
        let doc_range = (last_doc_id - self.level0_last_doc_id) as usize;
        let bpv = for_util::for_delta_bits_required(doc_delta_buffer);

        if doc_range == BLOCK_SIZE {
            self.level0_buf.push(0u8);
        } else {
            let num_bitset_longs = bits2words(doc_range);
            let num_bits_next_bpv = (bpv + 1).min(32) as usize * BLOCK_SIZE;

            if num_bits_next_bpv <= doc_range {
                self.level0_buf.push(bpv as u8);
                let mut enc = VecOutput(&mut self.level0_buf);
                for_util::for_delta_encode(bpv, doc_delta_buffer, &mut enc)?;
            } else {
                assert!(num_bitset_longs <= BLOCK_SIZE / 2);
                self.level0_buf.push((-(num_bitset_longs as i8)) as u8);
                // Reuse stack-resident bitset buffer instead of heap-allocating
                self.bitset_buf[..num_bitset_longs].fill(0);
                let mut s: i32 = -1;
                for &delta in doc_delta_buffer.iter() {
                    s += delta;
                    let word = s as usize / 64;
                    let bit = s as usize % 64;
                    self.bitset_buf[word] |= 1u64 << bit;
                }
                let mut enc = VecOutput(&mut self.level0_buf);
                for &word in &self.bitset_buf[..num_bitset_longs] {
                    enc.write_le_long(word as i64)?;
                }
            }
        }

        // 3. Freq encoding
        if self.write_freqs {
            let mut longs = [0i64; BLOCK_SIZE];
            for i in 0..BLOCK_SIZE {
                longs[i] = freq_buffer[i] as i64;
            }
            let mut enc = VecOutput(&mut self.level0_buf);
            for_util::pfor_encode(&mut longs, &mut enc)?;
        }

        // 4. Write skip header to scratch_buf
        let doc_delta = last_doc_id - self.level0_last_doc_id;
        write_vint15(&mut VecOutput(&mut self.scratch_buf), doc_delta)?;
        write_vlong15(
            &mut VecOutput(&mut self.scratch_buf),
            self.level0_buf.len() as i64,
        )?;

        let num_skip_bytes = num_skip_bytes_before_doc + self.scratch_buf.len();

        // 5. Append to level1_buf: VLong(numSkipBytes) + scratch + level0
        {
            let mut enc = VecOutput(&mut self.level1_buf);
            enc.write_vlong(num_skip_bytes as i64)?;
        }
        self.level1_buf.extend_from_slice(&self.scratch_buf);
        self.level1_buf.extend_from_slice(&self.level0_buf);

        // Update tracking state for next block
        self.level0_last_doc_id = last_doc_id;
        self.level0_last_pos_fp = pos_fp;
        if self.write_freqs {
            self.level1_accumulator.add_all(&self.level0_accumulator);
        }
        self.level0_accumulator.clear();

        Ok(())
    }
}

/// Writes postings (.doc, .pos, .psm) files.
pub struct PostingsWriter {
    doc_out: Box<dyn IndexOutput>,
    pos_out: Option<Box<dyn IndexOutput>>,
    meta_out: Box<dyn IndexOutput>,
    // Track impact metadata (minimal for MVP)
    max_num_impacts_at_level0: i32,
    max_impact_num_bytes_at_level0: i32,
    max_num_impacts_at_level1: i32,
    max_impact_num_bytes_at_level1: i32,
}

impl PostingsWriter {
    /// Creates a new PostingsWriter that streams to outputs from the directory.
    pub fn new(
        directory: &SharedDirectory,
        segment: &str,
        suffix: &str,
        id: &[u8; 16],
        has_positions: bool,
    ) -> io::Result<Self> {
        let doc_name = segment_file_name(segment, suffix, DOC_EXTENSION);
        let meta_name = segment_file_name(segment, suffix, META_EXTENSION);

        let (mut doc_out, mut meta_out, mut pos_out) = {
            let mut dir = directory.lock().unwrap();
            let doc_out = dir.create_output(&doc_name)?;
            let meta_out = dir.create_output(&meta_name)?;
            let pos_out = if has_positions {
                let pos_name = segment_file_name(segment, suffix, POS_EXTENSION);
                Some(dir.create_output(&pos_name)?)
            } else {
                None
            };
            (doc_out, meta_out, pos_out)
        };

        codec_util::write_index_header(&mut *doc_out, DOC_CODEC, VERSION_CURRENT, id, suffix)?;
        codec_util::write_index_header(&mut *meta_out, META_CODEC, VERSION_CURRENT, id, suffix)?;

        if let Some(ref mut pos_out) = pos_out {
            codec_util::write_index_header(&mut **pos_out, POS_CODEC, VERSION_CURRENT, id, suffix)?;
        }

        Ok(Self {
            doc_out,
            pos_out,
            meta_out,
            max_num_impacts_at_level0: 0,
            max_impact_num_bytes_at_level0: 0,
            max_num_impacts_at_level1: 0,
            max_impact_num_bytes_at_level1: 0,
        })
    }

    /// Write postings for one term. Returns metadata for the term dictionary.
    ///
    /// - Singleton (docFreq==1): docID pulsed into IntBlockTermState, nothing to .doc
    /// - Block encoding (docFreq >= 128): FOR-encoded blocks with skip data
    /// - VInt tail (docFreq < 128):
    ///   - With freqs: GroupVInt doc deltas with freq bits, then freqs for non-1
    ///   - Without freqs (DOCS only): GroupVInt doc deltas
    /// - Positions: PFOR-encoded blocks of 128 + VInt tail for remainder
    pub fn write_term(
        &mut self,
        postings: &[(i32, i32, &[i32])], // (doc_id, freq, positions)
        index_options: IndexOptions,
        norms: &NormsLookup,
    ) -> io::Result<IntBlockTermState> {
        let doc_freq = postings.len() as i32;
        let write_freqs = index_options.has_freqs();
        let write_positions = index_options.has_positions();

        let total_term_freq: i64 = if write_freqs {
            postings.iter().map(|&(_, freq, _)| freq as i64).sum()
        } else {
            -1
        };

        let doc_start_fp = self.doc_out.file_pointer() as i64;
        let pos_start_fp = self
            .pos_out
            .as_ref()
            .map(|p| p.file_pointer() as i64)
            .unwrap_or(0);

        if doc_freq == 1 {
            // Singleton: pulse docID into term dictionary
            let singleton_doc_id = postings[0].0;
            debug!(
                "postings_writer: singleton term, doc_id={}",
                singleton_doc_id
            );

            // Write positions for singleton
            let mut last_pos_block_offset = -1i64;
            if write_positions && let Some(ref mut pos_out) = self.pos_out {
                let (_, _, positions) = postings[0];
                let mut pe = PositionEncoder::new();
                let mut last_pos = 0i32;
                for &pos in positions {
                    pe.add_position(pos - last_pos, &mut **pos_out)?;
                    last_pos = pos;
                }
                last_pos_block_offset = pe.finish(&mut **pos_out, total_term_freq, pos_start_fp)?;
            }

            Ok(IntBlockTermState {
                doc_freq,
                total_term_freq,
                doc_start_fp,
                pos_start_fp,

                last_pos_block_offset,
                singleton_doc_id,
            })
        } else if doc_freq >= BLOCK_SIZE as i32 {
            // Block encoding path for high-frequency terms
            debug!("postings_writer: block encoding, doc_freq={}", doc_freq);
            self.write_term_blocks(
                postings,
                doc_freq,
                total_term_freq,
                doc_start_fp,
                pos_start_fp,
                write_freqs,
                write_positions,
                norms,
            )
        } else {
            // VInt block (docFreq < BLOCK_SIZE)
            let mut doc_deltas: Vec<i32> = Vec::with_capacity(doc_freq as usize);
            let mut freqs: Vec<i32> = Vec::with_capacity(doc_freq as usize);
            let mut last_doc_id = -1i32;

            for &(doc_id, freq, _) in postings {
                let delta = doc_id - last_doc_id;
                doc_deltas.push(delta);
                freqs.push(freq);
                last_doc_id = doc_id;
            }

            write_vint_block(
                &mut *self.doc_out,
                &mut doc_deltas,
                &freqs,
                doc_freq as usize,
                write_freqs,
            )?;

            debug!(
                "postings_writer: VInt block, doc_freq={}, bytes_written={}",
                doc_freq,
                self.doc_out.file_pointer() as i64 - doc_start_fp
            );

            // Write positions
            let mut last_pos_block_offset = -1i64;
            if write_positions && let Some(ref mut pos_out) = self.pos_out {
                let mut pe = PositionEncoder::new();
                for &(_, _, positions) in postings {
                    let mut last_pos = 0i32;
                    for &pos in positions {
                        pe.add_position(pos - last_pos, &mut **pos_out)?;
                        last_pos = pos;
                    }
                }
                last_pos_block_offset = pe.finish(&mut **pos_out, total_term_freq, pos_start_fp)?;
            }

            Ok(IntBlockTermState {
                doc_freq,
                total_term_freq,
                doc_start_fp,
                pos_start_fp,

                last_pos_block_offset,
                singleton_doc_id: -1,
            })
        }
    }

    /// Write a term with docFreq >= BLOCK_SIZE using block encoding with skip data.
    #[allow(clippy::too_many_arguments)]
    fn write_term_blocks(
        &mut self,
        postings: &[(i32, i32, &[i32])],
        doc_freq: i32,
        total_term_freq: i64,
        doc_start_fp: i64,
        pos_start_fp: i64,
        write_freqs: bool,
        write_positions: bool,
        norms: &NormsLookup,
    ) -> io::Result<IntBlockTermState> {
        let mut doc_delta_buffer = [0i32; BLOCK_SIZE];
        let mut freq_buffer = [0i32; BLOCK_SIZE];
        let mut doc_buffer_upto = 0usize;

        let mut pe = PositionEncoder::new();
        let mut bfs = BlockFlushState::new(pos_start_fp, write_freqs, write_positions);

        let mut last_doc_id = -1i32;
        let mut doc_count = 0usize;

        for &(doc_id, freq, positions) in postings {
            // Buffer doc delta and freq
            let doc_delta = doc_id - last_doc_id;
            doc_delta_buffer[doc_buffer_upto] = doc_delta;
            if write_freqs {
                freq_buffer[doc_buffer_upto] = freq;
                bfs.level0_accumulator.add(freq, norms.get(doc_id));
            }
            last_doc_id = doc_id;

            // Buffer positions
            if write_positions && let Some(ref mut pos_out) = self.pos_out {
                let mut last_pos = 0i32;
                for &pos in positions {
                    pe.add_position(pos - last_pos, &mut **pos_out)?;
                    last_pos = pos;
                }
            }

            doc_buffer_upto += 1;
            doc_count += 1;

            // Flush a full doc block
            if doc_buffer_upto == BLOCK_SIZE {
                bfs.flush_doc_block(
                    &doc_delta_buffer,
                    &mut freq_buffer,
                    doc_id,
                    pe.buffer_upto(),
                    self.pos_out
                        .as_ref()
                        .map(|p| p.file_pointer() as i64)
                        .unwrap_or(0),
                )?;
                doc_buffer_upto = 0;
            }
        }

        // Flush remaining VInt tail (< BLOCK_SIZE docs)
        if doc_buffer_upto > 0 {
            let mut tail_deltas = doc_delta_buffer[..doc_buffer_upto].to_vec();
            write_vint_block_to_buf(
                &mut bfs.level1_buf,
                &mut tail_deltas,
                &freq_buffer[..doc_buffer_upto],
                doc_buffer_upto,
                write_freqs,
            )?;
        }

        // Copy level1_buf to doc_out
        // For MVP we assert doc_count <= LEVEL1_NUM_DOCS (no level1 skip needed)
        assert!(
            doc_count <= postings_format::LEVEL1_NUM_DOCS,
            "doc_count {} exceeds LEVEL1_NUM_DOCS {} — level1 skip not yet implemented",
            doc_count,
            postings_format::LEVEL1_NUM_DOCS,
        );
        self.doc_out.write_bytes(&bfs.level1_buf)?;

        // Write remaining position VInt tail
        let mut last_pos_block_offset = -1i64;
        if write_positions && let Some(ref mut pos_out) = self.pos_out {
            last_pos_block_offset = pe.finish(&mut **pos_out, total_term_freq, pos_start_fp)?;
        }

        // Note: level 1 impact metadata (max_num_impacts_at_level1,
        // max_impact_num_bytes_at_level1) is only updated inside writeLevel1SkipData(),
        // which triggers every LEVEL1_NUM_DOCS (4096) docs. For terms below that
        // threshold, these fields correctly stay at 0 — matching Java's behavior.

        // Copy impact metadata back from BlockFlushState
        self.max_num_impacts_at_level0 = self
            .max_num_impacts_at_level0
            .max(bfs.max_num_impacts_at_level0);
        self.max_impact_num_bytes_at_level0 = self
            .max_impact_num_bytes_at_level0
            .max(bfs.max_impact_num_bytes_at_level0);
        self.max_num_impacts_at_level1 = self
            .max_num_impacts_at_level1
            .max(bfs.max_num_impacts_at_level1);
        self.max_impact_num_bytes_at_level1 = self
            .max_impact_num_bytes_at_level1
            .max(bfs.max_impact_num_bytes_at_level1);

        Ok(IntBlockTermState {
            doc_freq,
            total_term_freq,
            doc_start_fp,
            pos_start_fp,
            last_pos_block_offset,
            singleton_doc_id: -1,
        })
    }

    /// Encode a term's metadata for the block tree terms dictionary.
    /// Called by BlockTreeTermsWriter for each term in a block.
    ///
    pub fn encode_term(
        &self,
        out: &mut Vec<u8>,
        _field_info: &FieldInfo,
        state: &IntBlockTermState,
        last_state: &IntBlockTermState,
        write_positions: bool,
    ) -> io::Result<()> {
        let mut buf = VecOutput(out);

        if last_state.singleton_doc_id != -1
            && state.singleton_doc_id != -1
            && state.doc_start_fp == last_state.doc_start_fp
        {
            // Both singletons at same file position: encode docID delta
            let delta = state.singleton_doc_id as i64 - last_state.singleton_doc_id as i64;
            let encoded = zigzag::encode_i64(delta);
            buf.write_vlong((encoded << 1) | 0x01)?;
        } else {
            // Normal: encode file pointer delta
            buf.write_vlong((state.doc_start_fp - last_state.doc_start_fp) << 1)?;
            if state.singleton_doc_id != -1 {
                buf.write_vint(state.singleton_doc_id)?;
            }
        }

        if write_positions {
            buf.write_vlong(state.pos_start_fp - last_state.pos_start_fp)?;
        }

        if write_positions && state.last_pos_block_offset != -1 {
            buf.write_vlong(state.last_pos_block_offset)?;
        }

        Ok(())
    }

    /// Finalize: write footers, drop outputs (auto-persists to directory).
    /// Returns the file names written.
    pub fn finish(mut self) -> io::Result<Vec<String>> {
        let mut names = Vec::new();

        // Write .doc footer
        codec_util::write_footer(&mut *self.doc_out)?;

        // Write .pos footer if present
        if let Some(ref mut pos_out) = self.pos_out {
            codec_util::write_footer(&mut **pos_out)?;
        }

        // Write .psm metadata: impact stats + file lengths + footer
        self.meta_out.write_le_int(self.max_num_impacts_at_level0)?;
        self.meta_out
            .write_le_int(self.max_impact_num_bytes_at_level0)?;
        self.meta_out.write_le_int(self.max_num_impacts_at_level1)?;
        self.meta_out
            .write_le_int(self.max_impact_num_bytes_at_level1)?;
        self.meta_out
            .write_le_long(self.doc_out.file_pointer() as i64)?;
        if let Some(ref pos_out) = self.pos_out {
            self.meta_out.write_le_long(pos_out.file_pointer() as i64)?;
        }
        codec_util::write_footer(&mut *self.meta_out)?;

        names.push(self.doc_out.name().to_string());
        if let Some(ref pos_out) = self.pos_out {
            names.push(pos_out.name().to_string());
        }
        names.push(self.meta_out.name().to_string());

        // Dropping self auto-persists all outputs to directory
        Ok(names)
    }
}

/// Write freq buffer with variable-length encoding and doc buffer with group-varint encoding.
fn write_vint_block(
    out: &mut dyn DataOutput,
    doc_deltas: &mut [i32],
    freqs: &[i32],
    num: usize,
    write_freqs: bool,
) -> io::Result<()> {
    write_vint_block_impl(out, doc_deltas, freqs, num, write_freqs)
}

/// Special vints encoded on 2 bytes if they require 15 bits or less.
fn write_vlong15(out: &mut dyn DataOutput, v: i64) -> io::Result<()> {
    debug_assert!(v >= 0);
    if (v & !0x7FFF_i64) == 0 {
        out.write_le_short(v as i16)?;
    } else {
        out.write_le_short((0x8000 | (v & 0x7FFF)) as i16)?;
        out.write_vlong(v >> 15)?;
    }
    Ok(())
}

fn write_vint15(out: &mut dyn DataOutput, v: i32) -> io::Result<()> {
    debug_assert!(v >= 0);
    write_vlong15(out, v as i64)
}

/// Delta-encodes competitive impacts into the buffer.
///
/// Each impact is encoded relative to the previous one:
/// - `freqDelta = freq - prevFreq - 1`
/// - `normDelta = norm - prevNorm - 1`
/// - If normDelta == 0: write `freqDelta << 1` as VInt
/// - Otherwise: write `(freqDelta << 1) | 1` as VInt, then normDelta as ZLong
///
/// Returns number of bytes written.
fn write_impacts(impacts: &[Impact], buf: &mut Vec<u8>) -> io::Result<usize> {
    let start = buf.len();
    let mut enc = VecOutput(buf);
    let mut prev = Impact { freq: 0, norm: 0 };
    for &impact in impacts {
        let freq_delta = impact.freq - prev.freq - 1;
        let norm_delta = impact.norm - prev.norm - 1;
        if norm_delta == 0 {
            enc.write_vint(freq_delta << 1)?;
        } else {
            enc.write_vint((freq_delta << 1) | 1)?;
            enc.write_zlong(norm_delta)?;
        }
        prev = impact;
    }
    let bytes_written = enc.0.len() - start;
    Ok(bytes_written)
}

/// Returns the number of longs needed to store the given number of bits.
fn bits2words(num_bits: usize) -> usize {
    num_bits.div_ceil(64)
}

/// Write VInt block to a Vec<u8> buffer (for the tail in block encoding).
fn write_vint_block_to_buf(
    buf: &mut Vec<u8>,
    doc_deltas: &mut [i32],
    freqs: &[i32],
    num: usize,
    write_freqs: bool,
) -> io::Result<()> {
    let mut enc = VecOutput(buf);
    write_vint_block_impl(&mut enc, doc_deltas, freqs, num, write_freqs)
}

/// Shared VInt block implementation that works with any DataOutput.
fn write_vint_block_impl(
    out: &mut dyn DataOutput,
    doc_deltas: &mut [i32],
    freqs: &[i32],
    num: usize,
    write_freqs: bool,
) -> io::Result<()> {
    if write_freqs {
        for i in 0..num {
            doc_deltas[i] = (doc_deltas[i] << 1) | if freqs[i] == 1 { 1 } else { 0 };
        }
    }
    out.write_group_vints(doc_deltas, num)?;
    if write_freqs {
        for &freq in &freqs[..num] {
            if freq != 1 {
                out.write_vint(freq)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::PointDimensionConfig;
    use crate::store::{MemoryDirectory, SharedDirectory};

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    #[test]
    fn test_singleton_term() {
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        let postings = vec![(5, 1, [].as_slice())];
        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqs,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.singleton_doc_id, 5);
        // Nothing written to .doc beyond the header
        let header_size = codec_util::index_header_length(DOC_CODEC, "");
        assert_eq!(pw.doc_out.file_pointer() as usize, header_size);
    }

    #[test]
    fn test_multi_doc_term_with_freqs() {
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        let postings = vec![
            (0, 1, [].as_slice()),
            (5, 3, [].as_slice()),
            (10, 1, [].as_slice()),
        ];
        let header_size = pw.doc_out.file_pointer();
        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqs,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 3);
        assert_eq!(state.singleton_doc_id, -1);
        assert_eq!(state.total_term_freq, 5); // 1 + 3 + 1
        assert_eq!(state.doc_start_fp, header_size as i64);
        // Something was written to .doc
        assert_gt!(pw.doc_out.file_pointer(), header_size);
    }

    #[test]
    fn test_multi_doc_term_docs_only() {
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        let postings = vec![(7, 1, [].as_slice()), (11, 1, [].as_slice())];
        let header_size = pw.doc_out.file_pointer();
        let state = pw
            .write_term(&postings, IndexOptions::Docs, &NormsLookup::no_norms())
            .unwrap();

        assert_eq!(state.doc_freq, 2);
        assert_eq!(state.singleton_doc_id, -1);
        assert_eq!(state.total_term_freq, -1); // DOCS only
        assert_gt!(pw.doc_out.file_pointer(), header_size);
    }

    #[test]
    fn test_term_with_positions_vint_tail() {
        // totalTermFreq < BLOCK_SIZE uses VInt tail only
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], true).unwrap();
        let positions0 = vec![0, 5, 10];
        let positions1 = vec![3];
        let postings = vec![(0, 3, positions0.as_slice()), (2, 1, positions1.as_slice())];
        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqsAndPositions,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 2);
        assert_eq!(state.total_term_freq, 4);
        assert_ge!(state.pos_start_fp, 0);
        // No PFOR blocks: last_pos_block_offset must be -1
        assert_eq!(state.last_pos_block_offset, -1);

        // Finish writing to flush output to directory
        let names = pw.finish().unwrap();
        let pos_name = names.iter().find(|n| n.ends_with(".pos")).unwrap();
        let pos_data = dir.lock().unwrap().read_file(pos_name).unwrap();
        let pos_header_size = codec_util::index_header_length(POS_CODEC, "");
        let footer_size = codec_util::FOOTER_LENGTH;
        let pos_bytes = &pos_data[pos_header_size..pos_data.len() - footer_size];
        // 4 positions with deltas: [0, 5, 10] -> deltas [0, 5, 5], [3] -> delta [3]
        // VInt encoding: 0, 5, 5, 3 = 4 bytes (all single-byte VInts)
        assert_len_eq_x!(&pos_bytes, 4);
        assert_eq!(pos_bytes[0], 0); // pos delta 0
        assert_eq!(pos_bytes[1], 5); // pos delta 5
        assert_eq!(pos_bytes[2], 5); // pos delta 5
        assert_eq!(pos_bytes[3], 3); // pos delta 3 (new doc, last_pos resets)
    }

    #[test]
    fn test_term_with_positions_pfor_one_block() {
        // Ported from Lucene103PostingsWriter: exactly BLOCK_SIZE positions = one PFOR block, no tail
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], true).unwrap();

        // Create a single doc with exactly 128 positions (0, 1, 2, ..., 127)
        let positions: Vec<i32> = (0..postings_format::BLOCK_SIZE as i32).collect();
        let postings = vec![(0, postings_format::BLOCK_SIZE as i32, positions.as_slice())];

        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqsAndPositions,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.total_term_freq, postings_format::BLOCK_SIZE as i64);
        assert_eq!(state.singleton_doc_id, 0);
        // Exactly BLOCK_SIZE: last_pos_block_offset = -1
        // (Java: totalTermFreq > BLOCK_SIZE is the threshold, not >=)
        assert_eq!(state.last_pos_block_offset, -1);

        // Verify PFOR block was written (pos data starts with a PFOR token byte)
        let pos_out = pw.pos_out.as_ref().unwrap();
        let pos_header_size = codec_util::index_header_length(POS_CODEC, "");
        let pos_data_len = pos_out.file_pointer() as usize - pos_header_size;
        // Must have written something (PFOR block, not empty)
        assert_gt!(pos_data_len, 0);
    }

    #[test]
    fn test_term_with_positions_pfor_plus_tail() {
        // Ported from Lucene103PostingsWriter: totalTermFreq > BLOCK_SIZE = PFOR blocks + VInt tail
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], true).unwrap();

        // 130 positions across 2 docs: first doc has 128 positions, second has 2
        let positions0: Vec<i32> = (0..128).collect();
        let positions1: Vec<i32> = vec![0, 5];
        let postings = vec![
            (0, 128, positions0.as_slice()),
            (1, 2, positions1.as_slice()),
        ];

        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqsAndPositions,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 2);
        assert_eq!(state.total_term_freq, 130);
        // totalTermFreq > BLOCK_SIZE: last_pos_block_offset must be set (not -1)
        assert_ne!(state.last_pos_block_offset, -1);
        assert_gt!(state.last_pos_block_offset, 0);

        // Verify the .pos file has data (PFOR block + VInt tail)
        let pos_out = pw.pos_out.as_ref().unwrap();
        let pos_header_size = codec_util::index_header_length(POS_CODEC, "");
        let pos_data_len = pos_out.file_pointer() as usize - pos_header_size;
        assert_gt!(
            pos_data_len,
            0,
            "PFOR block + VInt tail should produce output"
        );

        // last_pos_block_offset points to the start of the VInt tail
        // It must be less than the total pos data length (tail comes after blocks)
        assert_lt!(
            state.last_pos_block_offset as usize,
            pos_data_len,
            "last_pos_block_offset ({}) should be < total pos data ({})",
            state.last_pos_block_offset,
            pos_data_len
        );
    }

    #[test]
    fn test_term_with_positions_pfor_multiple_blocks() {
        // Ported from Lucene103PostingsWriter: multiple PFOR blocks + tail
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], true).unwrap();

        // 300 positions: 2 full PFOR blocks (256) + 44 VInt tail
        let positions: Vec<i32> = (0..300).collect();
        let postings = vec![(0, 300, positions.as_slice())];

        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqsAndPositions,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 1);
        assert_eq!(state.total_term_freq, 300);
        assert_ne!(state.last_pos_block_offset, -1);

        let pos_out = pw.pos_out.as_ref().unwrap();
        let pos_header_size = codec_util::index_header_length(POS_CODEC, "");
        let pos_data_len = pos_out.file_pointer() as usize - pos_header_size;

        // Must have written data (2 PFOR blocks + 44 VInt tail)
        assert_gt!(
            pos_data_len,
            44,
            "2 PFOR blocks + tail should be larger than tail alone"
        );

        // last_pos_block_offset should point past the 2 PFOR blocks, before the VInt tail
        assert_lt!(
            state.last_pos_block_offset as usize,
            pos_data_len,
            "last_pos_block_offset ({}) should be < total pos data ({})",
            state.last_pos_block_offset,
            pos_data_len
        );
    }

    #[test]
    fn test_term_with_positions_exactly_two_blocks() {
        // Ported from Lucene103PostingsWriter: exactly 2*BLOCK_SIZE positions = 2 PFOR blocks, no tail
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], true).unwrap();

        let positions: Vec<i32> = (0..256).collect();
        let postings = vec![(0, 256, positions.as_slice())];

        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqsAndPositions,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.total_term_freq, 256);
        assert_ne!(state.last_pos_block_offset, -1);

        let pos_out = pw.pos_out.as_ref().unwrap();
        let pos_header_size = codec_util::index_header_length(POS_CODEC, "");
        let pos_data_len = pos_out.file_pointer() as usize - pos_header_size;

        // 2 PFOR blocks, no VInt tail
        assert_gt!(pos_data_len, 0);

        // last_pos_block_offset should equal total length (tail is empty, offset is at end)
        assert_eq!(
            state.last_pos_block_offset as usize, pos_data_len,
            "With no VInt tail, last_pos_block_offset should equal total pos data length"
        );
    }

    #[test]
    fn test_term_with_positions_multi_doc_block_boundary() {
        // Verify position deltas reset per document when spanning a PFOR block boundary
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], true).unwrap();

        // Doc 0: 100 positions (0..100), Doc 1: 50 positions (0..50)
        // Total = 150 > BLOCK_SIZE, so we get 1 PFOR block + 22 VInt tail
        // Crucially, last_pos resets at doc boundary: doc1's first delta is 0, not (0 - 99)
        let positions0: Vec<i32> = (0..100).collect();
        let positions1: Vec<i32> = (0..50).collect();
        let postings = vec![
            (0, 100, positions0.as_slice()),
            (1, 50, positions1.as_slice()),
        ];

        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqsAndPositions,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.total_term_freq, 150);
        assert_ne!(state.last_pos_block_offset, -1);

        // Verify positions were written (no panic from negative deltas)
        let pos_out = pw.pos_out.as_ref().unwrap();
        let pos_header_size = codec_util::index_header_length(POS_CODEC, "");
        assert_gt!(pos_out.file_pointer() as usize, pos_header_size);
    }

    #[test]
    fn test_term_with_positions_large_deltas() {
        // Ported from TestPForUtil: positions with large gaps need more bits per value
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], true).unwrap();

        // 128 positions with gaps of 1000 each — requires ~10 bits per value
        let positions: Vec<i32> = (0..128).map(|i| i * 1000).collect();
        let postings = vec![(0, 128, positions.as_slice())];

        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqsAndPositions,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.total_term_freq, 128);
        // Exactly BLOCK_SIZE: last_pos_block_offset = -1
        assert_eq!(state.last_pos_block_offset, -1);

        let pos_out = pw.pos_out.as_ref().unwrap();
        let pos_header_size = codec_util::index_header_length(POS_CODEC, "");
        let pos_data_len = pos_out.file_pointer() as usize - pos_header_size;

        // Deltas are all 1000 (except first which is 0). bits_required(1000) = 10.
        // PFOR block: token(1) + 10*128/8 = 1 + 160 = 161 bytes
        assert!(
            pos_data_len > 16 && pos_data_len < 200,
            "PFOR block with 10bpv should be ~161 bytes, got {}",
            pos_data_len
        );
    }

    #[test]
    fn test_term_with_positions_pfor_compresses_real_data() {
        // Verify PFOR is more compact than VInt for realistic position data
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], true).unwrap();

        // 256 positions with varied gaps (simulating real text positions)
        // Mix of common deltas (1-5) with occasional larger gaps
        let mut positions = Vec::with_capacity(256);
        let mut pos = 0i32;
        for i in 0..256 {
            positions.push(pos);
            // Vary the gap: mostly small (1-3), occasionally larger
            pos += match i % 10 {
                0 => 15,
                1 => 8,
                _ => 1 + (i % 3),
            };
        }
        let postings = vec![(0, 256, positions.as_slice())];

        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqsAndPositions,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.total_term_freq, 256);
        assert_ne!(state.last_pos_block_offset, -1);

        let pos_out = pw.pos_out.as_ref().unwrap();
        let pos_header_size = codec_util::index_header_length(POS_CODEC, "");
        let pos_data_len = pos_out.file_pointer() as usize - pos_header_size;

        // With varied deltas (max ~15, ~4 bpv), PFOR should be much smaller than VInt.
        // VInt for 256 values averaging ~2 bytes each = ~512 bytes.
        // PFOR for 2 blocks at ~4bpv each = 2*(1 + 64) = ~130 bytes.
        assert_lt!(
            pos_data_len,
            256,
            "PFOR should compress 256 varied position deltas below 256 bytes, got {}",
            pos_data_len
        );
    }

    #[test]
    fn test_encode_term_singleton() {
        let dir = test_directory();
        let pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        let fi = FieldInfo::new(
            "test".to_string(),
            0,
            false,
            false,
            IndexOptions::DocsAndFreqs,
            crate::document::DocValuesType::None,
            PointDimensionConfig::default(),
        );

        let empty = IntBlockTermState::new();
        let state = IntBlockTermState {
            doc_freq: 1,
            total_term_freq: 1,
            doc_start_fp: 0,
            pos_start_fp: 0,
            last_pos_block_offset: -1,
            singleton_doc_id: 5,
        };

        let mut buf = Vec::new();
        pw.encode_term(&mut buf, &fi, &state, &empty, false)
            .unwrap();

        // First VLong: (doc_start_fp_delta << 1) = (0 << 1) = 0
        assert_eq!(buf[0], 0);
        // Then VInt(singletonDocID=5)
        assert_eq!(buf[1], 5);
    }

    #[test]
    fn test_finish_produces_files() {
        let dir = test_directory();
        let pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], true).unwrap();
        let names = pw.finish().unwrap();

        // Should produce .doc, .pos, .psm files
        assert_len_eq_x!(&names, 3);
        assert_ends_with!(names[0], ".doc");
        assert_ends_with!(names[1], ".pos");
        assert_ends_with!(names[2], ".psm");

        // Each file should have at least header + footer
        for name in &names {
            let data = dir.lock().unwrap().read_file(name).unwrap();
            assert_ge!(
                data.len(),
                codec_util::FOOTER_LENGTH,
                "file {name} too small: {} bytes",
                data.len()
            );
        }
    }

    // --- Tests for vint15/vlong15 helpers ---

    #[test]
    fn test_vint15_small_value() {
        // Value <= 0x7FFF fits in 2 bytes (a short)
        let mut buf = Vec::new();
        write_vint15(&mut VecOutput(&mut buf), 0).unwrap();
        assert_len_eq_x!(&buf, 2); // always 2 bytes (a short)

        let mut buf = Vec::new();
        write_vint15(&mut VecOutput(&mut buf), 0x7FFF).unwrap();
        assert_len_eq_x!(&buf, 2);
    }

    #[test]
    fn test_vint15_large_value() {
        // Value > 0x7FFF needs 2 bytes (short with high bit) + VLong
        let mut buf = Vec::new();
        write_vint15(&mut VecOutput(&mut buf), 0x8000).unwrap();
        assert_gt!(buf.len(), 2); // short + at least 1 vlong byte
        // High bit of first short should be set
        // Low byte first, high byte second
        assert_ne!(buf[1] & 0x80, 0, "high bit of short should be set");
    }

    #[test]
    fn test_vlong15_roundtrip() {
        // Verify various values encode without panic
        for v in [0i64, 1, 127, 128, 0x7FFF, 0x8000, 0xFFFF, 100000] {
            let mut buf = Vec::new();
            write_vlong15(&mut VecOutput(&mut buf), v).unwrap();
            assert_not_empty!(buf);
        }
    }

    // --- Tests for write_impacts ---

    #[test]
    fn test_write_impacts_single_norm_one() {
        // Single impact with freq=1, norm=1
        // freqDelta = 1 - 0 - 1 = 0, normDelta = 1 - 0 - 1 = 0
        // Encoding: VInt(0 << 1) = VInt(0)
        let impacts = [Impact { freq: 1, norm: 1 }];
        let mut buf = Vec::new();
        let len = write_impacts(&impacts, &mut buf).unwrap();
        assert_eq!(len, 1);
        assert_eq!(buf[0], 0);

        // Single impact with freq=5, norm=1
        // freqDelta = 5 - 0 - 1 = 4, normDelta = 1 - 0 - 1 = 0
        // Encoding: VInt(4 << 1) = VInt(8)
        let impacts = [Impact { freq: 5, norm: 1 }];
        let mut buf = Vec::new();
        let len = write_impacts(&impacts, &mut buf).unwrap();
        assert_eq!(len, 1);
        assert_eq!(buf[0], 8);

        // Larger freq
        // freqDelta = 100 - 0 - 1 = 99, normDelta = 0
        // VInt(99 << 1) = VInt(198) = 2 bytes
        let impacts = [Impact { freq: 100, norm: 1 }];
        let mut buf = Vec::new();
        let len = write_impacts(&impacts, &mut buf).unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn test_write_impacts_with_norm() {
        // Single impact with freq=3, norm=5
        // freqDelta = 3 - 0 - 1 = 2, normDelta = 5 - 0 - 1 = 4
        // Encoding: VInt((2 << 1) | 1) = VInt(5), ZLong(4) = VLong(zigzag(4)) = VLong(8)
        let impacts = [Impact { freq: 3, norm: 5 }];
        let mut buf = Vec::new();
        let len = write_impacts(&impacts, &mut buf).unwrap();
        assert_ge!(len, 2); // At least VInt + ZLong
        assert_eq!(buf[0], 5); // (2 << 1) | 1

        // Two impacts: delta-encoded
        let impacts = [Impact { freq: 3, norm: 5 }, Impact { freq: 10, norm: 13 }];
        let mut buf = Vec::new();
        let len = write_impacts(&impacts, &mut buf).unwrap();
        assert_gt!(len, 2); // Two impacts encoded
    }

    // --- Tests for block encoding (docFreq >= 128) ---

    #[test]
    fn test_block_encoding_128_docs() {
        // Exactly 128 docs: 1 full block, no VInt tail
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        let postings: Vec<(i32, i32, &[i32])> =
            (0..128).map(|i| (i, 1i32, [].as_slice())).collect();
        let header_size = pw.doc_out.file_pointer();
        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqs,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 128);
        assert_eq!(state.singleton_doc_id, -1);
        assert_gt!(pw.doc_out.file_pointer(), header_size);
        // Should have written block data (skip header + doc encoding + freq encoding)
        // Consecutive docs with freq=1 are very compact (~10 bytes per block)
        let written = pw.doc_out.file_pointer() - header_size;
        assert_ge!(
            written,
            8,
            "block encoding should produce output, got {written}"
        );
    }

    #[test]
    fn test_block_encoding_130_docs() {
        // 130 docs: 1 full block + 2 VInt tail docs
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        let postings: Vec<(i32, i32, &[i32])> =
            (0..130).map(|i| (i, 1i32, [].as_slice())).collect();
        let header_size = pw.doc_out.file_pointer();
        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqs,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 130);
        assert_eq!(state.singleton_doc_id, -1);
        let written = pw.doc_out.file_pointer() - header_size;
        assert_gt!(
            written,
            10,
            "block + tail should produce output, got {written}"
        );
    }

    #[test]
    fn test_block_encoding_256_docs() {
        // 256 docs: 2 full blocks, no tail
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        let postings: Vec<(i32, i32, &[i32])> =
            (0..256).map(|i| (i, 1i32, [].as_slice())).collect();
        let header_size = pw.doc_out.file_pointer();
        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqs,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 256);
        let written = pw.doc_out.file_pointer() - header_size;
        // 2 blocks should produce more output than 1
        assert_ge!(written, 16);
    }

    #[test]
    fn test_block_encoding_with_positions() {
        // 128+ docs with positions: verify pos skip data is present
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], true).unwrap();
        // Each doc has 2 positions [0, 1]
        let positions = vec![0i32, 1];
        let postings: Vec<(i32, i32, &[i32])> =
            (0..128).map(|i| (i, 2i32, positions.as_slice())).collect();

        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqsAndPositions,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 128);
        assert_eq!(state.total_term_freq, 256); // 128 * 2
        // totalTermFreq > 128, so last_pos_block_offset should be set
        assert_ne!(state.last_pos_block_offset, -1);

        // Verify pos data was written
        let pos_out = pw.pos_out.as_ref().unwrap();
        let pos_header_size = codec_util::index_header_length(POS_CODEC, "");
        assert_gt!(pos_out.file_pointer() as usize, pos_header_size);
    }

    #[test]
    fn test_block_encoding_consecutive_docs() {
        // Docs 0..127 with deltas all = 1 → doc_range == 128 == BLOCK_SIZE
        // This should use the byte(0) consecutive encoding
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        // doc IDs: 0, 1, 2, ..., 127 — deltas from -1 are 1, 1, 1, ... = range 128
        let postings: Vec<(i32, i32, &[i32])> =
            (0..128).map(|i| (i, 1i32, [].as_slice())).collect();
        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqs,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 128);
        // The block encoding should succeed (we can't easily inspect the byte,
        // but the fact that it doesn't panic or error is the key assertion)
    }

    #[test]
    fn test_block_encoding_sparse_docs() {
        // Sparse doc IDs with large deltas → uses FOR encoding
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        // doc IDs: 0, 100, 200, ..., 12700 — delta = 100 each (except first which is 1 from -1)
        let postings: Vec<(i32, i32, &[i32])> =
            (0..128).map(|i| (i * 100, 1i32, [].as_slice())).collect();
        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqs,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 128);
        assert_eq!(state.singleton_doc_id, -1);
    }

    #[test]
    fn test_block_encoding_docs_only() {
        // DOCS-only index option (no freqs) with 128 docs
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        let postings: Vec<(i32, i32, &[i32])> =
            (0..128).map(|i| (i, 1i32, [].as_slice())).collect();
        let state = pw
            .write_term(&postings, IndexOptions::Docs, &NormsLookup::no_norms())
            .unwrap();

        assert_eq!(state.doc_freq, 128);
        assert_eq!(state.total_term_freq, -1);
        assert_eq!(state.singleton_doc_id, -1);
    }

    #[test]
    fn test_block_encoding_varied_freqs() {
        // 128 docs with varied frequencies
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        let postings: Vec<(i32, i32, &[i32])> =
            (0..128).map(|i| (i, (i % 10) + 1, [].as_slice())).collect();
        let state = pw
            .write_term(
                &postings,
                IndexOptions::DocsAndFreqs,
                &NormsLookup::no_norms(),
            )
            .unwrap();

        assert_eq!(state.doc_freq, 128);
        let expected_ttf: i64 = (0..128).map(|i: i64| (i % 10) + 1).sum();
        assert_eq!(state.total_term_freq, expected_ttf);
    }

    #[test]
    fn test_psm_level1_impact_metadata_zero_below_threshold() {
        // Java only updates level 1 impact metadata inside writeLevel1SkipData(),
        // which triggers every 4096 docs. For terms below that threshold, the
        // level 1 fields must be 0. This test writes 128 docs with varied freqs
        // (enough to produce non-zero level 0 impacts) and verifies level 1 stays 0.
        let dir = test_directory();
        let mut pw = PostingsWriter::new(&dir, "_0", "", &[0u8; 16], false).unwrap();
        let postings: Vec<(i32, i32, &[i32])> =
            (0..128).map(|i| (i, (i % 10) + 1, [].as_slice())).collect();
        pw.write_term(
            &postings,
            IndexOptions::DocsAndFreqs,
            &NormsLookup::no_norms(),
        )
        .unwrap();

        let names = pw.finish().unwrap();
        let psm_name = names.iter().find(|n| n.ends_with(".psm")).unwrap();
        let psm_data = dir.lock().unwrap().read_file(psm_name).unwrap();

        // Parse the 4 LE i32 metadata fields after the codec header
        let header_size = codec_util::index_header_length(META_CODEC, "");
        let meta = &psm_data[header_size..];
        let max_num_impacts_l0 = i32::from_le_bytes(meta[0..4].try_into().unwrap());
        let max_impact_bytes_l0 = i32::from_le_bytes(meta[4..8].try_into().unwrap());
        let max_num_impacts_l1 = i32::from_le_bytes(meta[8..12].try_into().unwrap());
        let max_impact_bytes_l1 = i32::from_le_bytes(meta[12..16].try_into().unwrap());

        // Level 0 should be non-zero (128 docs with freqs triggers impact computation)
        assert_gt!(
            max_num_impacts_l0,
            0,
            "level 0 impact count should be > 0, got {max_num_impacts_l0}"
        );
        assert_gt!(
            max_impact_bytes_l0,
            0,
            "level 0 impact bytes should be > 0, got {max_impact_bytes_l0}"
        );

        // Level 1 must be 0 (no level 1 skip triggered for < 4096 docs)
        assert_eq!(
            max_num_impacts_l1, 0,
            "level 1 impact count should be 0, got {max_num_impacts_l1}"
        );
        assert_eq!(
            max_impact_bytes_l1, 0,
            "level 1 impact bytes should be 0, got {max_impact_bytes_l1}"
        );
    }
}
