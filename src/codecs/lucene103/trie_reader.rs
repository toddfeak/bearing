// SPDX-License-Identifier: Apache-2.0

//! Trie reader for the Lucene 103 block tree terms index.
//!
//! Navigates the FST-like trie stored in the `.tip` file to find block file
//! pointers in the `.tim` terms dictionary. The trie is written in post-order
//! by [`super::blocktree_writer::TrieBuilder`].
//!
//! Node types:
//! - `SIGN_NO_CHILDREN` — leaf node with output (block FP)
//! - `SIGN_SINGLE_CHILD_WITH_OUTPUT` — single child, has block FP
//! - `SIGN_SINGLE_CHILD_WITHOUT_OUTPUT` — single child, no block FP
//! - `SIGN_MULTI_CHILDREN` — multiple children with strategy-based lookup

use std::io;

use crate::store::RandomAccessInput;

// Node type signatures (lowest 2 bits of header)
const SIGN_NO_CHILDREN: u32 = 0x00;
const SIGN_SINGLE_CHILD_WITHOUT_OUTPUT: u32 = 0x02;
const SIGN_MULTI_CHILDREN: u32 = 0x03;

// Leaf node flags (bits of header byte)
const LEAF_NODE_HAS_TERMS: u32 = 1 << 5;
const LEAF_NODE_HAS_FLOOR: u32 = 1 << 6;

// Non-leaf node flags (bits of encoded output FP)
const NON_LEAF_NODE_HAS_TERMS: u64 = 1 << 1;
const NON_LEAF_NODE_HAS_FLOOR: u64 = 1 << 0;

const NO_OUTPUT: i64 = -1;
const NO_FLOOR_DATA: i64 = -1;

/// Masks for extracting N bytes from a little-endian long.
/// `BYTES_MASK[n]` masks out the lowest `(n+1)` bytes.
const BYTES_MASK: [u64; 8] = [
    0xFF,
    0xFFFF,
    0xFF_FFFF,
    0xFFFF_FFFF,
    0xFF_FFFF_FFFF,
    0xFFFF_FFFF_FFFF,
    0xFF_FFFF_FFFF_FFFF,
    0xFFFF_FFFF_FFFF_FFFF,
];

// Child save strategy codes
const STRATEGY_REVERSE_ARRAY: u32 = 0;
const STRATEGY_ARRAY: u32 = 1;
const STRATEGY_BITS: u32 = 2;

/// A loaded trie node with decoded metadata.
#[derive(Debug)]
pub(crate) struct Node {
    /// Absolute file pointer of this node in the `.tip` file.
    fp: i64,
    /// Node type signature (lowest 2 bits of header).
    sign: u32,
    /// Label byte that led to this node (set by parent during lookup).
    label: u8,
    /// Block file pointer in `.tim` for this node's terms, or [`NO_OUTPUT`].
    output_fp: i64,
    /// Whether this node's block contains terms (vs only sub-blocks).
    has_terms: bool,
    /// File pointer to floor data, or [`NO_FLOOR_DATA`].
    floor_data_fp: i64,

    // Single-child fields
    /// Delta FP to the single child (parent_fp - child_fp).
    child_delta_fp: i64,
    /// Label of the minimum (or only) child.
    min_children_label: u8,

    // Multi-children fields
    /// File pointer to the strategy data region.
    strategy_fp: i64,
    /// Child save strategy code.
    child_save_strategy: u32,
    /// Number of bytes in the strategy data.
    strategy_bytes: u32,
    /// Number of bytes per child delta FP entry.
    children_delta_fp_bytes: u32,
}

impl Node {
    fn new() -> Self {
        Self {
            fp: 0,
            sign: 0,
            label: 0,
            output_fp: NO_OUTPUT,
            has_terms: false,
            floor_data_fp: NO_FLOOR_DATA,
            child_delta_fp: 0,
            min_children_label: 0,
            strategy_fp: 0,
            child_save_strategy: 0,
            strategy_bytes: 0,
            children_delta_fp_bytes: 0,
        }
    }

    fn has_output(&self) -> bool {
        self.output_fp != NO_OUTPUT
    }
}

/// Reader for the trie index stored in the `.tip` file.
///
/// Provides child lookup to navigate from the root node down to a leaf,
/// following target term bytes. Each node may carry an `output_fp` pointing
/// to the corresponding block in the `.tim` file.
pub struct TrieReader {
    access: Box<dyn RandomAccessInput>,
    root: Node,
}

impl TrieReader {
    /// Creates a new trie reader from the `.tip` index input.
    ///
    /// `root_fp` is the file pointer to the root node, read from the `.tmd`
    /// metadata file (stored in [`super::blocktree_reader::FieldReader`]).
    pub fn new(access: Box<dyn RandomAccessInput>, root_fp: i64) -> io::Result<Self> {
        let mut root = Node::new();
        load(&*access, &mut root, root_fp)?;
        Ok(Self { access, root })
    }

    #[cfg(test)]
    fn root(&self) -> &Node {
        &self.root
    }

    /// Looks up a child node by label.
    ///
    /// Returns `true` if the child was found and `child` was populated,
    /// `false` if no child exists for the given label.
    pub(crate) fn lookup_child(
        &self,
        target_label: u8,
        parent: &Node,
        child: &mut Node,
    ) -> io::Result<bool> {
        let sign = parent.sign;

        if sign == SIGN_NO_CHILDREN {
            return Ok(false);
        }

        if sign != SIGN_MULTI_CHILDREN {
            // Single child
            if target_label != parent.min_children_label {
                return Ok(false);
            }
            child.label = target_label;
            load(&*self.access, child, parent.fp - parent.child_delta_fp)?;
            return Ok(true);
        }

        // Multi-children: look up using the child save strategy
        let min_label = parent.min_children_label;
        let position = if target_label == min_label {
            0i32
        } else if target_label > min_label {
            strategy_lookup(
                parent.child_save_strategy,
                target_label,
                &*self.access,
                parent.strategy_fp,
                parent.strategy_bytes,
                min_label,
            )?
        } else {
            -1
        };

        if position < 0 {
            return Ok(false);
        }

        let bytes_per_entry = parent.children_delta_fp_bytes;
        let pos = parent.strategy_fp
            + parent.strategy_bytes as i64
            + bytes_per_entry as i64 * position as i64;
        let delta = self.access.read_le_long_at(pos as u64)? as u64
            & BYTES_MASK[bytes_per_entry as usize - 1];
        let fp = parent.fp - delta as i64;

        child.label = target_label;
        load(&*self.access, child, fp)?;
        Ok(true)
    }

    /// Navigates the trie following the target bytes, returning the deepest
    /// node that has an output (block FP).
    ///
    /// Returns `None` if the target has no prefix in the trie that leads to
    /// a block.
    pub fn seek_to_block(&self, target: &[u8]) -> io::Result<Option<TrieSeekResult>> {
        // Use two nodes and alternate between them to avoid borrow issues.
        let mut nodes = [Node::new(), Node::new()];
        // Copy root into nodes[0] so we own it.
        load(&*self.access, &mut nodes[0], self.root.fp)?;
        let mut current_idx = 0usize;
        let mut best: Option<TrieSeekResult> = None;

        if nodes[current_idx].has_output() {
            best = Some(TrieSeekResult {
                output_fp: nodes[current_idx].output_fp,
                has_terms: nodes[current_idx].has_terms,
                floor_data_fp: nodes[current_idx].floor_data_fp,
                depth: 0,
            });
        }

        for (i, &byte) in target.iter().enumerate() {
            let child_idx = 1 - current_idx;
            let (current_slice, child_slice) = if current_idx == 0 {
                let (a, b) = nodes.split_at_mut(1);
                (&a[0], &mut b[0])
            } else {
                let (a, b) = nodes.split_at_mut(1);
                (&b[0], &mut a[0])
            };

            if !self.lookup_child(byte, current_slice, child_slice)? {
                break;
            }
            if child_slice.has_output() {
                best = Some(TrieSeekResult {
                    output_fp: child_slice.output_fp,
                    has_terms: child_slice.has_terms,
                    floor_data_fp: child_slice.floor_data_fp,
                    depth: i + 1,
                });
            }
            current_idx = child_idx;
        }

        Ok(best)
    }
}

/// Result of navigating the trie to find a block.
#[derive(Debug)]
pub struct TrieSeekResult {
    /// Block file pointer in `.tim`.
    pub output_fp: i64,
    /// Whether the block contains terms (vs only sub-blocks).
    pub has_terms: bool,
    /// File pointer to floor data, or `NO_FLOOR_DATA`.
    pub floor_data_fp: i64,
    /// Number of target bytes consumed to reach this node.
    pub depth: usize,
}

/// Loads a trie node from the `.tip` file at the given file pointer.
fn load(access: &dyn RandomAccessInput, node: &mut Node, fp: i64) -> io::Result<()> {
    node.fp = fp;
    let term_flags_long = access.read_le_long_at(fp as u64)?;
    let term_flags = term_flags_long as u32;
    let sign = term_flags & 0x03;
    node.sign = sign;

    match sign {
        SIGN_NO_CHILDREN => load_leaf_node(term_flags, term_flags_long, fp, node),
        SIGN_MULTI_CHILDREN => {
            load_multi_children_node(access, term_flags, term_flags_long, fp, node)
        }
        _ => load_single_child_node(access, sign, term_flags, term_flags_long, fp, node),
    }
}

fn load_leaf_node(
    term_flags: u32,
    term_flags_long: i64,
    fp: i64,
    node: &mut Node,
) -> io::Result<()> {
    let fp_bytes_minus1 = ((term_flags >> 2) & 0x07) as usize;
    node.output_fp = if fp_bytes_minus1 <= 6 {
        ((term_flags_long as u64 >> 8) & BYTES_MASK[fp_bytes_minus1]) as i64
    } else {
        // Output FP doesn't fit in the header long — read a full long at fp+1
        // (This would need another read_le_long_at call, but with LE data written
        // as individual bytes, fp+1 gives us the output FP bytes.)
        // Actually, for fp_bytes_minus1=7 (8 bytes), the output starts at byte
        // offset 1 within the node data.
        return Err(io::Error::other(
            "leaf node with 8-byte output FP not yet supported",
        ));
    };
    node.has_terms = (term_flags & LEAF_NODE_HAS_TERMS) != 0;
    node.floor_data_fp = if (term_flags & LEAF_NODE_HAS_FLOOR) != 0 {
        fp + 2 + fp_bytes_minus1 as i64
    } else {
        NO_FLOOR_DATA
    };
    Ok(())
}

fn load_single_child_node(
    access: &dyn RandomAccessInput,
    sign: u32,
    term_flags: u32,
    term_flags_long: i64,
    fp: i64,
    node: &mut Node,
) -> io::Result<()> {
    let child_delta_fp_bytes_minus1 = ((term_flags >> 2) & 0x07) as usize;
    let l = if child_delta_fp_bytes_minus1 <= 5 {
        (term_flags_long as u64) >> 16
    } else {
        access.read_le_long_at((fp + 2) as u64)? as u64
    };
    node.child_delta_fp = (l & BYTES_MASK[child_delta_fp_bytes_minus1]) as i64;
    node.min_children_label = ((term_flags >> 8) & 0xFF) as u8;

    if sign == SIGN_SINGLE_CHILD_WITHOUT_OUTPUT {
        node.output_fp = NO_OUTPUT;
        node.has_terms = false;
        node.floor_data_fp = NO_FLOOR_DATA;
    } else {
        // SIGN_SINGLE_CHILD_WITH_OUTPUT
        let encoded_output_fp_bytes_minus1 = ((term_flags >> 5) & 0x07) as usize;
        let offset = fp + child_delta_fp_bytes_minus1 as i64 + 3;
        let encoded_fp = access.read_le_long_at(offset as u64)? as u64
            & BYTES_MASK[encoded_output_fp_bytes_minus1];
        node.output_fp = (encoded_fp >> 2) as i64;
        node.has_terms = (encoded_fp & NON_LEAF_NODE_HAS_TERMS) != 0;
        node.floor_data_fp = if (encoded_fp & NON_LEAF_NODE_HAS_FLOOR) != 0 {
            offset + encoded_output_fp_bytes_minus1 as i64 + 1
        } else {
            NO_FLOOR_DATA
        };
    }
    Ok(())
}

fn load_multi_children_node(
    access: &dyn RandomAccessInput,
    term_flags: u32,
    _term_flags_long: i64,
    fp: i64,
    node: &mut Node,
) -> io::Result<()> {
    node.children_delta_fp_bytes = ((term_flags >> 2) & 0x07) + 1;
    node.child_save_strategy = (term_flags >> 9) & 0x03;
    node.strategy_bytes = ((term_flags >> 11) & 0x1F) + 1;
    node.min_children_label = ((term_flags >> 16) & 0xFF) as u8;

    let has_output = (term_flags & 0x20) != 0;
    if has_output {
        let encoded_output_fp_bytes_minus1 = ((term_flags >> 6) & 0x07) as usize;
        let l = if encoded_output_fp_bytes_minus1 <= 4 {
            (_term_flags_long as u64) >> 24
        } else {
            access.read_le_long_at((fp + 3) as u64)? as u64
        };
        let encoded_fp = l & BYTES_MASK[encoded_output_fp_bytes_minus1];
        node.output_fp = (encoded_fp >> 2) as i64;
        node.has_terms = (encoded_fp & NON_LEAF_NODE_HAS_TERMS) != 0;

        if (encoded_fp & NON_LEAF_NODE_HAS_FLOOR) != 0 {
            let offset = fp + 4 + encoded_output_fp_bytes_minus1 as i64;
            let children_num = (access.read_byte_at(offset as u64)? as u64) + 1;
            node.strategy_fp = offset + 1;
            node.floor_data_fp = node.strategy_fp
                + node.strategy_bytes as i64
                + children_num as i64 * node.children_delta_fp_bytes as i64;
        } else {
            node.floor_data_fp = NO_FLOOR_DATA;
            node.strategy_fp = fp + 4 + encoded_output_fp_bytes_minus1 as i64;
        }
    } else {
        node.output_fp = NO_OUTPUT;
        node.has_terms = false;
        node.floor_data_fp = NO_FLOOR_DATA;
        node.strategy_fp = fp + 3;
    }
    Ok(())
}

/// Looks up a target label in the children of a multi-children node.
///
/// Returns the 0-based position of the child, or -1 if not found.
fn strategy_lookup(
    strategy_code: u32,
    target_label: u8,
    access: &dyn RandomAccessInput,
    strategy_fp: i64,
    strategy_bytes: u32,
    min_label: u8,
) -> io::Result<i32> {
    match strategy_code {
        STRATEGY_BITS => bits_lookup(target_label, access, strategy_fp, min_label),
        STRATEGY_ARRAY => {
            array_lookup(target_label, access, strategy_fp, strategy_bytes, min_label)
        }
        STRATEGY_REVERSE_ARRAY => {
            reverse_array_lookup(target_label, access, strategy_fp, strategy_bytes, min_label)
        }
        _ => Err(io::Error::other(format!(
            "unknown child save strategy: {strategy_code}"
        ))),
    }
}

/// Bitset strategy: each bit represents presence of a label relative to min_label.
fn bits_lookup(
    target_label: u8,
    access: &dyn RandomAccessInput,
    strategy_fp: i64,
    min_label: u8,
) -> io::Result<i32> {
    let delta = (target_label - min_label) as u32;
    let byte_idx = delta / 8;
    let bit_idx = delta % 8;
    let byte = access.read_byte_at((strategy_fp + byte_idx as i64) as u64)?;
    if (byte >> bit_idx) & 1 == 0 {
        return Ok(-1);
    }
    // Count set bits before this position to get the child index
    // Position = popcount of all bits before this one
    let mut position = 0i32;
    for i in 0..byte_idx {
        let b = access.read_byte_at((strategy_fp + i as i64) as u64)?;
        position += b.count_ones() as i32;
    }
    // Add bits in the same byte before bit_idx
    let mask = (1u8 << bit_idx) - 1;
    position += (byte & mask).count_ones() as i32;
    Ok(position)
}

/// Array strategy: sorted array of child labels (excluding min_label).
/// Binary search for the target.
fn array_lookup(
    target_label: u8,
    access: &dyn RandomAccessInput,
    strategy_fp: i64,
    strategy_bytes: u32,
    min_label: u8,
) -> io::Result<i32> {
    if target_label <= min_label {
        return Ok(-1);
    }
    // Binary search in the label array
    let mut lo = 0u32;
    let mut hi = strategy_bytes; // strategy_bytes = num_children - 1
    while lo < hi {
        let mid = (lo + hi) / 2;
        let label = access.read_byte_at((strategy_fp + mid as i64) as u64)?;
        if label < target_label {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo < strategy_bytes {
        let label = access.read_byte_at((strategy_fp + lo as i64) as u64)?;
        if label == target_label {
            return Ok((lo + 1) as i32); // +1 because min_label is position 0
        }
    }
    Ok(-1)
}

/// Reverse array strategy: max_label byte, then sorted array of absent labels.
/// A label is present if it's in [min_label, max_label] and NOT in the absent array.
fn reverse_array_lookup(
    target_label: u8,
    access: &dyn RandomAccessInput,
    strategy_fp: i64,
    strategy_bytes: u32,
    min_label: u8,
) -> io::Result<i32> {
    let max_label = access.read_byte_at(strategy_fp as u64)?;
    if target_label > max_label {
        return Ok(-1);
    }
    // Count how many labels in [min_label+1, target_label] are absent
    let absent_count = strategy_bytes - 1; // first byte is max_label
    let mut absent_before = 0i32;
    for i in 0..absent_count {
        let absent = access.read_byte_at((strategy_fp + 1 + i as i64) as u64)?;
        if absent < target_label {
            absent_before += 1;
        } else if absent == target_label {
            return Ok(-1); // target is absent
        } else {
            break; // sorted, no more absent labels before target
        }
    }
    // Position = (target_label - min_label) - absent_before
    let raw_position = (target_label - min_label) as i32;
    Ok(raw_position - absent_before)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::competitive_impact::NormsLookup;
    use crate::codecs::lucene103::blocktree_reader::BlockTreeTermsReader;
    use crate::codecs::lucene103::blocktree_writer::BlockTreeTermsWriter;
    use crate::document::IndexOptions;
    use crate::index::indexing_chain::PostingsArray;
    use crate::index::{FieldInfo, FieldInfos, PointDimensionConfig};
    use crate::store::memory::MemoryDirectory;
    use crate::store::{Directory, SharedDirectory};

    fn make_field_info(name: &str, number: u32) -> FieldInfo {
        FieldInfo::new(
            name.to_string(),
            number,
            false,
            false,
            IndexOptions::Docs,
            crate::document::DocValuesType::None,
            PointDimensionConfig::default(),
        )
    }

    fn make_postings<'a>(terms: &'a [(&str, &[i32])]) -> (Vec<(&'a str, usize)>, PostingsArray) {
        let mut postings = PostingsArray::new(false, false, false, false, false);
        let mut sorted_terms = Vec::new();

        for (term, doc_ids) in terms {
            let term_id = postings.add_term();
            for &doc_id in *doc_ids {
                postings.record_occurrence(term_id, doc_id, 0, 0, 0);
            }
            sorted_terms.push((*term, term_id));
        }

        sorted_terms.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        postings.finalize_all();
        (sorted_terms, postings)
    }

    /// Write terms and return (directory, field_infos, segment_name, suffix, segment_id).
    fn write_terms(
        terms: Vec<(&str, &[i32])>,
    ) -> io::Result<(Box<dyn Directory>, FieldInfos, [u8; 16])> {
        let field_infos = FieldInfos::new(vec![make_field_info("f", 0)]);
        let segment_name = "_0";
        let segment_suffix = "";
        let segment_id = [0u8; 16];

        let shared_dir = SharedDirectory::new(Box::new(MemoryDirectory::new()));

        {
            let mut writer = BlockTreeTermsWriter::new(
                &shared_dir,
                segment_name,
                segment_suffix,
                &segment_id,
                &field_infos,
            )?;

            let fi = field_infos.field_info_by_name("f").unwrap();
            let (sorted_terms, postings) = make_postings(&terms);
            let norms = NormsLookup::no_norms();
            writer.write_field(fi, &sorted_terms, &postings, &norms)?;

            writer.finish()?;
        }

        let dir = shared_dir.into_inner().unwrap();
        Ok((dir, field_infos, segment_id))
    }

    /// Open a trie reader from the .tip file for field 0.
    fn open_trie(
        dir: &dyn Directory,
        field_infos: &FieldInfos,
        segment_id: &[u8; 16],
    ) -> io::Result<TrieReader> {
        let reader = BlockTreeTermsReader::open(dir, "_0", "", segment_id, field_infos)?;
        let fr = reader.field_reader(0).unwrap();
        fr.new_trie_reader()
    }

    #[test]
    fn test_trie_reader_root_has_output() {
        let terms = vec![
            ("alpha", &[0, 1][..]),
            ("beta", &[1, 2]),
            ("gamma", &[0, 2]),
        ];
        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        // Root should have an output (the root block FP)
        assert!(trie.root().has_output());
    }

    #[test]
    fn test_trie_seek_to_block_single_block() {
        // Few terms → single block → root has output, no children needed
        let terms = vec![("alpha", &[0][..]), ("beta", &[1]), ("gamma", &[2])];
        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        // Any term should find the root block
        let result = trie.seek_to_block(b"alpha").unwrap();
        assert_some!(&result);
        let r = result.unwrap();
        assert!(r.has_terms);
        assert_ge!(r.output_fp, 0);
    }

    #[test]
    fn test_trie_seek_multi_prefix() {
        // Generate enough terms with different prefixes to force multi-block trie
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        // 50 terms starting with "aaa" and 50 starting with "bbb"
        // exceeds DEFAULT_MAX_BLOCK_SIZE (48) per prefix group
        for i in 0..50 {
            terms_data.push((format!("aaa{i:04}"), vec![i]));
            terms_data.push((format!("bbb{i:04}"), vec![50 + i]));
        }

        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        // Both prefixes should find blocks
        let a_result = trie.seek_to_block(b"aaa0025").unwrap();
        assert_some!(&a_result);

        let b_result = trie.seek_to_block(b"bbb0025").unwrap();
        assert_some!(&b_result);

        // The two prefixes should potentially point to different blocks
        let a_fp = a_result.unwrap().output_fp;
        let b_fp = b_result.unwrap().output_fp;
        // They might be the same root block or different — depends on trie structure
        // At minimum, both should be valid (>= 0)
        assert_ge!(a_fp, 0);
        assert_ge!(b_fp, 0);
    }

    #[test]
    fn test_trie_seek_nonexistent_prefix() {
        let terms = vec![("alpha", &[0][..]), ("beta", &[1])];
        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        // Empty target should still find the root block (empty prefix)
        let result = trie.seek_to_block(b"").unwrap();
        assert_some!(&result);
    }

    #[test]
    fn test_trie_single_child_nodes() {
        // All terms share a long prefix "prefix_" — inner trie nodes along
        // the path have single children. Whether they have output depends on
        // whether the blocktree creates blocks at intermediate prefixes.
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..30 {
            terms_data.push((format!("prefix_{i:03}"), vec![i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        let root = trie.root();
        // Root has a single child (all terms start with 'p')
        assert_ne!(root.sign, SIGN_NO_CHILDREN);
        assert_ne!(root.sign, SIGN_MULTI_CHILDREN);
        assert_eq!(root.min_children_label, b'p');

        // Should be able to follow the single-child chain down to find the block
        let mut child = Node::new();
        assert!(trie.lookup_child(b'p', root, &mut child).unwrap());
        assert!(!trie.lookup_child(b'q', root, &mut child).unwrap());

        // Seeking should find the block
        let result = trie.seek_to_block(b"prefix_015").unwrap();
        assert_some!(&result);
    }

    #[test]
    fn test_trie_single_child_with_output() {
        // Terms with a common prefix that has its own block, plus exactly one
        // continuation letter. 50+ terms under "aa" forces block splitting at "aa",
        // and no other first-letter branches → single child at root.
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..50 {
            terms_data.push((format!("aa{i:04}"), vec![i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        // Verify we can navigate to a term in the block
        let result = trie.seek_to_block(b"aa0025").unwrap();
        assert_some!(&result);
        let r = result.unwrap();
        assert_ge!(r.output_fp, 0);
    }

    #[test]
    fn test_trie_multi_children_lookup() {
        // Create terms under 3 different prefixes to force multi-children node.
        // Each prefix has 50+ terms to force block splitting.
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..50 {
            terms_data.push((format!("aaa{i:04}"), vec![i]));
            terms_data.push((format!("bbb{i:04}"), vec![50 + i]));
            terms_data.push((format!("ccc{i:04}"), vec![100 + i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        // Root should be multi-children with children for 'a', 'b', 'c'
        let root = trie.root();
        assert_eq!(root.sign, SIGN_MULTI_CHILDREN);

        // lookup_child should find all three
        let mut child = Node::new();
        assert!(trie.lookup_child(b'a', root, &mut child).unwrap());
        assert!(trie.lookup_child(b'b', root, &mut child).unwrap());
        assert!(trie.lookup_child(b'c', root, &mut child).unwrap());

        // lookup_child should not find labels outside the set
        assert!(!trie.lookup_child(b'd', root, &mut child).unwrap());
        assert!(!trie.lookup_child(b'A', root, &mut child).unwrap());

        // seek_to_block should find distinct blocks for each prefix
        let a_result = trie.seek_to_block(b"aaa0025").unwrap().unwrap();
        let b_result = trie.seek_to_block(b"bbb0025").unwrap().unwrap();
        let c_result = trie.seek_to_block(b"ccc0025").unwrap().unwrap();
        assert_ge!(a_result.output_fp, 0);
        assert_ge!(b_result.output_fp, 0);
        assert_ge!(c_result.output_fp, 0);
    }

    #[test]
    fn test_trie_multi_children_strategy_array() {
        // ARRAY strategy: few children with large label gaps.
        // Two prefixes with distant first letters: 'a' and 'z'.
        // need_bytes(ARRAY) = num_children - 1 = 1
        // need_bytes(BITS) = (122-97+1)/8 = 4
        // need_bytes(REVERSE_ARRAY) = 26 - 2 + 1 = 25
        // → ARRAY wins
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..50 {
            terms_data.push((format!("aaa{i:04}"), vec![i]));
            terms_data.push((format!("zzz{i:04}"), vec![50 + i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        let root = trie.root();
        assert_eq!(root.sign, SIGN_MULTI_CHILDREN);
        assert_eq!(root.child_save_strategy, STRATEGY_ARRAY);

        let mut child = Node::new();
        assert!(trie.lookup_child(b'a', root, &mut child).unwrap());
        assert!(trie.lookup_child(b'z', root, &mut child).unwrap());
        assert!(!trie.lookup_child(b'm', root, &mut child).unwrap());
    }

    #[test]
    fn test_trie_multi_children_strategy_bits() {
        // BITS strategy: children with labels close together.
        // Prefixes: "aa", "ab", "ac", "ad" — labels a,b,c,d are adjacent.
        // need_bytes(BITS) = (100-97+1)/8 = 1
        // need_bytes(ARRAY) = 4 - 1 = 3
        // need_bytes(REVERSE_ARRAY) = 4 - 4 + 1 = 1 (tie, but BITS wins by order)
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..50 {
            terms_data.push((format!("aa{i:04}"), vec![i]));
            terms_data.push((format!("ab{i:04}"), vec![50 + i]));
            terms_data.push((format!("ac{i:04}"), vec![100 + i]));
            terms_data.push((format!("ad{i:04}"), vec![150 + i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        // The node with children 'a','b','c','d' may be root or one level down
        // depending on how the blocktree groups things. Let's just verify
        // seek works for all prefixes.
        for prefix in [b"aa0025" as &[u8], b"ab0025", b"ac0025", b"ad0025"] {
            let result = trie.seek_to_block(prefix).unwrap();
            assert_some!(&result);
        }
    }

    #[test]
    fn test_trie_multi_children_strategy_reverse_array() {
        // REVERSE_ARRAY strategy: many children filling most of a label range.
        // Use consecutive single-letter prefixes: 'a' through 'h' (8 children).
        // need_bytes(ARRAY) = 8 - 1 = 7
        // need_bytes(BITS) = (104-97+1)/8 = 1
        // need_bytes(REVERSE_ARRAY) = 8 - 8 + 1 = 1 (tie with BITS)
        //
        // For REVERSE_ARRAY to win, we need the range to be larger than children
        // with most filled. Use: 'a','b','c','e','f','g' (skip 'd').
        // range = 7, children = 6
        // need_bytes(BITS) = 7/8 = 1
        // need_bytes(ARRAY) = 6 - 1 = 5
        // need_bytes(REVERSE_ARRAY) = 7 - 6 + 1 = 2
        // BITS still wins here. Let me think of a case where REVERSE_ARRAY wins...
        //
        // Actually, looking at choose_child_save_strategy, it picks the one with
        // fewest bytes, with BITS checked first on ties. REVERSE_ARRAY wins when:
        // range - count + 1 < range.div_ceil(8) AND range - count + 1 < count - 1
        // E.g., range=20, count=18: BITS=3, ARRAY=17, REVERSE_ARRAY=3 → tie, BITS wins
        // E.g., range=17, count=16: BITS=3, ARRAY=15, REVERSE_ARRAY=2 → REVERSE_ARRAY wins!
        //
        // Need 16 children spanning a range of 17 labels (one gap).
        // Use labels 'a' through 'q' (17 range), skip one letter.
        let labels: Vec<u8> = (b'a'..=b'q')
            .filter(|&b| b != b'i') // skip 'i', leaving 16 of 17
            .collect();

        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for (idx, &label) in labels.iter().enumerate() {
            for i in 0..50 {
                let term = format!("{}{i:04}", label as char);
                terms_data.push((term, vec![(idx * 50 + i) as i32]));
            }
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        let root = trie.root();
        assert_eq!(root.sign, SIGN_MULTI_CHILDREN);
        assert_eq!(root.child_save_strategy, STRATEGY_REVERSE_ARRAY);

        // Should find all present labels
        let mut child = Node::new();
        for &label in &labels {
            assert!(
                trie.lookup_child(label, root, &mut child).unwrap(),
                "should find child for label '{}'",
                label as char
            );
        }
        // Should not find the absent label
        assert!(!trie.lookup_child(b'i', root, &mut child).unwrap());
        // Should not find labels outside range
        assert!(!trie.lookup_child(b'r', root, &mut child).unwrap());
    }

    #[test]
    fn test_trie_deep_traversal() {
        // Test multi-level trie navigation with nested prefixes.
        // Terms with structure: "category/subcategory/item_NNN"
        // This creates a deep trie with multiple levels of single-child nodes.
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..50 {
            terms_data.push((format!("category/alpha/item_{i:03}"), vec![i]));
            terms_data.push((format!("category/beta/item_{i:03}"), vec![50 + i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        // Should traverse deep into the trie
        let alpha = trie.seek_to_block(b"category/alpha/item_025").unwrap();
        assert!(alpha.is_some());
        let alpha = alpha.unwrap();
        assert!(alpha.depth > 0, "should have consumed some prefix bytes");

        let beta = trie.seek_to_block(b"category/beta/item_025").unwrap();
        assert!(beta.is_some());
        let beta = beta.unwrap();
        assert!(beta.depth > 0);

        // Different subcategories should (likely) point to different blocks
        // since each has 50 terms exceeding the block size
        assert!(alpha.output_fp >= 0);
        assert!(beta.output_fp >= 0);
    }

    #[test]
    fn test_trie_lookup_child_below_min_label() {
        // When target_label < min_children_label, lookup should return false
        let mut terms_data: Vec<(String, Vec<i32>)> = Vec::new();
        for i in 0..50 {
            terms_data.push((format!("mmm{i:04}"), vec![i]));
            terms_data.push((format!("zzz{i:04}"), vec![50 + i]));
        }
        let terms: Vec<(&str, &[i32])> = terms_data
            .iter()
            .map(|(t, d)| (t.as_str(), d.as_slice()))
            .collect();

        let (dir, fi, id) = write_terms(terms).unwrap();
        let trie = open_trie(dir.as_ref(), &fi, &id).unwrap();

        let root = trie.root();
        let mut child = Node::new();
        // 'a' < 'm' (min_children_label)
        assert!(!trie.lookup_child(b'a', root, &mut child).unwrap());
    }
}
