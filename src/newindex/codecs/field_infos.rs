// SPDX-License-Identifier: Apache-2.0

// DEBT: adapted from codecs::lucene94::field_infos_format — reconcile after
// switchover by updating the original to accept newindex types directly.

//! Field infos (.fnm) writer for the newindex pipeline.

use std::collections::HashMap;
use std::io;

use log::debug;

use crate::codecs::codec_util;
use crate::newindex::index_file_names;
use crate::store::SharedDirectory;

const CODEC_NAME: &str = "Lucene94FieldInfos";
const FORMAT_CURRENT: i32 = 2; // FORMAT_DOCVALUE_SKIPPER
const EXTENSION: &str = "fnm";

/// Per-field metadata for writing the .fnm file.
// DEBT: parallel to index::FieldInfo — merge after switchover
#[derive(Debug, Clone)]
pub(crate) struct FieldInfo {
    pub name: String,
    pub number: u32,
    pub stored: bool,
}

/// Collection of field metadata for a segment.
// DEBT: parallel to index::FieldInfos — merge after switchover
pub(crate) struct FieldInfos {
    fields: Vec<FieldInfo>,
}

impl FieldInfos {
    pub fn new(fields: Vec<FieldInfo>) -> Self {
        Self { fields }
    }
}

/// Writes the .fnm file for a segment. Returns the file name written.
pub(crate) fn write(
    directory: &SharedDirectory,
    segment_name: &str,
    segment_suffix: &str,
    segment_id: &[u8; 16],
    field_infos: &FieldInfos,
) -> io::Result<String> {
    let file_name = index_file_names::segment_file_name(segment_name, segment_suffix, EXTENSION);
    let mut output = directory.lock().unwrap().create_output(&file_name)?;

    codec_util::write_index_header(
        &mut *output,
        CODEC_NAME,
        FORMAT_CURRENT,
        segment_id,
        segment_suffix,
    )?;

    output.write_vint(field_infos.fields.len() as i32)?;

    for fi in &field_infos.fields {
        assert!(
            fi.stored,
            "field_infos::write only supports stored-only fields (field {:?} has stored=false)",
            fi.name
        );

        debug!(
            "field_infos: field={:?} #{}, stored={}",
            fi.name, fi.number, fi.stored
        );

        // Field name
        output.write_string(&fi.name)?;

        // Field number
        output.write_vint(fi.number as i32)?;

        // Field bits: stored-only fields have no term vectors, norms, payloads, etc.
        let bits: u8 = 0b0000_0010; // OMIT_NORMS — stored-only fields omit norms
        output.write_byte(bits)?;

        // Index options: 0 = NONE (stored-only, not indexed)
        output.write_byte(0)?;

        // Doc values type: 0 = NONE
        output.write_byte(0)?;

        // Doc values skip index type: 0 = NONE
        output.write_byte(0)?;

        // Doc values gen: -1 (no doc values)
        output.write_le_long(-1)?;

        // Attributes: empty
        output.write_map_of_strings(&HashMap::new())?;

        // Point dimensions: 0 (no points)
        output.write_vint(0)?;

        // Vector dimension: 0
        output.write_vint(0)?;

        // Vector encoding: 0 = BYTE
        output.write_byte(0)?;

        // Vector similarity: 0 = EUCLIDEAN
        output.write_byte(0)?;
    }

    codec_util::write_footer(&mut *output)?;

    Ok(file_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{MemoryDirectory, SharedDirectory};

    fn test_directory() -> SharedDirectory {
        SharedDirectory::new(Box::new(MemoryDirectory::new()))
    }

    #[test]
    fn write_produces_fnm_file() {
        let dir = test_directory();
        let fields = vec![
            FieldInfo {
                name: "title".to_string(),
                number: 0,
                stored: true,
            },
            FieldInfo {
                name: "body".to_string(),
                number: 1,
                stored: true,
            },
        ];
        let fis = FieldInfos::new(fields);
        let name = write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();
        assert_eq!(name, "_0.fnm");

        let data = dir.lock().unwrap().read_file(&name).unwrap();
        // Header magic
        assert_eq!(&data[0..4], &[0x3f, 0xd7, 0x6c, 0x17]);
        // Footer magic
        let footer_start = data.len() - 16;
        assert_eq!(
            &data[footer_start..footer_start + 4],
            &[0xc0, 0x28, 0x93, 0xe8]
        );
    }

    #[test]
    fn write_encodes_field_count_and_names() {
        let dir = test_directory();
        let fields = vec![
            FieldInfo {
                name: "title".to_string(),
                number: 0,
                stored: true,
            },
            FieldInfo {
                name: "body".to_string(),
                number: 1,
                stored: true,
            },
        ];
        let fis = FieldInfos::new(fields);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();

        // Header = 4(magic) + 1+18(codec "Lucene94FieldInfos") + 4(version) + 16(id) + 1(suffix) = 44
        let offset = 44;

        // Field count = 2 (VInt)
        assert_eq!(data[offset], 2);

        // First field name: VInt length(5) + "title"
        assert_eq!(data[offset + 1], 5); // length
        assert_eq!(&data[offset + 2..offset + 7], b"title");

        // File should be substantial (2 fields with all metadata)
        assert!(data.len() > 80);
    }

    #[test]
    fn write_stored_only_field_has_omit_norms() {
        let dir = test_directory();
        let fields = vec![FieldInfo {
            name: "f".to_string(),
            number: 0,
            stored: true,
        }];
        let fis = FieldInfos::new(fields);
        write(&dir, "_0", "", &[0u8; 16], &fis).unwrap();

        let data = dir.lock().unwrap().read_file("_0.fnm").unwrap();

        // Header(44) + field_count(1) + name_len(1) + "f"(1) + field_number(1) = 48
        let bits_offset = 48;

        // bits byte: OMIT_NORMS = 0x02
        assert_eq!(data[bits_offset], 0x02);

        // index options byte: NONE = 0
        assert_eq!(data[bits_offset + 1], 0);

        // doc values type byte: NONE = 0
        assert_eq!(data[bits_offset + 2], 0);
    }
}
