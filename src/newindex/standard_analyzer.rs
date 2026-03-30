// SPDX-License-Identifier: Apache-2.0

//! Standard analyzer for tokenizing text fields.

use std::io;

use crate::newindex::analyzer::{Analyzer, Token};

/// Standard text analyzer.
///
/// Will tokenize, lowercase, and filter stop words from text fields.
#[derive(Debug)]
pub struct StandardAnalyzer;

impl Analyzer for StandardAnalyzer {
    fn next_token<'b>(
        &mut self,
        _reader: &mut dyn io::Read,
        _buf: &'b mut String,
    ) -> io::Result<Option<Token<'b>>> {
        todo!("StandardAnalyzer tokenization not yet implemented")
    }

    fn reset(&mut self) {}
}
