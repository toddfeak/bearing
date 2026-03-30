// SPDX-License-Identifier: Apache-2.0

// Placeholder bounded channel. Will be replaced with crossbeam-channel.

use std::io;

use crate::newindex::document::Document;

/// Sending half of a bounded channel.
pub struct Sender {
    // Will hold crossbeam_channel::Sender<Document>
}

/// Receiving half of a bounded channel.
pub struct Receiver {
    // Will hold crossbeam_channel::Receiver<Document>
}

/// Creates a bounded channel with the given capacity.
///
/// `send()` blocks when the channel is full.
/// Returns the sending and receiving halves.
pub fn bounded(_capacity: usize) -> (Sender, Receiver) {
    todo!("replace with crossbeam_channel::bounded")
}

impl Sender {
    /// Sends a document into the channel. Blocks if the channel is full.
    pub fn send(&self, _doc: Document) -> io::Result<()> {
        todo!("replace with crossbeam send")
    }
}

impl Receiver {
    /// Receives a document from the channel. Blocks if empty.
    pub fn recv(&self) -> Option<Document> {
        todo!("replace with crossbeam recv")
    }
}
