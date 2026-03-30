// SPDX-License-Identifier: Apache-2.0

/// Generates unique 16-byte identifiers for segments.
///
/// Production implementations use OS randomness. Test implementations
/// return deterministic values for reproducible output.
// LOCKED
pub trait IdGenerator: Send {
    /// Returns the next 16-byte identifier.
    fn next_id(&mut self) -> [u8; 16];
}
