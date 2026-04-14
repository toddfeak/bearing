// SPDX-License-Identifier: Apache-2.0

//! Per-document numeric values, used for norms and numeric doc values.

use std::io;

use crate::search::DocIdSetIterator;

/// A per-document numeric value.
///
/// Provides lazy access to norm (or doc value) data. Callers position the
/// iterator with [`advance_exact`](Self::advance_exact), then read the value
/// with [`long_value`](Self::long_value).
///
/// Extends [`DocIdSetIterator`] for forward iteration.
pub trait NumericDocValues: DocIdSetIterator {
    /// Advances to exactly `target` and returns whether `target` has a value.
    ///
    /// After this returns, the iterator is positioned at `target`. If it returns
    /// `false`, calling [`long_value`](Self::long_value) is illegal.
    fn advance_exact(&mut self, target: i32) -> io::Result<bool>;

    /// Returns the numeric value for the current document.
    ///
    /// Must only be called after [`advance_exact`](Self::advance_exact) returned `true`.
    fn long_value(&self) -> io::Result<i64>;
}
