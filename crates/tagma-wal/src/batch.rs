use std::io;

use serde::{Deserialize, Serialize};
use tagma_raft::{Entry, HardState, LogIndex, Term};

/// Atomic durable batch frame corresponding to a [`tagma_raft::Ready`] output.
#[derive(Serialize)]
struct Batch<'a> {
    pub hard_state: Option<HardState>,
    pub truncate_from: Option<LogIndex>,
    pub snapshot: Option<(LogIndex, Term)>,
    pub entries: &'a [Entry],
}

/// Atomic durable batch frame corresponding to a [`tagma_raft::Ready`] output.
#[derive(Deserialize)]
struct OwnedBatch {
    pub hard_state: Option<HardState>,
    pub truncate_from: Option<LogIndex>,
    pub snapshot: Option<(LogIndex, Term)>,
    pub entries: Vec<Entry>,
}

/// Encode a [`Batch`] and writes it to the `out` buffer.
pub(crate) fn encode(batch: &Batch<'_>, out: &mut Vec<u8>) {
    todo!()
}

/// Decode all valid [`Batch`] frames form 'buf'. Stops at the first torn or corrupted frame.
/// Returns `(batches, bytes_consumed, has_corrupted_tail)`
pub(crate) fn decode(buf: &[u8]) -> io::Result<(Vec<OwnedBatch>, usize, bool)> {
    todo!()
}
