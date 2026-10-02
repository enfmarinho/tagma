use core::default::Default;

use crate::{
    message::Envelope,
    types::{Entry, HardState, LogIndex, ReadReady, Snapshot},
};

/// Output produced by [`crate::raft::Raft`] that the driver must perform.
///
/// To ensure the correctness guarantees of the Raft algorithm, the work has to be done in the following order:
/// 1st: Persist the durable state: `hard_state`, `truncate_from`, `entries` and `snapshot`.
/// 2nd: send `messages`.
/// 3rd: apply `committed` entries to the state machine.
/// 4th: answer `reads`.
pub struct Ready {
    pub hard_state: Option<HardState>,
    pub truncate_from: Option<LogIndex>,
    pub entries: Vec<Entry>,
    pub snapshot: Option<Snapshot>,
    pub messages: Vec<Envelope>,
    pub committed: Vec<Entry>,
    pub reads: Vec<ReadReady>,
}

impl Ready {
    pub fn needs_persist(&self) -> bool {
        todo!()
    }

    pub fn empty(&self) -> bool {
        todo!()
    }
}

impl Default for Ready {
    fn default() -> Self {
        todo!()
    }
}
