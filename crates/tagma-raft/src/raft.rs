use crate::{
    log::RaftLog,
    message::Envelope,
    rng::Rng,
    role::Role,
    types::{HardState, LogIndex, ReadId, SnapshotMeta},
};

pub(crate) struct Raft {
    rng: Rng,
    hard_state: HardState,
    log: RaftLog,
    role: Role,
    commit_index: LogIndex,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
}

impl Raft {
    pub fn new() -> Self {
        todo!()
    }

    /// Advance logical time by one tick
    pub fn tick(&mut self) {
        todo!()
    }

    /// Handle the 'Envelope' incoming message
    pub fn step(envelope: Envelope) {
        todo!()
    }

    /// Append `command` to the leader log
    pub fn propose(&mut self, command: Vec<u8>) {
        todo!()
    }

    /// Register a linearisable read
    pub fn read_index(&self, id: ReadId) {
        todo!()
    }

    /// Discard entries covered by `SnapshotMeta`
    pub fn compact(&mut self, meta: SnapshotMeta) {
        todo!()
    }
}
