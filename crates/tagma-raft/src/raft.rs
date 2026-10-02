use crate::{
    config::Config,
    log::RaftLog,
    message::{Envelope, Message},
    ready::Ready,
    rng::Rng,
    role::Role,
    types::{HardState, LogIndex, ReadId, SnapshotMeta},
};

pub(crate) struct Raft {
    config: Config,
    rng: Rng,
    hard_state: HardState,
    log: RaftLog,
    role: Role,
    commit_index: LogIndex,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
    ready: Ready,
}

impl Raft {
    pub fn new() -> Self {
        todo!()
    }

    /// Advances logical time by one tick.
    pub fn tick(&mut self) {
        todo!()
    }

    /// Dispatches an incoming [`Envelope`].
    pub fn step(&mut self, envelope: Envelope) {
        let from = envelope.from;
        match envelope.message {
            Message::RequestVote(m) => self.on_request_vote(from, m),
            Message::RequestVoteResp(m) => self.on_request_vote_resp(from, m),
            Message::AppendEntries(m) => self.on_append_entries(from, m),
            Message::AppendEntriesResp(m) => self.on_append_entries_resp(from, m),
            Message::InstallSnapshot(m) => self.on_install_snapshot(from, m),
            Message::InstallSnapshotResp(m) => self.on_install_snapshot_resp(from, m),
        }
    }

    /// Appends `command` to the leader's log.
    pub fn propose(&mut self, command: Vec<u8>) {
        todo!()
    }

    /// Registers a linearisable read request for `id`.
    pub fn read_index(&self, id: ReadId) {
        todo!()
    }

    /// Discards entries covered by `meta`.
    pub fn compact(&mut self, meta: SnapshotMeta) {
        todo!()
    }

    /// Takes the raft output that must be handled by the driver.
    pub fn take_ready(&mut self) -> Option<Ready> {
        if self.ready.empty() {
            None
        } else {
            Some(std::mem::take(&mut self.ready))
        }
    }
}
