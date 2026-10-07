use core::{default::Default, result::Result};

use crate::{
    config::Config,
    log::RaftLog,
    message::{AppendEntries, Envelope, Message},
    ready::Ready,
    rng::Rng,
    role::Role,
    types::{Entry, HardState, LogIndex, NodeId, NotLeader, ReadId, Restored, SnapshotMeta, Term},
};

#[derive(Debug)]
pub(crate) struct Raft {
    pub config: Config,
    pub rng: Rng,
    pub hard_state: HardState,
    pub log: RaftLog,
    pub role: Role,
    pub commit_index: LogIndex,
    pub election_elapsed: u64,
    pub election_timeout: u64,
    pub heartbeat_elapsed: u64,
    pub ready: Ready,
}

impl Raft {
    pub fn new(config: Config, restored: Restored) -> Self {
        let mut rng = Rng::new(config.seed);
        let election_timeout = rng.range(config.min_election_tick, config.max_election_tick);
        Self {
            config,
            rng,
            hard_state: restored.hard_state,
            log: RaftLog::restore(restored.snapshot, restored.entries),
            role: Role::Follower { leader: None },
            commit_index: restored.snapshot.map_or(0, |s| s.last_index),
            election_elapsed: 0,
            election_timeout,
            heartbeat_elapsed: 0,
            ready: Ready::default(),
        }
    }

    /// Advances logical time by one tick.
    pub fn tick(&mut self) {
        match self.role {
            Role::Leader { .. } => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    self.send_heartbeats();
                }
            }
            Role::Follower { .. } | Role::Candidate { .. } => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    self.campaign(); // note: election_elapsed is reset here
                }
            }
        }
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

    /// Appends `command` to the leader's log and stages it for replication.
    ///
    /// If the node is the leader, the command is assigned to the next log index, appended to the
    /// log, and staged in `ready` so the driver persists in the disk and network broadcast to the
    /// followers [`Message::AppendEntries`]
    ///
    /// In a single node cluster, where the leader alone forms a quorum, the commit index advances
    /// immediately
    ///
    /// # Errors
    ///
    /// Returns [`NotLeader`] if the local node is not a [`Role::Leader`].
    pub fn propose(&mut self, command: Vec<u8>) -> Result<LogIndex, NotLeader> {
        match self.role {
            Role::Follower { leader } => {
                return Err(NotLeader {
                    leader_hint: leader,
                });
            }
            Role::Candidate { .. } => return Err(NotLeader { leader_hint: None }),
            Role::Leader { .. } => (),
        }

        let index = self.log.last_index() + 1;

        let entry = Entry {
            term: self.hard_state.term,
            index,
            payload: crate::types::Payload::Command(command),
        };

        self.append_local(&[entry]);
        self.maybe_advance_commit();
        self.broadcast_append();

        Ok(index)
    }

    /// Registers a linearisable read request for `id`.
    pub fn read_index(&mut self, id: ReadId) -> Result<(), NotLeader> {
        todo!()
    }

    /// Discards entries covered by `meta`.
    pub fn compact(&mut self, meta: SnapshotMeta) {
        debug_assert!(
            meta.last_index <= self.commit_index,
            "snapshot at {} index is past commit index {}",
            meta.last_index,
            self.commit_index
        );
        self.log.compact(meta);
    }

    /// Takes the raft output that must be handled by the driver.
    pub fn take_ready(&mut self) -> Option<Ready> {
        if self.ready.empty() {
            None
        } else {
            Some(std::mem::take(&mut self.ready))
        }
    }

    /// Appends the log and update `self.ready`, adding command to `entries`.
    pub(crate) fn append_local(&mut self, entries: &[Entry]) {
        todo!()
    }

    pub(crate) fn update_hard_state(&mut self, term: Term, voted_for: Option<NodeId>) {
        self.hard_state = HardState { term, voted_for };
    }

    pub(crate) fn send(ready: &mut Ready, from: NodeId, to: NodeId, message: Message) {
        ready.messages.push(Envelope { from, to, message });
    }

    pub(crate) fn send_all(&mut self, message: Message) {
        let id = self.config.id;
        for &voter in &self.config.voters {
            if voter == id {
                // avoid sending a message to ourself
                continue;
            }
            Self::send(&mut self.ready, id, voter, message.clone());
        }
    }

    pub(crate) fn send_heartbeats(&mut self) {
        debug_assert!(matches!(self.role, Role::Leader { .. }));
        self.send_all(Message::AppendEntries(AppendEntries {
            term: self.hard_state.term,
            prev_log_index: self.log.last_index(),
            prev_log_term: self.log.last_term(),
            entries: Vec::new(),
            leader_commit: self.commit_index,
        }));
    }
}
