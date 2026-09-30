use crate::types::{Entry, LogIndex, NodeId, Snapshot, Term};

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Message {
    RequestVote(RequestVote),
    RequestVoteResp(RequestVoteResp),
    AppendEntries(AppendEntries),
    AppendEntriesResp(AppendEntriesResp),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotResp(InstallSnapshotResp),
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote(m) => m.term,
            Message::RequestVoteResp(m) => m.term,
            Message::AppendEntries(m) => m.term,
            Message::AppendEntriesResp(m) => m.term,
            Message::InstallSnapshot(m) => m.term,
            Message::InstallSnapshotResp(m) => m.term,
        }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestVote {
    pub term: Term,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestVoteResp {
    pub term: Term,
    pub granted: bool,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppendEntries {
    pub term: Term,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<Entry>,
    pub leader_commit: LogIndex,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppendEntriesResp {
    pub term: Term,
    pub success: bool,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstallSnapshot {
    pub term: Term,
    pub snapshot: Snapshot,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstallSnapshotResp {
    pub term: Term,
    pub last_index: LogIndex,
}
