pub type NodeId = u64;
pub type LogIndex = u64;
pub type Term = u64;
pub type ReadId = u64;

#[derive(Debug, Copy, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Entry {
    pub term: Term,
    pub index: LogIndex,
    pub payload: Payload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Payload {
    Noop, // leader change
    Command(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotMeta {
    pub last_index: LogIndex,
    pub last_term: Term,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct ReadReady {
    pub id: ReadId,
    pub index: LogIndex,
}

#[derive(Debug)]
pub struct Restored {
    pub hard_state: HardState,
    pub snapshot: Option<SnapshotMeta>,
    pub entries: Vec<Entry>,
}

#[derive(Debug)]
pub struct NotLeader {
    pub leader_hint: Option<NodeId>,
}
