pub type NodeId = u64;
pub type LogIndex = u64;
pub type Term = u64;
pub type ReadId = u64;

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

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

pub struct ReadReady {
    pub id: ReadId,
    pub index: LogIndex,
}

pub struct Restored {
    pub hard_state: HardState,
    pub snapshot: Option<SnapshotMeta>,
    pub entries: Vec<Entry>,
}
