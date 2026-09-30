pub type NodeId = u64;
pub type LogIndex = u64;
pub type Term = u64;
pub type ReadId = u64;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Entry {
    term: Term,
    index: LogIndex,
    payload: Payload,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Payload {
    Noop, // leader change
    Command(Vec<u8>),
}

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
