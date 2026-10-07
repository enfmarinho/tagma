use crate::types::{LogIndex, NodeId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
pub(crate) enum Role {
    Follower {
        leader: Option<NodeId>,
    },
    Candidate {
        votes: BTreeSet<NodeId>,
    },
    Leader {
        next_index: BTreeMap<NodeId, Progress>,
    },
}

#[derive(Debug)]
pub struct Progress {
    pub(crate) next_index: LogIndex,
    pub(crate) match_index: LogIndex,
}
