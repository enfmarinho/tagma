use crate::types::{LogIndex, NodeId};
use std::collections::{BTreeMap, BTreeSet};

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

pub struct Progress {
    next_index: LogIndex,
    match_index: LogIndex,
}
