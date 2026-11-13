use serde::{Deserialize, Serialize};
use tagma_raft::NodeId;

pub type ClientId = u64;
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Serialize, Deserialize)]
pub enum Op {
    Get {
        key: Key,
    },
    Put {
        key: Key,
        value: Value,
    },
    Delete {
        key: Key,
    },
    Cas {
        key: Key,
        expected: Option<Value>,
        new: Value,
    },
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub client: ClientId,
    pub seq: u64,
    pub op: Op,
}

impl Request {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("Request is always serializable")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

pub enum Outcome {
    Value(Option<Value>),
    Done,
    CasFailed { current: Option<Value> },
}

pub enum Response {
    Ok(Outcome),
    NotLeader { leader_hint: Option<NodeId> },
}

pub struct Reply {
    client: ClientId,
    seq: u64,
    response: Response,
}
