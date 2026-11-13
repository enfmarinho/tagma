use std::collections::BTreeMap;

use tagma_raft::LogIndex;

use crate::api::{ClientId, Key, Outcome, Request, Value};

#[derive(Default)]
pub struct KvMachine {
    data: BTreeMap<Key, Value>,
    sessions: BTreeMap<ClientId, Session>,
    last_applied: LogIndex,
}

struct Session {
    last_seq: u64,
    last_outcome: Outcome,
}

impl KvMachine {
    pub fn restore(bytes: &[u8]) -> Self {
        todo!()
    }

    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    pub fn apply(&mut self, index: LogIndex, req: &Request) -> Outcome {
        todo!()
    }

    pub fn snapshot(&self) -> Vec<u8> {
        todo!()
    }
}
