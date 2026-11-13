use std::collections::BTreeMap;
use tagma_raft::{Config, Entry, Envelope, LogIndex, Raft, ReadId, ReadReady, Ready, Restored};

use crate::{
    api::{Reply, Request},
    machine::KvMachine,
};

pub struct Replica {
    raft: Raft,
    machine: KvMachine,
    pending_writes: BTreeMap<LogIndex, Request>,
    pending_reads: BTreeMap<ReadId, Request>,
}

impl Replica {
    pub fn new(config: Config, restored: Restored, snapshot: Option<&[u8]>) -> Self {
        Self {
            raft: Raft::new(config, restored),
            machine: snapshot.map_or_else(KvMachine::default, KvMachine::restore),
            pending_writes: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
        }
    }

    pub fn tick(&mut self) {
        self.raft.tick();
    }

    pub fn step(&mut self, envelope: Envelope) {
        self.raft.step(envelope);
    }

    pub fn take_ready(&mut self) -> Option<Ready> {
        self.raft.take_ready()
    }

    pub fn submit(&mut self, req: Request) -> Option<Reply> {
        todo!()
    }

    pub fn apply(&mut self, commited: &[Entry], read: &[ReadReady]) -> Vec<Reply> {
        todo!()
    }
}
