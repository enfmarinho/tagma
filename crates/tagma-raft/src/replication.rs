use crate::{
    message::{AppendEntries, AppendEntriesResp},
    raft::Raft,
    types::NodeId,
};

impl Raft {
    pub fn on_append_entries(&mut self, from: NodeId, msg: AppendEntries) {
        todo!()
    }

    pub fn on_append_entries_resp(&mut self, from: NodeId, msg: AppendEntriesResp) {
        todo!()
    }

    pub fn send_append(&mut self, to: NodeId) {
        todo!()
    }

    pub fn broadcast_append(&mut self) {
        todo!()
    }

    pub fn maybe_advance_commit(&mut self) {
        todo!()
    }
}
