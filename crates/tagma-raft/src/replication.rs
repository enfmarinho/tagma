use crate::{
    message::{AppendEntries, AppendEntriesResp},
    raft::Raft,
    types::NodeId,
};

impl Raft {
    /// Returns the minimum number of voters to achieve a quorum.
    pub(crate) fn quorum(&self) -> usize {
        self.config.voters.len() / 2 + 1
    }

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

    /// Checks if there is quorum to advance the commit index, if so advance it.
    pub fn maybe_advance_commit(&mut self) {
        todo!()
    }
}
