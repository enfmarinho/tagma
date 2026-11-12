use crate::{
    message::{AppendEntries, AppendEntriesResp},
    raft::Raft,
    role::Role,
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
        if msg.term > self.hard_state.term {
            self.become_follower(msg.term, None);
            return;
        }
        if msg.term < self.hard_state.term {
            return;
        }

        let Role::Leader { progress } = &mut self.role else {
            return;
        };

        let Some(follower_progress) = progress.get_mut(&from) else {
            return;
        };

        if msg.success {
            if msg.last_index > follower_progress.next_index {
                follower_progress.match_index = msg.last_index;
                follower_progress.next_index = follower_progress.next_index.max(msg.last_index + 1);
                self.maybe_advance_commit();
            }
        } else {
            follower_progress.next_index = follower_progress
                .next_index
                .saturating_sub(1)
                .max(follower_progress.match_index + 1);
            self.send_append(from);
        }
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
