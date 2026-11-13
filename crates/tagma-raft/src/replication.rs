use crate::{
    message::{AppendEntries, AppendEntriesResp, Message},
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
        if msg.term < self.hard_state.term {
            // stale message
            Self::send(
                &mut self.ready,
                self.config.id,
                from,
                Message::AppendEntriesResp(AppendEntriesResp {
                    term: self.hard_state.term,
                    last_index: msg.prev_log_index,
                    success: false,
                }),
            );
            return;
        }

        if msg.term > self.hard_state.term || matches!(self.role, Role::Candidate { .. }) {
            self.become_follower(msg.term, Some(from));
        }
        assert!(matches!(self.role, Role::Follower { .. }));

        self.reset_election_timer();

        // Log Matching Property
        let prev_matches = self
            .log
            .term_at(msg.prev_log_index)
            .is_some_and(|t| t == msg.prev_log_term);
        if !prev_matches {
            Self::send(
                &mut self.ready,
                self.config.id,
                from,
                Message::AppendEntriesResp(AppendEntriesResp {
                    term: self.hard_state.term,
                    last_index: msg.prev_log_index,
                    success: false,
                }),
            );
            return;
        }

        // TODO, cannot do this
        self.log.append(&msg.entries);

        let last_new_index = msg.prev_log_index + msg.entries.len() as u64;
        if msg.leader_commit > self.commit_index {
            self.commit_index = msg.leader_commit.min(last_new_index);
        }

        Self::send(
            &mut self.ready,
            self.config.id,
            from,
            Message::AppendEntriesResp(AppendEntriesResp {
                term: self.hard_state.term,
                last_index: last_new_index,
                success: true,
            }),
        );
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
