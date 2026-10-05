use crate::{
    message::{Message, RequestVote, RequestVoteResp},
    raft::Raft,
    role::Role,
    types::{NodeId, Term},
};
use std::collections::BTreeSet;

impl Raft {
    pub fn on_request_vote(&mut self, from: NodeId, msg: RequestVote) {
        todo!()
    }

    pub fn on_request_vote_resp(&mut self, from: NodeId, msg: RequestVoteResp) {
        todo!()
    }

    pub fn campaign(&mut self) {
        self.reset_election_timer();

        // Increment term
        self.update_hard_state(self.hard_state.term + 1, Some(self.config.id));

        // Vote for self
        let votes = BTreeSet::from([self.config.id]);

        // Check for quorum, happens in single node configuration
        if votes.len() >= self.quorum() {
            self.become_leader();
            return;
        }

        // Become candidate
        self.role = Role::Candidate { votes };

        // Send RequestVote RPC to all other nodes
        self.send_all(Message::RequestVote(RequestVote {
            term: self.hard_state.term,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        }));
    }

    pub fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        todo!()
    }

    pub fn become_leader(&mut self) {
        todo!()
    }

    pub fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self
            .rng
            .range(self.config.min_election_tick, self.config.max_election_tick);
    }
}
