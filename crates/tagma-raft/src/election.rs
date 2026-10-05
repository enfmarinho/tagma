use crate::{
    message::{RequestVote, RequestVoteResp},
    raft::Raft,
    types::{NodeId, Term},
};

impl Raft {
    pub fn on_request_vote(&mut self, from: NodeId, msg: RequestVote) {
        todo!()
    }

    pub fn on_request_vote_resp(&mut self, from: NodeId, msg: RequestVoteResp) {
        todo!()
    }

    pub fn campaign(&mut self) {
        todo!()
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
