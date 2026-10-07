use crate::{
    message::{Message, RequestVote, RequestVoteResp},
    raft::Raft,
    role::{Progress, Role},
    types::{Entry, NodeId, Payload, Term},
};
use std::collections::{BTreeMap, BTreeSet};

impl Raft {
    pub fn on_request_vote(&mut self, from: NodeId, msg: RequestVote) {
        // only grant vote if candidate's log is not older than ours
        let granted = msg.term > self.log.last_term()
            || (msg.term == self.log.last_term() && msg.last_log_index >= self.log.last_index());

        Self::send(
            &mut self.ready,
            self.config.id,
            from,
            Message::RequestVoteResp(RequestVoteResp {
                term: self.hard_state.term,
                granted,
            }),
        );
    }

    pub fn on_request_vote_resp(&mut self, from: NodeId, msg: RequestVoteResp) {
        if msg.term > self.hard_state.term {
            // Term validation
            self.become_follower(msg.term, None);
            return;
        }
        if msg.term < self.hard_state.term {
            // Ignore stale vote
            return;
        }
        if !msg.granted {
            // Vote not granted
            return;
        }

        // Ignore vote if we are not a candidate anymore
        let Role::Candidate { votes } = &mut self.role else {
            return;
        };

        votes.insert(from);
        if votes.len() >= self.quorum() {
            self.become_leader();
        }
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
        debug_assert!(
            term >= self.hard_state.term,
            "cannot become follower with a regressed term: {} < {}",
            term,
            self.hard_state.term
        );

        self.heartbeat_elapsed = 0;

        let voted_for = if term > self.hard_state.term {
            self.hard_state.voted_for
        } else {
            None
        };

        self.role = Role::Follower { leader };
        self.update_hard_state(term, voted_for);
    }

    pub fn become_leader(&mut self) {
        let last_index = self.log.last_index();
        let mut next_index: BTreeMap<NodeId, Progress> = BTreeMap::new();
        for &voter in &self.config.voters {
            let is_self = voter == self.config.id;
            next_index.insert(
                voter,
                Progress {
                    next_index: last_index + 1,
                    match_index: if is_self { last_index } else { 0 },
                },
            );
        }

        self.role = Role::Leader { next_index };
        self.heartbeat_elapsed = 0;

        // append no-op entry
        self.append_local(&[Entry {
            term: self.hard_state.term,
            index: self.log.last_index() + 1,
            payload: Payload::Noop,
        }]);
        self.broadcast_append();

        self.maybe_advance_commit();
    }

    pub fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self
            .rng
            .range(self.config.min_election_tick, self.config.max_election_tick);
    }
}
