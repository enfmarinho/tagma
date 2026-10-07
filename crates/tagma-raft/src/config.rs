use crate::types::NodeId;

#[derive(Debug)]
pub struct Config {
    pub id: NodeId,
    pub voters: Vec<NodeId>,
    pub min_election_tick: u64,
    pub max_election_tick: u64,
    pub heartbeat_ticks: u64,
    pub seed: u64,
}
