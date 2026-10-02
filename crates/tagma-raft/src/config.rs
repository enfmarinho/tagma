use crate::types::NodeId;

#[derive(Debug)]
pub struct Config {
    pub id: NodeId,
    pub voters: Vec<NodeId>,
    pub election_ticks: (u64, u64),
    pub heartbeat_ticks: u64,
    pub seed: u64,
}
