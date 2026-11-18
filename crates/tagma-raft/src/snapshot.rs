use crate::{
    message::{InstallSnapshot, InstallSnapshotResp},
    raft::Raft,
    types::NodeId,
};

impl Raft {
    pub fn on_install_snapshot(&mut self, _from: NodeId, _msg: InstallSnapshot) {
        todo!()
    }

    pub fn on_install_snapshot_resp(&mut self, _from: NodeId, _msg: InstallSnapshotResp) {
        todo!()
    }
}
