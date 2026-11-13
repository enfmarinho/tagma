use std::io;

use tagma_raft::{Ready, Restored};

use crate::fs::Fs;

pub struct Storage<F: Fs> {
    fs: F,
}

/// Data to rebuild a replica after a restart
pub struct Recovered {
    pub restored: Restored,
    pub snapshot_data: Option<Vec<u8>>,
}

impl<F: Fs> Storage<F> {
    /// Replays the log from the disk, truncating any torn data left in the tail, possible by a
    /// crash
    pub fn open(fs: F) -> io::Result<(Self, Recovered)> {
        todo!()
    }

    /// Step on of the [`Ready`] contract. Persist the durable state to the disk
    /// Returns only when data is persisted
    pub fn persist(&mut self, ready: &Ready) -> io::Result<()> {
        todo!()
    }
}
