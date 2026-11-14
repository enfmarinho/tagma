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
    /// Opens the `fs` storage directory, loads the latest snapshot and replays all the WAL
    /// segments in order to reconstruct the durable Raft state.
    ///
    /// In case the `fs` directoy is empty, it initializes it's state. Otherwise, it performs
    /// the following operation in order:
    /// 1. Removed any `.tmp` files from interrupted writes.
    /// 2. Loads and verify the checksum of the latest snapshot, if there is one.
    /// 3. Scans all WAL segments in order, verifying its data and applying all frames to the
    ///    rebuild raft state.
    /// 4. Truncate any torn writes at the tail of the WAL segments
    ///
    /// Returns the initialized Raft WAL and the recovered state machine bytes in order to boot
    /// the Raft in memory WAL and the KvMachine.
    ///
    /// # Errors
    /// Returns an [`io::Error`] in case any underlying `fs` operations fails or the latest snapshot
    /// is corrupted or missing.
    pub fn open(fs: F) -> io::Result<(Self, Recovered)> {
        todo!()
    }

    /// Step on of the [`Ready`] contract. Persist the durable state to the disk
    /// Returns only when data is persisted
    pub fn persist(&mut self, ready: &Ready) -> io::Result<()> {
        todo!()
    }
}
