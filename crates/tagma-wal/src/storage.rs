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

    /// Step 1 of the [`Ready`] contract. Persist the durable state to disk.
    /// Returns only when data is persisted
    ///
    /// # Binary Frame Specification
    /// All data is encoded in little endian order.
    /// `Length` is the number of bytes of `Batch Header + Entries payload`.
    /// `CRC32` is the checksum over the Batch Header and Entries payload bytes.
    ///
    /// ```text
    /// |---------------------------------------------------------------------|
    /// | Magic (4B)    | Length (4B)    | CRC32 (4B)                         |
    /// |---------------------------------------------------------------------|
    /// | Batch Header:                                                       |
    /// |     has_hard_state (1B) | term(8B) | vote (8B) | commit (8B)        |
    /// |     has_truncate (1B)   | truncate_from (8B)                        |  
    /// |     has_snapshot (1B)   | snap_index (8B)     | snap_term (8B)      |
    /// |     entry_count(8B)                                                 |
    /// |---------------------------------------------------------------------|
    /// | Entries payload:                                                    |
    /// |     [Entry 0: index (8B) | term (8B) | type (1B) | len (4B) | data] |
    /// |     etc...                                                          |
    /// |---------------------------------------------------------------------|
    /// ```
    /// If there is a snapshot to write, it will be written and synced to a separate
    /// file before writing the above WAL frame.
    ///
    /// # Errors
    /// Returns and [`io::Error`] if writing or syncing the data failed.
    /// on error durability is not garanteed and the caller cannot proceed with the
    /// [`Ready`] contract, i.e. cannot procede to step 2.
    pub fn persist(&mut self, ready: &Ready) -> io::Result<()> {
        todo!()
    }
}
