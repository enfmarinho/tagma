use crate::fs::Fs;
use std::io;
use tagma_raft::{LogIndex, Ready, Restored, Snapshot, Term};

/// Filename prefix for segmented WAL files (`WAL_FILE_PREF-{segment_id}.log`).
pub(crate) const WAL_FILE_PREF: &str = "wal";

/// Filename prefix for state machine snapshots (`SNAPSHOT_FILE_PREF-{index}-{term}.log`).
pub(crate) const SNAPSHOT_FILE_PREF: &str = "snapshot";

/// Magic number marking the start of a serialized WAL batch frame.
pub(crate) const WAL_MAGIC: u32 = u32::from_le_bytes(*b"TGM\x01");

/// WAL Segment size in MB.
pub(crate) const SEGMENT_SIZE: usize = 64 * 1024 * 1024;

/// Persistent storage engine managing the segmented Raft WAL and the snapshot files.
pub struct Storage<F: Fs> {
    fs: F,
}

/// Raft state and snapshot payload reconstructed from disk on startup.
pub struct Recovered {
    pub restored: Restored,
    pub snapshot_data: Option<Vec<u8>>,
}

struct DirInventory {
    segments: Vec<(u64, String)>,             // (id, filename)
    snapshots: Vec<(LogIndex, Term, String)>, // (index, term, filename)
}

impl<F: Fs> Storage<F> {
    /// Opens the `fs` storage directory, loads the latest snapshot and replays all the WAL
    /// segments in order to reconstruct the durable Raft state.
    ///
    /// In case the `fs` directory is empty, it initializes it's state. Otherwise, it performs
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
    pub fn open(mut fs: F) -> io::Result<(Self, Recovered)> {
        let inv = Self::scan_dir(&mut fs)?;

        if inv.segments.is_empty() && inv.snapshots.is_empty() {
            Self::init_fs(&mut fs)?;
            // return empty
        }

        todo!()
    }

    fn scan_dir(fs: &mut F) -> io::Result<DirInventory> {
        let mut inv = DirInventory {
            segments: Vec::new(),
            snapshots: Vec::new(),
        };
        let mut deleted = false;

        for name in fs.list()? {
            if let Some(id) = Self::parse_wal_filename(&name) {
                inv.segments.push((id, name));
            } else if let Some((index, term)) = Self::parse_snapshot_filename(&name) {
                inv.snapshots.push((index, term, name));
            } else {
                // `.tmp` files or any unrecognized file
                fs.remove(&name)?;
                deleted = true;
            }
        }

        if deleted {
            fs.sync_dir()?;
        }

        // sort so segment and snapshot files are in order, newest at the end.
        inv.segments.sort_by_key(|(idx, _)| *idx);
        inv.snapshots.sort_by_key(|(idx, term, _)| (*idx, *term));

        Ok(inv)
    }

    fn init_fs(fs: &mut F) -> io::Result<()> {
        fs.create(&Self::wal_filename(1), SEGMENT_SIZE)?;
        fs.sync_dir()?;
        Ok(())
    }

    fn wal_filename(id: u64) -> String {
        WAL_FILE_PREF.to_string() + "-" + &id.to_string() + ".log"
    }

    fn snapshot_filename(index: LogIndex, term: Term) -> String {
        SNAPSHOT_FILE_PREF.to_string() + "-" + &index.to_string() + "-" + &term.to_string + ".log"
    }

    fn parse_wal_filename(filename: &String) -> Option<u64> {
        filename
            .strip_prefix(WAL_FILE_PREF)?
            .strip_suffix(".log")?
            .parse()
            .ok()
    }

    fn parse_snapshot_filename(filename: &String) -> Option<(LogIndex, Term)> {
        let (index, term) = filename
            .strip_prefix(SNAPSHOT_FILE_PREF)?
            .strip_suffix(".log")?
            .split_once("-")?;

        Some((index.parse().ok()?, term.parse().ok()?))
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
        if let Some(snapshot) = &ready.snapshot {
            Self::persist_snapshot(&snapshot)?;
        }

        todo!()
    }

    fn persist_snapshot(snapshot: &Snapshot) -> io::Result<()> {
        todo!()
    }
}
