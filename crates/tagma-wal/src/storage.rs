use crate::{
    frame::SnapshotFrameHeader,
    frame::{self, Batch, WalFrameHeader},
    fs::Fs,
};
use std::{
    collections::VecDeque,
    io::{self, Error},
};
use tagma_raft::{HardState, LogIndex, Ready, Restored, Snapshot, SnapshotMeta, Term};

/// Filename prefix for segmented WAL files (`WAL_FILE_PREF-{segment_id}.log`).
pub(crate) const WAL_FILE_PREF: &str = "wal";

/// Filename prefix for state machine snapshots (`SNAPSHOT_FILE_PREF-{index}-{term}.log`).
pub(crate) const SNAPSHOT_FILE_PREF: &str = "snapshot";

/// Magic number marking the start of a serialized WAL batch frame.
pub(crate) const WAL_MAGIC: u32 = u32::from_le_bytes(*b"TGM\x01");

/// WAL Segment size in MB.
pub(crate) const SEGMENT_SIZE: usize = 64 * 1024 * 1024;

/// WAL sealed segment metadata.
struct SealedSegment {
    id: u64,
    max_index: LogIndex,
}

/// Persistent storage engine managing the segmented Raft WAL and the snapshot files.
pub struct Storage<F: Fs> {
    fs: F,

    /// Id of the current active WAL segment.
    active_segment_id: u64,

    /// Byte offset within the current WAL segment, i.e. how many bytes has already been written.
    active_segment_offset: usize,

    /// Max index present in the active WAL segment.
    active_segment_max_index: LogIndex,

    /// Index when the latest snapshot was taken.
    latest_snapshot_index: LogIndex,

    /// Sealed segments metadata.
    sealed_segments: VecDeque<SealedSegment>,

    /// Reusable buffer for WAL batch frame serialization.
    scratch_buf: Vec<u8>,
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
            return Ok((
                Self {
                    fs,
                    active_segment_id: 1,
                    active_segment_offset: 0,
                    active_segment_max_index: 0,
                    latest_snapshot_index: 0,
                    sealed_segments: VecDeque::new(),
                    scratch_buf: Vec::new(),
                },
                Recovered {
                    restored: Restored {
                        hard_state: HardState {
                            term: 0,
                            voted_for: None,
                        },
                        snapshot: None,
                        entries: Vec::new(),
                    },
                    snapshot_data: None,
                },
            ));
        }

        // check for a missing segment pair
        // replay segments in order

        todo!()
    }

    fn scan_dir(fs: &mut F) -> io::Result<DirInventory> {
        let mut inv = DirInventory {
            segments: Vec::new(),
            snapshots: Vec::new(),
        };
        let mut deleted = false;

        for name in fs.list()? {
            if let Some(id) = Self::parse_segment_filename(&name) {
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
        fs.create(&Self::segment_filename_log(1), SEGMENT_SIZE)?;
        fs.sync_dir()?;
        Ok(())
    }

    fn segment_filename(id: u64) -> String {
        WAL_FILE_PREF.to_string() + "-" + &id.to_string()
    }
    fn segment_filename_log(id: u64) -> String {
        Self::segment_filename(id) + ".log"
    }
    fn segment_filename_tmp(id: u64) -> String {
        Self::segment_filename(id) + ".tmp"
    }

    fn snapshot_filename(meta: SnapshotMeta) -> String {
        SNAPSHOT_FILE_PREF.to_string()
            + "-"
            + &meta.last_index.to_string()
            + "-"
            + &meta.last_term.to_string()
    }
    fn snapshot_filename_log(meta: SnapshotMeta) -> String {
        Self::snapshot_filename(meta) + ".log"
    }
    fn snapshot_filename_tmp(meta: SnapshotMeta) -> String {
        Self::snapshot_filename(meta) + ".tmp"
    }

    fn parse_segment_filename(filename: &str) -> Option<u64> {
        filename
            .strip_prefix(WAL_FILE_PREF)?
            .strip_suffix(".log")?
            .parse()
            .ok()
    }

    fn parse_snapshot_filename(filename: &str) -> Option<(LogIndex, Term)> {
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
            self.persist_snapshot(snapshot)?;
        }

        self.scratch_buf.clear();
        self.scratch_buf.resize(WalFrameHeader::LEN, 0);

        let batch = Batch::new(ready);
        frame::encode(&batch, &mut self.scratch_buf);

        let payload_len = self.scratch_buf[WalFrameHeader::LEN..].len();

        if payload_len > SEGMENT_SIZE {
            // this is an unrealistic control path, considering the unlikelihood of the
            // Vec<Entry> growing so much to make this happens. I believe its impossible
            return Err(Error::new(
                io::ErrorKind::InvalidInput,
                "Payload is bigger than a segment file.",
            ));
        }

        // Create new segment
        if payload_len + self.active_segment_offset > SEGMENT_SIZE {
            self.new_segment()?;
        }

        if let Some(max_index) = ready.entries.last().map(|e| e.index) {
            self.active_segment_max_index = max_index;
        };
        let payload = &self.scratch_buf[WalFrameHeader::LEN..];

        // Safety: this cast to u32 is safe since payload_len is smaller than SEGMENT_SIZE
        let frame_header = WalFrameHeader::new(payload_len as u32, crc32fast::hash(payload));

        self.scratch_buf[..WalFrameHeader::LEN].copy_from_slice(&frame_header.to_bytes());

        let segment_filename = &Self::segment_filename(self.active_segment_id);
        let segment_filename_tmp = &(segment_filename.clone() + ".tmp");
        let segment_filename_log = &(segment_filename.clone() + ".log");

        self.fs.write(segment_filename_tmp, &self.scratch_buf)?;
        self.fs.sync(segment_filename_tmp)?;

        self.fs.rename(segment_filename_tmp, segment_filename_log)?;
        self.fs.sync_dir()?;

        Ok(())
    }

    /// Persist the snapshot bytes to disk.
    ///
    /// Encoded as:
    /// ```text
    /// |---------------------------------------------------------|
    /// | Length (8B) | CRC32 (4B) | Padding (4B) | Bytes payload |
    /// |---------------------------------------------------------|
    /// ```
    ///
    /// # Errors
    /// Returns an [`io::Error`] in case any underlying `fs` operations fails.
    fn persist_snapshot(&mut self, snapshot: &Snapshot) -> io::Result<()> {
        let payload = &snapshot.data;
        let len = payload.len();
        let num_bytes = 8 + 4 + len;

        self.scratch_buf.clear();
        self.scratch_buf.resize(num_bytes, 0);

        let frame_header = SnapshotFrameHeader::new(len, crc32fast::hash(payload));
        self.scratch_buf[..SnapshotFrameHeader::LEN].copy_from_slice(&frame_header.to_bytes());
        self.scratch_buf.extend_from_slice(payload);

        let snapshot_filename = &Self::snapshot_filename(snapshot.meta);
        let snapshot_filename_tmp = &(snapshot_filename.clone() + ".tmp");
        let snapshot_filename_log = &(snapshot_filename.clone() + ".log");

        self.fs.write(snapshot_filename_tmp, &self.scratch_buf)?;
        self.fs.sync(snapshot_filename_tmp)?;

        self.fs
            .rename(snapshot_filename_tmp, snapshot_filename_log)?;
        self.fs.sync_dir()?;

        let old_snapshot_meta = self.latest_snapshot_meta;
        self.latest_snapshot_meta = snapshot.meta;

        let old_snapshot_filename = Self::snapshot_filename_log(old_snapshot_meta);
        self.fs.remove(&old_snapshot_filename)?;
        self.fs.sync_dir()?;

        Ok(())
    }

    fn new_segment(&mut self) -> io::Result<()> {
        let new_segment_id = self.active_segment_id + 1;
        let segment_filename_tmp = Self::segment_filename_tmp(new_segment_id);
        let segment_filename_log = Self::segment_filename_log(new_segment_id);

        self.fs.create(&segment_filename_tmp, SEGMENT_SIZE)?;
        self.fs.sync(&segment_filename_tmp)?;
        self.fs
            .rename(&segment_filename_tmp, &segment_filename_log)?;
        self.fs.sync_dir()?;

        self.active_segment_id += 1;
        self.active_segment_offset = 0;
        self.active_segment_max_index = 0;
        self.sealed_segments.push_back(SealedSegment {
            id: self.active_segment_id,
            max_index: self.active_segment_max_index,
        });

        Ok(())
    }

    /// Clean up storage space by removing superseded WAL segments.
    /// Delete all segments whose max stored log index is smaller than `compact_to`.
    ///
    /// # Errors
    /// Returns an [`io::Error`] in case any underlying `fs` operations fails.
    pub fn compact(&mut self, compact_to: LogIndex) -> io::Result<()> {
        todo!()
    }
}
