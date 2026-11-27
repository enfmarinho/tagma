use crate::{
    fs::Fs,
    snapshot_frame::SnapshotFrameHeader,
    wal_frame::{self, SEGMENT_SIZE, WalBatch, WalFrame},
};
use std::{
    collections::VecDeque,
    io::{self, Error},
};
use tagma_raft::{HardState, LogIndex, Ready, Restored, Snapshot, SnapshotMeta};

/// Filename prefix for segmented WAL files (`WAL_FILE_PREF-{segment_id}.log`).
const WAL_FILE_PREF: &str = "wal";
/// Filename prefix for state machine snapshots (`SNAPSHOT_FILE_PREF-{index}-{term}.log`).
const SNAPSHOT_FILE_PREF: &str = "snapshot";
// Temporary file extension.
const TMP_FILE_EXT: &str = ".tmp";
// Log file extension.
const LOG_FILE_EXT: &str = ".log";
/// Initial capacity of the scratch buffer.
const SCRATCH_BUF_INIT_CAP: usize = 64 * 1024;

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

    /// Latest hard state metadata.
    latest_hard_state: HardState,

    /// Metadata of the latest snapshot.
    latest_snapshot_meta: Option<SnapshotMeta>,

    /// Sealed segments metadata.
    sealed_segments: VecDeque<SealedSegment>,

    /// Reusable buffer for WAL batch frame serialization.
    scratch_buf: Vec<u8>,
}

/// Raft state and snapshot payload reconstructed from disk on startup.
#[derive(Default)]
pub struct Recovered {
    pub restored: Restored,
    pub snapshot_data: Option<Vec<u8>>,
}

struct DirInventory {
    segments: Vec<(u64, String)>,           // (id, filename)
    snapshots: Vec<(SnapshotMeta, String)>, // (meta, filename)
}

impl<F: Fs> Storage<F> {
    fn new(mut fs: F) -> io::Result<Self> {
        for f in fs.list()? {
            fs.remove(&f)?;
        }

        fs.create(&Self::segment_filename_log(1), wal_frame::SEGMENT_SIZE)?;
        fs.sync_dir()?;

        Ok(Self {
            fs,
            active_segment_id: 1,
            active_segment_offset: 0,
            active_segment_max_index: 0,
            latest_hard_state: HardState::default(),
            latest_snapshot_meta: None,
            sealed_segments: VecDeque::new(),
            scratch_buf: Vec::with_capacity(SCRATCH_BUF_INIT_CAP),
        })
    }

    fn scan_dir(fs: &mut F) -> io::Result<DirInventory> {
        let mut inv = DirInventory {
            segments: Vec::new(),
            snapshots: Vec::new(),
        };
        let mut deleted = false;

        for f in fs.list()? {
            if let Some(id) = Self::parse_segment_filename(&f) {
                inv.segments.push((id, f));
            } else if let Some(meta) = Self::parse_snapshot_filename(&f) {
                inv.snapshots.push((meta, f));
            } else {
                // `.tmp` files or any unrecognized file
                fs.remove(&f)?;
                deleted = true;
            }
        }
        if deleted {
            fs.sync_dir()?;
        }

        // sort so segment and snapshot files are in order, newest at the end.
        inv.segments.sort_by_key(|(idx, _)| *idx);
        inv.snapshots
            .sort_by_key(|(meta, _)| (meta.last_index, meta.last_term));

        Ok(inv)
    }

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
            return Ok((Self::new(fs)?, Recovered::default()));
        }

        Self::check_for_missing_segment(&inv)?;

        let snapshot = Self::load_snapshot(&mut fs, &inv)?;
        // Delete all snapshots older than the one used
        let mut deleted = false;
        let meta = if let Some(snapshot) = &snapshot {
            for (meta, filename) in inv.snapshots {
                if meta.last_index < snapshot.meta.last_index {
                    fs.remove(&filename)?;
                    deleted = true;
                }
            }
            if deleted {
                fs.sync_dir()?;
            }

            Some(snapshot.meta)
        } else {
            None
        };

        let mut last_id: u64 = 0;
        let mut last_offset = 0;
        let mut last_max_index = 0;
        let mut last_hard_state = HardState {
            term: 0,
            voted_for: None,
        };
        let mut entries = Vec::new();
        for (id, filename) in &inv.segments {
            // TODO improve error handling, it may still be possible to build the state without a
            // giving segment
            let buf = match fs.read(filename)? {
                Some(v) => v,
                None => {
                    // segment
                    fs.remove(filename)?;
                    fs.sync_dir()?;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "WAL segment could not be read",
                    ));
                }
            };

            let out = wal_frame::WalFrame::decode_all(&buf);
            if out.has_corrupted_tail {
                fs.truncate(filename, out.bytes_consumed as u64)?;
                fs.sync(filename)?;
            }

            last_id = *id;
            last_offset = out.bytes_consumed;

            let Some(last_batch) = out.batches.last() else {
                continue;
            };
            for batch in &out.batches {
                entries.extend_from_slice(batch.entries.as_slice());
                last_max_index = if let Some(entry) = entries.last() {
                    entry.index
                } else {
                    0
                };
            }

            last_hard_state = last_batch.batch_header.hard_state;
        }

        if inv.segments.is_empty() { // TODO: all segments were compacted after a snapshot
        }

        Ok((
            Self {
                fs,
                active_segment_id: last_id,
                latest_hard_state: last_hard_state,
                latest_snapshot_meta: meta,
                active_segment_offset: last_offset,
                active_segment_max_index: last_max_index,
                sealed_segments: VecDeque::new(),
                scratch_buf: Vec::with_capacity(SEGMENT_SIZE),
            },
            Recovered {
                restored: Restored {
                    hard_state: last_hard_state,
                    snapshot: meta,
                    entries,
                },
                snapshot_data: snapshot.map(|s| s.data),
            },
        ))
    }

    fn segment_filename_with_ext(id: u64, ext: &str) -> String {
        format!("{}-{}{}", WAL_FILE_PREF, id, ext)
    }
    fn segment_filename_log(id: u64) -> String {
        Self::segment_filename_with_ext(id, LOG_FILE_EXT)
    }
    fn segment_filename_tmp(id: u64) -> String {
        Self::segment_filename_with_ext(id, TMP_FILE_EXT)
    }

    fn snapshot_filename_with_ext(meta: SnapshotMeta, ext: &str) -> String {
        format!(
            "{}-{}-{}{}",
            SNAPSHOT_FILE_PREF, meta.last_index, meta.last_term, ext
        )
    }
    fn snapshot_filename_log(meta: SnapshotMeta) -> String {
        Self::snapshot_filename_with_ext(meta, LOG_FILE_EXT)
    }
    fn snapshot_filename_tmp(meta: SnapshotMeta) -> String {
        Self::snapshot_filename_with_ext(meta, TMP_FILE_EXT)
    }

    fn parse_segment_filename(filename: &str) -> Option<u64> {
        filename
            .strip_prefix(WAL_FILE_PREF)?
            .strip_suffix(".log")?
            .parse()
            .ok()
    }

    fn parse_snapshot_filename(filename: &str) -> Option<SnapshotMeta> {
        let (index, term) = filename
            .strip_prefix(SNAPSHOT_FILE_PREF)?
            .strip_suffix(".log")?
            .split_once("-")?;

        Some(SnapshotMeta {
            last_index: index.parse().ok()?,
            last_term: term.parse().ok()?,
        })
    }

    fn check_for_missing_segment(inv: &DirInventory) -> io::Result<()> {
        // TODO improve, this is to strict and may invalidate valid conditions
        for pair in inv.segments.windows(2) {
            if pair[1].0 != pair[0].0 + 1 {
                return Err(Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "missing WAL segments between {} and {}",
                        pair[0].0, pair[1].0
                    ),
                ));
            }
        }
        Ok(())
    }

    fn load_snapshot(fs: &mut F, inv: &DirInventory) -> io::Result<Option<Snapshot>> {
        let Some((meta, _)) = inv.snapshots.last() else {
            return Ok(None);
        };

        let filename = Self::snapshot_filename_log(*meta);
        let snapshot_frame_data = fs.read(&filename)?;
        let Some(mut raw_bytes) = snapshot_frame_data else {
            return Ok(None);
        };

        let (frame_header_bytes, _) = raw_bytes.split_at(SnapshotFrameHeader::LEN);

        let frame_header_bytes = frame_header_bytes
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid snapshot."))?;

        let snapshot_header = SnapshotFrameHeader::from_bytes(frame_header_bytes);

        raw_bytes.copy_within(SnapshotFrameHeader::LEN.., 0);
        raw_bytes.truncate(raw_bytes.len() - SnapshotFrameHeader::LEN);


        let crc = crc32fast::hash(&raw_bytes);

        if crc != snapshot_header.crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid snapshot: CRC key does not match.",
            ));
        }
        if raw_bytes.len() != snapshot_header.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid snapshot: file lenght does not match expected `len`.",
            ));
        }

        Ok(Some(Snapshot {
            meta: *meta,
            data: raw_bytes
        }))
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
    /// |     has_hard_state (1B) | term(8B)            | vote (8B)           |
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

        if let Some(hs) = ready.hard_state {
            self.latest_hard_state = hs;
        }

        let batch = WalBatch::from_ready(ready, self.latest_hard_state, self.latest_snapshot_meta);
        self.scratch_buf.clear();
        WalFrame::encode(&batch, &mut self.scratch_buf)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Could not encode ready."))?;

        let len = self.scratch_buf.len();

        if len > SEGMENT_SIZE {
            return Err(Error::new(
                io::ErrorKind::InvalidInput,
                "Payload is bigger than a segment file.",
            ));
        }

        // Create new segment
        if len + self.active_segment_offset > SEGMENT_SIZE {
            self.new_segment()?;
        }

        if let Some(max_index) = ready.entries.last().map(|e| e.index) {
            self.active_segment_max_index = max_index;
        };

        let filename_tmp = Self::segment_filename_tmp(self.active_segment_id);
        self.fs.write(&filename_tmp, &self.scratch_buf)?;
        self.fs.sync(&filename_tmp)?;

        let filename_log = Self::segment_filename_log(self.active_segment_id);
        self.fs.rename(&filename_tmp, &filename_log)?;
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
        let num_bytes = SnapshotFrameHeader::LEN + len;

        self.scratch_buf.clear();
        self.scratch_buf.resize(num_bytes, 0);

        let frame_header = SnapshotFrameHeader::new(len, crc32fast::hash(payload));
        self.scratch_buf[..SnapshotFrameHeader::LEN].copy_from_slice(&frame_header.to_bytes());
        self.scratch_buf.extend_from_slice(payload);

        let filename_tmp = Self::snapshot_filename_tmp(snapshot.meta);

        self.fs.write(&filename_tmp, &self.scratch_buf)?;
        self.fs.sync(&filename_tmp)?;

        let filename_log = Self::snapshot_filename_log(snapshot.meta);
        self.fs.rename(&filename_tmp, &filename_log)?;
        self.fs.sync_dir()?;

        let old_meta = self.latest_snapshot_meta;
        self.latest_snapshot_meta = Some(snapshot.meta);

        if let Some(old_snapshot_meta) = old_meta {
            let old_snapshot_filename = Self::snapshot_filename_log(old_snapshot_meta);
            self.fs.remove(&old_snapshot_filename)?;
            self.fs.sync_dir()?;
        }

        Ok(())
    }

    fn new_segment(&mut self) -> io::Result<()> {
        let new_segment_id = self.active_segment_id + 1;

        let filename_tmp = Self::segment_filename_tmp(new_segment_id);
        self.fs.create(&filename_tmp, wal_frame::SEGMENT_SIZE)?;
        self.fs.sync(&filename_tmp)?;

        let filename_log = Self::segment_filename_log(new_segment_id);
        self.fs.rename(&filename_tmp, &filename_log)?;
        self.fs.sync_dir()?;

        self.sealed_segments.push_back(SealedSegment {
            id: self.active_segment_id,
            max_index: self.active_segment_max_index,
        });
        self.active_segment_id += 1;
        self.active_segment_offset = 0;
        self.active_segment_max_index = 0;

        Ok(())
    }

    /// Clean up storage space by removing superseded WAL segments.
    /// Delete all segments whose max stored log index is smaller than `compact_to`,
    /// given that there is a valid snapshot, i.e. a snapshot whose index is bigger
    /// than `compact_to`.
    ///
    /// # Errors
    /// Returns an [`io::Error`] in case any underlying `fs` operations fails.
    pub fn compact(&mut self, compact_to: LogIndex) -> io::Result<()> {
        let meta = if let Some(meta) = self.latest_snapshot_meta {
            meta
        } else {
            return Ok(());
        };

        if meta.last_index < compact_to {
            return Ok(());
        }

        let mut removed = false;
        for segment in &self.sealed_segments {
            if segment.max_index < compact_to {
                self.fs.remove(&Self::segment_filename_log(segment.id))?;
                removed = true;
            }
        }
        if removed {
            self.fs.sync_dir()?;
        }

        Ok(())
    }
}
