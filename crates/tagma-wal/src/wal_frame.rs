use core::fmt;
use tagma_raft::{
    Entry, HardState, LogIndex, PAYLOAD_COMMAND, PAYLOAD_NOOP, Payload, Ready, SnapshotMeta,
};

/// Magic number marking the start of a serialized WAL batch frame.
const WAL_MAGIC: u32 = u32::from_le_bytes(*b"TGM\x01");

/// WAL Segment size in MB.
pub(crate) const SEGMENT_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalError {
    FrameToLarge { len: usize, max: usize },
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FrameToLarge { len, max } => {
                write!(
                    f,
                    "WAL frame payload ({len} bytes) exceeds max ({max} bytes)"
                )
            }
        }
    }
}
impl std::error::Error for WalError {}

pub(crate) struct DecodeOutcome {
    pub batches: Vec<WalBatch>,
    pub bytes_consumed: usize,
    pub has_corrupted_tail: bool,
}

pub(crate) struct WalFrame;

impl WalFrame {
    /// Encode a [`Batch`] and writes it to the `out` buffer.
    ///
    /// |--------------------------------------------------------------------|
    /// | Magic (4b)    | Length (4b)    | CRC32 (4b)                        |
    /// |--------------------------------------------------------------------|
    /// | Batch Header:                                                      |
    /// |     term(8b)            | has_voted_for (1b)  | voted_for (8b)     |
    /// |     has_truncate (1b)   | truncate_from (8b)                       |  
    /// |     has_snapshot (1b)   | snap_index (8b)     | snap_term (8b)     |
    /// |     entry_count(8b)                                                |
    /// |--------------------------------------------------------------------|
    /// | entries payload:                                                   |
    /// |     [entry 0: index (8b) | term (8b) | type (1b) | len (4b) | data |
    /// |     etc...                                                         |
    /// |--------------------------------------------------------------------|
    pub(crate) fn encode<E: AsRef<[Entry]>>(
        batch: &WalBatch<E>,
        out: &mut Vec<u8>,
    ) -> Result<(), WalError> {
        let entries = batch.entries.as_ref();

        let mut entries_len: usize = 0;
        for entry in entries {
            entries_len += EntryRecord::new(entry).encoded_len()?;
        }
        let wal_batch_len = BatchHeader::LEN + entries_len;
        let frame_len = WalFrameHeader::LEN + wal_batch_len;
        if frame_len > SEGMENT_SIZE {
            return Err(WalError::FrameToLarge {
                len: frame_len,
                max: SEGMENT_SIZE,
            });
        }

        let start = out.len();
        out.resize(start + frame_len, 0);

        let batch_start = start + WalFrameHeader::LEN;
        let mut w = WriteCursor::new(&mut out[batch_start..]);
        batch.encode(&mut w);
        debug_assert_eq!(w.remaining(), 0);

        let crc32 = crc32fast::hash(&out[batch_start..]);
        let wal_batch_len =
            u32::try_from(wal_batch_len).expect("max size (SEGMENT_SIZE) fits in u32");
        let frame_header = WalFrameHeader::new(wal_batch_len, crc32);

        out[start..batch_start].copy_from_slice(&frame_header.to_bytes());

        Ok(())
    }

    pub(crate) fn decode_all(buf: &[u8]) -> DecodeOutcome {
        let mut batches: Vec<WalBatch> = Vec::new();
        let mut bytes_consumed = 0;

        while bytes_consumed < buf.len() {
            let rem = &buf[bytes_consumed..];

            // torn frame header
            if rem.len() < WalFrameHeader::LEN {
                return DecodeOutcome {
                    batches,
                    bytes_consumed,
                    has_corrupted_tail: true,
                };
            }

            let frame_header =
                WalFrameHeader::from_bytes(rem[..WalFrameHeader::LEN].try_into().unwrap());

            if frame_header.magic == 0 {
                let is_tail_clean = rem.iter().all(|&v| v == 0);
                return DecodeOutcome {
                    batches,
                    bytes_consumed,
                    has_corrupted_tail: !is_tail_clean,
                };
            }

            // corrupted magic
            if frame_header.magic != WAL_MAGIC {
                return DecodeOutcome {
                    batches,
                    bytes_consumed,
                    has_corrupted_tail: true,
                };
            }

            // torn payload
            let frame_len = WalFrameHeader::LEN + frame_header.len as usize;
            if frame_len > rem.len() {
                return DecodeOutcome {
                    batches,
                    bytes_consumed,
                    has_corrupted_tail: true,
                };
            }

            // corrupted payload
            let payload = &rem[WalFrameHeader::LEN..frame_len];
            if frame_header.crc != crc32fast::hash(payload) {
                return DecodeOutcome {
                    batches,
                    bytes_consumed,
                    has_corrupted_tail: true,
                };
            }

            // batch decode failed
            let Some(batch) = WalBatch::decode(payload) else {
                return DecodeOutcome {
                    batches,
                    bytes_consumed,
                    has_corrupted_tail: true,
                };
            };

            bytes_consumed += frame_len;
            batches.push(batch);
        }

        DecodeOutcome {
            batches,
            bytes_consumed,
            has_corrupted_tail: false,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalFrameHeader {
    magic: u32,
    len: u32,
    crc: u32,
}

impl WalFrameHeader {
    const LEN: usize = std::mem::size_of::<Self>();

    fn new(len: u32, crc: u32) -> Self {
        Self {
            magic: WAL_MAGIC,
            len,
            crc,
        }
    }

    fn to_bytes(self) -> [u8; Self::LEN] {
        unsafe { std::mem::transmute(self) }
    }

    fn from_bytes(bytes: &[u8; Self::LEN]) -> Self {
        unsafe { std::mem::transmute(*bytes) }
    }
}

/// Atomic durable batch frame corresponding to a [`tagma_raft::Ready`] output.
pub(crate) struct WalBatch<E = Vec<Entry>> {
    pub(crate) batch_header: BatchHeader,
    pub(crate) entries: E,
}

impl<'a> WalBatch<&'a [Entry]> {
    pub(crate) fn from_ready(
        ready: &'a Ready,
        hard_state: HardState,
        meta: Option<SnapshotMeta>,
    ) -> Self {
        Self {
            batch_header: BatchHeader::from_ready(ready, hard_state, meta),
            entries: &ready.entries,
        }
    }
}
impl<E: AsRef<[Entry]>> WalBatch<E> {
    fn encode(&self, w: &mut WriteCursor) {
        self.batch_header.encode(w);
        for entry in self.entries.as_ref() {
            EntryRecord::new(entry).encode(w);
        }
    }
}
impl WalBatch<Vec<Entry>> {
    fn decode(payload: &[u8]) -> Option<Self> {
        let mut r = ReadCursor::new(payload);
        let batch_header = BatchHeader::decode(&mut r)?;

        let count = batch_header.entry_count as usize;
        if count > r.remaining() / EntryRecord::LEN {
            return None;
        }

        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let entry = EntryRecord::decode(&mut r)?;
            entries.push(entry);
        }

        (r.remaining() == 0).then_some(WalBatch {
            batch_header,
            entries,
        })
    }
}

pub(crate) struct BatchHeader {
    pub(crate) hard_state: HardState,
    pub(crate) truncate_from: Option<LogIndex>,
    pub(crate) meta: Option<SnapshotMeta>,
    pub(crate) entry_count: u64,
}

impl BatchHeader {
    const LEN: usize = 18 + 9 + 17 + 8;

    fn from_ready(ready: &Ready, hard_state: HardState, meta: Option<SnapshotMeta>) -> Self {
        Self {
            hard_state,
            truncate_from: ready.truncate_from,
            meta,
            entry_count: ready.entries.len() as u64,
        }
    }

    fn decode(r: &mut ReadCursor) -> Option<Self> {
        let term = r.read_u64()?;
        let has_voted_for = r.read_u8()?;
        let voted_for = match has_voted_for {
            0 => {
                r.advance(8);
                None
            }
            1 => Some(r.read_u64()?),
            _ => return None,
        };
        let hard_state = HardState { term, voted_for };

        let truncate_from: Option<LogIndex> = match r.read_u8()? {
            0 => {
                r.advance(8);
                None
            }
            1 => {
                let truncate_from = r.read_u64()?;
                Some(truncate_from)
            }
            _ => return None,
        };

        let meta = match r.read_u8()? {
            0 => {
                r.advance(16);
                None
            }
            1 => {
                let last_index = r.read_u64()?;
                let last_term = r.read_u64()?;
                Some(SnapshotMeta {
                    last_index,
                    last_term,
                })
            }
            _ => return None,
        };

        let entry_count = r.read_u64()?;

        Some(Self {
            hard_state,
            truncate_from,
            meta,
            entry_count,
        })
    }

    fn encode(&self, w: &mut WriteCursor) {
        w.write_u64(self.hard_state.term);
        match self.hard_state.voted_for {
            Some(voted_for) => {
                w.write_u8(1);
                w.write_u64(voted_for);
            }
            None => w.write_zeros(9),
        }

        match self.truncate_from {
            Some(truncate_from) => {
                w.write_u8(1);
                w.write_u64(truncate_from);
            }
            None => w.write_zeros(9),
        }

        match self.meta {
            Some(meta) => {
                w.write_u8(1);
                w.write_u64(meta.last_index);
                w.write_u64(meta.last_term);
            }
            None => w.write_zeros(17),
        }

        w.write_u64(self.entry_count);
    }
}

struct EntryRecord<'a> {
    entry: &'a Entry,
}

impl<'a> EntryRecord<'a> {
    const LEN: usize = 8 + 8 + 1 + 4;

    fn new(entry: &'a Entry) -> Self {
        Self { entry }
    }

    fn decode(r: &mut ReadCursor) -> Option<Entry> {
        let index = r.read_u64()?;
        let term = r.read_u64()?;
        let payload_type = r.read_u8()?;
        let len = r.read_u32()? as usize;

        let payload = match payload_type {
            PAYLOAD_NOOP => {
                if len != 0 {
                    return None;
                }
                Payload::Noop
            }
            PAYLOAD_COMMAND => {
                let data = r.read_bytes(len)?.to_vec();
                Payload::Command(data)
            }
            _ => return None,
        };

        Some(Entry {
            term,
            index,
            payload,
        })
    }

    fn encoded_len(&self) -> Result<usize, WalError> {
        let payload_len = match &self.entry.payload {
            Payload::Noop => 0,
            Payload::Command(payload) => {
                if payload.len() > SEGMENT_SIZE {
                    return Err(WalError::FrameToLarge {
                        len: payload.len(),
                        max: SEGMENT_SIZE,
                    });
                }
                payload.len()
            }
        };

        Ok(Self::LEN + payload_len)
    }

    fn encode(&self, w: &mut WriteCursor) {
        w.write_u64(self.entry.index);
        w.write_u64(self.entry.term);
        match &self.entry.payload {
            Payload::Noop => {
                w.write_u8(PAYLOAD_NOOP);
                w.write_u64(0);
            }
            Payload::Command(data) => {
                w.write_u8(PAYLOAD_COMMAND);
                w.write_u64(data.len() as u64);
                w.write_bytes(data);
            }
        }
    }
}

struct ReadCursor<'a> {
    buf: &'a [u8],
    offset: usize,
}

impl<'a> ReadCursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.offset
    }

    fn advance(&mut self, n: usize) {
        self.offset += n;
    }

    fn read_bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let bytes = self.buf.get(self.offset..self.offset + n);
        self.offset += n;
        bytes
    }

    fn read_u8(&mut self) -> Option<u8> {
        Some(self.read_bytes(1)?[0])
    }

    fn read_u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.read_bytes(4)?.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.read_bytes(8)?.try_into().unwrap()))
    }
}

struct WriteCursor<'a> {
    buf: &'a mut [u8],
    offset: usize,
}

impl<'a> WriteCursor<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.offset
    }

    fn write_bytes(&mut self, src: &[u8]) {
        let end = self.offset + src.len();
        self.buf[self.offset..end].copy_from_slice(src);
        self.offset = end;
    }

    fn write_zeros(&mut self, n: usize) {
        let end = self.offset + n;
        self.buf[self.offset..end].fill(0);
        self.offset = end;
    }

    fn write_u8(&mut self, v: u8) {
        self.buf[self.offset] = v;
        self.offset += 1;
    }

    fn write_u64(&mut self, v: u64) {
        self.write_bytes(&v.to_le_bytes());
    }
}
