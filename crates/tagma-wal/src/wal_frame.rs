use std::io;

use serde::{Deserialize, Serialize};
use tagma_raft::{
    Entry, HardState, LogIndex, PAYLOAD_COMMAND, PAYLOAD_NOOP, Payload, Ready, SnapshotMeta,
};

use crate::storage::{self, WAL_MAGIC};

enum WalError {
    FrameToLarge { len: usize, max: usize },
}

struct DecodeOutcome {
    pub batches: Vec<WalBatch>,
    pub bytes_consumed: usize,
    pub has_corrupted_tail: bool,
}

pub(crate) struct WalFrame;

impl WalFrame {
    /// Encode a [`Batch`] and writes it to the `out` buffer.
    ///
    /// |--------------------------------------------------------------------------|
    /// | Magic (4B)    | Length (4B)    | CRC32 (4B)                              |
    /// |--------------------------------------------------------------------------|
    /// | Batch Header:                                                            |
    /// |     has_hard_state (1B) | term(8B) | has_voted_for (1B) | voted_for (u8) |
    /// |     has_truncate (1B)   | truncate_from (8B)                             |  
    /// |     has_snapshot (1B)   | snap_index (8B)     | snap_term (8B)           |
    /// |     entry_count(8b)                                                      |
    /// |--------------------------------------------------------------------      |
    /// | entries payload:                                                         |
    /// |     [entry 0: index (8b) | term (8b) | type (1b) | len (4b) | data]      |
    /// |     etc...                                                               |
    /// |--------------------------------------------------------------------------|
    pub(crate) fn encode<E: AsRef<[Entry]>>(
        batch: &WalBatch<E>,
        out: &mut Vec<u8>,
    ) -> Result<(), WalError> {
        todo!()
    }

    pub(crate) fn decode_all(buf: &[u8]) -> DecodeOutcome {
        todo!()
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalFrameHeader {
    pub magic: u32,
    pub len: u32,
    pub crc: u32,
}

impl WalFrameHeader {
    pub(crate) const LEN: usize = std::mem::size_of::<Self>();

    pub(crate) fn new(len: u32, crc: u32) -> Self {
        Self {
            magic: storage::WAL_MAGIC,
            len,
            crc,
        }
    }

    pub(crate) fn to_bytes(self) -> [u8; Self::LEN] {
        unsafe { std::mem::transmute(self) }
    }

    pub(crate) fn from_bytes(bytes: &[u8; Self::LEN]) -> Self {
        unsafe { std::mem::transmute(*bytes) }
    }
}

/// Atomic durable batch frame corresponding to a [`tagma_raft::Ready`] output.
struct WalBatch<E = Vec<Entry>> {
    batch_header: BatchHeader,
    entries_record: EntryRecord<E>,
}


pub(crate) struct BatchHeader {
    pub hard_state: Option<HardState>,
    pub truncate_from: Option<LogIndex>,
    pub meta: Option<SnapshotMeta>,
    pub entry_count: u64,
}

impl BatchHeader {
    const LEN: usize = todo!();

    fn new(ready: &Ready) -> Self {
        Self {
            hard_state: ready.hard_state,
            truncate_from: ready.truncate_from,
            meta: ready.snapshot.as_ref().map(|s| s.meta),
            entry_count: ready.entries.len() as u64,
        }
    }

    fn decode(c: ReadCursor) -> Option<Self> {
        todo!()
    }

    fn encode(&self, c: &mut WriteCursor) {
        todo!()
    }
}

pub(crate) struct EntryRecord<E = Vec<Entry>> {
    records: E,
}

impl EntryRecord {
    fn new(ready: &Ready) -> Self {
        todo!()
    }

    fn decode(c: ReadCursor) -> Option<Self> {
        todo!()
    }

    fn encode(&self, c: &mut WriteCursor) {
        todo!()
    }
}

// #[derive(Serialize)]
// pub(crate) struct Batch<'a> {
//     pub hard_state: Option<HardState>,
//     pub truncate_from: Option<LogIndex>,
//     pub snapshot: Option<SnapshotMeta>,
//     pub entries: &'a [Entry],
// }
//
// impl<'a> Batch<'a> {
//     pub(crate) fn new(ready: &'a Ready) -> Self {
//         Self {
//             hard_state: ready.hard_state,
//             truncate_from: ready.truncate_from,
//             snapshot: ready.snapshot.as_ref().map(|s| s.meta),
//             entries: &ready.entries,
//         }
//     }
// }
//
// /// Atomic durable batch frame corresponding to a [`tagma_raft::Ready`] output.
// #[derive(Deserialize)]
// pub(crate) struct OwnedBatch {
//     pub hard_state: Option<HardState>,
//     pub truncate_from: Option<LogIndex>,
//     pub snapshot_meta: Option<SnapshotMeta>,
//     pub entries: Vec<Entry>,
// }

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

    fn write_u32(&mut self, v: u32) {
        self.write_bytes(&v.to_le_bytes());
    }

    fn write_u64(&mut self, v: u64) {
        self.write_bytes(&v.to_le_bytes());
    }
}

// pub(crate) fn encode(batch: &Batch<'_>, out: &mut Vec<u8>) {
//     let mut c = WriteCursor::new(out);
//
//     c.write_u32(WAL_MAGIC);
//     c.write_zeros(8); // save space for later, length and crc32
//
//     match batch.hard_state {
//         Some(hs) => {
//             c.write_u8(1);
//             c.write_u64(hs.term);
//             match hs.voted_for {
//                 Some(voted_for) => {
//                     c.write_u8(1);
//                     c.write_u64(voted_for);
//                 }
//                 None => c.write_zeros(9),
//             }
//         }
//         None => c.write_zeros(18),
//     }
//
//     match batch.truncate_from {
//         Some(truncate_from) => {
//             c.write_u8(1);
//             c.write_u64(truncate_from);
//         }
//         None => c.write_zeros(9),
//     }
//
//     match batch.snapshot {
//         Some(meta) => {
//             c.write_u8(1);
//             c.write_u64(meta.last_index);
//             c.write_u64(meta.last_term);
//         }
//         None => c.write_zeros(17),
//     }
//
//     // |     entry_count(8b)                                                      |
//     // |--------------------------------------------------------------------      |
//     // | entries payload:                                                         |
//     // |     [entry 0: index (8b) | term (8b) | type (1b) | len (4b) | data]      |
//     // |     etc...                                                               |
//     // TODO: write crc and len
//
//     let entries = Vec::new();
//     for entry in batch.entries {}
//
//     todo!()
// }
//
// fn encode_entry(entry: &Entry) -> Option<Vec<u8>> {
//     todo!()
// }
//
// /// Decode all valid [`Batch`] frames form 'buf'. Stops at the first torn or corrupted frame.
// /// Returns `(batches, bytes_consumed, has_corrupted_tail)`
// pub(crate) fn decode(buf: &[u8]) -> io::Result<(Vec<OwnedBatch>, usize, bool)> {
//     let mut batches: Vec<OwnedBatch> = Vec::new();
//     let mut offset = 0;
//
//     while offset < buf.len() {
//         let rem = &buf[offset..];
//
//         // torn frame header
//         if rem.len() < WalFrameHeader::LEN {
//             return Ok((batches, offset, true));
//         }
//
//         let frame_header = WalFrameHeader::from_bytes(
//             rem[..WalFrameHeader::LEN]
//                 .try_into()
//                 .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, ""))?,
//         );
//
//         if frame_header.magic == 0 {
//             let is_tail_clean = rem.iter().all(|&v| v == 0);
//             return Ok((batches, offset, !is_tail_clean));
//         }
//
//         // corrupted magic
//         if frame_header.magic != WAL_MAGIC {
//             return Ok((batches, offset, true));
//         }
//
//         // torn payload
//         let frame_len = WalFrameHeader::LEN + frame_header.len as usize;
//         if frame_len > rem.len() {
//             return Ok((batches, offset, true));
//         }
//
//         // corrupted payload
//         let payload = &rem[WalFrameHeader::LEN..frame_len];
//         if frame_header.crc != crc32fast::hash(payload) {
//             return Ok((batches, offset, true));
//         }
//
//         // batch decode failed
//         let Some(batch) = decode_batch(payload) else {
//             return Ok((batches, offset, true));
//         };
//
//         offset += frame_len;
//         batches.push(batch);
//     }
//
//     Ok((batches, offset, false))
// }
//
// fn decode_batch(buf: &[u8]) -> Option<OwnedBatch> {
//     let mut c = ReadCursor::new(buf);
//
//     let hard_state = match c.read_u8()? {
//         0 => {
//             c.advance(24);
//             None
//         }
//         1 => {
//             let term = c.read_u64()?;
//             let voted_for = c.read_u64();
//             Some(HardState { term, voted_for })
//         }
//         _ => return None,
//     };
//
//     let truncate_from: Option<LogIndex> = match c.read_u8()? {
//         0 => {
//             c.advance(8);
//             None
//         }
//         1 => {
//             let truncate_from = c.read_u64()?;
//             Some(truncate_from)
//         }
//         _ => return None,
//     };
//
//     let snapshot_meta = match c.read_u8()? {
//         0 => {
//             c.advance(16);
//             None
//         }
//         1 => {
//             let last_index = c.read_u64()?;
//             let last_term = c.read_u64()?;
//             Some(SnapshotMeta {
//                 last_index,
//                 last_term,
//             })
//         }
//         _ => return None,
//     };
//
//     let entry_count = c.read_u64()?;
//     let mut entries = Vec::new();
//     for _ in 0..entry_count {
//         let index = c.read_u64()?;
//         let term = c.read_u64()?;
//         let payload_type = c.read_u8()?;
//         let len = c.read_u32()?;
//         let data = c.read_bytes(len as usize)?.to_vec();
//
//         let payload = match payload_type {
//             PAYLOAD_NOOP => Payload::Noop,
//             PAYLOAD_COMMAND => Payload::Command(data),
//             _ => return None,
//         };
//
//         entries.push(Entry {
//             term,
//             index,
//             payload,
//         });
//     }
//
//     Some(OwnedBatch {
//         hard_state,
//         truncate_from,
//         snapshot_meta,
//         entries,
//     })
// }
