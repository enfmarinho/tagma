use std::io;

use serde::{Deserialize, Serialize};
use tagma_raft::{Entry, HardState, LogIndex, Ready, SnapshotMeta, Term};

use crate::storage;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalFrameHeader {
    magic: u32,
    len: u32,
    crc: u32,
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

    pub(crate) fn from_bytes(bytes: [u8; Self::LEN]) -> Self {
        unsafe { std::mem::transmute(bytes) }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapshotFrameHeader {
    len: usize,
    crc: u32,
    padding: u32,
}

impl SnapshotFrameHeader {
    pub(crate) const LEN: usize = std::mem::size_of::<Self>();

    pub(crate) fn new(len: usize, crc: u32) -> Self {
        Self {
            len,
            crc,
            padding: 0,
        }
    }

    pub(crate) fn to_bytes(self) -> [u8; Self::LEN] {
        unsafe { std::mem::transmute(self) }
    }

    pub(crate) fn from_bytes(bytes: [u8; Self::LEN]) -> Self {
        unsafe { std::mem::transmute(bytes) }
    }
}

/// Atomic durable batch frame corresponding to a [`tagma_raft::Ready`] output.
#[derive(Serialize)]
pub(crate) struct Batch<'a> {
    pub hard_state: Option<HardState>,
    pub truncate_from: Option<LogIndex>,
    pub snapshot: Option<SnapshotMeta>,
    pub entries: &'a [Entry],
}

impl<'a> Batch<'a> {
    pub(crate) fn new(ready: &'a Ready) -> Self {
        Self {
            hard_state: ready.hard_state,
            truncate_from: ready.truncate_from,
            snapshot: ready.snapshot.as_ref().map(|s| s.meta.clone()),
            entries: &ready.entries,
        }
    }
}

/// Atomic durable batch frame corresponding to a [`tagma_raft::Ready`] output.
#[derive(Deserialize)]
pub(crate) struct OwnedBatch {
    pub hard_state: Option<HardState>,
    pub truncate_from: Option<LogIndex>,
    pub snapshot_meta: Option<SnapshotMeta>,
    pub entries: Vec<Entry>,
}

/// Encode a [`Batch`] and writes it to the `out` buffer.
pub(crate) fn encode(batch: &Batch<'_>, out: &mut Vec<u8>) {
    todo!()
}

/// Decode all valid [`Batch`] frames form 'buf'. Stops at the first torn or corrupted frame.
/// Returns `(batches, bytes_consumed, has_corrupted_tail)`
pub(crate) fn decode(buf: &[u8]) -> io::Result<(Vec<OwnedBatch>, usize, bool)> {
    todo!()
}
