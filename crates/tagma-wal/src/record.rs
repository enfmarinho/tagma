use serde::{Deserialize, Serialize};
use tagma_raft::{Entry, HardState};

/// One logical WAL record.
/// Stored on disk as: `len: u32 | crc32: u32 | postcard payload`.
#[derive(Serialize, Deserialize, Clone)]
enum Record {
    HardState(HardState),
    Entry(Entry),
}

/// Encode a [`Record`] an writes it to the `out` buffer.
pub(crate) fn encode(record: &Record, out: &mut Vec<u8>) {
    let payload = postcard::to_allocvec(record).expect("Record is always serializable");
    let len = u32::try_from(payload.len()).expect("Record is larger than 4GiB");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
}

/// Decode multiple [`Record`] form the 'buf' buffer. Stops at the first torn or corrupted frame.
pub(crate) fn decode(buf: &[u8]) -> Vec<Record> {
    let mut cursor = buf;
    let mut records: Vec<Record> = Vec::new();

    while !cursor.is_empty() {
        let Some((len_bytes, rest)) = cursor.split_first_chunk::<4>() else {
            break;
        };
        let len = u32::from_le_bytes(*len_bytes);
        cursor = rest;

        let Some((crc32_bytes, rest)) = cursor.split_first_chunk::<4>() else {
            break;
        };
        let crc32 = u32::from_le_bytes(*crc32_bytes);
        cursor = rest;

        let Some((payload, rest)) = cursor.split_at_checked(len as usize) else {
            break;
        };
        cursor = rest;

        if crc32fast::hash(payload) != crc32 {
            break;
        }

        let Ok(record) = postcard::from_bytes::<Record>(payload) else {
            break;
        };

        records.push(record);
    }

    records
}
