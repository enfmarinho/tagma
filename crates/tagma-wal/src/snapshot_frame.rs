#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapshotFrameHeader {
    pub len: usize,
    pub crc: u32,
    pub padding: u32,
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

    pub(crate) fn from_bytes(bytes: &[u8; Self::LEN]) -> Self {
        unsafe { std::mem::transmute(*bytes) }
    }
}
