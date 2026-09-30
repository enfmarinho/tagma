use crate::types::{Entry, LogIndex, SnapshotMeta};

pub(crate) struct RaftLog {
    snapshot: Option<SnapshotMeta>,
    entries: Vec<Entry>,
}

impl RaftLog {
    pub(crate) fn restore(snapshot: Option<SnapshotMeta>, entries: Vec<Entry>) -> Self {
        Self { snapshot, entries }
    }

    pub(crate) fn snapshot(&self) -> Option<SnapshotMeta> {
        todo!()
    }

    pub(crate) fn entries(&self) -> &[Entry] {
        todo!()
    }

    pub(crate) fn last_index(&self) -> LogIndex {
        todo!()
    }

    pub(crate) fn last_term(&self) -> LogIndex {
        todo!()
    }

    pub(crate) fn entries_from(&self, index: LogIndex, max: usize) -> &[Entry] {
        todo!()
    }

    pub(crate) fn truncate_from(&mut self, index: LogIndex) {
        todo!()
    }

    pub(crate) fn append(&mut self, entries: &[Entry]) {
        todo!()
    }

    pub(crate) fn compact(&mut self, meta: SnapshotMeta) {
        todo!()
    }
}
