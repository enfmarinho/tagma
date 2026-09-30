use crate::types::{Entry, LogIndex, SnapshotMeta, Term};

pub(crate) struct RaftLog {
    snapshot: Option<SnapshotMeta>,
    entries: Vec<Entry>,
}

impl RaftLog {
    pub(crate) fn restore(snapshot: Option<SnapshotMeta>, entries: Vec<Entry>) -> Self {
        Self { snapshot, entries }
    }

    pub(crate) fn snapshot(&self) -> Option<SnapshotMeta> {
        self.snapshot
    }

    pub(crate) fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub(crate) fn last_index(&self) -> LogIndex {
        if let Some(last_entry) = self.entries().last() {
            last_entry.index
        } else if let Some(meta) = self.snapshot() {
            meta.last_index
        } else {
            0 // sentinel
        }
    }

    pub(crate) fn last_term(&self) -> Term {
        if let Some(last_entry) = self.entries().last() {
            last_entry.term
        } else if let Some(meta) = self.snapshot() {
            meta.last_term
        } else {
            0 // sentinel
        }
    }

    /// `None` if `index` is past the end or inside the compacted prefix
    pub(crate) fn term_at(&self, index: LogIndex) -> Option<Term> {
        let offset = self.offset();
        if index == offset {
            return Some(self.snapshot.map_or(0, |s| s.last_term));
        }
        if index < offset {
            return None;
        }
        self.entries
            .get((index - offset - 1) as usize)
            .map(|e| e.term)
    }

    fn offset(&self) -> LogIndex {
        self.snapshot.map_or(0, |s| s.last_index)
    }

    pub(crate) fn entries_from(&self, index: LogIndex, max: usize) -> &[Entry] {
        debug_assert!(
            index > self.offset(),
            "entries before {index} are compacted"
        );
        let start = ((index - self.offset() - 1) as usize).min(self.entries.len());
        let end = start.saturating_add(max).min(self.entries.len());
        &self.entries[start..end]
    }

    pub(crate) fn truncate_from(&mut self, index: LogIndex) {
        debug_assert!(
            index > self.offset(),
            "cannot truncate commited entries at {index}"
        );
        self.entries.truncate((index - self.offset() - 1) as usize);
    }

    pub(crate) fn append(&mut self, entries: &[Entry]) {
        let Some(first) = entries.first() else {
            return;
        };
        debug_assert_eq!(
            first.index,
            self.last_index() + 1,
            "append must continue the log"
        );
        debug_assert!(
            entries.windows(2).all(|w| w[1].index == w[0].index + 1),
            "appended entries must have consecutive indices"
        );
        self.entries.extend_from_slice(entries);
    }

    pub(crate) fn compact(&mut self, meta: SnapshotMeta) {
        let offset = self.offset();
        if meta.last_index <= offset {
            return;
        }
        debug_assert_eq!(self.term_at(meta.last_index), Some(meta.last_term));
        self.entries.drain(..(meta.last_index - offset) as usize);
        self.snapshot = Some(meta);
    }
}
