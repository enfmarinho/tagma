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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Payload;

    fn entry(index: LogIndex, term: Term) -> Entry {
        Entry {
            term,
            index,
            payload: Payload::Noop,
        }
    }

    fn indices(entries: &[Entry]) -> Vec<LogIndex> {
        entries.iter().map(|e| e.index).collect()
    }

    #[test]
    fn empty_log_sentinel() {
        let log = RaftLog::restore(None, vec![]);

        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.term_at(0), Some(0));
        assert_eq!(log.term_at(1), None);

        assert!(log.entries_from(1, 10).is_empty());
    }

    #[test]
    fn append_extends_log() {
        let mut log = RaftLog::restore(None, vec![]);

        log.append(&[]);
        assert!(log.entries().is_empty());

        log.append(&[entry(1, 1), entry(2, 1)]);
        log.append(&[entry(3, 2)]);
        assert_eq!(indices(&log.entries), vec![1, 2, 3]); // check if indices match
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "append must continue the log")]
    fn append_overlapping_panics() {
        let mut log = RaftLog::restore(None, vec![]);
        log.append(&[entry(1, 1), entry(2, 1)]);
        log.append(&[entry(2, 1)]);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "appended entries must have consecutive indices")]
    fn append_non_consecutive_panics() {
        let mut log = RaftLog::restore(None, vec![]);
        log.append(&[entry(1, 1), entry(3, 1)]);
    }

    #[test]
    fn term_at_indices() {
        let mut log = RaftLog::restore(None, vec![]);
        let terms = vec![1, 1, 1, 2, 2, 3];
        for (i, &t) in (1..).zip(&terms) {
            log.append(&[entry(i, t)]);
        }

        for (i, &t) in (1..).zip(&terms) {
            assert_eq!(log.term_at(i), Some(t));
        }
        assert_eq!(log.term_at(7), None);
        assert_eq!(log.term_at(LogIndex::MAX), None);

        log.compact(SnapshotMeta {
            last_index: 4,
            last_term: 2,
        });
        assert_eq!(log.term_at(6), Some(3));
        assert_eq!(log.term_at(7), None);
    }

    #[test]
    fn entries_from_bounds() {
        let mut log = RaftLog::restore(None, vec![]);
        for (i, t) in (1..).zip(1..5) {
            log.append(&[entry(i, t)]);
        }

        assert_eq!(
            indices(log.entries_from(1, usize::MAX)),
            (1..5).collect::<Vec<_>>()
        );
        assert_eq!(
            indices(log.entries_from(3, usize::MAX)),
            (3..5).collect::<Vec<_>>()
        );
        assert_eq!(indices(log.entries_from(3, 1)), (3..4).collect::<Vec<_>>());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "entries before 2 are compacted")]
    fn entries_from_compacted_index_panics() {
        let mut log = RaftLog::restore(None, vec![entry(1, 1), entry(2, 2)]);
        log.compact(SnapshotMeta {
            last_index: 2,
            last_term: 2,
        });
        log.entries_from(2, 10);
    }

    #[test]
    fn truncate_from() {
        let mut log = RaftLog::restore(
            None,
            vec![entry(1, 1), entry(2, 2), entry(3, 2), entry(4, 2)],
        );

        log.truncate_from(9);
        assert_eq!(indices(log.entries()), vec![1, 2, 3, 4]);

        log.truncate_from(3);
        assert_eq!(indices(log.entries()), vec![1, 2]);
        assert_eq!(log.last_term(), 2);

        log.truncate_from(1);
        assert!(log.entries().is_empty());
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "left == right")]
    fn compact_with_wrong_term_panics() {
        let mut log = RaftLog::restore(None, vec![entry(1, 1), entry(2, 1), entry(3, 1)]);
        log.compact(SnapshotMeta {
            last_index: 3,
            last_term: 2,
        });
    }
}
