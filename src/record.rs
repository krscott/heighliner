//! The replicated record store.
//!
//! A record is the set of all its retained versions, keyed by [`Hlc`]. The
//! latest version wins (last-write-wins); a tombstone is a version that
//! deletes the record. Merging two stores is the union of their versions,
//! which makes merge commutative, associative and idempotent by construction.
//!
//! Versions are opaque JSON fields plus a schema version, so record kinds and
//! fields this version of heighliner doesn't know are kept and passed on
//! unchanged. The envelope around them (the [`Hlc`] and the [`Content`]
//! states) is part of the protocol between Nodes, so changing it needs a new
//! major protocol version.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::hlc::{Hlc, Timestamp};

/// Identifies a record: its kind (such as `item`) and its ID within the kind.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecordKey {
    pub kind: String,
    pub id: String,
}

/// The content of one version of a record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum Content {
    Live {
        schema: u32,
        fields: Map<String, Value>,
    },
    Deleted,
}

impl Content {
    /// Orders two contents written with the same [`Hlc`], which only a bug or
    /// a corrupt Node can produce, so that every Node keeps the same one.
    fn tie_break_key(&self) -> String {
        // serde_json's Map is sorted by key, so this serialization is canonical.
        serde_json::to_string(self).expect("JSON values always serialize")
    }
}

/// All retained versions of one record. Never empty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    versions: BTreeMap<Hlc, Content>,
}

impl Record {
    fn new(hlc: Hlc, content: Content) -> Self {
        Self {
            versions: BTreeMap::from([(hlc, content)]),
        }
    }

    /// The winning version.
    pub fn latest(&self) -> (&Hlc, &Content) {
        self.versions
            .last_key_value()
            .expect("records are never empty")
    }

    /// Every retained version, oldest first.
    pub fn versions(&self) -> impl DoubleEndedIterator<Item = (&Hlc, &Content)> {
        self.versions.iter()
    }

    fn insert(&mut self, hlc: Hlc, content: Content) {
        match self.versions.entry(hlc) {
            Entry::Vacant(entry) => {
                entry.insert(content);
            }
            Entry::Occupied(mut entry) => {
                if content.tie_break_key() > entry.get().tie_break_key() {
                    entry.insert(content);
                }
            }
        }
    }

    /// Drops expired versions, and returns whether anything is left.
    ///
    /// A superseded version expires one window after the version that
    /// superseded it was written, by that version's [`Hlc`], not when this
    /// Node learned of it; a version that arrives late can expire what it
    /// supersedes at once. A winning tombstone expires on its own, taking the
    /// record with it. The latest live version and the latest tombstone are
    /// kept as long as the record, so a deleted record's last content, and
    /// the last deletion of a record that came back, stay known. With
    /// `keep`, the record stays even if its winning tombstone has expired.
    fn prune(&mut self, expired: impl Fn(&Hlc) -> bool, keep: bool) -> bool {
        let (latest_hlc, latest) = self.latest();
        if !keep && matches!(latest, Content::Deleted) && expired(latest_hlc) {
            return false;
        }
        let kept: Vec<Hlc> = self.latest_of_each_state().into_iter().cloned().collect();
        let hlcs: Vec<Hlc> = self.versions.keys().cloned().collect();
        for pair in hlcs.windows(2) {
            if expired(&pair[1]) && !kept.contains(&pair[0]) {
                self.versions.remove(&pair[0]);
            }
        }
        true
    }

    /// The latest live version and the latest tombstone, where they exist.
    fn latest_of_each_state(&self) -> Vec<&Hlc> {
        let latest = |deleted: bool| {
            self.versions
                .iter()
                .rev()
                .find(|(_, content)| matches!(content, Content::Deleted) == deleted)
                .map(|(hlc, _)| hlc)
        };
        latest(false).into_iter().chain(latest(true)).collect()
    }
}

/// A set of records.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<WireRecord>", into = "Vec<WireRecord>")]
pub struct Store {
    records: BTreeMap<RecordKey, Record>,
}

impl Store {
    pub fn get(&self, key: &RecordKey) -> Option<&Record> {
        self.records.get(key)
    }

    pub fn records(&self) -> impl Iterator<Item = (&RecordKey, &Record)> {
        self.records.iter()
    }

    /// Adds one version. If it is the latest, it becomes the record's value.
    pub fn insert(&mut self, key: RecordKey, hlc: Hlc, content: Content) {
        match self.records.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(Record::new(hlc, content));
            }
            Entry::Occupied(mut entry) => entry.get_mut().insert(hlc, content),
        }
    }

    /// Adds every version from `other` that `accept` accepts, and returns how
    /// many it didn't.
    pub fn merge(&mut self, other: &Store, accept: impl Fn(&Hlc) -> bool) -> usize {
        let mut rejected = 0;
        for (key, record) in &other.records {
            for (hlc, content) in &record.versions {
                if accept(hlc) {
                    self.insert(key.clone(), hlc.clone(), content.clone());
                } else {
                    rejected += 1;
                }
            }
        }
        rejected
    }

    /// The latest [`Hlc`] of any retained version.
    pub fn max_hlc(&self) -> Option<&Hlc> {
        self.records
            .values()
            .flat_map(|record| record.versions.keys())
            .max()
    }

    /// Drops versions and tombstones that expired more than `window` ago.
    /// Records for which `keep` returns true stay, with their latest version
    /// and latest live version, even if their winning tombstone expired.
    pub fn prune(&mut self, now: Timestamp, window: Duration, keep: impl Fn(&RecordKey) -> bool) {
        let expired = |hlc: &Hlc| hlc.wall.saturating_add(window) < now;
        self.records
            .retain(|key, record| record.prune(expired, keep(key)));
    }
}

/// How a record is serialized, since JSON map keys must be strings.
#[derive(Clone, Serialize, Deserialize)]
struct WireRecord {
    kind: String,
    id: String,
    versions: Vec<WireVersion>,
}

#[derive(Clone, Serialize, Deserialize)]
struct WireVersion {
    hlc: Hlc,
    content: Content,
}

/// A record as read: each version is decoded on its own, so one bad
/// version doesn't cost the rest.
#[derive(Deserialize)]
struct LenientRecord {
    kind: String,
    id: String,
    versions: Vec<Value>,
}

/// Refuses a record without versions, which is never written.
impl TryFrom<Vec<WireRecord>> for Store {
    type Error = String;

    fn try_from(records: Vec<WireRecord>) -> Result<Self, Self::Error> {
        let mut store = Self::default();
        for record in records {
            if record.versions.is_empty() {
                return Err(format!(
                    "record {}/{} has no versions",
                    record.kind, record.id
                ));
            }
            let key = RecordKey {
                kind: record.kind,
                id: record.id,
            };
            for version in record.versions {
                store.insert(key.clone(), version.hlc, version.content);
            }
        }
        Ok(store)
    }
}

impl Store {
    /// Decodes a serialized store one record and version at a time, and
    /// returns how many it skipped because they couldn't be read. Used for
    /// stores from other Nodes, so that one bad record doesn't cost the
    /// rest; a Node's own state is decoded strictly.
    pub fn decode_lenient(records: Vec<Value>) -> (Self, usize) {
        let mut store = Self::default();
        let mut skipped = 0;
        for record in records {
            let Ok(record) = serde_json::from_value::<LenientRecord>(record) else {
                skipped += 1;
                continue;
            };
            let key = RecordKey {
                kind: record.kind,
                id: record.id,
            };
            for version in record.versions {
                match serde_json::from_value::<WireVersion>(version) {
                    Ok(version) => store.insert(key.clone(), version.hlc, version.content),
                    Err(_) => skipped += 1,
                }
            }
        }
        (store, skipped)
    }
}

impl From<Store> for Vec<WireRecord> {
    fn from(store: Store) -> Self {
        store
            .records
            .into_iter()
            .map(|(key, record)| WireRecord {
                kind: key.kind,
                id: key.id,
                versions: record
                    .versions
                    .into_iter()
                    .map(|(hlc, content)| WireVersion { hlc, content })
                    .collect(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::model::NodeId;

    const DAY: Duration = Duration::from_secs(24 * 60 * 60);

    fn key(kind: &str, id: &str) -> RecordKey {
        RecordKey {
            kind: kind.to_owned(),
            id: id.to_owned(),
        }
    }

    fn hlc(wall: u64, counter: u32, node: &str) -> Hlc {
        Hlc {
            wall: Timestamp::from_millis(wall),
            counter,
            node: NodeId::new(node).unwrap(),
        }
    }

    fn live(value: i64) -> Content {
        Content::Live {
            schema: 1,
            fields: Map::from_iter([("value".to_owned(), json!(value))]),
        }
    }

    fn days(n: u64) -> u64 {
        n * 24 * 60 * 60 * 1000
    }

    fn prune(store: &mut Store, now: u64) {
        store.prune(Timestamp::from_millis(now), DAY * 30, |_| false);
    }

    #[test]
    fn latest_version_wins() {
        let mut store = Store::default();
        store.insert(key("k", "1"), hlc(2, 0, "a"), live(2));
        store.insert(key("k", "1"), hlc(1, 0, "b"), live(1));
        assert_eq!(store.get(&key("k", "1")).unwrap().latest().1, &live(2));
    }

    #[test]
    fn prune_keeps_superseded_versions_for_the_window() {
        let mut store = Store::default();
        store.insert(key("k", "1"), hlc(0, 0, "a"), live(0));
        store.insert(key("k", "1"), hlc(days(10), 0, "a"), live(1));
        store.insert(key("k", "1"), hlc(days(20), 0, "a"), live(2));

        // The first version was superseded on day 10, so it expires on day 40.
        prune(&mut store, days(40));
        assert_eq!(store.get(&key("k", "1")).unwrap().versions().count(), 3);
        prune(&mut store, days(40) + 1);
        assert_eq!(store.get(&key("k", "1")).unwrap().versions().count(), 2);

        // The winner never expires.
        prune(&mut store, days(1000));
        let record = store.get(&key("k", "1")).unwrap();
        assert_eq!(record.versions().count(), 1);
        assert_eq!(record.latest().1, &live(2));
    }

    #[test]
    fn prune_drops_expired_tombstones() {
        let mut store = Store::default();
        store.insert(key("k", "1"), hlc(0, 0, "a"), live(0));
        store.insert(key("k", "1"), hlc(days(1), 0, "a"), Content::Deleted);
        prune(&mut store, days(31));
        assert_eq!(store.get(&key("k", "1")).unwrap().versions().count(), 2);
        prune(&mut store, days(31) + 1);
        assert!(store.get(&key("k", "1")).is_none());
    }

    #[test]
    fn prune_keeps_a_tombstones_last_live_version() {
        let mut store = Store::default();
        store.insert(key("k", "1"), hlc(0, 0, "a"), live(0));
        store.insert(key("k", "1"), hlc(days(1), 0, "a"), Content::Deleted);
        store.insert(key("k", "1"), hlc(days(50), 0, "a"), Content::Deleted);
        prune(&mut store, days(51));
        let versions: Vec<_> = store.get(&key("k", "1")).unwrap().versions().collect();
        assert_eq!(versions.len(), 3);
        prune(&mut store, days(81) + 1);
        assert!(store.get(&key("k", "1")).is_none());
    }

    #[test]
    fn prune_keeps_kept_records_but_prunes_their_history() {
        let mut store = Store::default();
        for id in ["1", "2"] {
            store.insert(key("k", id), hlc(0, 0, "a"), live(0));
            store.insert(key("k", id), hlc(1, 0, "a"), live(1));
            store.insert(key("k", id), hlc(2, 0, "a"), Content::Deleted);
        }
        store.prune(Timestamp::from_millis(days(100)), DAY * 30, |k| k.id == "1");
        let kept: Vec<_> = store.get(&key("k", "1")).unwrap().versions().collect();
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].1, &live(1));
        assert!(store.get(&key("k", "2")).is_none());
    }

    #[test]
    fn unreadable_versions_are_skipped() {
        let json = serde_json::json!([
            {"kind": "k", "id": "1", "versions": [
                {"hlc": {"wall": 1, "counter": 0, "node": "a"}, "content": {"state": "deleted"}},
                {"hlc": {"wall": 2, "counter": 0, "node": "a b"}, "content": {"state": "deleted"}},
            ]},
            {"kind": "k"},
            {"kind": "k", "id": "2", "versions": [
                {"hlc": {"wall": 1, "counter": 0, "node": "a"}, "content": {"state": "archived"}},
            ]},
        ]);
        let serde_json::Value::Array(records) = json.clone() else {
            unreachable!()
        };
        let (store, skipped) = Store::decode_lenient(records);
        assert_eq!(skipped, 3);
        assert_eq!(store.records().count(), 1);
        // Strict decoding refuses the whole store, including a record
        // without versions.
        assert!(serde_json::from_value::<Store>(json).is_err());
        let empty = serde_json::json!([{"kind": "k", "id": "1", "versions": []}]);
        assert!(serde_json::from_value::<Store>(empty).is_err());
        assert_eq!(store.get(&key("k", "1")).unwrap().versions().count(), 1);
    }

    #[test]
    fn unknown_kinds_survive_serialization() {
        let mut store = Store::default();
        store.insert(key("future-kind", "x"), hlc(1, 0, "a"), live(7));
        let json = serde_json::to_string(&store).unwrap();
        let mut copy = Store::default();
        copy.merge(&serde_json::from_str(&json).unwrap(), |_| true);
        assert_eq!(copy, store);
    }

    fn arb_content() -> impl Strategy<Value = Content> {
        prop_oneof![
            Just(Content::Deleted),
            (0..3u32, 0..3i64).prop_map(|(schema, value)| Content::Live {
                schema,
                fields: Map::from_iter([("value".to_owned(), json!(value))]),
            }),
        ]
    }

    fn arb_store() -> impl Strategy<Value = Store> {
        // Small pools so that keys and HLCs collide, including the same HLC
        // with different content.
        let version = (
            prop::sample::select(vec!["item", "replica", "unknown"]),
            0..3u8,
            0..4u64,
            0..2u32,
            prop::sample::select(vec!["a", "b"]),
            arb_content(),
        );
        prop::collection::vec(version, 0..12).prop_map(|versions| {
            let mut store = Store::default();
            for (kind, id, wall, counter, node, content) in versions {
                store.insert(
                    key(kind, &id.to_string()),
                    hlc(wall, counter, node),
                    content,
                );
            }
            store
        })
    }

    fn merged(a: &Store, b: &Store) -> Store {
        let mut out = a.clone();
        out.merge(b, |_| true);
        out
    }

    proptest! {
        #[test]
        fn merge_is_commutative(a in arb_store(), b in arb_store()) {
            prop_assert_eq!(merged(&a, &b), merged(&b, &a));
        }

        #[test]
        fn merge_is_associative(a in arb_store(), b in arb_store(), c in arb_store()) {
            prop_assert_eq!(merged(&merged(&a, &b), &c), merged(&a, &merged(&b, &c)));
        }

        #[test]
        fn merge_is_idempotent(a in arb_store(), b in arb_store()) {
            prop_assert_eq!(merged(&a, &a), a.clone());
            let ab = merged(&a, &b);
            prop_assert_eq!(merged(&ab, &b), ab);
        }

        #[test]
        fn serialization_round_trips(a in arb_store()) {
            let json = serde_json::to_string(&a).unwrap();
            prop_assert_eq!(serde_json::from_str::<Store>(&json).unwrap(), a);
        }
    }
}
