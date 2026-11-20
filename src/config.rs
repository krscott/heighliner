//! The Fleet's configuration: Items and Replicas stored as records.
//!
//! [`Config`] is derived from a [`Store`], so every merge result has a valid
//! configuration. An Item is tracked while it has a live Replica; its own
//! record only holds its settings. Merges can produce situations local edits
//! never would, such as overlapping Replicas or a resurrected Item, and the
//! configuration reports those as [`Problem`]s instead of rejecting them.
//!
//! [`ConfigDb`] is one Node's copy of the store. It refuses local edits that
//! would add a Problem, and versions edits with the Node's clock.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::hlc::{Clock, Timestamp};
use crate::model::{AbsPath, Item, ItemId, NodeId, Replica, Role};
use crate::record::{Content, Record, RecordKey, Store};

pub const ITEM_KIND: &str = "item";
pub const REPLICA_KIND: &str = "replica";

/// The record schema version this version of heighliner reads and writes.
///
/// Adding fields doesn't change it, since unknown fields are preserved. A
/// change that older versions would misread increments it, and records with
/// a newer schema are kept but not used.
pub const SCHEMA: u32 = 1;

/// How far ahead of the local clock a merged version may be. Later versions
/// are left out until the local clock catches up, so one Node's wrong clock
/// can't drag every Node's clock forward.
pub const MAX_CLOCK_DRIFT: Duration = Duration::from_secs(5 * 60);

fn item_key(item: ItemId) -> RecordKey {
    RecordKey {
        kind: ITEM_KIND.to_owned(),
        id: item.to_string(),
    }
}

fn replica_key(item: ItemId, node: &NodeId) -> RecordKey {
    RecordKey {
        kind: REPLICA_KIND.to_owned(),
        id: format!("{item}/{node}"),
    }
}

/// Parses an Item record ID, which must be the ID's canonical form so that
/// every Item has exactly one record.
fn parse_item_id(id: &str) -> Result<ItemId, String> {
    let item: ItemId = id.parse().map_err(|e| format!("{e}"))?;
    if item.to_string() != id {
        return Err(format!("item ID {id:?} is not in canonical form"));
    }
    Ok(item)
}

fn parse_replica_id(id: &str) -> Result<(ItemId, NodeId), String> {
    let (item, node) = id
        .split_once('/')
        .ok_or_else(|| format!("invalid replica ID {id:?}"))?;
    Ok((
        parse_item_id(item)?,
        NodeId::new(node).map_err(|e| format!("{e}"))?,
    ))
}

fn parse_fields<T: DeserializeOwned>(content: &Content) -> Option<Result<T, String>> {
    match content {
        Content::Live { schema, .. } if *schema > SCHEMA => Some(Err(format!(
            "schema {schema} is newer than this version's {SCHEMA}"
        ))),
        Content::Live { fields, .. } => {
            Some(serde_json::from_value(Value::Object(fields.clone())).map_err(|e| e.to_string()))
        }
        Content::Deleted => None,
    }
}

fn is_live(record: Option<&Record>) -> bool {
    record.is_some_and(|record| matches!(record.latest().1, Content::Live { .. }))
}

/// A tracked Item with its Replicas, keyed by Node. There is at least one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ItemEntry {
    pub item: Item,
    pub replicas: BTreeMap<NodeId, Replica>,
}

impl ItemEntry {
    /// The Role of the Replica on `node`, if there is one.
    pub fn role(&self, node: &NodeId) -> Option<Role> {
        let replica = self.replicas.get(node)?;
        Some(self.item.direction.role(node, replica.mirror))
    }

    /// Whether the Replica on `node` is paused, directly or through its Item.
    pub fn is_paused(&self, node: &NodeId) -> Option<bool> {
        let replica = self.replicas.get(node)?;
        Some(self.item.paused || replica.paused)
    }

    fn has_sender(&self) -> bool {
        self.replicas
            .keys()
            .any(|node| self.role(node) != Some(Role::Mirror))
    }
}

/// Something in a configuration that needs the user's attention.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Problem {
    /// A record of a known kind that this version can't read, for example
    /// one with a newer schema. It is kept but not used.
    Unreadable { key: RecordKey, error: String },
    /// An untracked Item that is back because one of its Replicas was added
    /// or changed without seeing the untrack.
    Resurrected { item: ItemId },
    /// A Replica whose Item is unknown or unreadable. It is not synced.
    Orphaned { item: ItemId, node: NodeId },
    /// Items that share a name.
    DuplicateName { name: String, items: Vec<ItemId> },
    /// An Item none of whose Replicas sends: a one-way Item without a Replica
    /// at its source, or a two-way Item whose Replicas are all mirrors.
    NoSender { item: ItemId },
    /// Two Replicas on one Node whose paths overlap. Neither may be synced.
    Overlap { node: NodeId, items: [ItemId; 2] },
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable { key, error } => {
                write!(f, "record {}/{} can't be read: {error}", key.kind, key.id)
            }
            Self::Resurrected { item } => write!(
                f,
                "item {item} was untracked, but a replica changed without seeing that brought it back"
            ),
            Self::Orphaned { item, node } => {
                write!(f, "the replica on {node} belongs to unknown item {item}")
            }
            Self::DuplicateName { name, items } => {
                write!(f, "{} items are named {name:?}", items.len())
            }
            Self::NoSender { item } => write!(f, "no replica of item {item} sends changes"),
            Self::Overlap {
                node,
                items: [a, b],
            } => write!(f, "the replicas of items {a} and {b} on {node} overlap"),
        }
    }
}

/// The configuration derived from a store.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Config {
    pub items: BTreeMap<ItemId, ItemEntry>,
    pub problems: Vec<Problem>,
}

/// The readable settings in an Item record.
enum ItemSettings {
    Live(Item),
    /// The record is a tombstone; this is its last live content.
    Untracked(Item),
}

impl Config {
    pub fn new(store: &Store) -> Self {
        let mut config = Self::default();
        let mut settings: BTreeMap<ItemId, ItemSettings> = BTreeMap::new();
        let mut replicas: BTreeMap<ItemId, BTreeMap<NodeId, Replica>> = BTreeMap::new();

        for (key, record) in store.records() {
            let unreadable = |error: String| Problem::Unreadable {
                key: key.clone(),
                error,
            };
            match key.kind.as_str() {
                ITEM_KIND => {
                    let id = match parse_item_id(&key.id) {
                        Ok(id) => id,
                        Err(e) => {
                            config.problems.push(unreadable(e));
                            continue;
                        }
                    };
                    match parse_fields::<Item>(record.latest().1) {
                        Some(Ok(item)) => {
                            settings.insert(id, ItemSettings::Live(item));
                        }
                        Some(Err(e)) => config.problems.push(unreadable(e)),
                        None => {
                            let last_live = record
                                .versions()
                                .rev()
                                .find_map(|(_, content)| parse_fields::<Item>(content));
                            if let Some(Ok(item)) = last_live {
                                settings.insert(id, ItemSettings::Untracked(item));
                            }
                        }
                    }
                }
                REPLICA_KIND => {
                    let (item, node) = match parse_replica_id(&key.id) {
                        Ok(id) => id,
                        Err(e) => {
                            config.problems.push(unreadable(e));
                            continue;
                        }
                    };
                    match parse_fields::<Replica>(record.latest().1) {
                        Some(Ok(replica)) => {
                            replicas.entry(item).or_default().insert(node, replica);
                        }
                        Some(Err(e)) => config.problems.push(unreadable(e)),
                        None => {}
                    }
                }
                // Kinds from newer versions are kept in the store and ignored.
                _ => {}
            }
        }

        for (id, item_replicas) in replicas {
            let item = match settings.remove(&id) {
                Some(ItemSettings::Live(item)) => item,
                Some(ItemSettings::Untracked(item)) => {
                    config.problems.push(Problem::Resurrected { item: id });
                    item
                }
                None => {
                    for node in item_replicas.into_keys() {
                        config.problems.push(Problem::Orphaned { item: id, node });
                    }
                    continue;
                }
            };
            config.items.insert(
                id,
                ItemEntry {
                    item,
                    replicas: item_replicas,
                },
            );
        }

        config.find_duplicate_names();
        config.find_items_without_senders();
        config.find_overlaps();
        config
    }

    fn find_duplicate_names(&mut self) {
        let mut by_name: BTreeMap<&str, Vec<ItemId>> = BTreeMap::new();
        for (id, entry) in &self.items {
            by_name.entry(&entry.item.name).or_default().push(*id);
        }
        for (name, items) in by_name {
            if items.len() > 1 {
                self.problems.push(Problem::DuplicateName {
                    name: name.to_owned(),
                    items,
                });
            }
        }
    }

    fn find_items_without_senders(&mut self) {
        for (id, entry) in &self.items {
            if !entry.has_sender() {
                self.problems.push(Problem::NoSender { item: *id });
            }
        }
    }

    fn find_overlaps(&mut self) {
        let mut by_node: BTreeMap<&NodeId, Vec<(ItemId, &AbsPath)>> = BTreeMap::new();
        for (id, entry) in &self.items {
            for (node, replica) in &entry.replicas {
                by_node.entry(node).or_default().push((*id, replica.path()));
            }
        }
        let mut overlaps = Vec::new();
        for (node, paths) in by_node {
            for (i, (a, a_path)) in paths.iter().enumerate() {
                for (b, b_path) in &paths[i + 1..] {
                    if a_path.overlaps(b_path) {
                        overlaps.push(Problem::Overlap {
                            node: node.clone(),
                            items: [*a, *b],
                        });
                    }
                }
            }
        }
        self.problems.extend(overlaps);
    }
}

/// Why a local edit was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EditError {
    ItemExists(ItemId),
    UnknownItem(ItemId),
    ReplicaExists {
        item: ItemId,
        node: NodeId,
    },
    UnknownReplica {
        item: ItemId,
        node: NodeId,
    },
    /// Removing an Item's last Replica; untrack the Item instead.
    LastReplica {
        item: ItemId,
    },
    /// The edit would cause a Problem that wasn't there before.
    WouldCause(Problem),
}

impl fmt::Display for EditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ItemExists(item) => write!(f, "item {item} already exists"),
            Self::UnknownItem(item) => write!(f, "no item {item}"),
            Self::ReplicaExists { item, node } => {
                write!(f, "item {item} already has a replica on {node}")
            }
            Self::UnknownReplica { item, node } => {
                write!(f, "item {item} has no replica on {node}")
            }
            Self::LastReplica { item } => write!(
                f,
                "that is the last replica of item {item}; untrack the item instead"
            ),
            Self::WouldCause(problem) => write!(f, "refused, because {problem}"),
        }
    }
}

impl std::error::Error for EditError {}

/// One Node's copy of the Fleet's configuration records.
#[derive(Clone, Debug)]
pub struct ConfigDb {
    store: Store,
    clock: Clock,
}

impl ConfigDb {
    /// Opens `store` on `node`. The clock starts after every stored version,
    /// so local edits supersede them even if the wall clock went back.
    pub fn new(node: NodeId, store: Store) -> Self {
        let mut clock = Clock::new(node);
        if let Some(hlc) = store.max_hlc() {
            clock.observe(hlc);
        }
        Self { store, clock }
    }

    pub fn node(&self) -> &NodeId {
        self.clock.node()
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn config(&self) -> Config {
        Config::new(&self.store)
    }

    /// Merges records from another Node, and returns how many versions were
    /// left out for being more than [`MAX_CLOCK_DRIFT`] ahead of `now`.
    /// Later local edits supersede everything merged.
    pub fn merge(&mut self, other: &Store, now: Timestamp) -> usize {
        let limit = now.saturating_add(MAX_CLOCK_DRIFT);
        let mut rejected = 0;
        for (key, record) in other.records() {
            for (hlc, content) in record.versions() {
                if hlc.wall > limit {
                    rejected += 1;
                    continue;
                }
                self.clock.observe(hlc);
                self.store.insert(key.clone(), hlc.clone(), content.clone());
            }
        }
        rejected
    }

    /// Drops versions and tombstones that expired more than `window` ago. An
    /// untracked Item's record is kept while a live Replica refers to it, so
    /// a resurrected Item keeps its settings.
    pub fn prune(&mut self, now: Timestamp, window: Duration) {
        let referenced: BTreeSet<RecordKey> = self
            .store
            .records()
            .filter(|(key, record)| key.kind == REPLICA_KIND && is_live(Some(record)))
            .filter_map(|(key, _)| parse_replica_id(&key.id).ok())
            .map(|(item, _)| item_key(item))
            .collect();
        self.store
            .prune(now, window, |key| referenced.contains(key));
    }

    /// Creates an Item with its first Replica.
    pub fn track(
        &mut self,
        now: Timestamp,
        id: ItemId,
        item: Item,
        node: &NodeId,
        replica: Replica,
    ) -> Result<(), EditError> {
        if self.store.get(&item_key(id)).is_some() {
            return Err(EditError::ItemExists(id));
        }
        self.edit(|db| {
            db.write(now, item_key(id), &item);
            db.write(now, replica_key(id, node), &replica);
        })
    }

    /// Edits an Item's settings. Editing a resurrected Item restores it.
    pub fn update_item(
        &mut self,
        now: Timestamp,
        id: ItemId,
        edit: impl FnOnce(&mut Item),
    ) -> Result<(), EditError> {
        let mut item = self.item(id)?.item.clone();
        edit(&mut item);
        self.edit(|db| db.write(now, item_key(id), &item))
    }

    /// Stops tracking an Item and removes all its Replicas, including ones
    /// this version can't read. Files stay in place.
    pub fn untrack(&mut self, now: Timestamp, id: ItemId) -> Result<(), EditError> {
        let item_key = item_key(id);
        let prefix = format!("{id}/");
        let replica_keys: Vec<RecordKey> = self
            .store
            .records()
            .filter(|(key, record)| {
                key.kind == REPLICA_KIND && key.id.starts_with(&prefix) && is_live(Some(record))
            })
            .map(|(key, _)| key.clone())
            .collect();
        if replica_keys.is_empty() && !is_live(self.store.get(&item_key)) {
            return Err(EditError::UnknownItem(id));
        }
        for key in replica_keys {
            self.delete(now, key);
        }
        self.delete(now, item_key);
        Ok(())
    }

    /// Adds a Replica. On a one-way Item, a Replica away from the source is
    /// stored as a mirror, so it stays receive-only if the Item becomes
    /// two-way.
    pub fn add_replica(
        &mut self,
        now: Timestamp,
        id: ItemId,
        node: &NodeId,
        mut replica: Replica,
    ) -> Result<(), EditError> {
        let direction = self.item(id)?.item.direction.clone();
        let key = replica_key(id, node);
        if is_live(self.store.get(&key)) {
            return Err(EditError::ReplicaExists {
                item: id,
                node: node.clone(),
            });
        }
        if direction.role(node, replica.mirror) == Role::Mirror {
            replica.mirror = true;
        }
        self.edit(|db| db.write(now, key, &replica))
    }

    /// Edits a Replica's settings. Its path can't change; remove the Replica
    /// and add a new one instead.
    pub fn update_replica(
        &mut self,
        now: Timestamp,
        id: ItemId,
        node: &NodeId,
        edit: impl FnOnce(&mut Replica),
    ) -> Result<(), EditError> {
        let mut replica = self
            .item(id)?
            .replicas
            .get(node)
            .ok_or_else(|| EditError::UnknownReplica {
                item: id,
                node: node.clone(),
            })?
            .clone();
        edit(&mut replica);
        self.edit(|db| db.write(now, replica_key(id, node), &replica))
    }

    /// Removes a Replica, including one this version can't read. Its files
    /// stay in place.
    pub fn remove_replica(
        &mut self,
        now: Timestamp,
        id: ItemId,
        node: &NodeId,
    ) -> Result<(), EditError> {
        let key = replica_key(id, node);
        if !is_live(self.store.get(&key)) {
            return Err(EditError::UnknownReplica {
                item: id,
                node: node.clone(),
            });
        }
        if let Ok(entry) = self.item(id)
            && entry.replicas.len() == 1
            && entry.replicas.contains_key(node)
        {
            return Err(EditError::LastReplica { item: id });
        }
        self.edit(|db| db.delete(now, key))
    }

    fn item(&self, id: ItemId) -> Result<ItemEntry, EditError> {
        self.config()
            .items
            .remove(&id)
            .ok_or(EditError::UnknownItem(id))
    }

    /// Applies `write` unless it would add a Problem. Duplicate names are
    /// allowed, since names need not be unique.
    fn edit(&mut self, write: impl FnOnce(&mut Self)) -> Result<(), EditError> {
        let before = self.config().problems;
        let mut after = self.clone();
        write(&mut after);
        let added = after.config().problems.into_iter().find(|problem| {
            !matches!(problem, Problem::DuplicateName { .. }) && !before.contains(problem)
        });
        if let Some(problem) = added {
            return Err(EditError::WouldCause(problem));
        }
        *self = after;
        Ok(())
    }

    fn write(&mut self, now: Timestamp, key: RecordKey, value: &impl Serialize) {
        let Value::Object(fields) = serde_json::to_value(value).expect("records serialize") else {
            unreachable!("records serialize to JSON objects");
        };
        let hlc = self.clock.tick(now);
        self.store.insert(
            key,
            hlc,
            Content::Live {
                schema: SCHEMA,
                fields,
            },
        );
    }

    fn delete(&mut self, now: Timestamp, key: RecordKey) {
        let hlc = self.clock.tick(now);
        self.store.insert(key, hlc, Content::Deleted);
    }
}

#[cfg(test)]
mod tests;
