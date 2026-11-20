//! The Fleet's configuration: Items and Replicas stored as records.
//!
//! [`Config`] is derived from a [`Store`], so every merge result has a valid
//! configuration. Merges can still produce situations a single Node's edits
//! never would, such as overlapping Replicas or a resurrected Item; the
//! configuration reports those as [`Problem`]s instead of rejecting them.
//!
//! [`ConfigDb`] is one Node's copy of the store. It validates local edits and
//! versions them with the Node's clock.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::hlc::{Clock, Timestamp};
use crate::model::{AbsPath, Direction, Item, ItemId, NodeId, Replica, Role};
use crate::record::{Content, Record, RecordKey, Store};

pub const ITEM_KIND: &str = "item";
pub const REPLICA_KIND: &str = "replica";

/// The record schema version this version of heighliner writes.
pub const SCHEMA: u32 = 1;

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

fn parse_replica_id(id: &str) -> Result<(ItemId, NodeId), String> {
    let (item, node) = id
        .split_once('/')
        .ok_or_else(|| format!("invalid replica ID {id:?}"))?;
    Ok((
        item.parse().map_err(|e| format!("{e}"))?,
        NodeId::new(node).map_err(|e| format!("{e}"))?,
    ))
}

fn parse_fields<T: DeserializeOwned>(content: &Content) -> Option<Result<T, String>> {
    match content {
        Content::Live { fields, .. } => {
            Some(serde_json::from_value(Value::Object(fields.clone())).map_err(|e| e.to_string()))
        }
        Content::Deleted => None,
    }
}

fn is_live(record: Option<&Record>) -> bool {
    record.is_some_and(|record| matches!(record.latest().1, Content::Live { .. }))
}

/// An Item with its Replicas, keyed by Node.
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
}

/// Something in a merged configuration that needs the user's attention.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Problem {
    /// A record of a known kind that this version can't read, for example
    /// one written by a newer version. It is kept but not acted on.
    Unreadable { key: RecordKey, error: String },
    /// An untracked Item that is back because one of its Replicas was added
    /// or changed without seeing the untrack.
    Resurrected { item: ItemId },
    /// A Replica whose Item is unknown or unreadable. It is not synced.
    Orphaned { item: ItemId, node: NodeId },
    /// Items that share a name.
    DuplicateName { name: String, items: Vec<ItemId> },
    /// A one-way Item whose source has no Replica, so nothing sends.
    MissingSource { item: ItemId, source: NodeId },
    /// Two Replicas on one Node whose paths overlap. Neither may be synced.
    Overlap { node: NodeId, items: [ItemId; 2] },
}

/// The configuration derived from a store.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Config {
    pub items: BTreeMap<ItemId, ItemEntry>,
    pub problems: Vec<Problem>,
}

impl Config {
    pub fn new(store: &Store) -> Self {
        let mut config = Self::default();
        // Untracked Items, with their last live content if it is retained.
        let mut untracked: BTreeMap<ItemId, Item> = BTreeMap::new();
        let mut replicas: BTreeMap<ItemId, BTreeMap<NodeId, Replica>> = BTreeMap::new();

        for (key, record) in store.records() {
            let unreadable = |error: String| Problem::Unreadable {
                key: key.clone(),
                error,
            };
            match key.kind.as_str() {
                ITEM_KIND => {
                    let id = match key.id.parse::<ItemId>() {
                        Ok(id) => id,
                        Err(e) => {
                            config.problems.push(unreadable(e.to_string()));
                            continue;
                        }
                    };
                    match parse_fields::<Item>(record.latest().1) {
                        Some(Ok(item)) => {
                            config.items.insert(
                                id,
                                ItemEntry {
                                    item,
                                    replicas: BTreeMap::new(),
                                },
                            );
                        }
                        Some(Err(e)) => config.problems.push(unreadable(e)),
                        None => {
                            let last_live = record
                                .versions()
                                .rev()
                                .find_map(|(_, content)| parse_fields::<Item>(content));
                            if let Some(Ok(item)) = last_live {
                                untracked.insert(id, item);
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
            if let Some(entry) = config.items.get_mut(&id) {
                entry.replicas = item_replicas;
            } else if let Some(item) = untracked.remove(&id) {
                config.problems.push(Problem::Resurrected { item: id });
                config.items.insert(
                    id,
                    ItemEntry {
                        item,
                        replicas: item_replicas,
                    },
                );
            } else {
                for node in item_replicas.into_keys() {
                    config.problems.push(Problem::Orphaned { item: id, node });
                }
            }
        }

        config.find_duplicate_names();
        config.find_missing_sources();
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

    fn find_missing_sources(&mut self) {
        for (id, entry) in &self.items {
            if let Direction::OneWay { source } = &entry.item.direction
                && !entry.replicas.contains_key(source)
            {
                self.problems.push(Problem::MissingSource {
                    item: *id,
                    source: source.clone(),
                });
            }
        }
    }

    fn find_overlaps(&mut self) {
        let mut by_node: BTreeMap<&NodeId, Vec<(ItemId, &AbsPath)>> = BTreeMap::new();
        for (id, entry) in &self.items {
            for (node, replica) in &entry.replicas {
                by_node.entry(node).or_default().push((*id, &replica.path));
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

    /// Checks that a Replica of `item` at `path` on `node` would not overlap
    /// another Item's Replica.
    fn check_overlap(&self, item: ItemId, node: &NodeId, path: &AbsPath) -> Result<(), EditError> {
        for (other, entry) in &self.items {
            if *other == item {
                continue;
            }
            if let Some(replica) = entry.replicas.get(node)
                && replica.path.overlaps(path)
            {
                return Err(EditError::Overlap {
                    node: node.clone(),
                    item: *other,
                });
            }
        }
        Ok(())
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
    /// The path overlaps a Replica of `item` on the same Node.
    Overlap {
        node: NodeId,
        item: ItemId,
    },
    /// A one-way Item's source must be a Node with a Replica.
    SourceWithoutReplica {
        item: ItemId,
        source: NodeId,
    },
    /// A one-way Item's source Replica can't be removed; change the Direction
    /// first.
    RemovesSource {
        item: ItemId,
        node: NodeId,
    },
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
            Self::Overlap { node, item } => {
                write!(f, "path overlaps the replica of item {item} on {node}")
            }
            Self::SourceWithoutReplica { item, source } => {
                write!(f, "item {item} has no replica on its source {source}")
            }
            Self::RemovesSource { item, node } => write!(
                f,
                "the replica on {node} is the source of one-way item {item}; change its direction first"
            ),
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

    /// Merges records from another Node. Later local edits supersede them.
    pub fn merge(&mut self, other: &Store) {
        if let Some(hlc) = other.max_hlc() {
            self.clock.observe(hlc);
        }
        self.store.merge(other);
    }

    pub fn prune(&mut self, now: Timestamp, window: Duration) {
        self.store.prune(now, window);
    }

    /// Creates an Item with its first Replica.
    pub fn track(
        &mut self,
        now: Timestamp,
        id: ItemId,
        item: &Item,
        node: &NodeId,
        replica: &Replica,
    ) -> Result<(), EditError> {
        if self.store.get(&item_key(id)).is_some() {
            return Err(EditError::ItemExists(id));
        }
        if let Direction::OneWay { source } = &item.direction
            && source != node
        {
            return Err(EditError::SourceWithoutReplica {
                item: id,
                source: source.clone(),
            });
        }
        self.config().check_overlap(id, node, &replica.path)?;
        self.write(now, item_key(id), item);
        self.write(now, replica_key(id, node), replica);
        Ok(())
    }

    /// Edits an Item. Editing a resurrected Item restores it.
    pub fn update_item(
        &mut self,
        now: Timestamp,
        id: ItemId,
        edit: impl FnOnce(&mut Item),
    ) -> Result<(), EditError> {
        let config = self.config();
        let entry = config.items.get(&id).ok_or(EditError::UnknownItem(id))?;
        let mut item = entry.item.clone();
        edit(&mut item);
        if let Direction::OneWay { source } = &item.direction
            && !entry.replicas.contains_key(source)
        {
            return Err(EditError::SourceWithoutReplica {
                item: id,
                source: source.clone(),
            });
        }
        self.write(now, item_key(id), &item);
        Ok(())
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

    pub fn add_replica(
        &mut self,
        now: Timestamp,
        id: ItemId,
        node: &NodeId,
        replica: &Replica,
    ) -> Result<(), EditError> {
        let config = self.config();
        if !config.items.contains_key(&id) {
            return Err(EditError::UnknownItem(id));
        }
        let key = replica_key(id, node);
        if is_live(self.store.get(&key)) {
            return Err(EditError::ReplicaExists {
                item: id,
                node: node.clone(),
            });
        }
        config.check_overlap(id, node, &replica.path)?;
        self.write(now, key, replica);
        Ok(())
    }

    pub fn update_replica(
        &mut self,
        now: Timestamp,
        id: ItemId,
        node: &NodeId,
        edit: impl FnOnce(&mut Replica),
    ) -> Result<(), EditError> {
        let config = self.config();
        let entry = config.items.get(&id).ok_or(EditError::UnknownItem(id))?;
        let mut replica = entry
            .replicas
            .get(node)
            .ok_or_else(|| EditError::UnknownReplica {
                item: id,
                node: node.clone(),
            })?
            .clone();
        edit(&mut replica);
        config.check_overlap(id, node, &replica.path)?;
        self.write(now, replica_key(id, node), &replica);
        Ok(())
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
        if let Some(entry) = self.config().items.get(&id)
            && entry.item.direction
                == (Direction::OneWay {
                    source: node.clone(),
                })
        {
            return Err(EditError::RemovesSource {
                item: id,
                node: node.clone(),
            });
        }
        self.delete(now, key);
        Ok(())
    }

    /// Writes a new version of a record. It keeps the record's schema version
    /// if that is newer, since the value carries forward unknown fields.
    fn write(&mut self, now: Timestamp, key: RecordKey, value: &impl Serialize) {
        let Value::Object(fields) = serde_json::to_value(value).expect("records serialize") else {
            unreachable!("records serialize to JSON objects");
        };
        let stored_schema = self.store.get(&key).and_then(|record| {
            record
                .versions()
                .rev()
                .find_map(|(_, content)| match content {
                    Content::Live { schema, .. } => Some(*schema),
                    Content::Deleted => None,
                })
        });
        let schema = stored_schema.map_or(SCHEMA, |stored| stored.max(SCHEMA));
        let hlc = self.clock.tick(now);
        self.store
            .insert(key, hlc, Content::Live { schema, fields });
    }

    fn delete(&mut self, now: Timestamp, key: RecordKey) {
        let hlc = self.clock.tick(now);
        self.store.insert(key, hlc, Content::Deleted);
    }
}

#[cfg(test)]
mod tests;
