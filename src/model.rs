//! Domain types: Node identity, Items, Replicas, Roles and Replica State.
//!
//! The vocabulary follows `GLOSSARY.md`.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::hlc::Timestamp;

/// Fields of a record that this version of heighliner does not know. They are
/// kept and written back unchanged.
type UnknownFields = Map<String, Value>;

/// A value that failed validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidValue {
    pub what: &'static str,
    pub value: String,
    pub reason: &'static str,
}

impl fmt::Display for InvalidValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid {} {:?}: {}", self.what, self.value, self.reason)
    }
}

impl std::error::Error for InvalidValue {}

/// A Node's stable Tailscale node ID.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Result<Self, InvalidValue> {
        let id = id.into();
        if id.is_empty() || id.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(InvalidValue {
                what: "node ID",
                value: id,
                reason: "must be non-empty without whitespace",
            });
        }
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for NodeId {
    type Error = InvalidValue;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl From<NodeId> for String {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// An Item's permanent identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ItemId(Uuid);

impl ItemId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    #[cfg(test)]
    pub(crate) const fn from_u128(id: u128) -> Self {
        Self(Uuid::from_u128(id))
    }
}

impl FromStr for ItemId {
    type Err = InvalidValue;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Uuid::try_parse(id).map(Self).map_err(|_| InvalidValue {
            what: "item ID",
            value: id.to_owned(),
            reason: "must be a UUID",
        })
    }
}

impl fmt::Display for ItemId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.hyphenated().fmt(f)
    }
}

/// A normalized absolute path on some Node: it starts with `/` and has no
/// empty, `.` or `..` components and no trailing `/`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AbsPath(String);

impl AbsPath {
    pub fn new(path: impl Into<String>) -> Result<Self, InvalidValue> {
        let path = path.into();
        let invalid = |reason| InvalidValue {
            what: "path",
            value: path.clone(),
            reason,
        };
        let Some(rest) = path.strip_prefix('/') else {
            return Err(invalid("must be absolute"));
        };
        if path.contains('\0') {
            return Err(invalid("must not contain NUL"));
        }
        if !rest.is_empty()
            && rest
                .split('/')
                .any(|c| c.is_empty() || c == "." || c == "..")
        {
            return Err(invalid("must be normalized"));
        }
        Ok(Self(path))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn components(&self) -> impl Iterator<Item = &str> {
        self.0.split('/').filter(|c| !c.is_empty())
    }

    /// Whether one path is the other or contains it.
    pub fn overlaps(&self, other: &Self) -> bool {
        self.components()
            .zip(other.components())
            .all(|(a, b)| a == b)
    }
}

impl TryFrom<String> for AbsPath {
    type Error = InvalidValue;

    fn try_from(path: String) -> Result<Self, Self::Error> {
        Self::new(path)
    }
}

impl From<AbsPath> for String {
    fn from(path: AbsPath) -> Self {
        path.0
    }
}

impl fmt::Display for AbsPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One line of an Item's exclude patterns, in Syncthing's `.stignore` syntax.
///
/// Directives (`#include`, `#escape`) and the `(?d)` flag are refused: the
/// flag lets Syncthing delete excluded files.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ExcludePattern(String);

impl ExcludePattern {
    pub fn new(pattern: impl Into<String>) -> Result<Self, InvalidValue> {
        let pattern = pattern.into();
        let reason = if pattern.trim().is_empty() || pattern.contains(['\n', '\r']) {
            "must be one non-blank line"
        } else if pattern.starts_with('#') {
            "must not be a directive"
        } else if pattern.contains("(?d)") {
            "must not use the (?d) flag"
        } else {
            return Ok(Self(pattern));
        };
        Err(InvalidValue {
            what: "exclude pattern",
            value: pattern,
            reason,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ExcludePattern {
    type Error = InvalidValue;

    fn try_from(pattern: String) -> Result<Self, Self::Error> {
        Self::new(pattern)
    }
}

impl From<ExcludePattern> for String {
    fn from(pattern: ExcludePattern) -> Self {
        pattern.0
    }
}

/// Whether an Item is one-way, from the Replica on `source`, or two-way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Direction {
    OneWay { source: NodeId },
    TwoWay,
}

impl Direction {
    /// The Role of an Item's Replica on `node`.
    pub fn role(&self, node: &NodeId, mirror: bool) -> Role {
        match self {
            Self::OneWay { source } if source == node => Role::Source,
            Self::OneWay { .. } => Role::Mirror,
            Self::TwoWay if mirror => Role::Mirror,
            Self::TwoWay => Role::Peer,
        }
    }
}

/// How a Replica takes part in its Item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Source,
    Mirror,
    Peer,
}

/// A tracked directory tree.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ItemRecord", into = "ItemRecord")]
pub struct Item {
    pub name: String,
    pub excludes: Vec<ExcludePattern>,
    pub direction: Direction,
    pub paused: bool,
    unknown_fields: UnknownFields,
}

impl Item {
    pub fn new(name: impl Into<String>, direction: Direction) -> Self {
        Self {
            name: name.into(),
            excludes: Vec::new(),
            direction,
            paused: false,
            unknown_fields: UnknownFields::new(),
        }
    }
}

/// How an Item is stored: flat, so every unknown field is at the top level.
#[derive(Serialize, Deserialize)]
struct ItemRecord {
    name: String,
    #[serde(default)]
    excludes: Vec<ExcludePattern>,
    direction: DirectionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<NodeId>,
    #[serde(default)]
    paused: bool,
    #[serde(flatten)]
    unknown_fields: UnknownFields,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum DirectionKind {
    OneWay,
    TwoWay,
}

impl TryFrom<ItemRecord> for Item {
    type Error = &'static str;

    fn try_from(record: ItemRecord) -> Result<Self, Self::Error> {
        let direction = match (record.direction, record.source) {
            (DirectionKind::OneWay, Some(source)) => Direction::OneWay { source },
            (DirectionKind::TwoWay, None) => Direction::TwoWay,
            (DirectionKind::OneWay, None) => return Err("one-way item without a source"),
            (DirectionKind::TwoWay, Some(_)) => return Err("two-way item with a source"),
        };
        Ok(Self {
            name: record.name,
            excludes: record.excludes,
            direction,
            paused: record.paused,
            unknown_fields: record.unknown_fields,
        })
    }
}

impl From<Item> for ItemRecord {
    fn from(item: Item) -> Self {
        let (direction, source) = match item.direction {
            Direction::OneWay { source } => (DirectionKind::OneWay, Some(source)),
            Direction::TwoWay => (DirectionKind::TwoWay, None),
        };
        Self {
            name: item.name,
            excludes: item.excludes,
            direction,
            source,
            paused: item.paused,
            unknown_fields: item.unknown_fields,
        }
    }
}

/// One copy of an Item on one Node. An Item has at most one Replica per Node,
/// so the Replica is identified by its Item and Node. Its path is fixed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replica {
    path: AbsPath,
    /// Whether this Replica only receives changes in a two-way Item.
    #[serde(default)]
    pub mirror: bool,
    #[serde(default)]
    pub paused: bool,
    #[serde(flatten)]
    unknown_fields: UnknownFields,
}

impl Replica {
    pub fn new(path: AbsPath) -> Self {
        Self {
            path,
            mirror: false,
            paused: false,
            unknown_fields: UnknownFields::new(),
        }
    }

    pub fn path(&self) -> &AbsPath {
        &self.path
    }
}

/// What a Replica knows about its own freshness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicaState {
    /// Has everything its reachable senders have.
    Current,
    Syncing {
        files_left: u64,
        bytes_left: u64,
    },
    /// Its senders are unreachable. `last_current` is when it was last
    /// confirmed current, if ever.
    Behind {
        last_current: Option<Timestamp>,
    },
    Error {
        message: String,
    },
    Paused,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn path(p: &str) -> AbsPath {
        AbsPath::new(p).unwrap()
    }

    #[test]
    fn abs_path_validation() {
        for ok in ["/", "/a", "/a/b.c", "/a/.b", "/a/..b"] {
            assert!(AbsPath::new(ok).is_ok(), "{ok}");
        }
        for bad in [
            "", "a", "a/b", "//", "/a/", "/a//b", "/a/./b", "/a/..", "/a\0b",
        ] {
            assert!(AbsPath::new(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn exclude_pattern_validation() {
        for ok in ["*.tmp", "!keep", "(?i)cache", "// comment", "a#b"] {
            assert!(ExcludePattern::new(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            " ",
            "a\nb",
            "#include x",
            "#escape=\\",
            "(?d)*",
            "!(?i)(?d)x",
        ] {
            assert!(ExcludePattern::new(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn abs_path_overlap() {
        assert!(path("/a").overlaps(&path("/a")));
        assert!(path("/a").overlaps(&path("/a/b")));
        assert!(path("/a/b").overlaps(&path("/a")));
        assert!(path("/").overlaps(&path("/a")));
        assert!(!path("/a").overlaps(&path("/ab")));
        assert!(!path("/a/b").overlaps(&path("/a/c")));
    }

    #[test]
    fn roles() {
        let a = NodeId::new("a").unwrap();
        let b = NodeId::new("b").unwrap();
        let one_way = Direction::OneWay { source: a.clone() };
        assert_eq!(one_way.role(&a, true), Role::Source);
        assert_eq!(one_way.role(&b, false), Role::Mirror);
        assert_eq!(Direction::TwoWay.role(&a, false), Role::Peer);
        assert_eq!(Direction::TwoWay.role(&a, true), Role::Mirror);
    }

    #[test]
    fn item_round_trip_keeps_unknown_fields() {
        let stored = json!({
            "name": "docs",
            "excludes": ["*.tmp"],
            "direction": "one-way",
            "source": "n1",
            "paused": false,
            "future": {"nested": [1, 2]},
        });
        let item: Item = serde_json::from_value(stored.clone()).unwrap();
        assert_eq!(
            item.direction,
            Direction::OneWay {
                source: NodeId::new("n1").unwrap()
            }
        );
        assert_eq!(serde_json::to_value(&item).unwrap(), stored);
    }

    #[test]
    fn item_rejects_inconsistent_direction() {
        for stored in [
            json!({"name": "x", "direction": "one-way"}),
            json!({"name": "x", "direction": "two-way", "source": "n1"}),
            json!({"name": "x", "direction": "n-way"}),
        ] {
            assert!(serde_json::from_value::<Item>(stored).is_err());
        }
    }

    #[test]
    fn replica_round_trip_keeps_unknown_fields() {
        let stored = json!({"path": "/srv/a", "mirror": true, "paused": false, "future": 1});
        let replica: Replica = serde_json::from_value(stored.clone()).unwrap();
        assert!(replica.mirror);
        assert_eq!(serde_json::to_value(&replica).unwrap(), stored);
    }
}
