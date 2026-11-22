//! The daemon's API, which the CLI (and later the web UI) use: its paths
//! and the JSON they take and give.
//!
//! It is served with the Node protocol on each Node's Tailscale addresses,
//! so the same ownership check authorizes every request (ADR-0003). Items
//! are named by ID or, when only one has it, by name, and Nodes by node ID
//! or machine name. A path on this Node may start with `~`, the daemon's
//! user's home; a path on another Node must be absolute.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::model::{ItemId, NodeId, ReplicaState, Role};

/// `path`, absolute, with `.` and empty components dropped and `..`
/// resolved by its name alone, as a Replica's path is written.
pub fn normalize(path: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }
    format!("/{}", parts.join("/"))
}

/// This Node's view of the Fleet's configuration.
pub const STATUS: &str = "/api/v1/status";

/// Items: POST tracks a path as a new one.
pub const ITEMS: &str = "/api/v1/items";

/// An Item, by ID or name: PATCH pauses or resumes it, DELETE untracks it.
pub fn item(item: &str) -> String {
    format!("{ITEMS}/{}", crate::http::percent_encode(item))
}

/// An Item's Replicas: POST adds one.
pub fn replicas(item: &str) -> String {
    format!("{}/replicas", self::item(item))
}

/// An Item's Replica on a Node, by node ID or machine name: PATCH pauses or
/// resumes it, DELETE removes it.
pub fn replica(item: &str, node: &str) -> String {
    format!("{}/{}", replicas(item), crate::http::percent_encode(node))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    /// The Node answering.
    pub node: NodeId,
    pub items: Vec<ItemStatus>,
    /// The configuration's problems, as sentences.
    pub problems: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemStatus {
    pub id: ItemId,
    pub name: String,
    /// The source's Node, for a one-way Item.
    pub source: Option<NodeId>,
    pub excludes: Vec<String>,
    pub paused: bool,
    pub retention_days: Option<NonZeroU32>,
    pub replicas: Vec<ReplicaStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaStatus {
    pub node: NodeId,
    /// Its machine name, if the Node is on the tailnet.
    pub node_name: Option<String>,
    pub path: String,
    pub role: Role,
    /// Whether the Replica itself is paused; its Item may be too.
    pub paused: bool,
    /// Its state, for a Replica on the Node answering.
    pub state: Option<ReplicaState>,
}

/// Tracks a path as a new Item, with its first Replica there, which adopts
/// the files already at it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Track {
    /// Its Node; this one if none.
    pub node: Option<String>,
    pub path: String,
    /// Its name; the path's last component if none.
    pub name: Option<String>,
    /// Whether it is one-way, from this first Replica.
    pub one_way: bool,
    pub excludes: Vec<String>,
    pub retention_days: Option<NonZeroU32>,
}

/// Adds a Replica of an Item.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddReplica {
    /// Its Node; this one if none.
    pub node: Option<String>,
    pub path: String,
    pub mirror: bool,
    /// Whether it may start at a path that already holds files, merging
    /// them with the Item's.
    pub adopt: bool,
}

/// Pauses or resumes an Item or a Replica.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetPaused {
    pub paused: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tracked {
    pub id: ItemId,
}

/// Why a request was refused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refused {
    pub error: String,
}
