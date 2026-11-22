//! The daemon's API, which the CLI uses and the web UI shows: its paths
//! and the JSON they take and give.
//!
//! It is served with the Node protocol on each Node's Tailscale addresses,
//! so the same ownership check authorizes every request (ADR-0003). Items
//! are named by ID or, when only one has it, by name, and Nodes by node ID
//! or machine name. A path on this Node may start with `~`, the daemon's
//! user's home; a path on another Node must be absolute.

use std::net::IpAddr;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::hlc::Timestamp;
use crate::model::{ItemId, NodeId, ReplicaState, Role};

/// `path`, absolute, with `.` and empty components dropped. A `..` is
/// kept, since resolving it by name alone would be wrong after a symlink;
/// only the Node the path is on can resolve it.
pub fn normalize(path: &str) -> String {
    let parts: Vec<&str> = path
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect();
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
    /// The Nodes the records name, and the ones that speak another major
    /// version of the Node protocol.
    #[serde(default)]
    pub nodes: Vec<NodeStatus>,
    pub items: Vec<ItemStatus>,
    /// The configuration's problems, as sentences.
    pub problems: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub node: NodeId,
    /// Its machine name, if the Node is on the tailnet.
    pub name: Option<String>,
    /// A Tailscale address of it, if it is on the tailnet.
    pub ip: Option<IpAddr>,
    /// Whether it left the Fleet.
    pub departed: bool,
    pub reach: Reach,
}

/// Whether the Node answering can reach a Node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reach", rename_all = "kebab-case")]
pub enum Reach {
    /// It is the Node answering.
    Here,
    /// Its latest round read the Node's states, at `read_at`.
    Live { read_at: Timestamp },
    /// Its latest round couldn't read the Node's states; they were last
    /// read at `read_at`, if ever.
    Unreachable { read_at: Option<Timestamp> },
    /// The Node speaks major version `protocol` of the Node protocol, not
    /// the answering Node's, so the two exchange nothing until the older one
    /// is upgraded.
    Incompatible { protocol: u32 },
}

/// Whether every copy of an Item is known to be current.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "replication", rename_all = "kebab-case")]
pub enum Replication {
    /// Every Replica on a Node still in the Fleet is current and connected
    /// to every Replica that sends to it, live here or as the answering
    /// Node's latest round read.
    Full,
    /// Not every one is confirmed current now. `oldest` is the oldest time
    /// one of them was last confirmed so, or none if one never was.
    Unconfirmed { oldest: Option<Timestamp> },
}

impl Default for Replication {
    fn default() -> Self {
        Self::Unconfirmed { oldest: None }
    }
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
    /// Whether it was untracked, then brought back by an edit made
    /// without seeing that.
    #[serde(default)]
    pub resurrected: bool,
    /// Whether another Item has its name.
    #[serde(default)]
    pub duplicate_name: bool,
    #[serde(default)]
    pub replication: Replication,
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
    /// Whether its Node left the Fleet.
    #[serde(default)]
    pub departed: bool,
    /// Its state: live for a Replica on the Node answering, and otherwise
    /// as its Node last said, if it has.
    pub state: Option<ReplicaState>,
    /// When another Node's state was read.
    #[serde(default)]
    pub state_read_at: Option<Timestamp>,
    /// When its Node last confirmed it current, if ever.
    #[serde(default)]
    pub confirmed_at: Option<Timestamp>,
    /// Whether, when its state was read, it was connected to every Replica
    /// that sends to it now, so that being current means it has what they
    /// all have.
    #[serde(default)]
    pub every_sender: bool,
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

/// This Node's Data Conflicts: GET lists them.
pub const CONFLICTS: &str = "/api/v1/conflicts";

/// POST a [`Resolve`] to keep a Data Conflict's winning change.
pub const KEEP: &str = "/api/v1/conflicts/keep";

/// POST a [`Resolve`] to make a Data Conflict's losing change the file's
/// content again.
pub const SWAP: &str = "/api/v1/conflicts/swap";

/// POST to have this Node leave the Fleet.
pub const LEAVE: &str = "/api/v1/leave";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conflicts {
    pub conflicts: Vec<Conflict>,
    /// Replicas whose conflicts can't be read, and why.
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conflict {
    pub item: ItemId,
    pub item_name: String,
    /// The file's path in the Replica.
    pub path: String,
    /// When it happened, in Syncthing's local-time format.
    pub at: String,
    /// The short ID of the Syncthing device whose change won.
    pub winner: String,
    /// What identifies it, to keep or swap it.
    pub version: String,
    /// When its losing side expires, unless it is resolved first.
    pub expires_at: Timestamp,
}

/// Leaves the Fleet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Leave {
    /// That it can't be undone.
    pub confirm: bool,
}

/// Keeps or swaps a Data Conflict.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resolve {
    /// Its Item, by ID or name.
    pub item: String,
    pub version: String,
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
