//! The daemon's API, which the CLI uses and the web UI shows: its paths
//! and the JSON they take and give.
//!
//! It is served with the Node protocol on each Node's Tailscale addresses,
//! so the same ownership check authorizes every request (ADR-0003). Items
//! are named by ID or, when only one has it, by name, and Nodes by node ID
//! or machine name. A path is absolute or starts with `~`, the home of
//! the daemon's user on the path's Node.

use std::net::IpAddr;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::bluetooth::Address;
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
    /// The Nodes the records name, the ones whose states were read unless
    /// Tailscale says they have left the tailnet, and the ones that speak
    /// another major version of the Node protocol.
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

impl Conflict {
    /// When it happened, as `2026-11-22 20:50:22`, in the local time of
    /// the device that saw it, or as Syncthing wrote it if it can't be read.
    pub fn local_time(&self) -> String {
        let at = self.at.as_bytes();
        let digits = |range: std::ops::Range<usize>| at[range].iter().all(u8::is_ascii_digit);
        if at.len() != 15 || at[8] != b'-' || !digits(0..8) || !digits(9..15) {
            return self.at.clone();
        }
        let at = &self.at;
        format!(
            "{}-{}-{} {}:{}:{}",
            &at[0..4],
            &at[4..6],
            &at[6..8],
            &at[9..11],
            &at[11..13],
            &at[13..15]
        )
    }
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

/// The Fleet's Peripherals: GET lists them, as [`Peripherals`].
pub const PERIPHERALS: &str = "/api/v1/peripherals";

/// A Peripheral, by address or by a name a Node gives it: POST a
/// [`PeripheralAction`] to act on it.
pub fn peripheral(peripheral: &str) -> String {
    format!("{PERIPHERALS}/{}", crate::http::percent_encode(peripheral))
}

/// The Peripherals paired with the Nodes that could be asked, and what
/// each Node answered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peripherals {
    /// The Fleet's Nodes, the answering one first.
    pub nodes: Vec<BluetoothNode>,
    pub peripherals: Vec<Peripheral>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BluetoothNode {
    pub node: NodeId,
    /// Its machine name, if the Node is on the tailnet.
    pub name: Option<String>,
    pub bluetooth: BluetoothState,
}

/// What a Node said of its Bluetooth.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum BluetoothState {
    /// It listed its Peripherals.
    Ready,
    /// It has no Bluetooth adapter, or BlueZ isn't running there.
    NoAdapter,
    Unreachable,
    /// Its heighliner is older than Peripherals.
    Older,
    /// It speaks another major version of the Node protocol.
    Incompatible,
    /// It couldn't list them.
    Failed {
        error: String,
    },
}

impl BluetoothState {
    /// Why the Node's Peripherals aren't known, if they aren't.
    pub fn problem(&self) -> Option<String> {
        match self {
            Self::Ready => None,
            Self::NoAdapter => Some("has no Bluetooth adapter, or BlueZ isn't running".into()),
            Self::Unreachable => Some("can't be reached".into()),
            Self::Older => {
                Some("runs an older heighliner that can't manage Peripherals; upgrade it".into())
            }
            Self::Incompatible => Some("speaks another version of the Node protocol".into()),
            Self::Failed { error } => Some(format!("can't list its Peripherals: {error}")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peripheral {
    pub address: Address,
    /// The name shown: the alias of the first Node it is paired with, in
    /// the order of [`Peripherals::nodes`], so the answering Node's when it
    /// is paired there.
    pub alias: String,
    /// The Nodes it is paired with, in the order of [`Peripherals::nodes`].
    pub paired: Vec<PairedOn>,
}

/// A Peripheral as one Node it is paired with sees it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairedOn {
    pub node: NodeId,
    /// What that Node calls it.
    pub alias: String,
    pub connected: bool,
}

impl Peripheral {
    /// Each name its Nodes give it, with those Nodes, in the order they
    /// first appear.
    pub fn aliases(&self) -> Vec<(&str, Vec<&NodeId>)> {
        let mut aliases: Vec<(&str, Vec<&NodeId>)> = Vec::new();
        for paired in &self.paired {
            match aliases.iter_mut().find(|(alias, _)| *alias == paired.alias) {
                Some((_, nodes)) => nodes.push(&paired.node),
                None => aliases.push((&paired.alias, vec![&paired.node])),
            }
        }
        aliases
    }

    /// The Nodes of `nodes` that can pair with it: those that listed
    /// their Peripherals without it.
    pub fn pairable<'a>(
        &'a self,
        nodes: &'a [BluetoothNode],
    ) -> impl Iterator<Item = &'a BluetoothNode> {
        nodes.iter().filter(|node| {
            node.bluetooth == BluetoothState::Ready
                && !self.paired.iter().any(|paired| paired.node == node.node)
        })
    }
}

impl Peripherals {
    /// The Nodes that said they have no Bluetooth.
    pub fn without_bluetooth(&self) -> impl Iterator<Item = &BluetoothNode> {
        self.nodes
            .iter()
            .filter(|node| node.bluetooth == BluetoothState::NoAdapter)
    }

    /// The Nodes whose Peripherals aren't known, with why: they may have
    /// some paired, or connected.
    pub fn not_shown(&self) -> impl Iterator<Item = (&BluetoothNode, String)> {
        self.nodes
            .iter()
            .filter(|node| node.bluetooth != BluetoothState::NoAdapter)
            .filter_map(|node| Some((node, node.bluetooth.problem()?)))
    }

    /// `node`'s machine name, or its node ID.
    pub fn node_name<'a>(&'a self, node: &'a NodeId) -> &'a str {
        self.nodes
            .iter()
            .find(|known| known.node == *node)
            .and_then(|known| known.name.as_deref())
            .unwrap_or(node.as_str())
    }
}

/// What to do with a Peripheral. Each Node is named by node ID or machine
/// name, and is this one if none.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum PeripheralAction {
    /// Connect it on the Node, disconnecting it from every other.
    Handoff {
        node: Option<String>,
    },
    /// Disconnect it from every other Node, then pair it with the Node,
    /// where it must be in pairing mode, and connect it there.
    Pair {
        node: Option<String>,
    },
    Disconnect {
        node: Option<String>,
    },
    /// Unpair it from the Node.
    Forget {
        node: Option<String>,
    },
    /// Give it `alias`, a name one of its Nodes gives it, on every Node it
    /// is paired with.
    Name {
        alias: String,
    },
}

/// An action that was carried out, with what went wrong along the way
/// without stopping it, as sentences.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Done {
    pub notes: Vec<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_times_read_as_dates() {
        let conflict = |at: &str| Conflict {
            item: ItemId::from_u128(1),
            item_name: "docs".into(),
            path: "notes.txt".into(),
            at: at.into(),
            winner: "ABCDEFG".into(),
            version: "v".into(),
            expires_at: Timestamp::from_millis(0),
        };
        assert_eq!(
            conflict("20261122-205022").local_time(),
            "2026-11-22 20:50:22"
        );
        // Anything else is shown as Syncthing wrote it.
        for odd in ["2026112-205022", "20261122_205022", "2026112x-205022", ""] {
            assert_eq!(conflict(odd).local_time(), odd);
        }
    }
}
