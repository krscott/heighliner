//! Serving the daemon's API (see [`crate::api`]).

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use tokio::sync::Notify;

use super::conflicts::{ActionError, Conflicts};
use super::{Changes, Remote, Replicas, StateFile, signal};
use crate::api::{
    self, AddReplica, Conflict, ItemStatus, NodeStatus, Reach, Refused, ReplicaStatus, Replication,
    Resolve, SetPaused, Status, Track, Tracked,
};
use crate::bluetooth::Bluetooth;
use crate::config::{Config, ConfigDb, EditError, Problem};
use crate::hlc::Timestamp;
use crate::model::{
    AbsPath, Direction, ExcludePattern, Item, ItemId, NodeId, Replica, ReplicaState,
};
use crate::protocol::{self, NodeState, NodeStates};
use crate::syncthing::conflicts::DataConflict;
use crate::tailscale::LocalApi;

/// What the API's handlers share.
pub(super) struct Api<T> {
    pub(super) db: Arc<Mutex<ConfigDb>>,
    pub(super) replicas: Arc<Mutex<Replicas>>,
    pub(super) tailscale: T,
    /// Notified after each edit, so that it is applied without waiting
    /// for the next round.
    pub(super) edited: Arc<Notify>,
    pub(super) remote: Arc<Mutex<Remote>>,
    pub(super) conflicts: Conflicts,
    pub(super) state: Arc<StateFile>,
    /// The daemon's user's home, which `~` in a path on this Node stands
    /// for.
    pub(super) home: Option<AbsPath>,
    /// The port the daemons serve on.
    pub(super) port: u16,
    pub(super) changes: Changes,
    pub(super) bluetooth: Option<Bluetooth>,
}

impl<T: Clone> Clone for Api<T> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            replicas: self.replicas.clone(),
            tailscale: self.tailscale.clone(),
            edited: self.edited.clone(),
            remote: self.remote.clone(),
            conflicts: self.conflicts.clone(),
            state: self.state.clone(),
            home: self.home.clone(),
            port: self.port,
            changes: self.changes.clone(),
            bluetooth: self.bluetooth.clone(),
        }
    }
}

pub(super) fn router<T: LocalApi + Clone>(api: Api<T>) -> Router {
    Router::new()
        .route(api::STATUS, get(status::<T>))
        .route(api::ITEMS, post(track::<T>))
        .route(
            "/api/v1/items/{item}",
            patch(pause_item::<T>).delete(untrack::<T>),
        )
        .route("/api/v1/items/{item}/replicas", post(add_replica::<T>))
        .route(
            "/api/v1/items/{item}/replicas/{node}",
            patch(pause_replica::<T>).delete(remove_replica::<T>),
        )
        .route(api::CONFLICTS, get(conflicts::<T>))
        .route(api::KEEP, post(keep::<T>))
        .route(api::SWAP, post(swap::<T>))
        .route(api::LEAVE, post(leave::<T>))
        .route(protocol::STATES_PATH, get(node_states::<T>))
        .with_state(api)
}

/// A refused request: its status, and why, as a sentence.
pub(super) struct Refusal(pub(super) StatusCode, pub(super) String);

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        (self.0, Json(Refused { error: self.1 })).into_response()
    }
}

impl From<ActionError> for Refusal {
    fn from(error: ActionError) -> Self {
        let status = match &error {
            ActionError::NoReplica(_) => StatusCode::NOT_FOUND,
            ActionError::DoesntSend(_) => StatusCode::CONFLICT,
            ActionError::Syncthing(_) => StatusCode::SERVICE_UNAVAILABLE,
            ActionError::Conflict(crate::syncthing::conflicts::Error::NotFound(_)) => {
                StatusCode::NOT_FOUND
            }
            ActionError::Conflict(_) => StatusCode::CONFLICT,
            ActionError::Busy | ActionError::Hung => StatusCode::SERVICE_UNAVAILABLE,
        };
        Self(status, error.to_string())
    }
}

impl From<EditError> for Refusal {
    fn from(error: EditError) -> Self {
        Self(StatusCode::CONFLICT, error.to_string())
    }
}

pub(super) type Answer<T> = Result<Json<T>, Refusal>;

pub(super) fn invalid(error: impl ToString) -> Refusal {
    Refusal(StatusCode::BAD_REQUEST, error.to_string())
}

/// A request body that isn't the JSON a route takes.
pub(super) fn rejected(rejection: JsonRejection) -> Refusal {
    invalid(rejection.body_text())
}

impl<T: LocalApi + Clone> Api<T> {
    fn me(&self) -> NodeId {
        self.db.lock().unwrap().node().clone()
    }

    /// This Node's view of the Fleet.
    pub(super) async fn status(&self) -> Status {
        // Names are a convenience; without Tailscale, IDs do.
        let devices: BTreeMap<NodeId, (String, Option<IpAddr>)> =
            match self.tailscale.status().await {
                Ok(tailnet) => std::iter::once(tailnet.me())
                    .chain(tailnet.owned_devices())
                    .map(|device| {
                        let ip = device
                            .ips
                            .iter()
                            .find(|ip| ip.is_ipv4())
                            .or(device.ips.first())
                            .copied();
                        (device.id.clone(), (device.name.clone(), ip))
                    })
                    .collect(),
                Err(_) => BTreeMap::new(),
            };
        let name_of = |node: &NodeId| devices.get(node).map(|(name, _)| name.clone());
        let (config, me) = {
            let db = self.db.lock().unwrap();
            (db.config(), db.node().clone())
        };
        let remote = self.remote.lock().unwrap().clone();
        let replicas = self.replicas.lock().unwrap();
        let mut items: Vec<ItemStatus> = config
            .items
            .iter()
            .map(|(id, entry)| {
                let shown: Vec<(ReplicaStatus, bool)> = entry
                    .replicas
                    .iter()
                    .map(|(node, replica)| {
                        let path = replica.path();
                        // Only for the path it was read for: one added again
                        // elsewhere starts afresh.
                        // Against the senders it has now, which may have
                        // changed since it was read.
                        let senders = config.senders_to(entry, node);
                        let reaches_all =
                            |connected: &BTreeSet<NodeId>| senders.is_subset(connected);
                        let (state, state_read_at, confirmed_at, every_sender, live) =
                            if *node == me {
                                let read = replicas.read_for.get(id) == Some(path);
                                let state =
                                    read.then(|| replicas.states.get(id).cloned()).flatten();
                                let every_sender = read && replicas.connected.get(id).is_some_and(reaches_all);
                                (state, None, replicas.confirmed(id, path), every_sender, true)
                            } else {
                                match remote.states.get(node) {
                                    Some((read_at, states)) => {
                                        let read = states
                                            .get(id)
                                            .filter(|state| state.path == path.as_str());
                                        (
                                            read.map(|read| read.state.clone()),
                                            Some(*read_at),
                                            read.and_then(|read| read.confirmed_at),
                                            read.is_some_and(|read| reaches_all(&read.connected)),
                                            remote.live.contains(node),
                                        )
                                    }
                                    None => (None, None, None, false, false),
                                }
                            };
                        let current_now =
                            live && every_sender && state == Some(ReplicaState::Current);
                        let status = ReplicaStatus {
                            node: node.clone(),
                            node_name: name_of(node),
                            path: path.as_str().to_owned(),
                            role: entry.item.direction.role(node, replica.mirror),
                            paused: replica.paused,
                            departed: config.departed.contains(node),
                            state,
                            state_read_at,
                            confirmed_at,
                            every_sender,
                        };
                        (status, current_now)
                    })
                    .collect();
                let counted: Vec<&(ReplicaStatus, bool)> =
                    shown.iter().filter(|(replica, _)| !replica.departed).collect();
                let replication = if !counted.is_empty() && counted.iter().all(|(_, now)| *now) {
                    Replication::Full
                } else {
                    let confirmed: Option<Vec<Timestamp>> = counted
                        .iter()
                        .map(|(replica, _)| replica.confirmed_at)
                        .collect();
                    let oldest = confirmed.and_then(|times| times.into_iter().min());
                    Replication::Unconfirmed { oldest }
                };
                ItemStatus {
                    id: *id,
                    name: entry.item.name.clone(),
                    source: match &entry.item.direction {
                        Direction::OneWay { source } => Some(source.clone()),
                        Direction::TwoWay => None,
                    },
                    excludes: entry
                        .item
                        .excludes
                        .iter()
                        .map(|pattern| pattern.as_str().to_owned())
                        .collect(),
                    paused: entry.item.paused,
                    retention_days: entry.item.retention_days,
                    resurrected: config.problems.iter().any(|problem| {
                        matches!(problem, Problem::Resurrected { item } if item == id)
                    }),
                    duplicate_name: config.problems.iter().any(|problem| {
                        matches!(problem, Problem::DuplicateName { items, .. } if items.contains(id))
                    }),
                    replication,
                    replicas: shown.into_iter().map(|(replica, _)| replica).collect(),
                }
            })
            .collect();
        drop(replicas);
        items.sort_by(|a, b| (&a.name, a.id).cmp(&(&b.name, b.id)));
        let named: BTreeSet<&NodeId> = config
            .nodes
            .keys()
            .chain(
                config
                    .items
                    .values()
                    .flat_map(|entry| entry.replicas.keys()),
            )
            .chain(&config.departed)
            .chain(remote.incompatible.keys())
            // A Node that answered, even one that hasn't published its
            // record yet or holds no Replica.
            .chain(remote.states.keys())
            .chain([&me])
            .collect();
        let mut nodes: Vec<NodeStatus> = named
            .into_iter()
            .map(|node| {
                let read_at = remote.states.get(node).map(|(read_at, _)| *read_at);
                let reach = if *node == me {
                    Reach::Here
                } else if let Some(protocol) = remote.incompatible.get(node) {
                    Reach::Incompatible {
                        protocol: *protocol,
                    }
                } else if let (true, Some(read_at)) = (remote.live.contains(node), read_at) {
                    Reach::Live { read_at }
                } else {
                    Reach::Unreachable { read_at }
                };
                NodeStatus {
                    node: node.clone(),
                    name: name_of(node),
                    ip: devices.get(node).and_then(|(_, ip)| *ip),
                    departed: config.departed.contains(node),
                    reach,
                }
            })
            .collect();
        nodes.sort_by(|a, b| {
            let key = |node: &NodeStatus| {
                (
                    node.reach != Reach::Here,
                    node.name.clone(),
                    node.node.clone(),
                )
            };
            key(a).cmp(&key(b))
        });
        Status {
            node: me,
            nodes,
            items,
            problems: config.problems.iter().map(ToString::to_string).collect(),
        }
    }

    /// This Node's Data Conflicts.
    pub(super) async fn list_conflicts(&self) -> api::Conflicts {
        let config = self.db.lock().unwrap().config();
        let mut listed = api::Conflicts {
            conflicts: Vec::new(),
            errors: Vec::new(),
        };
        for (item, found) in self.conflicts.list().await {
            let entry = config.items.get(&item);
            let item_name = entry.map_or_else(String::new, |entry| entry.item.name.clone());
            match found {
                Ok(found) => {
                    let default = self.conflicts.retention;
                    let retention = entry.map_or(default, |entry| entry.item.retention(default));
                    listed.conflicts.extend(found.into_iter().map(|conflict| {
                        let DataConflict {
                            path,
                            at,
                            winner,
                            version,
                            moved_at,
                        } = conflict;
                        Conflict {
                            item,
                            item_name: item_name.clone(),
                            path,
                            at,
                            winner,
                            version,
                            expires_at: moved_at.saturating_add(retention),
                        }
                    }));
                }
                Err(error) => listed.errors.push(format!("{item_name} ({item}): {error}")),
            }
        }
        listed
    }

    /// The Node `name` names, by node ID or machine name; this one if
    /// none. For a Replica that exists, node IDs the records know need no
    /// Tailscale, so one on a device that has left the tailnet can still be
    /// named; one being `placing` must be on one of the owner's devices on
    /// the tailnet now.
    pub(super) async fn node(&self, name: Option<&str>, placing: bool) -> Result<NodeId, Refusal> {
        let node = self.find_node(name, placing).await?;
        if placing && self.db.lock().unwrap().config().departed.contains(&node) {
            return Err(Refusal(
                StatusCode::CONFLICT,
                format!("{node} has left the Fleet, so nothing can be added there"),
            ));
        }
        Ok(node)
    }

    async fn find_node(&self, name: Option<&str>, placing: bool) -> Result<NodeId, Refusal> {
        let me = self.me();
        let Some(name) = name else {
            return Ok(me);
        };
        if let Ok(id) = NodeId::new(name) {
            let known = id == me || (!placing && self.db.lock().unwrap().knows_node(&id));
            if known {
                return Ok(id);
            }
        }
        let tailnet = self.tailscale.status().await.map_err(|error| {
            Refusal(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("can't ask Tailscale for the Nodes' names: {error}"),
            )
        })?;
        let devices = std::iter::once(tailnet.me()).chain(tailnet.owned_devices());
        let matching: Vec<NodeId> = devices
            .filter(|device| device.name == name || device.id.as_str() == name)
            .map(|device| device.id.clone())
            .collect();
        match &matching[..] {
            [node] => Ok(node.clone()),
            [] => Err(Refusal(
                StatusCode::NOT_FOUND,
                format!("no Node of the owner's on the tailnet is named {name}"),
            )),
            _ => Err(Refusal(
                StatusCode::CONFLICT,
                format!("{} Nodes are named {name}; use a node ID", matching.len()),
            )),
        }
    }

    /// `path` on `node`, where `~` stands for the daemon's user's home on
    /// that Node, written as a Replica's path is.
    fn path(&self, node: &NodeId, path: &str) -> Result<AbsPath, Refusal> {
        let path = if let Some(rest) = path.strip_prefix('~')
            && (rest.is_empty() || rest.starts_with('/'))
        {
            let home = if *node == self.me() {
                self.home.clone().ok_or_else(|| {
                    invalid("this Node's daemon doesn't know its user's home, so ~ can't be used")
                })?
            } else {
                let nodes = self.db.lock().unwrap().config().nodes;
                nodes.get(node).and_then(|info| info.home.clone()).ok_or_else(|| {
                    invalid("that Node hasn't shared its user's home yet, so ~ can't be used there; give an absolute path")
                })?
            };
            format!("{}{rest}", home.as_str())
        } else if !path.starts_with('/') {
            return Err(invalid(format!(
                "{path} isn't absolute; give an absolute path, or one that starts with ~/"
            )));
        } else {
            path.to_owned()
        };
        let path = api::normalize(&path);
        if path.split('/').any(|part| part == "..") {
            return Err(invalid(format!(
                "{path} has a .. in it, which only its Node can resolve; give the path without it"
            )));
        }
        AbsPath::new(path).map_err(invalid)
    }

    /// Applies `edit` to the records, saves them, and has it applied to
    /// Syncthing. Names in it are resolved under the same lock, so that
    /// they can't change meaning before it is made.
    fn edit<R>(
        &self,
        edit: impl FnOnce(&mut ConfigDb, Timestamp) -> Result<R, Refusal>,
    ) -> Result<R, Refusal> {
        let done = edit(&mut self.db.lock().unwrap(), Timestamp::now())?;
        self.edited.notify_one();
        signal(&self.changes);
        self.state.save().map_err(|error| {
            Refusal(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("the edit was made, but saving it failed: {error}"),
            )
        })?;
        Ok(done)
    }
}

/// The Item `name` names, by ID or, when only one has it, by name. An ID
/// is only ever an ID, so retrying an edit by ID can't reach another Item
/// named with it.
fn item(config: &Config, name: &str) -> Result<ItemId, Refusal> {
    if let Ok(id) = name.parse::<ItemId>() {
        return if config.items.contains_key(&id) {
            Ok(id)
        } else {
            Err(Refusal(StatusCode::NOT_FOUND, format!("no item {id}")))
        };
    }
    let matching: Vec<ItemId> = config
        .items
        .iter()
        .filter(|(_, entry)| entry.item.name == name)
        .map(|(id, _)| *id)
        .collect();
    match &matching[..] {
        [id] => Ok(*id),
        [] => Err(Refusal(
            StatusCode::NOT_FOUND,
            format!("no item is named {name}"),
        )),
        _ => {
            let ids: Vec<String> = matching.iter().map(ItemId::to_string).collect();
            Err(Refusal(
                StatusCode::CONFLICT,
                format!(
                    "{} items are named {name}; use one of their IDs: {}",
                    ids.len(),
                    ids.join(", ")
                ),
            ))
        }
    }
}

async fn status<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Json<Status> {
    Json(api.status().await)
}

/// The edits the API and the web UI make. Each names its Item by ID or
/// name and its Node by node ID or machine name, as [`api`] describes.
impl<T: LocalApi + Clone> Api<T> {
    pub(super) async fn track(&self, track: Track) -> Result<ItemId, Refusal> {
        let node = self.node(track.node.as_deref(), true).await?;
        let path = self.path(&node, &track.path)?;
        let name = match track.name {
            Some(name) if name.trim().is_empty() => {
                return Err(invalid("an item's name can't be blank"));
            }
            Some(name) => name,
            None => path
                .as_str()
                .rsplit('/')
                .find(|part| !part.is_empty())
                .unwrap_or("/")
                .to_owned(),
        };
        let direction = if track.one_way {
            Direction::OneWay {
                source: node.clone(),
            }
        } else {
            Direction::TwoWay
        };
        let mut item = Item::new(name, direction);
        item.excludes = track
            .excludes
            .into_iter()
            .map(ExcludePattern::new)
            .collect::<Result<_, _>>()
            .map_err(invalid)?;
        item.retention_days = track.retention_days;
        let id = ItemId::generate();
        self.edit(|db, now| Ok(db.track(now, id, item, &node, Replica::new(path))?))?;
        Ok(id)
    }

    pub(super) async fn add_replica(&self, name: &str, add: AddReplica) -> Result<(), Refusal> {
        let node = self.node(add.node.as_deref(), true).await?;
        let mut replica = Replica::new(self.path(&node, &add.path)?);
        replica.mirror = add.mirror;
        replica.adopt = add.adopt;
        self.edit(|db, now| {
            let id = item(&db.config(), name)?;
            Ok(db.add_replica(now, id, &node, replica)?)
        })
    }

    pub(super) async fn remove_replica(&self, name: &str, node: &str) -> Result<(), Refusal> {
        let node = self.node(Some(node), false).await?;
        self.edit(|db, now| {
            let id = item(&db.config(), name)?;
            Ok(db.remove_replica(now, id, &node)?)
        })
    }

    pub(super) fn untrack(&self, name: &str) -> Result<(), Refusal> {
        self.edit(|db, now| {
            let id = item(&db.config(), name)?;
            Ok(db.untrack(now, id)?)
        })
    }

    pub(super) fn pause_item(&self, name: &str, paused: bool) -> Result<(), Refusal> {
        self.edit(|db, now| {
            let id = item(&db.config(), name)?;
            Ok(db.update_item(now, id, |item| item.paused = paused)?)
        })
    }

    pub(super) async fn pause_replica(
        &self,
        name: &str,
        node: &str,
        paused: bool,
    ) -> Result<(), Refusal> {
        let node = self.node(Some(node), false).await?;
        self.edit(|db, now| {
            let id = item(&db.config(), name)?;
            Ok(db.update_replica(now, id, &node, |replica| {
                replica.paused = paused;
            })?)
        })
    }

    pub(super) async fn keep(&self, resolve: &Resolve) -> Result<(), Refusal> {
        let id = item(&self.db.lock().unwrap().config(), &resolve.item)?;
        self.conflicts.keep(id, &resolve.version).await?;
        Ok(())
    }

    pub(super) async fn swap(&self, resolve: &Resolve) -> Result<(), Refusal> {
        let id = item(&self.db.lock().unwrap().config(), &resolve.item)?;
        self.conflicts.swap(id, &resolve.version).await?;
        Ok(())
    }

    pub(super) fn leave(&self, leave: &api::Leave) -> Result<(), Refusal> {
        if !leave.confirm {
            return Err(invalid("leaving can't be undone; confirm it"));
        }
        self.edit(|db, now| Ok(db.leave(now)?))
    }
}

async fn track<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    track: Result<Json<Track>, JsonRejection>,
) -> Answer<Tracked> {
    let Json(track) = track.map_err(rejected)?;
    let id = api.track(track).await?;
    Ok(Json(Tracked { id }))
}

async fn add_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(name): Path<String>,
    add: Result<Json<AddReplica>, JsonRejection>,
) -> Answer<()> {
    let Json(add) = add.map_err(rejected)?;
    Ok(Json(api.add_replica(&name, add).await?))
}

async fn remove_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path((name, node)): Path<(String, String)>,
) -> Answer<()> {
    Ok(Json(api.remove_replica(&name, &node).await?))
}

async fn untrack<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(name): Path<String>,
) -> Answer<()> {
    Ok(Json(api.untrack(&name)?))
}

async fn pause_item<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(name): Path<String>,
    set: Result<Json<SetPaused>, JsonRejection>,
) -> Answer<()> {
    let Json(set) = set.map_err(rejected)?;
    Ok(Json(api.pause_item(&name, set.paused)?))
}

async fn pause_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path((name, node)): Path<(String, String)>,
    set: Result<Json<SetPaused>, JsonRejection>,
) -> Answer<()> {
    let Json(set) = set.map_err(rejected)?;
    Ok(Json(api.pause_replica(&name, &node, set.paused).await?))
}

/// This Node's Replicas' states, for the other Nodes (Node protocol).
async fn node_states<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Json<NodeStates> {
    let replicas = api.replicas.lock().unwrap();
    let states = replicas
        .states
        .iter()
        .filter_map(|(item, state)| {
            let path = replicas.read_for.get(item)?;
            let read = NodeState {
                path: path.as_str().to_owned(),
                state: state.clone(),
                confirmed_at: replicas.confirmed(item, path),
                connected: replicas.connected.get(item).cloned().unwrap_or_default(),
            };
            Some((*item, read))
        })
        .collect();
    Json(NodeStates { states })
}

async fn conflicts<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Json<api::Conflicts> {
    Json(api.list_conflicts().await)
}

async fn keep<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    resolve: Result<Json<Resolve>, JsonRejection>,
) -> Answer<()> {
    let Json(resolve) = resolve.map_err(rejected)?;
    Ok(Json(api.keep(&resolve).await?))
}

async fn swap<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    resolve: Result<Json<Resolve>, JsonRejection>,
) -> Answer<()> {
    let Json(resolve) = resolve.map_err(rejected)?;
    Ok(Json(api.swap(&resolve).await?))
}

async fn leave<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    leave: Result<Json<api::Leave>, JsonRejection>,
) -> Answer<()> {
    let Json(leave) = leave.map_err(rejected)?;
    Ok(Json(api.leave(&leave)?))
}
