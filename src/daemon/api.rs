//! Serving the daemon's API (see [`crate::api`]).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use tokio::sync::Notify;

use super::conflicts::{ActionError, Conflicts};
use super::{RETENTION_WINDOW, RemoteStates, Replicas};
use crate::api::{
    self, AddReplica, Conflict, ItemStatus, Refused, ReplicaStatus, Resolve, SetPaused, Status,
    Track, Tracked,
};
use crate::config::{Config, ConfigDb, EditError};
use crate::hlc::Timestamp;
use crate::model::{AbsPath, Direction, ExcludePattern, Item, ItemId, NodeId, Replica};
use crate::protocol::{self, NodeStates};
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
    pub(super) remote: RemoteStates,
    pub(super) conflicts: Conflicts,
    /// The daemon's user's home, which `~` stands for.
    pub(super) home: Option<PathBuf>,
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
            home: self.home.clone(),
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

/// A refused request.
struct Refusal(StatusCode, String);

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
        };
        Self(status, error.to_string())
    }
}

impl From<EditError> for Refusal {
    fn from(error: EditError) -> Self {
        Self(StatusCode::CONFLICT, error.to_string())
    }
}

type Answer<T> = Result<Json<T>, Refusal>;

fn invalid(error: impl ToString) -> Refusal {
    Refusal(StatusCode::BAD_REQUEST, error.to_string())
}

impl<T: LocalApi + Clone> Api<T> {
    fn me(&self) -> NodeId {
        self.db.lock().unwrap().node().clone()
    }

    /// The Node `name` names, by node ID or machine name; this one if
    /// none. Node IDs the records know need no Tailscale, so a Replica on a
    /// device that has left the tailnet can still be named.
    async fn node(&self, name: Option<&str>) -> Result<NodeId, Refusal> {
        let me = self.me();
        let Some(name) = name else {
            return Ok(me);
        };
        if let Ok(id) = NodeId::new(name) {
            let config = self.db.lock().unwrap().config();
            let known = id == me
                || config.nodes.contains_key(&id)
                || config
                    .items
                    .values()
                    .any(|entry| entry.replicas.contains_key(&id));
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
                format!("no Node is named {name}"),
            )),
            _ => Err(Refusal(
                StatusCode::CONFLICT,
                format!("{} Nodes are named {name}; use a node ID", matching.len()),
            )),
        }
    }

    /// `path` on `node`, where `~` stands for the daemon's user's home on
    /// this Node, written as a Replica's path is.
    fn path(&self, node: &NodeId, path: &str) -> Result<AbsPath, Refusal> {
        let path = if *node == self.me()
            && let Some(rest) = path.strip_prefix('~')
            && (rest.is_empty() || rest.starts_with('/'))
        {
            let home = self
                .home
                .as_ref()
                .and_then(|home| home.to_str())
                .ok_or_else(|| {
                    invalid("this Node's daemon doesn't know its user's home, so ~ can't be used")
                })?;
            format!("{home}{rest}")
        } else if !path.starts_with('/') {
            return Err(invalid(format!(
                "{path} isn't absolute; only a path on this Node may start with ~ instead"
            )));
        } else {
            path.to_owned()
        };
        AbsPath::new(api::normalize(&path)).map_err(invalid)
    }

    /// Applies `edit` to the records, and has it applied to Syncthing.
    fn edit<R>(
        &self,
        edit: impl FnOnce(&mut ConfigDb, Timestamp) -> Result<R, EditError>,
    ) -> Result<R, Refusal> {
        let done = edit(&mut self.db.lock().unwrap(), Timestamp::now())?;
        self.edited.notify_one();
        Ok(done)
    }
}

/// The Item `name` names, by ID or, when only one has it, by name.
fn item(config: &Config, name: &str) -> Result<ItemId, Refusal> {
    if let Ok(id) = name.parse::<ItemId>()
        && config.items.contains_key(&id)
    {
        return Ok(id);
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
    let (config, me) = {
        let db = api.db.lock().unwrap();
        (db.config(), db.node().clone())
    };
    let states = api.replicas.lock().unwrap().states.clone();
    let remote = api.remote.lock().unwrap().clone();
    // Names are a convenience; without Tailscale, IDs do.
    let names: Vec<(NodeId, String)> = match api.tailscale.status().await {
        Ok(tailnet) => std::iter::once(tailnet.me())
            .chain(tailnet.owned_devices())
            .map(|device| (device.id.clone(), device.name.clone()))
            .collect(),
        Err(_) => Vec::new(),
    };
    let name_of = |node: &NodeId| {
        names
            .iter()
            .find(|(id, _)| id == node)
            .map(|(_, name)| name.clone())
    };
    let mut items: Vec<ItemStatus> = config
        .items
        .iter()
        .map(|(id, entry)| ItemStatus {
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
            replicas: entry
                .replicas
                .iter()
                .map(|(node, replica)| {
                    let (state, state_read_at) = if *node == me {
                        (states.get(id).cloned(), None)
                    } else {
                        remote.get(node).map_or((None, None), |(read_at, states)| {
                            (states.get(id).cloned(), Some(*read_at))
                        })
                    };
                    ReplicaStatus {
                        node: node.clone(),
                        node_name: name_of(node),
                        path: replica.path().as_str().to_owned(),
                        role: entry.item.direction.role(node, replica.mirror),
                        paused: replica.paused,
                        departed: config.nodes.get(node).is_some_and(|info| info.departed),
                        state,
                        state_read_at,
                    }
                })
                .collect(),
        })
        .collect();
    items.sort_by(|a, b| (&a.name, a.id).cmp(&(&b.name, b.id)));
    Json(Status {
        node: me,
        items,
        problems: config.problems.iter().map(ToString::to_string).collect(),
    })
}

async fn track<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Json(track): Json<Track>,
) -> Answer<Tracked> {
    let node = api.node(track.node.as_deref()).await?;
    let path = api.path(&node, &track.path)?;
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
    api.edit(|db, now| db.track(now, id, item, &node, Replica::new(path)))?;
    Ok(Json(Tracked { id }))
}

async fn add_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(name): Path<String>,
    Json(add): Json<AddReplica>,
) -> Answer<()> {
    let id = item(&api.db.lock().unwrap().config(), &name)?;
    let node = api.node(add.node.as_deref()).await?;
    let mut replica = Replica::new(api.path(&node, &add.path)?);
    replica.mirror = add.mirror;
    replica.adopt = add.adopt;
    api.edit(|db, now| db.add_replica(now, id, &node, replica))?;
    Ok(Json(()))
}

async fn remove_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path((name, node)): Path<(String, String)>,
) -> Answer<()> {
    let id = item(&api.db.lock().unwrap().config(), &name)?;
    let node = api.node(Some(&node)).await?;
    api.edit(|db, now| db.remove_replica(now, id, &node))?;
    Ok(Json(()))
}

async fn untrack<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(name): Path<String>,
) -> Answer<()> {
    let id = item(&api.db.lock().unwrap().config(), &name)?;
    api.edit(|db, now| db.untrack(now, id))?;
    Ok(Json(()))
}

async fn pause_item<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(name): Path<String>,
    Json(set): Json<SetPaused>,
) -> Answer<()> {
    let id = item(&api.db.lock().unwrap().config(), &name)?;
    api.edit(|db, now| db.update_item(now, id, |item| item.paused = set.paused))?;
    Ok(Json(()))
}

async fn pause_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path((name, node)): Path<(String, String)>,
    Json(set): Json<SetPaused>,
) -> Answer<()> {
    let id = item(&api.db.lock().unwrap().config(), &name)?;
    let node = api.node(Some(&node)).await?;
    api.edit(|db, now| db.update_replica(now, id, &node, |replica| replica.paused = set.paused))?;
    Ok(Json(()))
}

/// This Node's Replicas' states, for the other Nodes (Node protocol).
async fn node_states<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Json<NodeStates> {
    let states = api.replicas.lock().unwrap().states.clone();
    Json(NodeStates { states })
}

async fn conflicts<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Json<api::Conflicts> {
    let config = api.db.lock().unwrap().config();
    let mut listed = api::Conflicts {
        conflicts: Vec::new(),
        errors: Vec::new(),
    };
    for (item, found) in api.conflicts.list().await {
        let entry = config.items.get(&item);
        let item_name = entry.map_or_else(String::new, |entry| entry.item.name.clone());
        match found {
            Ok(found) => {
                let retention = entry
                    .and_then(|entry| entry.item.retention_days)
                    .map_or(RETENTION_WINDOW, |days| {
                        std::time::Duration::from_secs(u64::from(days.get()) * 24 * 60 * 60)
                    });
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
    Json(listed)
}

async fn keep<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Json(resolve): Json<Resolve>,
) -> Answer<()> {
    let id = item(&api.db.lock().unwrap().config(), &resolve.item)?;
    api.conflicts.keep(id, &resolve.version).await?;
    Ok(Json(()))
}

async fn swap<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Json(resolve): Json<Resolve>,
) -> Answer<()> {
    let id = item(&api.db.lock().unwrap().config(), &resolve.item)?;
    api.conflicts.swap(id, &resolve.version).await?;
    Ok(Json(()))
}

async fn leave<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Answer<()> {
    api.edit(|db, now| db.leave(now))?;
    Ok(Json(()))
}
