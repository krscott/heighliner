//! Serving the daemon's API (see [`crate::api`]).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use tokio::sync::Notify;

use super::{Replicas, StateFile};
use crate::api::{
    self, AddReplica, ItemStatus, Refused, ReplicaStatus, SetPaused, Status, Track, Tracked,
};
use crate::config::{Config, ConfigDb, EditError};
use crate::hlc::Timestamp;
use crate::model::{AbsPath, Direction, ExcludePattern, Item, ItemId, NodeId, Replica};
use crate::tailscale::LocalApi;

/// What the API's handlers share.
pub(super) struct Api<T> {
    pub(super) db: Arc<Mutex<ConfigDb>>,
    pub(super) replicas: Arc<Mutex<Replicas>>,
    pub(super) tailscale: T,
    /// Notified after each edit, so that it is applied without waiting
    /// for the next round.
    pub(super) edited: Arc<Notify>,
    pub(super) state: Arc<StateFile>,
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
            state: self.state.clone(),
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
        .with_state(api)
}

/// A refused request.
struct Refusal(StatusCode, String);

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        (self.0, Json(Refused { error: self.1 })).into_response()
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

/// A request body that isn't the JSON a route takes.
fn rejected(rejection: JsonRejection) -> Refusal {
    invalid(rejection.body_text())
}

impl<T: LocalApi + Clone> Api<T> {
    fn me(&self) -> NodeId {
        self.db.lock().unwrap().node().clone()
    }

    /// The Node `name` names, by node ID or machine name; this one if
    /// none. For a Replica that exists, node IDs the records know need no
    /// Tailscale, so one on a device that has left the tailnet can still be
    /// named; one being `placing` must be on one of the owner's devices on
    /// the tailnet now.
    async fn node(&self, name: Option<&str>, placing: bool) -> Result<NodeId, Refusal> {
        let me = self.me();
        let Some(name) = name else {
            return Ok(me);
        };
        if let Ok(id) = NodeId::new(name) {
            let config = self.db.lock().unwrap().config();
            let known = id == me
                || (!placing
                    && (config.nodes.contains_key(&id)
                        || config
                            .items
                            .values()
                            .any(|entry| entry.replicas.contains_key(&id))));
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

    /// Applies `edit` to the records, saves them, and has it applied to
    /// Syncthing. Names in it are resolved under the same lock, so that
    /// they can't change meaning before it is made.
    fn edit<R>(
        &self,
        edit: impl FnOnce(&mut ConfigDb, Timestamp) -> Result<R, Refusal>,
    ) -> Result<R, Refusal> {
        let done = edit(&mut self.db.lock().unwrap(), Timestamp::now())?;
        self.edited.notify_one();
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
    let (config, me) = {
        let db = api.db.lock().unwrap();
        (db.config(), db.node().clone())
    };
    let (states, read_for) = {
        let replicas = api.replicas.lock().unwrap();
        (replicas.states.clone(), replicas.read_for.clone())
    };
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
                .map(|(node, replica)| ReplicaStatus {
                    node: node.clone(),
                    node_name: name_of(node),
                    path: replica.path().as_str().to_owned(),
                    role: entry.item.direction.role(node, replica.mirror),
                    paused: replica.paused,
                    // Only for the path it was read for: one added again
                    // elsewhere starts afresh.
                    state: (*node == me && read_for.get(id) == Some(replica.path()))
                        .then(|| states.get(id).cloned())
                        .flatten(),
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
    track: Result<Json<Track>, JsonRejection>,
) -> Answer<Tracked> {
    let Json(track) = track.map_err(rejected)?;
    let node = api.node(track.node.as_deref(), true).await?;
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
    api.edit(|db, now| Ok(db.track(now, id, item, &node, Replica::new(path))?))?;
    Ok(Json(Tracked { id }))
}

async fn add_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(name): Path<String>,
    add: Result<Json<AddReplica>, JsonRejection>,
) -> Answer<()> {
    let Json(add) = add.map_err(rejected)?;
    let node = api.node(add.node.as_deref(), true).await?;
    let mut replica = Replica::new(api.path(&node, &add.path)?);
    replica.mirror = add.mirror;
    replica.adopt = add.adopt;
    api.edit(|db, now| {
        let id = item(&db.config(), &name)?;
        Ok(db.add_replica(now, id, &node, replica)?)
    })?;
    Ok(Json(()))
}

async fn remove_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path((name, node)): Path<(String, String)>,
) -> Answer<()> {
    let node = api.node(Some(&node), false).await?;
    api.edit(|db, now| {
        let id = item(&db.config(), &name)?;
        Ok(db.remove_replica(now, id, &node)?)
    })?;
    Ok(Json(()))
}

async fn untrack<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(name): Path<String>,
) -> Answer<()> {
    api.edit(|db, now| {
        let id = item(&db.config(), &name)?;
        Ok(db.untrack(now, id)?)
    })?;
    Ok(Json(()))
}

async fn pause_item<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(name): Path<String>,
    set: Result<Json<SetPaused>, JsonRejection>,
) -> Answer<()> {
    let Json(set) = set.map_err(rejected)?;
    api.edit(|db, now| {
        let id = item(&db.config(), &name)?;
        Ok(db.update_item(now, id, |item| item.paused = set.paused)?)
    })?;
    Ok(Json(()))
}

async fn pause_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path((name, node)): Path<(String, String)>,
    set: Result<Json<SetPaused>, JsonRejection>,
) -> Answer<()> {
    let Json(set) = set.map_err(rejected)?;
    let node = api.node(Some(&node), false).await?;
    api.edit(|db, now| {
        let id = item(&db.config(), &name)?;
        Ok(db.update_replica(now, id, &node, |replica| {
            replica.paused = set.paused;
        })?)
    })?;
    Ok(Json(()))
}
