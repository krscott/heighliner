//! Replica State, read from Syncthing.
//!
//! Syncthing's event stream only says when to look again; the states come
//! from its folder status and connections, so a missed event delays a
//! state but never makes it wrong.

use std::collections::BTreeMap;
use std::time::Duration;

use hyper::Method;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Notify;

use super::{Client, Driver, Error, Outcome};
use crate::config::{Config, LocalReplica};
use crate::model::{ItemId, NodeId, ReplicaState, Role};

/// The events after which a Replica's state may have changed.
const EVENTS: &str = "StateChanged,FolderSummary,FolderCompletion,FolderErrors,FolderPaused,FolderResumed,DeviceConnected,DeviceDisconnected,DevicePaused,DeviceResumed,ClusterConfigReceived";

/// How long Syncthing holds a request for events open when none happen.
const EVENTS_TIMEOUT: Duration = Duration::from_secs(60);

/// How long to wait before asking for events again after a failure.
const EVENTS_RETRY: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
struct Event {
    id: u64,
}

impl Client {
    /// The IDs of the events after `since`, waiting up to `timeout` for
    /// one.
    pub(super) async fn events(&self, since: u64, timeout: Duration) -> Result<Vec<u64>, Error> {
        let path = format!(
            "/rest/events?events={EVENTS}&since={since}&timeout={}",
            timeout.as_secs()
        );
        let body = self
            .call_within(timeout + super::TIMEOUT, Method::GET, &path, None)
            .await?;
        let events: Vec<Event> = serde_json::from_slice(&body).map_err(|source| Error::Decode {
            path: path.clone(),
            source,
        })?;
        Ok(events.into_iter().map(|event| event.id).collect())
    }

    /// The ID of the latest event, or 0 if there is none yet. Syncthing
    /// keeps events for a filter only from when it is first asked for them.
    pub(super) async fn latest_event(&self) -> Result<u64, Error> {
        let path = format!("/rest/events?events={EVENTS}&since=0&limit=1&timeout=0");
        let events: Vec<Event> = self.get(&path).await?;
        Ok(events.iter().map(|event| event.id).max().unwrap_or(0))
    }

    /// Whether `device` is connected and shares `folder`, as far as
    /// Syncthing has heard from it.
    async fn shares_now(&self, folder: ItemId, device: &str) -> Result<bool, Error> {
        let path = format!("/rest/db/completion?folder={folder}&device={device}");
        let completion: Value = self.get(&path).await?;
        Ok(completion["remoteState"] == "valid")
    }
}

/// Whether a Replica's senders can be reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Senders {
    /// It has none: it is a source, or the only Replica that sends.
    None,
    Unreachable,
    Connected,
}

impl Driver {
    /// The states of the Replicas in `outcomes`, the last reconcile's, as
    /// Syncthing has them now, for the Node `me`. A Replica that is behind
    /// has no `last_current` here; the caller knows when it last was.
    pub async fn states(
        &self,
        config: &Config,
        me: &NodeId,
        outcomes: &BTreeMap<ItemId, Outcome>,
    ) -> BTreeMap<ItemId, ReplicaState> {
        let locals: BTreeMap<ItemId, LocalReplica<'_>> = config
            .replicas_on(me)
            .into_iter()
            .map(|local| (local.item_id, local))
            .collect();
        let mut states = BTreeMap::new();
        for (item, outcome) in outcomes {
            let state = match locals.get(item) {
                // Removed since the reconcile.
                None if outcome.is_configured() => continue,
                None => ReplicaState::Error {
                    message: outcome.to_string(),
                },
                Some(local) if local.paused => ReplicaState::Paused,
                Some(_) if *outcome != Outcome::Configured => ReplicaState::Error {
                    message: outcome.to_string(),
                },
                Some(local) => self
                    .observe(config, me, local)
                    .await
                    .unwrap_or_else(|error| ReplicaState::Error {
                        message: format!("can't be read from Syncthing: {error}"),
                    }),
            };
            states.insert(*item, state);
        }
        states
    }

    async fn observe(
        &self,
        config: &Config,
        me: &NodeId,
        local: &LocalReplica<'_>,
    ) -> Result<ReplicaState, Error> {
        let client = &self.client;
        let item = local.item_id;
        let status: Value = client
            .get(&format!("/rest/db/status?folder={item}"))
            .await?;
        let direction = &local.entry.item.direction;
        let sending = local
            .entry
            .replicas
            .iter()
            .filter(|(node, replica)| {
                *node != me && direction.role(node, replica.mirror) != Role::Mirror
            })
            .map(|(node, _)| node);
        let mut senders = Senders::None;
        for node in sending {
            senders = Senders::Unreachable;
            if let Some(info) = config.nodes.get(node)
                && client.shares_now(item, info.device_id.as_str()).await?
            {
                senders = Senders::Connected;
                break;
            }
        }
        Ok(state_of(&status, senders))
    }

    /// Notifies `changed` whenever Syncthing reports an event after which
    /// a Replica's state may have changed, and once when Syncthing stops
    /// answering. Runs until it is dropped.
    pub async fn watch(&self, changed: &Notify) {
        let mut since = self.client.latest_event().await.unwrap_or(0);
        let mut failing = false;
        loop {
            match self.client.events(since, EVENTS_TIMEOUT).await {
                Ok(ids) if ids.is_empty() => {
                    // A restarted Syncthing numbers its events afresh.
                    if let Ok(latest) = self.client.latest_event().await
                        && latest < since
                    {
                        since = latest;
                        changed.notify_one();
                    }
                }
                Ok(ids) => {
                    since = ids.into_iter().max().unwrap_or(since);
                    failing = false;
                    changed.notify_one();
                }
                Err(error) => {
                    if !failing {
                        tracing::debug!(%error, "can't read Syncthing's events");
                        failing = true;
                        changed.notify_one();
                    }
                    tokio::time::sleep(EVENTS_RETRY).await;
                }
            }
        }
    }
}

/// A configured, running folder's state, from its Syncthing status.
fn state_of(status: &Value, senders: Senders) -> ReplicaState {
    let count = |key: &str| status[key].as_u64().unwrap_or(0);
    let error = status["error"].as_str().unwrap_or_default();
    if status["state"] == "error" || !error.is_empty() {
        return ReplicaState::Error {
            message: format!("can't be synced: Syncthing reports: {error}"),
        };
    }
    let failed = count("pullErrors");
    if failed > 0 {
        return ReplicaState::Error {
            message: format!("can't sync {failed} items; Syncthing lists them as failed items"),
        };
    }
    let changed = count("receiveOnlyTotalItems");
    if changed > 0 {
        return ReplicaState::Error {
            message: format!(
                "has {changed} items changed locally, which a mirror doesn't send and Syncthing doesn't overwrite; undo the changes (Syncthing lists them as locally changed items)"
            ),
        };
    }
    match senders {
        Senders::None => ReplicaState::Current,
        Senders::Unreachable => ReplicaState::Behind { last_current: None },
        Senders::Connected if status["state"] == "idle" && count("needTotalItems") == 0 => {
            ReplicaState::Current
        }
        Senders::Connected => ReplicaState::Syncing {
            files_left: count("needTotalItems"),
            bytes_left: count("needBytes"),
        },
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn idle() -> Value {
        json!({ "state": "idle", "error": "", "needTotalItems": 0, "needBytes": 0 })
    }

    #[test]
    fn a_folder_with_nothing_to_receive_is_current() {
        assert_eq!(state_of(&idle(), Senders::None), ReplicaState::Current);
        assert_eq!(state_of(&idle(), Senders::Connected), ReplicaState::Current);
    }

    #[test]
    fn a_folder_without_reachable_senders_is_behind() {
        assert_eq!(
            state_of(&idle(), Senders::Unreachable),
            ReplicaState::Behind { last_current: None }
        );
    }

    #[test]
    fn a_folder_with_items_to_receive_is_syncing() {
        let mut status = idle();
        status["needTotalItems"] = json!(3);
        status["needBytes"] = json!(1024);
        assert_eq!(
            state_of(&status, Senders::Connected),
            ReplicaState::Syncing {
                files_left: 3,
                bytes_left: 1024
            }
        );
        // Still working through what it was told about.
        let mut status = idle();
        status["state"] = json!("syncing");
        assert!(matches!(
            state_of(&status, Senders::Connected),
            ReplicaState::Syncing { .. }
        ));
    }

    #[test]
    fn errors_come_first() {
        let mut status = idle();
        status["state"] = json!("error");
        status["error"] = json!("folder marker missing");
        assert!(matches!(
            state_of(&status, Senders::Connected),
            ReplicaState::Error { message } if message.contains("folder marker missing")
        ));
        let mut status = idle();
        status["pullErrors"] = json!(2);
        assert!(matches!(
            state_of(&status, Senders::None),
            ReplicaState::Error { .. }
        ));
        let mut status = idle();
        status["receiveOnlyTotalItems"] = json!(1);
        assert!(matches!(
            state_of(&status, Senders::Connected),
            ReplicaState::Error { .. }
        ));
    }
}
