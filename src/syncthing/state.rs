//! Replica State, read from Syncthing.
//!
//! The states come from Syncthing's folder status and connections; its
//! event stream (see `events`) only says when to look again, so a missed
//! event delays a state but never makes it wrong.

use std::collections::BTreeMap;

use serde_json::Value;

use super::{Client, Driver, Error, Outcome};
use crate::config::{Config, LocalReplica};
use crate::model::{ItemId, NodeId, ReplicaState, Role};

impl Client {
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
    /// It has none: it is a source, or the only Replica that sends. (A
    /// mirror with none is an error.)
    None,
    Unreachable,
    /// Some are connected; `all` if every one is.
    Connected {
        all: bool,
    },
}

/// A Replica's state, and whether it is connected to every one of its
/// senders, so that being current means it has what they all have.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observed {
    pub state: ReplicaState,
    pub every_sender: bool,
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
        self.observe_all(config, me, outcomes)
            .await
            .into_iter()
            .map(|(item, observed)| (item, observed.state))
            .collect()
    }

    /// [`Self::states`], with whether each Replica reaches every sender.
    pub async fn observe_all(
        &self,
        config: &Config,
        me: &NodeId,
        outcomes: &BTreeMap<ItemId, Outcome>,
    ) -> BTreeMap<ItemId, Observed> {
        let locals: BTreeMap<ItemId, LocalReplica<'_>> = config
            .replicas_on(me)
            .into_iter()
            .map(|local| (local.item_id, local))
            .collect();
        let mut states = BTreeMap::new();
        let unseen = |state| Observed {
            state,
            every_sender: false,
        };
        for (item, outcome) in outcomes {
            let observed = match locals.get(item) {
                // Removed since the reconcile.
                None if outcome.is_configured() => continue,
                None => unseen(ReplicaState::Error {
                    message: outcome.to_string(),
                }),
                Some(local) if local.paused => unseen(ReplicaState::Paused),
                Some(_) if *outcome != Outcome::Configured => unseen(ReplicaState::Error {
                    message: outcome.to_string(),
                }),
                Some(local) => self
                    .observe(config, me, local)
                    .await
                    .unwrap_or_else(|error| {
                        unseen(ReplicaState::Error {
                            message: format!("can't be read from Syncthing: {error}"),
                        })
                    }),
            };
            states.insert(*item, observed);
        }
        states
    }

    async fn observe(
        &self,
        config: &Config,
        me: &NodeId,
        local: &LocalReplica<'_>,
    ) -> Result<Observed, Error> {
        let client = &self.client;
        let item = local.item_id;
        let direction = &local.entry.item.direction;
        let sending = local
            .entry
            .replicas
            .iter()
            // A Departed Node no longer sends to anyone.
            .filter(|(node, replica)| {
                *node != me
                    && direction.role(node, replica.mirror) != Role::Mirror
                    && !config.departed.contains(*node)
            })
            .map(|(node, _)| node);
        let (mut count, mut connected) = (0, 0);
        for node in sending {
            count += 1;
            if let Some(info) = config.nodes.get(node)
                && client.shares_now(item, info.device_id.as_str()).await?
            {
                connected += 1;
            }
        }
        let senders = match (count, connected) {
            (0, _) => Senders::None,
            (_, 0) => Senders::Unreachable,
            _ => Senders::Connected {
                all: connected == count,
            },
        };
        if senders == Senders::None && local.role == Role::Mirror {
            return Ok(Observed {
                state: ReplicaState::Error {
                    message:
                        "can't be synced: it's a mirror, and no other replica of its item sends"
                            .into(),
                },
                every_sender: false,
            });
        }
        // Read after the senders, so that it includes what Syncthing knows
        // of a sender that has just connected.
        let status: Value = client
            .get(&format!("/rest/db/status?folder={item}"))
            .await?;
        Ok(Observed {
            state: state_of(&status, senders),
            every_sender: matches!(senders, Senders::None | Senders::Connected { all: true }),
        })
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
        Senders::Connected { .. } if status["state"] == "idle" && count("needTotalItems") == 0 => {
            ReplicaState::Current
        }
        Senders::Connected { .. } => ReplicaState::Syncing {
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
        assert_eq!(
            state_of(&idle(), Senders::Connected { all: true }),
            ReplicaState::Current
        );
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
            state_of(&status, Senders::Connected { all: true }),
            ReplicaState::Syncing {
                files_left: 3,
                bytes_left: 1024
            }
        );
        // Still working through what it was told about.
        let mut status = idle();
        status["state"] = json!("syncing");
        assert!(matches!(
            state_of(&status, Senders::Connected { all: true }),
            ReplicaState::Syncing { .. }
        ));
    }

    #[test]
    fn errors_come_first() {
        let mut status = idle();
        status["state"] = json!("error");
        status["error"] = json!("folder marker missing");
        assert!(matches!(
            state_of(&status, Senders::Connected { all: true }),
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
            state_of(&status, Senders::Connected { all: true }),
            ReplicaState::Error { .. }
        ));
    }
}
