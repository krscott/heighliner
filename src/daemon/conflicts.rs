//! Finding this Node's Data Conflicts, and keeping or swapping them.
//!
//! Conflict copies are moved into the Displaced Versions by a worker, one
//! job at a time and away from the async runtime, so a slow or hung
//! filesystem holds up only the worker. It looks in the directory of each
//! item Syncthing finishes syncing, which is where Syncthing leaves a
//! conflict's copy, and sweeps every Replica after the daemon's first
//! round and then daily, for copies made while it wasn't looking.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc;

use super::Daemon;
use crate::model::{ItemId, Role};
use crate::syncthing::Finished;
use crate::syncthing::conflicts::{self, Collected, DataConflict};
use crate::tailscale::LocalApi;

/// How often every Replica is swept for conflict copies.
pub(super) const SWEEP_EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// A Replica of this Node whose folder is configured.
#[derive(Clone, Debug)]
pub(super) struct Root {
    item: ItemId,
    name: String,
    path: PathBuf,
    role: Role,
}

/// Work for the conflict worker.
#[derive(Debug)]
pub(super) enum Job {
    /// Look in these directories of these Replicas.
    Collect(Vec<(Root, BTreeSet<String>)>),
    /// Look everywhere in these Replicas.
    Sweep(Vec<Root>),
}

impl Job {
    pub(super) fn run(self) {
        match self {
            Self::Collect(dirs) => {
                for (root, dirs) in dirs {
                    for dir in dirs {
                        report(&root, conflicts::collect(&root.path, &dir));
                    }
                }
            }
            Self::Sweep(roots) => {
                for root in roots {
                    report(&root, conflicts::sweep(&root.path));
                }
            }
        }
    }
}

fn report(root: &Root, collected: Result<Collected, conflicts::Error>) {
    let Root { item, name, .. } = root;
    let path = root.path.display();
    match collected {
        // Its Replica State already says so.
        Err(conflicts::Error::NotMounted) => {}
        Err(error) => tracing::warn!(%item, %name, %path, %error, "can't look for data conflicts"),
        Ok(Collected { found, failed }) => {
            for conflict in found {
                let DataConflict {
                    path: file,
                    winner,
                    version,
                    ..
                } = conflict;
                tracing::info!(
                    %item, %name, %path, %file, %winner,
                    "a data conflict: device {winner}'s change won, and the other is kept as .stversions/{version}"
                );
            }
            for error in failed {
                tracing::warn!(%item, %name, %path, %error, "can't keep a data conflict's copy in .stversions");
            }
        }
    }
}

/// Runs the jobs sent to `jobs`, one at a time, until it closes.
pub(super) async fn worker(mut jobs: mpsc::UnboundedReceiver<Job>) {
    while let Some(job) = jobs.recv().await {
        // Each move is one rename, so a job cut short by the daemon
        // stopping leaves no file half moved.
        let _ = tokio::task::spawn_blocking(move || job.run()).await;
    }
}

/// Why a Data Conflict couldn't be kept or swapped.
#[derive(Debug)]
pub enum ActionError {
    /// The Item has no configured Replica on this Node.
    NoReplica(ItemId),
    /// Only a peer's conflicts can be swapped, since a swap is a local
    /// change.
    NotAPeer(ItemId),
    Conflict(conflicts::Error),
}

impl fmt::Display for ActionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoReplica(item) => write!(f, "item {item} has no synced replica on this Node"),
            Self::NotAPeer(item) => write!(
                f,
                "this Node's replica of item {item} doesn't send, so its conflicts can't be swapped"
            ),
            Self::Conflict(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ActionError {}

impl<T: LocalApi + Clone> Daemon<T> {
    /// This Node's Replicas whose folders the last reconcile configured.
    pub(super) fn roots(&self) -> Vec<Root> {
        let Some(reconciled) = self.replicas.lock().unwrap().reconciled.clone() else {
            return Vec::new();
        };
        let Ok(outcomes) = &reconciled.outcomes else {
            return Vec::new();
        };
        let me = self.db.lock().unwrap().node().clone();
        reconciled
            .config
            .replicas_on(&me)
            .into_iter()
            .filter(|local| {
                outcomes
                    .get(&local.item_id)
                    .is_some_and(|outcome| outcome.is_configured())
            })
            .map(|local| Root {
                item: local.item_id,
                name: local.entry.item.name.clone(),
                path: PathBuf::from(local.replica.path().as_str()),
                role: local.role,
            })
            .collect()
    }

    /// The job that looks for conflicts where Syncthing finished `items`.
    pub(super) fn collect_job(&self, items: Vec<Finished>) -> Option<Job> {
        let mut dirs: BTreeMap<ItemId, BTreeSet<String>> = BTreeMap::new();
        for Finished { item, path } in items {
            let dir = path.rsplit_once('/').map_or("", |(dir, _)| dir);
            dirs.entry(item).or_default().insert(dir.to_owned());
        }
        let jobs: Vec<_> = self
            .roots()
            .into_iter()
            .filter_map(|root| Some((dirs.remove(&root.item)?, root)))
            .map(|(dirs, root)| (root, dirs))
            .collect();
        (!jobs.is_empty()).then_some(Job::Collect(jobs))
    }

    /// The job that sweeps every configured Replica.
    pub(super) fn sweep_job(&self) -> Option<Job> {
        let roots = self.roots();
        (!roots.is_empty()).then_some(Job::Sweep(roots))
    }

    /// Moves every conflict copy in this Node's Replicas into their
    /// Displaced Versions now.
    pub async fn sweep_conflicts(&self) {
        if let Some(job) = self.sweep_job() {
            let _ = tokio::task::spawn_blocking(move || job.run()).await;
        }
    }

    /// The Data Conflicts kept on this Node, for each Replica whose folder
    /// is configured, or why they can't be read.
    pub async fn conflicts(&self) -> BTreeMap<ItemId, Result<Vec<DataConflict>, conflicts::Error>> {
        let roots = self.roots();
        tokio::task::spawn_blocking(move || {
            roots
                .into_iter()
                .map(|root| (root.item, conflicts::list(&root.path)))
                .collect()
        })
        .await
        .expect("listing conflicts doesn't panic")
    }

    fn root(&self, item: ItemId) -> Result<Root, ActionError> {
        self.roots()
            .into_iter()
            .find(|root| root.item == item)
            .ok_or(ActionError::NoReplica(item))
    }

    /// Keeps the winning change of the Data Conflict `version` in `item`'s
    /// Replica.
    pub async fn keep_conflict(
        &self,
        item: ItemId,
        version: &str,
    ) -> Result<DataConflict, ActionError> {
        let root = self.root(item)?;
        let version = version.to_owned();
        tokio::task::spawn_blocking(move || conflicts::keep(&root.path, &version))
            .await
            .expect("keeping a conflict doesn't panic")
            .map_err(ActionError::Conflict)
    }

    /// Makes the losing change of the Data Conflict `version` in `item`'s
    /// Replica the file's content again, which Syncthing then sends to the
    /// other Replicas.
    pub async fn swap_conflict(
        &self,
        item: ItemId,
        version: &str,
    ) -> Result<DataConflict, ActionError> {
        let root = self.root(item)?;
        if root.role != Role::Peer {
            return Err(ActionError::NotAPeer(item));
        }
        let path = root.path.clone();
        let version = version.to_owned();
        let conflict = tokio::task::spawn_blocking(move || conflicts::swap(&path, &version))
            .await
            .expect("swapping a conflict doesn't panic")
            .map_err(ActionError::Conflict)?;
        // Syncthing would notice on its own, but later.
        if let Some(driver) = &self.driver
            && let Err(error) = driver.client().scan_path(item, &conflict.path).await
        {
            tracing::warn!(%item, file = %conflict.path, %error, "can't ask Syncthing to scan a swapped file");
        }
        Ok(conflict)
    }
}
