//! Finding this Node's Data Conflicts, expiring them, and keeping or
//! swapping them.
//!
//! Conflict copies are moved into the Displaced Versions by a worker, away
//! from the async runtime, so a slow or hung filesystem holds up only the
//! worker. It looks in the directory of each item Syncthing finishes
//! syncing, which is where Syncthing leaves a conflict's copy. It also
//! sweeps every Replica, expiring old conflicts too, after the daemon's
//! first round, daily, and whenever events may have been missed. Work
//! waiting for it is merged, and when there is too much it becomes one
//! sweep, so it stays bounded.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::mem;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Notify;

use super::{Daemon, RETENTION_WINDOW};
use crate::hlc::Timestamp;
use crate::model::{ItemId, Role};
use crate::syncthing::conflicts::{self, Collected, DataConflict};
use crate::syncthing::{Finished, Outcome, Seen};
use crate::tailscale::LocalApi;

/// How often every Replica is swept for conflict copies.
pub(super) const SWEEP_EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// How many directories may wait to be looked in before a sweep replaces
/// them.
const MAX_WAITING_DIRS: usize = 10_000;

/// A Replica of this Node whose folder is configured.
#[derive(Clone, Debug)]
pub(super) struct Root {
    item: ItemId,
    name: String,
    path: PathBuf,
    retention: Duration,
    /// Whether its folder sends, so that a local change reaches the other
    /// Replicas.
    sends: bool,
}

/// Work waiting for the conflict worker.
#[derive(Debug, Default)]
struct Waiting {
    dirs: BTreeMap<ItemId, (Root, BTreeSet<String>)>,
    count: usize,
    sweep: Option<Vec<Root>>,
}

impl Waiting {
    fn run(self) {
        if let Some(roots) = self.sweep {
            for root in roots {
                report(&root, conflicts::sweep(&root.path));
                let Root { item, name, .. } = &root;
                match conflicts::expire(&root.path, root.retention, Timestamp::now()) {
                    Ok(0) | Err(conflicts::Error::NotMounted) => {}
                    Ok(removed) => {
                        tracing::info!(%item, %name, removed, "expired data conflicts' kept versions");
                    }
                    Err(error) => {
                        tracing::warn!(%item, %name, %error, "can't expire data conflicts")
                    }
                }
            }
        }
        for (root, dirs) in self.dirs.into_values() {
            for dir in dirs {
                report(&root, conflicts::collect(&root.path, &dir));
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
                    "a data conflict: device {winner}'s change won, and the other is kept in .stversions/{version}"
                );
            }
            for error in failed {
                tracing::warn!(%item, %name, %path, %error, "can't keep a data conflict's copy in .stversions");
            }
        }
    }
}

/// The conflict worker's waiting work.
#[derive(Debug, Default)]
pub(super) struct Queue {
    waiting: Mutex<Waiting>,
    ready: Notify,
}

impl Queue {
    fn add_dirs(&self, dirs: Vec<(Root, BTreeSet<String>)>, roots: impl FnOnce() -> Vec<Root>) {
        let mut waiting = self.waiting.lock().unwrap();
        // A sweep looks everywhere anyway.
        if waiting.sweep.is_none() {
            for (root, new) in dirs {
                let (_, known) = waiting
                    .dirs
                    .entry(root.item)
                    .or_insert_with(|| (root, BTreeSet::new()));
                let before = known.len();
                known.extend(new);
                let added = known.len() - before;
                waiting.count += added;
            }
            if waiting.count > MAX_WAITING_DIRS {
                *waiting = Waiting {
                    sweep: Some(roots()),
                    ..Waiting::default()
                };
            }
        }
        drop(waiting);
        self.ready.notify_one();
    }

    fn add_sweep(&self, roots: Vec<Root>) {
        *self.waiting.lock().unwrap() = Waiting {
            sweep: Some(roots),
            ..Waiting::default()
        };
        self.ready.notify_one();
    }

    fn take(&self) -> Waiting {
        mem::take(&mut *self.waiting.lock().unwrap())
    }

    /// Runs the waiting work as it arrives. Runs until it is dropped.
    pub(super) async fn work(&self) {
        loop {
            self.ready.notified().await;
            let waiting = self.take();
            if waiting.sweep.is_some() || !waiting.dirs.is_empty() {
                // Each move is one rename, so work cut short by the daemon
                // stopping leaves no file half moved.
                let _ = tokio::task::spawn_blocking(move || waiting.run()).await;
            }
        }
    }

    /// Runs the waiting work now.
    #[cfg(test)]
    pub(super) fn run_now(&self) {
        self.take().run();
    }
}

/// Why a Data Conflict couldn't be kept or swapped.
#[derive(Debug)]
pub enum ActionError {
    /// The Item has no configured Replica on this Node.
    NoReplica(ItemId),
    /// This Node's Replica doesn't send, so a swap, which is a local
    /// change, wouldn't reach the others.
    DoesntSend(ItemId),
    Conflict(conflicts::Error),
}

impl fmt::Display for ActionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoReplica(item) => write!(f, "item {item} has no synced replica on this Node"),
            Self::DoesntSend(item) => write!(
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
            .filter_map(|local| {
                let outcome = outcomes.get(&local.item_id)?;
                let retention = local
                    .entry
                    .item
                    .retention_days
                    .map_or(RETENTION_WINDOW, |days| {
                        Duration::from_secs(u64::from(days.get()) * 24 * 60 * 60)
                    });
                outcome.is_configured().then(|| Root {
                    item: local.item_id,
                    name: local.entry.item.name.clone(),
                    path: PathBuf::from(local.replica.path().as_str()),
                    retention,
                    // A peer with local changes from when it was a mirror
                    // is still receive-only.
                    sends: local.role == Role::Peer && *outcome == Outcome::Configured,
                })
            })
            .collect()
    }

    /// Queues the work for what the watcher saw.
    pub(super) fn look_for_conflicts(&self, queue: &Queue, seen: Vec<Seen>) {
        let mut dirs: BTreeMap<ItemId, BTreeSet<String>> = BTreeMap::new();
        for seen in seen {
            match seen {
                Seen::Finished(Finished { item, path }) => {
                    let dir = path.rsplit_once('/').map_or("", |(dir, _)| dir);
                    dirs.entry(item).or_default().insert(dir.to_owned());
                }
                Seen::Missed => {
                    self.queue_sweep(queue);
                    return;
                }
            }
        }
        let jobs: Vec<_> = self
            .roots()
            .into_iter()
            .filter_map(|root| Some((dirs.remove(&root.item)?, root)))
            .map(|(dirs, root)| (root, dirs))
            .collect();
        if !jobs.is_empty() {
            queue.add_dirs(jobs, || self.roots());
        }
    }

    /// Queues a sweep of every configured Replica, if there are any.
    /// Returns whether it did.
    pub(super) fn queue_sweep(&self, queue: &Queue) -> bool {
        let roots = self.roots();
        let any = !roots.is_empty();
        if any {
            queue.add_sweep(roots);
        }
        any
    }

    /// Sweeps every configured Replica now, moving conflict copies into the
    /// Displaced Versions and expiring old conflicts.
    pub async fn sweep_conflicts(&self) {
        let queue = Queue::default();
        if self.queue_sweep(&queue) {
            let waiting = queue.take();
            let _ = tokio::task::spawn_blocking(move || waiting.run()).await;
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
        if !root.sends {
            return Err(ActionError::DoesntSend(item));
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
