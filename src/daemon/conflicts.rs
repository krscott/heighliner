//! Finding this Node's Data Conflicts, expiring them, and keeping or
//! swapping them.
//!
//! Conflict copies are moved into the store by a worker, away from the
//! async runtime, so a slow or hung filesystem holds up only the worker.
//! It looks in the directory of each item Syncthing finishes syncing,
//! which is where Syncthing leaves a conflict's copy. It also sweeps every
//! Replica, expiring old conflicts too, after the daemon's first round,
//! daily, and whenever events may have been missed. Waiting work names
//! Items, not paths, so each run uses the Replicas' current paths and
//! Retention Windows; it is merged, and when there is too much it becomes
//! one sweep, so it stays bounded.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::mem;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use super::{Daemon, RETENTION_WINDOW, Replicas};
use crate::hlc::Timestamp;
use crate::model::{ItemId, NodeId};
use crate::syncthing::conflicts::{self, Collected, DataConflict};
use crate::syncthing::{self, Driver, Finished, Seen};
use crate::tailscale::LocalApi;

/// How often every Replica is swept for conflict copies.
pub(super) const SWEEP_EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// How many directories may wait to be looked in before a sweep replaces
/// them.
const MAX_WAITING_DIRS: usize = 10_000;

/// A Replica of this Node whose folder is configured.
#[derive(Clone, Debug)]
struct Root {
    item: ItemId,
    name: String,
    path: PathBuf,
    retention: Duration,
}

/// This Node's (`me`'s) Replicas whose folders the last reconcile
/// configured.
fn roots(replicas: &Mutex<Replicas>, me: &NodeId) -> Vec<Root> {
    let Some(reconciled) = replicas.lock().unwrap().reconciled.clone() else {
        return Vec::new();
    };
    let Ok(outcomes) = &reconciled.outcomes else {
        return Vec::new();
    };
    reconciled
        .config
        .replicas_on(me)
        .into_iter()
        .filter(|local| {
            outcomes
                .get(&local.item_id)
                .is_some_and(|o| o.is_configured())
        })
        .map(|local| Root {
            item: local.item_id,
            name: local.entry.item.name.clone(),
            path: PathBuf::from(local.replica.path().as_str()),
            retention: local
                .entry
                .item
                .retention_days
                .map_or(RETENTION_WINDOW, |days| {
                    Duration::from_secs(u64::from(days.get()) * 24 * 60 * 60)
                }),
        })
        .collect()
}

/// Work waiting for the conflict worker.
#[derive(Debug, Default)]
struct Waiting {
    dirs: BTreeMap<ItemId, BTreeSet<String>>,
    count: usize,
    sweep: bool,
}

impl Waiting {
    fn is_empty(&self) -> bool {
        !self.sweep && self.dirs.is_empty()
    }

    fn swept() -> Self {
        Self {
            sweep: true,
            ..Self::default()
        }
    }

    fn add(&mut self, item: ItemId, dir: String) {
        // A sweep looks everywhere anyway.
        if self.sweep {
            return;
        }
        if self.dirs.entry(item).or_default().insert(dir) {
            self.count += 1;
        }
        if self.count > MAX_WAITING_DIRS {
            *self = Self::swept();
        }
    }

    fn run(self, roots: Vec<Root>) {
        for root in roots {
            if self.sweep {
                report(&root, conflicts::sweep(&root.path));
                let Root { item, name, .. } = &root;
                match conflicts::expire(&root.path, root.retention, Timestamp::now()) {
                    Ok(0) | Err(conflicts::Error::NotMounted) => {}
                    Ok(removed) => {
                        tracing::info!(%item, %name, removed, "expired data conflicts' kept sides");
                    }
                    Err(error) => {
                        tracing::warn!(%item, %name, %error, "can't expire data conflicts")
                    }
                }
            } else if let Some(dirs) = self.dirs.get(&root.item) {
                for dir in dirs {
                    report(&root, conflicts::collect(&root.path, dir));
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
                    path: file, winner, ..
                } = conflict;
                tracing::info!(
                    %item, %name, %path, %file, %winner,
                    "a data conflict: device {winner}'s change won, and the other is kept in {}",
                    conflicts::STORE
                );
            }
            for error in failed {
                tracing::warn!(%item, %name, %path, %error, "can't keep a data conflict's copy");
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
    /// Queues the work for what the watcher saw.
    pub(super) fn add(&self, seen: Seen) {
        let mut waiting = self.waiting.lock().unwrap();
        match seen {
            Seen::Finished(Finished { item, path }) => {
                let dir = path.rsplit_once('/').map_or("", |(dir, _)| dir);
                waiting.add(item, dir.to_owned());
            }
            Seen::Missed => *waiting = Waiting::swept(),
        }
        drop(waiting);
        self.ready.notify_one();
    }

    /// Queues a sweep of every configured Replica.
    pub(super) fn sweep(&self) {
        *self.waiting.lock().unwrap() = Waiting::swept();
        self.ready.notify_one();
    }

    /// Has waiting work tried again, now that there may be Replicas to do
    /// it in.
    pub(super) fn retry(&self) {
        self.ready.notify_one();
    }

    fn take(&self) -> Waiting {
        mem::take(&mut *self.waiting.lock().unwrap())
    }

    fn give_back(&self, back: Waiting) {
        let mut waiting = self.waiting.lock().unwrap();
        if back.sweep {
            *waiting = Waiting::swept();
        } else {
            for (item, dirs) in back.dirs {
                for dir in dirs {
                    waiting.add(item, dir);
                }
            }
        }
    }

    /// Runs the waiting work as it arrives, in `me`'s configured Replicas
    /// as `replicas` has them then. Runs until it is dropped.
    pub(super) async fn work(&self, replicas: Arc<Mutex<Replicas>>, me: NodeId) {
        loop {
            self.ready.notified().await;
            let waiting = self.take();
            if waiting.is_empty() {
                continue;
            }
            let roots = roots(&replicas, &me);
            if roots.is_empty() {
                // Until a reconcile configures some.
                self.give_back(waiting);
                continue;
            }
            // Directories of Replicas not configured yet wait for them.
            let mut waiting = waiting;
            let mut later = Waiting::default();
            if !waiting.sweep {
                let ready: BTreeSet<ItemId> = roots.iter().map(|root| root.item).collect();
                let (now, not_yet) = mem::take(&mut waiting.dirs)
                    .into_iter()
                    .partition(|(item, _)| ready.contains(item));
                waiting.dirs = now;
                later.dirs = not_yet;
            }
            self.give_back(later);
            // Each move is one rename, so work cut short by the daemon
            // stopping leaves no file half moved.
            let _ = tokio::task::spawn_blocking(move || waiting.run(roots)).await;
        }
    }

    /// Runs the waiting work now.
    #[cfg(test)]
    pub(super) fn run_now(&self, replicas: &Mutex<Replicas>, me: &NodeId) {
        self.take().run(roots(replicas, me));
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
    /// Syncthing couldn't say whether it does.
    Syncthing(syncthing::Error),
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
            Self::Syncthing(error) => write!(f, "can't ask Syncthing about the replica: {error}"),
            Self::Conflict(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ActionError {}

/// This Node's Data Conflicts, as the daemon and its API reach them.
#[derive(Clone)]
pub(super) struct Conflicts {
    pub(super) replicas: Arc<Mutex<Replicas>>,
    pub(super) me: NodeId,
    pub(super) driver: Option<Driver>,
}

impl Conflicts {
    fn roots(&self) -> Vec<Root> {
        roots(&self.replicas, &self.me)
    }

    /// Sweeps every configured Replica now, moving conflict copies into the
    /// store and expiring old conflicts.
    pub(super) async fn sweep(&self) {
        let roots = self.roots();
        let _ = tokio::task::spawn_blocking(move || Waiting::swept().run(roots)).await;
    }

    /// The Data Conflicts kept on this Node, for each Replica whose folder
    /// is configured, or why they can't be read.
    pub(super) async fn list(
        &self,
    ) -> BTreeMap<ItemId, Result<Vec<DataConflict>, conflicts::Error>> {
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
    pub(super) async fn keep(
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
    /// other Replicas. Its folder must send now, as Syncthing has it.
    pub(super) async fn swap(
        &self,
        item: ItemId,
        version: &str,
    ) -> Result<DataConflict, ActionError> {
        let root = self.root(item)?;
        let driver = self.driver.as_ref().ok_or(ActionError::NoReplica(item))?;
        let sends = driver
            .client()
            .folder_sends(item)
            .await
            .map_err(ActionError::Syncthing)?;
        if !sends {
            return Err(ActionError::DoesntSend(item));
        }
        let version = version.to_owned();
        let conflict = tokio::task::spawn_blocking(move || conflicts::swap(&root.path, &version))
            .await
            .expect("swapping a conflict doesn't panic")
            .map_err(ActionError::Conflict)?;
        // Syncthing would notice on its own, but later.
        if let Err(error) = driver.client().scan_path(item, &conflict.path).await {
            tracing::warn!(%item, file = %conflict.path, %error, "can't ask Syncthing to scan a swapped file");
        }
        Ok(conflict)
    }
}

impl<T: LocalApi + Clone> Daemon<T> {
    pub(super) fn conflict_ops(&self) -> Conflicts {
        Conflicts {
            replicas: self.replicas.clone(),
            me: self.db.lock().unwrap().node().clone(),
            driver: self.driver.clone(),
        }
    }

    /// Sweeps every configured Replica now, moving conflict copies into the
    /// store and expiring old conflicts.
    pub async fn sweep_conflicts(&self) {
        self.conflict_ops().sweep().await;
    }

    /// The Data Conflicts kept on this Node, for each Replica whose folder
    /// is configured, or why they can't be read.
    pub async fn conflicts(&self) -> BTreeMap<ItemId, Result<Vec<DataConflict>, conflicts::Error>> {
        self.conflict_ops().list().await
    }

    /// Keeps the winning change of the Data Conflict `version` in `item`'s
    /// Replica.
    pub async fn keep_conflict(
        &self,
        item: ItemId,
        version: &str,
    ) -> Result<DataConflict, ActionError> {
        self.conflict_ops().keep(item, version).await
    }

    /// Makes the losing change of the Data Conflict `version` in `item`'s
    /// Replica the file's content again, which Syncthing then sends to the
    /// other Replicas.
    pub async fn swap_conflict(
        &self,
        item: ItemId,
        version: &str,
    ) -> Result<DataConflict, ActionError> {
        self.conflict_ops().swap(item, version).await
    }
}
