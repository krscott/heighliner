//! Finding this Node's Data Conflicts, expiring them, and keeping or
//! swapping them.
//!
//! Conflict copies are moved into the store by a worker, away from the
//! async runtime. It runs at most one job at a time in each Replica, and
//! Replicas' jobs run side by side, so a slow or hung filesystem holds up
//! only its own Replicas. It looks in the directory of each item Syncthing
//! finishes syncing, which is where Syncthing leaves a conflict's copy. It
//! also sweeps every Replica, expiring old conflicts too, after the
//! daemon's first round, daily, and whenever events may have been missed.
//! Waiting work names Items, not paths, so each job uses its Replica's
//! current path and Retention Window; it is merged, and when there is too
//! much it becomes one sweep, so it stays bounded.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use super::{Changes, Daemon, Replicas, signal};
use crate::config::ConfigDb;
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
fn roots(replicas: &Mutex<Replicas>, me: &NodeId, retention: Duration) -> Vec<Root> {
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
            retention: local.entry.item.retention(retention),
        })
        .collect()
}

/// A job for the conflict worker in one Replica.
#[derive(Debug, PartialEq, Eq)]
enum Work {
    /// Look in these directories.
    Dirs(BTreeSet<String>),
    /// Look everywhere, and expire old conflicts.
    Sweep,
}

impl Work {
    fn run(self, root: &Root) {
        match self {
            Self::Dirs(dirs) => {
                for dir in dirs {
                    report(root, conflicts::collect(&root.path, &dir));
                }
            }
            Self::Sweep => {
                report(root, conflicts::sweep(&root.path));
                let Root { item, name, .. } = root;
                match conflicts::expire(&root.path, root.retention, Timestamp::now()) {
                    Ok(0) | Err(conflicts::Error::NotMounted) => {}
                    Ok(removed) => {
                        tracing::info!(%item, %name, removed, "expired data conflicts' kept sides");
                    }
                    Err(error) => {
                        tracing::warn!(%item, %name, %error, "can't expire data conflicts")
                    }
                }
            }
        }
    }
}

/// Work waiting for the conflict worker.
#[derive(Debug, Default)]
struct Waiting {
    /// Each Item's job, for its Replica on this Node.
    items: BTreeMap<ItemId, Work>,
    /// How many directories `items` names.
    dirs: usize,
    /// Every Replica is to be swept, whatever `items` says.
    sweep: bool,
}

impl Waiting {
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
        let work = self
            .items
            .entry(item)
            .or_insert_with(|| Work::Dirs(BTreeSet::new()));
        if let Work::Dirs(dirs) = work
            && dirs.insert(dir)
        {
            self.dirs += 1;
        }
        if self.dirs > MAX_WAITING_DIRS {
            *self = Self::swept();
        }
    }

    /// Takes the jobs of `roots` but those at the paths in `busy`, which
    /// stay waiting, as does the work of Items with no Replica in `roots`.
    fn take(&mut self, roots: Vec<Root>, busy: &BTreeSet<PathBuf>) -> Vec<(Root, Work)> {
        // A sweep waits until a reconcile configures some.
        if self.sweep && !roots.is_empty() {
            *self = Self::default();
            for root in &roots {
                self.items.insert(root.item, Work::Sweep);
            }
        }
        let mut jobs = Vec::new();
        for root in roots {
            if busy.contains(&root.path) {
                continue;
            }
            if let Some(work) = self.items.remove(&root.item) {
                if let Work::Dirs(dirs) = &work {
                    self.dirs -= dirs.len();
                }
                jobs.push((root, work));
            }
        }
        jobs
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

    /// Runs the waiting work as it arrives, in `me`'s configured Replicas
    /// as `replicas` has them then, at most one job at a time in each,
    /// signalling `changes` after each job. Runs until it is dropped.
    pub(super) async fn work(
        &self,
        replicas: Arc<Mutex<Replicas>>,
        me: NodeId,
        retention: Duration,
        changes: Changes,
    ) {
        let mut running = tokio::task::JoinSet::new();
        let mut busy = BTreeSet::new();
        loop {
            tokio::select! {
                () = self.ready.notified() => {}
                Some(done) = running.join_next() => {
                    busy.remove(&done.expect("only dropping the worker cancels its jobs"));
                    signal(&changes);
                }
            }
            let roots = roots(&replicas, &me, retention);
            let jobs = self.waiting.lock().unwrap().take(roots, &busy);
            for (root, work) in jobs {
                busy.insert(root.path.clone());
                running.spawn(async move {
                    let path = root.path.clone();
                    // Each move is one rename, so a job cut short by the
                    // daemon stopping leaves no file half moved.
                    let _ = tokio::task::spawn_blocking(move || work.run(&root)).await;
                    path
                });
            }
        }
    }

    /// Runs the waiting work now.
    #[cfg(test)]
    pub(super) fn run_now(&self, replicas: &Mutex<Replicas>, me: &NodeId) {
        let roots = roots(replicas, me, super::RETENTION_WINDOW);
        let jobs = self.waiting.lock().unwrap().take(roots, &BTreeSet::new());
        for (root, work) in jobs {
            work.run(&root);
        }
    }
}

/// How long listing, keeping or swapping a Replica's Data Conflicts may
/// take, including waiting for earlier work on it.
const WORK_TIMEOUT: Duration = Duration::from_secs(5);

/// One gate per Replica path, so that a hung filesystem holds at most one
/// thread per Replica, however often its conflicts are asked for.
pub(super) type Gates = Arc<Mutex<BTreeMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>>;

/// The gate for the Replica at `path`.
fn gate(gates: &Gates, path: &std::path::Path) -> Arc<tokio::sync::Mutex<()>> {
    let mut gates = gates.lock().unwrap();
    // Gates no work holds or waits on are dropped, so they don't pile up
    // as Replicas move.
    gates.retain(|_, gate| Arc::strong_count(gate) > 1);
    gates.entry(path.to_owned()).or_default().clone()
}

/// Runs `work` on the Replica at `path` away from the async runtime, after
/// any earlier work on it finishes, for at most [`WORK_TIMEOUT`]. Work
/// that times out still runs to its end, holding the gate; work that
/// times out waiting for it is dropped.
async fn gated<R: Send + 'static>(
    gates: &Gates,
    path: PathBuf,
    work: impl FnOnce(&std::path::Path) -> R + Send + 'static,
) -> Result<R, ActionError> {
    let gate = gate(gates, &path);
    let started = Arc::new(AtomicBool::new(false));
    let run = {
        let started = started.clone();
        async move {
            let open = gate.lock_owned().await;
            started.store(true, Ordering::SeqCst);
            tokio::task::spawn_blocking(move || {
                let done = work(&path);
                drop(open);
                done
            })
            .await
            .expect("conflict work doesn't panic")
        }
    };
    tokio::time::timeout(WORK_TIMEOUT, run).await.map_err(|_| {
        if started.load(Ordering::SeqCst) {
            ActionError::Hung
        } else {
            ActionError::Busy
        }
    })
}

/// Like [`gated`], for an action the user asked for: once started, it
/// runs to its end even if the answer times out, then `then` sees what it
/// did. One still waiting for the gate when the answer times out is
/// dropped, so the answer can say it wasn't done.
async fn gated_action<R: Send + 'static>(
    gates: &Gates,
    path: PathBuf,
    work: impl FnOnce(&std::path::Path) -> R + Send + 'static,
    then: impl FnOnce(&R) + Send + 'static,
) -> Result<R, ActionError> {
    const WAITING: u8 = 0;
    const STARTED: u8 = 1;
    const DROPPED: u8 = 2;
    let state = Arc::new(AtomicU8::new(WAITING));
    let gate = gate(gates, &path);
    let claimed = state.clone();
    let mut task = tokio::spawn(async move {
        let open = gate.lock_owned().await;
        claimed
            .compare_exchange(WAITING, STARTED, Ordering::SeqCst, Ordering::SeqCst)
            .ok()?;
        let done = tokio::task::spawn_blocking(move || {
            let done = work(&path);
            drop(open);
            done
        })
        .await
        .expect("conflict work doesn't panic");
        then(&done);
        Some(done)
    });
    match tokio::time::timeout(WORK_TIMEOUT, &mut task).await {
        Ok(done) => Ok(done
            .expect("conflict work doesn't panic")
            .expect("only a timed-out answer drops the work")),
        Err(_) => {
            let dropped =
                state.compare_exchange(WAITING, DROPPED, Ordering::SeqCst, Ordering::SeqCst);
            if dropped.is_ok() {
                // It would only wait, then do nothing.
                task.abort();
                Err(ActionError::Busy)
            } else {
                Err(ActionError::Hung)
            }
        }
    }
}

/// Why a Data Conflict couldn't be listed, kept or swapped.
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
    /// Earlier work on the Replica didn't finish in time, so this wasn't
    /// done.
    Busy,
    /// The Replica's filesystem didn't answer in time. A keep or swap may
    /// still finish.
    Hung,
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
            Self::Busy => write!(
                f,
                "earlier work on the replica's files didn't finish within {}s, so nothing was done; is its filesystem hung?",
                WORK_TIMEOUT.as_secs()
            ),
            Self::Hung => write!(
                f,
                "the replica's filesystem didn't answer within {}s; is it hung? A keep or swap may still finish",
                WORK_TIMEOUT.as_secs()
            ),
        }
    }
}

impl std::error::Error for ActionError {}

/// This Node's Data Conflicts, as the daemon and its API reach them.
#[derive(Clone)]
pub(super) struct Conflicts {
    pub(super) replicas: Arc<Mutex<Replicas>>,
    pub(super) db: Arc<Mutex<ConfigDb>>,
    pub(super) me: NodeId,
    pub(super) driver: Option<Driver>,
    pub(super) changes: Changes,
    pub(super) gates: Gates,
    /// The Retention Window of Items without their own.
    pub(super) retention: Duration,
}

impl Conflicts {
    fn roots(&self) -> Vec<Root> {
        roots(&self.replicas, &self.me, self.retention)
    }

    /// Sweeps every configured Replica now, moving conflict copies into the
    /// store and expiring old conflicts.
    pub(super) async fn sweep(&self) {
        let roots = self.roots();
        let _ = tokio::task::spawn_blocking(move || {
            for root in roots {
                Work::Sweep.run(&root);
            }
        })
        .await;
    }

    /// The Data Conflicts kept on this Node, for each Replica whose folder
    /// is configured, or why they can't be read.
    pub(super) async fn list(&self) -> BTreeMap<ItemId, Result<Vec<DataConflict>, ActionError>> {
        let mut listings = tokio::task::JoinSet::new();
        for root in self.roots() {
            let gates = self.gates.clone();
            listings.spawn(async move {
                let listed = gated(&gates, root.path, conflicts::list).await;
                (
                    root.item,
                    listed.and_then(|listed| listed.map_err(ActionError::Conflict)),
                )
            });
        }
        listings.join_all().await.into_iter().collect()
    }

    /// `item`'s configured Replica on this Node, if the records still have
    /// it there: one removed since the last reconcile is left alone.
    fn root(&self, item: ItemId) -> Result<Root, ActionError> {
        let config = self.db.lock().unwrap().config();
        let declared = config
            .items
            .get(&item)
            .and_then(|entry| entry.replicas.get(&self.me))
            .map(|replica| PathBuf::from(replica.path().as_str()));
        self.roots()
            .into_iter()
            .find(|root| root.item == item && declared.as_ref() == Some(&root.path))
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
        let changes = self.changes.clone();
        gated_action(
            &self.gates,
            root.path,
            move |path| conflicts::keep(path, &version),
            move |kept| {
                if kept.is_ok() {
                    signal(&changes);
                }
            },
        )
        .await?
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
        let (changes, client) = (self.changes.clone(), driver.client().clone());
        gated_action(
            &self.gates,
            root.path,
            move |path| conflicts::swap(path, &version),
            move |swapped| {
                let Ok(conflict) = swapped else {
                    return;
                };
                signal(&changes);
                // Syncthing would notice on its own, but later. The swap is
                // done whatever the scan takes.
                let file = conflict.path.clone();
                tokio::spawn(async move {
                    if let Err(error) = client.scan_path(item, &file).await {
                        tracing::warn!(%item, %file, %error, "can't ask Syncthing to scan a swapped file");
                    }
                });
            },
        )
        .await?
        .map_err(ActionError::Conflict)
    }
}

impl<T: LocalApi + Clone> Daemon<T> {
    pub(super) fn conflict_ops(&self) -> Conflicts {
        Conflicts {
            replicas: self.replicas.clone(),
            db: self.db.clone(),
            me: self.db.lock().unwrap().node().clone(),
            driver: self.driver.clone(),
            changes: self.changes.clone(),
            gates: self.conflict_gates.clone(),
            retention: self.retention,
        }
    }

    /// Sweeps every configured Replica now, moving conflict copies into the
    /// store and expiring old conflicts.
    pub async fn sweep_conflicts(&self) {
        self.conflict_ops().sweep().await;
    }

    /// The Data Conflicts kept on this Node, for each Replica whose folder
    /// is configured, or why they can't be read.
    pub async fn conflicts(&self) -> BTreeMap<ItemId, Result<Vec<DataConflict>, ActionError>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Waits up to a second for `flag`.
    async fn eventually(flag: &AtomicBool) -> bool {
        for _ in 0..100 {
            if flag.load(Ordering::SeqCst) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    fn root(item: u128, path: &str) -> Root {
        Root {
            item: ItemId::from_u128(item),
            name: String::new(),
            path: PathBuf::from(path),
            retention: super::super::RETENTION_WINDOW,
        }
    }

    fn dirs(names: &[&str]) -> Work {
        Work::Dirs(names.iter().map(|&name| name.to_owned()).collect())
    }

    #[test]
    fn a_busy_replica_holds_up_only_its_own_work() {
        let mut waiting = Waiting::default();
        for item in 1..=3 {
            waiting.add(ItemId::from_u128(item), "docs".to_owned());
        }
        waiting.add(ItemId::from_u128(1), "more".to_owned());
        let roots = || vec![root(1, "/srv/a"), root(2, "/srv/b")];
        let busy = BTreeSet::from([PathBuf::from("/srv/a")]);
        let jobs = waiting.take(roots(), &busy);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].0.path, PathBuf::from("/srv/b"));
        assert_eq!(jobs[0].1, dirs(&["docs"]));
        // The busy Replica's, and that of an Item with no Replica yet, wait.
        assert_eq!(waiting.dirs, 3);
        let jobs = waiting.take(roots(), &BTreeSet::new());
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1, dirs(&["docs", "more"]));
        assert_eq!(waiting.dirs, 1);
        assert!(waiting.items.contains_key(&ItemId::from_u128(3)));
    }

    #[test]
    fn a_sweep_is_a_job_in_every_replica() {
        let mut waiting = Waiting::default();
        waiting.add(ItemId::from_u128(1), "docs".to_owned());
        waiting = Waiting::swept();
        // Until there are Replicas.
        assert!(waiting.take(Vec::new(), &BTreeSet::new()).is_empty());
        assert!(waiting.sweep);
        waiting.add(ItemId::from_u128(1), "docs".to_owned());
        let busy = BTreeSet::from([PathBuf::from("/srv/a")]);
        let jobs = waiting.take(vec![root(1, "/srv/a"), root(2, "/srv/b")], &busy);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1, Work::Sweep);
        assert_eq!(waiting.items.get(&ItemId::from_u128(1)), Some(&Work::Sweep));
        // A directory adds nothing to a sweep.
        waiting.add(ItemId::from_u128(1), "docs".to_owned());
        assert_eq!(waiting.dirs, 0);
        let jobs = waiting.take(vec![root(1, "/srv/a")], &BTreeSet::new());
        assert_eq!(jobs[0].1, Work::Sweep);
    }

    #[test]
    fn too_many_waiting_directories_become_a_sweep() {
        let mut waiting = Waiting::default();
        for n in 0..=MAX_WAITING_DIRS {
            waiting.add(ItemId::from_u128(1), n.to_string());
        }
        assert!(waiting.sweep && waiting.items.is_empty());
    }

    #[tokio::test]
    async fn an_action_still_waiting_when_its_answer_times_out_isnt_done() {
        let gates = Gates::default();
        let path = PathBuf::from("/srv/x");
        // As if earlier work were stuck on the Replica's filesystem.
        let stuck = gate(&gates, &path).lock_owned().await;
        let ran = Arc::new(AtomicBool::new(false));
        let answer = {
            let ran = ran.clone();
            gated_action(
                &gates,
                path,
                move |_| ran.store(true, Ordering::SeqCst),
                |()| {},
            )
            .await
        };
        assert!(matches!(answer, Err(ActionError::Busy)), "{answer:?}");
        // Nothing of it is kept waiting, even while the gate stays shut.
        for _ in 0..100 {
            if Arc::strong_count(&ran) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(Arc::strong_count(&ran), 1);
        drop(stuck);
        assert!(!eventually(&ran).await);
    }

    #[tokio::test]
    async fn an_action_that_outlives_its_answer_still_finishes_and_follows_up() {
        let gates = Gates::default();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let followed = Arc::new(AtomicBool::new(false));
        let answer = {
            let followed = followed.clone();
            gated_action(
                &gates,
                PathBuf::from("/srv/x"),
                // As if its filesystem were slow to answer.
                move |_| wait.recv().unwrap(),
                move |()| followed.store(true, Ordering::SeqCst),
            )
            .await
        };
        assert!(matches!(answer, Err(ActionError::Hung)), "{answer:?}");
        assert!(!followed.load(Ordering::SeqCst));
        release.send(()).unwrap();
        assert!(eventually(&followed).await);
    }
}
