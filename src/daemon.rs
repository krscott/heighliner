//! The daemon: serves the Node protocol and the API the CLI uses on this
//! Node's Tailscale addresses, pulls the other Nodes' records on a timer,
//! and keeps its copy of all records in a state file. When it drives Syncthing, it configures it each
//! round and follows its Replicas' states.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::net::{IpAddr, SocketAddr};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::signal::unix::{self, SignalKind};
use tokio::sync::Notify;
use tokio::task::JoinSet;

use crate::auth::{self, Authorizer};
use crate::config::{Config, ConfigDb};
use crate::discovery;
use crate::hlc::{Hlc, Timestamp};
use crate::model::{AbsPath, ItemId, NodeId, ReplicaState};
use crate::protocol::{self, Hello};
use crate::record::Store;
use crate::syncthing::{self, Driver, Outcome};
use crate::tailscale::{self, Client, LocalApi, Owner, Tailnet};

/// The port heighliner listens on unless configured otherwise.
pub const DEFAULT_PORT: u16 = 7130;

/// How long superseded record versions, tombstones and Displaced Versions
/// are kept.
pub const RETENTION_WINDOW: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// How long each of a device's addresses may take to answer hello. Devices
/// are asked concurrently, so a round waits this long for each address of
/// an offline device.
const HELLO_TIMEOUT: Duration = Duration::from_secs(3);

/// How long pulling another Node's records may take.
const PULL_TIMEOUT: Duration = Duration::from_secs(10);

const STATE_FILE: &str = "state.json";

/// Locked for as long as a daemon uses the state directory.
const LOCK_FILE: &str = "lock";

/// The version of the state file's layout this build reads and writes.
const STATE_FORMAT: u32 = 1;

/// What the state file holds.
#[derive(Serialize, Deserialize)]
struct SavedState {
    format: u32,
    /// The user whose Fleet the records belong to.
    owner: Owner,
    /// The clock's latest reading, so it resumes after it on restart. Its
    /// node is the Node the state belongs to.
    clock: Hlc,
    records: Store,
    /// When each of this Node's Replicas was last confirmed current, as of
    /// when it last stopped being current or the daemon last stopped.
    #[serde(default)]
    last_current: BTreeMap<ItemId, Confirmed>,
}

/// When a Replica was confirmed current, and at which path, since one moved
/// elsewhere starts afresh.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Confirmed {
    path: AbsPath,
    at: Timestamp,
}

#[derive(Deserialize)]
struct StateFormat {
    format: u32,
}

/// Why the daemon stopped or a step of it failed.
#[derive(Debug)]
pub enum Error {
    Tailscale(tailscale::Error),
    State {
        path: PathBuf,
        source: io::Error,
    },
    StateDecode {
        path: PathBuf,
        source: serde_json::Error,
    },
    StateFormat {
        path: PathBuf,
        format: u32,
    },
    /// The state was saved by another Tailscale node, for example before
    /// this device logged in again with a new identity.
    StateNode {
        path: PathBuf,
        saved: NodeId,
        current: NodeId,
    },
    /// The state belongs to another user's Fleet.
    StateOwner {
        path: PathBuf,
    },
    /// Another daemon uses the state directory.
    StateLocked {
        path: PathBuf,
    },
    /// The file with Syncthing's API key can't be read or used.
    ApiKey {
        path: PathBuf,
        reason: String,
    },
    Listen {
        addr: SocketAddr,
        source: io::Error,
    },
    /// None of this device's Tailscale addresses could be listened on, for
    /// example because the tailnet interface isn't up yet.
    NoAddress,
    /// This device's Tailscale addresses changed since the daemon last
    /// listened.
    AddressesChanged,
    /// This device's Tailscale node ID or owner changed since the daemon
    /// started, so its records belong to another identity.
    IdentityChanged,
    Serve(tokio::task::JoinError),
    /// The handlers for stop signals couldn't be set up.
    Signals(io::Error),
}

impl Error {
    /// Whether starting again later may succeed, as when tailscaled is
    /// still coming up.
    fn is_transient(&self) -> bool {
        match self {
            Self::Tailscale(tailscale::Error::Tagged) => false,
            Self::Tailscale(_) | Self::NoAddress => true,
            _ => false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tailscale(error) => error.fmt(f),
            Self::State { path, source } => write!(f, "can't use {}: {source}", path.display()),
            Self::StateDecode { path, source } => {
                write!(f, "can't read {}: {source}", path.display())
            }
            Self::StateFormat { path, format } => write!(
                f,
                "{} has format {format}, but this heighliner reads format {STATE_FORMAT}; run a newer heighliner",
                path.display()
            ),
            Self::StateNode {
                path,
                saved,
                current,
            } => write!(
                f,
                "{} belongs to Tailscale node {saved}, but this device is now {current}; its Replicas would belong to a Node that no longer exists. Move the file aside to start this Node afresh, then add its Replicas again",
                path.display()
            ),
            Self::StateOwner { path } => write!(
                f,
                "{} belongs to another Tailscale user's Fleet; move it aside to start this Node afresh",
                path.display()
            ),
            Self::StateLocked { path } => {
                write!(f, "another heighliner daemon is using {}", path.display())
            }
            Self::ApiKey { path, reason } => write!(
                f,
                "can't use Syncthing's API key from {}: {reason}",
                path.display()
            ),
            Self::Listen { addr, source } => write!(f, "can't listen on {addr}: {source}"),
            Self::NoAddress => {
                f.write_str("can't listen on any of this device's Tailscale addresses")
            }
            Self::AddressesChanged => f.write_str("this device's Tailscale addresses changed"),
            Self::IdentityChanged => f.write_str(
                "this device's Tailscale node ID or owner changed since heighliner started, so its records belong to the old identity",
            ),
            Self::Serve(error) => write!(f, "a server failed: {error}"),
            Self::Signals(error) => write!(f, "can't handle stop signals: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Tailscale(error) => Some(error),
            Self::State { source, .. } | Self::Listen { source, .. } | Self::Signals(source) => {
                Some(source)
            }
            Self::StateDecode { source, .. } => Some(source),
            Self::Serve(error) => Some(error),
            Self::StateFormat { .. }
            | Self::StateNode { .. }
            | Self::StateOwner { .. }
            | Self::StateLocked { .. }
            | Self::ApiKey { .. }
            | Self::NoAddress
            | Self::AddressesChanged
            | Self::IdentityChanged => None,
        }
    }
}

impl From<tailscale::Error> for Error {
    fn from(error: tailscale::Error) -> Self {
        Self::Tailscale(error)
    }
}

/// The outcome of a pull from every Node that answered.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Round {
    /// The Nodes whose records were merged.
    pub pulled: Vec<NodeId>,
    /// The Nodes that speak another major version of the protocol.
    pub incompatible: Vec<(NodeId, Hello)>,
    /// The Nodes that answered but whose records couldn't be pulled.
    pub failed: Vec<NodeId>,
    /// What configuring Syncthing did, if the daemon drives Syncthing and
    /// could reach it.
    pub syncthing: Option<syncthing::Report>,
}

/// One Node's daemon.
pub struct Daemon<T> {
    api: T,
    port: u16,
    /// The status when the daemon last listened.
    tailnet: Tailnet,
    state_file: PathBuf,
    /// Held for the daemon's lifetime; see [`LOCK_FILE`].
    _lock: File,
    db: Arc<Mutex<ConfigDb>>,
    /// What was last written to the state file, to skip unchanged writes.
    /// Held while saving, so saves happen one at a time and in order.
    saved: Mutex<Vec<u8>>,
    driver: Option<Driver>,
    replicas: Arc<Mutex<Replicas>>,
    /// Notified after each edit through the API.
    edited: Arc<Notify>,
    /// The other Nodes' Replicas' states, as last read, and when.
    remote: Arc<Mutex<BTreeMap<NodeId, (Timestamp, BTreeMap<ItemId, ReplicaState>)>>>,
}

/// A reconcile: the configuration it applied, and its outcome for each
/// Replica or why it couldn't run. States are read against it, so that they
/// describe what Syncthing was told.
struct Reconciled {
    config: Config,
    outcomes: Result<BTreeMap<ItemId, Outcome>, String>,
}

/// This Node's Replicas, as last seen in Syncthing.
struct Replicas {
    /// The last reconcile, if one has run.
    reconciled: Option<Arc<Reconciled>>,
    states: BTreeMap<ItemId, ReplicaState>,
    /// How many refreshes have started, and which of them `states` are
    /// from, so that one overtaken by a later one is dropped.
    started: u64,
    applied: u64,
    /// When each Replica that is current was last read so.
    current: BTreeMap<ItemId, Confirmed>,
    /// When each Replica was last confirmed current, as of when it last
    /// stopped being current; see [`SavedState::last_current`].
    last_current: BTreeMap<ItemId, Confirmed>,
    /// The state last logged for each Replica, to log changes only.
    logged: BTreeMap<ItemId, ReplicaState>,
}

impl<T: LocalApi + Clone> Daemon<T> {
    /// Opens the state in `state_dir`, creating it if needed, for this
    /// device. Fails if another daemon uses it.
    pub async fn open(api: T, state_dir: &Path, port: u16) -> Result<Self, Error> {
        let tailnet = api.status().await?;
        let state_error = |path: &Path| {
            let path = path.to_owned();
            move |source| Error::State { path, source }
        };
        fs::create_dir_all(state_dir).map_err(state_error(state_dir))?;
        let lock_file = state_dir.join(LOCK_FILE);
        let lock = File::create(&lock_file).map_err(state_error(&lock_file))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(Error::StateLocked {
                    path: state_dir.to_owned(),
                });
            }
            Err(fs::TryLockError::Error(source)) => return Err(state_error(&lock_file)(source)),
        }
        let state_file = state_dir.join(STATE_FILE);
        let (db, last_current) = load(&state_file, tailnet.me().id.clone(), tailnet.owner())?;
        // Count the state as it was opened as saved, so nothing is written
        // until it changes; a daemon that changed nothing writes no file.
        let saved = Mutex::new(snapshot(&db, tailnet.owner(), &last_current));
        Ok(Self {
            api,
            port,
            tailnet,
            state_file,
            _lock: lock,
            db: Arc::new(Mutex::new(db)),
            saved,
            driver: None,
            replicas: Arc::new(Mutex::new(Replicas {
                reconciled: None,
                states: BTreeMap::new(),
                started: 0,
                applied: 0,
                current: BTreeMap::new(),
                last_current,
                logged: BTreeMap::new(),
            })),
            edited: Arc::default(),
            remote: Arc::default(),
        })
    }

    /// Makes the daemon configure Syncthing through `driver` each round.
    pub fn with_driver(mut self, driver: Driver) -> Self {
        self.driver = Some(driver);
        self
    }

    pub fn db(&self) -> &Arc<Mutex<ConfigDb>> {
        &self.db
    }

    /// Reads this device's status again and starts serving on its current
    /// addresses, retrying while tailscaled isn't ready or none of them can
    /// be bound. Fails if this device's node ID or owner changed.
    pub async fn relisten(&mut self) -> Result<JoinSet<Infallible>, Error> {
        let mut delay = Duration::from_secs(1);
        loop {
            let attempt = async {
                let tailnet = self.api.status().await?;
                if (tailnet.me().id.clone(), tailnet.owner())
                    != (self.tailnet.me().id.clone(), self.tailnet.owner())
                {
                    return Err(Error::IdentityChanged);
                }
                self.tailnet = tailnet;
                self.listen().await
            };
            match attempt.await {
                Err(error) if error.is_transient() => {
                    tracing::warn!(%error, "can't listen yet; retrying in {}s", delay.as_secs());
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
                result => return result,
            }
        }
    }

    /// Starts serving the Node protocol on each of this device's Tailscale
    /// addresses. An address that isn't configured on this machine is
    /// skipped, unless none can be used.
    pub async fn listen(&self) -> Result<JoinSet<Infallible>, Error> {
        let authorizer = Arc::new(Authorizer::for_tailnet(self.api.clone(), &self.tailnet));
        let mut servers = JoinSet::new();
        for ip in &self.tailnet.me().ips {
            let addr = SocketAddr::new(*ip, self.port);
            let listener = match TcpListener::bind(addr).await {
                Ok(listener) => listener,
                Err(error) if error.kind() == io::ErrorKind::AddrNotAvailable => {
                    tracing::warn!(%addr, %error, "skipping an address this machine doesn't have");
                    continue;
                }
                Err(source) => return Err(Error::Listen { addr, source }),
            };
            tracing::info!(%addr, "serving the Node protocol and the API");
            let api = api::Api {
                db: self.db.clone(),
                replicas: self.replicas.clone(),
                tailscale: self.api.clone(),
                edited: self.edited.clone(),
                remote: self.remote.clone(),
                home: std::env::var_os("HOME").map(PathBuf::from),
            };
            let hosts = self
                .tailnet
                .me()
                .ips
                .iter()
                .map(|ip| SocketAddr::new(*ip, self.port).to_string())
                .collect();
            let router = auth::only_for_hosts(
                protocol::router(self.db.clone()).merge(api::router(api)),
                hosts,
            );
            servers.spawn(auth::serve(listener, router, authorizer.clone()));
        }
        if servers.is_empty() {
            return Err(Error::NoAddress);
        }
        Ok(servers)
    }

    /// Pulls and merges the records of every Node that answers, drops
    /// versions older than the Retention Window, then saves.
    pub async fn pull_round(&self) -> Result<Round, Error> {
        let tailnet = self.api.status().await?;
        let identity = |tailnet: &Tailnet| (tailnet.me().id.clone(), tailnet.owner());
        if identity(&tailnet) != identity(&self.tailnet) {
            return Err(Error::IdentityChanged);
        }
        let addresses = |tailnet: &Tailnet| {
            tailnet
                .me()
                .ips
                .iter()
                .copied()
                .collect::<BTreeSet<IpAddr>>()
        };
        if addresses(&tailnet) != addresses(&self.tailnet) {
            return Err(Error::AddressesChanged);
        }
        let mut round = Round::default();
        let mut pulls = JoinSet::new();
        for discovery::Found {
            device,
            addr,
            hello,
        } in discovery::discover(&tailnet, self.port, HELLO_TIMEOUT).await
        {
            match hello {
                Ok(hello) if hello.protocol == protocol::MAJOR => {
                    let remote = self.remote.clone();
                    pulls.spawn(async move {
                        let pulled = protocol::pull(addr, &hello, PULL_TIMEOUT).await;
                        // Only shown, so a Node that can't answer keeps its
                        // last-known states.
                        if let Ok(read) = protocol::states(addr, PULL_TIMEOUT).await {
                            let read_at = Timestamp::now();
                            remote.lock().unwrap().insert(device.id.clone(), (read_at, read.states));
                        }
                        (device, pulled)
                    });
                }
                Ok(hello) => {
                    let error = protocol::Error::Incompatible {
                        addr,
                        theirs: hello.clone(),
                    };
                    tracing::warn!(node = %device.name, %error, "not exchanging records");
                    round.incompatible.push((device.id, hello));
                }
                Err(error) => {
                    tracing::warn!(node = %device.name, %error, "a Node answered with an error");
                    round.failed.push(device.id);
                }
            }
        }
        // Merge each Node's records as they arrive, so a slow Node doesn't
        // hold up the rest.
        while let Some(joined) = pulls.join_next().await {
            let (device, pulled) = joined.expect("pull tasks don't panic");
            match pulled {
                Ok((store, skipped)) => {
                    if skipped > 0 {
                        tracing::warn!(
                            from = %device.name,
                            skipped,
                            "skipped records or versions that can't be read"
                        );
                    }
                    let rejected = self.db.lock().unwrap().merge(&store, Timestamp::now());
                    if rejected > 0 {
                        tracing::warn!(
                            from = %device.name,
                            rejected,
                            "left out versions written more than 5 minutes ahead of this Node's clock; check the clocks of the Nodes and of this machine"
                        );
                    }
                    round.pulled.push(device.id);
                }
                Err(error) => {
                    tracing::warn!(node = %device.name, %error, "can't pull records");
                    round.failed.push(device.id);
                }
            }
        }
        // Keep what was merged even if configuring Syncthing, which can be
        // slow, is interrupted.
        self.save()?;
        round.syncthing = self.drive(&tailnet).await;
        self.refresh().await;
        self.db
            .lock()
            .unwrap()
            .prune(Timestamp::now(), RETENTION_WINDOW);
        tracing::debug!(
            pulled = round.pulled.len(),
            incompatible = round.incompatible.len(),
            failed = round.failed.len(),
            "pull round done"
        );
        self.save()?;
        Ok(round)
    }

    /// Publishes Syncthing's device ID and configures Syncthing to match the
    /// configuration, as this device's status `tailnet` describes it.
    async fn drive(&self, tailnet: &Tailnet) -> Option<syncthing::Report> {
        let driver = self.driver.as_ref()?;
        let record = |config: Config, outcomes| {
            let reconciled = Reconciled { config, outcomes };
            self.replicas.lock().unwrap().reconciled = Some(Arc::new(reconciled));
        };
        let device_id = match driver.client().device_id().await {
            Ok(device_id) => device_id,
            Err(error) => {
                let config = self.db.lock().unwrap().config();
                let reason = format!("can't be synced: Syncthing can't be reached: {error}");
                record(config, Err(reason));
                return None;
            }
        };
        if let Err(error) = self
            .db
            .lock()
            .unwrap()
            .set_device_id(Timestamp::now(), device_id.clone())
        {
            tracing::warn!(%error, "can't publish this Node's Syncthing device ID");
        }
        let config = self.db.lock().unwrap().config();
        match driver.reconcile(&config, tailnet, &device_id).await {
            Ok(report) => {
                record(config, Ok(report.replicas.clone()));
                Some(report)
            }
            Err(error) => {
                let reason = format!("can't be synced: configuring Syncthing failed: {error}");
                record(config, Err(reason));
                None
            }
        }
    }

    /// Reads this Node's Replicas' states from Syncthing again, and logs
    /// the ones that changed. Refreshes may overlap; one that finishes after
    /// a later one is dropped.
    pub async fn refresh(&self) {
        let Some(driver) = &self.driver else {
            return;
        };
        let me = self.db.lock().unwrap().node().clone();
        let (reconciled, number) = {
            let mut replicas = self.replicas.lock().unwrap();
            let Some(reconciled) = replicas.reconciled.clone() else {
                return;
            };
            replicas.started += 1;
            (reconciled, replicas.started)
        };
        let config = &reconciled.config;
        // Taken before reading, so that a confirmation is never dated later
        // than it happened.
        let read_at = Timestamp::now();
        let mut states = match &reconciled.outcomes {
            Ok(outcomes) => driver.states(config, &me, outcomes).await,
            Err(reason) => config
                .replicas_on(&me)
                .into_iter()
                .map(|local| {
                    let message = reason.clone();
                    (local.item_id, ReplicaState::Error { message })
                })
                .collect(),
        };
        let mut replicas = self.replicas.lock().unwrap();
        if number < replicas.applied {
            return;
        }
        replicas.applied = number;
        let Replicas {
            states: before,
            current,
            last_current,
            logged,
            ..
        } = &mut *replicas;
        // An unreadable Replica's Item and path aren't known.
        let path_of = |item: &ItemId| {
            config
                .items
                .get(item)
                .and_then(|entry| entry.replicas.get(&me))
                .map(|replica| replica.path())
        };
        for (item, state) in &mut states {
            let path = path_of(item);
            match (&*state, path) {
                (ReplicaState::Current, Some(path)) => {
                    let path = path.clone();
                    current.insert(*item, Confirmed { path, at: read_at });
                }
                _ => {
                    if let Some(confirmed) = current.remove(item) {
                        last_current.insert(*item, confirmed);
                    }
                }
            }
            if let ReplicaState::Behind { last_current: at } = state {
                *at = last_current
                    .get(item)
                    .filter(|confirmed| Some(&confirmed.path) == path)
                    .map(|confirmed| confirmed.at);
            }
            // Syncing comes and goes with each change, so it isn't logged.
            if matches!(state, ReplicaState::Syncing { .. })
                || logged
                    .get(item)
                    .is_some_and(|logged| same_kind(logged, state))
            {
                continue;
            }
            let name = config
                .items
                .get(item)
                .map_or("", |entry| entry.item.name.as_str());
            let path = path.map_or("", AbsPath::as_str);
            if matches!(state, ReplicaState::Error { .. }) {
                tracing::warn!(%item, %name, %path, "this Node's replica {state}");
            } else {
                tracing::info!(%item, %name, %path, "this Node's replica {state}");
            }
            logged.insert(*item, state.clone());
        }
        for (item, confirmed) in std::mem::take(current) {
            if states.contains_key(&item) {
                current.insert(item, confirmed);
            } else {
                last_current.insert(item, confirmed);
            }
        }
        // Forget the Replicas that are gone, or moved elsewhere; keep the
        // ones that can't be read.
        let unusable = config.unusable_on(&me);
        last_current.retain(|item, confirmed| {
            unusable.contains(item)
                || match path_of(item) {
                    Some(path) => *path == confirmed.path,
                    None => !config.was_removed(*item, &me) && !config.items.contains_key(item),
                }
        });
        logged.retain(|item, _| states.contains_key(item));
        *before = states;
    }

    /// This Node's Replicas' states, as last read.
    pub fn states(&self) -> BTreeMap<ItemId, ReplicaState> {
        self.replicas.lock().unwrap().states.clone()
    }

    /// Saves, also keeping when the Replicas that are current now were last
    /// confirmed so, for once the daemon runs again.
    pub fn save_on_stop(&self) -> Result<(), Error> {
        {
            let mut replicas = self.replicas.lock().unwrap();
            let current = replicas.current.clone();
            replicas.last_current.extend(current);
        }
        self.save()
    }

    /// Writes the records and clock to the state file, if they changed.
    pub fn save(&self) -> Result<(), Error> {
        let mut saved = self.saved.lock().unwrap();
        let bytes = snapshot(
            &self.db.lock().unwrap(),
            self.tailnet.owner(),
            &self.replicas.lock().unwrap().last_current,
        );
        if *saved == bytes {
            return Ok(());
        }
        write_atomically(&self.state_file, &bytes).map_err(|source| Error::State {
            path: self.state_file.clone(),
            source,
        })?;
        *saved = bytes;
        Ok(())
    }
}

/// The state file's contents for `db`, a copy of `owner`'s Fleet.
fn snapshot(db: &ConfigDb, owner: Owner, last_current: &BTreeMap<ItemId, Confirmed>) -> Vec<u8> {
    let state = SavedState {
        format: STATE_FORMAT,
        owner,
        clock: db.last_reading(),
        records: db.store().clone(),
        last_current: last_current.clone(),
    };
    serde_json::to_vec(&state).expect("the state serializes")
}

/// Reads the state file for `node` of `owner`'s Fleet, and when its
/// Replicas were last confirmed current, or starts empty if there is none.
fn load(
    path: &Path,
    node: NodeId,
    owner: Owner,
) -> Result<(ConfigDb, BTreeMap<ItemId, Confirmed>), Error> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok((ConfigDb::new(node, Store::default()), BTreeMap::new()));
        }
        Err(source) => {
            return Err(Error::State {
                path: path.to_owned(),
                source,
            });
        }
    };
    let decode_error = |source| Error::StateDecode {
        path: path.to_owned(),
        source,
    };
    let StateFormat { format } = serde_json::from_slice(&bytes).map_err(decode_error)?;
    if format != STATE_FORMAT {
        return Err(Error::StateFormat {
            path: path.to_owned(),
            format,
        });
    }
    let saved: SavedState = serde_json::from_slice(&bytes).map_err(decode_error)?;
    if saved.owner != owner {
        return Err(Error::StateOwner {
            path: path.to_owned(),
        });
    }
    if saved.clock.node != node {
        return Err(Error::StateNode {
            path: path.to_owned(),
            saved: saved.clock.node,
            current: node,
        });
    }
    let db = ConfigDb::resume(node, saved.records, &saved.clock);
    Ok((db, saved.last_current))
}

/// Whether two states are the same kind, and for errors the same error.
fn same_kind(a: &ReplicaState, b: &ReplicaState) -> bool {
    match (a, b) {
        (ReplicaState::Error { message: a }, ReplicaState::Error { message: b }) => a == b,
        _ => std::mem::discriminant(a) == std::mem::discriminant(b),
    }
}

/// Replaces `path` with `bytes`, readable only by this user, so that a
/// crash leaves either the old or the new file. Callers hold the state
/// directory's lock, so the temporary file is theirs.
fn write_atomically(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    // A file left by a crash may have other permissions, so start afresh.
    match fs::remove_file(&tmp) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// How to run the daemon.
#[derive(Clone, Debug)]
pub struct Options {
    pub state_dir: PathBuf,
    pub port: u16,
    pub tailscale_socket: PathBuf,
    pub pull_every: Duration,
    /// How to reach Syncthing, if the daemon is to drive it.
    pub syncthing: Option<SyncthingOptions>,
}

/// How the daemon reaches the Syncthing instance it drives.
#[derive(Clone, Debug)]
pub struct SyncthingOptions {
    /// Syncthing's GUI and REST API socket.
    pub socket: PathBuf,
    /// A file holding Syncthing's API key.
    pub api_key_file: PathBuf,
}

/// Runs the daemon. It waits for tailscaled to be ready, then serves and
/// pulls, listening again when this device's Tailscale addresses change.
/// It stops when a server fails, or when this device's Tailscale identity
/// changes, since its records then belong to the old one.
pub async fn run(options: Options) -> Result<(), Error> {
    let api = Client::new(options.tailscale_socket);
    let mut daemon = open_when_ready(api, &options.state_dir, options.port).await?;
    if let Some(syncthing) = &options.syncthing {
        let path = &syncthing.api_key_file;
        let api_key_error = |reason: String| Error::ApiKey {
            path: path.clone(),
            reason,
        };
        let api_key = fs::read_to_string(path).map_err(|error| api_key_error(error.to_string()))?;
        let client = syncthing::Client::new(&syncthing.socket, api_key.trim())
            .map_err(|error| api_key_error(error.to_string()))?;
        daemon = daemon.with_driver(Driver::new(client, RETENTION_WINDOW));
    }
    let stopped = serve(&mut daemon, options.pull_every).await;
    // Whatever stopped the daemon, keep what it merged since the last save.
    if let Err(error) = daemon.save_on_stop() {
        tracing::error!(%error, "saving before stopping failed");
    }
    stopped
}

/// Serves and pulls until something stops the daemon, such as SIGTERM or
/// SIGINT, which stop it normally.
async fn serve(daemon: &mut Daemon<Client>, pull_every: Duration) -> Result<(), Error> {
    let signal = |kind| unix::signal(kind).map_err(Error::Signals);
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut servers = daemon.relisten().await?;
    let mut pulls = tokio::time::interval(pull_every);
    pulls.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Dropped, and so stopped, with the daemon's other tasks.
    let mut tasks = JoinSet::new();
    let changed = Arc::new(Notify::new());
    let queue = Arc::new(conflicts::Queue::default());
    {
        let (queue, replicas) = (queue.clone(), daemon.replicas.clone());
        let me = daemon.db.lock().unwrap().node().clone();
        tasks.spawn(async move { queue.work(replicas, me).await });
    }
    if let Some(driver) = daemon.driver.clone() {
        let (changed, queue) = (changed.clone(), queue.clone());
        tasks.spawn(async move { driver.watch(&changed, |seen| queue.add(seen)).await });
    }
    let mut last_sweep: Option<Instant> = None;
    let edited = daemon.edited.clone();
    loop {
        tokio::select! {
            // An edit is applied by a round, now rather than at its time.
            _ = edited.notified() => pulls.reset_immediately(),
            _ = changed.notified() => {
                tokio::select! {
                    () = daemon.refresh() => {}
                    _ = terminate.recv() => return Ok(()),
                    _ = interrupt.recv() => return Ok(()),
                }
            }
            _ = pulls.tick() => {
                // A round can take a while, for example scanning a mirror
                // before it starts to send, so stop signals interrupt it,
                // and Syncthing's events still refresh the states meanwhile.
                let round = {
                    let daemon = &*daemon;
                    let round = daemon.pull_round();
                    tokio::pin!(round);
                    let refreshing = Cell::new(false);
                    let round = loop {
                        tokio::select! {
                            round = &mut round => break round,
                            () = async {
                                changed.notified().await;
                                refreshing.set(true);
                                daemon.refresh().await;
                                refreshing.set(false);
                            } => {}
                            _ = terminate.recv() => return Ok(()),
                            _ = interrupt.recv() => return Ok(()),
                        }
                    };
                    // The round's own refresh may have read a Replica before
                    // the change that started the one it cut short.
                    if refreshing.get() {
                        changed.notify_one();
                    }
                    round
                };
                match round {
                Err(Error::AddressesChanged) => {
                    tracing::info!("this device's Tailscale addresses changed; listening on the new ones");
                    // Stop the servers and their connections first, so the
                    // port is free on addresses that stay.
                    servers.shutdown().await;
                    servers = daemon.relisten().await?;
                }
                Err(error @ Error::IdentityChanged) => return Err(error),
                Err(error) => tracing::warn!(%error, "pulling records failed"),
                Ok(_) => {}
                }
                if last_sweep.is_none_or(|at| at.elapsed() >= conflicts::SWEEP_EVERY) {
                    queue.sweep();
                    last_sweep = Some(Instant::now());
                } else {
                    queue.retry();
                }
            }
            Some(Err(error)) = servers.join_next() => return Err(Error::Serve(error)),
            _ = terminate.recv() => return Ok(()),
            _ = interrupt.recv() => return Ok(()),
        }
    }
}

/// Opens the daemon, retrying while tailscaled isn't ready.
async fn open_when_ready(
    api: Client,
    state_dir: &Path,
    port: u16,
) -> Result<Daemon<Client>, Error> {
    let mut delay = Duration::from_secs(1);
    loop {
        match Daemon::open(api.clone(), state_dir, port).await {
            Err(error) if error.is_transient() => {
                tracing::warn!(%error, "can't start yet; retrying in {}s", delay.as_secs());
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
            result => return result,
        }
    }
}

mod api;
mod conflicts;

pub use conflicts::ActionError;

#[cfg(test)]
mod tests;
