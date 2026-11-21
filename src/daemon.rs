//! The daemon: serves the Node protocol on this Node's Tailscale addresses,
//! pulls the other Nodes' records on a timer, and keeps its copy of all
//! records in a state file.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::net::{IpAddr, SocketAddr};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use crate::auth::{self, Authorizer};
use crate::config::ConfigDb;
use crate::discovery;
use crate::hlc::{Hlc, Timestamp};
use crate::model::NodeId;
use crate::protocol::{self, Hello};
use crate::record::Store;
use crate::tailscale::{self, Client, LocalApi, Owner, Tailnet};

/// The port heighliner listens on unless configured otherwise.
pub const DEFAULT_PORT: u16 = 7130;

/// How long superseded record versions and tombstones are kept.
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
            Self::Listen { addr, source } => write!(f, "can't listen on {addr}: {source}"),
            Self::NoAddress => {
                f.write_str("can't listen on any of this device's Tailscale addresses")
            }
            Self::AddressesChanged => f.write_str("this device's Tailscale addresses changed"),
            Self::IdentityChanged => f.write_str(
                "this device's Tailscale node ID or owner changed since heighliner started, so its records belong to the old identity",
            ),
            Self::Serve(error) => write!(f, "a server failed: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Tailscale(error) => Some(error),
            Self::State { source, .. } | Self::Listen { source, .. } => Some(source),
            Self::StateDecode { source, .. } => Some(source),
            Self::Serve(error) => Some(error),
            Self::StateFormat { .. }
            | Self::StateNode { .. }
            | Self::StateOwner { .. }
            | Self::StateLocked { .. }
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
        let db = load(&state_file, tailnet.me().id.clone(), tailnet.owner())?;
        Ok(Self {
            api,
            port,
            tailnet,
            state_file,
            _lock: lock,
            db: Arc::new(Mutex::new(db)),
            saved: Mutex::new(Vec::new()),
        })
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
            tracing::info!(%addr, "serving the Node protocol");
            servers.spawn(auth::serve(
                listener,
                protocol::router(self.db.clone()),
                authorizer.clone(),
            ));
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
                    pulls.spawn(async move {
                        let pulled = protocol::pull(addr, &hello, PULL_TIMEOUT).await;
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

    /// Writes the records and clock to the state file, if they changed.
    pub fn save(&self) -> Result<(), Error> {
        let mut saved = self.saved.lock().unwrap();
        let state = {
            let db = self.db.lock().unwrap();
            SavedState {
                format: STATE_FORMAT,
                owner: self.tailnet.owner(),
                clock: db.last_reading(),
                records: db.store().clone(),
            }
        };
        let bytes = serde_json::to_vec(&state).expect("the state serializes");
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

/// Reads the state file for `node` of `owner`'s Fleet, or starts empty if
/// there is none.
fn load(path: &Path, node: NodeId, owner: Owner) -> Result<ConfigDb, Error> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ConfigDb::new(node, Store::default()));
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
    Ok(ConfigDb::resume(node, saved.records, &saved.clock))
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
}

/// Runs the daemon. It waits for tailscaled to be ready, then serves and
/// pulls, listening again when this device's Tailscale addresses change.
/// It stops when a server fails, or when this device's Tailscale identity
/// changes, since its records then belong to the old one.
pub async fn run(options: Options) -> Result<(), Error> {
    let api = Client::new(options.tailscale_socket);
    let mut daemon = open_when_ready(api, &options.state_dir, options.port).await?;
    let stopped = serve(&mut daemon, options.pull_every).await;
    // Whatever stopped the daemon, keep what it merged since the last save.
    if let Err(error) = daemon.save() {
        tracing::error!(%error, "saving before stopping failed");
    }
    stopped
}

/// Serves and pulls until something stops the daemon.
async fn serve(daemon: &mut Daemon<Client>, pull_every: Duration) -> Result<(), Error> {
    let mut servers = daemon.relisten().await?;
    let mut pulls = tokio::time::interval(pull_every);
    pulls.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = pulls.tick() => match daemon.pull_round().await {
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
            },
            Some(Err(error)) = servers.join_next() => return Err(Error::Serve(error)),
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

#[cfg(test)]
mod tests;
