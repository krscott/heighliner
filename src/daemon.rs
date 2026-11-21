//! The daemon: serves the Node protocol on this Node's Tailscale addresses,
//! pulls the other Nodes' records on a timer, and keeps its own records in
//! a state file.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::net::SocketAddr;
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
use crate::tailscale::{self, Client, LocalApi};

/// The port heighliner listens on unless configured otherwise.
pub const DEFAULT_PORT: u16 = 7130;

/// How long another Node may take to answer hello. Most devices are often
/// offline, and every pull round waits this long for each of them.
const HELLO_TIMEOUT: Duration = Duration::from_secs(3);

/// How long pulling another Node's records may take.
const PULL_TIMEOUT: Duration = Duration::from_secs(10);

const STATE_FILE: &str = "state.json";

/// The version of the state file's layout this build reads and writes.
const STATE_FORMAT: u32 = 1;

/// What the state file holds.
#[derive(Serialize, Deserialize)]
struct SavedState {
    format: u32,
    /// The clock's latest reading, so it resumes after it on restart.
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
    Listen {
        addr: SocketAddr,
        source: io::Error,
    },
    Serve(io::Error),
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
            Self::Listen { addr, source } => write!(f, "can't listen on {addr}: {source}"),
            Self::Serve(source) => write!(f, "serving stopped: {source}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Tailscale(error) => Some(error),
            Self::State { source, .. } | Self::Listen { source, .. } | Self::Serve(source) => {
                Some(source)
            }
            Self::StateDecode { source, .. } => Some(source),
            Self::StateFormat { .. } => None,
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
    /// The Nodes that answered hello but whose records couldn't be pulled.
    pub failed: Vec<NodeId>,
}

/// One Node's daemon.
pub struct Daemon<T> {
    api: T,
    port: u16,
    state_file: PathBuf,
    db: Arc<Mutex<ConfigDb>>,
    /// What was last written to the state file, to skip unchanged writes.
    saved: Mutex<Vec<u8>>,
}

impl<T: LocalApi + Clone> Daemon<T> {
    /// Opens the state in `state_dir`, creating it if needed, for this
    /// device.
    pub async fn open(api: T, state_dir: &Path, port: u16) -> Result<Self, Error> {
        let tailnet = api.status().await?;
        fs::create_dir_all(state_dir).map_err(|source| Error::State {
            path: state_dir.to_owned(),
            source,
        })?;
        let state_file = state_dir.join(STATE_FILE);
        let db = load(&state_file, tailnet.me().id.clone())?;
        Ok(Self {
            api,
            port,
            state_file,
            db: Arc::new(Mutex::new(db)),
            saved: Mutex::new(Vec::new()),
        })
    }

    pub fn db(&self) -> &Arc<Mutex<ConfigDb>> {
        &self.db
    }

    /// Starts serving the Node protocol on each of this device's Tailscale
    /// addresses. The servers run until one fails.
    pub async fn listen(&self) -> Result<JoinSet<io::Result<()>>, Error> {
        let authorizer = Arc::new(Authorizer::new(self.api.clone()).await?);
        let tailnet = self.api.status().await?;
        let mut servers = JoinSet::new();
        for ip in &tailnet.me().ips {
            let addr = SocketAddr::new(*ip, self.port);
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|source| Error::Listen { addr, source })?;
            tracing::info!(%addr, "serving the Node protocol");
            servers.spawn(auth::serve(
                listener,
                protocol::router(self.db.clone()),
                authorizer.clone(),
            ));
        }
        Ok(servers)
    }

    /// Pulls and merges the records of every Node that answers, then saves.
    pub async fn pull_round(&self) -> Result<Round, Error> {
        let tailnet = self.api.status().await?;
        let found = discovery::discover(&tailnet, self.port, HELLO_TIMEOUT).await;
        let mut pulls = JoinSet::new();
        for found in found {
            pulls.spawn(async move {
                let pulled = protocol::pull(found.addr, &found.hello, PULL_TIMEOUT).await;
                (found, pulled)
            });
        }
        let mut round = Round::default();
        for (found, pulled) in pulls.join_all().await {
            let node = found.device.id;
            let name = found.device.name;
            match pulled {
                Ok(store) => {
                    let rejected = self.db.lock().unwrap().merge(&store, Timestamp::now());
                    if rejected > 0 {
                        tracing::warn!(
                            node = %name,
                            rejected,
                            "left out versions from a Node whose clock is ahead"
                        );
                    }
                    round.pulled.push(node);
                }
                Err(protocol::Error::Incompatible { theirs, .. }) => {
                    tracing::warn!(
                        node = %name,
                        theirs = theirs.protocol,
                        ours = protocol::MAJOR,
                        "not exchanging records with a Node that speaks another protocol version"
                    );
                    round.incompatible.push((node, theirs));
                }
                Err(error) => {
                    tracing::warn!(node = %name, %error, "can't pull records");
                    round.failed.push(node);
                }
            }
        }
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
        let state = {
            let db = self.db.lock().unwrap();
            SavedState {
                format: STATE_FORMAT,
                clock: db.last_reading(),
                records: db.store().clone(),
            }
        };
        let bytes = serde_json::to_vec(&state).expect("the state serializes");
        let mut saved = self.saved.lock().unwrap();
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

/// Reads the state file for `node`, or starts empty if there is none.
fn load(path: &Path, node: NodeId) -> Result<ConfigDb, Error> {
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
    let mut db = ConfigDb::new(node, saved.records);
    db.observe(&saved.clock);
    Ok(db)
}

/// Replaces `path` with `bytes`, readable only by this user, so that a
/// crash leaves either the old or the new file.
fn write_atomically(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
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

/// Runs the daemon until serving fails.
pub async fn run(options: Options) -> Result<(), Error> {
    let api = Client::new(options.tailscale_socket);
    let daemon = Daemon::open(api, &options.state_dir, options.port).await?;
    let mut servers = daemon.listen().await?;
    let mut pulls = tokio::time::interval(options.pull_every);
    pulls.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = pulls.tick() => {
                if let Err(error) = daemon.pull_round().await {
                    tracing::warn!(%error, "pulling records failed");
                }
            }
            Some(served) = servers.join_next() => {
                let error = match served {
                    Ok(Ok(())) => io::Error::other("a server stopped"),
                    Ok(Err(error)) => error,
                    Err(error) => io::Error::other(error),
                };
                return Err(Error::Serve(error));
            }
        }
    }
}

#[cfg(test)]
mod tests;
