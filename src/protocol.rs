//! The Node protocol: the HTTP API daemons use to talk to each other.
//!
//! Daemons pull each other's records, and the states of each other's
//! Replicas, and ask each other for their Peripherals and to act on them
//! (ADR-0006). `GET /node/hello` says which major
//! version of the protocol a Node speaks; every version keeps its
//! `protocol` and `heighliner` fields. Every other route sits under
//! `/node/v<major>/`, and Nodes whose majors differ don't exchange records.
//!
//! The routes are served through [`crate::auth::serve`], so only the
//! owner's devices can use them.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use http_body_util::Full;
use hyper::StatusCode;
use hyper::body::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;

use std::collections::{BTreeMap, BTreeSet};

use crate::api::Refused;
use crate::bluetooth::{Address, Paired};
use crate::config::ConfigDb;
use crate::hlc::Timestamp;
use crate::http::{self, BoxError};
use crate::model::{ItemId, NodeId, ReplicaState};
use crate::record::Store;

/// The major version of the protocol this build speaks.
pub const MAJOR: u32 = 1;

pub const HELLO_PATH: &str = "/node/hello";
const RECORDS_PATH: &str = "/node/v1/records";

/// A Node's Replicas' states, as [`NodeStates`].
pub const STATES_PATH: &str = "/node/v1/states";

/// The answer to `GET` [`STATES_PATH`]: the states of the answering Node's
/// Replicas, as it last read them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStates {
    pub states: BTreeMap<ItemId, NodeState>,
}

/// A Replica's state, with the path it was read for, since a Replica added
/// again elsewhere starts afresh.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeState {
    pub path: String,
    pub state: ReplicaState,
    /// When its Node last confirmed it current, if ever.
    #[serde(default)]
    pub confirmed_at: Option<Timestamp>,
    /// The Nodes whose Replicas send to it that it was connected to.
    #[serde(default)]
    pub connected: BTreeSet<NodeId>,
}

/// GET: the answering Node's Peripherals, as [`NodePeripherals`].
pub const PERIPHERALS_PATH: &str = "/node/v1/peripherals";

/// One of the answering Node's Peripherals: POST a [`Command`] to act on
/// it.
pub fn peripheral_path(address: &Address) -> String {
    format!(
        "{PERIPHERALS_PATH}/{}",
        http::percent_encode(address.as_str())
    )
}

/// The answer to `GET` [`PERIPHERALS_PATH`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePeripherals {
    /// Whether the Node has a Bluetooth adapter that BlueZ manages.
    pub adapter: bool,
    pub peripherals: Vec<Paired>,
}

/// What a Node is asked to do with one of its Peripherals. A body is
/// required, so a page on another site can't send one as a plain form.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "kebab-case")]
pub enum Command {
    Connect,
    Disconnect,
    /// Scan for it in pairing mode, then pair, trust and connect it.
    Pair,
    Forget,
    /// Name it `alias` on the Node.
    Name {
        alias: String,
    },
}

/// The answer to `GET /node/hello`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// The major protocol version.
    pub protocol: u32,
    /// The heighliner release, for messages about upgrading.
    pub heighliner: String,
}

impl Hello {
    pub fn ours() -> Self {
        Self {
            protocol: MAJOR,
            heighliner: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }
}

/// The Node protocol's routes, serving the records in `db`. Serve them
/// only through [`crate::auth::serve`].
pub(crate) fn router(db: Arc<Mutex<ConfigDb>>) -> Router {
    Router::new()
        .route(HELLO_PATH, get(|| async { Json(Hello::ours()) }))
        .route(RECORDS_PATH, get(records))
        .with_state(db)
}

async fn records(State(db): State<Arc<Mutex<ConfigDb>>>) -> Json<Store> {
    let store = db.lock().unwrap().store().clone();
    Json(store)
}

/// Why a request to another Node failed.
#[derive(Debug)]
pub enum Error {
    Connect {
        addr: SocketAddr,
        source: io::Error,
    },
    Http {
        addr: SocketAddr,
        source: BoxError,
    },
    Timeout {
        addr: SocketAddr,
    },
    Status {
        addr: SocketAddr,
        status: StatusCode,
        body: String,
    },
    /// The Node refused, saying why.
    Refused {
        addr: SocketAddr,
        status: StatusCode,
        message: String,
    },
    Decode {
        addr: SocketAddr,
        source: serde_json::Error,
    },
    /// The Node speaks another major version of the protocol.
    Incompatible {
        addr: SocketAddr,
        theirs: Hello,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect { addr, source } => write!(f, "can't connect to {addr}: {source}"),
            Self::Http { addr, source } => write!(f, "request to {addr} failed: {source}"),
            Self::Timeout { addr } => write!(f, "{addr} didn't answer in time"),
            Self::Status { addr, status, body } => {
                write!(f, "{addr} answered {status}: {}", body.trim())
            }
            Self::Refused { message, .. } => f.write_str(message),
            Self::Decode { addr, source } => write!(f, "can't read {addr}'s answer: {source}"),
            Self::Incompatible { addr, theirs } => {
                let upgrade = if theirs.protocol < MAJOR {
                    "upgrade heighliner there"
                } else {
                    "upgrade heighliner here"
                };
                write!(
                    f,
                    "{addr} runs heighliner {} (protocol {}), but this Node runs {} (protocol {MAJOR}); {upgrade}",
                    theirs.heighliner,
                    theirs.protocol,
                    env!("CARGO_PKG_VERSION")
                )
            }
        }
    }
}

impl Error {
    /// Whether nothing answered at all, as when the device is offline or
    /// doesn't run heighliner.
    pub fn is_unreachable(&self) -> bool {
        matches!(self, Self::Connect { .. } | Self::Timeout { .. })
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect { source, .. } => Some(source),
            Self::Http { source, .. } => Some(source.as_ref()),
            Self::Decode { source, .. } => Some(source),
            Self::Timeout { .. }
            | Self::Status { .. }
            | Self::Refused { .. }
            | Self::Incompatible { .. } => None,
        }
    }
}

/// Sends `request` to the Node at `addr` and decodes its JSON answer.
async fn exchange<T: DeserializeOwned>(
    addr: SocketAddr,
    request: hyper::Request<Full<Bytes>>,
    timeout: Duration,
) -> Result<T, Error> {
    let exchange = async {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|source| Error::Connect { addr, source })?;
        let (status, body) = http::send(stream, &addr.to_string(), request)
            .await
            .map_err(|source| Error::Http { addr, source })?;
        if !status.is_success() {
            return Err(match serde_json::from_slice::<Refused>(&body) {
                Ok(refused) => Error::Refused {
                    addr,
                    status,
                    message: refused.error,
                },
                Err(_) => Error::Status {
                    addr,
                    status,
                    body: String::from_utf8_lossy(&body).into_owned(),
                },
            });
        }
        serde_json::from_slice(&body).map_err(|source| Error::Decode { addr, source })
    };
    tokio::time::timeout(timeout, exchange)
        .await
        .map_err(|_| Error::Timeout { addr })?
}

/// Sends a GET for `path` to the Node at `addr` and decodes its JSON answer.
async fn get_json<T: DeserializeOwned>(
    addr: SocketAddr,
    path: &str,
    timeout: Duration,
) -> Result<T, Error> {
    let request = hyper::Request::get(path)
        .body(Full::default())
        .expect("a valid request");
    exchange(addr, request, timeout).await
}

/// Asks the Node at `addr` which protocol it speaks.
pub async fn hello(addr: SocketAddr, timeout: Duration) -> Result<Hello, Error> {
    get_json(addr, HELLO_PATH, timeout).await
}

/// Pulls the records of the Node at `addr`, whose `hello` answer was
/// `theirs`, and returns them with how many records or versions couldn't be
/// read and were skipped. Refuses if it speaks another major version.
pub async fn pull(
    addr: SocketAddr,
    theirs: &Hello,
    timeout: Duration,
) -> Result<(Store, usize), Error> {
    if theirs.protocol != MAJOR {
        return Err(Error::Incompatible {
            addr,
            theirs: theirs.clone(),
        });
    }
    let records: Vec<serde_json::Value> = get_json(addr, RECORDS_PATH, timeout).await?;
    Ok(Store::decode_lenient(records))
}

/// The states of the Replicas of the Node at `addr`. A Node that doesn't
/// serve them yet fails.
pub async fn states(addr: SocketAddr, timeout: Duration) -> Result<NodeStates, Error> {
    get_json(addr, STATES_PATH, timeout).await
}

/// The Peripherals of the Node at `addr`. A Node whose heighliner is older
/// than Peripherals answers [`Error::Status`] with 404.
pub async fn peripherals(addr: SocketAddr, timeout: Duration) -> Result<NodePeripherals, Error> {
    get_json(addr, PERIPHERALS_PATH, timeout).await
}

/// Has the Node at `addr` carry out `command` on its Peripheral at
/// `address`.
pub async fn command(
    addr: SocketAddr,
    address: &Address,
    command: &Command,
    timeout: Duration,
) -> Result<(), Error> {
    let body = serde_json::to_vec(command).expect("commands serialize");
    let request = hyper::Request::post(peripheral_path(address))
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("a valid request");
    exchange(addr, request, timeout).await
}

impl Error {
    /// Whether the Node answered that it has no such route, as one running
    /// an older heighliner does.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::Status { status, .. } if *status == StatusCode::NOT_FOUND)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versioned_paths_use_the_major_version() {
        assert_eq!(RECORDS_PATH, format!("/node/v{MAJOR}/records"));
        assert_eq!(PERIPHERALS_PATH, format!("/node/v{MAJOR}/peripherals"));
    }

    #[test]
    fn incompatible_names_who_should_upgrade() {
        let addr: SocketAddr = "100.64.0.2:7130".parse().unwrap();
        let older = Error::Incompatible {
            addr,
            theirs: Hello {
                protocol: 0,
                heighliner: "0.0.1".into(),
            },
        };
        assert!(
            older.to_string().ends_with("upgrade heighliner there"),
            "{older}"
        );
    }
}
