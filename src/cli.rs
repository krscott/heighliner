//! The command line's client of this Node's daemon (see [`crate::api`]).
//!
//! It reaches the daemon at this device's Tailscale address, as any other
//! client does, and leaves every decision to it.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;

use crate::api::{
    self, AddReplica, Reach, Refused, Replication, SetPaused, Status, Track, Tracked,
};
use crate::http::{self, BoxError};
use crate::model::{NodeId, ReplicaState};
use crate::tailscale::{self, LocalApi};

/// How long a request may take.
const TIMEOUT: Duration = Duration::from_secs(30);

/// How long the daemon may take to act on a Peripheral, which can involve
/// another Node scanning for it, then pairing and connecting it.
const PERIPHERAL_TIMEOUT: Duration = Duration::from_secs(300);

/// How long connecting to one of the daemon's addresses may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug)]
pub enum Error {
    Tailscale(tailscale::Error),
    /// This device, or the Node asked for, has no Tailscale address.
    NoAddress,
    /// No Node, or more than one, has that name.
    UnknownNode(String),
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
    /// The daemon refused the request.
    Refused {
        status: StatusCode,
        message: String,
    },
    Decode {
        source: serde_json::Error,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tailscale(error) => write!(f, "can't ask Tailscale for this device: {error}"),
            Self::UnknownNode(name) => {
                write!(
                    f,
                    "no one Node of the owner's on the tailnet is named {name}"
                )
            }
            Self::NoAddress => f.write_str("the device has no Tailscale address; is Tailscale up?"),
            Self::Connect { addr, source } => write!(
                f,
                "can't reach this Node's daemon at {addr}: {source}; is it running?"
            ),
            Self::Http { addr, source } => write!(f, "talking to the daemon at {addr}: {source}"),
            Self::Timeout { addr } => write!(f, "the daemon at {addr} didn't answer in time"),
            Self::Refused { message, .. } => f.write_str(message),
            Self::Decode { source } => write!(f, "the daemon's answer can't be read: {source}"),
        }
    }
}

impl std::error::Error for Error {}

/// This Node's daemon.
pub struct Client {
    /// The device's Tailscale addresses, tried in turn, starting with the
    /// one that last connected.
    addrs: Vec<SocketAddr>,
    last: AtomicUsize,
    me: NodeId,
    name: String,
    /// Whether the daemon is this device's.
    here: bool,
}

impl Client {
    /// The daemon on this device, or on the Node `on` names by machine
    /// name or node ID, serving on `port`.
    pub async fn connect(
        tailscale: &impl LocalApi,
        port: u16,
        on: Option<&str>,
    ) -> Result<Self, Error> {
        let (device, here) = match on {
            None => (
                tailscale.this_device().await.map_err(Error::Tailscale)?,
                true,
            ),
            Some(name) => {
                let tailnet = tailscale.status().await.map_err(Error::Tailscale)?;
                let mut matching = std::iter::once(tailnet.me())
                    .chain(tailnet.owned_devices())
                    .filter(|device| device.name == name || device.id.as_str() == name);
                match (matching.next(), matching.next()) {
                    (Some(device), None) => (device.clone(), device.id == tailnet.me().id),
                    _ => return Err(Error::UnknownNode(name.to_owned())),
                }
            }
        };
        if device.ips.is_empty() {
            return Err(Error::NoAddress);
        }
        Ok(Self {
            addrs: device
                .ips
                .iter()
                .map(|ip| SocketAddr::new(*ip, port))
                .collect(),
            last: AtomicUsize::new(0),
            me: device.id,
            name: device.name,
            here,
        })
    }

    /// The Node whose daemon this is.
    pub fn me(&self) -> &NodeId {
        &self.me
    }

    /// Its machine name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether the daemon is this device's, whose paths are this machine's.
    pub fn is_here(&self) -> bool {
        self.here
    }

    /// The daemon's web UI, at the address that last connected.
    pub fn url(&self) -> String {
        format!("http://{}/", self.addrs[self.last.load(Ordering::Relaxed)])
    }

    pub async fn conflicts(&self) -> Result<api::Conflicts, Error> {
        self.call(Method::GET, api::CONFLICTS, None::<&()>).await
    }

    pub async fn keep(&self, resolve: &api::Resolve) -> Result<(), Error> {
        self.call(Method::POST, api::KEEP, Some(resolve)).await
    }

    pub async fn swap(&self, resolve: &api::Resolve) -> Result<(), Error> {
        self.call(Method::POST, api::SWAP, Some(resolve)).await
    }

    pub async fn leave(&self) -> Result<(), Error> {
        let leave = api::Leave { confirm: true };
        self.call(Method::POST, api::LEAVE, Some(&leave)).await
    }

    pub async fn peripherals(&self) -> Result<api::Peripherals, Error> {
        self.call(Method::GET, api::PERIPHERALS, None::<&()>).await
    }

    /// Carries out `action` on `peripheral`, which can take a while, as
    /// when pairing scans for it.
    pub async fn act_on_peripheral(
        &self,
        peripheral: &str,
        action: &api::PeripheralAction,
    ) -> Result<api::Done, Error> {
        let path = api::peripheral(peripheral);
        self.call_for(Method::POST, &path, Some(action), PERIPHERAL_TIMEOUT)
            .await
    }

    async fn call<R: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
    ) -> Result<R, Error> {
        self.call_for(method, path, body, TIMEOUT).await
    }

    /// Sends a request, and waits up to `timeout` for its answer.
    async fn call_for<R: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
        timeout: Duration,
    ) -> Result<R, Error> {
        let body = body.map_or_else(Bytes::new, |body| {
            Bytes::from(serde_json::to_vec(body).expect("requests serialize"))
        });
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(body))
            .expect("a valid request");
        // The daemon may not have every address, so each is tried until
        // one connects; the request is sent only once.
        let mut connected = None;
        let mut failed = None;
        let first = self.last.load(Ordering::Relaxed);
        for i in (first..self.addrs.len()).chain(0..first) {
            let addr = self.addrs[i];
            match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    self.last.store(i, Ordering::Relaxed);
                    connected = Some((addr, stream));
                    break;
                }
                Ok(Err(source)) => failed = Some(Error::Connect { addr, source }),
                Err(_) => failed = Some(Error::Timeout { addr }),
            }
        }
        let Some((addr, stream)) = connected else {
            return Err(failed.expect("an address was tried"));
        };
        let host = addr.to_string();
        let exchange = http::send(stream, &host, request);
        let (status, body) = tokio::time::timeout(timeout, exchange)
            .await
            .map_err(|_| Error::Timeout { addr })?
            .map_err(|source| Error::Http { addr, source })?;
        if !status.is_success() {
            let message = match serde_json::from_slice::<Refused>(&body) {
                Ok(refused) => refused.error,
                Err(_) => match String::from_utf8_lossy(&body).trim() {
                    "" => format!("the daemon refused: {status}"),
                    text => format!("the daemon refused: {text}"),
                },
            };
            return Err(Error::Refused { status, message });
        }
        serde_json::from_slice(&body).map_err(|source| Error::Decode { source })
    }

    pub async fn status(&self) -> Result<Status, Error> {
        self.call(Method::GET, api::STATUS, None::<&()>).await
    }

    pub async fn track(&self, track: &Track) -> Result<Tracked, Error> {
        self.call(Method::POST, api::ITEMS, Some(track)).await
    }

    pub async fn add_replica(&self, item: &str, add: &AddReplica) -> Result<(), Error> {
        self.call(Method::POST, &api::replicas(item), Some(add))
            .await
    }

    pub async fn remove_replica(&self, item: &str, node: &str) -> Result<(), Error> {
        self.call(Method::DELETE, &api::replica(item, node), None::<&()>)
            .await
    }

    pub async fn untrack(&self, item: &str) -> Result<(), Error> {
        self.call(Method::DELETE, &api::item(item), None::<&()>)
            .await
    }

    pub async fn pause_item(&self, item: &str, paused: bool) -> Result<(), Error> {
        self.call(Method::PATCH, &api::item(item), Some(&SetPaused { paused }))
            .await
    }

    pub async fn pause_replica(&self, item: &str, node: &str, paused: bool) -> Result<(), Error> {
        let path = api::replica(item, node);
        self.call(Method::PATCH, &path, Some(&SetPaused { paused }))
            .await
    }
}

/// A `[node:]path` argument, split. A path on the daemon's Node, which
/// `local` names (by machine name or node ID) or which has no node, is
/// made absolute from `cwd`, if this is that Node, when it is relative,
/// with each `..` resolved on this machine; `~` is left for the daemon.
pub fn node_path(argument: &str, cwd: Option<&Path>, local: &[&str]) -> (Option<String>, String) {
    let here = |path: &str| match cwd {
        _ if path == "~" || path.starts_with("~/") => path.to_owned(),
        Some(cwd) => api::normalize(&resolve_parents(&cwd.join(path)).to_string_lossy()),
        None if path.starts_with('/') => api::normalize(path),
        None => path.to_owned(),
    };
    match argument.split_once(':') {
        Some((node, path))
            if !node.is_empty()
                && !node.contains('/')
                && !argument.starts_with(['/', '~', '.']) =>
        {
            if local.contains(&node) {
                (None, here(path))
            } else {
                (Some(node.to_owned()), path.to_owned())
            }
        }
        _ => (None, here(argument)),
    }
}

/// `path` with each `..` resolved as the filesystem does, after following
/// the symlinks before it, as far as the path exists.
fn resolve_parents(path: &Path) -> PathBuf {
    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                if let Ok(real) = std::fs::canonicalize(&resolved) {
                    resolved = real;
                }
                resolved.pop();
            }
            component => resolved.push(component),
        }
    }
    resolved
}

/// `status` as text, one Item and its Replicas after another.
pub fn format_status(status: &Status) -> String {
    let mut out = String::new();
    if status.items.is_empty() {
        out.push_str("No items are tracked.\n");
    }
    for item in &status.items {
        let direction = match &item.source {
            Some(source) => {
                let name = item
                    .replicas
                    .iter()
                    .find(|replica| &replica.node == source)
                    .and_then(|replica| replica.node_name.as_deref())
                    .unwrap_or(source.as_str());
                format!("one-way from {name}")
            }
            None => "two-way".to_owned(),
        };
        let mut flags = String::new();
        for (set, flag) in [
            (item.paused, ", paused"),
            (item.resurrected, ", resurrected"),
            (item.duplicate_name, ", name shared"),
        ] {
            if set {
                flags.push_str(flag);
            }
        }
        out.push_str(&format!(
            "{} ({}, {direction}{flags})\n",
            item.name, item.id
        ));
        match &item.replication {
            Replication::Full => out.push_str("  fully replicated\n"),
            Replication::Unconfirmed { oldest: Some(at) } => out.push_str(&format!(
                "  not confirmed fully replicated; the oldest confirmation is from {at}\n"
            )),
            Replication::Unconfirmed { oldest: None } => out.push_str(
                "  not confirmed fully replicated; a replica was never confirmed current\n",
            ),
        }
        if !item.excludes.is_empty() {
            out.push_str(&format!("  excludes {}\n", item.excludes.join(" ")));
        }
        for replica in &item.replicas {
            let node = replica
                .node_name
                .as_deref()
                .unwrap_or(replica.node.as_str());
            let mut line = format!("  {node}:{} {}", replica.path, replica.role);
            if replica.departed {
                line.push_str(", departed");
            }
            if replica.paused {
                line.push_str(", paused");
            }
            let partly = if replica.state == Some(ReplicaState::Current) && !replica.every_sender {
                " with only the senders it reaches"
            } else {
                ""
            };
            match (&replica.state, replica.state_read_at) {
                (Some(state), None) => line.push_str(&format!(", {state}{partly}")),
                (Some(state), Some(at)) => {
                    line.push_str(&format!(", {state}{partly} (as of {at})"));
                }
                (None, _) => {}
            }
            out.push_str(&line);
            out.push('\n');
        }
    }
    if !status.nodes.is_empty() {
        out.push_str("Nodes:\n");
    }
    for node in &status.nodes {
        let name = node.name.as_deref().unwrap_or(node.node.as_str());
        let reach = match &node.reach {
            Reach::Here => "this Node".to_owned(),
            Reach::Live { .. } => "reachable".to_owned(),
            Reach::Unreachable { read_at: Some(at) } => format!("unreachable; last read at {at}"),
            Reach::Unreachable { read_at: None } => "never reached".to_owned(),
            Reach::Incompatible { protocol } => {
                let older = if *protocol < crate::protocol::MAJOR {
                    "upgrade it"
                } else {
                    "upgrade this Node"
                };
                format!(
                    "speaks protocol {protocol}, not {}; {older}",
                    crate::protocol::MAJOR
                )
            }
        };
        let departed = if node.departed { ", departed" } else { "" };
        out.push_str(&format!("  {name}: {reach}{departed}\n"));
    }
    if !status.problems.is_empty() {
        out.push_str("Problems:\n");
        for problem in &status.problems {
            out.push_str(&format!("  {problem}\n"));
        }
    }
    out
}

/// The Fleet's Peripherals as text: each one's Nodes and its state there,
/// then the Nodes that couldn't say.
pub fn format_peripherals(view: &api::Peripherals) -> String {
    let mut out = String::new();
    if view.peripherals.is_empty() {
        out.push_str("No Node that could be asked has a Peripheral paired.\n");
    }
    for peripheral in &view.peripherals {
        out.push_str(&format!("{} ({})\n", peripheral.alias, peripheral.address));
        for on in &peripheral.paired {
            let state = if on.connected {
                "connected"
            } else {
                "not connected"
            };
            let named = if on.alias == peripheral.alias {
                String::new()
            } else {
                format!(", named {:?}", on.alias)
            };
            out.push_str(&format!("  {}: {state}{named}\n", view.node_name(&on.node)));
        }
        for node in peripheral.pairable(&view.nodes) {
            out.push_str(&format!("  {}: not paired\n", view.node_name(&node.node)));
        }
    }
    let without: Vec<&str> = view
        .without_bluetooth()
        .map(|node| view.node_name(&node.node))
        .collect();
    if !without.is_empty() {
        out.push_str(&format!("Without Bluetooth: {}\n", without.join(", ")));
    }
    let unknown: Vec<String> = view
        .not_shown()
        .map(|(node, problem)| format!("  {} {problem}\n", view.node_name(&node.node)))
        .collect();
    if !unknown.is_empty() {
        out.push_str("Nodes not shown:\n");
        out.extend(unknown);
    }
    out
}

/// This Node's Data Conflicts as text.
pub fn format_conflicts(listed: &api::Conflicts) -> String {
    let mut out = String::new();
    if listed.conflicts.is_empty() {
        out.push_str("No data conflicts.\n");
    }
    for conflict in &listed.conflicts {
        out.push_str(&format!(
            "{} {}: device {}'s change won at {}; the other expires at {}\n  version {}\n",
            conflict.item_name,
            conflict.path,
            conflict.winner,
            conflict.local_time(),
            conflict.expires_at,
            conflict.version
        ));
    }
    for error in &listed.errors {
        out.push_str(&format!("Can't read the conflicts of {error}\n"));
    }
    out
}

#[cfg(test)]
mod tests;
