//! The command line's client of this Node's daemon (see [`crate::api`]).
//!
//! It reaches the daemon at this device's Tailscale address, as any other
//! client does, and leaves every decision to it.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;

use crate::api::{self, AddReplica, Refused, SetPaused, Status, Track, Tracked};
use crate::http::{self, BoxError};
use crate::model::NodeId;
use crate::tailscale::{self, LocalApi};

/// How long a request may take.
const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum Error {
    Tailscale(tailscale::Error),
    /// This device has no Tailscale address.
    NoAddress,
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
            Self::NoAddress => {
                f.write_str("this device has no Tailscale address; is Tailscale up?")
            }
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
    addr: SocketAddr,
    me: NodeId,
}

impl Client {
    /// The daemon on this device, serving on `port`.
    pub async fn connect(tailscale: &impl LocalApi, port: u16) -> Result<Self, Error> {
        let me = tailscale.this_device().await.map_err(Error::Tailscale)?;
        let ip = *me.ips.first().ok_or(Error::NoAddress)?;
        Ok(Self {
            addr: SocketAddr::new(ip, port),
            me: me.id,
        })
    }

    /// This Node.
    pub fn me(&self) -> &NodeId {
        &self.me
    }

    async fn call<R: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
    ) -> Result<R, Error> {
        let addr = self.addr;
        let body = body.map_or_else(Bytes::new, |body| {
            Bytes::from(serde_json::to_vec(body).expect("requests serialize"))
        });
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(body))
            .expect("a valid request");
        let exchange = async {
            let stream = TcpStream::connect(addr)
                .await
                .map_err(|source| Error::Connect { addr, source })?;
            http::send(stream, &addr.to_string(), request)
                .await
                .map_err(|source| Error::Http { addr, source })
        };
        let (status, body) = tokio::time::timeout(TIMEOUT, exchange)
            .await
            .map_err(|_| Error::Timeout { addr })??;
        if !status.is_success() {
            let message = serde_json::from_slice::<Refused>(&body).map_or_else(
                |_| format!("the daemon refused: {status}"),
                |refused| refused.error,
            );
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

/// A `[node:]path` argument, split. A path on this Node that is relative
/// is made absolute from `cwd`; `~` is left for the daemon.
pub fn node_path(argument: &str, cwd: &Path) -> (Option<String>, String) {
    let local = |path: &str| {
        if path.starts_with('/') || path == "~" || path.starts_with("~/") {
            path.to_owned()
        } else {
            cwd.join(path).to_string_lossy().into_owned()
        }
    };
    match argument.split_once(':') {
        Some((node, path))
            if !node.is_empty()
                && !node.contains('/')
                && !argument.starts_with(['/', '~', '.']) =>
        {
            (Some(node.to_owned()), path.to_owned())
        }
        _ => (None, local(argument)),
    }
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
        let paused = if item.paused { ", paused" } else { "" };
        out.push_str(&format!(
            "{} ({}, {direction}{paused})\n",
            item.name, item.id
        ));
        if !item.excludes.is_empty() {
            out.push_str(&format!("  excludes {}\n", item.excludes.join(" ")));
        }
        for replica in &item.replicas {
            let node = replica
                .node_name
                .as_deref()
                .unwrap_or(replica.node.as_str());
            let mut line = format!("  {node}:{} {}", replica.path, replica.role);
            if replica.paused {
                line.push_str(", paused");
            }
            if let Some(state) = &replica.state {
                line.push_str(&format!(", {state}"));
            }
            out.push_str(&line);
            out.push('\n');
        }
    }
    if !status.problems.is_empty() {
        out.push_str("Problems:\n");
        for problem in &status.problems {
            out.push_str(&format!("  {problem}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests;
