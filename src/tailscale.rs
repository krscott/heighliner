//! A small client for the Tailscale local API, which `tailscaled` serves on a
//! unix socket.
//!
//! The local API is not an officially stable interface, so this module keeps
//! to two read-only endpoints (`status` and `whois`) and to the fields
//! heighliner needs; everything else in their responses is ignored.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::net::{TcpStream, UnixStream};
use tokio::task::JoinSet;

use crate::model::NodeId;

/// Where `tailscaled` listens on Linux.
pub const DEFAULT_SOCKET: &str = "/var/run/tailscale/tailscaled.sock";

/// How long a local API call may take.
const TIMEOUT: Duration = Duration::from_secs(5);

/// A Tailscale user ID. Tagged devices belong to a separate tagged-devices
/// user, so they never have the ID of a person.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(transparent)]
pub struct UserId(i64);

impl UserId {
    pub const fn new(id: i64) -> Self {
        Self(id)
    }
}

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A device on the tailnet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Device {
    pub id: NodeId,
    /// The machine name: the first label of its MagicDNS name.
    pub name: String,
    pub user: UserId,
    pub ips: Vec<IpAddr>,
    pub online: bool,
}

/// This device and the others it can see.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tailnet {
    pub me: Device,
    pub peers: Vec<Device>,
}

impl Tailnet {
    /// The other devices owned by this device's user, which are the ones
    /// that can be Nodes.
    pub fn owned_peers(&self) -> impl Iterator<Item = &Device> {
        self.peers.iter().filter(|peer| peer.user == self.me.user)
    }
}

/// Who is on the other end of a tailnet connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub node: NodeId,
    /// The machine name, as in [`Device::name`].
    pub name: String,
    pub user: UserId,
}

/// The parts of the local API heighliner uses. [`Client`] talks to
/// `tailscaled`; tests use a fake.
pub trait LocalApi: Send + Sync + 'static {
    fn status(&self) -> impl Future<Output = Result<Tailnet, Error>> + Send;

    /// Identifies the device a connection from `addr` comes from, or `None`
    /// if `addr` isn't on the tailnet.
    fn whois(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = Result<Option<Identity>, Error>> + Send;
}

/// Why a local API call failed.
#[derive(Debug)]
pub enum Error {
    Connect {
        socket: PathBuf,
        source: std::io::Error,
    },
    Http {
        path: String,
        source: hyper::Error,
    },
    Timeout {
        path: String,
    },
    Status {
        path: String,
        status: StatusCode,
        body: String,
    },
    Decode {
        path: String,
        source: serde_json::Error,
    },
    /// `tailscaled` is up but not connected, for example because it needs
    /// a login.
    NotRunning {
        state: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect { socket, source } => write!(
                f,
                "can't connect to tailscaled at {}: {source}; is tailscaled running?",
                socket.display()
            ),
            Self::Http { path, source } => write!(f, "tailscaled request {path} failed: {source}"),
            Self::Timeout { path } => write!(
                f,
                "tailscaled didn't answer {path} within {}s",
                TIMEOUT.as_secs()
            ),
            Self::Status { path, status, body } => {
                write!(
                    f,
                    "tailscaled answered {path} with {status}: {}",
                    body.trim()
                )
            }
            Self::Decode { path, source } => {
                write!(f, "can't read tailscaled's answer to {path}: {source}")
            }
            Self::NotRunning { state } => write!(
                f,
                "tailscale is not connected (state {state}); run `tailscale up`"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect { source, .. } => Some(source),
            Self::Http { source, .. } => Some(source),
            Self::Decode { source, .. } => Some(source),
            Self::Timeout { .. } | Self::Status { .. } | Self::NotRunning { .. } => None,
        }
    }
}

/// A client for the local API on a unix socket.
#[derive(Clone, Debug)]
pub struct Client {
    socket: PathBuf,
}

impl Client {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Sends a GET and returns the status and body.
    async fn get(&self, path: &str) -> Result<(StatusCode, Bytes), Error> {
        let request = async {
            let stream =
                UnixStream::connect(&self.socket)
                    .await
                    .map_err(|source| Error::Connect {
                        socket: self.socket.clone(),
                        source,
                    })?;
            let http = |source| Error::Http {
                path: path.to_owned(),
                source,
            };
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream))
                    .await
                    .map_err(http)?;
            tokio::spawn(connection);
            let request = Request::get(path)
                // tailscaled refuses local API requests with any other host.
                .header(hyper::header::HOST, "local-tailscaled.sock")
                .body(Empty::<Bytes>::new())
                .expect("the request is well-formed");
            let response = sender.send_request(request).await.map_err(http)?;
            let status = response.status();
            let body = response.into_body().collect().await.map_err(http)?;
            Ok((status, body.to_bytes()))
        };
        tokio::time::timeout(TIMEOUT, request)
            .await
            .map_err(|_| Error::Timeout {
                path: path.to_owned(),
            })?
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>, Error> {
        let (status, body) = self.get(path).await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(Error::Status {
                path: path.to_owned(),
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        serde_json::from_slice(&body)
            .map(Some)
            .map_err(|source| Error::Decode {
                path: path.to_owned(),
                source,
            })
    }
}

impl LocalApi for Client {
    async fn status(&self) -> Result<Tailnet, Error> {
        const PATH: &str = "/localapi/v0/status";
        let status: StatusResponse = self.get_json(PATH).await?.ok_or(Error::Status {
            path: PATH.to_owned(),
            status: StatusCode::NOT_FOUND,
            body: String::new(),
        })?;
        status.into_tailnet()
    }

    async fn whois(&self, addr: SocketAddr) -> Result<Option<Identity>, Error> {
        let path = format!(
            "/localapi/v0/whois?addr={}",
            percent_encode(&addr.to_string())
        );
        let response: Option<WhoIsResponse> = self.get_json(&path).await?;
        Ok(response.map(WhoIsResponse::into_identity))
    }
}

/// Percent-encodes everything but unreserved characters, as a query value.
fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

/// The machine name in a MagicDNS name such as `laptop.tailnet.ts.net.`,
/// falling back to the OS host name.
fn machine_name(dns_name: &str, host_name: &str) -> String {
    match dns_name.split('.').next() {
        Some(label) if !label.is_empty() => label.to_owned(),
        _ => host_name.to_owned(),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct StatusResponse {
    backend_state: String,
    #[serde(rename = "Self")]
    me: Option<PeerStatus>,
    /// Keyed by node key.
    #[serde(default)]
    peer: Option<BTreeMap<String, PeerStatus>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PeerStatus {
    /// The stable node ID.
    #[serde(rename = "ID")]
    id: NodeId,
    #[serde(default)]
    host_name: String,
    #[serde(rename = "DNSName", default)]
    dns_name: String,
    #[serde(rename = "UserID")]
    user_id: UserId,
    #[serde(rename = "TailscaleIPs", default)]
    tailscale_ips: Option<Vec<IpAddr>>,
    #[serde(default)]
    online: bool,
}

impl PeerStatus {
    fn into_device(self) -> Device {
        Device {
            name: machine_name(&self.dns_name, &self.host_name),
            id: self.id,
            user: self.user_id,
            ips: self.tailscale_ips.unwrap_or_default(),
            online: self.online,
        }
    }
}

impl StatusResponse {
    fn into_tailnet(self) -> Result<Tailnet, Error> {
        let Self {
            backend_state,
            me,
            peer,
        } = self;
        let Some(me) = me.filter(|_| backend_state == "Running") else {
            return Err(Error::NotRunning {
                state: backend_state,
            });
        };
        Ok(Tailnet {
            me: Device {
                online: true,
                ..me.into_device()
            },
            peers: peer
                .unwrap_or_default()
                .into_values()
                .map(PeerStatus::into_device)
                .collect(),
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct WhoIsResponse {
    node: WhoIsNode,
    user_profile: WhoIsUser,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct WhoIsNode {
    #[serde(rename = "StableID")]
    stable_id: NodeId,
    /// The MagicDNS name.
    #[serde(default)]
    name: String,
}

#[derive(Deserialize)]
struct WhoIsUser {
    #[serde(rename = "ID")]
    id: UserId,
}

impl WhoIsResponse {
    fn into_identity(self) -> Identity {
        Identity {
            name: machine_name(&self.node.name, ""),
            node: self.node.stable_id,
            user: self.user_profile.id,
        }
    }
}

/// Finds the devices owned by this device's user that accept TCP
/// connections on `port`, the port heighliner listens on. Each device's
/// addresses are tried in turn, each for at most `timeout`.
pub async fn discover(
    api: &impl LocalApi,
    port: u16,
    timeout: Duration,
) -> Result<Vec<Device>, Error> {
    let tailnet = api.status().await?;
    let mut probes = JoinSet::new();
    for device in tailnet.owned_peers().cloned() {
        probes.spawn(async move {
            for ip in &device.ips {
                let connect = TcpStream::connect(SocketAddr::new(*ip, port));
                if let Ok(Ok(_)) = tokio::time::timeout(timeout, connect).await {
                    return Some(device);
                }
            }
            None
        });
    }
    let mut found: Vec<Device> = probes.join_all().await.into_iter().flatten().collect();
    found.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(found)
}

#[cfg(test)]
pub(crate) mod fake;
#[cfg(test)]
mod tests;
