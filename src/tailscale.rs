//! A small client for the Tailscale local API, which `tailscaled` serves on a
//! unix socket.
//!
//! The local API is not an officially stable interface, so this module keeps
//! to two read-only endpoints (`status` and `whois`) and to the fields
//! heighliner needs; everything else in their responses is ignored.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::net::UnixStream;

use crate::model::NodeId;

/// Where `tailscaled` listens on Linux.
pub const DEFAULT_SOCKET: &str = "/var/run/tailscale/tailscaled.sock";

/// How long a local API call may take.
const TIMEOUT: Duration = Duration::from_secs(5);

/// A Tailscale user ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(transparent)]
pub struct UserId(i64);

impl UserId {
    #[cfg(test)]
    pub(crate) const fn new(id: i64) -> Self {
        Self(id)
    }
}

/// The user who owns this device. Only their untagged devices can be Nodes
/// (ADR-0003): tagging a device moves its ownership to Tailscale's shared
/// tagged-devices user.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Owner(UserId);

impl Owner {
    /// Whether a device of `user`, with or without tags, is the owner's.
    pub fn owns(&self, user: UserId, tagged: bool) -> bool {
        !tagged && user == self.0
    }
}

/// A device on the tailnet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Device {
    pub id: NodeId,
    /// The machine name: the first label of its MagicDNS name.
    pub name: String,
    pub user: UserId,
    pub tagged: bool,
    pub ips: Vec<IpAddr>,
}

/// This device and the others on its tailnet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tailnet {
    pub me: Device,
    pub others: Vec<Device>,
}

impl Tailnet {
    /// The user who owns this device, unless it is tagged.
    pub fn owner(&self) -> Result<Owner, Error> {
        if self.me.tagged {
            return Err(Error::Tagged);
        }
        Ok(Owner(self.me.user))
    }

    /// The other devices the owner owns, which are the ones that can be
    /// Nodes.
    pub fn owned_devices(&self) -> Result<impl Iterator<Item = &Device>, Error> {
        let owner = self.owner()?;
        Ok(self
            .others
            .iter()
            .filter(move |device| owner.owns(device.user, device.tagged)))
    }
}

/// Who is on the other end of a tailnet connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub node: NodeId,
    /// The machine name, as in [`Device::name`].
    pub name: String,
    pub user: UserId,
    pub tagged: bool,
}

/// The parts of the local API heighliner uses. [`Client`] talks to
/// `tailscaled`; tests use a fake.
pub trait LocalApi: Send + Sync + 'static {
    fn status(&self) -> impl Future<Output = Result<Tailnet, Error>> + Send;

    /// Identifies the device a TCP connection from `addr` comes from, or
    /// `None` if `addr` isn't on the tailnet.
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
        source: io::Error,
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
    /// `tailscaled` is up but not connected to the tailnet, for example
    /// because it needs a login.
    NotConnected {
        state: String,
    },
    /// This device is tagged, so it isn't owned by a user.
    Tagged,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect { socket, source } => {
                let hint = if source.kind() == io::ErrorKind::PermissionDenied {
                    "check that this user may use it"
                } else {
                    "is tailscaled running?"
                };
                write!(
                    f,
                    "can't connect to tailscaled at {}: {source}; {hint}",
                    socket.display()
                )
            }
            Self::Http { path, source } => write!(f, "tailscaled request {path} failed: {source}"),
            Self::Timeout { path } => write!(
                f,
                "tailscaled didn't answer {path} within {}s",
                TIMEOUT.as_secs()
            ),
            Self::Status { path, status, body } => {
                write!(f, "tailscaled answered {path} with {status}: {}", body.trim())
            }
            Self::Decode { path, source } => {
                write!(f, "can't read tailscaled's answer to {path}: {source}")
            }
            Self::NotConnected { state } => write!(
                f,
                "tailscale is not connected (state {state}); run `tailscale up`"
            ),
            Self::Tagged => f.write_str(
                "this device is tagged, but heighliner only runs on devices owned by a user; remove its tags",
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
            Self::Timeout { .. }
            | Self::Status { .. }
            | Self::NotConnected { .. }
            | Self::Tagged => None,
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
}

/// Decodes a successful JSON answer to `path`.
fn decode<T: DeserializeOwned>(path: &str, status: StatusCode, body: &[u8]) -> Result<T, Error> {
    if !status.is_success() {
        return Err(Error::Status {
            path: path.to_owned(),
            status,
            body: String::from_utf8_lossy(body).into_owned(),
        });
    }
    serde_json::from_slice(body).map_err(|source| Error::Decode {
        path: path.to_owned(),
        source,
    })
}

impl LocalApi for Client {
    async fn status(&self) -> Result<Tailnet, Error> {
        const PATH: &str = "/localapi/v0/status";
        let (status, body) = self.get(PATH).await?;
        // Without a network map, the rest of the answer holds zero values,
        // so look at the state first.
        let BackendState { backend_state } = decode(PATH, status, &body)?;
        if backend_state != "Running" {
            return Err(Error::NotConnected {
                state: backend_state,
            });
        }
        let status: StatusResponse = decode(PATH, status, &body)?;
        Ok(status.into_tailnet())
    }

    async fn whois(&self, addr: SocketAddr) -> Result<Option<Identity>, Error> {
        let path = format!(
            "/localapi/v0/whois?addr={}&proto=tcp",
            percent_encode(&addr.to_string())
        );
        let (status, body) = self.get(&path).await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let whois: WhoIsResponse = decode(&path, status, &body)?;
        Ok(Some(whois.into_identity()))
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

/// The machine name in a MagicDNS name such as `laptop.tailnet.ts.net.`.
fn machine_name(dns_name: &str) -> String {
    dns_name.split('.').next().unwrap_or_default().to_owned()
}

fn is_tagged(tags: Option<Vec<String>>) -> bool {
    tags.is_some_and(|tags| !tags.is_empty())
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BackendState {
    backend_state: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct StatusResponse {
    #[serde(rename = "Self")]
    me: PeerStatus,
    /// Keyed by node key.
    peer: Option<BTreeMap<String, PeerStatus>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PeerStatus {
    /// The stable node ID.
    #[serde(rename = "ID")]
    id: NodeId,
    #[serde(rename = "DNSName", default)]
    dns_name: String,
    #[serde(rename = "UserID")]
    user_id: UserId,
    tags: Option<Vec<String>>,
    #[serde(rename = "TailscaleIPs")]
    tailscale_ips: Option<Vec<IpAddr>>,
}

impl PeerStatus {
    fn into_device(self) -> Device {
        Device {
            name: machine_name(&self.dns_name),
            id: self.id,
            user: self.user_id,
            tagged: is_tagged(self.tags),
            ips: self.tailscale_ips.unwrap_or_default(),
        }
    }
}

impl StatusResponse {
    fn into_tailnet(self) -> Tailnet {
        Tailnet {
            me: self.me.into_device(),
            others: self
                .peer
                .unwrap_or_default()
                .into_values()
                .map(PeerStatus::into_device)
                .collect(),
        }
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
    tags: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct WhoIsUser {
    #[serde(rename = "ID")]
    id: UserId,
}

impl WhoIsResponse {
    fn into_identity(self) -> Identity {
        Identity {
            name: machine_name(&self.node.name),
            node: self.node.stable_id,
            user: self.user_profile.id,
            tagged: is_tagged(self.node.tags),
        }
    }
}

#[cfg(test)]
pub(crate) mod fake;
#[cfg(test)]
mod tests;
