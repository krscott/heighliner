//! The Syncthing driver.
//!
//! Each Node runs a Syncthing instance that heighliner owns completely
//! (ADR-0001). The driver reaches it through its REST API on a unix socket,
//! derives the whole configuration from the Fleet's, and replaces whatever
//! differs, including manual edits. Syncthing moves the files; heighliner
//! never does.
//!
//! The configuration keeps Syncthing on the tailnet: discovery, relays, NAT
//! traversal, STUN, crash and usage reporting and upgrades are off, it
//! listens only on this device's Tailscale addresses, and every other
//! device is reached at its Tailscale addresses. A device whose addresses
//! aren't known isn't configured, since Syncthing would fall back to
//! discovering it.
//!
//! Safety rules:
//! - A Replica is first set up at an empty or missing path, or with its
//!   `adopt` flag. A path that already holds files is otherwise refused.
//! - Removing a Replica only removes its folder from Syncthing; the files
//!   stay (Syncthing removes only its `.stfolder` marker). Heighliner never
//!   removes the marker of a configured folder, so a missing or unmounted
//!   path is an error rather than everything having been deleted.
//! - Overlapping Replicas aren't synced.
//! - Syncthing's "Override Changes" and "Revert Local Changes" are never
//!   used.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::UnixStream;

use crate::config::{Config, LocalReplica};
use crate::http::{self, BoxError};
use crate::model::{AbsPath, DeviceId, InvalidValue, ItemId, NodeId, Role};
use crate::tailscale::Tailnet;

/// The port Syncthing listens on unless configured otherwise. Every Node
/// must use the same one.
pub const DEFAULT_PORT: u16 = 22000;

/// How long a REST call may take.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The file holding an Item's exclude patterns in each of its Replicas.
const IGNORE_FILE: &str = ".stignore";

/// Why a call to Syncthing failed.
#[derive(Debug)]
pub enum Error {
    Connect {
        socket: PathBuf,
        source: io::Error,
    },
    Http {
        path: String,
        source: BoxError,
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
    /// Syncthing's answer lacked something heighliner relies on.
    Unexpected {
        path: String,
        what: &'static str,
    },
    DeviceId(InvalidValue),
}

impl Error {
    /// Whether Syncthing may simply not be up yet.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Connect { .. } | Self::Timeout { .. })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect { socket, source } => write!(
                f,
                "can't connect to Syncthing at {}: {source}; is it running?",
                socket.display()
            ),
            Self::Http { path, source } => write!(f, "Syncthing request {path} failed: {source}"),
            Self::Timeout { path } => write!(
                f,
                "Syncthing didn't answer {path} within {}s",
                TIMEOUT.as_secs()
            ),
            Self::Status { path, status, body } => {
                write!(
                    f,
                    "Syncthing answered {path} with {status}: {}",
                    body.trim()
                )
            }
            Self::Decode { path, source } => {
                write!(f, "can't read Syncthing's answer to {path}: {source}")
            }
            Self::Unexpected { path, what } => {
                write!(f, "Syncthing's answer to {path} has no {what}")
            }
            Self::DeviceId(error) => write!(f, "Syncthing reported {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect { source, .. } => Some(source),
            Self::Http { source, .. } => Some(source.as_ref()),
            Self::Decode { source, .. } => Some(source),
            Self::DeviceId(error) => Some(error),
            Self::Timeout { .. } | Self::Status { .. } | Self::Unexpected { .. } => None,
        }
    }
}

/// A client for Syncthing's REST API on its GUI unix socket.
#[derive(Clone, Debug)]
pub struct Client {
    socket: PathBuf,
    api_key: String,
}

impl Client {
    pub fn new(socket: impl Into<PathBuf>, api_key: impl Into<String>) -> Self {
        Self {
            socket: socket.into(),
            api_key: api_key.into(),
        }
    }

    /// Sends a request and returns the body of a successful answer.
    pub(crate) async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Bytes, Error> {
        let request = async {
            let stream =
                UnixStream::connect(&self.socket)
                    .await
                    .map_err(|source| Error::Connect {
                        socket: self.socket.clone(),
                        source,
                    })?;
            let body = body.map_or_else(Bytes::new, |body| {
                Bytes::from(serde_json::to_vec(body).expect("JSON values serialize"))
            });
            let request = Request::builder()
                .method(method)
                .uri(path)
                .header("X-API-Key", &self.api_key)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Full::new(body))
                .expect("the request is well-formed");
            http::send(stream, "localhost", request)
                .await
                .map_err(|source| Error::Http {
                    path: path.to_owned(),
                    source,
                })
        };
        let (status, body) =
            tokio::time::timeout(TIMEOUT, request)
                .await
                .map_err(|_| Error::Timeout {
                    path: path.to_owned(),
                })??;
        if !status.is_success() {
            return Err(Error::Status {
                path: path.to_owned(),
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        Ok(body)
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, Error> {
        let body = self.call(Method::GET, path, None).await?;
        serde_json::from_slice(&body).map_err(|source| Error::Decode {
            path: path.to_owned(),
            source,
        })
    }

    /// This instance's device ID.
    pub async fn device_id(&self) -> Result<DeviceId, Error> {
        const PATH: &str = "/rest/system/status";
        let status: Value = self.get(PATH).await?;
        let id = status["myID"].as_str().ok_or(Error::Unexpected {
            path: PATH.to_owned(),
            what: "myID",
        })?;
        DeviceId::new(id).map_err(Error::DeviceId)
    }

    async fn ignores(&self, folder: ItemId) -> Result<Vec<String>, Error> {
        let path = format!("/rest/db/ignores?folder={folder}");
        let answer: Value = self.get(&path).await?;
        Ok(match &answer["ignore"] {
            Value::Array(lines) => lines
                .iter()
                .filter_map(|line| line.as_str().map(str::to_owned))
                .collect(),
            _ => Vec::new(),
        })
    }

    async fn set_ignores(&self, folder: ItemId, lines: &[String]) -> Result<(), Error> {
        let path = format!("/rest/db/ignores?folder={folder}");
        self.call(Method::POST, &path, Some(&json!({ "ignore": lines })))
            .await?;
        Ok(())
    }
}

/// What happened to one of this Node's Replicas in a reconcile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Its folder is configured as it should be.
    Configured,
    /// It overlaps another Replica on this Node, so it isn't synced.
    Overlaps,
    /// Its path already holds files, and the Replica doesn't adopt them.
    NotEmpty,
    /// Its path can't be used.
    PathError(String),
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configured => f.write_str("configured"),
            Self::Overlaps => {
                f.write_str("overlaps another replica on this Node, so it isn't synced")
            }
            Self::NotEmpty => f.write_str(
                "its path already holds files; add the replica again with adopt to merge them",
            ),
            Self::PathError(error) => write!(f, "its path can't be used: {error}"),
        }
    }
}

/// The outcome of a reconcile.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Report {
    pub replicas: BTreeMap<ItemId, Outcome>,
    /// Whether Syncthing's configuration was changed.
    pub changed: bool,
}

/// Configures this Node's Syncthing from the Fleet's configuration.
#[derive(Clone, Debug)]
pub struct Driver {
    client: Client,
    port: u16,
    /// Clean-out age for Displaced Versions, in days.
    retention_days: u64,
}

impl Driver {
    pub fn new(client: Client, port: u16, retention: Duration) -> Self {
        Self {
            client,
            port,
            retention_days: retention.as_secs().div_ceil(24 * 60 * 60).max(1),
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Makes Syncthing's configuration match `config` for this device, as
    /// `tailnet` describes it.
    pub async fn reconcile(&self, config: &Config, tailnet: &Tailnet) -> Result<Report, Error> {
        let me = &tailnet.me().id;
        let my_device = self.client.device_id().await?;
        let current: Value = self.client.get("/rest/config").await?;
        let folder_template: Value = self.client.get("/rest/config/defaults/folder").await?;
        let device_template: Value = self.client.get("/rest/config/defaults/device").await?;

        let existing: BTreeMap<String, String> = current["folders"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|folder| {
                Some((
                    folder["id"].as_str()?.to_owned(),
                    folder["path"].as_str()?.to_owned(),
                ))
            })
            .collect();

        let mut report = Report::default();
        let mut folders = Vec::new();
        let mut peers: BTreeSet<NodeId> = BTreeSet::new();
        for local in config.replicas_on(me) {
            if local.overlaps {
                report.replicas.insert(local.item_id, Outcome::Overlaps);
                continue;
            }
            let path = local.replica.path();
            let known =
                existing.get(&local.item_id.to_string()).map(String::as_str) == Some(path.as_str());
            if !known && let Err(outcome) = prepare(path, local.replica.adopt, &excludes(&local)) {
                report.replicas.insert(local.item_id, outcome);
                continue;
            }
            let devices: Vec<NodeId> = local
                .entry
                .replicas
                .keys()
                .filter(|node| *node != me && self.reachable(config, tailnet, node).is_some())
                .cloned()
                .collect();
            peers.extend(devices.iter().cloned());
            folders.push(self.folder(&folder_template, &local, &my_device, config, &devices));
            report.replicas.insert(local.item_id, Outcome::Configured);
        }

        let mut this_device = self_device(&current, &device_template, &my_device);
        this_device["addresses"] = Value::Array(self.addresses(&tailnet.me().ips));
        let mut devices = vec![this_device];
        for node in &peers {
            let (device_id, ips) = self
                .reachable(config, tailnet, node)
                .expect("filtered above");
            let name = tailnet
                .others()
                .iter()
                .find(|device| &device.id == node)
                .map_or_else(|| node.to_string(), |device| device.name.clone());
            devices.push(self.device(&device_template, device_id, &name, &ips));
        }

        let mut desired = current.clone();
        harden(&mut desired["options"], &self.addresses(&tailnet.me().ips));
        desired["devices"] = Value::Array(devices);
        desired["folders"] = Value::Array(folders);
        if normalized(&desired) != normalized(&current) {
            self.client
                .call(Method::PUT, "/rest/config", Some(&desired))
                .await?;
            report.changed = true;
        }

        for local in config.replicas_on(me) {
            if report.replicas.get(&local.item_id) != Some(&Outcome::Configured) {
                continue;
            }
            let wanted = excludes(&local);
            if self.client.ignores(local.item_id).await? != wanted {
                self.client.set_ignores(local.item_id, &wanted).await?;
                report.changed = true;
            }
        }
        Ok(report)
    }

    /// The device ID and Tailscale addresses of `node`, if both are known.
    fn reachable<'a>(
        &self,
        config: &'a Config,
        tailnet: &Tailnet,
        node: &NodeId,
    ) -> Option<(&'a DeviceId, Vec<IpAddr>)> {
        let info = config.nodes.get(node)?;
        let device = tailnet.others().iter().find(|device| &device.id == node)?;
        (!device.ips.is_empty()).then(|| (&info.device_id, device.ips.clone()))
    }

    fn addresses(&self, ips: &[IpAddr]) -> Vec<Value> {
        ips.iter()
            .map(|ip| {
                let host = match ip {
                    IpAddr::V4(ip) => ip.to_string(),
                    IpAddr::V6(ip) => format!("[{ip}]"),
                };
                Value::String(format!("tcp://{host}:{}", self.port))
            })
            .collect()
    }

    fn device(&self, template: &Value, id: &DeviceId, name: &str, ips: &[IpAddr]) -> Value {
        let mut device = template.clone();
        device["deviceID"] = json!(id.as_str());
        device["name"] = json!(name);
        device["addresses"] = Value::Array(self.addresses(ips));
        device["introducer"] = json!(false);
        device["autoAcceptFolders"] = json!(false);
        device["paused"] = json!(false);
        device
    }

    fn folder(
        &self,
        template: &Value,
        local: &LocalReplica<'_>,
        my_device: &DeviceId,
        config: &Config,
        peers: &[NodeId],
    ) -> Value {
        // Complete entries, as Syncthing stores them, so that an unchanged
        // folder compares equal.
        let shared = |id: &DeviceId| json!({ "deviceID": id.as_str(), "introducedBy": "", "encryptionPassword": "" });
        let mut devices = vec![shared(my_device)];
        devices.extend(
            peers
                .iter()
                .map(|node| shared(&config.nodes[node].device_id)),
        );
        let mut folder = template.clone();
        folder["id"] = json!(local.item_id.to_string());
        folder["label"] = json!(local.entry.item.name);
        folder["path"] = json!(local.replica.path().as_str());
        folder["type"] = json!(match local.role {
            Role::Source => "sendonly",
            Role::Mirror => "receiveonly",
            Role::Peer => "sendreceive",
        });
        folder["devices"] = Value::Array(devices);
        folder["paused"] = json!(local.paused);
        folder["versioning"] = json!({
            "type": "trashcan",
            "params": { "cleanoutDays": self.retention_days.to_string() },
            "cleanupIntervalS": 3600,
            "fsPath": "",
            "fsType": "basic",
        });
        folder
    }
}

/// An Item's exclude patterns, as `.stignore` lines.
fn excludes(local: &LocalReplica<'_>) -> Vec<String> {
    local
        .entry
        .item
        .excludes
        .iter()
        .map(|pattern| pattern.as_str().to_owned())
        .collect()
}

/// Gets a new Replica's path ready: creates it if it's missing, refuses it
/// if it holds files the Replica doesn't adopt, and writes the exclude
/// patterns before Syncthing first scans it.
fn prepare(path: &AbsPath, adopt: bool, excludes: &[String]) -> Result<(), Outcome> {
    let path = Path::new(path.as_str());
    let error = |error: io::Error| Outcome::PathError(error.to_string());
    match fs::read_dir(path) {
        Ok(mut entries) => {
            if !adopt && entries.next().is_some() {
                return Err(Outcome::NotEmpty);
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => fs::create_dir_all(path).map_err(error)?,
        Err(e) => return Err(error(e)),
    }
    let lines: String = excludes.iter().map(|line| format!("{line}\n")).collect();
    fs::write(path.join(IGNORE_FILE), lines).map_err(error)
}

/// This device's entry, kept as Syncthing has it, or made from `template`.
fn self_device(current: &Value, template: &Value, id: &DeviceId) -> Value {
    current["devices"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|device| device["deviceID"] == id.as_str())
        .cloned()
        .unwrap_or_else(|| {
            let mut device = template.clone();
            device["deviceID"] = json!(id.as_str());
            device
        })
}

/// Sets the options that keep Syncthing on the tailnet.
fn harden(options: &mut Value, listen: &[Value]) {
    for (key, value) in [
        ("listenAddresses", Value::Array(listen.to_vec())),
        ("globalAnnounceEnabled", json!(false)),
        ("globalAnnounceServers", json!([])),
        ("localAnnounceEnabled", json!(false)),
        ("relaysEnabled", json!(false)),
        ("natEnabled", json!(false)),
        ("stunKeepaliveStartS", json!(0)),
        ("crashReportingEnabled", json!(false)),
        ("urAccepted", json!(-1)),
        ("autoUpgradeIntervalH", json!(0)),
        ("announceLANAddresses", json!(false)),
        ("startBrowser", json!(false)),
    ] {
        options[key] = value;
    }
}

/// `config` with its device and folder lists sorted, so that their order
/// doesn't count as a difference.
fn normalized(config: &Value) -> Value {
    let mut config = config.clone();
    for (list, key) in [("devices", "deviceID"), ("folders", "id")] {
        if let Value::Array(entries) = &mut config[list] {
            entries.sort_by(|a, b| a[key].as_str().cmp(&b[key].as_str()));
            for entry in entries.iter_mut() {
                if let Value::Array(devices) = &mut entry["devices"] {
                    devices.sort_by(|a, b| a["deviceID"].as_str().cmp(&b["deviceID"].as_str()));
                }
            }
        }
    }
    config
}

#[cfg(test)]
pub(crate) mod test_instance;
#[cfg(test)]
mod tests;
