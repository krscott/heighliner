//! The Syncthing driver.
//!
//! Each Node runs a Syncthing instance that heighliner owns completely
//! (ADR-0001). The driver reaches it through its REST API on a unix socket
//! and replaces, whenever they differ, its options, devices and folders
//! with ones derived from the Fleet's configuration, including manual
//! edits. Folders and devices start from Syncthing's defaults, but every
//! field the rules below rely on is set explicitly. Syncthing moves the
//! files; heighliner never does.
//!
//! The configuration keeps Syncthing on the tailnet: discovery, relays, NAT
//! traversal, STUN, crash and usage reporting and upgrades are off, it
//! listens only on this device's Tailscale addresses, and every other Node
//! is reached at its Tailscale addresses. Only the owner's devices are
//! configured, and only when their addresses are known, since Syncthing
//! would otherwise fall back to discovering them.
//!
//! Safety rules:
//! - A Replica is first set up at an empty path, or at a missing one whose
//!   parent exists (so an unmounted disk isn't filled in its place), or
//!   with its `adopt` flag. A path that already holds files is otherwise
//!   refused, and so is one with its own `.stignore`. Once set up at a path,
//!   a Replica isn't refused there again.
//! - Removing a Replica only removes its folder from Syncthing; the files
//!   stay (Syncthing removes only its `.stfolder` marker). Heighliner never
//!   removes the marker of a configured folder and never recreates a
//!   configured folder's missing path, so a missing or unmounted path is an
//!   error rather than everything having been deleted.
//! - A mirror with local changes isn't made to send: it stays receive-only
//!   until the changes are dealt with.
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
use hyper::header::HeaderValue;
use hyper::{Method, Request, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::UnixStream;

use crate::config::{Config, LocalReplica};
use crate::http::{self, BoxError};
use crate::model::{AbsPath, DeviceId, InvalidValue, ItemId, NodeId, Role};
use crate::tailscale::{Device, Tailnet};

/// The port Syncthing listens on. Every Node uses the same one.
pub const PORT: u16 = 22000;

/// How long a REST call may take.
const TIMEOUT: Duration = Duration::from_secs(30);

/// How long inspecting or preparing a Replica's path may take, for example
/// on a hung network filesystem.
const PATH_TIMEOUT: Duration = Duration::from_secs(10);

/// The file holding an Item's exclude patterns in each of its Replicas.
const IGNORE_FILE: &str = ".stignore";

/// Syncthing's marker of a folder's root.
const MARKER: &str = ".stfolder";

/// How many Displaced Versions of one file to keep. The Retention Window
/// is what clears them out; this only bounds a file replaced very often.
const KEEP_VERSIONS: u32 = 10_000;

/// The Replicas that have been set up on this Node, with their paths. A
/// Replica in it isn't refused as not empty at that path again, for
/// example when its folder comes back after being left out for a while.
pub type Established = BTreeSet<(ItemId, AbsPath)>;

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
    /// The API key can't be sent in a request header.
    ApiKey,
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
            Self::ApiKey => {
                f.write_str("Syncthing's API key holds characters a request can't carry")
            }
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
            Self::Timeout { .. } | Self::Status { .. } | Self::Unexpected { .. } | Self::ApiKey => {
                None
            }
        }
    }
}

/// A client for Syncthing's REST API on its GUI unix socket.
#[derive(Clone, Debug)]
pub struct Client {
    socket: PathBuf,
    api_key: HeaderValue,
}

impl Client {
    pub fn new(socket: impl Into<PathBuf>, api_key: &str) -> Result<Self, Error> {
        let mut api_key = HeaderValue::from_str(api_key).map_err(|_| Error::ApiKey)?;
        api_key.set_sensitive(true);
        Ok(Self {
            socket: socket.into(),
            api_key,
        })
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
                .header("X-API-Key", self.api_key.clone())
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Full::new(body))
                .map_err(|source| Error::Http {
                    path: path.to_owned(),
                    source: source.into(),
                })?;
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

    pub(crate) async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, Error> {
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

    pub(crate) async fn ignores(&self, folder: ItemId) -> Result<Vec<String>, Error> {
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

    /// How many files a receive-only folder has changed locally.
    async fn local_changes(&self, folder: &str) -> Result<u64, Error> {
        let path = format!("/rest/db/status?folder={folder}");
        let status: Value = self.get(&path).await?;
        status["receiveOnlyTotalItems"]
            .as_u64()
            .ok_or(Error::Unexpected {
                path,
                what: "receiveOnlyTotalItems",
            })
    }
}

/// What happened to one of this Node's Replicas in a reconcile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Its folder is configured as it should be.
    Configured,
    /// It was a mirror and has local changes, so its folder stays
    /// receive-only instead of starting to send them.
    LocalChanges,
    /// It overlaps another Replica on this Node, so it isn't synced.
    Overlaps,
    /// Its path already holds files, and the Replica doesn't adopt them.
    NotEmpty,
    /// Its path has its own `.stignore`, which the Item's excludes would
    /// replace.
    OwnIgnoreFile,
    /// Its path, or its parent for a new Replica, or the marker of a
    /// configured one, is missing.
    Missing,
    /// Its path can't be used.
    PathError(String),
    /// Syncthing failed to apply part of it.
    Failed(String),
}

impl Outcome {
    /// Whether the Replica's folder is configured in Syncthing.
    pub fn is_configured(&self) -> bool {
        matches!(self, Self::Configured | Self::LocalChanges)
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configured => f.write_str("is configured"),
            Self::LocalChanges => f.write_str(
                "has changes made locally while it was a mirror, so it stays receive-only until they are dealt with",
            ),
            Self::Overlaps => f.write_str("overlaps another replica on this Node, so it isn't synced"),
            Self::NotEmpty => f.write_str(
                "can't start: its path already holds files; add the replica again with adopt to merge them",
            ),
            Self::OwnIgnoreFile => f.write_str(
                "can't start: its path has its own .stignore; move it aside, or copy its patterns into the item's excludes",
            ),
            Self::Missing => f.write_str("can't be synced: its path is missing; is its disk mounted?"),
            Self::PathError(error) => write!(f, "can't be synced: {error}"),
            Self::Failed(error) => write!(f, "couldn't be configured: {error}"),
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
    pub fn new(client: Client, retention: Duration) -> Self {
        Self::with_port(client, PORT, retention)
    }

    /// Uses another port than [`PORT`], for tests.
    pub(crate) fn with_port(client: Client, port: u16, retention: Duration) -> Self {
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
    /// `tailnet` describes it. `device_id` is Syncthing's, and
    /// `established` the Replicas already set up here.
    pub async fn reconcile(
        &self,
        config: &Config,
        tailnet: &Tailnet,
        device_id: &DeviceId,
        established: &Established,
    ) -> Result<Report, Error> {
        let me = &tailnet.me().id;
        let current: Value = self.client.get("/rest/config").await?;
        let folder_template = &current["defaults"]["folder"];
        let device_template = &current["defaults"]["device"];
        let existing: BTreeMap<&str, &Value> = current["folders"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|folder| Some((folder["id"].as_str()?, folder)))
            .collect();
        let peers: BTreeMap<&NodeId, (&DeviceId, &Device)> = tailnet
            .owned_devices()
            .filter(|device| !device.ips.is_empty())
            .filter_map(|device| {
                Some((
                    &device.id,
                    (&config.nodes.get(&device.id)?.device_id, device),
                ))
            })
            .collect();

        let mut report = Report::default();
        let mut folders = Vec::new();
        let mut shared_with: BTreeSet<&NodeId> = BTreeSet::new();
        let mut moved = BTreeSet::new();
        for local in config.replicas_on(me) {
            let id = local.item_id.to_string();
            let path = local.replica.path();
            if local.overlaps {
                report.replicas.insert(local.item_id, Outcome::Overlaps);
                continue;
            }
            let was = existing.get(id.as_str());
            let in_place = was.is_some_and(|folder| folder["path"] == path.as_str());
            if !in_place {
                let ready = if established.contains(&(local.item_id, path.clone())) {
                    in_blocking(check_exists, path.clone()).await
                } else {
                    let (adopt, excludes) = (local.replica.adopt, excludes(&local));
                    in_blocking(move |path| prepare(&path, adopt, &excludes), path.clone()).await
                };
                if let Err(outcome) = ready {
                    report.replicas.insert(local.item_id, outcome);
                    continue;
                }
                if was.is_some() {
                    moved.insert(id.clone());
                }
            }
            let mut outcome = Outcome::Configured;
            let mut folder_type = folder_type(local.role);
            if in_place
                && was.is_some_and(|folder| folder["type"] == "receiveonly")
                && folder_type != "receiveonly"
            {
                match self.client.local_changes(&id).await {
                    Ok(0) => {}
                    Ok(_) => {
                        folder_type = "receiveonly";
                        outcome = Outcome::LocalChanges;
                    }
                    Err(error) => {
                        folder_type = "receiveonly";
                        outcome = Outcome::Failed(error.to_string());
                    }
                }
            }
            let devices: Vec<&NodeId> = local
                .entry
                .replicas
                .keys()
                .filter(|node| *node != me && peers.contains_key(node))
                .collect();
            shared_with.extend(devices.iter().copied());
            folders.push(self.folder(
                folder_template,
                &local,
                folder_type,
                device_id,
                &peers,
                &devices,
            ));
            report.replicas.insert(local.item_id, outcome);
        }

        let mut this_device = current["devices"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|device| device["deviceID"] == device_id.as_str())
            .cloned()
            .unwrap_or_else(|| device_template.clone());
        this_device["deviceID"] = json!(device_id.as_str());
        this_device["addresses"] = Value::Array(self.addresses(&tailnet.me().ips));
        let mut devices = vec![this_device];
        for node in shared_with {
            let (peer_id, peer) = peers[node];
            devices.push(self.device(device_template, peer_id, peer));
        }

        let mut desired = current.clone();
        harden(&mut desired["options"], &self.addresses(&tailnet.me().ips));
        desired["devices"] = Value::Array(devices);
        desired["folders"] = Value::Array(folders);
        if !moved.is_empty() {
            // Syncthing keeps a folder's state across a change of path, so
            // drop the folder first to start afresh at the new one.
            let mut without = current.clone();
            if let Value::Array(folders) = &mut without["folders"] {
                folders
                    .retain(|folder| !folder["id"].as_str().is_some_and(|id| moved.contains(id)));
            }
            self.client
                .call(Method::PUT, "/rest/config", Some(&without))
                .await?;
        }
        if !moved.is_empty() || normalized(&desired) != normalized(&current) {
            self.client
                .call(Method::PUT, "/rest/config", Some(&desired))
                .await?;
            report.changed = true;
        }

        for local in config.replicas_on(me) {
            if !report.replicas[&local.item_id].is_configured() {
                continue;
            }
            match self.sync_ignores(&local).await {
                Ok(false) => {}
                Ok(true) => report.changed = true,
                Err(outcome) => {
                    report.replicas.insert(local.item_id, outcome);
                }
            }
        }
        Ok(report)
    }

    /// Sets a configured Replica's excludes, unless its path or marker is
    /// missing, since Syncthing would recreate the path. Returns whether
    /// they changed.
    async fn sync_ignores(&self, local: &LocalReplica<'_>) -> Result<bool, Outcome> {
        let wanted = excludes(local);
        let failed = |error: Error| Outcome::Failed(error.to_string());
        if self.client.ignores(local.item_id).await.map_err(failed)? == wanted {
            return Ok(false);
        }
        in_blocking(
            |path| {
                Path::new(path.as_str())
                    .join(MARKER)
                    .exists()
                    .then_some(())
                    .ok_or(Outcome::Missing)
            },
            local.replica.path().clone(),
        )
        .await?;
        self.client
            .set_ignores(local.item_id, &wanted)
            .await
            .map_err(failed)?;
        Ok(true)
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

    fn device(&self, template: &Value, id: &DeviceId, peer: &Device) -> Value {
        let mut device = template.clone();
        device["deviceID"] = json!(id.as_str());
        device["name"] = json!(peer.name);
        device["addresses"] = Value::Array(self.addresses(&peer.ips));
        device["allowedNetworks"] = json!([]);
        device["introducer"] = json!(false);
        device["autoAcceptFolders"] = json!(false);
        device["untrusted"] = json!(false);
        device["paused"] = json!(false);
        device
    }

    fn folder(
        &self,
        template: &Value,
        local: &LocalReplica<'_>,
        folder_type: &str,
        device_id: &DeviceId,
        peers: &BTreeMap<&NodeId, (&DeviceId, &Device)>,
        shared_with: &[&NodeId],
    ) -> Value {
        // Complete entries, as Syncthing stores them, so that an unchanged
        // folder compares equal.
        let entry = |id: &DeviceId| json!({ "deviceID": id.as_str(), "introducedBy": "", "encryptionPassword": "" });
        let mut devices = vec![entry(device_id)];
        devices.extend(shared_with.iter().map(|node| entry(peers[node].0)));
        let mut folder = template.clone();
        folder["id"] = json!(local.item_id.to_string());
        folder["label"] = json!(local.entry.item.name);
        folder["path"] = json!(local.replica.path().as_str());
        folder["filesystemType"] = json!("basic");
        folder["type"] = json!(folder_type);
        folder["devices"] = Value::Array(devices);
        folder["paused"] = json!(local.paused);
        folder["markerName"] = json!(MARKER);
        folder["ignoreDelete"] = json!(false);
        folder["versioning"] = json!({
            "type": "simple",
            "params": {
                "keep": KEEP_VERSIONS.to_string(),
                "cleanoutDays": self.retention_days.to_string(),
            },
            "cleanupIntervalS": 3600,
            "fsPath": "",
            "fsType": "basic",
        });
        folder
    }
}

fn folder_type(role: Role) -> &'static str {
    match role {
        Role::Source => "sendonly",
        Role::Mirror => "receiveonly",
        Role::Peer => "sendreceive",
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

/// Runs `check` on `path` away from the async runtime, so that a hung
/// filesystem only times out.
async fn in_blocking(
    check: impl FnOnce(AbsPath) -> Result<(), Outcome> + Send + 'static,
    path: AbsPath,
) -> Result<(), Outcome> {
    let task = tokio::task::spawn_blocking(move || check(path));
    match tokio::time::timeout(PATH_TIMEOUT, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(Outcome::PathError(error.to_string())),
        Err(_) => Err(Outcome::PathError(format!(
            "its path didn't answer within {}s",
            PATH_TIMEOUT.as_secs()
        ))),
    }
}

/// Checks that an established Replica's path is still there.
fn check_exists(path: AbsPath) -> Result<(), Outcome> {
    match fs::metadata(path.as_str()) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(Outcome::PathError("its path isn't a directory".into())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Err(Outcome::Missing),
        Err(error) => Err(Outcome::PathError(error.to_string())),
    }
}

/// Gets a new Replica's path ready: creates it if it's missing and its
/// parent exists, refuses it if it holds files the Replica doesn't adopt
/// or its own `.stignore`, and writes the exclude patterns before Syncthing
/// first scans it. Running it again on a path it prepared succeeds.
fn prepare(path: &AbsPath, adopt: bool, excludes: &[String]) -> Result<(), Outcome> {
    let path = Path::new(path.as_str());
    let error = |error: io::Error| Outcome::PathError(error.to_string());
    let lines: String = excludes.iter().map(|line| format!("{line}\n")).collect();
    match fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries {
                let name = entry.map_err(error)?.file_name();
                if name == MARKER || name == IGNORE_FILE {
                    continue;
                }
                if !adopt {
                    return Err(Outcome::NotEmpty);
                }
            }
            match fs::read_to_string(path.join(IGNORE_FILE)) {
                Ok(existing) if existing != lines => return Err(Outcome::OwnIgnoreFile),
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(error(e)),
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => match fs::create_dir(path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Outcome::Missing),
            Err(e) => return Err(error(e)),
        },
        Err(e) => return Err(error(e)),
    }
    fs::write(path.join(IGNORE_FILE), lines).map_err(error)
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
