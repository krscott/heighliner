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
//!   refused, and so is one with its own `.stignore`. A new or moved folder
//!   waits until any other folder at an overlapping path, including its
//!   own old one, has been removed, since Syncthing removes a folder's
//!   marker with it and would carry its state across a change of path.
//! - A configured folder is removed only when its Replica is removed or
//!   its Item untracked. While it can't be synced for a while (its path is
//!   missing, it overlaps another, or its records can't be read or haven't
//!   arrived yet), it is paused instead, keeping its marker and index.
//! - Overlapping Replicas aren't synced, including ones whose paths
//!   overlap once symlinks are resolved.
//! - Removing a Replica only removes its folder from Syncthing; the files
//!   stay (Syncthing removes only its `.stfolder` marker). Heighliner never
//!   removes the marker of a configured folder and never recreates a
//!   configured folder's missing path, so a missing or unmounted path is an
//!   error rather than everything having been deleted.
//! - A configured folder's new excludes are applied before it runs with
//!   the rest of its new configuration, and while they can't be, it is
//!   paused; a new folder's are written before Syncthing first scans it.
//!   Heighliner replaces a `.stignore` only if it wrote it, or it holds
//!   exactly the excludes.
//! - A mirror is made to send only while it runs and right after a scan
//!   finds no local changes; otherwise it stays receive-only.
//! - Syncthing's "Override Changes" and "Revert Local Changes" are never
//!   used.
//! - A Data Conflict's losing copy is never deleted early: Syncthing keeps
//!   every one (`maxConflicts` -1), and heighliner moves it into its own
//!   ignored directory in the Replica, `.heighliner`, where it expires a
//!   Retention Window after the move. Conflict copies already in a path
//!   when it was set up are the user's files, and stay.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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

/// How long a scan, which a REST call waits for, may take.
const SCAN_TIMEOUT: Duration = Duration::from_secs(10 * 60);

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
        after: Duration,
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
            Self::Timeout { path, after } => write!(
                f,
                "Syncthing didn't answer {path} within {}s",
                after.as_secs()
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
        self.call_within(TIMEOUT, method, path, body).await
    }

    async fn call_within(
        &self,
        timeout: Duration,
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
            tokio::time::timeout(timeout, request)
                .await
                .map_err(|_| Error::Timeout {
                    path: path.to_owned(),
                    after: timeout,
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

    /// A folder's ignore patterns, and Syncthing's error if it can't parse
    /// them.
    pub(crate) async fn ignores(
        &self,
        folder: ItemId,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let path = format!("/rest/db/ignores?folder={folder}");
        let answer: Value = self.get(&path).await?;
        let lines = match &answer["ignore"] {
            Value::Array(lines) => lines
                .iter()
                .filter_map(|line| line.as_str().map(str::to_owned))
                .collect(),
            _ => Vec::new(),
        };
        Ok((lines, answer["error"].as_str().map(str::to_owned)))
    }

    async fn set_ignores(&self, folder: ItemId, lines: &[String]) -> Result<(), Error> {
        let path = format!("/rest/db/ignores?folder={folder}");
        self.call(Method::POST, &path, Some(&json!({ "ignore": lines })))
            .await?;
        Ok(())
    }

    /// Scans a folder and waits for the scan to finish.
    async fn scan(&self, folder: &str) -> Result<(), Error> {
        let path = format!("/rest/db/scan?folder={folder}");
        self.call_within(SCAN_TIMEOUT, Method::POST, &path, None)
            .await?;
        Ok(())
    }

    /// Pauses or resumes a folder.
    async fn set_folder_paused(&self, folder: ItemId, paused: bool) -> Result<(), Error> {
        let path = format!("/rest/config/folders/{folder}");
        self.call(Method::PATCH, &path, Some(&json!({ "paused": paused })))
            .await?;
        Ok(())
    }

    /// Whether a folder sends its local changes now: it is send-receive
    /// and running.
    pub(crate) async fn folder_sends(&self, folder: ItemId) -> Result<bool, Error> {
        let folder: Value = self.get(&format!("/rest/config/folders/{folder}")).await?;
        Ok(folder["type"] == "sendreceive" && folder["paused"] == false)
    }

    /// Scans one file or directory of a folder, and waits for the scan.
    pub(crate) async fn scan_path(&self, folder: ItemId, sub: &str) -> Result<(), Error> {
        let sub = http::percent_encode(sub);
        let path = format!("/rest/db/scan?folder={folder}&sub={sub}");
        self.call_within(SCAN_TIMEOUT, Method::POST, &path, None)
            .await?;
        Ok(())
    }

    /// How many files a receive-only folder has changed locally, as of its
    /// last scan.
    pub(crate) async fn local_changes(&self, folder: &str) -> Result<u64, Error> {
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
    /// It's new or moved, and waits for Syncthing to remove the folder at
    /// this path, another at an overlapping one or its own at its old one,
    /// first.
    Waiting(String),
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
    /// It or its Item was written by a newer heighliner and can't be read.
    Unreadable,
    /// Syncthing has a folder for it, but this Node doesn't know its Item,
    /// for example before it has pulled the other Nodes' records.
    Unknown,
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
                "has changes made locally while it was a mirror, so it stays receive-only; make it a mirror again, or undo the changes (Syncthing lists them as locally changed items)",
            ),
            Self::Overlaps => f.write_str("overlaps another replica on this Node, so it isn't synced"),
            Self::Waiting(path) => write!(
                f,
                "waits for the Syncthing folder at {path} to be removed first"
            ),
            Self::NotEmpty => f.write_str(
                "can't start: its path already holds files; add the replica again with adopt to merge them",
            ),
            Self::OwnIgnoreFile => f.write_str(
                "can't start: its path has its own .stignore; move it aside, or copy its patterns into the item's excludes",
            ),
            Self::Missing => f.write_str(
                "can't be synced: its path or Syncthing's marker in it is missing; is its disk mounted?",
            ),
            Self::PathError(error) => write!(f, "can't be synced: {error}"),
            Self::Unreadable => f.write_str(
                "can't be synced: its records were written by a newer heighliner; upgrade this Node",
            ),
            Self::Unknown => f.write_str(
                "is paused: this Node doesn't know its item, for example before it has pulled the other Nodes' records",
            ),
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
    /// Paths with a check still running, so that a hung filesystem holds
    /// one thread, not one more each reconcile.
    checking: Arc<Mutex<BTreeSet<AbsPath>>>,
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
            checking: Arc::default(),
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Makes Syncthing's configuration match `config` for this device, as
    /// `tailnet` describes it. `device_id` is Syncthing's.
    pub async fn reconcile(
        &self,
        config: &Config,
        tailnet: &Tailnet,
        device_id: &DeviceId,
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
        let locals = config.replicas_on(me);
        let aliased = self.aliases(&locals).await;
        let mut folders = Vec::new();
        let mut shared_with: BTreeSet<&NodeId> = BTreeSet::new();
        let mut ignores_after = Vec::new();
        for local in &locals {
            let id = local.item_id.to_string();
            let path = local.replica.path();
            if local.overlaps || aliased.contains(&local.item_id) {
                folders.extend(existing.get(id.as_str()).map(|folder| paused(folder)));
                report.replicas.insert(local.item_id, Outcome::Overlaps);
                continue;
            }
            let was = existing.get(id.as_str()).copied();
            let in_place = was.filter(|folder| folder["path"] == path.as_str());
            let was_paused = in_place.is_some_and(|folder| folder["paused"] == true);
            let ready = match in_place {
                // A folder that runs gets its excludes before the rest of its
                // new configuration, so that it never scans with old ones,
                // and one being paused gets them once it is.
                Some(_) => match self
                    .in_blocking(|path| check_initialized(&path), path.clone())
                    .await
                {
                    Ok(()) if !local.paused => {
                        self.sync_ignores(local, !was_paused, &mut report.changed)
                            .await
                    }
                    other => other,
                },
                // Syncthing keeps a folder's state across a change of path,
                // so a moved folder is removed and set up afresh next round.
                None if was.is_some() => Err(Outcome::Waiting(
                    was.and_then(|folder| folder["path"].as_str())
                        .unwrap_or_default()
                        .to_owned(),
                )),
                None => {
                    let (adopt, excludes) = (local.replica.adopt, excludes(local));
                    let others: Vec<PathBuf> = existing
                        .values()
                        .filter_map(|folder| folder["path"].as_str())
                        .map(PathBuf::from)
                        .collect();
                    self.in_blocking(
                        move |path| {
                            wait_for(&path, &others)?;
                            prepare(&path, adopt, &excludes)
                        },
                        path.clone(),
                    )
                    .await
                }
            };
            if let Err(outcome) = ready {
                // Keep it, with its index, until its path is back.
                folders.extend(in_place.map(paused));
                report.replicas.insert(local.item_id, outcome);
                continue;
            }
            if in_place.is_none() || local.paused {
                ignores_after.push(local);
            }
            let mut outcome = Outcome::Configured;
            let mut folder_type = folder_type(local.role);
            if in_place.is_some_and(|folder| folder["type"] == "receiveonly")
                && folder_type != "receiveonly"
            {
                // A paused folder isn't scanned, so its local changes can't
                // be known until it runs again.
                if was_paused || local.paused {
                    folder_type = "receiveonly";
                } else {
                    let scanned = self
                        .local_changes_now(&id)
                        .await
                        .map_err(|error| Outcome::Failed(error.to_string()));
                    match scanned {
                        Ok(0) => {}
                        Ok(_) => {
                            folder_type = "receiveonly";
                            outcome = Outcome::LocalChanges;
                        }
                        Err(failed) => {
                            folder_type = "receiveonly";
                            outcome = failed;
                        }
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
            let mut folder = self.folder(
                folder_template,
                local,
                folder_type,
                device_id,
                &peers,
                &devices,
            );
            if in_place.is_none() {
                // Until Syncthing has loaded its excludes without an error.
                folder["paused"] = json!(true);
            }
            folders.push(folder);
            report.replicas.insert(local.item_id, outcome);
        }
        for item in config.unusable_on(me) {
            folders.extend(
                existing
                    .get(item.to_string().as_str())
                    .map(|folder| paused(folder)),
            );
            report.replicas.insert(item, Outcome::Unreadable);
        }
        // Folders of Items this Node doesn't know are kept, paused, until
        // their records show them removed.
        for (id, folder) in &existing {
            let Ok(item) = id.parse::<ItemId>() else {
                continue;
            };
            if item.to_string() == *id
                && !report.replicas.contains_key(&item)
                && !config.was_removed(item, me)
            {
                folders.push(paused(folder));
                report.replicas.insert(item, Outcome::Unknown);
            }
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
        if normalized(&desired) != normalized(&current) {
            self.client
                .call(Method::PUT, "/rest/config", Some(&desired))
                .await?;
            report.changed = true;
        }

        // A new folder starts paused with the `.stignore` prepared for it,
        // so this checks that Syncthing can use it, then lets it run.
        for local in ignores_after {
            let fresh = existing
                .get(local.item_id.to_string().as_str())
                .is_none_or(|folder| folder["path"] != local.replica.path().as_str());
            let mut synced = self.sync_ignores(local, false, &mut report.changed).await;
            if synced.is_ok() && fresh && !local.paused {
                synced = self
                    .client
                    .set_folder_paused(local.item_id, false)
                    .await
                    .map_err(|error| Outcome::Failed(error.to_string()));
            }
            if let Err(outcome) = synced {
                report.replicas.insert(local.item_id, outcome);
            }
        }
        Ok(report)
    }

    /// Sets a configured Replica's excludes, unless its marker is missing,
    /// pausing its folder meanwhile if it is `running`: while Syncthing
    /// can't parse a folder's ignore patterns, it ignores nothing, so new
    /// ones must not reach a folder that is scanning or pulling until they
    /// are known to parse. If they don't, the folder stays paused.
    /// since Syncthing would recreate its path. Sets `changed` if they
    /// changed.
    async fn sync_ignores(
        &self,
        local: &LocalReplica<'_>,
        running: bool,
        changed: &mut bool,
    ) -> Result<(), Outcome> {
        let wanted = ignore_lines(&excludes(local));
        let failed = |error: Error| Outcome::Failed(error.to_string());
        let unparsed = |error: String| {
            Outcome::Failed(format!("Syncthing can't use the item's excludes: {error}"))
        };
        let (lines, error) = self.client.ignores(local.item_id).await.map_err(failed)?;
        if lines == wanted {
            return error.map_or(Ok(()), |error| Err(unparsed(error)));
        }
        self.in_blocking(
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
        if running {
            self.client
                .set_folder_paused(local.item_id, true)
                .await
                .map_err(failed)?;
        }
        self.client
            .set_ignores(local.item_id, &wanted)
            .await
            .map_err(failed)?;
        *changed = true;
        let (_, error) = self.client.ignores(local.item_id).await.map_err(failed)?;
        if let Some(error) = error {
            return Err(unparsed(error));
        }
        if running {
            self.client
                .set_folder_paused(local.item_id, false)
                .await
                .map_err(failed)?;
        }
        Ok(())
    }

    /// How many local changes a folder has, scanning it first so that none
    /// made since its last scan are missed.
    async fn local_changes_now(&self, folder: &str) -> Result<u64, Error> {
        self.client.scan(folder).await?;
        self.client.local_changes(folder).await
    }

    /// The Items of `locals` whose paths overlap another's once symlinks are
    /// resolved on this machine.
    async fn aliases(&self, locals: &[LocalReplica<'_>]) -> BTreeSet<ItemId> {
        let mut resolved = Vec::new();
        for local in locals {
            let real = Arc::new(Mutex::new(None));
            let found = real.clone();
            let _ = self
                .in_blocking(
                    move |path| {
                        *found.lock().unwrap() = Some(resolve(Path::new(path.as_str())));
                        Ok(())
                    },
                    local.replica.path().clone(),
                )
                .await;
            let real = real.lock().unwrap().take();
            let path = real.unwrap_or_else(|| PathBuf::from(local.replica.path().as_str()));
            resolved.push((local.item_id, path));
        }
        let mut aliased = BTreeSet::new();
        for (i, (a, a_path)) in resolved.iter().enumerate() {
            for (b, b_path) in &resolved[i + 1..] {
                if a_path.starts_with(b_path) || b_path.starts_with(a_path) {
                    aliased.insert(*a);
                    aliased.insert(*b);
                }
            }
        }
        aliased
    }

    /// Runs `check` on `path` away from the async runtime, so that a hung
    /// filesystem only times out. While an earlier check of the same path
    /// is still running, it isn't started again.
    async fn in_blocking(
        &self,
        check: impl FnOnce(AbsPath) -> Result<(), Outcome> + Send + 'static,
        path: AbsPath,
    ) -> Result<(), Outcome> {
        if !self.checking.lock().unwrap().insert(path.clone()) {
            return Err(Outcome::PathError(
                "an earlier check of its path hasn't finished; is its filesystem hung?".into(),
            ));
        }
        let checking = self.checking.clone();
        let task = tokio::task::spawn_blocking(move || {
            let result = check(path.clone());
            checking.lock().unwrap().remove(&path);
            result
        });
        match tokio::time::timeout(PATH_TIMEOUT, task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(Outcome::PathError(error.to_string())),
            Err(_) => Err(Outcome::PathError(format!(
                "its path didn't answer within {}s",
                PATH_TIMEOUT.as_secs()
            ))),
        }
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
        // Syncthing never deletes a conflict's copy; heighliner moves it
        // into the Displaced Versions (see `conflicts`).
        folder["maxConflicts"] = json!(-1);
        // Any change of modification time makes Syncthing look at a file's
        // content, as a swapped conflict needs.
        folder["modTimeWindowS"] = json!(0);
        let retention_days = local
            .entry
            .item
            .retention_days
            .map_or(self.retention_days, |days| u64::from(days.get()));
        folder["versioning"] = json!({
            "type": "simple",
            "params": {
                "keep": KEEP_VERSIONS.to_string(),
                "cleanoutDays": retention_days.to_string(),
            },
            "cleanupIntervalS": 3600,
            "fsPath": "",
            "fsType": "basic",
        });
        folder
    }
}

/// `path` with symlinks resolved, as far as it exists: a path that doesn't
/// exist yet is resolved through its deepest ancestor that does.
fn resolve(path: &Path) -> PathBuf {
    let mut missing = Vec::new();
    let mut existing = path;
    loop {
        if let Ok(mut real) = fs::canonicalize(existing) {
            real.extend(missing.iter().rev());
            return real;
        }
        match (existing.parent(), existing.file_name()) {
            (Some(parent), Some(name)) => {
                missing.push(name);
                existing = parent;
            }
            _ => return path.to_owned(),
        }
    }
}

/// Refuses a new folder's `path` while another Syncthing folder overlaps
/// it, as given or once symlinks are resolved: Syncthing removes a folder's
/// marker when the folder goes, so the new one waits for that first.
fn wait_for(path: &AbsPath, others: &[PathBuf]) -> Result<(), Outcome> {
    let overlap = |a: &Path, b: &Path| a.starts_with(b) || b.starts_with(a);
    let path = Path::new(path.as_str());
    let real = resolve(path);
    match others
        .iter()
        .find(|other| overlap(path, other) || overlap(&real, &resolve(other)))
    {
        Some(other) => Err(Outcome::Waiting(other.display().to_string())),
        None => Ok(()),
    }
}

/// A configured folder, paused.
fn paused(folder: &Value) -> Value {
    let mut folder = folder.clone();
    folder["paused"] = json!(true);
    folder
}

fn folder_type(role: Role) -> &'static str {
    match role {
        Role::Source => "sendonly",
        Role::Mirror => "receiveonly",
        Role::Peer => "sendreceive",
    }
}

/// Where a new `.stignore` is written before it replaces the old one.
const STAGED_IGNORE_FILE: &str = ".stignore.heighliner";

/// The first line of each `.stignore` heighliner writes, so that it can
/// tell its own from a user's.
const IGNORE_HEADER: &str =
    "// Written by heighliner from the item's excludes; edit those instead.";

/// An Item's exclude patterns.
fn excludes(local: &LocalReplica<'_>) -> Vec<String> {
    local
        .entry
        .item
        .excludes
        .iter()
        .map(|pattern| pattern.as_str().to_owned())
        .collect()
}

/// The `.stignore` lines for `excludes`.
/// Data Conflicts' copies and heighliner's own directory are ignored
/// first, so that no exclude can undo it.
fn ignore_lines(excludes: &[String]) -> Vec<String> {
    [
        IGNORE_HEADER,
        conflicts::IGNORE_PATTERN,
        conflicts::STORE_PATTERN,
    ]
    .into_iter()
    .map(str::to_owned)
    .chain(excludes.iter().cloned())
    .collect()
}

/// `lines` as a file.
fn as_file(lines: &[String]) -> String {
    lines.iter().map(|line| format!("{line}\n")).collect()
}

/// Checks that a configured folder's path and marker are there.
fn check_initialized(path: &AbsPath) -> Result<(), Outcome> {
    check_exists(path)?;
    if Path::new(path.as_str()).join(MARKER).exists() {
        Ok(())
    } else {
        Err(Outcome::Missing)
    }
}

/// Checks that a path is a directory that is there.
fn check_exists(path: &AbsPath) -> Result<(), Outcome> {
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
/// first scans it. Running it again on a path it prepared succeeds, even
/// once the excludes have changed. A `.stignore` holding exactly the
/// excludes counts as the Replica's own.
fn prepare(path: &AbsPath, adopt: bool, excludes: &[String]) -> Result<(), Outcome> {
    let path = Path::new(path.as_str());
    let error = |error: io::Error| Outcome::PathError(error.to_string());
    let ignore_file = path.join(IGNORE_FILE);
    match fs::read_dir(path) {
        Ok(entries) => {
            // A .heighliner that isn't heighliner's holds the user's files,
            // which every folder ignores, so the path is refused even
            // with adopt.
            conflicts::has_store(path).map_err(|error| Outcome::PathError(error.to_string()))?;
            for entry in entries {
                let name = entry.map_err(error)?.file_name();
                if name == MARKER
                    || name == IGNORE_FILE
                    || name == STAGED_IGNORE_FILE
                    || name == conflicts::STORE
                {
                    continue;
                }
                if !adopt {
                    return Err(Outcome::NotEmpty);
                }
            }
            match fs::symlink_metadata(&ignore_file) {
                // Not a symlink, so writing it can't reach outside the path.
                Ok(metadata) if metadata.is_file() => {
                    let existing = fs::read_to_string(&ignore_file).map_err(error)?;
                    let ours = existing.lines().next() == Some(IGNORE_HEADER)
                        || existing == as_file(excludes);
                    if !ours {
                        return Err(Outcome::OwnIgnoreFile);
                    }
                }
                Ok(_) => return Err(Outcome::OwnIgnoreFile),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(error(e)),
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => match fs::create_dir(path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Outcome::Missing),
            // Not there to read, yet there to create: a dangling symlink.
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                return Err(Outcome::PathError(
                    "its path is a symlink to something that doesn't exist".into(),
                ));
            }
            Err(e) => return Err(error(e)),
        },
        Err(e) => return Err(error(e)),
    }
    // Create the marker now, rather than when Syncthing first runs the
    // folder, so that a configured folder always has one while its disk is
    // there.
    match fs::create_dir(path.join(MARKER)) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(error(e)),
    }
    // Conflict copies already here are the user's, not ones to move.
    conflicts::mark_set_up(path).map_err(|error| Outcome::PathError(error.to_string()))?;
    // Written beside it and renamed over it, so that neither a symlink nor
    // a hard link to a file elsewhere is written through.
    let staged = path.join(STAGED_IGNORE_FILE);
    match fs::remove_file(&staged) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(error(e)),
    }
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staged)
        .and_then(|mut file| {
            io::Write::write_all(&mut file, as_file(&ignore_lines(excludes)).as_bytes())
        })
        .and_then(|()| fs::rename(&staged, &ignore_file))
        .map_err(error)
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

pub mod conflicts;
mod events;
mod state;

pub use events::{Finished, Seen};
#[cfg(test)]
pub(crate) mod test_instance;
#[cfg(test)]
mod tests;
