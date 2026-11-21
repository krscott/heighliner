//! Syncthing's event stream, which says when to read the Replicas' states
//! again and where to look for new Data Conflicts.

use std::time::Duration;

use hyper::Method;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{Notify, mpsc};

use super::{Client, Driver, Error};
use crate::model::ItemId;

/// The events after which a Replica's state may have changed, or a Data
/// Conflict may have appeared (`ItemFinished`).
const EVENTS: &str = "StateChanged,FolderSummary,FolderCompletion,FolderErrors,FolderPaused,FolderResumed,DeviceConnected,DeviceDisconnected,DevicePaused,DeviceResumed,ClusterConfigReceived,ItemFinished";

/// How long Syncthing holds a request for events open when none happen.
const EVENTS_TIMEOUT: Duration = Duration::from_secs(60);

/// How long to wait before asking for events again after a failure.
const EVENTS_RETRY: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
pub(super) struct Event {
    pub(super) id: u64,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    data: Value,
}

/// A file or directory Syncthing finished syncing into a folder, or failed
/// to: it may have moved a conflict's copy aside either way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finished {
    pub item: ItemId,
    /// Its path within the folder.
    pub path: String,
}

/// What the watcher saw that may have left a conflict's copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Seen {
    Finished(Finished),
    /// Events may have been missed: Syncthing restarted or didn't answer,
    /// or dropped some.
    Missed,
}

impl Event {
    fn finished(&self) -> Option<Finished> {
        if self.kind != "ItemFinished" {
            return None;
        }
        let folder = self.data["folder"].as_str()?;
        let item: ItemId = folder.parse().ok()?;
        let path = self.data["item"].as_str()?.to_owned();
        // Only heighliner's folders, whose IDs are canonical Item IDs.
        (item.to_string() == folder).then_some(Finished { item, path })
    }
}

impl Client {
    /// The events after `since`, waiting up to `timeout` for one.
    pub(super) async fn events(&self, since: u64, timeout: Duration) -> Result<Vec<Event>, Error> {
        let path = format!(
            "/rest/events?events={EVENTS}&since={since}&timeout={}",
            timeout.as_secs()
        );
        let body = self
            .call_within(timeout + super::TIMEOUT, Method::GET, &path, None)
            .await?;
        serde_json::from_slice(&body).map_err(|source| Error::Decode { path, source })
    }

    /// The ID of the latest event, or 0 if there is none yet. Syncthing
    /// keeps events for a filter only from when it is first asked for them.
    pub(super) async fn latest_event(&self) -> Result<u64, Error> {
        let path = format!("/rest/events?events={EVENTS}&since=0&limit=1&timeout=0");
        let events: Vec<Event> = self.get(&path).await?;
        Ok(events.iter().map(|event| event.id).max().unwrap_or(0))
    }
}

impl Driver {
    /// Notifies `changed` whenever Syncthing reports an event after which
    /// a Replica's state may have changed, and when Syncthing stops or
    /// starts answering again. Sends `seen` each item Syncthing finishes
    /// syncing, and [`Seen::Missed`] when events may have been missed.
    /// Runs until it is dropped.
    pub async fn watch(&self, changed: &Notify, seen: &mpsc::UnboundedSender<Seen>) {
        // Only a dropped daemon stops listening.
        let send = |what| {
            let _ = seen.send(what);
        };
        let mut since = self.client.latest_event().await.unwrap_or(0);
        let mut failing = false;
        loop {
            let events = self.client.events(since, EVENTS_TIMEOUT).await;
            if events.is_ok() && failing {
                // Syncthing is back, perhaps with nothing new to report.
                failing = false;
                changed.notify_one();
                send(Seen::Missed);
            }
            match events {
                Ok(events) if events.is_empty() => {
                    // A restarted Syncthing numbers its events afresh.
                    if let Ok(latest) = self.client.latest_event().await
                        && latest < since
                    {
                        since = latest;
                        changed.notify_one();
                        send(Seen::Missed);
                    }
                }
                Ok(events) => {
                    // Syncthing numbers each filter's events one by one,
                    // so a gap means some were dropped.
                    let mut next = since + 1;
                    let mut missed = false;
                    for event in &events {
                        missed |= event.id != next;
                        next = event.id + 1;
                    }
                    since = next - 1;
                    if missed {
                        send(Seen::Missed);
                    }
                    for item in events.iter().filter_map(Event::finished) {
                        send(Seen::Finished(item));
                    }
                    changed.notify_one();
                }
                Err(error) => {
                    if !failing {
                        tracing::debug!(%error, "can't read Syncthing's events");
                        failing = true;
                        changed.notify_one();
                    }
                    tokio::time::sleep(EVENTS_RETRY).await;
                }
            }
        }
    }
}
