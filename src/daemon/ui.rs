//! The web UI: pages the daemon renders from its API's answers (see
//! [`crate::api`]), so they show what the CLI does. A small script has a
//! page fetched again whenever the daemon signals a change over
//! Server-Sent Events; without it, pages refresh on a timer.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::sse::{Event, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use futures_util::Stream;
use maud::{DOCTYPE, Markup, html};

use super::api::Api;
use crate::api::{self, ItemStatus, NodeStatus, Reach, ReplicaStatus, Replication, Status};
use crate::hlc::Timestamp;
use crate::http::percent_encode;
use crate::model::{ItemId, ReplicaState, Role};
use crate::protocol;
use crate::tailscale::LocalApi;

/// Where pages listen for changes; `ui/live.js` names it too.
const EVENTS: &str = "/ui/events";

/// How long to wait after a change for the rest of a burst, so that it
/// updates pages once.
const SETTLE: Duration = Duration::from_millis(500);

/// How often pages are told to update even with no change, so that the
/// ages they show stay right.
const TICK: Duration = Duration::from_secs(30);

/// Pages load only the daemon's own script and style, can't be framed,
/// and send forms only to it.
const POLICY: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
    connect-src 'self'; img-src 'self'; form-action 'self'; base-uri 'none'; \
    frame-ancestors 'none'";

pub(super) fn router<T: LocalApi + Clone>(api: Api<T>) -> Router {
    Router::new()
        .route("/", get(fleet::<T>))
        .route("/items/{item}", get(item::<T>))
        .route("/items/{item}/replicas/{node}", get(replica::<T>))
        .route("/conflicts", get(conflicts::<T>))
        .route(EVENTS, get(events::<T>))
        .route("/ui/style.css", get(style))
        .route("/ui/live.js", get(script))
        .with_state(api.clone())
        .merge(actions::router(api))
}

/// The path of `item`'s page.
fn item_href(item: ItemId) -> String {
    format!("/items/{item}")
}

/// The path of the page of `replica` of `item`.
fn replica_href(item: ItemId, replica: &ReplicaStatus) -> String {
    format!(
        "/items/{item}/replicas/{}",
        percent_encode(replica.node.as_str())
    )
}

async fn style() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("ui/style.css"),
    )
}

async fn script() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("ui/live.js"),
    )
}

/// Says `changed` after each change, once its burst settles, and every
/// [`TICK`] regardless. The event carries no data: pages fetch themselves
/// again, so that each read is authorized as it happens.
async fn events<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let changes = api.changes.subscribe();
    let stream = futures_util::stream::unfold(changes, |mut changes| async move {
        tokio::select! {
            changed = changes.changed() => {
                // The daemon is stopping.
                changed.ok()?;
                tokio::time::sleep(SETTLE).await;
                changes.mark_unchanged();
            }
            () = tokio::time::sleep(TICK) => {}
        }
        // An event with no data isn't delivered.
        let event = Event::default().event("changed").data("changed");
        Some((Ok(event), changes))
    });
    Sse::new(stream)
}

/// A page titled `title`, served by `status.node`.
fn page(status: &Status, title: &str, code: StatusCode, main: Markup) -> Response {
    let me = node_name(status, &status.node);
    let markup = html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) " · heighliner on " (me) }
                link rel="stylesheet" href="/ui/style.css";
                script src="/ui/live.js" defer {}
                noscript { meta http-equiv="refresh" content=(TICK.as_secs()); }
            }
            body {
                header {
                    nav {
                        a.home href="/" { "heighliner" }
                        span.here { "on " (me) }
                        a href="/conflicts" { "Conflicts" }
                    }
                    p.lost { "Lost touch with this Node's daemon; this is what it last showed." }
                }
                main { (main) }
            }
        }
    };
    (
        code,
        [
            (header::CONTENT_SECURITY_POLICY, POLICY),
            (header::CACHE_CONTROL, "no-store"),
        ],
        Html(markup.into_string()),
    )
        .into_response()
}

fn not_found(status: &Status, what: &str) -> Response {
    let main = html! {
        h1 { "Not found" }
        p { (what) " " a href="/" { "Back to the Fleet" } }
    };
    page(status, "Not found", StatusCode::NOT_FOUND, main)
}

/// A Node's machine name, or its node ID if it isn't on the tailnet.
fn node_name(status: &Status, node: &crate::model::NodeId) -> String {
    status
        .nodes
        .iter()
        .find(|known| known.node == *node)
        .and_then(|known| known.name.clone())
        .or_else(|| {
            status
                .items
                .iter()
                .flat_map(|item| &item.replicas)
                .find(|replica| replica.node == *node)
                .and_then(|replica| replica.node_name.clone())
        })
        .unwrap_or_else(|| node.to_string())
}

/// The web UI of `node`, if it is on the tailnet.
fn node_url(node: &NodeStatus, port: u16) -> Option<String> {
    node.ip
        .map(|ip| format!("http://{}/", SocketAddr::new(ip, port)))
}

/// How long ago `then` was, as of `now`, at a glance.
fn ago(now: Timestamp, then: Timestamp) -> String {
    let secs = now.as_millis().saturating_sub(then.as_millis()) / 1000;
    match secs {
        0..60 => "just now".to_owned(),
        60..3600 => format!("{} min ago", secs / 60),
        3600..86_400 => format!("{} h ago", secs / 3600),
        _ => format!("{} days ago", secs / 86_400),
    }
}

/// How long until `then`, as of `now`, at a glance.
fn until(now: Timestamp, then: Timestamp) -> String {
    let secs = then.as_millis().saturating_sub(now.as_millis()) / 1000;
    match secs {
        0..3600 => "within the hour".to_owned(),
        3600..86_400 => format!("in {} h", secs / 3600),
        _ => format!("in {} days", secs / 86_400),
    }
}

/// `then`, as an age with its exact time on hover.
fn time_ago(now: Timestamp, then: Timestamp) -> Markup {
    html! { time datetime=(then) title=(then) { (ago(now, then)) } }
}

/// `bytes` in binary units, to three figures.
fn size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    let digits = if value < 10.0 { 1 } else { 0 };
    format!("{value:.digits$} {}", UNITS[unit])
}

fn role(role: Role) -> &'static str {
    match role {
        Role::Source => "source",
        Role::Mirror => "mirror",
        Role::Peer => "peer",
    }
}

/// Whether `replica`'s state is as last read from a Node that can't be
/// reached now.
fn is_stale(status: &Status, replica: &ReplicaStatus) -> bool {
    replica.node != status.node
        && !status
            .nodes
            .iter()
            .any(|node| node.node == replica.node && matches!(node.reach, Reach::Live { .. }))
}

/// A Replica's state, as a badge with what it means.
fn state(now: Timestamp, status: &Status, replica: &ReplicaStatus) -> Markup {
    let stale = is_stale(status, replica);
    html! {
        span.state {
            @match &replica.state {
                None => span.badge.unknown { "unknown" },
                Some(ReplicaState::Current) => {
                    span.badge.good { "current" }
                    @if !replica.every_sender {
                        " " span.note { "with the senders it reaches; not every one is connected" }
                    }
                },
                Some(ReplicaState::Syncing { files_left, bytes_left }) => {
                    span.badge.busy { "syncing" }
                    " " (files_left) " items, " (size(*bytes_left)) " left"
                }
                Some(ReplicaState::Behind { last_current }) => {
                    span.badge.warn { "behind" }
                    " its senders are unreachable; "
                    @match last_current {
                        Some(at) => { "last current " (time_ago(now, *at)) }
                        None => "never confirmed current",
                    }
                }
                Some(ReplicaState::Error { message }) => {
                    span.badge.bad { "error" } " " (message)
                }
                Some(ReplicaState::Paused) => span.badge.idle { "paused" },
            }
            @if stale {
                " "
                span.stale {
                    @match replica.state_read_at {
                        Some(at) => { "as of " (time_ago(now, at)) "; its Node is unreachable" }
                        None => "its Node hasn't been reached",
                    }
                }
            }
        }
    }
}

fn replication(now: Timestamp, replication: &Replication) -> Markup {
    html! {
        @match replication {
            Replication::Full => span.badge.good { "fully replicated" },
            Replication::Unconfirmed { oldest } => {
                span.badge.warn { "not confirmed fully replicated" }
                " "
                span.note {
                    @match oldest {
                        Some(at) => { "oldest confirmation " (time_ago(now, *at)) }
                        None => "a replica was never confirmed current",
                    }
                }
            }
        }
    }
}

/// An Item's flags, as badges.
fn flags(item: &ItemStatus) -> Markup {
    html! {
        @if item.paused { " " span.badge.idle { "paused" } }
        @if item.resurrected { " " span.badge.warn { "resurrected" } }
        @if item.duplicate_name { " " span.badge.warn { "name shared" } }
    }
}

fn direction(status: &Status, item: &ItemStatus) -> String {
    match &item.source {
        Some(source) => format!("one-way from {}", node_name(status, source)),
        None => "two-way".to_owned(),
    }
}

fn reach(now: Timestamp, node: &NodeStatus) -> Markup {
    html! {
        @match &node.reach {
            Reach::Here => span.badge.good { "this Node" },
            Reach::Live { .. } => span.badge.good { "reachable" },
            Reach::Unreachable { read_at } => {
                span.badge.warn { "unreachable" }
                " "
                span.note {
                    @match read_at {
                        Some(at) => { "last read " (time_ago(now, *at)) }
                        None => "never reached",
                    }
                }
            }
            Reach::Incompatible { protocol } => {
                span.badge.bad { "can't talk" }
                " "
                span.note {
                    "speaks protocol " (protocol) ", not " (protocol::MAJOR) "; "
                    @if *protocol < protocol::MAJOR { "upgrade it" } @else { "upgrade this Node" }
                }
            }
        }
        @if node.departed { " " span.badge.idle { "departed" } }
    }
}

/// A button that posts `fields` to `action`, one of [`actions`]' paths.
fn button(action: &str, fields: &[(&str, &str)], label: &str) -> Markup {
    html! {
        form.inline method="post" action=(action) {
            @for (name, value) in fields { input type="hidden" name=(name) value=(value); }
            button { (label) }
        }
    }
}

/// An action that takes a second step: opening `summary` shows what it
/// does and its button.
fn confirmed(summary: &str, explanation: &str, button: Markup) -> Markup {
    html! {
        details.confirm {
            summary { (summary) }
            p.note { (explanation) }
            (button)
        }
    }
}

/// A choice of the Nodes something can be placed on, this one first.
fn node_select(status: &Status) -> Markup {
    html! {
        select name="node" {
            @for node in &status.nodes {
                @if !node.departed && !matches!(node.reach, Reach::Incompatible { .. }) {
                    option value=(node.node) selected[node.reach == Reach::Here] {
                        (node_name(status, &node.node))
                    }
                }
            }
        }
    }
}

/// Pausing or resuming, posted to `action`, and coming `back`.
fn pause_button(action: &str, paused: bool, back: &str) -> Markup {
    let (value, label) = if paused {
        ("false", "Resume")
    } else {
        ("true", "Pause")
    };
    button(action, &[("paused", value), ("back", back)], label)
}

fn track_form(status: &Status) -> Markup {
    html! {
        details.form {
            summary { "Track a directory" }
            form.stack method="post" action=(actions::TRACK) {
                label { "Node " (node_select(status)) }
                label {
                    "Path "
                    input name="path" required placeholder="/srv/photos, or ~/docs on this Node";
                }
                label { "Name " input name="name" placeholder="the path's last part"; }
                label.check { input type="checkbox" name="one_way"; " One-way, from this replica" }
                label {
                    "Excludes, one per line "
                    textarea name="excludes" rows="2" {}
                }
                label {
                    "Keep displaced versions for "
                    input name="retention_days" type="number" min="1" placeholder="the Retention Window";
                    " days"
                }
                p.note { "The files already at the path become the item's." }
                button { "Track" }
            }
        }
    }
}

async fn fleet<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Response {
    let status = api.status().await;
    let conflicts = api.list_conflicts().await;
    let now = Timestamp::now();
    let main = html! {
        h1 { "Fleet" }
        @if !status.problems.is_empty() {
            section.problems {
                h2 { "Problems" }
                ul { @for problem in &status.problems { li { (problem) } } }
            }
        }
        @if !conflicts.conflicts.is_empty() {
            p.callout {
                a href="/conflicts" {
                    (conflicts.conflicts.len()) " data conflicts on this Node to review"
                }
            }
        }
        @if !conflicts.errors.is_empty() {
            p.callout {
                a href="/conflicts" {
                    "Some of this Node's data conflicts can't be read"
                }
            }
        }
        section {
            h2 { "Items" }
            @if status.items.is_empty() {
                p.note { "No items are tracked." }
            }
            (track_form(&status))
            ul.cards {
                @for item in &status.items {
                    li.card {
                        p.title {
                            a href=(item_href(item.id)) { (item.name) }
                            (flags(item))
                        }
                        p.note { (direction(&status, item)) }
                        p { (replication(now, &item.replication)) }
                        ul.replicas {
                            @for replica in &item.replicas {
                                li {
                                    a href=(replica_href(item.id, replica)) {
                                        (node_name(&status, &replica.node))
                                    }
                                    " " (state(now, &status, replica))
                                }
                            }
                        }
                    }
                }
            }
        }
        section {
            h2 { "Nodes" }
            ul.cards {
                @for node in &status.nodes {
                    li.card {
                        p.title {
                            @match (&node.reach, node_url(node, api.port)) {
                                (Reach::Here, _) | (_, None) => { (node_name(&status, &node.node)) }
                                (_, Some(url)) => a href=(url) { (node_name(&status, &node.node)) },
                            }
                        }
                        p { (reach(now, node)) }
                        @if node.reach == Reach::Here && !node.departed {
                            details.confirm {
                                summary { "Leave the Fleet" }
                                p.note {
                                    "No Node will sync with this one any more, and it can't rejoin. "
                                    "Its replicas' files stay where they are."
                                }
                                form method="post" action=(actions::LEAVE) {
                                    label.check {
                                        input type="checkbox" name="confirm" required;
                                        " I understand this can't be undone"
                                    }
                                    button { "Leave" }
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    page(&status, "Fleet", StatusCode::OK, main)
}

/// Pausing, resuming and removing `replica` of `item`, coming `back`.
fn replica_actions(item: &ItemStatus, replica: &ReplicaStatus, back: &str) -> Markup {
    let node = replica.node.as_str();
    html! {
        div.actions {
            (pause_button(&actions::replica_paused(item.id, node), replica.paused, back))
            (confirmed(
                "Remove",
                "It stops syncing. Its files stay where they are.",
                button(&actions::remove(item.id, node), &[], "Remove this replica"),
            ))
        }
    }
}

/// The Item `id` names in `status`.
fn find_item<'a>(status: &'a Status, id: &str) -> Option<&'a ItemStatus> {
    let id: ItemId = id.parse().ok()?;
    status.items.iter().find(|item| item.id == id)
}

async fn item<T: LocalApi + Clone>(State(api): State<Api<T>>, Path(id): Path<String>) -> Response {
    let status = api.status().await;
    let Some(item) = find_item(&status, &id) else {
        return not_found(&status, "No item has that ID; it may have been untracked.");
    };
    let now = Timestamp::now();
    let main = html! {
        h1 { (item.name) (flags(item)) }
        @if item.resurrected {
            p.callout {
                "This item was untracked, but an edit made without seeing the untrack brought it back. "
                "Untrack it again, or edit its settings to keep it."
            }
        }
        @if item.duplicate_name {
            p.callout {
                "Another item is also named " q { (item.name) } ". They are separate items; "
                "name them by ID on the command line."
            }
        }
        p { (replication(now, &item.replication)) }
        div.actions {
            (pause_button(&actions::item_paused(item.id), item.paused, &item_href(item.id)))
            (confirmed(
                "Untrack",
                "Every replica stops syncing. Their files stay where they are.",
                button(&actions::untrack(item.id), &[], &format!("Untrack {}", item.name)),
            ))
        }
        dl {
            dt { "ID" } dd { code { (item.id) } }
            dt { "Direction" } dd { (direction(&status, item)) }
            dt { "Excludes" }
            dd {
                @if item.excludes.is_empty() { "none" }
                @for pattern in &item.excludes { code { (pattern) } " " }
            }
            dt { "Displaced versions kept" }
            dd {
                @match item.retention_days {
                    Some(days) => { (days) " days" }
                    None => "for the Retention Window",
                }
            }
        }
        h2 { "Replicas" }
        @if item.replicas.is_empty() { p.note { "It has no replicas." } }
        ul.cards {
            @for replica in &item.replicas {
                li.card {
                    p.title {
                        a href=(replica_href(item.id, replica)) { (node_name(&status, &replica.node)) }
                        " " span.note { (role(replica.role)) }
                        @if replica.paused { " " span.badge.idle { "paused" } }
                        @if replica.departed { " " span.badge.idle { "departed" } }
                    }
                    p.path { code { (replica.path) } }
                    p { (state(now, &status, replica)) }
                    (replica_actions(item, replica, &item_href(item.id)))
                }
            }
        }
        details.form {
            summary { "Add a replica" }
            form.stack method="post" action=(actions::replicas(item.id)) {
                label { "Node " (node_select(&status)) }
                label {
                    "Path "
                    input name="path" required placeholder="/srv/photos, or ~/docs on this Node";
                }
                label.check { input type="checkbox" name="mirror"; " Mirror: receives only" }
                label.check {
                    input type="checkbox" name="adopt";
                    " Adopt the files already at the path, merging them with the item's"
                }
                p.note { "Without adopting, the path must be empty or not exist yet." }
                button { "Add" }
            }
        }
    };
    page(&status, &item.name, StatusCode::OK, main)
}

async fn replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path((id, node)): Path<(String, String)>,
) -> Response {
    let status = api.status().await;
    let found = find_item(&status, &id).and_then(|item| {
        let replica = item
            .replicas
            .iter()
            .find(|replica| replica.node.as_str() == node)?;
        Some((item, replica))
    });
    let Some((item, replica)) = found else {
        return not_found(
            &status,
            "That item has no replica there; it may have been removed.",
        );
    };
    let here = replica.node == status.node;
    let conflicts = if here {
        let mut listed = api.list_conflicts().await;
        listed.conflicts.retain(|conflict| conflict.item == item.id);
        Some(listed)
    } else {
        None
    };
    let now = Timestamp::now();
    let node = status.nodes.iter().find(|known| known.node == replica.node);
    let name = node_name(&status, &replica.node);
    let main = html! {
        h1 { a href=(item_href(item.id)) { (item.name) } " on " (name) }
        p { (state(now, &status, replica)) }
        (replica_actions(item, replica, &replica_href(item.id, replica)))
        dl {
            dt { "Node" }
            dd {
                (name) " " code { (replica.node) }
                @if let Some(node) = node { " " (reach(now, node)) }
            }
            dt { "Path" } dd { code { (replica.path) } }
            dt { "Role" } dd { (role(replica.role)) }
            dt { "Paused" } dd { @if replica.paused { "yes" } @else { "no" } }
            dt { "Last confirmed current" }
            dd {
                @match replica.confirmed_at {
                    Some(at) => (time_ago(now, at)),
                    None => "never",
                }
            }
            @if let Some(at) = replica.state_read_at {
                dt { "State read" } dd { (time_ago(now, at)) }
            }
        }
        h2 { "Data conflicts" }
        @match &conflicts {
            Some(listed) => (conflict_list(now, listed, &replica_href(item.id, replica))),
            None => p.note {
                "Its data conflicts are kept on its Node. "
                @if let Some(url) = node.and_then(|node| node_url(node, api.port)) {
                    a href={ (url) "conflicts" } { "Review them there" } "."
                }
            },
        }
    };
    let title = format!("{} on {name}", item.name);
    page(&status, &title, StatusCode::OK, main)
}

/// This Node's Data Conflicts, to keep or swap, coming `back`.
fn conflict_list(now: Timestamp, listed: &api::Conflicts, back: &str) -> Markup {
    html! {
        @if listed.conflicts.is_empty() && listed.errors.is_empty() {
            p.note { "None." }
        }
        ul.cards {
            @for conflict in &listed.conflicts {
                li.card {
                    p.title {
                        a href=(item_href(conflict.item)) { (conflict.item_name) }
                        " " code { (conflict.path) }
                    }
                    p {
                        "Device " code { (conflict.winner) } "'s change won at " (conflict.at) ". "
                        "The other change is kept until "
                        time datetime=(conflict.expires_at) title=(conflict.expires_at) {
                            (until(now, conflict.expires_at))
                        }
                        "."
                    }
                    p.note { "Version " code { (conflict.version) } }
                    @let item = conflict.item.to_string();
                    @let fields = [("item", item.as_str()), ("version", conflict.version.as_str()), ("back", back)];
                    div.actions {
                        (button(actions::KEEP, &fields, "Keep the file"))
                        (button(actions::SWAP, &fields, "Swap in the other change"))
                    }
                }
            }
        }
        @for error in &listed.errors {
            p.callout { "Can't read the conflicts of " (error) }
        }
    }
}

async fn conflicts<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Response {
    let status = api.status().await;
    let listed = api.list_conflicts().await;
    let now = Timestamp::now();
    let others: Vec<(&NodeStatus, String)> = status
        .nodes
        .iter()
        .filter(|node| node.reach != Reach::Here && !node.departed)
        .filter_map(|node| Some((node, node_url(node, api.port)?)))
        .collect();
    let main = html! {
        h1 { "Data conflicts on " (node_name(&status, &status.node)) }
        p.note {
            "When two replicas change a file at once, Syncthing keeps one change as the file. "
            "The other is kept here, out of the item, until it expires."
        }
        p.note {
            "Keeping leaves the file as it is. Swapping makes the other change the file's "
            "content again, and Syncthing sends it to the other replicas. Either way, the "
            "side that isn't the file is kept until it expires."
        }
        (conflict_list(now, &listed, "/conflicts"))
        @if !others.is_empty() {
            h2 { "On other Nodes" }
            ul {
                @for (node, url) in &others {
                    li { a href={ (url) "conflicts" } { (node_name(&status, &node.node)) } }
                }
            }
        }
    };
    page(&status, "Data conflicts", StatusCode::OK, main)
}

mod actions;

#[cfg(test)]
mod tests;
