//! The Remote Input page: a touchpad, buttons and keys that `ui/input.js`
//! sends to this Node's virtual keyboard and pointer over a WebSocket, with
//! the Fleet's Peripherals beside them. Each Node serves its own; the page
//! links to the others'.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{self, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, State};
use axum::http::{StatusCode, header};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use maud::{Markup, html};
use tokio::time::Instant;

use super::actions::{self, same_origin};
use super::{PERIPHERALS, any_page, button, node_name, other_nodes};
use crate::api::{Peripherals, Status};
use crate::auth::Recheck;
use crate::daemon::api::Api;
use crate::input::{Device, MAX_TEXT, RemoteInput, Session};
use crate::tailscale::LocalApi;

/// The Remote Input page.
pub(super) const PAGE: &str = "/input";

/// The page's WebSocket.
const SOCKET: &str = "/ui/input";

/// How often the daemon pings a page, so that a page that went away, such
/// as on a phone that slept, is noticed.
const PING_EVERY: Duration = Duration::from_secs(2);

/// How long a page may go without a message or an answer to a ping before
/// its connection is closed, releasing what it held.
const SILENCE: Duration = Duration::from_secs(6);

/// How often a page's caller is authorized again while it is connected.
const RECHECK_EVERY: Duration = Duration::from_secs(30);

/// The largest message a page may send: text up to [`MAX_TEXT`]
/// characters, each at most 12 bytes once escaped.
const MAX_MESSAGE: usize = 64 * 1024;

pub(super) fn router<T: LocalApi + Clone>(api: Api<T>) -> Router {
    Router::new()
        .route(PAGE, get(page::<T>))
        .route("/ui/input.js", get(script))
        .route(
            SOCKET,
            get(socket::<T>).layer(middleware::from_fn(same_origin)),
        )
        .with_state(api)
}

async fn script() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("input.js"),
    )
}

/// A button that presses `keys`, names of [`crate::input::Key`] separated
/// by spaces, in order, then releases them in reverse.
fn key(keys: &str, label: &str, title: &str) -> Markup {
    html! { button type="button" data-keys=(keys) title=(title) { (label) } }
}

/// A button that adds a modifier to the next key, character or click.
fn modifier(key: &str, label: &str) -> Markup {
    html! {
        button.modifier type="button" data-modifier=(key) aria-pressed="false"
            title={ (label) " with the next key, character or click" } { (label) "+" }
    }
}

/// The touchpad, buttons and keys.
fn controls(me: &str) -> Markup {
    html! {
        section.remote data-socket=(SOCKET) data-max-text=(MAX_TEXT) {
            div.remote-bar {
                h1 { "Remote Input on " (me) }
                span.badge.unknown.remote-state { "connecting" }
                button.fullscreen type="button" hidden { "Fullscreen" }
            }
            noscript { p.callout { "Remote Input needs JavaScript." } }
            div.touchpad role="application" aria-label="Touchpad" {
                span.note {
                    "Drag to move · tap to click · two-finger tap to right-click · "
                    "two fingers to scroll · double-tap and drag to drag"
                }
            }
            div.keys.mouse {
                button type="button" data-button="left" { "Left" }
                button type="button" data-button="middle" { "Middle" }
                button type="button" data-button="right" { "Right" }
            }
            div.keys {
                (key("escape", "Esc", "Escape"))
                (key("tab", "Tab", "Tab"))
                (key("enter", "Enter", "Enter"))
                (key("backspace", "⌫", "Backspace"))
                (key("left", "←", "Left"))
                (key("up", "↑", "Up"))
                (key("down", "↓", "Down"))
                (key("right", "→", "Right"))
            }
            div.keys {
                (key("super", "Super", "Super"))
                (key("alt tab", "Alt+Tab", "Alt+Tab"))
                (key("f11", "F11", "F11"))
                (modifier("ctrl", "Ctrl"))
                (modifier("alt", "Alt"))
                (modifier("shift", "Shift"))
                (modifier("super", "Super"))
            }
            div.keys {
                (key("volume_down", "Vol −", "Volume down"))
                (key("volume_up", "Vol +", "Volume up"))
                (key("mute", "Mute", "Mute"))
            }
            p.note.remote-note role="status" {}
            // Last, so the keys and notices stay in view above the phone's
            // keyboard.
            textarea.typing rows="1" placeholder="Type here" aria-label="Type"
                autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false" {}
            script src="/ui/input.js" defer {}
        }
    }
}

/// Why this Node takes no Remote Input.
fn unavailable(me: &str, why: Markup) -> Markup {
    html! {
        section.remote {
            h1 { "Remote Input on " (me) }
            p.callout { (why) }
        }
    }
}

/// Links to the other Nodes' Remote Input pages.
fn other_pages(status: &Status, port: u16) -> Markup {
    let others = other_nodes(status, port);
    html! {
        @if !others.is_empty() {
            h2 { "Other Nodes" }
            ul.inline-list {
                @for (node, url) in &others {
                    li { a href={ (url) "input" } { (node_name(status, &node.node)) } }
                }
            }
        }
    }
}

/// Each Peripheral, with a button to connect it on each Node it is paired
/// with but not connected to.
fn peripherals(view: &Peripherals) -> Markup {
    html! {
        h2 { "Peripherals" }
        @if view.peripherals.is_empty() {
            p.note { "No Node that could be asked has a Peripheral paired." }
        }
        ul.cards {
            @for peripheral in &view.peripherals {
                @let address = peripheral.address.as_str();
                li.card {
                    p.title { (peripheral.alias) }
                    div.actions {
                        @for on in &peripheral.paired {
                            @let name = view.node_name(&on.node);
                            @if on.connected {
                                span.badge.good { "connected on " (name) }
                            } @else {
                                (button(actions::PERIPHERAL, &[
                                    ("peripheral", address),
                                    ("action", "handoff"),
                                    ("node", on.node.as_str()),
                                    ("back", PAGE),
                                ], &format!("Connect on {name}")))
                            }
                        }
                    }
                }
            }
        }
        p.note { a href=(PERIPHERALS) { "Pair, forget or rename them" } }
    }
}

async fn page<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Response {
    let status = api.status().await;
    let me = node_name(&status, &status.node);
    let (fixed, view) = match &api.remote_input {
        RemoteInput::Ready(_) => (controls(&me), Some(api.peripherals(status.clone()).await)),
        RemoteInput::Off => (
            unavailable(
                &me,
                html! {
                    "Remote Input is off on this Node. Turn it on with "
                    code { "services.heighliner.remoteInput.enable = true" }
                    " in its NixOS configuration."
                },
            ),
            None,
        ),
        RemoteInput::Failed(error) => (
            unavailable(
                &me,
                html! { (error) ". heighliner tries again when it restarts." },
            ),
            None,
        ),
    };
    let main = html! {
        (other_pages(&status, api.port))
        @if let Some(view) = &view { (peripherals(view)) }
    };
    any_page(
        &status,
        "Remote Input",
        StatusCode::OK,
        main,
        true,
        Some(fixed),
    )
}

async fn socket<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Extension(recheck): Extension<Recheck<T>>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let device = match &api.remote_input {
        RemoteInput::Ready(device) => device.clone(),
        RemoteInput::Off => {
            return (StatusCode::NOT_FOUND, "Remote Input is off on this Node").into_response();
        }
        RemoteInput::Failed(error) => {
            return (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response();
        }
    };
    upgrade
        .max_message_size(MAX_MESSAGE)
        .on_upgrade(move |socket| serve(socket, device, recheck))
}

/// Carries out each message a page sends, answering one it can't with
/// `{"refused": "why"}`, until the page goes away or its caller may no
/// longer send them.
async fn serve<T: LocalApi>(mut socket: WebSocket, device: Arc<Device>, recheck: Recheck<T>) {
    let mut session = Session::new(device);
    let mut pings = tokio::time::interval(PING_EVERY);
    // Checked alongside the page's messages, since asking tailscaled can
    // take seconds.
    let revoked = async {
        let mut rechecks = tokio::time::interval_at(Instant::now() + RECHECK_EVERY, RECHECK_EVERY);
        loop {
            rechecks.tick().await;
            if let Err(denied) = recheck.authorize().await {
                break denied;
            }
        }
    };
    tokio::pin!(revoked);
    let mut deadline = Instant::now() + SILENCE;
    loop {
        let message = tokio::select! {
            received = socket.recv() => match received {
                Some(Ok(message)) => message,
                Some(Err(_)) | None => break,
            },
            _ = pings.tick() => {
                if socket.send(ws::Message::Ping(Default::default())).await.is_err() {
                    break;
                }
                continue;
            }
            denied = &mut revoked => {
                tracing::warn!(%denied, "closing a Remote Input page whose caller is no longer authorized");
                break;
            }
            () = tokio::time::sleep_until(deadline) => {
                tracing::info!("a Remote Input page went quiet; releasing what it held");
                break;
            }
        };
        let refused = match message {
            ws::Message::Text(text) => match serde_json::from_str(text.as_str()) {
                Ok(message) => session
                    .apply(message)
                    .await
                    .err()
                    .map(|refused| refused.to_string()),
                Err(error) => Some(format!("can't read the message: {error}")),
            },
            ws::Message::Binary(_) => Some("messages must be text".to_owned()),
            ws::Message::Close(_) => break,
            ws::Message::Ping(_) | ws::Message::Pong(_) => None,
        };
        // Typing a long text takes a while, during which nothing is read.
        deadline = Instant::now() + SILENCE;
        if let Some(refused) = refused {
            let answer = serde_json::json!({ "refused": refused }).to_string();
            if socket.send(ws::Message::Text(answer.into())).await.is_err() {
                break;
            }
        }
    }
}
