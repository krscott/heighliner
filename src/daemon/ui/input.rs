//! The Remote Input page: a touchpad, buttons and keys that `ui/input.js`
//! sends to this Node's virtual keyboard and pointer over a WebSocket, with
//! the Fleet's Peripherals beside them. Each Node serves its own; the page
//! links to the others'.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{self, WebSocket, WebSocketUpgrade};
use axum::http::{StatusCode, header};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use maud::{Markup, html};
use tokio::time::Instant;

use super::actions::{self, same_origin};
use super::{PERIPHERALS, any_page, button, node_name, node_url};
use crate::api::{Peripherals, Reach, Status};
use crate::daemon::api::Api;
use crate::input::{Device, RemoteInput, Session};
use crate::tailscale::LocalApi;

/// The Remote Input page.
pub(super) const PAGE: &str = "/input";

/// The page's WebSocket; `ui/input.js` names it too.
const SOCKET: &str = "/ui/input";

/// How often the daemon pings a page, so that a page that went away, such
/// as on a phone that slept, is noticed.
const PING_EVERY: Duration = Duration::from_secs(2);

/// How long a page may go without a message or an answer to a ping before
/// its connection is closed, releasing what it held.
const SILENCE: Duration = Duration::from_secs(6);

/// The largest message a page may send: text up to
/// [`crate::input::MAX_TEXT`]
/// characters, each up to four bytes, escaped or not.
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

/// A button that sends `key`, one of [`input::Key`]'s names, or several
/// separated by spaces, pressed in order and released in reverse.
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
        section.remote data-socket=(SOCKET) {
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
            textarea.typing rows="1" placeholder="Type here" aria-label="Type"
                autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false" {}
            p.note.remote-note role="status" {}
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
fn other_nodes(status: &Status, port: u16) -> Markup {
    html! {
        h2 { "Other Nodes" }
        ul.inline-list {
            @for node in status.nodes.iter().filter(|node| !node.departed && node.reach != Reach::Here) {
                @let name = node_name(status, &node.node);
                li {
                    @match node_url(node, port) {
                        Some(url) => a href={ (url) "input" } { (name) },
                        None => { (name) " " span.note { "(not on the tailnet)" } },
                    }
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
    let view = api.peripherals(status.clone()).await;
    let me = node_name(&status, &status.node);
    let fixed = match &api.remote_input {
        RemoteInput::Ready(_) => controls(&me),
        RemoteInput::Off => unavailable(
            &me,
            html! {
                "Remote Input is off on this Node. Turn it on with "
                code { "services.heighliner.remoteInput.enable = true" }
                " in its NixOS configuration."
            },
        ),
        RemoteInput::Failed(error) => unavailable(
            &me,
            html! {
                (error) ". heighliner tries again when it restarts."
            },
        ),
    };
    let main = html! {
        (other_nodes(&status, api.port))
        (peripherals(&view))
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
        .on_upgrade(move |socket| serve(socket, device))
}

/// Carries out each message a page sends, answering one it can't with
/// `{"refused": "why"}`, until the page goes away.
async fn serve(mut socket: WebSocket, device: Arc<Device>) {
    let mut session = Session::new(device);
    let mut pings = tokio::time::interval(PING_EVERY);
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
            () = tokio::time::sleep_until(deadline) => {
                tracing::info!("a Remote Input page went quiet; releasing what it held");
                break;
            }
        };
        deadline = Instant::now() + SILENCE;
        let refused = match message {
            ws::Message::Text(text) => match serde_json::from_str(text.as_str()) {
                Ok(message) => session
                    .apply(message)
                    .err()
                    .map(|refused| refused.to_string()),
                Err(error) => Some(format!("can't read the message: {error}")),
            },
            ws::Message::Binary(_) => Some("messages must be text".to_owned()),
            ws::Message::Close(_) => break,
            ws::Message::Ping(_) | ws::Message::Pong(_) => None,
        };
        if let Some(refused) = refused {
            let answer = serde_json::json!({ "refused": refused }).to_string();
            if socket.send(ws::Message::Text(answer.into())).await.is_err() {
                break;
            }
        }
    }
}
