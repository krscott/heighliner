//! The web UI's actions: forms the pages post, which make the same edits
//! as the API, then show the page they came from again.
//!
//! A page on another site could post a form here from the user's own
//! device, which WhoIs accepts, so each post must come from one of the
//! daemon's own pages, as its `Origin` says.

use std::num::NonZeroU32;

use axum::Router;
use axum::extract::rejection::FormRejection;
use axum::extract::{Form, Path, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::post;
use maud::html;
use serde::Deserialize;

use super::{PERIPHERALS, item_href, page};
use crate::api::{self, AddReplica, Done, PeripheralAction, Resolve, Track};
use crate::daemon::api::{Api, Refusal};
use crate::http::percent_encode;
use crate::model::ItemId;
use crate::tailscale::LocalApi;

pub(super) const TRACK: &str = "/ui/track";
pub(super) const KEEP: &str = "/ui/conflicts/keep";
pub(super) const SWAP: &str = "/ui/conflicts/swap";
pub(super) const LEAVE: &str = "/ui/leave";
pub(super) const PERIPHERAL: &str = "/ui/peripherals";

pub(super) fn item_paused(item: ItemId) -> String {
    format!("/ui/items/{item}/paused")
}

pub(super) fn untrack(item: ItemId) -> String {
    format!("/ui/items/{item}/untrack")
}

pub(super) fn replicas(item: ItemId) -> String {
    format!("/ui/items/{item}/replicas")
}

pub(super) fn replica_paused(item: ItemId, node: &str) -> String {
    format!("/ui/items/{item}/replicas/{}/paused", percent_encode(node))
}

pub(super) fn remove(item: ItemId, node: &str) -> String {
    format!("/ui/items/{item}/replicas/{}/remove", percent_encode(node))
}

pub(super) fn router<T: LocalApi + Clone>(api: Api<T>) -> Router {
    Router::new()
        .route(TRACK, post(track::<T>))
        .route("/ui/items/{item}/paused", post(pause_item::<T>))
        .route("/ui/items/{item}/untrack", post(untrack_item::<T>))
        .route("/ui/items/{item}/replicas", post(add_replica::<T>))
        .route(
            "/ui/items/{item}/replicas/{node}/paused",
            post(pause_replica::<T>),
        )
        .route(
            "/ui/items/{item}/replicas/{node}/remove",
            post(remove_replica::<T>),
        )
        .route(KEEP, post(keep::<T>))
        .route(SWAP, post(swap::<T>))
        .route(LEAVE, post(leave::<T>))
        .route(PERIPHERAL, post(peripheral::<T>))
        .layer(middleware::from_fn(same_origin))
        .with_state(api)
}

/// Refuses a post whose `Origin` isn't the daemon at the `Host` it was
/// sent to, which the Host check has already limited to this device's.
async fn same_origin(request: Request, next: Next) -> Response {
    let headers = request.headers();
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok());
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    let ours = match (origin, host) {
        (Some(origin), Some(host)) => origin.eq_ignore_ascii_case(&format!("http://{host}")),
        _ => false,
    };
    if ours {
        next.run(request).await
    } else {
        (
            StatusCode::FORBIDDEN,
            "only this daemon's own pages can post its forms",
        )
            .into_response()
    }
}

/// A checkbox, which a form sends only when it is checked.
type Checkbox = Option<String>;

/// The page to go back to: one of the daemon's own, or `default`. A
/// browser drops tabs and line breaks from a URL, so `/\t/site` would lead
/// to another site; none is accepted.
fn back(field: Option<String>, default: String) -> String {
    field
        .filter(|back| {
            back.starts_with('/')
                && !back.starts_with("//")
                && !back.contains('\\')
                && !back.chars().any(char::is_control)
        })
        .unwrap_or(default)
}

/// Goes to `back` once an action is done, or shows why `what` failed.
async fn done<T: LocalApi + Clone>(
    api: &Api<T>,
    what: &str,
    result: Result<(), Refusal>,
    back: String,
) -> Response {
    match result {
        Ok(()) => Redirect::to(&back).into_response(),
        Err(Refusal(code, message)) => {
            let status = api.status().await;
            let main = html! {
                h1 { "Couldn't " (what) }
                p.callout { (message) }
                p { a href=(back) { "Back" } }
            };
            page(&status, &format!("Couldn't {what}"), code, main)
        }
    }
}

/// The form's fields, or why they can't be read.
fn fields<F>(form: Result<Form<F>, FormRejection>) -> Result<F, Refusal> {
    form.map(|Form(fields)| fields)
        .map_err(|rejection| Refusal(StatusCode::BAD_REQUEST, rejection.body_text()))
}

#[derive(Deserialize)]
struct TrackForm {
    node: String,
    path: String,
    #[serde(default)]
    name: String,
    one_way: Checkbox,
    #[serde(default)]
    excludes: String,
    #[serde(default)]
    retention_days: String,
}

async fn track<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    form: Result<Form<TrackForm>, FormRejection>,
) -> Response {
    let tracked = async {
        let form = fields(form)?;
        let retention_days = match form.retention_days.trim() {
            "" => None,
            days => Some(days.parse::<NonZeroU32>().map_err(|_| {
                Refusal(
                    StatusCode::BAD_REQUEST,
                    format!("{days} isn't a whole number of days"),
                )
            })?),
        };
        let track = Track {
            node: Some(form.node),
            path: form.path.trim().to_owned(),
            name: Some(form.name.trim().to_owned()).filter(|name| !name.is_empty()),
            one_way: form.one_way.is_some(),
            excludes: form
                .excludes
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_owned)
                .collect(),
            retention_days,
        };
        api.track(track).await
    }
    .await;
    match tracked {
        Ok(id) => Redirect::to(&item_href(id)).into_response(),
        Err(refused) => done(&api, "track it", Err(refused), "/".to_owned()).await,
    }
}

#[derive(Deserialize)]
struct PauseForm {
    paused: bool,
    back: Option<String>,
}

async fn pause_item<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(item): Path<String>,
    form: Result<Form<PauseForm>, FormRejection>,
) -> Response {
    let (result, back_to) = match fields(form) {
        Ok(form) => (api.pause_item(&item, form.paused), form.back),
        Err(refused) => (Err(refused), None),
    };
    let back_to = back(back_to, format!("/items/{}", percent_encode(&item)));
    done(&api, "pause or resume it", result, back_to).await
}

async fn untrack_item<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(item): Path<String>,
) -> Response {
    let result = api.untrack(&item);
    let back_to = match result {
        Ok(()) => "/".to_owned(),
        Err(_) => format!("/items/{}", percent_encode(&item)),
    };
    done(&api, "untrack it", result, back_to).await
}

#[derive(Deserialize)]
struct AddForm {
    node: String,
    path: String,
    mirror: Checkbox,
    adopt: Checkbox,
}

async fn add_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(item): Path<String>,
    form: Result<Form<AddForm>, FormRejection>,
) -> Response {
    let result = match fields(form) {
        Ok(form) => {
            let add = AddReplica {
                node: Some(form.node),
                path: form.path.trim().to_owned(),
                mirror: form.mirror.is_some(),
                adopt: form.adopt.is_some(),
            };
            api.add_replica(&item, add).await
        }
        Err(refused) => Err(refused),
    };
    let back_to = format!("/items/{}", percent_encode(&item));
    done(&api, "add the replica", result, back_to).await
}

async fn pause_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path((item, node)): Path<(String, String)>,
    form: Result<Form<PauseForm>, FormRejection>,
) -> Response {
    let (result, back_to) = match fields(form) {
        Ok(form) => (
            api.pause_replica(&item, &node, form.paused).await,
            form.back,
        ),
        Err(refused) => (Err(refused), None),
    };
    let back_to = back(back_to, format!("/items/{}", percent_encode(&item)));
    done(&api, "pause or resume the replica", result, back_to).await
}

async fn remove_replica<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path((item, node)): Path<(String, String)>,
) -> Response {
    let result = api.remove_replica(&item, &node).await;
    let back_to = format!("/items/{}", percent_encode(&item));
    done(&api, "remove the replica", result, back_to).await
}

#[derive(Deserialize)]
struct ResolveForm {
    item: String,
    version: String,
    back: Option<String>,
}

async fn keep<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    form: Result<Form<ResolveForm>, FormRejection>,
) -> Response {
    resolve(api, form, "keep the file", |api, resolve| async move {
        api.keep(&resolve).await
    })
    .await
}

async fn swap<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    form: Result<Form<ResolveForm>, FormRejection>,
) -> Response {
    resolve(
        api,
        form,
        "swap in the other change",
        |api, resolve| async move { api.swap(&resolve).await },
    )
    .await
}

/// Keeps or swaps the Data Conflict the form names, with `action`.
async fn resolve<T, F>(
    api: Api<T>,
    form: Result<Form<ResolveForm>, FormRejection>,
    what: &str,
    action: impl FnOnce(Api<T>, Resolve) -> F,
) -> Response
where
    T: LocalApi + Clone,
    F: Future<Output = Result<(), Refusal>>,
{
    let (result, back_to) = match fields(form) {
        Ok(form) => {
            let resolve = Resolve {
                item: form.item,
                version: form.version,
            };
            (action(api.clone(), resolve).await, form.back)
        }
        Err(refused) => (Err(refused), None),
    };
    done(&api, what, result, back(back_to, "/conflicts".to_owned())).await
}

#[derive(Deserialize)]
struct LeaveForm {
    confirm: Checkbox,
}

async fn leave<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    form: Result<Form<LeaveForm>, FormRejection>,
) -> Response {
    let result = fields(form).and_then(|form| {
        api.leave(&api::Leave {
            confirm: form.confirm.is_some(),
        })
    });
    done(&api, "leave the Fleet", result, "/".to_owned()).await
}

#[derive(Deserialize)]
struct PeripheralForm {
    /// Its address.
    peripheral: String,
    #[serde(flatten)]
    action: PeripheralAction,
}

/// What `action` does, to say it couldn't.
fn what(action: &PeripheralAction) -> &'static str {
    match action {
        PeripheralAction::Handoff { .. } => "connect it",
        PeripheralAction::Pair { .. } => "pair it",
        PeripheralAction::Disconnect { .. } => "disconnect it",
        PeripheralAction::Forget { .. } => "forget it",
        PeripheralAction::Name { .. } => "rename it",
    }
}

async fn peripheral<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    form: Result<Form<PeripheralForm>, FormRejection>,
) -> Response {
    let back_to = PERIPHERALS.to_owned();
    let PeripheralForm { peripheral, action } = match fields(form) {
        Ok(form) => form,
        Err(refused) => return done(&api, "do that", Err(refused), back_to).await,
    };
    let what = what(&action);
    match api.act_on_peripheral(&peripheral, action).await {
        Ok(Done { notes }) if !notes.is_empty() => {
            let status = api.status().await;
            let main = html! {
                h1 { "Done, with problems" }
                ul { @for note in &notes { li { (note) } } }
                p { a href=(back_to) { "Back" } }
            };
            page(&status, "Done, with problems", StatusCode::OK, main)
        }
        result => done(&api, what, result.map(drop), back_to).await,
    }
}
