use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use tower::ServiceExt;

use super::*;
use crate::daemon::Daemon;
use crate::model::{AbsPath, Direction, Item, Replica};
use crate::tailscale::UserId;
use crate::tailscale::fake::{Fake, device};

const ITEM: ItemId = ItemId::from_u128(1);

async fn daemon(dir: &std::path::Path) -> Daemon<Fake> {
    let fake = Fake::new(device("laptop", UserId::new(1), "127.0.0.1"));
    Daemon::open(fake, dir, 1).await.unwrap()
}

fn track(daemon: &Daemon<Fake>, name: &str) {
    let mut db = daemon.db().lock().unwrap();
    let node = db.node().clone();
    db.track(
        Timestamp::now(),
        ITEM,
        Item::new(name, Direction::TwoWay),
        &node,
        Replica::new(AbsPath::new("/srv/docs").unwrap()),
    )
    .unwrap();
}

/// The status, headers and body of the page at `path`.
async fn get_page(daemon: &Daemon<Fake>, path: &str) -> (StatusCode, header::HeaderMap, String) {
    let request = Request::get(path).body(Body::empty()).unwrap();
    let response = router(daemon.api()).oneshot(request).await.unwrap();
    let (parts, body) = response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    (
        parts.status,
        parts.headers,
        String::from_utf8(body.to_vec()).unwrap(),
    )
}

#[tokio::test]
async fn the_fleet_page_shows_items_and_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = daemon(dir.path()).await;
    track(&daemon, "<b>docs</b>");
    let (code, headers, body) = get_page(&daemon, "/").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(headers[header::CONTENT_SECURITY_POLICY], POLICY);
    // Names are escaped.
    assert!(body.contains("&lt;b&gt;docs&lt;/b&gt;"), "{body}");
    assert!(body.contains(&format!("href=\"/items/{ITEM}\"")));
    assert!(body.contains("this Node"));
    assert!(body.contains("not confirmed fully replicated"));
    assert!(body.contains("<script src=\"/ui/live.js\" defer>"));
}

#[tokio::test]
async fn item_and_replica_pages_show_their_details() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = daemon(dir.path()).await;
    track(&daemon, "docs");
    let (code, _, body) = get_page(&daemon, &format!("/items/{ITEM}")).await;
    assert_eq!(code, StatusCode::OK);
    assert!(body.contains("<code>/srv/docs</code>"), "{body}");
    assert!(body.contains("two-way"));

    let path = format!("/items/{ITEM}/replicas/n-laptop");
    let (code, _, body) = get_page(&daemon, &path).await;
    assert_eq!(code, StatusCode::OK);
    assert!(body.contains("docs on laptop"), "{body}");
    assert!(body.contains("Last confirmed current</dt><dd>never"));

    // Gone, or never there.
    for path in [
        "/items/nonsense".to_owned(),
        format!("/items/{}", ItemId::from_u128(2)),
        format!("/items/{ITEM}/replicas/n-elsewhere"),
    ] {
        let (code, _, body) = get_page(&daemon, &path).await;
        assert_eq!(code, StatusCode::NOT_FOUND, "{path}");
        assert!(body.contains("<main>"), "{body}");
    }
}

#[tokio::test]
async fn the_conflicts_page_says_when_there_are_none() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = daemon(dir.path()).await;
    let (code, _, body) = get_page(&daemon, "/conflicts").await;
    assert_eq!(code, StatusCode::OK);
    assert!(body.contains("Data conflicts on laptop"), "{body}");
    assert!(body.contains("None."));
}

#[tokio::test]
async fn pages_hear_of_changes() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = daemon(dir.path()).await;
    let request = Request::get(EVENTS).body(Body::empty()).unwrap();
    let response = router(daemon.api()).oneshot(request).await.unwrap();
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream"
    );
    let mut body = response.into_body();
    track(&daemon, "docs");
    crate::daemon::signal(&daemon.changes);
    let frame = tokio::time::timeout(Duration::from_secs(5), body.frame())
        .await
        .expect("a change is sent before the tick")
        .unwrap()
        .unwrap();
    let text = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
    assert_eq!(text, "event: changed\ndata: changed\n\n");
}

#[test]
fn ages_and_sizes_read_at_a_glance() {
    let now = Timestamp::from_millis(10 * 86_400_000);
    let before = |secs: u64| Timestamp::from_millis(now.as_millis() - secs * 1000);
    assert_eq!(ago(now, before(5)), "just now");
    assert_eq!(ago(now, before(125)), "2 min ago");
    assert_eq!(ago(now, before(3 * 3600)), "3 h ago");
    assert_eq!(ago(now, before(2 * 86_400)), "2 days ago");
    // A clock ahead of this one's.
    assert_eq!(
        ago(now, now.saturating_add(Duration::from_secs(60))),
        "just now"
    );
    assert_eq!(
        until(now, now.saturating_add(Duration::from_secs(3 * 86_400))),
        "in 3 days"
    );
    assert_eq!(size(512), "512 B");
    assert_eq!(size(1536), "1.5 KiB");
    assert_eq!(size(300 * 1024 * 1024), "300 MiB");
}

#[tokio::test]
async fn the_fleet_page_says_when_conflicts_cant_be_read() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = daemon(dir.path()).await;
    track(&daemon, "docs");
    // Configured, but its path isn't there, so its conflicts can't be read.
    let config = daemon.db().lock().unwrap().config();
    let outcomes = [(ITEM, crate::syncthing::Outcome::Configured)].into();
    daemon.replicas.lock().unwrap().reconciled =
        Some(std::sync::Arc::new(crate::daemon::Reconciled {
            config,
            outcomes: Ok(outcomes),
        }));
    let (_, _, body) = get_page(&daemon, "/").await;
    assert!(
        body.contains("Some of this Node's data conflicts can't be read"),
        "{body}"
    );
}

/// Posts `form` to `path` from `origin`, if any, as a browser would, and
/// returns the status, `Location` and body.
async fn post_form(
    daemon: &Daemon<Fake>,
    path: &str,
    form: &str,
    origin: Option<&str>,
) -> (StatusCode, Option<String>, String) {
    let mut request = Request::post(path)
        .header(header::HOST, "127.0.0.1:7130")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
    if let Some(origin) = origin {
        request = request.header(header::ORIGIN, origin);
    }
    let request = request.body(Body::from(form.to_owned())).unwrap();
    let response = router(daemon.api()).oneshot(request).await.unwrap();
    let (parts, body) = response.into_parts();
    let location = parts
        .headers
        .get(header::LOCATION)
        .map(|value| value.to_str().unwrap().to_owned());
    let body = body.collect().await.unwrap().to_bytes();
    (
        parts.status,
        location,
        String::from_utf8(body.to_vec()).unwrap(),
    )
}

const OURS: Option<&str> = Some("http://127.0.0.1:7130");

#[tokio::test]
async fn only_the_daemons_own_pages_can_post() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = daemon(dir.path()).await;
    track(&daemon, "docs");
    let path = format!("/ui/items/{ITEM}/paused");
    for origin in [None, Some("http://evil.example"), Some("null")] {
        let (code, _, _) = post_form(&daemon, &path, "paused=true", origin).await;
        assert_eq!(code, StatusCode::FORBIDDEN, "{origin:?}");
    }
    assert!(
        !daemon.db().lock().unwrap().config().items[&ITEM]
            .item
            .paused
    );

    let (code, location, _) = post_form(&daemon, &path, "paused=true", OURS).await;
    assert_eq!(code, StatusCode::SEE_OTHER);
    assert_eq!(location.as_deref(), Some(format!("/items/{ITEM}").as_str()));
    assert!(
        daemon.db().lock().unwrap().config().items[&ITEM]
            .item
            .paused
    );
}

#[tokio::test]
async fn a_directory_is_tracked_and_edited_through_forms() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = daemon(dir.path()).await;
    let form = "node=n-laptop&path=%2Fsrv%2Fphotos&name=&one_way=on&excludes=*.tmp%0D%0A%0D%0Acache&retention_days=7";
    let (code, location, body) = post_form(&daemon, actions::TRACK, form, OURS).await;
    assert_eq!(code, StatusCode::SEE_OTHER, "{body}");
    let config = daemon.db().lock().unwrap().config();
    let (id, entry) = config.items.iter().next().unwrap();
    assert_eq!(location, Some(item_href(*id)));
    assert_eq!(entry.item.name, "photos");
    assert!(matches!(entry.item.direction, Direction::OneWay { .. }));
    let excludes: Vec<&str> = entry.item.excludes.iter().map(|p| p.as_str()).collect();
    assert_eq!(excludes, ["*.tmp", "cache"]);
    assert_eq!(entry.item.retention_days.map(|days| days.get()), Some(7));
    let id = *id;

    // A replica elsewhere, paused, then removed, coming back to the page
    // the button was on.
    daemon
        .db()
        .lock()
        .unwrap()
        .add_replica(
            Timestamp::now(),
            id,
            &crate::model::NodeId::new("n-nas").unwrap(),
            Replica::new(AbsPath::new("/srv/nas").unwrap()),
        )
        .unwrap();
    let back = format!("/items/{id}/replicas/n-nas");
    let (code, location, _) = post_form(
        &daemon,
        &actions::replica_paused(id, "n-nas"),
        &format!("paused=true&back={}", percent_encode(&back)),
        OURS,
    )
    .await;
    assert_eq!(
        (code, location.as_deref()),
        (StatusCode::SEE_OTHER, Some(back.as_str()))
    );
    let paused = |daemon: &Daemon<Fake>| {
        let config = daemon.db().lock().unwrap().config();
        config.items[&id]
            .replicas
            .get(&crate::model::NodeId::new("n-nas").unwrap())
            .map(|replica| replica.paused)
    };
    assert_eq!(paused(&daemon), Some(true));
    let (code, _, _) = post_form(&daemon, &actions::remove(id, "n-nas"), "", OURS).await;
    assert_eq!(code, StatusCode::SEE_OTHER);
    assert_eq!(paused(&daemon), None);

    let (code, location, _) = post_form(&daemon, &actions::untrack(id), "", OURS).await;
    assert_eq!(
        (code, location.as_deref()),
        (StatusCode::SEE_OTHER, Some("/"))
    );
    assert!(daemon.db().lock().unwrap().config().items.is_empty());
}

#[tokio::test]
async fn a_refused_action_says_why() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = daemon(dir.path()).await;
    let (code, _, body) =
        post_form(&daemon, actions::TRACK, "node=n-laptop&path=relative", OURS).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert!(body.contains("Couldn't track it"), "{body}");
    assert!(
        body.contains("isn&#39;t absolute") || body.contains("isn't absolute"),
        "{body}"
    );

    let (code, _, body) = post_form(&daemon, actions::TRACK, "node=n-laptop", OURS).await;
    assert_eq!(code, StatusCode::BAD_REQUEST, "{body}");

    // Leaving needs its box checked.
    let (code, _, body) = post_form(&daemon, actions::LEAVE, "", OURS).await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert!(body.contains("confirm it"), "{body}");
}

#[tokio::test]
async fn coming_back_stays_on_the_daemons_pages() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = daemon(dir.path()).await;
    track(&daemon, "docs");
    let path = format!("/ui/items/{ITEM}/paused");
    for back in [
        "%2F%2Fevil.example",
        "https%3A%2F%2Fevil.example",
        "%2F%5Cevil.example",
    ] {
        let form = format!("paused=false&back={back}");
        let (_, location, _) = post_form(&daemon, &path, &form, OURS).await;
        assert_eq!(location, Some(format!("/items/{ITEM}")), "{back}");
    }
}

#[tokio::test]
async fn pages_offer_their_actions() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = daemon(dir.path()).await;
    track(&daemon, "docs");
    let (_, _, fleet) = get_page(&daemon, "/").await;
    assert!(
        fleet.contains(&format!("action=\"{}\"", actions::TRACK)),
        "{fleet}"
    );
    assert!(fleet.contains(&format!("action=\"{}\"", actions::LEAVE)));
    let (_, _, item) = get_page(&daemon, &format!("/items/{ITEM}")).await;
    for action in [
        actions::item_paused(ITEM),
        actions::untrack(ITEM),
        actions::replicas(ITEM),
        actions::remove(ITEM, "n-laptop"),
    ] {
        assert!(
            item.contains(&format!("action=\"{action}\"")),
            "{action}: {item}"
        );
    }
}
