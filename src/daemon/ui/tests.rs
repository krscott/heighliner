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
