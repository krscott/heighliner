use std::path::PathBuf;

use tokio::net::TcpListener;

use super::*;
use crate::api::{AddReplica, Track};
use crate::daemon::Daemon;
use crate::tailscale::UserId;
use crate::tailscale::fake::{Fake, device};

const OWNER: UserId = UserId::new(1);

#[test]
fn node_paths_split() {
    let cwd = PathBuf::from("/home/me/work");
    let split = |argument| node_path(argument, Some(&cwd), &["laptop"]);
    assert_eq!(
        split("nas:/srv/docs"),
        (Some("nas".into()), "/srv/docs".into())
    );
    assert_eq!(split("nas:docs"), (Some("nas".into()), "docs".into()));
    assert_eq!(split("/srv/a:b"), (None, "/srv/a:b".into()));
    assert_eq!(split("~/docs"), (None, "~/docs".into()));
    assert_eq!(split("docs"), (None, "/home/me/work/docs".into()));
    assert_eq!(split("sub/a:b"), (None, "/home/me/work/sub/a:b".into()));
    // Written as a Replica's path is.
    assert_eq!(split("."), (None, "/home/me/work".into()));
    assert_eq!(split("./a:b"), (None, "/home/me/work/a:b".into()));
    assert_eq!(split("docs/"), (None, "/home/me/work/docs".into()));
    assert_eq!(split("../x"), (None, "/home/me/x".into()));
    assert_eq!(split("docs//sub"), (None, "/home/me/work/docs/sub".into()));
    // This Node, named.
    assert_eq!(split("laptop:docs"), (None, "/home/me/work/docs".into()));
    // Another Node's daemon, asked with --on, gets relative paths as they
    // are, to refuse.
    assert_eq!(node_path("docs", None, &["nas"]), (None, "docs".into()));
    assert_eq!(
        node_path("nas:/srv/x/", None, &["nas"]),
        (None, "/srv/x".into())
    );
}

fn track(path: &str) -> Track {
    Track {
        node: None,
        path: path.into(),
        name: None,
        one_way: false,
        excludes: vec!["*.tmp".into()],
        retention_days: None,
    }
}

#[tokio::test]
async fn the_cli_edits_through_the_daemon() {
    let port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let fake = Fake::new(device("laptop", OWNER, "127.0.0.1"));
    fake.add_device(device("nas", OWNER, "127.0.0.2"));
    let state = tempfile::tempdir().unwrap();
    let daemon = Daemon::open(fake.clone(), state.path(), port)
        .await
        .unwrap();
    let _servers = daemon.listen().await.unwrap();
    let client = Client::connect(&fake, port, None).await.unwrap();

    let docs = tempfile::tempdir().unwrap();
    let docs = docs.path().join("docs");
    let tracked = client.track(&track(docs.to_str().unwrap())).await.unwrap();
    let status = client.status().await.unwrap();
    let [item] = &status.items[..] else {
        panic!("{status:?}");
    };
    assert_eq!((item.id, item.name.as_str()), (tracked.id, "docs"));
    assert_eq!(item.excludes, ["*.tmp"]);
    assert_eq!(item.replicas[0].node_name.as_deref(), Some("laptop"));

    // A path on another Node must be absolute.
    let add = |node: &str, path: &str| AddReplica {
        node: Some(node.into()),
        path: path.into(),
        mirror: true,
        adopt: false,
    };
    let refused = client.add_replica("docs", &add("nas", "docs")).await;
    assert!(
        matches!(&refused, Err(Error::Refused { message, .. }) if message.contains("absolute")),
        "{refused:?}"
    );
    // Only at this device's address: not a rebound name.
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let (status, _) = crate::http::get(stream, "evil.example", crate::api::STATUS)
        .await
        .unwrap();
    assert_eq!(status, StatusCode::MISDIRECTED_REQUEST);
    let refused = client.add_replica("docs", &add("attic", "/srv/docs")).await;
    assert!(
        matches!(&refused, Err(Error::Refused { status, .. }) if *status == StatusCode::NOT_FOUND)
    );
    client
        .add_replica("docs", &add("nas", "/srv//docs/"))
        .await
        .unwrap();
    client.pause_replica("docs", "nas", true).await.unwrap();
    let status = client.status().await.unwrap();
    let nas = status.items[0]
        .replicas
        .iter()
        .find(|replica| replica.node_name.as_deref() == Some("nas"))
        .unwrap();
    assert_eq!(
        (nas.path.as_str(), nas.role, nas.paused),
        ("/srv/docs", crate::model::Role::Mirror, true)
    );
    assert!(format_status(&status).contains("  nas:/srv/docs mirror, paused"));

    // A name two Items share is refused.
    let other = tempfile::tempdir().unwrap();
    client
        .track(&track(other.path().join("docs").to_str().unwrap()))
        .await
        .unwrap();
    let refused = client.untrack("docs").await;
    assert!(
        matches!(&refused, Err(Error::Refused { message, .. }) if message.contains("IDs")),
        "{refused:?}"
    );
    client
        .remove_replica(&tracked.id.to_string(), "nas")
        .await
        .unwrap();
    client.untrack(&tracked.id.to_string()).await.unwrap();
    assert_eq!(client.status().await.unwrap().items.len(), 1);
}

#[tokio::test]
async fn a_tilde_is_the_daemons_users_home() {
    let Some(home) = std::env::var_os("HOME") else {
        return;
    };
    let port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let fake = Fake::new(device("laptop", OWNER, "127.0.0.1"));
    let state = tempfile::tempdir().unwrap();
    let daemon = Daemon::open(fake.clone(), state.path(), port)
        .await
        .unwrap();
    let _servers = daemon.listen().await.unwrap();
    let client = Client::connect(&fake, port, None).await.unwrap();
    client.track(&track("~/notes")).await.unwrap();
    let status = client.status().await.unwrap();
    let expected = format!("{}/notes", home.to_str().unwrap());
    assert_eq!(status.items[0].replicas[0].path, expected);
}

#[tokio::test]
async fn edits_are_saved_and_ids_are_only_ids() {
    let port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    // The first address isn't this machine's, so the daemon skips it.
    let mut me = device("laptop", OWNER, "192.0.2.1");
    me.ips.push("127.0.0.1".parse().unwrap());
    let fake = Fake::new(me);
    let state = tempfile::tempdir().unwrap();
    let daemon = Daemon::open(fake.clone(), state.path(), port)
        .await
        .unwrap();
    let _servers = daemon.listen().await.unwrap();
    let client = Client::connect(&fake, port, None).await.unwrap();

    let docs = tempfile::tempdir().unwrap();
    let first = client
        .track(&track(docs.path().to_str().unwrap()))
        .await
        .unwrap();
    // Saved before it was acknowledged.
    let saved = std::fs::read_to_string(state.path().join("state.json")).unwrap();
    assert!(saved.contains(&first.id.to_string()));

    // An Item named with another's ID isn't reached by that ID.
    let other = tempfile::tempdir().unwrap();
    let mut named = track(other.path().to_str().unwrap());
    named.name = Some(first.id.to_string());
    client.track(&named).await.unwrap();
    client.untrack(&first.id.to_string()).await.unwrap();
    let again = client.untrack(&first.id.to_string()).await;
    assert!(
        matches!(&again, Err(Error::Refused { status, .. }) if *status == StatusCode::NOT_FOUND),
        "{again:?}"
    );
    assert_eq!(client.status().await.unwrap().items.len(), 1);

    // A body that isn't the JSON a route takes is refused as JSON.
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let request = hyper::Request::post(crate::api::ITEMS)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(http_body_util::Full::new(hyper::body::Bytes::from(
            "{\"path\": 1}",
        )))
        .unwrap();
    let (status, body) = crate::http::send(stream, &format!("127.0.0.1:{port}"), request)
        .await
        .unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let refused: crate::api::Refused = serde_json::from_slice(&body).unwrap();
    assert!(!refused.error.is_empty());
}
