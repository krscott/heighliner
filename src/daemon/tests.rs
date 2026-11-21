use axum::Router;
use axum::routing::get;

use super::*;
use crate::model::{AbsPath, Direction, Item, ItemId, Replica};
use crate::tailscale::UserId;
use crate::tailscale::fake::{Fake, device};

const OWNER: UserId = UserId::new(1);
const ITEM: ItemId = ItemId::from_u128(1);

/// A free port, shared by every test daemon on its own loopback address.
async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// A Fake for the device at `me` that sees the devices at `others`.
fn tailnet(me: &str, others: &[&str]) -> Fake {
    let mut fake = Fake::new(device(me, OWNER, me));
    for other in others {
        fake.add_device(device(other, OWNER, other));
    }
    fake
}

fn track(daemon: &Daemon<Fake>, name: &str) {
    let mut db = daemon.db().lock().unwrap();
    let node = db.node().clone();
    db.track(
        Timestamp::now(),
        ITEM,
        Item::new(name, Direction::TwoWay),
        &node,
        Replica::new(AbsPath::new("/srv/x").unwrap()),
    )
    .unwrap();
}

#[tokio::test]
async fn daemons_pull_each_others_records() {
    let port = free_port().await;
    let (a_dir, b_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = Daemon::open(tailnet("127.0.0.1", &["127.0.0.2"]), a_dir.path(), port)
        .await
        .unwrap();
    let b = Daemon::open(tailnet("127.0.0.2", &["127.0.0.1"]), b_dir.path(), port)
        .await
        .unwrap();
    let _a_servers = a.listen().await.unwrap();
    let _b_servers = b.listen().await.unwrap();

    track(&a, "docs");
    let round = b.pull_round().await.unwrap();
    assert_eq!(round.pulled, [NodeId::new("n-127.0.0.1").unwrap()]);
    assert!(b.db().lock().unwrap().config().items.contains_key(&ITEM));

    // b's records reach a too.
    b.db()
        .lock()
        .unwrap()
        .update_item(Timestamp::now(), ITEM, |item| item.name = "renamed".into())
        .unwrap();
    a.pull_round().await.unwrap();
    assert_eq!(
        a.db().lock().unwrap().config().items[&ITEM].item.name,
        "renamed"
    );
}

#[tokio::test]
async fn nodes_with_another_major_version_are_left_out() {
    let port = free_port().await;
    let newer = Router::new().route(
        protocol::HELLO_PATH,
        get(|| async {
            axum::Json(Hello {
                protocol: protocol::MAJOR + 1,
                heighliner: "9.0.0".into(),
            })
        }),
    );
    let listener = TcpListener::bind(("127.0.0.3", port)).await.unwrap();
    tokio::spawn(async move { axum::serve(listener, newer).await });

    let dir = tempfile::tempdir().unwrap();
    let b = Daemon::open(tailnet("127.0.0.2", &["127.0.0.3"]), dir.path(), port)
        .await
        .unwrap();
    let round = b.pull_round().await.unwrap();
    assert_eq!(round.pulled, []);
    assert_eq!(round.incompatible.len(), 1);
    let (node, theirs) = &round.incompatible[0];
    assert_eq!(node.as_str(), "n-127.0.0.3");
    assert_eq!(theirs.protocol, protocol::MAJOR + 1);
}

#[tokio::test]
async fn state_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let fake = tailnet("127.0.0.1", &[]);
    let daemon = Daemon::open(fake.clone(), dir.path(), 1).await.unwrap();
    track(&daemon, "docs");
    let reading = daemon.db().lock().unwrap().last_reading();
    daemon.save().unwrap();
    drop(daemon);

    let reopened = Daemon::open(fake, dir.path(), 1).await.unwrap();
    let db = reopened.db().lock().unwrap();
    assert!(db.config().items.contains_key(&ITEM));
    assert_eq!(db.last_reading(), reading);
}

#[tokio::test]
async fn unknown_state_formats_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join(STATE_FILE),
        r#"{"format": 2, "future": true}"#,
    )
    .unwrap();
    let error = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1)
        .await
        .err()
        .unwrap();
    assert!(matches!(error, Error::StateFormat { format: 2, .. }));
    assert!(
        error.to_string().contains("run a newer heighliner"),
        "{error}"
    );
}
