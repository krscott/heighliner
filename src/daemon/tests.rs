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
    let fake = Fake::new(device(me, OWNER, me));
    for other in others {
        fake.add_device(device(other, OWNER, other));
    }
    fake
}

/// Opens a state directory a dropped daemon used. A test that starts a
/// process at the same moment forks this one, and the child holds the
/// directory's lock until it execs, so this retries briefly.
async fn reopen(api: Fake, dir: &std::path::Path) -> Result<Daemon<Fake>, Error> {
    for _ in 0..50 {
        match Daemon::open(api.clone(), dir, 1).await {
            Err(Error::StateLocked { .. }) => tokio::time::sleep(Duration::from_millis(20)).await,
            opened => return opened,
        }
    }
    Daemon::open(api, dir, 1).await
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

    let reopened = reopen(fake, dir.path()).await.unwrap();
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

#[tokio::test]
async fn state_from_another_node_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let before = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1)
        .await
        .unwrap();
    track(&before, "docs");
    before.save().unwrap();
    drop(before);

    let error = reopen(tailnet("127.0.0.9", &[]), dir.path())
        .await
        .err()
        .unwrap();
    assert!(matches!(error, Error::StateNode { .. }), "{error:?}");
    assert!(error.to_string().contains("Move the file aside"), "{error}");
}

#[tokio::test]
async fn addresses_this_machine_lacks_are_skipped() {
    let port = free_port().await;
    let dir = tempfile::tempdir().unwrap();
    let mut me = device("127.0.0.1", OWNER, "127.0.0.1");
    // TEST-NET-1: never configured on a machine.
    me.ips.push("192.0.2.1".parse().unwrap());
    let daemon = Daemon::open(Fake::new(me), dir.path(), port).await.unwrap();
    assert_eq!(daemon.listen().await.unwrap().len(), 1);

    let dir = tempfile::tempdir().unwrap();
    let nowhere = Daemon::open(Fake::new(device("x", OWNER, "192.0.2.1")), dir.path(), port)
        .await
        .unwrap();
    assert!(matches!(nowhere.listen().await, Err(Error::NoAddress)));
}

#[tokio::test]
async fn a_changed_tailnet_stops_the_round() {
    let dir = tempfile::tempdir().unwrap();
    let fake = tailnet("127.0.0.1", &[]);
    let daemon = Daemon::open(fake.clone(), dir.path(), 1).await.unwrap();
    let mut moved = device("127.0.0.1", OWNER, "127.0.0.1");
    moved.ips = vec!["127.0.0.8".parse().unwrap()];
    fake.set_me(moved);
    assert!(matches!(
        daemon.pull_round().await,
        Err(Error::AddressesChanged)
    ));

    // A new identity.
    fake.set_me(device("someone-new", OWNER, "127.0.0.1"));
    assert!(matches!(
        daemon.pull_round().await,
        Err(Error::IdentityChanged)
    ));
}

#[tokio::test]
async fn nodes_that_refuse_are_reported() {
    let port = free_port().await;
    let refusing = Router::new().route(
        protocol::HELLO_PATH,
        get(|| async { (axum::http::StatusCode::SERVICE_UNAVAILABLE, "busy") }),
    );
    let listener = TcpListener::bind(("127.0.0.3", port)).await.unwrap();
    tokio::spawn(async move { axum::serve(listener, refusing).await });

    let dir = tempfile::tempdir().unwrap();
    let b = Daemon::open(tailnet("127.0.0.2", &["127.0.0.3"]), dir.path(), port)
        .await
        .unwrap();
    let round = b.pull_round().await.unwrap();
    assert_eq!(round.failed, [NodeId::new("n-127.0.0.3").unwrap()]);
}

#[tokio::test]
async fn state_from_another_owner_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let before = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1)
        .await
        .unwrap();
    track(&before, "docs");
    before.save().unwrap();
    drop(before);

    let other = Fake::new(device("127.0.0.1", UserId::new(2), "127.0.0.1"));
    let error = reopen(other, dir.path()).await.err().unwrap();
    assert!(matches!(error, Error::StateOwner { .. }));
}

#[tokio::test]
async fn one_daemon_per_state_directory() {
    let dir = tempfile::tempdir().unwrap();
    let _first = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1)
        .await
        .unwrap();
    let second = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1).await;
    assert!(matches!(second, Err(Error::StateLocked { .. })));
}

#[tokio::test]
async fn a_stale_temporary_file_doesnt_loosen_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let stale = dir.path().join("state.tmp");
    fs::write(&stale, "stale").unwrap();
    fs::set_permissions(&stale, fs::Permissions::from_mode(0o644)).unwrap();
    let daemon = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1)
        .await
        .unwrap();
    track(&daemon, "docs");
    daemon.save().unwrap();
    let mode = fs::metadata(dir.path().join(STATE_FILE))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[tokio::test]
async fn rebinding_keeps_the_records() {
    let port = free_port().await;
    let dir = tempfile::tempdir().unwrap();
    let fake = tailnet("127.0.0.1", &[]);
    let mut daemon = Daemon::open(fake.clone(), dir.path(), port).await.unwrap();
    let servers = daemon.listen().await.unwrap();
    track(&daemon, "docs");

    let mut moved = device("127.0.0.1", OWNER, "127.0.0.1");
    moved.ips = vec!["127.0.0.7".parse().unwrap()];
    fake.set_me(moved.clone());
    // WhoIs from the new address must still identify this device.
    fake.add_device(moved);
    assert!(matches!(
        daemon.pull_round().await,
        Err(Error::AddressesChanged)
    ));
    drop(servers);
    let _servers = daemon.relisten().await.unwrap();

    let addr = SocketAddr::new("127.0.0.7".parse().unwrap(), port);
    let hello = protocol::hello(addr, Duration::from_secs(2)).await.unwrap();
    let (store, _) = protocol::pull(addr, &hello, Duration::from_secs(2))
        .await
        .unwrap();
    assert!(crate::config::Config::new(&store).items.contains_key(&ITEM));
}

#[tokio::test]
async fn relistening_refuses_a_new_identity() {
    let dir = tempfile::tempdir().unwrap();
    let fake = tailnet("127.0.0.1", &[]);
    let mut daemon = Daemon::open(fake.clone(), dir.path(), 1).await.unwrap();
    fake.set_me(device("someone-new", OWNER, "127.0.0.1"));
    assert!(matches!(
        daemon.relisten().await,
        Err(Error::IdentityChanged)
    ));
}

#[tokio::test]
async fn an_unchanged_daemon_writes_no_state() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1)
        .await
        .unwrap();
    daemon.save().unwrap();
    assert!(!dir.path().join(STATE_FILE).exists());
    track(&daemon, "docs");
    daemon.save().unwrap();
    assert!(dir.path().join(STATE_FILE).exists());
}

#[tokio::test]
async fn the_daemon_drives_syncthing() {
    let port = crate::syncthing::test_instance::free_port();
    let syncthing = crate::syncthing::test_instance::Syncthing::start("127.0.0.1", port).await;
    let dir = tempfile::tempdir().unwrap();
    let daemon = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1)
        .await
        .unwrap()
        .with_driver(syncthing.driver.clone());
    {
        let mut db = daemon.db().lock().unwrap();
        let node = db.node().clone();
        db.track(
            Timestamp::now(),
            ITEM,
            Item::new("docs", Direction::TwoWay),
            &node,
            Replica::new(syncthing.dir("docs")),
        )
        .unwrap();
    }

    let round = daemon.pull_round().await.unwrap();
    let report = round.syncthing.unwrap();
    assert_eq!(
        report.replicas[&ITEM],
        crate::syncthing::Outcome::Configured
    );
    assert!(syncthing.folder(ITEM).await.is_some());
    // The Node published its Syncthing device ID.
    let device_id = syncthing.driver.client().device_id().await.unwrap();
    let db = daemon.db().lock().unwrap();
    assert_eq!(db.config().nodes[db.node()].device_id, device_id);
}
