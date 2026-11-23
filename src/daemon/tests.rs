use axum::Router;
use axum::routing::get;

use super::*;
use crate::model::{AbsPath, Direction, Item, ItemId, Replica, ReplicaState};
use crate::tailscale::UserId;
use crate::tailscale::fake::{Fake, device};

const OWNER: UserId = UserId::new(1);
const ITEM: ItemId = ItemId::from_u128(1);

/// A free port, shared by every test daemon on its own loopback address.
async fn free_port() -> u16 {
    crate::syncthing::test_instance::free_port()
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
    {
        let db = daemon.db().lock().unwrap();
        assert_eq!(db.config().nodes[db.node()].device_id, device_id);
    }
    // With no other Replicas to receive from, it's current.
    assert_eq!(daemon.states()[&ITEM], ReplicaState::Current);
}

#[tokio::test]
async fn the_daemon_remembers_when_replicas_were_current() {
    let port = crate::syncthing::test_instance::free_port();
    let syncthing = crate::syncthing::test_instance::Syncthing::start("127.0.0.1", port).await;
    let dir = tempfile::tempdir().unwrap();
    let api = tailnet("127.0.0.1", &[]);
    let open = || async {
        reopen(api.clone(), dir.path())
            .await
            .unwrap()
            .with_driver(syncthing.driver.clone())
    };
    let elsewhere = NodeId::new("n-elsewhere").unwrap();
    let sender = || Replica::new(AbsPath::new("/srv/docs").unwrap());
    let daemon = open().await;
    {
        let mut db = daemon.db().lock().unwrap();
        let me = db.node().clone();
        db.track(
            Timestamp::now(),
            ITEM,
            Item::new("docs", Direction::TwoWay),
            &me,
            Replica::new(syncthing.dir("docs")),
        )
        .unwrap();
    }
    let before = Timestamp::now();
    daemon.pull_round().await.unwrap();
    let after = Timestamp::now();
    assert_eq!(daemon.states()[&ITEM], ReplicaState::Current);
    // Stopped while current.
    daemon.save_on_stop().unwrap();
    drop(daemon);

    // Running again, it gains a sender that it can't reach.
    let daemon = open().await;
    daemon
        .db()
        .lock()
        .unwrap()
        .add_replica(Timestamp::now(), ITEM, &elsewhere, sender())
        .unwrap();
    daemon.pull_round().await.unwrap();
    let ReplicaState::Behind {
        last_current: Some(at),
    } = daemon.states()[&ITEM]
    else {
        panic!("{:?}", daemon.states()[&ITEM]);
    };
    assert!(before <= at && at <= after);

    // Current again, then behind again: confirmed at its last read.
    daemon
        .db()
        .lock()
        .unwrap()
        .remove_replica(Timestamp::now(), ITEM, &elsewhere)
        .unwrap();
    let before = Timestamp::now();
    daemon.pull_round().await.unwrap();
    let after = Timestamp::now();
    assert_eq!(daemon.states()[&ITEM], ReplicaState::Current);
    daemon
        .db()
        .lock()
        .unwrap()
        .add_replica(Timestamp::now(), ITEM, &elsewhere, sender())
        .unwrap();
    daemon.pull_round().await.unwrap();
    let ReplicaState::Behind {
        last_current: Some(at),
    } = daemon.states()[&ITEM]
    else {
        panic!("{:?}", daemon.states()[&ITEM]);
    };
    assert!(before <= at && at <= after);

    // Moved to a new path, it hasn't been current there, even when its
    // state is read before Syncthing is told of the move.
    daemon
        .db()
        .lock()
        .unwrap()
        .remove_replica(Timestamp::now(), ITEM, &elsewhere)
        .unwrap();
    daemon.pull_round().await.unwrap();
    assert_eq!(daemon.states()[&ITEM], ReplicaState::Current);
    {
        let mut db = daemon.db().lock().unwrap();
        let me = db.node().clone();
        // Another Replica stands in while this one moves, so that it can be
        // removed.
        db.add_replica(Timestamp::now(), ITEM, &elsewhere, sender())
            .unwrap();
        db.remove_replica(Timestamp::now(), ITEM, &me).unwrap();
        db.add_replica(
            Timestamp::now(),
            ITEM,
            &me,
            Replica::new(syncthing.dir("moved")),
        )
        .unwrap();
        db.remove_replica(Timestamp::now(), ITEM, &elsewhere)
            .unwrap();
    }
    daemon.refresh().await;
    daemon
        .db()
        .lock()
        .unwrap()
        .add_replica(Timestamp::now(), ITEM, &elsewhere, sender())
        .unwrap();
    daemon.pull_round().await.unwrap();
    daemon.pull_round().await.unwrap();
    assert_eq!(
        daemon.states()[&ITEM],
        ReplicaState::Behind { last_current: None }
    );
}

#[tokio::test]
async fn a_replica_added_again_is_checked_afresh() {
    let port = crate::syncthing::test_instance::free_port();
    let syncthing = crate::syncthing::test_instance::Syncthing::start("127.0.0.1", port).await;
    let dir = tempfile::tempdir().unwrap();
    let daemon = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1)
        .await
        .unwrap()
        .with_driver(syncthing.driver.clone());
    let at = syncthing.dir("docs");
    let me = daemon.db().lock().unwrap().node().clone();
    let elsewhere = NodeId::new("n-elsewhere").unwrap();
    {
        let mut db = daemon.db().lock().unwrap();
        db.track(
            Timestamp::now(),
            ITEM,
            Item::new("docs", Direction::TwoWay),
            &elsewhere,
            Replica::new(AbsPath::new("/srv/docs").unwrap()),
        )
        .unwrap();
        db.add_replica(Timestamp::now(), ITEM, &me, Replica::new(at.clone()))
            .unwrap();
    }
    daemon.pull_round().await.unwrap();

    // Removed, then unrelated files arrive, then added again.
    daemon
        .db()
        .lock()
        .unwrap()
        .remove_replica(Timestamp::now(), ITEM, &me)
        .unwrap();
    daemon.pull_round().await.unwrap();
    fs::write(std::path::Path::new(at.as_str()).join("unrelated"), "x").unwrap();
    daemon
        .db()
        .lock()
        .unwrap()
        .add_replica(Timestamp::now(), ITEM, &me, Replica::new(at.clone()))
        .unwrap();
    let round = daemon.pull_round().await.unwrap();
    assert_eq!(
        round.syncthing.unwrap().replicas[&ITEM],
        crate::syncthing::Outcome::NotEmpty
    );
}

#[tokio::test]
async fn the_daemon_collects_keeps_and_swaps_data_conflicts() {
    let port = crate::syncthing::test_instance::free_port();
    let syncthing = crate::syncthing::test_instance::Syncthing::start("127.0.0.1", port).await;
    let dir = tempfile::tempdir().unwrap();
    let daemon = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1)
        .await
        .unwrap()
        .with_driver(syncthing.driver.clone());
    let at = syncthing.dir("docs");
    {
        let mut db = daemon.db().lock().unwrap();
        let me = db.node().clone();
        db.track(
            Timestamp::now(),
            ITEM,
            Item::new("docs", Direction::TwoWay),
            &me,
            Replica::new(at.clone()),
        )
        .unwrap();
    }
    daemon.pull_round().await.unwrap();
    let root = std::path::Path::new(at.as_str());
    let write = |path: &str, content: &str| {
        let path = root.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    };
    write("sub/a.txt", "won");
    // Named as Syncthing would now, so that they're newer than the set-up.
    let now = Timestamp::now().to_string();
    let at: String = now[..19].chars().filter(char::is_ascii_digit).collect();
    let at = format!("{}-{}", &at[..8], &at[8..]);
    write(&format!("sub/a.sync-conflict-{at}-ABCDEFG.txt"), "lost");
    write("b.txt", "won");
    write(&format!("b.sync-conflict-{at}-ABCDEFH.txt"), "lost");

    // Where Syncthing finished an item, its directory is looked in.
    let finished = crate::syncthing::Finished {
        item: ITEM,
        path: "sub/a.txt".into(),
    };
    let queue = super::conflicts::Queue::default();
    queue.add(crate::syncthing::Seen::Finished(finished));
    let me = daemon.db().lock().unwrap().node().clone();
    queue.run_now(&daemon.replicas, &me);
    async fn found(daemon: &Daemon<Fake>) -> Vec<crate::syncthing::conflicts::DataConflict> {
        daemon.conflicts().await.remove(&ITEM).unwrap().unwrap()
    }
    let listed = found(&daemon).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path, "sub/a.txt");
    // A sweep finds the rest.
    daemon.sweep_conflicts().await;
    let listed = found(&daemon).await;
    assert_eq!(listed.len(), 2);

    let kept = daemon
        .keep_conflict(ITEM, &listed[0].version)
        .await
        .unwrap();
    assert_eq!(kept.path, "b.txt");
    assert_eq!(fs::read_to_string(root.join("b.txt")).unwrap(), "won");
    let swapped = daemon
        .swap_conflict(ITEM, &listed[1].version)
        .await
        .unwrap();
    assert_eq!(swapped.path, "sub/a.txt");
    assert_eq!(fs::read_to_string(root.join("sub/a.txt")).unwrap(), "lost");
    assert!(found(&daemon).await.is_empty());
    assert!(matches!(
        daemon.keep_conflict(ItemId::from_u128(9), "x").await,
        Err(ActionError::NoReplica(_))
    ));
}

#[tokio::test]
async fn status_shows_the_other_nodes_states_as_last_read() {
    let port = free_port().await;
    let (a_dir, b_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a_tailnet = tailnet("127.0.0.1", &["127.0.0.2"]);
    let a = Daemon::open(a_tailnet.clone(), a_dir.path(), port)
        .await
        .unwrap();
    let b = Daemon::open(tailnet("127.0.0.2", &["127.0.0.1"]), b_dir.path(), port)
        .await
        .unwrap();
    let _a_servers = a.listen().await.unwrap();
    let _b_servers = b.listen().await.unwrap();
    track(&a, "docs");
    b.pull_round().await.unwrap();
    {
        let b_node = b.db().lock().unwrap().node().clone();
        b.db()
            .lock()
            .unwrap()
            .add_replica(
                Timestamp::now(),
                ITEM,
                &b_node,
                Replica::new(AbsPath::new("/srv/b").unwrap()),
            )
            .unwrap();
        let mut replicas = b.replicas.lock().unwrap();
        replicas.states.insert(ITEM, ReplicaState::Current);
        replicas
            .read_for
            .insert(ITEM, AbsPath::new("/srv/b").unwrap());
    }
    let before = Timestamp::now();
    a.pull_round().await.unwrap();
    let client = crate::cli::Client::connect(&a_tailnet, port, None)
        .await
        .unwrap();
    let status = client.status().await.unwrap();
    let theirs = status.items[0]
        .replicas
        .iter()
        .find(|replica| replica.path == "/srv/b")
        .unwrap();
    assert_eq!(theirs.state, Some(ReplicaState::Current));
    assert!(theirs.state_read_at.is_some_and(|at| at >= before));

    // B, asked through A's tailnet, says its own state is live.
    let on_b = crate::cli::Client::connect(&a_tailnet, port, Some("127.0.0.2"))
        .await
        .unwrap();
    let status = on_b.status().await.unwrap();
    let own = status.items[0]
        .replicas
        .iter()
        .find(|replica| replica.path == "/srv/b")
        .unwrap();
    assert_eq!(
        (own.state.clone(), own.state_read_at),
        (Some(ReplicaState::Current), None)
    );
}

#[tokio::test]
async fn a_node_leaves_through_the_api() {
    let port = free_port().await;
    let dir = tempfile::tempdir().unwrap();
    let fake = tailnet("127.0.0.1", &[]);
    let daemon = Daemon::open(fake.clone(), dir.path(), port).await.unwrap();
    let _servers = daemon.listen().await.unwrap();
    track(&daemon, "docs");
    let client = crate::cli::Client::connect(&fake, port, None)
        .await
        .unwrap();
    // Not without a JSON confirmation, which a web page can't send.
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let request = hyper::Request::post(crate::api::LEAVE)
        .header(hyper::header::CONTENT_TYPE, "text/plain")
        .body(http_body_util::Full::new(hyper::body::Bytes::new()))
        .unwrap();
    let (status, _) = crate::http::send(stream, &format!("127.0.0.1:{port}"), request)
        .await
        .unwrap();
    assert_eq!(status, hyper::StatusCode::BAD_REQUEST);
    let refused = client.leave().await;
    assert!(
        matches!(&refused, Err(crate::cli::Error::Refused { message, .. }) if message.contains("joined")),
        "{refused:?}"
    );
    daemon
        .db()
        .lock()
        .unwrap()
        .publish(
            Timestamp::now(),
            crate::model::DeviceId::new(
                "MFZWI3D-BONSGYC-YLTMRWG-C43ENR5-QXGZDMM-FZWI3DP-BONSGYY-LTMRWAD",
            )
            .unwrap(),
            None,
        )
        .unwrap();
    client.leave().await.unwrap();
    let status = client.status().await.unwrap();
    assert!(status.items[0].replicas[0].departed);
    // Nothing new goes on a Node that has left.
    let track = crate::api::Track {
        node: None,
        path: "/srv/more".into(),
        name: None,
        one_way: false,
        excludes: Vec::new(),
        retention_days: None,
    };
    let refused = client.track(&track).await;
    assert!(
        matches!(&refused, Err(crate::cli::Error::Refused { message, .. }) if message.contains("left")),
        "{refused:?}"
    );
    assert!(crate::cli::format_status(&status).contains("departed"));
}

#[tokio::test]
async fn conflicts_are_listed_and_kept_through_the_api() {
    let port = crate::syncthing::test_instance::free_port();
    let syncthing = crate::syncthing::test_instance::Syncthing::start("127.0.0.1", port).await;
    let dir = tempfile::tempdir().unwrap();
    let fake = tailnet("127.0.0.1", &[]);
    let api_port = free_port().await;
    let daemon = Daemon::open(fake.clone(), dir.path(), api_port)
        .await
        .unwrap()
        .with_driver(syncthing.driver.clone());
    let _servers = daemon.listen().await.unwrap();
    let at = syncthing.dir("docs");
    {
        let mut db = daemon.db().lock().unwrap();
        let me = db.node().clone();
        db.track(
            Timestamp::now(),
            ITEM,
            Item::new("docs", Direction::TwoWay),
            &me,
            Replica::new(at.clone()),
        )
        .unwrap();
    }
    daemon.pull_round().await.unwrap();
    let now = Timestamp::now().to_string();
    let stamp: String = now[..19].chars().filter(char::is_ascii_digit).collect();
    let copy = format!(
        "notes.sync-conflict-{}-{}-ABCDEFG.txt",
        &stamp[..8],
        &stamp[8..]
    );
    let root = std::path::Path::new(at.as_str());
    fs::write(root.join("notes.txt"), "won").unwrap();
    fs::write(root.join(&copy), "lost").unwrap();
    daemon.sweep_conflicts().await;

    let client = crate::cli::Client::connect(&fake, api_port, None)
        .await
        .unwrap();
    let listed = client.conflicts().await.unwrap();
    let [conflict] = &listed.conflicts[..] else {
        panic!("{listed:?}");
    };
    assert_eq!(
        (conflict.item_name.as_str(), conflict.path.as_str()),
        ("docs", "notes.txt")
    );
    assert!(crate::cli::format_conflicts(&listed).contains("docs notes.txt"));
    let resolve = crate::api::Resolve {
        item: "docs".into(),
        version: conflict.version.clone(),
    };
    client.keep(&resolve).await.unwrap();
    assert!(client.conflicts().await.unwrap().conflicts.is_empty());
    let again = client.keep(&resolve).await;
    assert!(
        matches!(&again, Err(crate::cli::Error::Refused { status, .. }) if *status == hyper::StatusCode::NOT_FOUND),
        "{again:?}"
    );
}

/// Marks `daemon`'s Replica of [`ITEM`] at `path` current, confirmed so at
/// `at` and connected to every sender it has, as a refresh would.
fn confirm_current(daemon: &Daemon<Fake>, path: &str, at: Timestamp) {
    let path = AbsPath::new(path).unwrap();
    let senders = {
        let db = daemon.db().lock().unwrap();
        let config = db.config();
        config.senders_to(&config.items[&ITEM], db.node())
    };
    let mut replicas = daemon.replicas.lock().unwrap();
    replicas.states.insert(ITEM, ReplicaState::Current);
    replicas.read_for.insert(ITEM, path.clone());
    replicas.current.insert(ITEM, Confirmed { path, at });
    replicas.connected.insert(ITEM, senders);
}

#[tokio::test]
async fn an_item_is_fully_replicated_only_while_every_replica_is_seen_current() {
    use crate::api::{Reach, Replication};

    let port = free_port().await;
    let (a_dir, b_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = Daemon::open(tailnet("127.0.0.1", &["127.0.0.2"]), a_dir.path(), port)
        .await
        .unwrap();
    let b = Daemon::open(tailnet("127.0.0.2", &["127.0.0.1"]), b_dir.path(), port)
        .await
        .unwrap();
    let _a_servers = a.listen().await.unwrap();
    let b_servers = b.listen().await.unwrap();
    track(&a, "docs");
    b.pull_round().await.unwrap();
    let b_node = b.db().lock().unwrap().node().clone();
    b.db()
        .lock()
        .unwrap()
        .add_replica(
            Timestamp::now(),
            ITEM,
            &b_node,
            Replica::new(AbsPath::new("/srv/b").unwrap()),
        )
        .unwrap();
    a.pull_round().await.unwrap();

    // B's Replica was never read current.
    let status = a.api().status().await;
    assert_eq!(
        status.items[0].replication,
        Replication::Unconfirmed { oldest: None }
    );

    let (a_at, b_at) = (Timestamp::from_millis(2_000), Timestamp::from_millis(1_000));
    confirm_current(&a, "/srv/x", a_at);
    confirm_current(&b, "/srv/b", b_at);
    a.pull_round().await.unwrap();
    let status = a.api().status().await;
    assert_eq!(status.items[0].replication, Replication::Full);

    // Current, but not connected to every sender, isn't enough.
    let a_senders = a.replicas.lock().unwrap().connected[&ITEM].clone();
    a.replicas
        .lock()
        .unwrap()
        .connected
        .insert(ITEM, BTreeSet::new());
    let status = a.api().status().await;
    assert_eq!(
        status.items[0].replication,
        Replication::Unconfirmed { oldest: Some(b_at) }
    );
    a.replicas
        .lock()
        .unwrap()
        .connected
        .insert(ITEM, a_senders.clone());

    // Nor is having been connected to every sender before another was
    // added: B was read before C was.
    let c_node = NodeId::new("n-127.0.0.9").unwrap();
    a.db()
        .lock()
        .unwrap()
        .add_replica(
            Timestamp::now(),
            ITEM,
            &c_node,
            Replica::new(AbsPath::new("/srv/c").unwrap()),
        )
        .unwrap();
    let mut with_c = a_senders;
    with_c.insert(c_node.clone());
    a.replicas.lock().unwrap().connected.insert(ITEM, with_c);
    let status = a.api().status().await;
    let ours = status.items[0]
        .replicas
        .iter()
        .find(|replica| replica.path == "/srv/x")
        .unwrap();
    let theirs = status.items[0]
        .replicas
        .iter()
        .find(|replica| replica.path == "/srv/b")
        .unwrap();
    assert!(ours.every_sender && !theirs.every_sender);
    assert!(matches!(
        status.items[0].replication,
        Replication::Unconfirmed { .. }
    ));
    a.db()
        .lock()
        .unwrap()
        .remove_replica(Timestamp::now(), ITEM, &c_node)
        .unwrap();
    let status = a.api().status().await;
    assert_eq!(status.items[0].replication, Replication::Full);
    let reaches: Vec<_> = status
        .nodes
        .iter()
        .map(|node| (node.node.as_str(), &node.reach))
        .collect();
    assert!(matches!(
        &reaches[..],
        [
            ("n-127.0.0.1", Reach::Here),
            ("n-127.0.0.2", Reach::Live { .. })
        ]
    ));

    // Once B can't be reached, its state is only as last read.
    drop(b_servers);
    a.pull_round().await.unwrap();
    let status = a.api().status().await;
    assert_eq!(
        status.items[0].replication,
        Replication::Unconfirmed { oldest: Some(b_at) }
    );
    assert!(matches!(
        status.nodes[1].reach,
        Reach::Unreachable { read_at: Some(_) }
    ));
    let theirs = &status.items[0].replicas[1];
    assert_eq!(
        (theirs.state.clone(), theirs.confirmed_at),
        (Some(ReplicaState::Current), Some(b_at))
    );
}

#[tokio::test]
async fn status_flags_shared_names_and_names_nodes_to_upgrade() {
    use crate::api::Reach;

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
    let daemon = Daemon::open(tailnet("127.0.0.2", &["127.0.0.3"]), dir.path(), port)
        .await
        .unwrap();
    track(&daemon, "docs");
    {
        let mut db = daemon.db().lock().unwrap();
        let node = db.node().clone();
        db.track(
            Timestamp::now(),
            ItemId::from_u128(2),
            Item::new("docs", Direction::TwoWay),
            &node,
            Replica::new(AbsPath::new("/srv/y").unwrap()),
        )
        .unwrap();
    }
    daemon.pull_round().await.unwrap();
    let status = daemon.api().status().await;
    assert!(status.items.iter().all(|item| item.duplicate_name));
    let newer = status
        .nodes
        .iter()
        .find(|node| node.node.as_str() == "n-127.0.0.3")
        .unwrap();
    assert_eq!(
        newer.reach,
        Reach::Incompatible {
            protocol: protocol::MAJOR + 1
        }
    );
}

#[test]
fn requests_may_name_the_daemon_by_address_or_magicdns_name() {
    let mut me = device("laptop", OWNER, "100.64.0.1");
    me.ips.push("fd7a:115c:a1e0::1".parse().unwrap());
    me.dns_name = "Laptop.tail.ts.net".into();
    let expected: BTreeSet<String> = [
        "100.64.0.1:7130",
        "[fd7a:115c:a1e0::1]:7130",
        "laptop.tail.ts.net:7130",
    ]
    .map(String::from)
    .into();
    assert_eq!(hosts(&me, 7130), expected);

    // Without MagicDNS, only by address.
    me.dns_name.clear();
    assert_eq!(hosts(&me, 7130).len(), 2);
}

#[tokio::test]
async fn a_hung_replica_doesnt_hold_up_listing_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = Daemon::open(tailnet("127.0.0.1", &[]), dir.path(), 1)
        .await
        .unwrap();
    track(&daemon, "docs");
    let config = daemon.db().lock().unwrap().config();
    let outcomes = BTreeMap::from([(ITEM, Outcome::Configured)]);
    daemon.replicas.lock().unwrap().reconciled = Some(Arc::new(Reconciled {
        config,
        outcomes: Ok(outcomes),
    }));
    // As if an earlier listing were stuck on the Replica's filesystem.
    let gate: Arc<tokio::sync::Mutex<()>> = daemon
        .conflict_gates
        .lock()
        .unwrap()
        .entry(PathBuf::from("/srv/x"))
        .or_default()
        .clone();
    let stuck = gate.lock().await;
    let listed = daemon.conflicts().await.remove(&ITEM).unwrap();
    assert!(matches!(listed, Err(ActionError::Busy)), "{listed:?}");

    drop(stuck);
    let listed = daemon.conflicts().await.remove(&ITEM).unwrap();
    assert!(
        !matches!(listed, Err(ActionError::Busy | ActionError::Hung)),
        "{listed:?}"
    );
}

#[test]
fn only_an_absolute_home_is_shared() {
    let home = |var: &str| super::home(Some(var.to_owned())).map(|home| home.as_str().to_owned());
    assert_eq!(home("/home/alice/"), Some("/home/alice".into()));
    assert_eq!(home("/"), Some("/".into()));
    assert_eq!(home(""), None);
    assert_eq!(home("relative/home"), None);
    assert_eq!(home("/home/../root"), None);
    assert_eq!(super::home(None), None);
}
