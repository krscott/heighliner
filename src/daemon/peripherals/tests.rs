use super::*;
use crate::bluetooth::fake::{Fake as FakeBluetooth, paired};
use crate::cli::{self, Client};
use crate::daemon::Daemon;
use crate::hlc::Timestamp;
use crate::model::{AbsPath, Direction, Item, ItemId, Replica};
use crate::tailscale::UserId;
use crate::tailscale::fake::{Fake, device};

const HOME: &str = "98:47:44:A6:21:19";
const WORK: &str = "98:47:44:A5:A9:3B";

/// A daemon at each of `ips`, each with the Bluetooth stack beside it,
/// all serving, and the first having pulled from the others, so it reaches
/// them.
struct Fleet {
    daemons: Vec<Daemon<Fake>>,
    tailnets: Vec<Fake>,
    _servers: Vec<JoinSet<std::convert::Infallible>>,
    _dirs: Vec<tempfile::TempDir>,
    port: u16,
}

impl Fleet {
    async fn start(nodes: Vec<(&str, FakeBluetooth)>) -> Self {
        let port = crate::syncthing::test_instance::free_port();
        let ips: Vec<&str> = nodes.iter().map(|(ip, _)| *ip).collect();
        let mut fleet = Self {
            daemons: Vec::new(),
            tailnets: Vec::new(),
            _servers: Vec::new(),
            _dirs: Vec::new(),
            port,
        };
        for (ip, bluetooth) in nodes {
            let tailnet = Fake::new(device(ip, UserId::new(1), ip));
            for other in ips.iter().filter(|other| **other != ip) {
                tailnet.add_device(device(other, UserId::new(1), other));
            }
            let dir = tempfile::tempdir().unwrap();
            let daemon = Daemon::open(tailnet.clone(), dir.path(), port)
                .await
                .unwrap()
                .with_bluetooth(bluetooth.bluetooth());
            fleet._servers.push(daemon.listen().await.unwrap());
            fleet.daemons.push(daemon);
            fleet.tailnets.push(tailnet);
            fleet._dirs.push(dir);
        }
        // An Item with a Replica on each Node, so that each is named.
        {
            let mut db = fleet.daemons[0].db().lock().unwrap();
            let id = ItemId::from_u128(1);
            let me = db.node().clone();
            let replica = || Replica::new(AbsPath::new("/srv/x").unwrap());
            db.track(
                Timestamp::now(),
                id,
                Item::new("x", Direction::TwoWay),
                &me,
                replica(),
            )
            .unwrap();
            for ip in &ips[1..] {
                let node = NodeId::new(format!("n-{ip}")).unwrap();
                db.add_replica(Timestamp::now(), id, &node, replica())
                    .unwrap();
            }
        }
        fleet.daemons[0].pull_round().await.unwrap();
        fleet
    }

    /// The first daemon's client.
    async fn client(&self) -> Client {
        Client::connect(&self.tailnets[0], self.port, None)
            .await
            .unwrap()
    }
}

fn node(ip: &str) -> Option<String> {
    Some(format!("n-{ip}"))
}

fn refused(result: Result<Done, cli::Error>) -> String {
    match result {
        Err(cli::Error::Refused { message, .. }) => message,
        other => panic!("not refused: {other:?}"),
    }
}

#[tokio::test]
async fn every_reachable_node_lists_its_peripherals() {
    let a = FakeBluetooth::with([paired(HOME, "Q20 Home", true)]);
    let b = FakeBluetooth::with([
        paired(HOME, "Home Q20", false),
        paired(WORK, "Q20 Work", false),
    ]);
    let fleet = Fleet::start(vec![
        ("127.0.0.1", a),
        ("127.0.0.2", b),
        ("127.0.0.3", FakeBluetooth::default()),
    ])
    .await;
    let view = fleet.client().await.peripherals().await.unwrap();
    let names: Vec<(&str, &str)> = view
        .nodes
        .iter()
        .map(|node| (node.node.as_str(), name(node)))
        .collect();
    assert_eq!(
        names,
        [
            ("n-127.0.0.1", "127.0.0.1"),
            ("n-127.0.0.2", "127.0.0.2"),
            ("n-127.0.0.3", "127.0.0.3")
        ]
    );
    assert_eq!(view.nodes[2].bluetooth, BluetoothState::NoAdapter);
    let home = &view.peripherals[0];
    // This Node's name for it.
    assert_eq!(home.alias, "Q20 Home");
    assert_eq!(
        home.aliases()
            .iter()
            .map(|(alias, _)| *alias)
            .collect::<Vec<_>>(),
        ["Q20 Home", "Home Q20"]
    );
    assert_eq!(home.pairable(&view.nodes).count(), 0);
    let work = &view.peripherals[1];
    assert_eq!(work.alias, "Q20 Work");
    let pairable: Vec<&str> = work
        .pairable(&view.nodes)
        .map(|node| node.node.as_str())
        .collect();
    assert_eq!(pairable, ["n-127.0.0.1"]);

    let text = cli::format_peripherals(&view);
    assert!(
        text.contains("Q20 Home (98:47:44:A6:21:19)\n  127.0.0.1: connected\n  127.0.0.2: not connected, named \"Home Q20\"\n"),
        "{text}"
    );
    assert!(text.contains("  127.0.0.1: not paired\n"), "{text}");
    assert!(text.ends_with("Without Bluetooth: 127.0.0.3\n"), "{text}");
}

#[tokio::test]
async fn a_handoff_disconnects_the_others_then_connects() {
    let a = FakeBluetooth::with([paired(HOME, "Q20 Home", true)]);
    let b = FakeBluetooth::with([paired(HOME, "Home Q20", false)]);
    let fleet = Fleet::start(vec![("127.0.0.1", a.clone()), ("127.0.0.2", b.clone())]).await;
    let client = fleet.client().await;
    let handoff = PeripheralAction::Handoff {
        node: node("127.0.0.2"),
    };
    // By any Node's name for it.
    let done = client
        .act_on_peripheral("Home Q20", &handoff)
        .await
        .unwrap();
    assert_eq!(done, Done::default());
    assert_eq!(a.log(), [format!("disconnect {HOME}")]);
    assert_eq!(b.log(), [format!("connect {HOME}")]);
    assert!(!a.get(HOME).unwrap().connected);
    assert!(b.get(HOME).unwrap().connected);

    // Back, by address, to this Node, which is the default.
    let back = PeripheralAction::Handoff { node: None };
    client
        .act_on_peripheral(&HOME.to_lowercase(), &back)
        .await
        .unwrap();
    assert!(a.get(HOME).unwrap().connected);
    assert!(!b.get(HOME).unwrap().connected);
}

#[tokio::test]
async fn a_failed_handoff_says_where_it_may_still_be() {
    let a = FakeBluetooth::with([paired(HOME, "Q20 Home", false)]);
    let b = FakeBluetooth::with([paired(HOME, "Q20 Home", false)]);
    b.make_unreachable(HOME);
    let mut fleet = Fleet::start(vec![
        ("127.0.0.1", a),
        ("127.0.0.2", b),
        ("127.0.0.3", FakeBluetooth::default()),
    ])
    .await;
    // The third Node goes away.
    fleet._servers.pop();
    fleet.daemons.pop();
    fleet.daemons[0].pull_round().await.unwrap();

    let client = fleet.client().await;
    let handoff = PeripheralAction::Handoff {
        node: node("127.0.0.2"),
    };
    let message = refused(client.act_on_peripheral("Q20 Home", &handoff).await);
    assert_eq!(
        message,
        "couldn't connect Q20 Home on 127.0.0.2: BlueZ says br-connection-page-timeout; \
         is the Peripheral on and in range?; \
         it may be connected to a Node that couldn't be asked: 127.0.0.3"
    );
    let view = client.peripherals().await.unwrap();
    assert_eq!(view.nodes[2].bluetooth, BluetoothState::Unreachable);
    let to_it = PeripheralAction::Handoff {
        node: node("127.0.0.3"),
    };
    let message = refused(client.act_on_peripheral("Q20 Home", &to_it).await);
    assert_eq!(message, "127.0.0.3 can't be reached");
}

#[tokio::test]
async fn refused_actions_say_why() {
    let a = FakeBluetooth::with([paired(HOME, "Q20", false)]);
    let b = FakeBluetooth::with([paired(WORK, "Q20", false)]);
    let fleet = Fleet::start(vec![("127.0.0.1", a), ("127.0.0.2", b)]).await;
    let client = fleet.client().await;
    let here = PeripheralAction::Handoff { node: None };
    let message = refused(client.act_on_peripheral("Q20", &here).await);
    assert_eq!(
        message,
        format!("2 Peripherals are named Q20; use one of their addresses: {WORK}, {HOME}")
    );
    let message = refused(client.act_on_peripheral("nothing", &here).await);
    assert_eq!(
        message,
        "no Node that could be asked has a Peripheral named nothing"
    );
    let message = refused(client.act_on_peripheral(WORK, &here).await);
    assert_eq!(
        message,
        "Q20 isn't paired with 127.0.0.1; pair it there first"
    );
    let pair = PeripheralAction::Pair {
        node: node("127.0.0.2"),
    };
    let message = refused(client.act_on_peripheral(WORK, &pair).await);
    assert_eq!(message, "Q20 is already paired with 127.0.0.2");
    let forget = PeripheralAction::Forget { node: None };
    let message = refused(client.act_on_peripheral(WORK, &forget).await);
    assert_eq!(message, "Q20 isn't paired with 127.0.0.1");
}

#[tokio::test]
async fn pairing_scans_then_takes_the_name_the_others_agree_on() {
    let a = FakeBluetooth::with([]);
    let b = FakeBluetooth::with([paired(WORK, "Work Q20", true)]);
    let c = FakeBluetooth::with([paired(WORK, "Work Q20", false)]);
    let fleet = Fleet::start(vec![
        ("127.0.0.1", a.clone()),
        ("127.0.0.2", b.clone()),
        ("127.0.0.3", c),
    ])
    .await;
    let client = fleet.client().await;
    let pair = PeripheralAction::Pair { node: None };
    // Not in pairing mode yet.
    let message = refused(client.act_on_peripheral("Work Q20", &pair).await);
    assert_eq!(
        message,
        "couldn't pair Work Q20 with 127.0.0.1: it didn't see the Peripheral within 30 s; \
         is the Peripheral in pairing mode?"
    );
    a.make_pairable(WORK, "soundcore Life Q20");
    client.act_on_peripheral("Work Q20", &pair).await.unwrap();
    let here = a.get(WORK).unwrap();
    assert_eq!(here.alias, "Work Q20");
    assert!(here.connected);
    assert!(!b.get(WORK).unwrap().connected);
}

#[tokio::test]
async fn pairing_keeps_its_own_name_when_the_others_disagree() {
    let a = FakeBluetooth::with([]);
    let b = FakeBluetooth::with([paired(WORK, "Work Q20", false)]);
    let c = FakeBluetooth::with([paired(WORK, "Q20 Work", false)]);
    let fleet = Fleet::start(vec![
        ("127.0.0.1", a.clone()),
        ("127.0.0.2", b),
        ("127.0.0.3", c),
    ])
    .await;
    a.make_pairable(WORK, "soundcore Life Q20");
    let client = fleet.client().await;
    let pair = PeripheralAction::Pair { node: None };
    client.act_on_peripheral(WORK, &pair).await.unwrap();
    assert_eq!(a.get(WORK).unwrap().alias, "soundcore Life Q20");
    assert_eq!(a.log(), [format!("pair {WORK}"), format!("connect {WORK}")]);
}

#[tokio::test]
async fn a_pairing_that_cant_connect_still_pairs() {
    let a = FakeBluetooth::with([]);
    let b = FakeBluetooth::with([paired(WORK, "Work Q20", false)]);
    let fleet = Fleet::start(vec![("127.0.0.1", a.clone()), ("127.0.0.2", b)]).await;
    a.make_pairable(WORK, "soundcore Life Q20");
    a.make_unreachable(WORK);
    let client = fleet.client().await;
    let pair = PeripheralAction::Pair { node: None };
    let done = client.act_on_peripheral(WORK, &pair).await.unwrap();
    assert_eq!(
        done.notes,
        [
            "paired it, but couldn't connect it on 127.0.0.1: BlueZ says br-connection-page-timeout; \
          is the Peripheral on and in range?"
        ]
    );
    assert_eq!(a.get(WORK).unwrap().alias, "Work Q20");
}

#[tokio::test]
async fn naming_gives_every_node_one_of_its_names() {
    let a = FakeBluetooth::with([paired(HOME, "Q20 Home", false)]);
    let b = FakeBluetooth::with([paired(HOME, "Home Q20", false)]);
    let c = FakeBluetooth::with([paired(HOME, "Home Q20", false)]);
    let fleet = Fleet::start(vec![
        ("127.0.0.1", a.clone()),
        ("127.0.0.2", b.clone()),
        ("127.0.0.3", c.clone()),
    ])
    .await;
    let client = fleet.client().await;
    let made_up = PeripheralAction::Name {
        alias: "Cans".into(),
    };
    let message = refused(client.act_on_peripheral(HOME, &made_up).await);
    assert_eq!(
        message,
        "none of its Nodes names it Cans; pick one of: Q20 Home, Home Q20"
    );
    let name = PeripheralAction::Name {
        alias: "Q20 Home".into(),
    };
    client.act_on_peripheral(HOME, &name).await.unwrap();
    for bluetooth in [&a, &b, &c] {
        assert_eq!(bluetooth.get(HOME).unwrap().alias, "Q20 Home");
    }
    // Only the Nodes that named it otherwise were asked.
    assert_eq!(a.log(), Vec::<String>::new());
    assert_eq!(b.log(), [format!("name \"Q20 Home\" {HOME}")]);
}

#[tokio::test]
async fn disconnecting_and_forgetting_act_on_one_node() {
    let a = FakeBluetooth::with([paired(HOME, "Q20 Home", false)]);
    let b = FakeBluetooth::with([paired(HOME, "Q20 Home", true)]);
    let fleet = Fleet::start(vec![("127.0.0.1", a.clone()), ("127.0.0.2", b.clone())]).await;
    let client = fleet.client().await;
    let disconnect = PeripheralAction::Disconnect {
        node: node("127.0.0.2"),
    };
    client.act_on_peripheral(HOME, &disconnect).await.unwrap();
    assert!(!b.get(HOME).unwrap().connected);
    let forget = PeripheralAction::Forget {
        node: Some("127.0.0.2".into()),
    };
    client.act_on_peripheral(HOME, &forget).await.unwrap();
    assert!(b.get(HOME).is_none());
    assert!(a.get(HOME).is_some());
}

#[tokio::test]
async fn nodes_take_commands_only_as_json() {
    let a = FakeBluetooth::with([paired(HOME, "Q20 Home", false)]);
    let fleet = Fleet::start(vec![("127.0.0.1", a.clone())]).await;
    let send = |content_type: &'static str, body: &'static str| async move {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", fleet.port))
            .await
            .unwrap();
        let address: Address = HOME.parse().unwrap();
        let request = hyper::Request::post(protocol::peripheral_path(&address))
            .header(hyper::header::CONTENT_TYPE, content_type)
            .body(http_body_util::Full::new(hyper::body::Bytes::from(body)))
            .unwrap();
        crate::http::send(stream, &format!("127.0.0.1:{}", fleet.port), request)
            .await
            .unwrap()
            .0
    };
    // As a page on another site could post it.
    let status = send("application/x-www-form-urlencoded", "command=connect").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(a.log().is_empty());
    let status = send("application/json", r#"{"command":"connect"}"#).await;
    assert_eq!(status, StatusCode::OK);
    assert!(a.get(HOME).unwrap().connected);
}

#[test]
fn an_older_node_answers_not_found() {
    let addr: SocketAddr = "127.0.0.2:7130".parse().unwrap();
    let older = protocol::Error::Status {
        addr,
        status: StatusCode::NOT_FOUND,
        body: String::new(),
    };
    assert_eq!(answered(Err(older)).0, BluetoothState::Older);
    assert_eq!(
        BluetoothState::Older.problem().unwrap(),
        "runs an older heighliner that can't manage Peripherals; upgrade it"
    );
}
