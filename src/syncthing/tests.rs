//! Tests against real Syncthing instances on loopback.

use super::test_instance::{Syncthing, free_port};
use super::*;
use crate::config::ConfigDb;
use crate::hlc::Timestamp;
use crate::model::{Direction, ExcludePattern, Item, Replica};
use crate::record::Store;
use crate::tailscale::fake::device;
use crate::tailscale::{Device, UserId};

const OWNER: UserId = UserId::new(1);
const ITEM: ItemId = ItemId::from_u128(0xabc);

fn node(name: &str) -> NodeId {
    device(name, OWNER, "127.0.0.1").id
}

fn tailnet(me: Device, others: Vec<Device>) -> Tailnet {
    Tailnet::new(me, others).unwrap()
}

/// Tracks a two-way Item with a Replica at `path` on `on`.
fn tracked(on: &str, path: &AbsPath, replica: impl FnOnce(&mut Replica)) -> ConfigDb {
    let mut db = ConfigDb::new(node(on), Store::default());
    let mut new = Replica::new(path.clone());
    replica(&mut new);
    let mut item = Item::new("docs", Direction::TwoWay);
    item.excludes = vec![ExcludePattern::new("*.tmp").unwrap()];
    db.track(Timestamp::now(), ITEM, item, &node(on), new)
        .unwrap();
    db
}

#[tokio::test]
async fn reconcile_configures_a_folder_once() {
    let port = free_port();
    let syncthing = Syncthing::start("127.0.0.1", port).await;
    let path = syncthing.dir("docs");
    let db = tracked("a", &path, |_| {});
    let tailnet = tailnet(device("a", OWNER, "127.0.0.1"), vec![]);

    let report = syncthing
        .driver
        .reconcile(&db.config(), &tailnet)
        .await
        .unwrap();
    assert_eq!(report.replicas[&ITEM], Outcome::Configured);
    assert!(report.changed);

    let config = syncthing.config().await;
    let options = &config["options"];
    assert_eq!(options["globalAnnounceEnabled"], false);
    assert_eq!(options["relaysEnabled"], false);
    assert_eq!(options["urAccepted"], -1);
    assert_eq!(
        options["listenAddresses"],
        json!([format!("tcp://127.0.0.1:{port}")])
    );
    let folder = syncthing.folder(ITEM).await.unwrap();
    assert_eq!(folder["type"], "sendreceive");
    assert_eq!(folder["path"], path.as_str());
    assert_eq!(folder["versioning"]["type"], "trashcan");
    assert_eq!(folder["versioning"]["params"]["cleanoutDays"], "30");
    assert_eq!(
        syncthing.driver.client().ignores(ITEM).await.unwrap(),
        ["*.tmp"]
    );

    // Nothing changes the second time.
    let again = syncthing
        .driver
        .reconcile(&db.config(), &tailnet)
        .await
        .unwrap();
    assert!(!again.changed);
}

#[tokio::test]
async fn paths_with_files_need_adopt() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let path = syncthing.dir("existing");
    fs::create_dir_all(path.as_str()).unwrap();
    fs::write(Path::new(path.as_str()).join("file"), "mine").unwrap();
    let tailnet = tailnet(device("a", OWNER, "127.0.0.1"), vec![]);

    let refused = tracked("a", &path, |_| {});
    let report = syncthing
        .driver
        .reconcile(&refused.config(), &tailnet)
        .await
        .unwrap();
    assert_eq!(report.replicas[&ITEM], Outcome::NotEmpty);
    assert!(syncthing.folder(ITEM).await.is_none());

    let adopting = tracked("a", &path, |replica| replica.adopt = true);
    let report = syncthing
        .driver
        .reconcile(&adopting.config(), &tailnet)
        .await
        .unwrap();
    assert_eq!(report.replicas[&ITEM], Outcome::Configured);
    assert!(syncthing.folder(ITEM).await.is_some());
}

#[tokio::test]
async fn untracking_keeps_the_files() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let path = syncthing.dir("docs");
    let mut db = tracked("a", &path, |_| {});
    let tailnet = tailnet(device("a", OWNER, "127.0.0.1"), vec![]);
    syncthing
        .driver
        .reconcile(&db.config(), &tailnet)
        .await
        .unwrap();
    let file = Path::new(path.as_str()).join("kept");
    fs::write(&file, "data").unwrap();

    db.untrack(Timestamp::now(), ITEM).unwrap();
    let report = syncthing
        .driver
        .reconcile(&db.config(), &tailnet)
        .await
        .unwrap();
    assert!(report.changed);
    assert!(syncthing.folder(ITEM).await.is_none());
    assert_eq!(fs::read_to_string(&file).unwrap(), "data");
}

#[tokio::test]
async fn devices_without_known_addresses_are_left_out() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let mut db = tracked("a", &syncthing.dir("docs"), |_| {});
    let mut other = ConfigDb::new(node("gone"), Store::default());
    other
        .set_device_id(
            Timestamp::now(),
            DeviceId::new("MFZWI3D-BONSGYC-YLTMRWG-C43ENR5-QXGZDMM-FZWI3DP-BONSGYY-LTMRWAD")
                .unwrap(),
        )
        .unwrap();
    db.merge(other.store(), Timestamp::now());
    db.add_replica(
        Timestamp::now(),
        ITEM,
        &node("gone"),
        Replica::new(AbsPath::new("/srv/docs").unwrap()),
    )
    .unwrap();
    // "gone" has a device ID, but isn't on the tailnet.
    let tailnet = tailnet(device("a", OWNER, "127.0.0.1"), vec![]);
    syncthing
        .driver
        .reconcile(&db.config(), &tailnet)
        .await
        .unwrap();
    let config = syncthing.config().await;
    assert_eq!(config["devices"].as_array().unwrap().len(), 1);
    assert!(
        config["devices"]
            .as_array()
            .unwrap()
            .iter()
            .all(|device| device["addresses"] != json!(["dynamic"]))
    );
}

#[tokio::test]
async fn two_nodes_sync_one_way() {
    let port = free_port();
    let a = Syncthing::start("127.0.0.1", port).await;
    let b = Syncthing::start("127.0.0.2", port).await;
    let (a_path, b_path) = (a.dir("source"), b.dir("mirror"));

    let mut a_db = ConfigDb::new(node("a"), Store::default());
    let mut item = Item::new("docs", Direction::OneWay { source: node("a") });
    item.excludes = vec![ExcludePattern::new("*.tmp").unwrap()];
    a_db.track(
        Timestamp::now(),
        ITEM,
        item,
        &node("a"),
        Replica::new(a_path.clone()),
    )
    .unwrap();
    a_db.add_replica(
        Timestamp::now(),
        ITEM,
        &node("b"),
        Replica::new(b_path.clone()),
    )
    .unwrap();
    a_db.set_device_id(
        Timestamp::now(),
        a.driver.client().device_id().await.unwrap(),
    )
    .unwrap();
    let mut b_db = ConfigDb::new(node("b"), Store::default());
    b_db.set_device_id(
        Timestamp::now(),
        b.driver.client().device_id().await.unwrap(),
    )
    .unwrap();
    a_db.merge(b_db.store(), Timestamp::now());
    let config = a_db.config();

    let (a_dev, b_dev) = (
        device("a", OWNER, "127.0.0.1"),
        device("b", OWNER, "127.0.0.2"),
    );
    a.driver
        .reconcile(&config, &tailnet(a_dev.clone(), vec![b_dev.clone()]))
        .await
        .unwrap();
    b.driver
        .reconcile(&config, &tailnet(b_dev, vec![a_dev]))
        .await
        .unwrap();
    assert_eq!(b.folder(ITEM).await.unwrap()["type"], "receiveonly");
    assert_eq!(a.folder(ITEM).await.unwrap()["type"], "sendonly");

    fs::write(Path::new(a_path.as_str()).join("hello.txt"), "hi").unwrap();
    fs::write(Path::new(a_path.as_str()).join("skip.tmp"), "excluded").unwrap();
    a.driver
        .client()
        .call(Method::POST, &format!("/rest/db/scan?folder={ITEM}"), None)
        .await
        .unwrap();
    let synced = Path::new(b_path.as_str()).join("hello.txt");
    for _ in 0..600 {
        if fs::read_to_string(&synced).is_ok_and(|text| text == "hi") {
            assert!(!Path::new(b_path.as_str()).join("skip.tmp").exists());
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the file didn't sync");
}
