//! Tests against real Syncthing instances on loopback.

use super::test_instance::{Syncthing, free_port};
use super::*;
use crate::config::ConfigDb;
use crate::hlc::Timestamp;
use crate::model::{Direction, ExcludePattern, Item, Replica};
use crate::record::Store;
use crate::tailscale::UserId;
use crate::tailscale::fake::device;

const OWNER: UserId = UserId::new(1);
const ITEM: ItemId = ItemId::from_u128(0xabc);
const OTHER: ItemId = ItemId::from_u128(0xdef);
const SOME_ID: &str = "MFZWI3D-BONSGYC-YLTMRWG-C43ENR5-QXGZDMM-FZWI3DP-BONSGYY-LTMRWAD";

fn node(name: &str) -> NodeId {
    device(name, OWNER, "127.0.0.1").id
}

fn alone() -> Tailnet {
    Tailnet::new(device("a", OWNER, "127.0.0.1"), vec![]).unwrap()
}

fn item(direction: Direction) -> Item {
    let mut item = Item::new("docs", direction);
    item.excludes = vec![ExcludePattern::new("*.tmp").unwrap()];
    item
}

fn path(path: impl AsRef<Path>) -> AbsPath {
    AbsPath::new(path.as_ref().to_str().unwrap()).unwrap()
}

/// Tracks the two-way Item `id` with a Replica at `at` on `on`.
fn tracked_as(id: ItemId, on: &str, at: &AbsPath) -> ConfigDb {
    let mut db = ConfigDb::new(node(on), Store::default());
    db.track(
        Timestamp::now(),
        id,
        item(Direction::TwoWay),
        &node(on),
        Replica::new(at.clone()),
    )
    .unwrap();
    db
}

fn tracked(on: &str, at: &AbsPath) -> ConfigDb {
    tracked_as(ITEM, on, at)
}

/// `ITEM`, tracked on another Node, with a Replica at `at` on this one.
fn added(at: &AbsPath, adopt: bool) -> ConfigDb {
    let mut db = tracked("x", &path("/srv/docs"));
    let mut replica = Replica::new(at.clone());
    replica.adopt = adopt;
    db.add_replica(Timestamp::now(), ITEM, &node("a"), replica)
        .unwrap();
    db
}

fn file(at: &AbsPath, name: &str) -> PathBuf {
    Path::new(at.as_str()).join(name)
}

async fn scan(syncthing: &Syncthing) {
    syncthing
        .driver
        .client()
        .call(Method::POST, &format!("/rest/db/scan?folder={ITEM}"), None)
        .await
        .unwrap();
}

/// Waits up to 60s for `done`.
async fn eventually(mut done: impl FnMut() -> bool) -> bool {
    for _ in 0..600 {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test]
async fn reconcile_configures_a_folder_once() {
    let port = free_port();
    let syncthing = Syncthing::start("127.0.0.1", port).await;
    let at = syncthing.dir("docs");
    // Tracking adopts the files already there.
    fs::create_dir(at.as_str()).unwrap();
    fs::write(file(&at, "existing"), "mine").unwrap();
    let db = tracked("a", &at);

    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Configured);
    assert!(report.changed);

    let config = syncthing.config().await;
    let options = &config["options"];
    assert_eq!(options["globalAnnounceEnabled"], false);
    assert_eq!(options["relaysEnabled"], false);
    assert_eq!(options["urAccepted"], -1);
    let listen = json!([format!("tcp://127.0.0.1:{port}")]);
    assert_eq!(options["listenAddresses"], listen);
    assert_eq!(config["devices"][0]["addresses"], listen);
    let folder = syncthing.folder(ITEM).await.unwrap();
    assert_eq!(folder["type"], "sendreceive");
    assert_eq!(folder["path"], at.as_str());
    assert_eq!(folder["versioning"]["type"], "simple");
    assert_eq!(folder["versioning"]["params"]["cleanoutDays"], "30");
    let (ignores, error) = syncthing.driver.client().ignores(ITEM).await.unwrap();
    assert_eq!((ignores, error), (vec!["*.tmp".to_owned()], None));

    // Nothing changes the second time.
    let again = syncthing.reconcile(&db.config(), &alone()).await;
    assert!(!again.changed);
}

#[tokio::test]
async fn new_replicas_need_an_empty_path_or_adopt() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("existing");
    fs::create_dir(at.as_str()).unwrap();
    fs::write(file(&at, "file"), "mine").unwrap();

    let report = syncthing
        .reconcile(&added(&at, false).config(), &alone())
        .await;
    assert_eq!(report.replicas[&ITEM], Outcome::NotEmpty);
    assert!(syncthing.folder(ITEM).await.is_none());

    let report = syncthing
        .reconcile(&added(&at, true).config(), &alone())
        .await;
    assert_eq!(report.replicas[&ITEM], Outcome::Configured);
    assert!(syncthing.folder(ITEM).await.is_some());
}

#[test]
fn preparing_a_path_twice_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let at = path(dir.path().join("new"));
    let excludes = ["*.tmp".to_owned()];
    prepare(&at, false, &excludes).unwrap();
    prepare(&at, false, &excludes).unwrap();
}

#[test]
fn preparing_refuses_risky_paths() {
    let dir = tempfile::tempdir().unwrap();
    // A missing parent, as on an unmounted disk.
    let unmounted = path(dir.path().join("unmounted/photos"));
    assert_eq!(prepare(&unmounted, true, &[]), Err(Outcome::Missing));
    // A user's own .stignore.
    let own = path(dir.path().join("own"));
    fs::create_dir(own.as_str()).unwrap();
    fs::write(file(&own, IGNORE_FILE), "precious\n").unwrap();
    assert_eq!(prepare(&own, true, &[]), Err(Outcome::OwnIgnoreFile));
    assert_eq!(
        fs::read_to_string(file(&own, IGNORE_FILE)).unwrap(),
        "precious\n"
    );
}

#[tokio::test]
async fn an_overlap_pauses_the_folder_instead_of_removing_it() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    let db = added(&at, false);
    syncthing.reconcile(&db.config(), &alone()).await;
    fs::write(file(&at, "synced"), "data").unwrap();
    assert!(eventually(|| file(&at, MARKER).exists()).await);

    // Another Node tracks an Item nested inside it, so both overlap.
    let nested = tracked_as(OTHER, "a", &path(file(&at, "nested")));
    let mut overlapping = db.store().clone();
    overlapping.merge(nested.store(), |_| true);
    let report = syncthing
        .reconcile(&Config::new(&overlapping), &alone())
        .await;
    assert_eq!(report.replicas[&ITEM], Outcome::Overlaps);
    assert_eq!(syncthing.folder(ITEM).await.unwrap()["paused"], true);
    assert!(file(&at, MARKER).exists());

    // Once the overlap is gone, it runs again.
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Configured);
    assert_eq!(syncthing.folder(ITEM).await.unwrap()["paused"], false);
}

#[tokio::test]
async fn an_unreadable_replica_pauses_its_folder() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    let db = tracked("a", &at);
    syncthing.reconcile(&db.config(), &alone()).await;

    // A newer heighliner rewrites the Item in a schema this one can't read.
    let mut newer = db.store().clone();
    newer.insert(
        crate::record::RecordKey {
            kind: crate::config::ITEM_KIND.to_owned(),
            id: ITEM.to_string(),
        },
        crate::hlc::Hlc {
            wall: Timestamp::from_millis(u64::MAX / 2),
            counter: 0,
            node: node("z"),
        },
        crate::record::Content::Live {
            schema: crate::config::SCHEMA + 1,
            fields: serde_json::Map::new(),
        },
    );
    let report = syncthing.reconcile(&Config::new(&newer), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Unreadable);
    assert_eq!(syncthing.folder(ITEM).await.unwrap()["paused"], true);
}

#[tokio::test]
async fn untracking_keeps_the_files() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    let mut db = tracked("a", &at);
    syncthing.reconcile(&db.config(), &alone()).await;
    fs::write(file(&at, "kept"), "data").unwrap();

    db.untrack(Timestamp::now(), ITEM).unwrap();
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert!(report.changed);
    assert!(syncthing.folder(ITEM).await.is_none());
    assert_eq!(fs::read_to_string(file(&at, "kept")).unwrap(), "data");
}

#[tokio::test]
async fn a_missing_path_is_not_recreated() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    let mut db = tracked("a", &at);
    syncthing.reconcile(&db.config(), &alone()).await;
    assert!(eventually(|| file(&at, MARKER).exists()).await);

    // The disk is unmounted, and the excludes change meanwhile.
    fs::remove_dir_all(at.as_str()).unwrap();
    db.update_item(Timestamp::now(), ITEM, |item| {
        item.excludes = vec![ExcludePattern::new("*.bak").unwrap()]
    })
    .unwrap();
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Missing);
    assert!(!Path::new(at.as_str()).exists());
}

#[tokio::test]
async fn defaults_cant_weaken_the_folders() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    syncthing
        .driver
        .client()
        .call(
            Method::PATCH,
            "/rest/config/defaults/folder",
            Some(&json!({ "markerName": ".", "ignoreDelete": true })),
        )
        .await
        .unwrap();
    let db = tracked("a", &syncthing.dir("docs"));
    syncthing.reconcile(&db.config(), &alone()).await;
    let folder = syncthing.folder(ITEM).await.unwrap();
    assert_eq!(folder["markerName"], ".stfolder");
    assert_eq!(folder["ignoreDelete"], false);
}

#[tokio::test]
async fn only_the_owners_devices_with_addresses_are_configured() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let mut db = tracked("a", &syncthing.dir("docs"));
    for (name, at) in [("gone", "/srv/gone"), ("tagged", "/srv/tagged")] {
        let mut other = ConfigDb::new(node(name), Store::default());
        other
            .set_device_id(Timestamp::now(), DeviceId::new(SOME_ID).unwrap())
            .unwrap();
        db.merge(other.store(), Timestamp::now());
        db.add_replica(Timestamp::now(), ITEM, &node(name), Replica::new(path(at)))
            .unwrap();
    }
    // "gone" isn't on the tailnet; "tagged" is, but isn't the owner's.
    let mut tagged = device("tagged", OWNER, "127.0.0.5");
    tagged.tagged = true;
    let tailnet = Tailnet::new(device("a", OWNER, "127.0.0.1"), vec![tagged]).unwrap();
    syncthing.reconcile(&db.config(), &tailnet).await;
    let config = syncthing.config().await;
    assert_eq!(config["devices"].as_array().unwrap().len(), 1);
    let folder = syncthing.folder(ITEM).await.unwrap();
    assert_eq!(folder["devices"].as_array().unwrap().len(), 1);
}

/// Two Nodes on loopback sharing `ITEM`, with A tracking it and B's Replica
/// optionally a mirror, and their records merged.
struct Pair {
    a: Syncthing,
    b: Syncthing,
    a_path: AbsPath,
    b_path: AbsPath,
    db: ConfigDb,
}

impl Pair {
    async fn start(direction: Direction, b_mirror: bool) -> Self {
        let port = free_port();
        let a = Syncthing::start("127.0.0.1", port).await;
        let b = Syncthing::start("127.0.0.2", port).await;
        let (a_path, b_path) = (a.dir("a"), b.dir("b"));
        let mut db = ConfigDb::new(node("a"), Store::default());
        db.track(
            Timestamp::now(),
            ITEM,
            item(direction),
            &node("a"),
            Replica::new(a_path.clone()),
        )
        .unwrap();
        let mut replica = Replica::new(b_path.clone());
        replica.mirror = b_mirror;
        db.add_replica(Timestamp::now(), ITEM, &node("b"), replica)
            .unwrap();
        db.set_device_id(
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
        db.merge(b_db.store(), Timestamp::now());
        let pair = Self {
            a,
            b,
            a_path,
            b_path,
            db,
        };
        pair.reconcile().await;
        pair
    }

    async fn reconcile(&self) -> (Report, Report) {
        let config = self.db.config();
        let (a, b) = (
            device("a", OWNER, "127.0.0.1"),
            device("b", OWNER, "127.0.0.2"),
        );
        let a_tailnet = Tailnet::new(a.clone(), vec![b.clone()]).unwrap();
        let b_tailnet = Tailnet::new(b, vec![a]).unwrap();
        (
            self.a.reconcile(&config, &a_tailnet).await,
            self.b.reconcile(&config, &b_tailnet).await,
        )
    }
}

#[tokio::test]
async fn two_nodes_sync_one_way() {
    let pair = Pair::start(Direction::OneWay { source: node("a") }, false).await;
    assert_eq!(pair.a.folder(ITEM).await.unwrap()["type"], "sendonly");
    assert_eq!(pair.b.folder(ITEM).await.unwrap()["type"], "receiveonly");

    fs::write(file(&pair.a_path, "hello.txt"), "hi").unwrap();
    fs::write(file(&pair.a_path, "skip.tmp"), "excluded").unwrap();
    scan(&pair.a).await;
    let synced = file(&pair.b_path, "hello.txt");
    assert!(eventually(|| fs::read_to_string(&synced).is_ok_and(|text| text == "hi")).await);
    assert!(!file(&pair.b_path, "skip.tmp").exists());
}

#[tokio::test]
async fn a_mirror_with_local_changes_doesnt_start_sending() {
    let mut pair = Pair::start(Direction::TwoWay, true).await;
    fs::write(file(&pair.a_path, "one"), "1").unwrap();
    scan(&pair.a).await;
    let synced = file(&pair.b_path, "one");
    assert!(eventually(|| synced.exists()).await);

    // The mirror's copy is deleted locally, then it is made a peer.
    fs::remove_file(&synced).unwrap();
    scan(&pair.b).await;
    let client = pair.b.driver.client().clone();
    let mut changed = false;
    for _ in 0..600 {
        if client.local_changes(&ITEM.to_string()).await.unwrap() > 0 {
            changed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(changed, "Syncthing saw the local change");
    pair.db
        .update_replica(Timestamp::now(), ITEM, &node("b"), |replica| {
            replica.mirror = false
        })
        .unwrap();
    let (_, b_report) = pair.reconcile().await;
    assert_eq!(b_report.replicas[&ITEM], Outcome::LocalChanges);
    assert_eq!(pair.b.folder(ITEM).await.unwrap()["type"], "receiveonly");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(file(&pair.a_path, "one").exists());
}

#[tokio::test]
async fn moving_a_replica_starts_afresh_at_the_new_path() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let (first, second) = (syncthing.dir("first"), syncthing.dir("second"));
    let mut db = added(&first, false);
    syncthing.reconcile(&db.config(), &alone()).await;
    assert!(eventually(|| file(&first, MARKER).exists()).await);

    // Removed and added again elsewhere, both seen in one round.
    db.remove_replica(Timestamp::now(), ITEM, &node("a"))
        .unwrap();
    db.add_replica(
        Timestamp::now(),
        ITEM,
        &node("a"),
        Replica::new(second.clone()),
    )
    .unwrap();
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Configured);
    assert_eq!(
        syncthing.folder(ITEM).await.unwrap()["path"],
        second.as_str()
    );
    assert!(eventually(|| file(&second, MARKER).exists()).await);
}

#[tokio::test]
async fn a_paused_mirror_isnt_promoted_until_scanned() {
    let mut pair = Pair::start(Direction::TwoWay, true).await;
    fs::write(file(&pair.a_path, "one"), "1").unwrap();
    scan(&pair.a).await;
    let synced = file(&pair.b_path, "one");
    assert!(eventually(|| synced.exists()).await);

    // The mirror is paused, its copy deleted, and then it is made a peer
    // and resumed at once.
    pair.db
        .update_replica(Timestamp::now(), ITEM, &node("b"), |replica| {
            replica.paused = true
        })
        .unwrap();
    pair.reconcile().await;
    fs::remove_file(&synced).unwrap();
    pair.db
        .update_replica(Timestamp::now(), ITEM, &node("b"), |replica| {
            replica.paused = false;
            replica.mirror = false;
        })
        .unwrap();
    pair.reconcile().await;
    assert_eq!(pair.b.folder(ITEM).await.unwrap()["type"], "receiveonly");
    // The next round scans before deciding, and finds the deletion.
    let (_, b_report) = pair.reconcile().await;
    assert_eq!(b_report.replicas[&ITEM], Outcome::LocalChanges);
    assert_eq!(pair.b.folder(ITEM).await.unwrap()["type"], "receiveonly");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(file(&pair.a_path, "one").exists());
}

#[tokio::test]
async fn an_unavailable_path_pauses_its_folder() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    let db = tracked("a", &at);
    syncthing.reconcile(&db.config(), &alone()).await;
    assert!(eventually(|| file(&at, MARKER).exists()).await);

    // The disk goes away, with nothing else changing.
    fs::rename(at.as_str(), syncthing.dir("away").as_str()).unwrap();
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Missing);
    assert_eq!(syncthing.folder(ITEM).await.unwrap()["paused"], true);
    assert!(!Path::new(at.as_str()).exists());

    // An empty directory where the disk was mounted stays missing too, round
    // after round.
    fs::create_dir(at.as_str()).unwrap();
    for _ in 0..3 {
        let report = syncthing.reconcile(&db.config(), &alone()).await;
        assert_eq!(report.replicas[&ITEM], Outcome::Missing);
        assert_eq!(syncthing.folder(ITEM).await.unwrap()["paused"], true);
    }
    fs::remove_dir(at.as_str()).unwrap();

    // When it comes back, the folder runs again.
    fs::rename(syncthing.dir("away").as_str(), at.as_str()).unwrap();
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Configured);
    assert_eq!(syncthing.folder(ITEM).await.unwrap()["paused"], false);
}

#[tokio::test]
async fn symlinked_aliases_count_as_overlapping() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    fs::create_dir(at.as_str()).unwrap();
    let alias = syncthing.dir("alias");
    std::os::unix::fs::symlink(at.as_str(), alias.as_str()).unwrap();
    let mut store = tracked("a", &at).store().clone();
    store.merge(tracked_as(OTHER, "a", &alias).store(), |_| true);
    let report = syncthing.reconcile(&Config::new(&store), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Overlaps);
    assert_eq!(report.replicas[&OTHER], Outcome::Overlaps);
}

#[tokio::test]
async fn unparseable_excludes_are_reported() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let mut db = tracked("a", &syncthing.dir("docs"));
    syncthing.reconcile(&db.config(), &alone()).await;
    db.update_item(Timestamp::now(), ITEM, |item| {
        item.excludes = vec![ExcludePattern::new("[").unwrap()]
    })
    .unwrap();
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert!(
        matches!(&report.replicas[&ITEM], Outcome::Failed(error) if error.contains("excludes")),
        "{:?}",
        report.replicas[&ITEM]
    );
}

#[tokio::test]
async fn unknown_items_keep_their_folders_paused() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    let db = added(&at, false);
    syncthing.reconcile(&db.config(), &alone()).await;
    fs::write(file(&at, "synced"), "data").unwrap();

    // A first round before any records have arrived.
    let report = syncthing.reconcile(&Config::default(), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Unknown);
    assert_eq!(syncthing.folder(ITEM).await.unwrap()["paused"], true);

    // Once they arrive, it runs again rather than being refused.
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Configured);
}

#[tokio::test]
async fn a_new_replica_through_a_symlink_overlaps_at_once() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    fs::create_dir(at.as_str()).unwrap();
    let link = syncthing.dir("link");
    std::os::unix::fs::symlink(at.as_str(), link.as_str()).unwrap();
    let mut store = tracked("a", &at).store().clone();
    // A new Replica inside the other, reached through the symlink.
    store.merge(tracked_as(OTHER, "a", &path(file(&link, "inner"))).store(), |_| true);
    let report = syncthing.reconcile(&Config::new(&store), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Overlaps);
    assert_eq!(report.replicas[&OTHER], Outcome::Overlaps);
}
