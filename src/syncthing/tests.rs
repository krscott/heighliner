//! Tests against real Syncthing instances on loopback.

use super::test_instance::{Syncthing, free_port};
use super::*;
use crate::config::ConfigDb;
use crate::hlc::Timestamp;
use crate::model::{Direction, ExcludePattern, Item, Replica, ReplicaState};
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
    assert_eq!(folder["maxConflicts"], -1);
    let (ignores, error) = syncthing.driver.client().ignores(ITEM).await.unwrap();
    assert_eq!(
        (ignores, error),
        (ignore_lines(&["*.tmp".to_owned()]), None)
    );

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
    // One that holds exactly the excludes is the Replica's own.
    fs::write(file(&own, IGNORE_FILE), "precious\n").unwrap();
    prepare(&own, true, &["precious".to_owned()]).unwrap();
    // A symlinked one isn't followed.
    let linked = path(dir.path().join("linked"));
    fs::create_dir(linked.as_str()).unwrap();
    let outside = dir.path().join("outside");
    std::os::unix::fs::symlink(&outside, file(&linked, IGNORE_FILE)).unwrap();
    assert_eq!(prepare(&linked, true, &[]), Err(Outcome::OwnIgnoreFile));
    assert!(!outside.exists());
    // A hard-linked one is replaced, not written through.
    let hard = path(dir.path().join("hard"));
    fs::create_dir(hard.as_str()).unwrap();
    fs::write(&outside, "precious\n").unwrap();
    fs::hard_link(&outside, file(&hard, IGNORE_FILE)).unwrap();
    prepare(&hard, true, &["precious".to_owned()]).unwrap();
    assert_eq!(fs::read_to_string(&outside).unwrap(), "precious\n");
}

#[test]
fn preparing_again_takes_the_new_excludes() {
    // As when a round prepared a path but Syncthing never added its
    // folder, and the excludes changed before the next.
    let dir = tempfile::tempdir().unwrap();
    let at = path(dir.path().join("new"));
    prepare(&at, false, &["*.tmp".to_owned()]).unwrap();
    prepare(&at, false, &["*.bak".to_owned()]).unwrap();
    assert_eq!(
        fs::read_to_string(file(&at, IGNORE_FILE)).unwrap(),
        as_file(&ignore_lines(&["*.bak".to_owned()]))
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

    /// Reconciles, then waits up to 60s for B's state to satisfy `done`,
    /// and returns it.
    /// B's Replica as Syncthing has it now.
    async fn b_observed(&self) -> super::Observed {
        let (_, report) = self.reconcile().await;
        let mut observed = self
            .b
            .driver
            .observe_all(&self.db.config(), &node("b"), &report.replicas)
            .await;
        observed.remove(&ITEM).unwrap()
    }

    async fn b_state(&self, done: impl Fn(&ReplicaState) -> bool) -> ReplicaState {
        let (_, report) = self.reconcile().await;
        state_of(&self.b, &self.db.config(), "b", &report, done).await
    }
}

/// Waits up to 60s for `syncthing`'s state for `ITEM` on `node` to satisfy
/// `done`, and returns the last one read.
async fn state_of(
    syncthing: &Syncthing,
    config: &Config,
    node_name: &str,
    report: &Report,
    done: impl Fn(&ReplicaState) -> bool,
) -> ReplicaState {
    let mut state = None;
    for _ in 0..600 {
        let states = syncthing
            .driver
            .states(config, &node(node_name), &report.replicas)
            .await;
        let read = states[&ITEM].clone();
        if done(&read) {
            return read;
        }
        state = Some(read);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    state.unwrap()
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
    assert_eq!(
        report.replicas[&ITEM],
        Outcome::Waiting(first.as_str().to_owned())
    );
    assert!(syncthing.folder(ITEM).await.is_none());
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
async fn excludes_syncthing_cant_parse_pause_the_folder() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let mut db = tracked("a", &syncthing.dir("docs"));
    syncthing.reconcile(&db.config(), &alone()).await;
    assert_eq!(syncthing.folder(ITEM).await.unwrap()["paused"], false);
    let unparseable = |db: &mut ConfigDb| {
        db.update_item(Timestamp::now(), ITEM, |item| {
            item.excludes = vec![ExcludePattern::new("[").unwrap()]
        })
        .unwrap();
    };
    let failed =
        |outcome: &Outcome| matches!(outcome, Outcome::Failed(error) if error.contains("excludes"));
    unparseable(&mut db);
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert!(
        failed(&report.replicas[&ITEM]),
        "{:?}",
        report.replicas[&ITEM]
    );
    assert_eq!(syncthing.folder(ITEM).await.unwrap()["paused"], true);
    // It runs again once they parse.
    db.update_item(Timestamp::now(), ITEM, |item| item.excludes.clear())
        .unwrap();
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Configured);
    assert_eq!(syncthing.folder(ITEM).await.unwrap()["paused"], false);

    // A new folder starts paused, and stays so with them.
    let mut other = tracked_as(OTHER, "a", &syncthing.dir("other"));
    other
        .update_item(Timestamp::now(), OTHER, |item| {
            item.excludes = vec![ExcludePattern::new("[").unwrap()]
        })
        .unwrap();
    let report = syncthing.reconcile(&other.config(), &alone()).await;
    assert!(
        failed(&report.replicas[&OTHER]),
        "{:?}",
        report.replicas[&OTHER]
    );
    assert_eq!(syncthing.folder(OTHER).await.unwrap()["paused"], true);
}

#[tokio::test]
async fn a_folder_is_paused_before_its_excludes_are_dropped() {
    let mut pair = Pair::start(Direction::TwoWay, false).await;
    fs::write(file(&pair.a_path, "private.tmp"), "mine").unwrap();
    scan(&pair.a).await;
    // Paused, and the exclude dropped, in one round.
    pair.db
        .update_item(Timestamp::now(), ITEM, |item| item.excludes.clear())
        .unwrap();
    pair.db
        .update_replica(Timestamp::now(), ITEM, &node("a"), |replica| {
            replica.paused = true
        })
        .unwrap();
    let (a_report, _) = pair.reconcile().await;
    assert_eq!(a_report.replicas[&ITEM], Outcome::Configured);
    let (ignores, _) = pair.a.driver.client().ignores(ITEM).await.unwrap();
    assert_eq!(ignores, ignore_lines(&[]));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!file(&pair.b_path, "private.tmp").exists());
}

#[tokio::test]
async fn a_resumed_folder_runs_only_with_its_new_excludes() {
    let mut pair = Pair::start(Direction::TwoWay, false).await;
    pair.db
        .update_replica(Timestamp::now(), ITEM, &node("a"), |replica| {
            replica.paused = true
        })
        .unwrap();
    pair.reconcile().await;
    // Made while paused, and excluded before it runs again.
    fs::write(file(&pair.a_path, "private"), "mine").unwrap();
    pair.db
        .update_item(Timestamp::now(), ITEM, |item| {
            item.excludes = vec![ExcludePattern::new("private").unwrap()]
        })
        .unwrap();
    pair.db
        .update_replica(Timestamp::now(), ITEM, &node("a"), |replica| {
            replica.paused = false
        })
        .unwrap();
    let (a_report, _) = pair.reconcile().await;
    assert_eq!(a_report.replicas[&ITEM], Outcome::Configured);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!file(&pair.b_path, "private").exists());
}

#[tokio::test]
async fn a_new_item_at_an_untracked_ones_path_waits_for_its_removal() {
    new_item_at_untracked_ones_path(false).await;
}

#[tokio::test]
async fn a_new_item_at_an_alias_of_an_untracked_ones_path_waits_too() {
    new_item_at_untracked_ones_path(true).await;
}

async fn new_item_at_untracked_ones_path(through_alias: bool) {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let first = syncthing.dir("docs");
    let mut db = tracked("a", &first);
    syncthing.reconcile(&db.config(), &alone()).await;
    assert!(eventually(|| file(&first, MARKER).exists()).await);
    let at = if through_alias {
        let alias = syncthing.dir("alias");
        std::os::unix::fs::symlink(first.as_str(), alias.as_str()).unwrap();
        alias
    } else {
        first.clone()
    };

    // Untracked, and tracked again as a new Item, in one round.
    db.untrack(Timestamp::now(), ITEM).unwrap();
    let mut store = db.store().clone();
    store.merge(tracked_as(OTHER, "a", &at).store(), |_| true);
    let config = Config::new(&store);
    let report = syncthing.reconcile(&config, &alone()).await;
    assert_eq!(
        report.replicas[&OTHER],
        Outcome::Waiting(first.as_str().to_owned())
    );
    assert!(syncthing.folder(ITEM).await.is_none());

    tokio::time::sleep(Duration::from_secs(1)).await;
    let report = syncthing.reconcile(&config, &alone()).await;
    assert_eq!(report.replicas[&OTHER], Outcome::Configured);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(file(&at, MARKER).exists());
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
    store.merge(
        tracked_as(OTHER, "a", &path(file(&link, "inner"))).store(),
        |_| true,
    );
    let report = syncthing.reconcile(&Config::new(&store), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Overlaps);
    assert_eq!(report.replicas[&OTHER], Outcome::Overlaps);
}

#[tokio::test]
async fn a_mirror_is_scanned_with_its_new_excludes_before_promotion() {
    let mut pair = Pair::start(Direction::TwoWay, true).await;
    // An excluded file made locally on the mirror.
    fs::write(file(&pair.b_path, "local.tmp"), "mine").unwrap();
    // The exclude goes, and the mirror is made a peer, in one round.
    pair.db
        .update_item(Timestamp::now(), ITEM, |item| item.excludes.clear())
        .unwrap();
    pair.db
        .update_replica(Timestamp::now(), ITEM, &node("b"), |replica| {
            replica.mirror = false
        })
        .unwrap();
    let (_, b_report) = pair.reconcile().await;
    assert_eq!(b_report.replicas[&ITEM], Outcome::LocalChanges);
    assert_eq!(pair.b.folder(ITEM).await.unwrap()["type"], "receiveonly");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!file(&pair.a_path, "local.tmp").exists());
}

#[tokio::test]
async fn an_alias_of_overlapping_replicas_overlaps_too() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    fs::create_dir(at.as_str()).unwrap();
    let alias = syncthing.dir("alias");
    std::os::unix::fs::symlink(at.as_str(), alias.as_str()).unwrap();
    let third = ItemId::from_u128(0x123);
    let mut store = tracked("a", &at).store().clone();
    store.merge(
        tracked_as(OTHER, "a", &path(file(&at, "nested"))).store(),
        |_| true,
    );
    store.merge(tracked_as(third, "a", &alias).store(), |_| true);
    let report = syncthing.reconcile(&Config::new(&store), &alone()).await;
    assert_eq!(report.replicas[&third], Outcome::Overlaps);
}

#[tokio::test]
async fn replica_states_follow_syncthing() {
    let mut pair = Pair::start(Direction::TwoWay, false).await;
    fs::write(file(&pair.a_path, "one"), "1").unwrap();
    scan(&pair.a).await;
    assert!(eventually(|| file(&pair.b_path, "one").exists()).await);
    let current = |state: &ReplicaState| *state == ReplicaState::Current;
    assert_eq!(pair.b_state(current).await, ReplicaState::Current);

    pair.db
        .update_replica(Timestamp::now(), ITEM, &node("b"), |replica| {
            replica.paused = true
        })
        .unwrap();
    assert_eq!(
        pair.b_state(|state| *state == ReplicaState::Paused).await,
        ReplicaState::Paused
    );
    pair.db
        .update_replica(Timestamp::now(), ITEM, &node("b"), |replica| {
            replica.paused = false
        })
        .unwrap();
    assert_eq!(pair.b_state(current).await, ReplicaState::Current);
    let only_a = BTreeSet::from([node("a")]);
    assert_eq!(pair.b_observed().await.connected, only_a);

    // A third peer, which isn't running: B is still current with A, and
    // connected to A alone.
    let mut x = ConfigDb::new(node("x"), Store::default());
    x.set_device_id(Timestamp::now(), DeviceId::new(SOME_ID).unwrap())
        .unwrap();
    pair.db.merge(x.store(), Timestamp::now());
    pair.db
        .add_replica(
            Timestamp::now(),
            ITEM,
            &node("x"),
            Replica::new(AbsPath::new("/srv/x").unwrap()),
        )
        .unwrap();
    assert_eq!(pair.b_state(current).await, ReplicaState::Current);
    assert_eq!(pair.b_observed().await.connected, only_a);
    pair.db
        .remove_replica(Timestamp::now(), ITEM, &node("x"))
        .unwrap();

    // Its only sender goes away.
    let Pair { a, b, db, .. } = pair;
    drop(a);
    let config = db.config();
    let tailnet = Tailnet::new(
        device("b", OWNER, "127.0.0.2"),
        vec![device("a", OWNER, "127.0.0.1")],
    )
    .unwrap();
    let report = b.reconcile(&config, &tailnet).await;
    let behind = ReplicaState::Behind { last_current: None };
    assert_eq!(
        state_of(&b, &config, "b", &report, |state| *state == behind).await,
        behind
    );
}

#[tokio::test]
async fn a_mirror_with_local_changes_is_an_error() {
    let pair = Pair::start(Direction::TwoWay, true).await;
    fs::write(file(&pair.b_path, "mine"), "local").unwrap();
    scan(&pair.b).await;
    let state = pair
        .b_state(|state| matches!(state, ReplicaState::Error { .. }))
        .await;
    assert!(
        matches!(&state, ReplicaState::Error { message } if message.contains("changed locally")),
        "{state:?}"
    );
}

#[tokio::test]
async fn a_replica_that_cant_be_configured_is_an_error() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    let db = tracked("a", &at);
    syncthing.reconcile(&db.config(), &alone()).await;
    assert!(eventually(|| file(&at, MARKER).exists()).await);
    fs::remove_dir_all(at.as_str()).unwrap();
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    let states = syncthing
        .driver
        .states(&db.config(), &node("a"), &report.replicas)
        .await;
    assert_eq!(
        states[&ITEM],
        ReplicaState::Error {
            message: Outcome::Missing.to_string()
        }
    );
}

#[tokio::test]
async fn syncthing_events_wake_the_watcher() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let changed = Arc::new(tokio::sync::Notify::new());
    let watcher = {
        let (driver, changed) = (syncthing.driver.clone(), changed.clone());
        let (finished, _) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            driver
                .watch(&changed, |seen| {
                    let _ = finished.send(seen);
                })
                .await
        })
    };
    // Let it catch up with the events so far.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let woken = changed.notified();
    syncthing
        .reconcile(&tracked("a", &syncthing.dir("docs")).config(), &alone())
        .await;
    tokio::time::timeout(Duration::from_secs(10), woken)
        .await
        .expect("a new folder's events wake the watcher");
    watcher.abort();
}

#[tokio::test]
async fn syncthing_takes_the_event_filter() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let client = syncthing.driver.client();
    assert_eq!(client.latest_event().await.unwrap(), 0);
    syncthing
        .reconcile(&tracked("a", &syncthing.dir("docs")).config(), &alone())
        .await;
    // Read first: more events may come while the others are read.
    let latest = client.latest_event().await.unwrap();
    assert!(latest > 0);
    let events = client.events(0, Duration::from_secs(1)).await.unwrap();
    let ids: Vec<u64> = events.iter().map(|event| event.id).collect();
    assert!(ids.contains(&latest), "{ids:?} has {latest}");
}

#[tokio::test]
async fn a_sender_syncthing_doesnt_know_is_unreachable() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    // Its device ID is known, but it isn't on the tailnet, so it isn't
    // configured.
    let mut db = added(&syncthing.dir("docs"), false);
    let mut x = ConfigDb::new(node("x"), Store::default());
    x.set_device_id(Timestamp::now(), DeviceId::new(SOME_ID).unwrap())
        .unwrap();
    db.merge(x.store(), Timestamp::now());
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    let states = syncthing
        .driver
        .states(&db.config(), &node("a"), &report.replicas)
        .await;
    assert_eq!(states[&ITEM], ReplicaState::Behind { last_current: None });
}

#[tokio::test]
async fn a_mirror_with_no_sender_is_an_error() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    // Made a mirror on one Node while its sender is removed on another.
    let base = added(&syncthing.dir("docs"), false);
    let mut db = ConfigDb::new(node("a"), base.store().clone());
    db.update_replica(Timestamp::now(), ITEM, &node("a"), |replica| {
        replica.mirror = true
    })
    .unwrap();
    let mut other = ConfigDb::new(node("x"), base.store().clone());
    other
        .remove_replica(Timestamp::now(), ITEM, &node("x"))
        .unwrap();
    db.merge(other.store(), Timestamp::now());
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    let states = syncthing
        .driver
        .states(&db.config(), &node("a"), &report.replicas)
        .await;
    assert!(
        matches!(&states[&ITEM], ReplicaState::Error { message } if message.contains("mirror")),
        "{:?}",
        states[&ITEM]
    );
}

#[tokio::test]
async fn the_watcher_reports_items_syncthing_finishes() {
    let pair = Pair::start(Direction::TwoWay, false).await;
    let changed = Arc::new(tokio::sync::Notify::new());
    let (finished, mut items) = tokio::sync::mpsc::unbounded_channel();
    let watcher = {
        let (driver, changed) = (pair.b.driver.clone(), changed.clone());
        tokio::spawn(async move {
            driver
                .watch(&changed, |seen| {
                    let _ = finished.send(seen);
                })
                .await
        })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    fs::create_dir(file(&pair.a_path, "sub")).unwrap();
    fs::write(file(&pair.a_path, "sub/hello.txt"), "hi").unwrap();
    scan(&pair.a).await;
    let wanted = Seen::Finished(Finished {
        item: ITEM,
        path: "sub/hello.txt".into(),
    });
    let seen = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(item) = items.recv().await {
            if item == wanted {
                return;
            }
        }
    })
    .await;
    assert!(seen.is_ok(), "B's watcher reported the file it received");
    watcher.abort();
}

#[tokio::test]
async fn an_items_retention_window_sets_its_versions_cleanout() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let mut db = tracked("a", &syncthing.dir("docs"));
    db.update_item(Timestamp::now(), ITEM, |item| {
        item.retention_days = std::num::NonZeroU32::new(90)
    })
    .unwrap();
    syncthing.reconcile(&db.config(), &alone()).await;
    let folder = syncthing.folder(ITEM).await.unwrap();
    assert_eq!(folder["versioning"]["params"]["cleanoutDays"], "90");
}

/// The conflict copies directly in `dir`.
fn conflict_copies(dir: &AbsPath) -> Vec<String> {
    fs::read_dir(dir.as_str())
        .unwrap()
        .filter_map(|entry| entry.unwrap().file_name().into_string().ok())
        .filter(|name| name.contains(".sync-conflict-"))
        .collect()
}

#[tokio::test]
async fn a_data_conflict_stays_on_its_node_and_can_be_swapped() {
    let mut pair = Pair::start(Direction::TwoWay, false).await;
    let (a_notes, b_notes) = (
        file(&pair.a_path, "notes.txt"),
        file(&pair.b_path, "notes.txt"),
    );
    fs::write(&a_notes, "base").unwrap();
    scan(&pair.a).await;
    assert!(eventually(|| fs::read_to_string(&b_notes).is_ok_and(|text| text == "base")).await);

    // Both change the file while paused; B's change is older, so it loses.
    let pause = |db: &mut ConfigDb, paused: bool| {
        for name in ["a", "b"] {
            db.update_replica(Timestamp::now(), ITEM, &node(name), |replica| {
                replica.paused = paused
            })
            .unwrap();
        }
    };
    pause(&mut pair.db, true);
    pair.reconcile().await;
    fs::write(&b_notes, "from b").unwrap();
    let an_hour_ago = std::time::SystemTime::now() - Duration::from_secs(3600);
    fs::File::options()
        .write(true)
        .open(&b_notes)
        .unwrap()
        .set_modified(an_hour_ago)
        .unwrap();
    fs::write(&a_notes, "from a").unwrap();
    pause(&mut pair.db, false);
    pair.reconcile().await;
    assert!(eventually(|| fs::read_to_string(&b_notes).is_ok_and(|text| text == "from a")).await);
    assert!(eventually(|| conflict_copies(&pair.b_path).len() == 1).await);
    // The copy is ignored, so it stays on B.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(conflict_copies(&pair.a_path).is_empty());

    let b_root = Path::new(pair.b_path.as_str());
    let collected = conflicts::sweep(b_root).unwrap();
    assert!(collected.failed.is_empty(), "{:?}", collected.failed);
    let [conflict] = &collected.found[..] else {
        panic!("{:?}", collected.found);
    };
    assert_eq!(conflict.path, "notes.txt");
    assert!(conflict_copies(&pair.b_path).is_empty());
    assert_eq!(conflicts::list(b_root).unwrap(), collected.found);

    // B's change is made the file again, and reaches A.
    conflicts::swap(b_root, &conflict.version).unwrap();
    pair.b
        .driver
        .client()
        .scan_path(ITEM, "notes.txt")
        .await
        .unwrap();
    assert!(eventually(|| fs::read_to_string(&a_notes).is_ok_and(|text| text == "from b")).await);
    assert!(conflicts::list(b_root).unwrap().is_empty());
}

#[test]
fn an_adopted_paths_conflict_copies_are_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let at = path(dir.path().join("adopted"));
    fs::create_dir(at.as_str()).unwrap();
    let copy = "thesis.sync-conflict-20240101-120000-ABCDEFG.tex";
    fs::write(file(&at, copy), "the only copy of some edits").unwrap();
    // Set up in a later second than the copy was made.
    std::thread::sleep(Duration::from_millis(1100));
    prepare(&at, true, &[]).unwrap();
    let swept = conflicts::sweep(Path::new(at.as_str())).unwrap();
    assert!(swept.found.is_empty() && swept.failed.is_empty());
    assert!(file(&at, copy).exists());
}

#[tokio::test]
async fn a_replica_removed_and_added_again_starts_cleanly() {
    let syncthing = Syncthing::start("127.0.0.1", free_port()).await;
    let at = syncthing.dir("docs");
    let mut db = added(&at, false);
    syncthing.reconcile(&db.config(), &alone()).await;
    assert!(eventually(|| file(&at, MARKER).exists()).await);

    db.remove_replica(Timestamp::now(), ITEM, &node("a"))
        .unwrap();
    syncthing.reconcile(&db.config(), &alone()).await;
    assert!(syncthing.folder(ITEM).await.is_none());
    // Syncthing removed its marker whole, leaving nothing of its own.
    let names: Vec<String> = fs::read_dir(at.as_str())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        names.iter().all(|name| !name.starts_with(".stfolder")),
        "{names:?}"
    );

    db.add_replica(Timestamp::now(), ITEM, &node("a"), Replica::new(at.clone()))
        .unwrap();
    let report = syncthing.reconcile(&db.config(), &alone()).await;
    assert_eq!(report.replicas[&ITEM], Outcome::Configured);
}

#[test]
fn a_path_with_someone_elses_heighliner_directory_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let at = path(dir.path().join("adopted"));
    fs::create_dir_all(Path::new(at.as_str()).join(".heighliner")).unwrap();
    fs::write(file(&at, ".heighliner/notes"), "the user's").unwrap();
    assert!(matches!(
        prepare(&at, true, &[]),
        Err(Outcome::PathError(_))
    ));
    assert_eq!(
        fs::read_to_string(file(&at, ".heighliner/notes")).unwrap(),
        "the user's"
    );
}

#[tokio::test]
async fn a_departed_node_is_left_out() {
    let mut pair = Pair::start(Direction::TwoWay, false).await;
    let devices = |config: Value| config["devices"].as_array().unwrap().len();
    assert_eq!(devices(pair.a.config().await), 2);
    let mut b = ConfigDb::new(node("b"), pair.db.store().clone());
    b.leave(Timestamp::now()).unwrap();
    pair.db.merge(b.store(), Timestamp::now());
    let (a_report, _) = pair.reconcile().await;
    // Neither syncs with the other.
    assert_eq!(devices(pair.a.config().await), 1);
    assert_eq!(devices(pair.b.config().await), 1);
    // A doesn't wait for B any more.
    let config = pair.db.config();
    let states = pair
        .a
        .driver
        .states(&config, &node("a"), &a_report.replicas)
        .await;
    assert_eq!(states[&ITEM], ReplicaState::Current);
}
