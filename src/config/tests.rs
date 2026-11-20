use proptest::prelude::*;
use serde_json::json;

use super::*;
use crate::hlc::Hlc;
use crate::model::{Direction, ExcludePattern};

const ITEM: ItemId = ItemId::from_u128(1);
const OTHER: ItemId = ItemId::from_u128(2);
const DAY: Duration = Duration::from_secs(24 * 60 * 60);

fn node(id: &str) -> NodeId {
    NodeId::new(id).unwrap()
}

fn replica(p: &str) -> Replica {
    Replica::new(AbsPath::new(p).unwrap())
}

fn mirror(p: &str) -> Replica {
    let mut replica = replica(p);
    replica.mirror = true;
    replica
}

fn t(millis: u64) -> Timestamp {
    Timestamp::from_millis(millis)
}

fn db(id: &str) -> ConfigDb {
    ConfigDb::new(node(id), Store::default())
}

fn sync(a: &mut ConfigDb, b: &mut ConfigDb, now: Timestamp) {
    let a_store = a.store().clone();
    assert_eq!(b.merge(&a_store, now), 0);
    let b_store = b.store().clone();
    assert_eq!(a.merge(&b_store, now), 0);
}

fn two_way(name: &str) -> Item {
    Item::new(name, Direction::TwoWay)
}

fn one_way(name: &str, source: &str) -> Item {
    Item::new(
        name,
        Direction::OneWay {
            source: node(source),
        },
    )
}

fn would_cause(problem: Problem) -> Result<(), EditError> {
    Err(EditError::WouldCause(problem))
}

#[test]
fn track_and_derive_roles() {
    let mut a = db("a");
    a.track(
        t(1),
        ITEM,
        one_way("docs", "a"),
        &node("a"),
        replica("/srv"),
    )
    .unwrap();
    a.add_replica(t(2), ITEM, &node("b"), replica("/srv"))
        .unwrap();

    let config = a.config();
    let entry = &config.items[&ITEM];
    assert_eq!(entry.role(&node("a")), Some(Role::Source));
    assert_eq!(entry.role(&node("b")), Some(Role::Mirror));
    assert_eq!(entry.role(&node("c")), None);
    assert_eq!(config.problems, []);

    // The mirror stays a mirror when the Item becomes two-way.
    a.update_item(t(3), ITEM, |item| item.direction = Direction::TwoWay)
        .unwrap();
    let entry = &a.config().items[&ITEM];
    assert_eq!(entry.role(&node("a")), Some(Role::Peer));
    assert_eq!(entry.role(&node("b")), Some(Role::Mirror));
}

#[test]
fn edits_that_leave_no_sender_are_refused() {
    let mut a = db("a");
    let no_sender = || would_cause(Problem::NoSender { item: ITEM });
    assert_eq!(
        a.track(t(1), ITEM, one_way("x", "b"), &node("a"), replica("/x")),
        no_sender()
    );
    assert_eq!(
        a.track(t(1), ITEM, two_way("x"), &node("a"), mirror("/x")),
        no_sender()
    );

    a.track(t(1), ITEM, two_way("x"), &node("a"), replica("/x"))
        .unwrap();
    a.add_replica(t(2), ITEM, &node("b"), mirror("/x")).unwrap();
    assert_eq!(
        a.update_replica(t(3), ITEM, &node("a"), |r| r.mirror = true),
        no_sender()
    );
    let to_one_way = |item: &mut Item| item.direction = Direction::OneWay { source: node("c") };
    assert_eq!(a.update_item(t(3), ITEM, to_one_way), no_sender());

    a.update_item(t(4), ITEM, |item| {
        item.direction = Direction::OneWay { source: node("b") }
    })
    .unwrap();
    assert_eq!(a.remove_replica(t(5), ITEM, &node("b")), no_sender());
    a.remove_replica(t(5), ITEM, &node("a")).unwrap();
    assert_eq!(
        a.remove_replica(t(6), ITEM, &node("b")),
        Err(EditError::LastReplica { item: ITEM })
    );
}

#[test]
fn edits_that_overlap_are_refused() {
    let mut a = db("a");
    a.track(t(1), ITEM, two_way("x"), &node("a"), replica("/srv"))
        .unwrap();
    let overlap = || {
        would_cause(Problem::Overlap {
            node: node("a"),
            items: [ITEM, OTHER],
        })
    };
    assert_eq!(
        a.track(t(2), OTHER, two_way("y"), &node("a"), replica("/srv/sub")),
        overlap()
    );
    a.track(t(2), OTHER, two_way("y"), &node("b"), replica("/srv/sub"))
        .unwrap();
    assert_eq!(
        a.add_replica(t(3), OTHER, &node("a"), replica("/")),
        overlap()
    );
    a.add_replica(t(3), OTHER, &node("a"), replica("/other"))
        .unwrap();
}

#[test]
fn merged_overlap_is_reported_and_editable() {
    let (mut a, mut b) = (db("a"), db("b"));
    a.track(t(1), ITEM, two_way("x"), &node("a"), replica("/srv"))
        .unwrap();
    b.track(t(1), OTHER, two_way("y"), &node("a"), replica("/srv/sub"))
        .unwrap();
    sync(&mut a, &mut b, t(2));
    assert_eq!(
        a.config().problems,
        [Problem::Overlap {
            node: node("a"),
            items: [ITEM, OTHER],
        }]
    );
    // Edits that don't add a Problem are still allowed.
    a.update_replica(t(3), OTHER, &node("a"), |r| r.paused = true)
        .unwrap();
}

#[test]
fn concurrent_edits_last_write_wins() {
    let (mut a, mut b) = (db("a"), db("b"));
    a.track(t(1), ITEM, two_way("x"), &node("a"), replica("/x"))
        .unwrap();
    sync(&mut a, &mut b, t(2));
    b.update_item(t(20), ITEM, |item| item.name = "from b".into())
        .unwrap();
    a.update_item(t(10), ITEM, |item| item.name = "from a".into())
        .unwrap();
    sync(&mut a, &mut b, t(21));
    assert_eq!(a.config(), b.config());
    assert_eq!(a.config().items[&ITEM].item.name, "from b");
}

#[test]
fn concurrent_rename_and_untrack_is_flagged() {
    let (mut a, mut b) = (db("a"), db("b"));
    a.track(t(1), ITEM, two_way("x"), &node("a"), replica("/x"))
        .unwrap();
    sync(&mut a, &mut b, t(2));
    a.untrack(t(10), ITEM).unwrap();
    b.update_item(t(20), ITEM, |item| item.name = "renamed".into())
        .unwrap();
    sync(&mut a, &mut b, t(21));

    // The rename wins the settings, but the untrack removed the Replicas.
    let config = a.config();
    assert_eq!(config.items[&ITEM].item.name, "renamed");
    assert!(config.items[&ITEM].replicas.is_empty());
    assert_eq!(
        config.problems,
        [
            Problem::Resurrected { item: ITEM },
            Problem::NoSender { item: ITEM }
        ]
    );
    a.untrack(t(22), ITEM).unwrap();
    assert_eq!(a.config(), Config::default());
}

#[test]
fn unseen_settings_edit_keeps_resurrection_flagged() {
    let (mut a, mut b) = (db("a"), db("b"));
    a.track(t(1), ITEM, two_way("x"), &node("a"), replica("/x"))
        .unwrap();
    sync(&mut a, &mut b, t(2));
    b.add_replica(t(5), ITEM, &node("b"), replica("/x"))
        .unwrap();
    b.update_item(t(20), ITEM, |item| item.name = "renamed".into())
        .unwrap();
    a.untrack(t(10), ITEM).unwrap();
    sync(&mut a, &mut b, t(21));
    assert_eq!(b.config().problems, [Problem::Resurrected { item: ITEM }]);

    // An edit that has seen the untrack restores the Item.
    b.update_item(t(22), ITEM, |_| {}).unwrap();
    sync(&mut a, &mut b, t(23));
    assert_eq!(a.config().problems, []);
    assert_eq!(a.config().items[&ITEM].item.name, "renamed");
}

#[test]
fn concurrent_removals_leave_an_item_to_fix() {
    let (mut a, mut b) = (db("a"), db("b"));
    a.track(t(1), ITEM, two_way("x"), &node("a"), replica("/x"))
        .unwrap();
    a.add_replica(t(2), ITEM, &node("b"), replica("/x"))
        .unwrap();
    sync(&mut a, &mut b, t(3));
    a.remove_replica(t(4), ITEM, &node("a")).unwrap();
    b.remove_replica(t(4), ITEM, &node("b")).unwrap();
    sync(&mut a, &mut b, t(5));
    assert_eq!(a.config().problems, [Problem::NoSender { item: ITEM }]);
    a.add_replica(t(6), ITEM, &node("a"), replica("/x"))
        .unwrap();
    assert_eq!(a.config().problems, []);
}

#[test]
fn edits_keep_fixed_and_unknown_fields() {
    let future = foreign_store(vec![
        (
            item_key(ITEM),
            live(1, json!({"name": "x", "direction": "two-way", "future": 1})),
        ),
        (
            replica_key(ITEM, &node("a")),
            live(1, json!({"path": "/x", "future": 2})),
        ),
    ]);
    let mut a = db("a");
    a.merge(&future, t(2));
    a.update_item(t(3), ITEM, |item| *item = two_way("y"))
        .unwrap();
    a.update_replica(t(3), ITEM, &node("a"), |r| *r = mirror("/elsewhere"))
        .unwrap_err();
    a.update_replica(t(3), ITEM, &node("a"), |r| {
        *r = replica("/elsewhere");
        r.paused = true;
    })
    .unwrap();

    let fields = |key| match a.store().get(&key).unwrap().latest().1 {
        Content::Live { fields, .. } => Value::Object(fields.clone()),
        Content::Deleted => panic!("deleted"),
    };
    assert_eq!(fields(item_key(ITEM))["name"], "y");
    assert_eq!(fields(item_key(ITEM))["future"], 1);
    let replica_fields = fields(replica_key(ITEM, &node("a")));
    assert_eq!(replica_fields["path"], "/x");
    assert_eq!(replica_fields["paused"], true);
    assert_eq!(replica_fields["future"], 2);
}

#[test]
fn edits_after_a_merge_supersede_what_was_merged() {
    let (mut a, mut b) = (db("a"), db("b"));
    // a's wall clock is a minute ahead of b's.
    a.track(t(60_000), ITEM, two_way("x"), &node("a"), replica("/x"))
        .unwrap();
    sync(&mut a, &mut b, t(5));
    b.update_item(t(5), ITEM, |item| item.name = "renamed".into())
        .unwrap();
    sync(&mut a, &mut b, t(6));
    assert_eq!(a.config().items[&ITEM].item.name, "renamed");
}

#[test]
fn merge_leaves_out_versions_from_far_ahead() {
    let mut ahead = db("ahead");
    let far = MAX_CLOCK_DRIFT.as_millis() as u64 + 1;
    ahead
        .track(t(far), ITEM, two_way("x"), &node("a"), replica("/x"))
        .unwrap();
    let mut a = db("a");
    assert_eq!(a.merge(ahead.store(), t(0)), 2);
    assert_eq!(a.config(), Config::default());
    assert_eq!(a.merge(ahead.store(), t(1)), 0);
    assert!(a.config().items.contains_key(&ITEM));
}

#[test]
fn reopening_a_store_continues_its_clock() {
    let mut a = db("a");
    a.track(t(1_000), ITEM, two_way("x"), &node("a"), replica("/x"))
        .unwrap();
    let mut reopened = ConfigDb::new(node("a"), a.store().clone());
    reopened
        .update_item(t(5), ITEM, |item| item.name = "renamed".into())
        .unwrap();
    assert_eq!(reopened.config().items[&ITEM].item.name, "renamed");
}

#[test]
fn replica_unseen_by_untrack_resurrects_the_item() {
    let (mut a, mut b) = (db("a"), db("b"));
    a.track(t(1), ITEM, two_way("x"), &node("a"), replica("/x"))
        .unwrap();
    sync(&mut a, &mut b, t(2));
    b.add_replica(t(5), ITEM, &node("b"), replica("/x"))
        .unwrap();
    a.untrack(t(10), ITEM).unwrap();
    sync(&mut a, &mut b, t(11));

    let resurrected = |db: &ConfigDb| {
        let config = db.config();
        assert_eq!(config.problems, [Problem::Resurrected { item: ITEM }]);
        let entry = &config.items[&ITEM];
        assert_eq!(entry.item.name, "x");
        assert_eq!(entry.replicas.keys().collect::<Vec<_>>(), [&node("b")]);
    };
    resurrected(&a);

    // The Item's settings outlive the Retention Window while it has Replicas.
    let later = t(11).saturating_add(DAY * 100);
    a.prune(later, DAY * 30);
    resurrected(&a);

    // Editing the Item restores it.
    a.update_item(later, ITEM, |_| {}).unwrap();
    assert_eq!(a.config().problems, []);
}

#[test]
fn untrack_removes_every_replica() {
    let mut a = db("a");
    a.track(t(1), ITEM, two_way("x"), &node("a"), replica("/x"))
        .unwrap();
    a.add_replica(t(2), ITEM, &node("b"), replica("/x"))
        .unwrap();
    a.untrack(t(3), ITEM).unwrap();
    assert_eq!(a.config(), Config::default());
    assert_eq!(a.untrack(t(4), ITEM), Err(EditError::UnknownItem(ITEM)));
    a.prune(t(4).saturating_add(DAY * 31), DAY * 30);
    assert_eq!(a.store(), &Store::default());
}

fn foreign_hlc() -> Hlc {
    Hlc {
        wall: t(1),
        counter: 0,
        node: node("z"),
    }
}

fn live(schema: u32, fields: Value) -> Content {
    let Value::Object(fields) = fields else {
        panic!("fields must be an object");
    };
    Content::Live { schema, fields }
}

fn foreign_store(records: Vec<(RecordKey, Content)>) -> Store {
    let mut store = Store::default();
    for (key, content) in records {
        store.insert(key, foreign_hlc(), content);
    }
    store
}

#[test]
fn unknown_fields_and_kinds_are_preserved() {
    let gadget = RecordKey {
        kind: "gadget".into(),
        id: "g".into(),
    };
    let future = foreign_store(vec![
        (
            item_key(ITEM),
            live(
                1,
                json!({"name": "x", "direction": "two-way", "future": true}),
            ),
        ),
        (
            replica_key(ITEM, &node("a")),
            live(1, json!({"path": "/x"})),
        ),
        (gadget.clone(), live(7, json!({"a": 1}))),
    ]);

    let mut a = db("a");
    a.merge(&future, t(2));
    a.update_item(t(2), ITEM, |item| item.name = "renamed".into())
        .unwrap();

    let (_, content) = a.store().get(&item_key(ITEM)).unwrap().latest();
    assert_eq!(
        content,
        &live(
            1,
            json!({
                "name": "renamed",
                "direction": "two-way",
                "excludes": [],
                "paused": false,
                "future": true,
            })
        )
    );
    assert_eq!(a.store().get(&gadget), future.get(&gadget));
}

#[test]
fn unreadable_records_are_kept_but_not_used() {
    for item in [
        live(SCHEMA + 1, json!({"name": "x", "direction": "two-way"})),
        live(1, json!({"name": "x", "direction": "n-way"})),
        live(
            1,
            json!({"name": "x", "direction": "two-way", "excludes": ["(?d)x"]}),
        ),
    ] {
        let future = foreign_store(vec![
            (item_key(ITEM), item),
            (
                replica_key(ITEM, &node("a")),
                live(1, json!({"path": "/x"})),
            ),
        ]);
        let mut a = db("a");
        a.merge(&future, t(2));
        let config = a.config();
        assert!(config.items.is_empty());
        assert!(matches!(
            config.problems.as_slice(),
            [
                Problem::Unreadable { .. },
                Problem::Orphaned { item: ITEM, .. }
            ]
        ));
        assert_eq!(
            a.update_item(t(2), ITEM, |_| {}),
            Err(EditError::UnknownItem(ITEM))
        );
        a.untrack(t(2), ITEM).unwrap();
        assert_eq!(a.config(), Config::default());
    }
}

#[test]
fn non_canonical_keys_are_unreadable() {
    let upper = ItemId::from_u128(0xabcdef).to_string().to_uppercase();
    let future = foreign_store(vec![
        (
            RecordKey {
                kind: ITEM_KIND.into(),
                id: upper.clone(),
            },
            live(1, json!({"name": "x", "direction": "two-way"})),
        ),
        (
            RecordKey {
                kind: REPLICA_KIND.into(),
                id: format!("{upper}/a"),
            },
            live(1, json!({"path": "/x"})),
        ),
    ]);
    let config = Config::new(&future);
    assert!(config.items.is_empty());
    assert_eq!(config.problems.len(), 2);
    assert!(
        config
            .problems
            .iter()
            .all(|p| matches!(p, Problem::Unreadable { .. }))
    );
}

#[test]
fn excludes_are_validated_when_set() {
    assert!(ExcludePattern::new("(?d)*").is_err());
    let mut item = two_way("x");
    item.excludes = vec![ExcludePattern::new("*.tmp").unwrap()];
    let mut a = db("a");
    a.track(t(1), ITEM, item, &node("a"), replica("/x"))
        .unwrap();
}

// Simulation: random edits on random Nodes with skewed clocks, interleaved
// with partial syncs, pruning and restarts.

const NODES: [&str; 3] = ["a", "b", "c"];
const PATHS: [&str; 4] = ["/a", "/a/b", "/c", "/d"];

#[derive(Clone, Debug)]
enum Op {
    Track {
        item: u8,
        node: usize,
        path: usize,
        one_way: bool,
        mirror: bool,
    },
    Rename {
        item: u8,
        name: u8,
    },
    SetDirection {
        item: u8,
        source: Option<usize>,
    },
    PauseItem {
        item: u8,
        paused: bool,
    },
    Untrack {
        item: u8,
    },
    AddReplica {
        item: u8,
        node: usize,
        path: usize,
        mirror: bool,
    },
    UpdateReplica {
        item: u8,
        node: usize,
        mirror: bool,
        paused: bool,
    },
    RemoveReplica {
        item: u8,
        node: usize,
    },
    Sync {
        to: usize,
    },
    Prune {
        window: u64,
    },
    Restart,
}

/// An Op, the Node it runs on, and how far time advances before it.
type Step = (usize, u64, Op);

fn arb_op() -> impl Strategy<Value = Op> {
    let item = 0..3u8;
    let node = 0..NODES.len();
    let path = 0..PATHS.len();
    prop_oneof![
        (
            item.clone(),
            node.clone(),
            path.clone(),
            any::<bool>(),
            any::<bool>()
        )
            .prop_map(|(item, node, path, one_way, mirror)| Op::Track {
                item,
                node,
                path,
                one_way,
                mirror
            }),
        (item.clone(), 0..2u8).prop_map(|(item, name)| Op::Rename { item, name }),
        (item.clone(), prop::option::of(node.clone()))
            .prop_map(|(item, source)| Op::SetDirection { item, source }),
        (item.clone(), any::<bool>()).prop_map(|(item, paused)| Op::PauseItem { item, paused }),
        item.clone().prop_map(|item| Op::Untrack { item }),
        (item.clone(), node.clone(), path, any::<bool>()).prop_map(|(item, node, path, mirror)| {
            Op::AddReplica {
                item,
                node,
                path,
                mirror,
            }
        }),
        (item.clone(), node.clone(), any::<bool>(), any::<bool>()).prop_map(
            |(item, node, mirror, paused)| Op::UpdateReplica {
                item,
                node,
                mirror,
                paused
            }
        ),
        (item, node.clone()).prop_map(|(item, node)| Op::RemoveReplica { item, node }),
        node.prop_map(|to| Op::Sync { to }),
        (0..20u64).prop_map(|window| Op::Prune { window }),
        Just(Op::Restart),
    ]
}

fn arb_steps(nodes: usize) -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec((0..nodes, 0..3u64, arb_op()), 0..60)
}

fn item_id(item: u8) -> ItemId {
    ItemId::from_u128(u128::from(item) + 1)
}

/// Applies `op` on `dbs[on]`. Edits that validation refuses are skipped.
fn apply(dbs: &mut [ConfigDb], on: usize, now: impl Fn(usize) -> Timestamp, op: &Op) {
    let nodes = NODES.map(node);
    let at = now(on);
    let db = &mut dbs[on];
    let _ = match *op {
        Op::Track {
            item,
            node,
            path,
            one_way,
            mirror,
        } => {
            let direction = if one_way {
                Direction::OneWay {
                    source: nodes[node].clone(),
                }
            } else {
                Direction::TwoWay
            };
            let mut new = replica(PATHS[path]);
            new.mirror = mirror;
            db.track(
                at,
                item_id(item),
                Item::new(format!("item {item}"), direction),
                &nodes[node],
                new,
            )
        }
        Op::Rename { item, name } => {
            db.update_item(at, item_id(item), |i| i.name = format!("name {name}"))
        }
        Op::SetDirection { item, source } => db.update_item(at, item_id(item), |i| {
            i.direction = match source {
                Some(source) => Direction::OneWay {
                    source: nodes[source].clone(),
                },
                None => Direction::TwoWay,
            }
        }),
        Op::PauseItem { item, paused } => db.update_item(at, item_id(item), |i| i.paused = paused),
        Op::Untrack { item } => db.untrack(at, item_id(item)),
        Op::AddReplica {
            item,
            node,
            path,
            mirror,
        } => {
            let mut new = replica(PATHS[path]);
            new.mirror = mirror;
            db.add_replica(at, item_id(item), &nodes[node], new)
        }
        Op::UpdateReplica {
            item,
            node,
            mirror,
            paused,
        } => db.update_replica(at, item_id(item), &nodes[node], |r| {
            r.mirror = mirror;
            r.paused = paused;
        }),
        Op::RemoveReplica { item, node } => db.remove_replica(at, item_id(item), &nodes[node]),
        Op::Sync { to } => {
            if to < dbs.len() {
                let store = dbs[on].store().clone();
                assert_eq!(dbs[to].merge(&store, now(to)), 0);
            }
            Ok(())
        }
        Op::Prune { window } => {
            db.prune(at, Duration::from_millis(window));
            Ok(())
        }
        Op::Restart => {
            *db = ConfigDb::new(nodes[on].clone(), db.store().clone());
            Ok(())
        }
    };
}

/// Checks that pruning left no version superseded more than `window` before
/// `now`, other than a record's latest live version.
fn check_pruned(store: &Store, now: Timestamp, window: Duration) -> Result<(), TestCaseError> {
    for (key, record) in store.records() {
        let versions: Vec<_> = record.versions().collect();
        let latest_live = versions
            .iter()
            .rev()
            .find(|(_, content)| matches!(content, Content::Live { .. }))
            .map(|(hlc, _)| *hlc);
        for pair in versions.windows(2) {
            let ((old, _), (successor, _)) = (pair[0], pair[1]);
            if Some(old) != latest_live {
                prop_assert!(successor.wall.saturating_add(window) >= now, "{key:?}");
            }
        }
    }
    Ok(())
}

/// Runs `steps`, checking every Node's configuration after each one. Each
/// Node's wall clock is offset by its index times `skew` milliseconds.
fn run(nodes: usize, skew: u64, steps: &[Step]) -> Result<Vec<ConfigDb>, TestCaseError> {
    let mut dbs: Vec<ConfigDb> = NODES[..nodes].iter().map(|id| db(id)).collect();
    let mut now = 0;
    for (on, advance, op) in steps {
        now += advance;
        let wall = |node: usize| t(now + u64::try_from(node).unwrap() * skew);
        apply(&mut dbs, *on, wall, op);
        if let Op::Prune { window } = op {
            check_pruned(dbs[*on].store(), wall(*on), Duration::from_millis(*window))?;
        }
        for db in &dbs {
            check_valid(db.store())?;
        }
    }
    Ok(dbs)
}

/// Checks the invariants every configuration must meet, however its store
/// was merged.
fn check_valid(store: &Store) -> Result<Config, TestCaseError> {
    let config = Config::new(store);

    // Every live, readable Replica record is either in the configuration or
    // reported as orphaned.
    for (key, record) in store.records() {
        if key.kind != REPLICA_KIND || !is_live(Some(record)) {
            continue;
        }
        let (item, node) = parse_replica_id(&key.id).unwrap();
        let listed = config
            .items
            .get(&item)
            .is_some_and(|entry| entry.replicas.contains_key(&node));
        let orphaned = config.problems.contains(&Problem::Orphaned {
            item,
            node: node.clone(),
        });
        prop_assert!(listed != orphaned, "{key:?}");
    }

    for (id, entry) in &config.items {
        let roles: Vec<Role> = entry
            .replicas
            .iter()
            .map(|(node, r)| entry.item.direction.role(node, r.mirror))
            .collect();
        let count = |role| roles.iter().filter(|r| **r == role).count();
        match &entry.item.direction {
            Direction::OneWay { .. } => {
                prop_assert!(count(Role::Source) <= 1);
                prop_assert_eq!(count(Role::Peer), 0);
            }
            Direction::TwoWay => prop_assert_eq!(count(Role::Source), 0),
        }
        if count(Role::Source) + count(Role::Peer) == 0 {
            let no_sender = Problem::NoSender { item: *id };
            prop_assert!(config.problems.contains(&no_sender));
        }
    }

    for (a, a_entry) in &config.items {
        for (b, b_entry) in config.items.range(a..).skip(1) {
            for (node, a_replica) in &a_entry.replicas {
                let Some(b_replica) = b_entry.replicas.get(node) else {
                    continue;
                };
                let (a_path, b_path) = (a_replica.path().as_str(), b_replica.path().as_str());
                let nested = |outer: &str, inner: &str| {
                    outer == "/" || inner == outer || inner.starts_with(&format!("{outer}/"))
                };
                if nested(a_path, b_path) || nested(b_path, a_path) {
                    let overlap = Problem::Overlap {
                        node: node.clone(),
                        items: [*a, *b],
                    };
                    prop_assert!(config.problems.contains(&overlap));
                }
            }
        }
    }

    for problem in &config.problems {
        if let Problem::Resurrected { item } = problem {
            prop_assert!(config.items.contains_key(item));
        }
    }
    Ok(config)
}

proptest! {
    #[test]
    fn merged_configurations_are_valid_and_converge(
        skew in 0..20u64,
        steps in arb_steps(NODES.len()),
    ) {
        let mut dbs = run(NODES.len(), skew, &steps)?;

        let mut all = Store::default();
        for db in &dbs {
            all.merge(db.store(), |_| true);
        }
        let config = check_valid(&all)?;
        for db in &mut dbs {
            db.merge(&all, t(u64::MAX / 2));
            prop_assert_eq!(&db.config(), &config);
        }
    }

    #[test]
    fn a_single_nodes_edits_cause_no_problems(steps in arb_steps(1)) {
        let dbs = run(1, 0, &steps)?;
        let problems = dbs[0].config().problems;
        prop_assert!(
            problems.iter().all(|p| matches!(p, Problem::DuplicateName { .. })),
            "{:?}",
            problems
        );
    }
}
