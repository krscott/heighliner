use proptest::prelude::*;
use serde_json::json;

use super::*;
use crate::hlc::Hlc;

const ITEM: ItemId = ItemId::from_u128(1);
const OTHER: ItemId = ItemId::from_u128(2);

fn node(id: &str) -> NodeId {
    NodeId::new(id).unwrap()
}

fn path(p: &str) -> AbsPath {
    AbsPath::new(p).unwrap()
}

fn t(millis: u64) -> Timestamp {
    Timestamp::from_millis(millis)
}

fn db(id: &str) -> ConfigDb {
    ConfigDb::new(node(id), Store::default())
}

fn sync(a: &mut ConfigDb, b: &mut ConfigDb) {
    let a_store = a.store().clone();
    b.merge(&a_store);
    let b_store = b.store().clone();
    a.merge(&b_store);
}

fn two_way(name: &str) -> Item {
    Item::new(name, Direction::TwoWay)
}

#[test]
fn track_and_derive_roles() {
    let mut a = db("a");
    let one_way = Item::new("docs", Direction::OneWay { source: node("a") });
    a.track(
        t(1),
        ITEM,
        &one_way,
        &node("a"),
        &Replica::new(path("/srv/docs")),
    )
    .unwrap();
    a.add_replica(t(2), ITEM, &node("b"), &Replica::new(path("/srv/docs")))
        .unwrap();

    let config = a.config();
    let entry = &config.items[&ITEM];
    assert_eq!(entry.role(&node("a")), Some(Role::Source));
    assert_eq!(entry.role(&node("b")), Some(Role::Mirror));
    assert_eq!(entry.role(&node("c")), None);
    assert_eq!(config.problems, []);
}

#[test]
fn one_way_source_needs_a_replica() {
    let mut a = db("a");
    let from_b = Item::new("docs", Direction::OneWay { source: node("b") });
    assert!(matches!(
        a.track(t(1), ITEM, &from_b, &node("a"), &Replica::new(path("/x"))),
        Err(EditError::SourceWithoutReplica { .. })
    ));

    a.track(
        t(1),
        ITEM,
        &two_way("docs"),
        &node("a"),
        &Replica::new(path("/x")),
    )
    .unwrap();
    let to_b = |item: &mut Item| item.direction = Direction::OneWay { source: node("b") };
    assert!(matches!(
        a.update_item(t(2), ITEM, to_b),
        Err(EditError::SourceWithoutReplica { .. })
    ));
    a.add_replica(t(3), ITEM, &node("b"), &Replica::new(path("/x")))
        .unwrap();
    a.update_item(t(4), ITEM, to_b).unwrap();
    assert!(matches!(
        a.remove_replica(t(5), ITEM, &node("b")),
        Err(EditError::RemovesSource { .. })
    ));
    a.remove_replica(t(5), ITEM, &node("a")).unwrap();
}

#[test]
fn local_edits_refuse_overlaps() {
    let mut a = db("a");
    a.track(
        t(1),
        ITEM,
        &two_way("x"),
        &node("a"),
        &Replica::new(path("/srv")),
    )
    .unwrap();
    assert!(matches!(
        a.track(
            t(2),
            OTHER,
            &two_way("y"),
            &node("a"),
            &Replica::new(path("/srv/sub"))
        ),
        Err(EditError::Overlap { .. })
    ));
    a.track(
        t(2),
        OTHER,
        &two_way("y"),
        &node("b"),
        &Replica::new(path("/srv/sub")),
    )
    .unwrap();
    assert!(matches!(
        a.add_replica(t(3), OTHER, &node("a"), &Replica::new(path("/"))),
        Err(EditError::Overlap { .. })
    ));
    a.add_replica(t(3), OTHER, &node("a"), &Replica::new(path("/other")))
        .unwrap();
    assert!(matches!(
        a.update_replica(t(4), OTHER, &node("a"), |r| r.path = path("/srv")),
        Err(EditError::Overlap { .. })
    ));
}

#[test]
fn merged_overlap_is_reported() {
    let (mut a, mut b) = (db("a"), db("b"));
    a.track(
        t(1),
        ITEM,
        &two_way("x"),
        &node("a"),
        &Replica::new(path("/srv")),
    )
    .unwrap();
    b.track(
        t(1),
        OTHER,
        &two_way("y"),
        &node("a"),
        &Replica::new(path("/srv/sub")),
    )
    .unwrap();
    sync(&mut a, &mut b);
    assert_eq!(
        a.config().problems,
        [Problem::Overlap {
            node: node("a"),
            items: [ITEM, OTHER],
        }]
    );
}

#[test]
fn concurrent_edits_last_write_wins() {
    let (mut a, mut b) = (db("a"), db("b"));
    a.track(
        t(1),
        ITEM,
        &two_way("x"),
        &node("a"),
        &Replica::new(path("/x")),
    )
    .unwrap();
    sync(&mut a, &mut b);
    b.update_item(t(20), ITEM, |item| item.name = "from b".into())
        .unwrap();
    a.update_item(t(10), ITEM, |item| item.name = "from a".into())
        .unwrap();
    sync(&mut a, &mut b);
    assert_eq!(a.config(), b.config());
    assert_eq!(a.config().items[&ITEM].item.name, "from b");
}

#[test]
fn edits_after_a_merge_supersede_what_was_merged() {
    let (mut a, mut b) = (db("a"), db("b"));
    // a's wall clock is far ahead of b's.
    a.track(
        t(1_000),
        ITEM,
        &two_way("x"),
        &node("a"),
        &Replica::new(path("/x")),
    )
    .unwrap();
    sync(&mut a, &mut b);
    b.update_item(t(5), ITEM, |item| item.name = "renamed".into())
        .unwrap();
    sync(&mut a, &mut b);
    assert_eq!(a.config().items[&ITEM].item.name, "renamed");
}

#[test]
fn reopening_a_store_continues_its_clock() {
    let mut a = db("a");
    a.track(
        t(1_000),
        ITEM,
        &two_way("x"),
        &node("a"),
        &Replica::new(path("/x")),
    )
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
    a.track(
        t(1),
        ITEM,
        &two_way("x"),
        &node("a"),
        &Replica::new(path("/x")),
    )
    .unwrap();
    sync(&mut a, &mut b);
    b.add_replica(t(5), ITEM, &node("b"), &Replica::new(path("/x")))
        .unwrap();
    a.untrack(t(10), ITEM).unwrap();
    sync(&mut a, &mut b);

    let config = a.config();
    assert_eq!(config.problems, [Problem::Resurrected { item: ITEM }]);
    let entry = &config.items[&ITEM];
    assert_eq!(entry.item.name, "x");
    assert_eq!(entry.replicas.keys().collect::<Vec<_>>(), [&node("b")]);

    // Editing the Item restores it.
    a.update_item(t(11), ITEM, |_| {}).unwrap();
    assert_eq!(a.config().problems, []);
}

#[test]
fn untrack_removes_every_replica() {
    let mut a = db("a");
    a.track(
        t(1),
        ITEM,
        &two_way("x"),
        &node("a"),
        &Replica::new(path("/x")),
    )
    .unwrap();
    a.add_replica(t(2), ITEM, &node("b"), &Replica::new(path("/x")))
        .unwrap();
    a.untrack(t(3), ITEM).unwrap();
    assert_eq!(a.config(), Config::default());
    assert_eq!(a.untrack(t(4), ITEM), Err(EditError::UnknownItem(ITEM)));
}

fn future_hlc() -> Hlc {
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

#[test]
fn unknown_fields_and_kinds_are_preserved() {
    let mut future = Store::default();
    future.insert(
        item_key(ITEM),
        future_hlc(),
        live(
            2,
            json!({"name": "x", "direction": "two-way", "future": true}),
        ),
    );
    let gadget = RecordKey {
        kind: "gadget".into(),
        id: "g".into(),
    };
    future.insert(gadget.clone(), future_hlc(), live(2, json!({"a": 1})));

    let mut a = db("a");
    a.merge(&future);
    a.update_item(t(2), ITEM, |item| item.name = "renamed".into())
        .unwrap();

    let (_, content) = a.store().get(&item_key(ITEM)).unwrap().latest();
    assert_eq!(
        content,
        &live(
            2,
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
    let mut future = Store::default();
    future.insert(
        item_key(ITEM),
        future_hlc(),
        live(2, json!({"name": "x", "direction": "n-way"})),
    );
    future.insert(
        replica_key(ITEM, &node("a")),
        future_hlc(),
        live(2, json!({"path": "/x"})),
    );

    let mut a = db("a");
    a.merge(&future);
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

// Simulation: random edits on random Nodes, interleaved with partial syncs.

const NODES: [&str; 3] = ["a", "b", "c"];
const PATHS: [&str; 4] = ["/a", "/a/b", "/c", "/d"];

#[derive(Clone, Debug)]
enum Op {
    Track {
        item: u8,
        node: usize,
        path: usize,
        one_way: bool,
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
        path: usize,
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
}

/// An Op, the Node it runs on, and how far time advances before it.
type Step = (usize, u64, Op);

fn arb_op() -> impl Strategy<Value = Op> {
    let item = 0..3u8;
    let node = 0..NODES.len();
    let path = 0..PATHS.len();
    prop_oneof![
        (item.clone(), node.clone(), path.clone(), any::<bool>()).prop_map(
            |(item, node, path, one_way)| Op::Track {
                item,
                node,
                path,
                one_way
            }
        ),
        (item.clone(), 0..2u8).prop_map(|(item, name)| Op::Rename { item, name }),
        (item.clone(), prop::option::of(node.clone()))
            .prop_map(|(item, source)| Op::SetDirection { item, source }),
        (item.clone(), any::<bool>()).prop_map(|(item, paused)| Op::PauseItem { item, paused }),
        item.clone().prop_map(|item| Op::Untrack { item }),
        (item.clone(), node.clone(), path.clone(), any::<bool>()).prop_map(
            |(item, node, path, mirror)| Op::AddReplica {
                item,
                node,
                path,
                mirror
            }
        ),
        (
            item.clone(),
            node.clone(),
            path,
            any::<bool>(),
            any::<bool>()
        )
            .prop_map(|(item, node, path, mirror, paused)| Op::UpdateReplica {
                item,
                node,
                path,
                mirror,
                paused
            }),
        (item, node.clone()).prop_map(|(item, node)| Op::RemoveReplica { item, node }),
        node.prop_map(|to| Op::Sync { to }),
    ]
}

fn arb_steps(nodes: usize) -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec((0..nodes, 0..3u64, arb_op()), 0..60)
}

fn item_id(item: u8) -> ItemId {
    ItemId::from_u128(u128::from(item) + 1)
}

/// Applies `op` on `dbs[on]`. Edits that validation refuses are skipped.
fn apply(dbs: &mut [ConfigDb], on: usize, now: Timestamp, op: &Op) {
    let nodes = NODES.map(node);
    let paths = PATHS.map(path);
    let db = &mut dbs[on];
    let _ = match *op {
        Op::Track {
            item,
            node,
            path,
            one_way,
        } => {
            let direction = if one_way {
                Direction::OneWay {
                    source: nodes[node].clone(),
                }
            } else {
                Direction::TwoWay
            };
            db.track(
                now,
                item_id(item),
                &Item::new(format!("item {item}"), direction),
                &nodes[node],
                &Replica::new(paths[path].clone()),
            )
        }
        Op::Rename { item, name } => {
            db.update_item(now, item_id(item), |i| i.name = format!("name {name}"))
        }
        Op::SetDirection { item, source } => db.update_item(now, item_id(item), |i| {
            i.direction = match source {
                Some(source) => Direction::OneWay {
                    source: nodes[source].clone(),
                },
                None => Direction::TwoWay,
            }
        }),
        Op::PauseItem { item, paused } => db.update_item(now, item_id(item), |i| i.paused = paused),
        Op::Untrack { item } => db.untrack(now, item_id(item)),
        Op::AddReplica {
            item,
            node,
            path,
            mirror,
        } => {
            let mut replica = Replica::new(paths[path].clone());
            replica.mirror = mirror;
            db.add_replica(now, item_id(item), &nodes[node], &replica)
        }
        Op::UpdateReplica {
            item,
            node,
            path,
            mirror,
            paused,
        } => db.update_replica(now, item_id(item), &nodes[node], |r| {
            r.path = paths[path].clone();
            r.mirror = mirror;
            r.paused = paused;
        }),
        Op::RemoveReplica { item, node } => db.remove_replica(now, item_id(item), &nodes[node]),
        Op::Sync { to } => {
            if to < dbs.len() {
                let store = dbs[on].store().clone();
                dbs[to].merge(&store);
            }
            Ok(())
        }
    };
}

/// Runs `steps`, checking every Node's configuration after each one. Each
/// Node's wall clock is offset by its index times `skew` milliseconds.
fn run(nodes: usize, skew: u64, steps: &[Step]) -> Result<Vec<ConfigDb>, TestCaseError> {
    let mut dbs: Vec<ConfigDb> = NODES[..nodes].iter().map(|id| db(id)).collect();
    let mut now = 0;
    for (on, advance, op) in steps {
        now += advance;
        let offset = u64::try_from(*on).unwrap() * skew;
        apply(&mut dbs, *on, t(now + offset), op);
        for db in &dbs {
            check_valid(&db.config())?;
        }
    }
    Ok(dbs)
}

/// Checks the invariants every configuration must meet, however it was
/// merged.
fn check_valid(config: &Config) -> Result<(), TestCaseError> {
    for (id, entry) in &config.items {
        let roles: Vec<Role> = entry
            .replicas
            .keys()
            .map(|node| entry.role(node).unwrap())
            .collect();
        let sources = roles.iter().filter(|r| **r == Role::Source).count();
        match &entry.item.direction {
            Direction::OneWay { source } => {
                prop_assert!(!roles.contains(&Role::Peer));
                if entry.replicas.contains_key(source) {
                    prop_assert_eq!(sources, 1);
                } else {
                    prop_assert_eq!(sources, 0);
                    let missing = Problem::MissingSource {
                        item: *id,
                        source: source.clone(),
                    };
                    prop_assert!(config.problems.contains(&missing));
                }
            }
            Direction::TwoWay => prop_assert_eq!(sources, 0),
        }
    }

    for (a, a_entry) in &config.items {
        for (b, b_entry) in config.items.range(a..).skip(1) {
            for (node, a_replica) in &a_entry.replicas {
                if let Some(b_replica) = b_entry.replicas.get(node)
                    && a_replica.path.overlaps(&b_replica.path)
                {
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
        match problem {
            Problem::Resurrected { item } => prop_assert!(config.items.contains_key(item)),
            Problem::Orphaned { item, .. } => prop_assert!(!config.items.contains_key(item)),
            _ => {}
        }
    }
    Ok(())
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
            all.merge(db.store());
        }
        for db in &mut dbs {
            db.merge(&all);
        }
        let config = dbs[0].config();
        check_valid(&config)?;
        for db in &dbs[1..] {
            prop_assert_eq!(db.store(), dbs[0].store());
            prop_assert_eq!(&db.config(), &config);
        }
    }

    #[test]
    fn a_single_node_edits_cause_no_problems(steps in arb_steps(1)) {
        let dbs = run(1, 0, &steps)?;
        let problems = dbs[0].config().problems;
        prop_assert!(
            problems.iter().all(|p| matches!(p, Problem::DuplicateName { .. })),
            "{:?}",
            problems
        );
    }
}
