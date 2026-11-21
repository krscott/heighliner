//! Hybrid logical clocks, which version every replicated record.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::model::NodeId;

/// Wall-clock time in milliseconds since the Unix epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// The current wall-clock time. A clock set before 1970 reads as the epoch.
    pub fn now() -> Self {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self(u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX))
    }

    pub fn saturating_add(self, duration: Duration) -> Self {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        Self(self.0.saturating_add(millis))
    }
}

/// A hybrid logical clock reading.
///
/// Readings are totally ordered: by wall time, then counter, then the Node
/// that took them. A Node never takes the same reading twice (short of
/// exhausting the clock at `u64::MAX` milliseconds), so two versions written
/// by different edits never compare equal.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Hlc {
    pub wall: Timestamp,
    pub counter: u32,
    pub node: NodeId,
}

/// A Node's hybrid logical clock.
#[derive(Clone, Debug)]
pub struct Clock {
    node: NodeId,
    /// The latest reading taken or observed, as wall time and counter.
    last: (Timestamp, u32),
}

impl Clock {
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            last: (Timestamp(0), 0),
        }
    }

    pub fn node(&self) -> &NodeId {
        &self.node
    }

    /// Takes a reading later than every earlier reading and every observed one.
    pub fn tick(&mut self, now: Timestamp) -> Hlc {
        let (last_wall, last_counter) = self.last;
        let (wall, counter) = if now > last_wall {
            (now, 0)
        } else {
            match last_counter.checked_add(1) {
                Some(counter) => (last_wall, counter),
                None => (last_wall.saturating_add(Duration::from_millis(1)), 0),
            }
        };
        self.last = (wall, counter);
        self.last()
    }

    /// The latest reading taken or observed, to persist so that a reopened
    /// clock resumes after it.
    pub fn last(&self) -> Hlc {
        let (wall, counter) = self.last;
        Hlc {
            wall,
            counter,
            node: self.node.clone(),
        }
    }

    /// Records a reading seen elsewhere, so the next tick is later than it.
    pub fn observe(&mut self, seen: &Hlc) {
        self.last = self.last.max((seen.wall, seen.counter));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> NodeId {
        NodeId::new(id).unwrap()
    }

    #[test]
    fn tick_follows_wall_clock() {
        let mut clock = Clock::new(node("a"));
        let first = clock.tick(Timestamp::from_millis(10));
        let second = clock.tick(Timestamp::from_millis(20));
        assert_eq!((first.wall.as_millis(), first.counter), (10, 0));
        assert_eq!((second.wall.as_millis(), second.counter), (20, 0));
    }

    #[test]
    fn tick_is_monotonic_when_wall_clock_goes_back() {
        let mut clock = Clock::new(node("a"));
        let first = clock.tick(Timestamp::from_millis(20));
        let second = clock.tick(Timestamp::from_millis(10));
        assert!(second > first);
        assert_eq!((second.wall.as_millis(), second.counter), (20, 1));
    }

    #[test]
    fn tick_after_observe_is_later_than_observed() {
        let mut clock = Clock::new(node("a"));
        let seen = Hlc {
            wall: Timestamp::from_millis(100),
            counter: 3,
            node: node("z"),
        };
        clock.observe(&seen);
        assert!(clock.tick(Timestamp::from_millis(5)) > seen);
    }

    #[test]
    fn counter_overflow_advances_wall() {
        let mut clock = Clock::new(node("a"));
        clock.observe(&Hlc {
            wall: Timestamp::from_millis(7),
            counter: u32::MAX,
            node: node("a"),
        });
        let next = clock.tick(Timestamp::from_millis(0));
        assert_eq!((next.wall.as_millis(), next.counter), (8, 0));
    }
}
