// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of queue.go from https://github.com/hashicorp/memberlist.

use std::any::Any;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::util::retransmit_limit;

/// Something that can be broadcasted via gossip to the memberlist cluster.
pub trait Broadcast: Any + Send + Sync {
    /// Checks if enqueuing the current broadcast invalidates a previous
    /// broadcast.
    fn invalidates(&self, other: &dyn Broadcast) -> bool;

    /// Returns a byte form of the message.
    fn message(&self) -> Bytes;

    /// Invoked when the message will no longer be broadcast, either due to
    /// invalidation or to the transmit limit being reached.
    fn finished(&self);

    /// An optional unique name for this broadcast message. A broadcast that
    /// returns `Some` here is a named broadcast: a new broadcast replaces the
    /// queued one with the same name, without scanning the queue.
    ///
    /// You should ensure that `invalidates` checks the same uniqueness, by
    /// comparing the names of the two broadcasts. `invalidates` isn't
    /// currently used for named broadcasts, but that may change in the
    /// future.
    fn name(&self) -> Option<&str> {
        None
    }

    /// Returns true if each message is intrinsically unique, so there is no
    /// need to scan the broadcast queue for duplicates.
    ///
    /// You should ensure that `invalidates` always returns false if this
    /// returns true. `invalidates` isn't currently used for unique
    /// broadcasts, but that may change in the future.
    fn unique_broadcast(&self) -> bool {
        false
    }
}

/// The position of a broadcast in the queue.
///
/// default ordering is
/// - [transmits=0, ..., transmits=inf]
/// - [transmits=0:len=999, ..., transmits=0:len=2, ...]
/// - [transmits=0:len=999,id=999, ..., transmits=0:len=999:id=1, ...]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct QueueKey {
    /// Number of transmissions attempted.
    pub(crate) transmits: usize,
    /// Copied from the length of the message.
    pub(crate) msg_len: Reverse<i64>,
    /// Unique incrementing id stamped at submission time.
    pub(crate) id: Reverse<i64>,
}

impl QueueKey {
    pub(crate) fn new(transmits: usize, msg_len: i64, id: i64) -> Self {
        QueueKey {
            transmits,
            msg_len: Reverse(msg_len),
            id: Reverse(id),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LimitedBroadcast {
    pub(crate) key: QueueKey,
    pub(crate) b: Arc<dyn Broadcast>,
    /// Set if the broadcast is a named broadcast.
    name: String,
}

#[derive(Default)]
struct QueueState {
    tq: BTreeMap<QueueKey, LimitedBroadcast>,
    tm: HashMap<String, QueueKey>,
    id_gen: i64,
}

impl QueueState {
    /// Removes the given item from the overall datastructure.
    fn delete_item(&mut self, key: &QueueKey) -> Option<LimitedBroadcast> {
        let cur = self.tq.remove(key)?;
        if !cur.name.is_empty() {
            self.tm.remove(&cur.name);
        }

        if self.tq.is_empty() {
            // At idle there's no reason to let the id generator keep going
            // indefinitely.
            self.id_gen = 0;
        }
        Some(cur)
    }

    /// Adds the given item into the overall datastructure.
    fn add_item(&mut self, cur: LimitedBroadcast) {
        if !cur.name.is_empty() {
            self.tm.insert(cur.name.clone(), cur.key);
        }
        self.tq.insert(cur.key, cur);
    }

    /// Returns a pair of min/max values for transmit values represented by
    /// the current queue contents.
    fn get_transmit_range(&self) -> (usize, usize) {
        match (self.tq.first_key_value(), self.tq.last_key_value()) {
            (Some((min, _)), Some((max, _))) => (min.transmits, max.transmits),
            _ => (0, 0),
        }
    }
}

/// Used to queue messages to broadcast to the cluster (via gossip) but
/// limits the number of transmits per message. It also prioritizes messages
/// with lower transmit counts (hence newer messages).
pub struct TransmitLimitedQueue {
    /// Returns the number of nodes in the cluster. This is used to determine
    /// the retransmit count, which is calculated based on the log of this.
    pub num_nodes: Box<dyn Fn() -> usize + Send + Sync>,

    /// The multiplier used to determine the maximum number of
    /// retransmissions attempted.
    pub retransmit_mult: usize,

    state: Mutex<QueueState>,
}

impl TransmitLimitedQueue {
    /// Creates an empty queue.
    pub fn new(
        retransmit_mult: usize,
        num_nodes: impl Fn() -> usize + Send + Sync + 'static,
    ) -> Self {
        TransmitLimitedQueue {
            num_nodes: Box::new(num_nodes),
            retransmit_mult,
            state: Mutex::new(QueueState::default()),
        }
    }

    /// Enqueues a broadcast.
    pub fn queue_broadcast(&self, b: Arc<dyn Broadcast>) {
        self.queue_broadcast_with_transmits(b, 0);
    }

    /// Like `queue_broadcast` but you can use a nonzero value for the
    /// initial transmit tier assigned to the message. This is meant to be
    /// used for unit testing.
    pub(crate) fn queue_broadcast_with_transmits(
        &self,
        b: Arc<dyn Broadcast>,
        initial_transmits: usize,
    ) {
        let mut q = self.state.lock();

        if q.id_gen == i64::MAX {
            // it's super duper unlikely to wrap around within the retransmit limit
            q.id_gen = 1;
        } else {
            q.id_gen += 1;
        }
        let id = q.id_gen;

        let named = b.name().is_some();
        let name = b.name().unwrap_or_default().to_string();
        let unique = !named && b.unique_broadcast();
        let key = QueueKey::new(initial_transmits, b.message().len() as i64, id);

        // Check if this message invalidates another.
        if !name.is_empty() {
            if let Some(old_key) = q.tm.get(&name).copied()
                && let Some(old) = q.delete_item(&old_key)
            {
                old.b.finished();
            }
        } else if !unique {
            // Slow path, hopefully nothing hot hits this.
            let remove: Vec<QueueKey> =
                q.tq.values()
                    // Special Broadcasts can only invalidate each other.
                    .filter(|cur| cur.b.name().is_none() && !cur.b.unique_broadcast())
                    .filter(|cur| b.invalidates(cur.b.as_ref()))
                    .map(|cur| cur.key)
                    .collect();
            for key in remove {
                if let Some(cur) = q.delete_item(&key) {
                    cur.b.finished();
                }
            }
        }

        // Append to the relevant queue.
        q.add_item(LimitedBroadcast { key, b, name });
    }

    /// Gets a number of broadcasts, up to a byte limit and applying a
    /// per-message overhead as provided.
    pub fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<Bytes> {
        let mut q = self.state.lock();

        // Fast path the default case
        if q.tq.is_empty() {
            return Vec::new();
        }

        let transmit_limit = retransmit_limit(self.retransmit_mult, (self.num_nodes)());

        let mut bytes_used = 0usize;
        let mut to_send = Vec::new();
        let mut reinsert = Vec::new();

        // Visit fresher items first, but only look at stuff that will fit.
        // We'll go tier by tier, grabbing the largest items first.
        let (min_tr, max_tr) = q.get_transmit_range();
        let mut transmits = min_tr;
        while transmits <= max_tr {
            let free = limit as i64 - bytes_used as i64 - overhead as i64;
            if free <= 0 {
                break; // bail out early
            }

            // Search for the least element on a given tier (by transmit
            // count) as defined in the QueueKey ordering that will fit into
            // our remaining space.
            let greater_or_equal = QueueKey::new(transmits, free, i64::MAX);
            let less_than = QueueKey::new(transmits + 1, i64::MAX, i64::MAX);
            let keep =
                q.tq.range(greater_or_equal..less_than)
                    // If a message is larger than its key says, it's a bug in
                    // the datastructure or surrounding use doing something like
                    // having the message length change over time. There's enough
                    // going on here that it's probably sane to just skip it and
                    // move on for now.
                    .find(|(_, cur)| cur.b.message().len() as i64 <= free)
                    .map(|(key, _)| *key);
            let Some(keep) = keep else {
                // No more items of an appropriate size in the tier.
                transmits += 1;
                continue;
            };

            // Check if we should stop transmission
            let Some(mut keep) = q.delete_item(&keep) else {
                continue;
            };
            let msg = keep.b.message();

            // Add to slice to send
            bytes_used += overhead + msg.len();
            to_send.push(msg);

            if keep.key.transmits + 1 >= transmit_limit {
                keep.b.finished();
            } else {
                // We need to bump this item down to another transmit tier,
                // but because it would be in the same direction that we're
                // walking the tiers, we will have to delay the reinsertion
                // until we are finished our search. Otherwise we'll possibly
                // re-add the message when we ascend to the next tier.
                keep.key.transmits += 1;
                reinsert.push(keep);
            }
        }

        for cur in reinsert {
            q.add_item(cur);
        }

        to_send
    }

    /// Returns the number of queued messages.
    pub fn num_queued(&self) -> usize {
        self.state.lock().tq.len()
    }

    /// Clears all the queued messages. Should only be used for tests.
    pub fn reset(&self) {
        let mut q = self.state.lock();

        for cur in q.tq.values() {
            cur.b.finished();
        }

        q.tq.clear();
        q.tm.clear();
        q.id_gen = 0;
    }

    /// Retains the `max_retain` latest messages, and the rest will be
    /// discarded. This can be used to prevent unbounded queue sizes.
    pub fn prune(&self, max_retain: usize) {
        let mut q = self.state.lock();

        // Do nothing if queue size is less than the limit
        while q.tq.len() > max_retain {
            let Some(key) = q.tq.last_key_value().map(|(key, _)| *key) else {
                break;
            };
            if let Some(cur) = q.delete_item(&key) {
                cur.b.finished();
            }
        }
    }

    /// Returns the queued items. They are in natural order (fewest transmits
    /// first) when `reverse` is false, and in the opposite order when it is
    /// true. For testing only.
    #[cfg(test)]
    pub(crate) fn ordered_view(&self, reverse: bool) -> Vec<LimitedBroadcast> {
        let q = self.state.lock();
        let mut out: Vec<LimitedBroadcast> = q.tq.values().cloned().collect();
        if reverse {
            out.reverse();
        }
        out
    }

    #[cfg(test)]
    pub(crate) fn id_gen(&self) -> i64 {
        self.state.lock().id_gen
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::Notify;

    use super::*;
    use crate::broadcast::MemberlistBroadcast;

    fn broadcast(node: &str, msg: &str, notify: Option<Arc<Notify>>) -> Arc<dyn Broadcast> {
        Arc::new(MemberlistBroadcast {
            node: node.to_string(),
            msg: Bytes::copy_from_slice(msg.as_bytes()),
            notify,
        })
    }

    fn node_of(lb: &LimitedBroadcast) -> String {
        let any: &dyn Any = lb.b.as_ref();
        any.downcast_ref::<MemberlistBroadcast>()
            .expect("not a memberlist broadcast")
            .node
            .clone()
    }

    #[test]
    fn test_limited_broadcast_less() {
        struct Case {
            name: &'static str,
            a: QueueKey, // lesser
            b: QueueKey,
        }
        let cases = [
            Case {
                name: "diff-transmits",
                a: QueueKey::new(0, 10, 100),
                b: QueueKey::new(1, 10, 100),
            },
            Case {
                name: "same-transmits--diff-len",
                a: QueueKey::new(0, 12, 100),
                b: QueueKey::new(0, 10, 100),
            },
            Case {
                name: "same-transmits--same-len--diff-id",
                a: QueueKey::new(0, 12, 100),
                b: QueueKey::new(0, 12, 90),
            },
        ];

        for c in cases {
            assert!(c.a < c.b, "{}", c.name);

            let mut tree = BTreeMap::new();
            tree.insert(c.b, ());
            tree.insert(c.a, ());

            let (min, _) = tree.first_key_value().unwrap();
            assert_eq!(&c.a, min, "{}", c.name);

            let (max, _) = tree.last_key_value().unwrap();
            assert_eq!(&c.b, max, "{}", c.name);
        }
    }

    #[test]
    fn test_transmit_limited_queue() {
        let q = TransmitLimitedQueue::new(1, || 1);
        q.queue_broadcast(broadcast("test", "", None));
        q.queue_broadcast(broadcast("foo", "", None));
        q.queue_broadcast(broadcast("bar", "", None));

        assert_eq!(q.num_queued(), 3, "bad len");
        let dump = q.ordered_view(true);
        assert_eq!(node_of(&dump[0]), "test", "missing test");
        assert_eq!(node_of(&dump[1]), "foo", "missing foo");
        assert_eq!(node_of(&dump[2]), "bar", "missing bar");

        // Should invalidate previous message
        q.queue_broadcast(broadcast("test", "", None));

        assert_eq!(q.num_queued(), 3, "bad len");
        let dump = q.ordered_view(true);
        assert_eq!(node_of(&dump[0]), "foo", "missing foo");
        assert_eq!(node_of(&dump[1]), "bar", "missing bar");
        assert_eq!(node_of(&dump[2]), "test", "missing test");
    }

    #[test]
    fn test_transmit_limited_get_broadcasts() {
        let q = TransmitLimitedQueue::new(3, || 10);

        // 18 bytes per message
        q.queue_broadcast(broadcast("test", "1. this is a test.", None));
        q.queue_broadcast(broadcast("foo", "2. this is a test.", None));
        q.queue_broadcast(broadcast("bar", "3. this is a test.", None));
        q.queue_broadcast(broadcast("baz", "4. this is a test.", None));

        // 2 byte overhead per message, should get all 4 messages
        let all = q.get_broadcasts(2, 80);
        assert_eq!(all.len(), 4, "missing messages: {all:?}");

        // 3 byte overhead, should only get 3 messages back
        let partial = q.get_broadcasts(3, 80);
        assert_eq!(partial.len(), 3, "missing messages: {partial:?}");
    }

    #[test]
    fn test_transmit_limited_get_broadcasts_limit() {
        let q = TransmitLimitedQueue::new(1, || 10);

        assert_eq!(q.id_gen(), 0, "the id generator seed starts at zero");
        assert_eq!(
            retransmit_limit(q.retransmit_mult, (q.num_nodes)()),
            2,
            "sanity check transmit limits"
        );

        // 18 bytes per message
        q.queue_broadcast(broadcast("test", "1. this is a test.", None));
        q.queue_broadcast(broadcast("foo", "2. this is a test.", None));
        q.queue_broadcast(broadcast("bar", "3. this is a test.", None));
        q.queue_broadcast(broadcast("baz", "4. this is a test.", None));

        assert_eq!(q.id_gen(), 4, "we handed out 4 IDs");

        // 3 byte overhead, should only get 3 messages back
        let partial1 = q.get_broadcasts(3, 80);
        assert_eq!(partial1.len(), 3, "missing messages: {partial1:?}");

        assert_eq!(q.id_gen(), 4, "id generator doesn't reset until empty");

        let partial2 = q.get_broadcasts(3, 80);
        assert_eq!(partial2.len(), 3, "missing messages: {partial2:?}");

        assert_eq!(q.id_gen(), 4, "id generator doesn't reset until empty");

        // Only two not expired
        let partial3 = q.get_broadcasts(3, 80);
        assert_eq!(partial3.len(), 2, "missing messages: {partial3:?}");

        assert_eq!(q.id_gen(), 0, "id generator resets on empty");

        // Should get nothing
        let partial5 = q.get_broadcasts(3, 80);
        assert_eq!(partial5.len(), 0, "missing messages: {partial5:?}");

        assert_eq!(q.id_gen(), 0, "id generator resets on empty");
    }

    #[tokio::test]
    async fn test_transmit_limited_prune() {
        let q = TransmitLimitedQueue::new(1, || 10);

        let ch1 = Arc::new(Notify::new());
        let ch2 = Arc::new(Notify::new());

        // 18 bytes per message
        q.queue_broadcast(broadcast("test", "1. this is a test.", Some(ch1.clone())));
        q.queue_broadcast(broadcast("foo", "2. this is a test.", Some(ch2.clone())));
        q.queue_broadcast(broadcast("bar", "3. this is a test.", None));
        q.queue_broadcast(broadcast("baz", "4. this is a test.", None));

        // Keep only 2
        q.prune(2);

        assert_eq!(q.num_queued(), 2);

        // Should notify the first two
        tokio::time::timeout(Duration::from_millis(10), ch1.notified())
            .await
            .expect("expected invalidation");
        tokio::time::timeout(Duration::from_millis(10), ch2.notified())
            .await
            .expect("expected invalidation");

        let dump = q.ordered_view(true);

        assert_eq!(node_of(&dump[0]), "bar", "missing bar");
        assert_eq!(node_of(&dump[1]), "baz", "missing baz");
    }

    #[test]
    fn test_transmit_limited_ordering() {
        let q = TransmitLimitedQueue::new(1, || 10);

        let insert = |name: &str, transmits: usize| {
            q.queue_broadcast_with_transmits(
                broadcast(name, name, Some(Arc::new(Notify::new()))),
                transmits,
            );
        };

        insert("node0", 0);
        insert("node1", 10);
        insert("node2", 3);
        insert("node3", 4);
        insert("node4", 7);

        let dump = q.ordered_view(true);

        let transmits: Vec<usize> = dump.iter().map(|lb| lb.key.transmits).collect();
        assert_eq!(transmits, vec![10, 7, 4, 3, 0]);
    }
}
