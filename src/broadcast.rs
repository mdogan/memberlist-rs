// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of broadcast.go from https://github.com/hashicorp/memberlist.

//! The broadcast mechanism works by maintaining a sorted list of messages to
//! be sent out. When a message is to be broadcast, the retransmit count is
//! set to zero and appended to the queue. The retransmit count serves as the
//! "priority", ensuring that newer messages get sent first. Once a message
//! hits the retransmit limit, it is removed from the queue.
//!
//! Additionally, older entries can be invalidated by new messages that are
//! contradictory. For example, if we send "{suspect M1 inc: 1}", then a
//! following "{alive M1 inc: 2}" will invalidate that message.

use std::any::Any;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Notify;

use crate::codec::Message;
use crate::memberlist::Memberlist;
use crate::messages::{MessageType, USER_MSG_OVERHEAD, encode};
use crate::queue::Broadcast;

pub(crate) struct MemberlistBroadcast {
    pub(crate) node: String,
    pub(crate) msg: Bytes,
    pub(crate) notify: Option<Arc<Notify>>,
}

impl Broadcast for MemberlistBroadcast {
    fn invalidates(&self, other: &dyn Broadcast) -> bool {
        // Check if that broadcast is a memberlist type
        let other: &dyn Any = other;
        let Some(mb) = other.downcast_ref::<MemberlistBroadcast>() else {
            return false;
        };

        // Invalidates any message about the same node
        self.node == mb.node
    }

    fn name(&self) -> Option<&str> {
        Some(&self.node)
    }

    fn message(&self) -> Bytes {
        self.msg.clone()
    }

    fn finished(&self) {
        if let Some(notify) = &self.notify {
            notify.notify_one();
        }
    }
}

impl Memberlist {
    /// Encodes a message and enqueues it for broadcast.
    pub(crate) fn encode_and_broadcast<T: Message>(
        &self,
        node: &str,
        msg_type: MessageType,
        msg: &T,
    ) {
        self.encode_broadcast_notify(node, msg_type, msg, None);
    }

    /// Encodes a message and enqueues it for broadcast and notifies the
    /// given handle when transmission is finished.
    pub(crate) fn encode_broadcast_notify<T: Message>(
        &self,
        node: &str,
        msg_type: MessageType,
        msg: &T,
        notify: Option<Arc<Notify>>,
    ) {
        let buf = encode(msg_type, msg);
        self.queue_broadcast(node, buf.into(), notify);
    }

    /// Starts dissemination of a message. It will be sent up to a configured
    /// number of times. The message could potentially be invalidated by a
    /// future message about the same node.
    pub(crate) fn queue_broadcast(&self, node: &str, msg: Bytes, notify: Option<Arc<Notify>>) {
        let b = MemberlistBroadcast {
            node: node.to_string(),
            msg,
            notify,
        };
        self.broadcasts.queue_broadcast(Arc::new(b));
    }

    /// Returns a list of broadcasts to send up to a maximum byte size, while
    /// imposing a per-broadcast overhead. This is used to fill a UDP packet
    /// with piggybacked data.
    pub(crate) fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<Bytes> {
        // Get memberlist messages first
        let mut to_send = self.broadcasts.get_broadcasts(overhead, limit);

        // Check if the user has anything to broadcast
        if let Some(d) = &self.config.delegate {
            // Determine the bytes used already
            let bytes_used: usize = to_send.iter().map(|msg| msg.len() + overhead).sum();

            // Check space remaining for user messages
            let avail = limit.saturating_sub(bytes_used);
            if avail > overhead + USER_MSG_OVERHEAD {
                let user_msgs = d.get_broadcasts(overhead + USER_MSG_OVERHEAD, avail);

                // Frame each user message
                for msg in user_msgs {
                    let mut buf = Vec::with_capacity(msg.len() + 1);
                    buf.push(MessageType::USER.0);
                    buf.extend_from_slice(&msg);
                    to_send.push(buf.into());
                }
            }
        }
        to_send
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broadcast(node: &str, msg: &'static [u8]) -> MemberlistBroadcast {
        MemberlistBroadcast {
            node: node.to_string(),
            msg: Bytes::from_static(msg),
            notify: None,
        }
    }

    #[test]
    fn test_memberlist_broadcast_invalidates() {
        let m1 = broadcast("test", b"");
        let m2 = broadcast("foo", b"");

        assert!(
            !m1.invalidates(&m2) && !m2.invalidates(&m1),
            "unexpected invalidation"
        );

        assert!(m1.invalidates(&m1), "expected invalidation");
    }

    #[test]
    fn test_memberlist_broadcast_message() {
        let m1 = broadcast("test", b"test");
        let msg = m1.message();
        assert_eq!(&msg[..], b"test", "messages do not match");
    }
}
