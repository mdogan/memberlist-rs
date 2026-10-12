// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of delegate.go and the other *_delegate.go files
// from https://github.com/hashicorp/memberlist.

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;

use crate::error::DelegateError;
use crate::state::Node;

/// The interface that clients must implement if they want to hook into the
/// gossip layer of Memberlist. All the methods must be thread-safe, as they
/// can and generally will be called concurrently.
pub trait Delegate: Send + Sync {
    /// Used to retrieve meta-data about the current node when broadcasting
    /// an alive message. It's length is limited to the given byte size. This
    /// metadata is available in the Node structure.
    fn node_meta(&self, limit: usize) -> Vec<u8>;

    /// Called when a user-data message is received. Care should be taken
    /// that this method does not block, since doing so would block the
    /// entire UDP packet receive loop.
    fn notify_msg(&self, msg: &[u8]);

    /// Called when user data messages can be broadcast. It can return a list
    /// of buffers to send. Each buffer should assume an overhead as provided
    /// with a limit on the total byte size allowed. The total byte size of
    /// the resulting data to send must not exceed the limit. Care should be
    /// taken that this method does not block, since doing so would block the
    /// entire UDP packet receive loop.
    fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<Bytes>;

    /// Used for a TCP Push/Pull. This is sent to the remote side in addition
    /// to the membership information. Any data can be sent here. See
    /// `merge_remote_state` as well. The `join` boolean indicates this is
    /// for a join instead of a push/pull.
    fn local_state(&self, join: bool) -> Vec<u8>;

    /// Invoked after a TCP Push/Pull. This is the state received from the
    /// remote side and is the result of the remote side's `local_state`
    /// call. The `join` boolean indicates this is for a join instead of a
    /// push/pull.
    fn merge_remote_state(&self, buf: &[u8], join: bool);
}

/// A simpler delegate that is used only to receive notifications about
/// members joining and leaving. The methods in this delegate may be called
/// by multiple tasks, but never concurrently. This allows you to reason
/// about ordering.
pub trait EventDelegate: Send + Sync {
    /// Invoked when a node is detected to have joined.
    fn notify_join(&self, node: &Node);

    /// Invoked when a node is detected to have left.
    fn notify_leave(&self, node: &Node);

    /// Invoked when a node is detected to have updated, usually involving
    /// the meta data.
    fn notify_update(&self, node: &Node);
}

/// Used to enable an application to receive events about joins and leaves
/// over a channel instead of a direct function call.
///
/// The channel is unbounded, so sending an event never blocks. Care must be
/// taken that events are processed in a timely manner from the channel,
/// since unprocessed events use memory.
pub struct ChannelEventDelegate {
    pub ch: UnboundedSender<NodeEvent>,
}

/// The types of events that can be sent from the ChannelEventDelegate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeEventType {
    NodeJoin,
    NodeLeave,
    NodeUpdate,
}

/// A single event related to node activity in the memberlist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEvent {
    pub event: NodeEventType,
    pub node: Node,
}

impl ChannelEventDelegate {
    fn send(&self, event: NodeEventType, node: &Node) {
        // The receiver can be dropped at any time. There is nobody left to
        // tell about the event then.
        let _ = self.ch.send(NodeEvent {
            event,
            node: node.clone(),
        });
    }
}

impl EventDelegate for ChannelEventDelegate {
    fn notify_join(&self, node: &Node) {
        self.send(NodeEventType::NodeJoin, node);
    }

    fn notify_leave(&self, node: &Node) {
        self.send(NodeEventType::NodeLeave, node);
    }

    fn notify_update(&self, node: &Node) {
        self.send(NodeEventType::NodeUpdate, node);
    }
}

/// Used to involve a client in processing a node "alive" message. When a
/// node joins, either through a UDP gossip or TCP push/pull, we update the
/// state of that node via an alive message. This can be used to filter a
/// node out and prevent it from being considered a peer using application
/// specific logic.
pub trait AliveDelegate: Send + Sync {
    /// Invoked when a message about a live node is received from the
    /// network. Returning an error prevents the node from being considered
    /// a peer.
    fn notify_alive(&self, peer: &Node) -> Result<(), DelegateError>;
}

/// Used to inform a client that a node has attempted to join which would
/// result in a name conflict. This happens if two clients are configured
/// with the same name but different addresses.
pub trait ConflictDelegate: Send + Sync {
    /// Invoked when a name conflict is detected.
    fn notify_conflict(&self, existing: &Node, other: &Node);
}

/// Used to involve a client in a potential cluster merge operation. Namely,
/// when a node does a TCP push/pull (as part of a join), the delegate is
/// involved and allowed to cancel the join based on custom logic. The merge
/// delegate is NOT invoked as part of the push-pull anti-entropy.
pub trait MergeDelegate: Send + Sync {
    /// Invoked when a merge could take place. Provides a list of the nodes
    /// known by the peer. If the return value is an error, the merge is
    /// canceled.
    fn notify_merge(&self, peers: &[Node]) -> Result<(), DelegateError>;
}

/// Used to notify an observer how long it took for a ping message to
/// complete a round trip. It can also be used for writing arbitrary byte
/// slices into ack messages. Note that in order to be meaningful for RTT
/// estimates, this delegate does not apply to indirect pings, nor fallback
/// pings sent over TCP.
pub trait PingDelegate: Send + Sync {
    /// Invoked when an ack is being sent; the returned bytes will be
    /// appended to the ack.
    fn ack_payload(&self) -> Vec<u8>;

    /// Invoked when an ack for a ping is received.
    fn notify_ping_complete(&self, other: &Node, rtt: Duration, payload: &[u8]);
}
