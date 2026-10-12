// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of net.go from https://github.com/hashicorp/memberlist.

//! The messages that members send to each other, and the constants of the
//! wire protocol.

use std::time::Duration;

use crate::codec::{Message, message};
use crate::state::NodeStateType;

/// The minimum protocol version that we can understand.
pub const PROTOCOL_VERSION_MIN: u8 = 1;

/// Version 3 added support for TCP pings but we kept the default
/// protocol version at 2 to ease transition to this new feature.
/// A memberlist speaking version 2 of the protocol will attempt
/// to TCP ping another memberlist who understands version 3 or
/// greater.
///
/// Version 4 added support for nacks as part of indirect probes.
/// A memberlist speaking version 2 of the protocol will expect
/// nacks from another memberlist who understands version 4 or
/// greater, and likewise nacks will be sent to memberlists who
/// understand version 4 or greater.
pub const PROTOCOL_VERSION_2_COMPATIBLE: u8 = 2;

/// The maximum protocol version that we can understand.
pub const PROTOCOL_VERSION_MAX: u8 = 5;

/// The integer ID of a type of message that can be received on network
/// channels from other members.
///
/// WARNING: ONLY APPEND TO THIS LIST! The numeric values are part of the
/// protocol itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MessageType(pub(crate) u8);

impl MessageType {
    pub(crate) const PING: MessageType = MessageType(0);
    pub(crate) const INDIRECT_PING: MessageType = MessageType(1);
    pub(crate) const ACK_RESP: MessageType = MessageType(2);
    pub(crate) const SUSPECT: MessageType = MessageType(3);
    pub(crate) const ALIVE: MessageType = MessageType(4);
    pub(crate) const DEAD: MessageType = MessageType(5);
    pub(crate) const PUSH_PULL: MessageType = MessageType(6);
    pub(crate) const COMPOUND: MessageType = MessageType(7);
    /// User message, not handled by us.
    pub(crate) const USER: MessageType = MessageType(8);
    pub(crate) const COMPRESS: MessageType = MessageType(9);
    /// Encryption is not supported by this port. The value is kept so that we
    /// can tell an encrypted stream apart and reject it with a clear error.
    pub(crate) const ENCRYPT: MessageType = MessageType(10);
    pub(crate) const NACK_RESP: MessageType = MessageType(11);
    pub(crate) const HAS_CRC: MessageType = MessageType(12);
    pub(crate) const ERR: MessageType = MessageType(13);

    /// Has a deliberately high value so that you can disambiguate it from the
    /// encryption version header, which is either 0 or 1 right now, and also
    /// from any of the existing message types.
    pub(crate) const HAS_LABEL: MessageType = MessageType(244);
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The compression algorithm of a compressed message. LZW is the only one.
pub(crate) const LZW_ALGO: u8 = 0;

/// Maximum size for node meta data.
pub const META_MAX_SIZE: usize = 512;

/// Assumed header overhead.
pub(crate) const COMPOUND_HEADER_OVERHEAD: usize = 2;
/// Assumed overhead per entry in a compound header.
pub(crate) const COMPOUND_OVERHEAD: usize = 2;
pub(crate) const USER_MSG_OVERHEAD: usize = 1;
/// Warn if a UDP packet takes this long to process.
#[allow(dead_code)]
pub(crate) const BLOCKING_WARNING: Duration = Duration::from_millis(10);
pub(crate) const MAX_PUSH_STATE_BYTES: usize = 20 * 1024 * 1024;
/// Each node requires conservatively ~20 bytes when encoded.
pub(crate) const MAX_PUSH_STATE_NODES: usize = 1024 * 1024;
/// Largest user message we will buffer off the wire.
pub(crate) const MAX_USER_MSG_BYTES: usize = 20 * 1024 * 1024;
/// Maximum number of concurrent push/pull requests.
pub(crate) const MAX_PUSH_PULL_REQUESTS: u32 = 128;
/// Largest push/pull we will decompress: user state plus an equal node budget.
pub(crate) const MAX_DECOMPRESSED_BYTES: usize = 2 * MAX_PUSH_STATE_BYTES;

message! {
    /// Ping request sent directly to a node.
    pub(crate) struct Ping {
        /// Node is sent so the target can verify they are the intended
        /// recipient. This is to protect against an agent restart with a new
        /// name.
        pub(crate) node: String = "Node";
        pub(crate) seq_no: u32 = "SeqNo";
        /// Source address, used for a direct reply.
        pub(crate) source_addr: Vec<u8> = "SourceAddr", omitempty;
        /// Source name, used for a direct reply.
        pub(crate) source_node: String = "SourceNode", omitempty;
        /// Source port, used for a direct reply.
        pub(crate) source_port: u16 = "SourcePort", omitempty;
    }
}

message! {
    /// Indirect ping sent to an indirect node.
    pub(crate) struct IndirectPingReq {
        /// True if we'd like a nack back.
        pub(crate) nack: bool = "Nack";
        /// Node is sent so the target can verify they are the intended
        /// recipient. This is to protect against an agent restart with a new
        /// name.
        pub(crate) node: String = "Node";
        pub(crate) port: u16 = "Port";
        pub(crate) seq_no: u32 = "SeqNo";
        /// Source address, used for a direct reply.
        pub(crate) source_addr: Vec<u8> = "SourceAddr", omitempty;
        /// Source name, used for a direct reply.
        pub(crate) source_node: String = "SourceNode", omitempty;
        /// Source port, used for a direct reply.
        pub(crate) source_port: u16 = "SourcePort", omitempty;
        pub(crate) target: Vec<u8> = "Target";
    }
}

message! {
    /// Ack response is sent for a ping.
    pub(crate) struct AckResp {
        pub(crate) payload: Vec<u8> = "Payload";
        pub(crate) seq_no: u32 = "SeqNo";
    }
}

message! {
    /// Nack response is sent for an indirect ping when the pinger doesn't
    /// hear from the ping-ee within the configured timeout. This lets the
    /// original node know that the indirect ping attempt happened but didn't
    /// succeed.
    pub(crate) struct NackResp {
        pub(crate) seq_no: u32 = "SeqNo";
    }
}

message! {
    /// Err response is sent to relay the error from the remote end.
    pub(crate) struct ErrResp {
        pub(crate) error: String = "Error";
    }
}

message! {
    /// Suspect is broadcast when we suspect a node is dead.
    pub(crate) struct Suspect {
        /// Include who is suspecting.
        pub(crate) from: String = "From";
        pub(crate) incarnation: u32 = "Incarnation";
        pub(crate) node: String = "Node";
    }
}

message! {
    /// Alive is broadcast when we know a node is alive.
    /// Overloaded for nodes joining.
    pub(crate) struct Alive {
        pub(crate) addr: Vec<u8> = "Addr";
        pub(crate) incarnation: u32 = "Incarnation";
        pub(crate) meta: Vec<u8> = "Meta";
        pub(crate) node: String = "Node";
        pub(crate) port: u16 = "Port";
        /// The versions of the protocol/delegate that are being spoken,
        /// order: pmin, pmax, pcur, dmin, dmax, dcur.
        pub(crate) vsn: Vec<u8> = "Vsn";
    }
}

message! {
    /// Dead is broadcast when we confirm a node is dead.
    /// Overloaded for nodes leaving.
    pub(crate) struct Dead {
        /// Include who is suspecting.
        pub(crate) from: String = "From";
        pub(crate) incarnation: u32 = "Incarnation";
        pub(crate) node: String = "Node";
    }
}

message! {
    /// Informs the other side how many states we are transferring.
    pub(crate) struct PushPullHeader {
        /// Is this a join request or an anti-entropy run.
        pub(crate) join: bool = "Join";
        pub(crate) nodes: i64 = "Nodes";
        /// Encodes the byte length of user state.
        pub(crate) user_state_len: i64 = "UserStateLen";
    }
}

message! {
    /// Encapsulates a user message that is sent over a stream.
    pub(crate) struct UserMsgHeader {
        /// Encodes the byte length of the user message.
        pub(crate) user_msg_len: i64 = "UserMsgLen";
    }
}

message! {
    /// Used for push/pull requests when we are transferring out node states.
    pub(crate) struct PushNodeState {
        pub(crate) addr: Vec<u8> = "Addr";
        pub(crate) incarnation: u32 = "Incarnation";
        pub(crate) meta: Vec<u8> = "Meta";
        pub(crate) name: String = "Name";
        pub(crate) port: u16 = "Port";
        /// The numeric value of a [`NodeStateType`]. It stays a raw integer
        /// so that a value we do not know cannot fail the whole state sync.
        pub(crate) state: i64 = "State";
        /// Protocol versions.
        pub(crate) vsn: Vec<u8> = "Vsn";
    }
}

impl PushNodeState {
    /// The node state, or `None` if the remote sent a value we do not know.
    pub(crate) fn node_state(&self) -> Option<NodeStateType> {
        NodeStateType::from_wire(self.state)
    }
}

message! {
    /// Wraps an underlying payload using a specified compression algorithm.
    pub(crate) struct Compress {
        pub(crate) algo: u8 = "Algo";
        pub(crate) buf: Vec<u8> = "Buf";
    }
}

/// Writes the message type byte followed by the encoded message.
pub(crate) fn encode<T: Message>(msg_type: MessageType, msg: &T) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(msg_type.0);
    msg.encode_to(&mut out);
    out
}
