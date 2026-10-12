// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of memberlist.go from https://github.com/hashicorp/memberlist.

//! memberlist is a library that manages cluster membership and member
//! failure detection using a gossip based protocol.
//!
//! The use cases for such a library are far-reaching: all distributed
//! systems require membership, and memberlist is a re-usable solution to
//! managing cluster membership and node failure detection.
//!
//! memberlist is eventually consistent but converges quickly on average.
//! The speed at which it converges can be heavily tuned via various knobs on
//! the protocol. Node failures are detected and network partitions are
//! partially tolerated by attempting to communicate to potentially dead
//! nodes through multiple routes.
//!
//! This is a Rust port of the Go library `hashicorp/memberlist`. It speaks
//! the same wire protocol, so Rust and Go members can be in the same
//! cluster. Encryption is not supported.
//!
//! # Example
//!
//! ```no_run
//! use memberlist::{Config, Memberlist};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), memberlist::Error> {
//!     // Create the initial memberlist from a safe configuration.
//!     let list = Memberlist::create(Config::default_local()).await?;
//!
//!     // Join an existing cluster by specifying at least one known member.
//!     let n = list.join(&["1.2.3.4"]).await?;
//!     println!("joined {n} members");
//!
//!     // Ask for members of the cluster
//!     for member in list.members() {
//!         println!("Member: {} {}", member.name, member.addr);
//!     }
//!
//!     // Continue doing whatever you need, memberlist will maintain
//!     // membership information in the background. Delegates can be used
//!     // for receiving events when members join or leave.
//!     Ok(())
//! }
//! ```

mod awareness;
mod broadcast;
mod codec;
mod config;
mod conn;
mod delegate;
mod dns;
mod error;
mod label;
mod logging;
mod memberlist;
mod messages;
mod mock_transport;
mod net;
mod net_transport;
mod queue;
mod sockaddr;
mod state;
mod suspicion;
mod timer;
mod transport;
mod util;

#[cfg(test)]
mod golden_test;
#[cfg(test)]
mod label_test;
#[cfg(test)]
mod memberlist_test;
#[cfg(test)]
mod net_test;
#[cfg(test)]
mod state_test;
#[cfg(test)]
mod testutil;
#[cfg(test)]
mod transport_test;
#[cfg(test)]
mod util_test;

pub use crate::config::{Config, parse_cidrs};
pub use crate::delegate::{
    AliveDelegate, ChannelEventDelegate, ConflictDelegate, Delegate, EventDelegate, MergeDelegate,
    NodeEvent, NodeEventType, PingDelegate,
};
pub use crate::error::{DelegateError, Error, Result};
pub use crate::label::{
    LABEL_MAX_SIZE, add_label_header_to_packet, add_label_header_to_stream,
    remove_label_header_from_packet, remove_label_header_from_stream,
};
pub use crate::logging::{log_address, log_conn, log_string_address};
pub use crate::memberlist::Memberlist;
pub use crate::messages::{
    META_MAX_SIZE, PROTOCOL_VERSION_2_COMPATIBLE, PROTOCOL_VERSION_MAX, PROTOCOL_VERSION_MIN,
};
pub use crate::mock_transport::{MockNetwork, MockTransport};
pub use crate::net_transport::{NetTransport, NetTransportConfig};
pub use crate::queue::{Broadcast, TransmitLimitedQueue};
pub use crate::state::{Node, NodeStateType};
pub use crate::transport::{Address, Connection, Packet, Stream, Transport};
