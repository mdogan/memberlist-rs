// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of transport.go from https://github.com/hashicorp/memberlist.

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{Error, Result};
use crate::label::{add_label_header_to_packet, add_label_header_to_stream};

/// Provides some metadata about incoming packets from peers over a packet
/// connection, as well as the packet payload.
#[derive(Debug, Clone)]
pub struct Packet {
    /// The raw contents of the packet.
    pub buf: Bytes,

    /// The address of the peer.
    pub from: SocketAddr,

    /// The time when the packet was received. This should be taken as close
    /// as possible to the actual receipt time to help make an accurate RTT
    /// measurement during probes.
    pub timestamp: Instant,
}

/// A reliable two-way connection to a peer, for example a TCP connection.
pub trait Connection: AsyncRead + AsyncWrite + Send + Unpin + 'static {
    /// The address of the other end of the connection, if it has one.
    fn remote_addr(&self) -> Option<SocketAddr> {
        None
    }
}

impl Connection for tokio::net::TcpStream {
    fn remote_addr(&self) -> Option<SocketAddr> {
        self.peer_addr().ok()
    }
}

impl Connection for tokio::io::DuplexStream {}

/// An owned stream connection, as it is handed out by a transport.
pub type Stream = Box<dyn Connection>;

impl Connection for Stream {
    fn remote_addr(&self) -> Option<SocketAddr> {
        (**self).remote_addr()
    }
}

/// The address of a node, optionally with its name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Address {
    /// A network address as a string, similar to Dial. This usually is in
    /// the form of "host:port". This is required.
    pub addr: String,

    /// The name of the node being addressed. This is optional but transports
    /// may require it.
    pub name: String,
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.name.is_empty() {
            return write!(f, "{} ({})", self.name, self.addr);
        }
        write!(f, "{}", self.addr)
    }
}

/// Used to abstract over communicating with other peers. The packet
/// interface is assumed to be best-effort and the stream interface is
/// assumed to be reliable.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Given the user's configured values (which might be empty), returns
    /// the desired IP and port to advertise to the rest of the cluster.
    fn final_advertise_addr(&self, ip: &str, port: u16) -> Result<(IpAddr, u16)>;

    /// A packet-oriented interface that fires off the given payload to the
    /// given address in a connectionless fashion. This should return a time
    /// stamp that's as close as possible to when the packet was transmitted
    /// to help make accurate RTT measurements during probes.
    ///
    /// We treat the address here as a string, similar to Dial, so it's
    /// network neutral, so this usually is in the form of "host:port".
    async fn write_to(&self, b: &[u8], addr: &str) -> Result<Instant>;

    /// Like `write_to`, but the transport also gets the name of the node
    /// being addressed. A transport that routes by node name should
    /// override this. The default ignores the name.
    async fn write_to_address(&self, b: &[u8], addr: &Address) -> Result<Instant> {
        self.write_to(b, &addr.addr).await
    }

    /// Waits for the next incoming packet from other peers. How this is set
    /// up for listening is left as an exercise for the concrete transport
    /// implementations. Returns `None` once the transport is shut down.
    async fn recv_packet(&self) -> Option<Packet>;

    /// Creates a connection that allows us to perform two-way communication
    /// with a peer. This is generally more expensive than packet connections
    /// so is used for more infrequent operations such as anti-entropy or
    /// fallback probes if the packet-oriented probe failed. A zero timeout
    /// means no timeout.
    async fn dial_timeout(&self, addr: &str, timeout: Duration) -> Result<Stream>;

    /// Like `dial_timeout`, but the transport also gets the name of the node
    /// being addressed. A transport that routes by node name should
    /// override this. The default ignores the name.
    async fn dial_address_timeout(&self, addr: &Address, timeout: Duration) -> Result<Stream> {
        self.dial_timeout(&addr.addr, timeout).await
    }

    /// Waits for the next incoming stream connection from other peers. How
    /// this is set up for listening is left as an exercise for the concrete
    /// transport implementations. Returns `None` once the transport is shut
    /// down.
    async fn accept_stream(&self) -> Option<Stream>;

    /// Called when memberlist is shutting down; this gives the transport a
    /// chance to clean up any listeners.
    async fn shutdown(&self) -> Result<()>;
}

/// Wraps a transport and puts the label header in front of every packet and
/// every stream that we send.
pub(crate) struct LabelWrappedTransport {
    pub(crate) label: String,
    pub(crate) transport: Arc<dyn Transport>,
}

#[async_trait]
impl Transport for LabelWrappedTransport {
    fn final_advertise_addr(&self, ip: &str, port: u16) -> Result<(IpAddr, u16)> {
        self.transport.final_advertise_addr(ip, port)
    }

    async fn write_to(&self, buf: &[u8], addr: &str) -> Result<Instant> {
        let buf = add_label_header_to_packet(buf, &self.label)?;
        self.transport.write_to(&buf, addr).await
    }

    async fn write_to_address(&self, buf: &[u8], addr: &Address) -> Result<Instant> {
        let buf = add_label_header_to_packet(buf, &self.label)
            .map_err(|err| Error::msg(format!("failed to add label header to packet: {err}")))?;
        self.transport.write_to_address(&buf, addr).await
    }

    async fn recv_packet(&self) -> Option<Packet> {
        self.transport.recv_packet().await
    }

    async fn dial_timeout(&self, addr: &str, timeout: Duration) -> Result<Stream> {
        let mut conn = self.transport.dial_timeout(addr, timeout).await?;
        add_label_header_to_stream(&mut conn, &self.label)
            .await
            .map_err(|err| Error::msg(format!("failed to add label header to stream: {err}")))?;
        Ok(conn)
    }

    async fn dial_address_timeout(&self, addr: &Address, timeout: Duration) -> Result<Stream> {
        let mut conn = self.transport.dial_address_timeout(addr, timeout).await?;
        add_label_header_to_stream(&mut conn, &self.label)
            .await
            .map_err(|err| Error::msg(format!("failed to add label header to stream: {err}")))?;
        Ok(conn)
    }

    async fn accept_stream(&self) -> Option<Stream> {
        self.transport.accept_stream().await
    }

    async fn shutdown(&self) -> Result<()> {
        self.transport.shutdown().await
    }
}
