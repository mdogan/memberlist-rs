// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of mock_transport.go from https://github.com/hashicorp/memberlist.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::transport::{Address, Packet, Stream, Transport};

/// The size of the in-memory buffer of a mock stream connection.
const MOCK_STREAM_BUF_SIZE: usize = 64 * 1024;

#[derive(Default)]
struct NetworkState {
    transports_by_addr: HashMap<String, Arc<Peer>>,
    transports_by_name: HashMap<String, Arc<Peer>>,
    port: u16,
}

/// Used as a factory that produces MockTransport instances which are
/// uniquely addressed and wired up to talk to each other.
#[derive(Clone, Default)]
pub struct MockNetwork {
    state: Arc<Mutex<NetworkState>>,
}

/// The sending half of a transport, which the other transports use to
/// deliver packets and streams to it.
struct Peer {
    packet_tx: mpsc::UnboundedSender<Packet>,
    stream_tx: mpsc::UnboundedSender<Stream>,
}

impl MockNetwork {
    /// Creates an empty network.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a new MockTransport with a unique address, wired up to talk
    /// to the other transports in the MockNetwork.
    pub fn new_transport(&self, name: &str) -> MockTransport {
        let mut state = self.state.lock();
        state.port += 1;
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), state.port);
        let (packet_tx, packet_rx) = mpsc::unbounded_channel();
        let (stream_tx, stream_rx) = mpsc::unbounded_channel();
        let peer = Arc::new(Peer {
            packet_tx,
            stream_tx,
        });

        state
            .transports_by_addr
            .insert(addr.to_string(), peer.clone());
        state.transports_by_name.insert(name.to_string(), peer);

        MockTransport {
            net: self.clone(),
            addr,
            packet_rx: tokio::sync::Mutex::new(packet_rx),
            stream_rx: tokio::sync::Mutex::new(stream_rx),
        }
    }
}

/// Directly plumbs messages to other transports in its MockNetwork.
pub struct MockTransport {
    net: MockNetwork,
    addr: SocketAddr,
    packet_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Packet>>,
    stream_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Stream>>,
}

impl MockTransport {
    /// The address of this transport in the mock network.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn get_peer(&self, a: &Address) -> Result<Arc<Peer>> {
        let state = self.net.state.lock();
        let dest = if !a.name.is_empty() {
            state.transports_by_name.get(&a.name)
        } else {
            state.transports_by_addr.get(&a.addr)
        };
        dest.cloned()
            .ok_or_else(|| Error::msg(format!("no route to {a}")))
    }
}

#[async_trait]
impl Transport for MockTransport {
    fn final_advertise_addr(&self, _ip: &str, _port: u16) -> Result<(IpAddr, u16)> {
        Ok((self.addr.ip(), self.addr.port()))
    }

    async fn write_to(&self, b: &[u8], addr: &str) -> Result<Instant> {
        let a = Address {
            addr: addr.to_string(),
            name: String::new(),
        };
        self.write_to_address(b, &a).await
    }

    async fn write_to_address(&self, b: &[u8], a: &Address) -> Result<Instant> {
        let dest = self.get_peer(a)?;

        let now = Instant::now();
        // The other transport may be gone. A packet to it is lost then, like
        // a packet on a real network.
        let _ = dest.packet_tx.send(Packet {
            buf: Bytes::copy_from_slice(b),
            from: self.addr,
            timestamp: now,
        });
        Ok(now)
    }

    async fn recv_packet(&self) -> Option<Packet> {
        self.packet_rx.lock().await.recv().await
    }

    async fn dial_timeout(&self, addr: &str, timeout: Duration) -> Result<Stream> {
        let a = Address {
            addr: addr.to_string(),
            name: String::new(),
        };
        self.dial_address_timeout(&a, timeout).await
    }

    async fn dial_address_timeout(&self, a: &Address, _timeout: Duration) -> Result<Stream> {
        let dest = self.get_peer(a)?;

        let (p1, p2) = tokio::io::duplex(MOCK_STREAM_BUF_SIZE);
        dest.stream_tx
            .send(Box::new(p1))
            .map_err(|_| Error::msg(format!("no route to {a}")))?;
        Ok(Box::new(p2))
    }

    async fn accept_stream(&self) -> Option<Stream> {
        self.stream_rx.lock().await.recv().await
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}
