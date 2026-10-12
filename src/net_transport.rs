// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of net_transport.go from https://github.com/hashicorp/memberlist.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use metrics::Label;
use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::error::{Error, Result, timeout_error};
use crate::logging::log_address;
use crate::sockaddr::get_private_ip;
use crate::transport::{Address, Packet, Stream, Transport};

/// Used to buffer incoming packets during read operations.
const UDP_PACKET_BUF_SIZE: usize = 65536;

/// A large buffer size that we attempt to set UDP sockets to in order to
/// handle a large volume of messages.
const UDP_RECV_BUF_SIZE: usize = 2 * 1024 * 1024;

/// Used to configure a net transport.
#[derive(Debug, Clone, Default)]
pub struct NetTransportConfig {
    /// A list of addresses to bind to for both TCP and UDP communications.
    pub bind_addrs: Vec<String>,

    /// The port to listen on, for each address above.
    pub bind_port: u16,

    /// Optional labels to apply to all metrics emitted by this transport.
    pub metric_labels: Vec<Label>,
}

/// A Transport implementation that uses connectionless UDP for packet
/// operations, and ad-hoc TCP connections for stream operations.
pub struct NetTransport {
    config: NetTransportConfig,
    packet_rx: tokio::sync::Mutex<mpsc::Receiver<Packet>>,
    stream_rx: tokio::sync::Mutex<mpsc::Receiver<Stream>>,
    /// The address of the first TCP listener.
    tcp_addr: SocketAddr,
    /// The first UDP socket, which is used to send packets. It is dropped at
    /// shutdown so that the port is free again.
    udp_socket: Mutex<Option<Arc<UdpSocket>>>,
    shutdown: CancellationToken,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl NetTransport {
    /// Returns a net transport with the given configuration. On success all
    /// the network listeners will be created and listening.
    pub async fn new(config: NetTransportConfig) -> Result<NetTransport> {
        // If we reject the empty list outright we can assume that there's at
        // least one listener of each type later during operation.
        if config.bind_addrs.is_empty() {
            return Err(Error::msg("at least one bind address is required"));
        }

        // Build all the TCP and UDP listeners. If there's an error, the
        // listeners that we have made so far are closed when they are
        // dropped.
        let mut port = config.bind_port;
        let mut tcp_listeners = Vec::with_capacity(config.bind_addrs.len());
        let mut udp_listeners = Vec::with_capacity(config.bind_addrs.len());
        for addr in &config.bind_addrs {
            let ip: IpAddr = addr
                .parse()
                .map_err(|_| Error::msg(format!("failed to parse bind address {addr:?}")))?;

            let tcp_ln = TcpListener::bind(SocketAddr::new(ip, port))
                .await
                .map_err(|err| listen_error("TCP", addr, port, err))?;

            // If the config port given was zero, use the first TCP listener
            // to pick an available port and then apply that to everything
            // else.
            if port == 0 {
                port = tcp_ln.local_addr()?.port();
            }
            tcp_listeners.push(tcp_ln);

            let udp_ln = UdpSocket::bind(SocketAddr::new(ip, port))
                .await
                .map_err(|err| listen_error("UDP", addr, port, err))?;
            set_udp_recv_buf(&udp_ln)
                .map_err(|err| Error::msg(format!("failed to resize UDP buffer: {err}")))?;
            udp_listeners.push(Arc::new(udp_ln));
        }

        // Fire them up now that we've been able to create them all.
        let (packet_tx, packet_rx) = mpsc::channel(1);
        let (stream_tx, stream_rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();
        let tcp_addr = tcp_listeners[0].local_addr()?;
        let udp_socket = udp_listeners[0].clone();
        let mut tasks = Vec::with_capacity(2 * tcp_listeners.len());
        for (tcp_ln, udp_ln) in tcp_listeners.into_iter().zip(udp_listeners) {
            tasks.push(tokio::spawn(tcp_listen(
                tcp_ln,
                stream_tx.clone(),
                shutdown.clone(),
            )));
            tasks.push(tokio::spawn(udp_listen(
                udp_ln,
                packet_tx.clone(),
                shutdown.clone(),
                config.metric_labels.clone(),
            )));
        }

        Ok(NetTransport {
            config,
            packet_rx: tokio::sync::Mutex::new(packet_rx),
            stream_rx: tokio::sync::Mutex::new(stream_rx),
            tcp_addr,
            udp_socket: Mutex::new(Some(udp_socket)),
            shutdown,
            tasks: Mutex::new(tasks),
        })
    }

    /// Returns the bind port that was automatically given by the kernel, if
    /// a bind port of 0 was given.
    pub fn get_auto_bind_port(&self) -> u16 {
        // We made sure there's at least one TCP listener, and that one's
        // port was applied to all the others for the dynamic bind case.
        self.tcp_addr.port()
    }
}

fn listen_error(kind: &str, addr: &str, port: u16, err: io::Error) -> Error {
    // Keep the kind of the error, so that the caller can tell an address
    // that is in use apart from other failures.
    Error::Io(io::Error::new(
        err.kind(),
        format!("failed to start {kind} listener on {addr:?} port {port}: {err}"),
    ))
}

#[async_trait]
impl Transport for NetTransport {
    fn final_advertise_addr(&self, ip: &str, port: u16) -> Result<(IpAddr, u16)> {
        if !ip.is_empty() {
            // If they've supplied an address, use that.
            let advertise_addr: IpAddr = ip
                .parse()
                .map_err(|_| Error::msg(format!("failed to parse advertise address {ip:?}")))?;

            // Ensure IPv4 conversion if necessary.
            return Ok((advertise_addr.to_canonical(), port));
        }

        let advertise_addr = if self.config.bind_addrs[0] == "0.0.0.0" {
            // Otherwise, if we're not bound to a specific IP, let's use a
            // suitable private IP address.
            get_private_ip()
                .map_err(|err| Error::msg(format!("failed to get interface addresses: {err}")))?
                .ok_or_else(|| {
                    Error::msg("no private IP address found, and explicit IP not provided")
                })?
        } else {
            // Use the IP that we're bound to, based on the first TCP
            // listener, which we already ensure is there.
            self.tcp_addr.ip()
        };

        // Use the port we are bound to.
        Ok((advertise_addr, self.get_auto_bind_port()))
    }

    async fn write_to(&self, b: &[u8], addr: &str) -> Result<Instant> {
        let a = Address {
            addr: addr.to_string(),
            name: String::new(),
        };
        self.write_to_address(b, &a).await
    }

    async fn write_to_address(&self, b: &[u8], a: &Address) -> Result<Instant> {
        let udp_addr = resolve_addr(&a.addr).await?;

        // We made sure there's at least one UDP listener, so just use the
        // packet sending interface on the first one. Take the time after the
        // write call comes back, which will underestimate the time a little,
        // but help account for any delays before the write occurs.
        let socket = self.udp_socket.lock().clone();
        let Some(socket) = socket else {
            return Err(write_error(io::Error::new(
                io::ErrorKind::NotConnected,
                "use of closed network connection",
            )));
        };
        socket.send_to(b, udp_addr).await.map_err(write_error)?;
        Ok(Instant::now())
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

    async fn dial_address_timeout(&self, a: &Address, timeout: Duration) -> Result<Stream> {
        let dial_error = |source| Error::Op {
            op: "dial",
            net: "tcp",
            source,
        };
        let connect = async {
            let addr = resolve_addr(&a.addr).await?;
            TcpStream::connect(addr).await.map_err(dial_error)
        };
        let conn = if timeout.is_zero() {
            connect.await?
        } else {
            match tokio::time::timeout(timeout, connect).await {
                Ok(res) => res?,
                Err(_) => return Err(dial_error(timeout_error())),
            }
        };
        // Go turns this on for every TCP connection.
        let _ = conn.set_nodelay(true);
        Ok(Box::new(conn))
    }

    async fn accept_stream(&self) -> Option<Stream> {
        self.stream_rx.lock().await.recv().await
    }

    async fn shutdown(&self) -> Result<()> {
        // This will avoid log spam about errors when we shut down.
        self.shutdown.cancel();

        // Rip through all the connections and shut them down.
        self.udp_socket.lock().take();

        // Block until all the listener tasks have died. Each task owns its
        // socket, so the ports are free again after this.
        let tasks = std::mem::take(&mut *self.tasks.lock());
        for task in tasks {
            let _ = task.await;
        }
        Ok(())
    }
}

fn write_error(source: io::Error) -> Error {
    Error::Op {
        op: "write",
        net: "udp",
        source,
    }
}

/// Resolves a "host:port" string into a socket address.
async fn resolve_addr(addr: &str) -> Result<SocketAddr> {
    if let Ok(addr) = addr.parse::<SocketAddr>() {
        return Ok(addr);
    }
    let mut addrs = tokio::net::lookup_host(addr).await?;
    addrs
        .next()
        .ok_or_else(|| Error::msg(format!("address {addr}: no such host")))
}

/// A long running task that accepts incoming TCP connections and hands them
/// off to the stream channel.
pub(crate) async fn tcp_listen(
    tcp_ln: TcpListener,
    stream_tx: mpsc::Sender<Stream>,
    shutdown: CancellationToken,
) {
    // The initial delay after an accept error before attempting again.
    const BASE_DELAY: Duration = Duration::from_millis(5);

    // The maximum delay after an accept error before attempting again.
    const MAX_DELAY: Duration = Duration::from_secs(1);

    let mut loop_delay = Duration::ZERO;
    loop {
        let res = tokio::select! {
            _ = shutdown.cancelled() => break,
            res = tcp_ln.accept() => res,
        };
        let conn = match res {
            Ok((conn, _)) => conn,
            Err(err) => {
                if loop_delay.is_zero() {
                    loop_delay = BASE_DELAY;
                } else {
                    loop_delay *= 2;
                }

                if loop_delay > MAX_DELAY {
                    loop_delay = MAX_DELAY;
                }

                error!("memberlist: Error accepting TCP connection: {err}");
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(loop_delay) => continue,
                }
            }
        };
        // No error, reset loop delay
        loop_delay = Duration::ZERO;

        // Go turns this on for every TCP connection.
        let _ = conn.set_nodelay(true);
        tokio::select! {
            _ = shutdown.cancelled() => break,
            res = stream_tx.send(Box::new(conn)) => {
                if res.is_err() {
                    break;
                }
            }
        }
    }
}

/// A long running task that accepts incoming UDP packets and hands them off
/// to the packet channel.
async fn udp_listen(
    udp_ln: Arc<UdpSocket>,
    packet_tx: mpsc::Sender<Packet>,
    shutdown: CancellationToken,
    metric_labels: Vec<Label>,
) {
    let mut buf = vec![0u8; UDP_PACKET_BUF_SIZE];
    loop {
        // Do a read into the buffer. Grab a time stamp as close as possible
        // to the I/O.
        let res = tokio::select! {
            _ = shutdown.cancelled() => break,
            res = udp_ln.recv_from(&mut buf) => res,
        };
        let ts = Instant::now();
        let (n, addr) = match res {
            Ok(res) => res,
            Err(err) => {
                error!("memberlist: Error reading UDP packet: {err}");
                continue;
            }
        };

        // Check the length - it needs to have at least one byte to be a
        // proper message.
        if n < 1 {
            error!(
                "memberlist: UDP packet too short ({n} bytes) {}",
                log_address(Some(addr))
            );
            continue;
        }

        // Ingest the packet.
        metrics::counter!("memberlist.udp.received", metric_labels.clone()).increment(n as u64);
        let packet = Packet {
            buf: Bytes::copy_from_slice(&buf[..n]),
            from: addr,
            timestamp: ts,
        };
        tokio::select! {
            _ = shutdown.cancelled() => break,
            res = packet_tx.send(packet) => {
                if res.is_err() {
                    break;
                }
            }
        }
    }
}

/// Resizes the UDP receive window. The function attempts to set the read
/// buffer to `UDP_RECV_BUF_SIZE` but backs off until the read buffer can be
/// set.
fn set_udp_recv_buf(c: &UdpSocket) -> io::Result<()> {
    let sock = socket2::SockRef::from(c);
    let mut size = UDP_RECV_BUF_SIZE;
    let mut res = Ok(());
    while size > 0 {
        res = sock.set_recv_buffer_size(size);
        if res.is_ok() {
            return Ok(());
        }
        size /= 2;
    }
    res
}
