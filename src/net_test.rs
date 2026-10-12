// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of net_test.go from https://github.com/hashicorp/memberlist.

// As a regression we left this test very low-level and network-ey, even after
// we abstracted the transport. We added some basic network-free transport
// tests in transport_test.rs to prove that we didn't hard code some network
// stuff outside of NetTransport.

use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::oneshot;

use crate::codec::{self, Message};
use crate::conn::{Conn, StreamReader};
use crate::error::Error;
use crate::messages::{
    AckResp, Alive, Compress, IndirectPingReq, MessageType, PROTOCOL_VERSION_MAX,
    PROTOCOL_VERSION_MIN, Ping, PushNodeState, PushPullHeader, UserMsgHeader, encode,
};
use crate::mock_transport::MockNetwork;
use crate::state::{Node, NodeState, NodeStateType};
use crate::testutil::{LOCALHOST, LOCALHOST_BYTES, capture_logs, get_memberlist, socket_addr};
use crate::transport::{Address, Connection, Transport};
use crate::util::{
    decode_compound_message, decompress_buffer, ip_to_bytes, join_host_port, make_compound_message,
};

/// Binds a UDP socket to a free port on 127.0.0.1.
async fn listen_udp() -> UdpSocket {
    UdpSocket::bind((LOCALHOST, 0))
        .await
        .expect("no udp listener")
}

/// Reads one packet from the socket.
async fn read_packet(udp: &UdpSocket) -> Vec<u8> {
    let mut buf = vec![0u8; 1500];
    let (n, _) = udp.recv_from(&mut buf).await.expect("unexpected err");
    buf.truncate(n);
    buf
}

/// Waits for the response that the future reads, and panics if it takes
/// longer than 2 seconds. Some Go tests read without a timeout. They use this
/// too, so that a lost packet fails the test instead of hanging it.
async fn wait_for_response<T>(fut: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), fut)
        .await
        .expect("timeout")
}

/// Returns a ping with sequence number 42 that asks for the ack to be sent
/// to the given socket.
fn ping_from(udp: &UdpSocket) -> Ping {
    let udp_addr = udp.local_addr().unwrap();
    Ping {
        seq_no: 42,
        source_addr: ip_to_bytes(&udp_addr.ip()),
        source_port: udp_addr.port(),
        source_node: "test".to_string(),
        ..Default::default()
    }
}

/// Returns a node with the given name and maximum protocol version.
fn node_with_pmax(name: &str, pmax: u8) -> Node {
    let mut node = Node::new(name, LOCALHOST, 0);
    node.pmax = pmax;
    node
}

#[tokio::test]
async fn test_handle_compound_ping() {
    let m = get_memberlist(|c| c.enable_compression = false).await;

    let udp = listen_udp().await;

    // Encode a ping
    let ping = ping_from(&udp);
    let buf = encode(MessageType::PING, &ping);

    // Make a compound message
    let compound = make_compound_message(&[&buf, &buf, &buf]);

    // Send compound version
    udp.send_to(&compound, socket_addr(&m))
        .await
        .expect("unexpected err");

    // Wait for responses
    wait_for_response(async {
        for _ in 0..3 {
            let inp = read_packet(&udp).await;

            let msg_type = MessageType(inp[0]);
            assert_eq!(msg_type, MessageType::ACK_RESP, "bad response {inp:?}");

            let ack: AckResp = codec::decode(&inp[1..]).expect("unexpected err");

            assert_eq!(ack.seq_no, 42, "bad sequence no");
        }
    })
    .await;

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_handle_ping() {
    let m = get_memberlist(|c| c.enable_compression = false).await;

    let udp = listen_udp().await;

    // Encode a ping
    let ping = ping_from(&udp);
    let buf = encode(MessageType::PING, &ping);

    // Send
    udp.send_to(&buf, socket_addr(&m))
        .await
        .expect("unexpected err");

    // Wait for response
    let inp = wait_for_response(read_packet(&udp)).await;

    let msg_type = MessageType(inp[0]);
    assert_eq!(msg_type, MessageType::ACK_RESP, "bad response {inp:?}");

    let ack: AckResp = codec::decode(&inp[1..]).expect("unexpected err");

    assert_eq!(ack.seq_no, 42, "bad sequence no");

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_handle_ping_wrong_node() {
    let m = get_memberlist(|c| c.enable_compression = false).await;

    let udp = listen_udp().await;

    // Encode a ping, wrong node!
    let ping = Ping {
        node: format!("{}-bad", m.config.name),
        ..ping_from(&udp)
    };
    let buf = encode(MessageType::PING, &ping);

    // Send
    udp.send_to(&buf, socket_addr(&m))
        .await
        .expect("unexpected err");

    // Wait for response
    let res = tokio::time::timeout(Duration::from_millis(50), read_packet(&udp)).await;

    // Should get an i/o timeout
    assert!(res.is_err(), "expected err, got {res:?}");

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_handle_indirect_ping() {
    let m = get_memberlist(|c| c.enable_compression = false).await;

    let udp = listen_udp().await;
    let udp_addr = udp.local_addr().unwrap();

    // Encode an indirect ping
    let ind = IndirectPingReq {
        seq_no: 100,
        target: LOCALHOST_BYTES.to_vec(),
        port: m.config.bind_port,
        node: m.config.name.clone(),
        source_addr: ip_to_bytes(&udp_addr.ip()),
        source_port: udp_addr.port(),
        source_node: "test".to_string(),
        ..Default::default()
    };
    let buf = encode(MessageType::INDIRECT_PING, &ind);

    // Send
    udp.send_to(&buf, socket_addr(&m))
        .await
        .expect("unexpected err");

    // Wait for response
    let inp = wait_for_response(read_packet(&udp)).await;

    let msg_type = MessageType(inp[0]);
    assert_eq!(msg_type, MessageType::ACK_RESP, "bad response {inp:?}");

    let ack: AckResp = codec::decode(&inp[1..]).expect("unexpected err");

    assert_eq!(ack.seq_no, 100, "bad sequence no");

    m.shutdown().await.unwrap();
}

/// Accepts one connection, and fails if none arrives within the timeout.
async fn accept_conn(tcp: &TcpListener, timeout: Duration) -> Result<Conn, String> {
    match tokio::time::timeout(timeout, tcp.accept()).await {
        Ok(Ok((stream, _))) => Ok(Conn::new(Box::new(stream))),
        Ok(Err(err)) => Err(format!("failed to connect: {err}")),
        Err(_) => Err("failed to connect: i/o timeout".to_string()),
    }
}

#[tokio::test]
async fn test_tcp_ping() {
    let tcp = TcpListener::bind((LOCALHOST, 0))
        .await
        .expect("no tcp listener");
    let tcp_addr = tcp.local_addr().unwrap();

    let tcp_addr2 = Address {
        addr: tcp_addr.to_string(),
        name: "test".to_string(),
    };

    // Note that tcp gets closed in the last test, so we avoid closing it
    // before that.

    let m = get_memberlist(|_| {}).await;

    let ping_timeout = m.config.probe_interval;
    let ping_time_max = m.config.probe_interval + Duration::from_millis(10);

    // Do a normal round trip.
    let ping_out = Ping {
        seq_no: 23,
        node: "mongo".to_string(),
        ..Default::default()
    };
    let server = async {
        let mut conn = accept_conn(&tcp, ping_time_max).await?;

        let (msg_type, mut reader) = m
            .read_stream(&mut conn)
            .await
            .map_err(|err| format!("failed to read ping: {err}"))?;

        if msg_type != MessageType::PING {
            return Err(format!("expecting ping, got message type ({msg_type})"));
        }

        let ping_in: Ping = reader
            .decode()
            .await
            .map_err(|err| format!("failed to decode ping: {err}"))?;
        drop(reader);

        if ping_in.seq_no != ping_out.seq_no {
            return Err(format!(
                "sequence number isn't correct ({}) vs ({})",
                ping_in.seq_no, ping_out.seq_no
            ));
        }

        if ping_in.node != ping_out.node {
            return Err(format!(
                "node name isn't correct ({}) vs ({})",
                ping_in.node, ping_out.node
            ));
        }

        let ack = AckResp {
            seq_no: ping_in.seq_no,
            payload: Vec::new(),
        };
        let out = encode(MessageType::ACK_RESP, &ack);

        m.raw_send_msg_stream(&mut conn, &out)
            .await
            .map_err(|err| format!("failed to send ack: {err}"))
    };
    let deadline = Instant::now() + ping_timeout;
    let (ping_err, res) = tokio::join!(
        server,
        m.send_ping_and_wait_for_ack(&tcp_addr2, &ping_out, deadline)
    );
    let did_contact = res.unwrap_or_else(|err| panic!("error trying to ping: {err}"));
    assert!(did_contact, "expected successful ping");
    if let Err(err) = ping_err {
        panic!("{err}");
    }

    // Make sure a mis-matched sequence number is caught.
    let server = async {
        let mut conn = accept_conn(&tcp, ping_time_max).await?;

        let (_, mut reader) = m
            .read_stream(&mut conn)
            .await
            .map_err(|err| format!("failed to read ping: {err}"))?;

        let ping_in: Ping = reader
            .decode()
            .await
            .map_err(|err| format!("failed to decode ping: {err}"))?;
        drop(reader);

        let ack = AckResp {
            seq_no: ping_in.seq_no + 1,
            payload: Vec::new(),
        };
        let out = encode(MessageType::ACK_RESP, &ack);

        m.raw_send_msg_stream(&mut conn, &out)
            .await
            .map_err(|err| format!("failed to send ack: {err}"))
    };
    let deadline = Instant::now() + ping_timeout;
    let (ping_err, res) = tokio::join!(
        server,
        m.send_ping_and_wait_for_ack(&tcp_addr2, &ping_out, deadline)
    );
    // A ping that failed with an error did not make contact.
    match res {
        Err(err) if err.to_string().contains("sequence number") => {}
        other => panic!("expected an error from mis-matched sequence number, got {other:?}"),
    }
    if let Err(err) = ping_err {
        panic!("{err}");
    }

    // Make sure an unexpected message type is handled gracefully.
    let server = async {
        let mut conn = accept_conn(&tcp, ping_time_max).await?;

        let (_, reader) = m
            .read_stream(&mut conn)
            .await
            .map_err(|err| format!("failed to read ping: {err}"))?;
        drop(reader);

        let bogus = IndirectPingReq::default();
        let out = encode(MessageType::INDIRECT_PING, &bogus);

        m.raw_send_msg_stream(&mut conn, &out)
            .await
            .map_err(|err| format!("failed to send bogus msg: {err}"))
    };
    let deadline = Instant::now() + ping_timeout;
    let (ping_err, res) = tokio::join!(
        server,
        m.send_ping_and_wait_for_ack(&tcp_addr2, &ping_out, deadline)
    );
    // A ping that failed with an error did not make contact.
    match res {
        Err(err) if err.to_string().contains("unexpected msgType") => {}
        other => panic!("expected an error from bogus message, got {other:?}"),
    }
    if let Err(err) = ping_err {
        panic!("{err}");
    }

    // Make sure failed I/O respects the deadline. In this case we try the
    // common case of the receiving node being totally down.
    drop(tcp);
    let deadline = Instant::now() + ping_timeout;
    let start_ping = Instant::now();
    let res = m
        .send_ping_and_wait_for_ack(&tcp_addr2, &ping_out, deadline)
        .await;
    let ping_time = start_ping.elapsed();
    let did_contact = res
        .unwrap_or_else(|err| panic!("expected no error during ping on closed socket, got: {err}"));
    assert!(!did_contact, "expected failed ping");
    assert!(
        ping_time <= ping_time_max,
        "took too long to fail ping, {:9.6}",
        ping_time.as_secs_f64()
    );

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_tcp_push_pull() {
    let m = get_memberlist(|_| {}).await;

    m.nodes.write().nodes.push(NodeState {
        node: Node::new("Test 0", LOCALHOST, m.config.bind_port),
        incarnation: 0,
        state: NodeStateType::Suspect,
        state_change: Some(Instant::now() - Duration::from_secs(1)),
    });

    let stream = TcpStream::connect(socket_addr(&m))
        .await
        .expect("unexpected err");
    let mut conn = Conn::new(Box::new(stream));
    // The Go test has no deadline. This one only makes sure that the test
    // fails instead of hanging if no answer comes.
    conn.set_deadline(Instant::now() + Duration::from_secs(10));

    let local_nodes: Vec<PushNodeState> = (0..3)
        .map(|i| PushNodeState {
            name: format!("Test {i}"),
            addr: LOCALHOST_BYTES.to_vec(),
            port: m.config.bind_port,
            incarnation: 1,
            state: NodeStateType::Alive as i64,
            ..Default::default()
        })
        .collect();

    // Send our node state
    let header = PushPullHeader {
        nodes: 3,
        ..Default::default()
    };

    // Send the push/pull indicator
    let mut out = vec![MessageType::PUSH_PULL.0];

    header.encode_to(&mut out);
    for node in &local_nodes {
        node.encode_to(&mut out);
    }
    conn.write_all(&out).await.expect("unexpected err");

    // Read the message type
    let mut msg_type = MessageType(conn.read_u8().await.expect("unexpected err"));

    let mut reader = StreamReader::Conn(&mut conn);

    // Check if we have a compressed message
    if msg_type == MessageType::COMPRESS {
        let c: Compress = reader.decode().await.expect("unexpected err");
        let mut decomp = decompress_buffer(&c).expect("unexpected err");

        // Reset the message type
        msg_type = MessageType(decomp.remove(0));

        // Create a new reader
        reader = StreamReader::mem(decomp);
    }

    // Quit if not push/pull
    assert_eq!(msg_type, MessageType::PUSH_PULL, "bad message type");

    let header: PushPullHeader = reader.decode().await.expect("unexpected err");

    // Try to decode all the states
    let mut remote_nodes = Vec::new();
    for _ in 0..header.nodes {
        let node: PushNodeState = reader.decode().await.expect("unexpected err");
        remote_nodes.push(node);
    }

    assert_eq!(remote_nodes.len(), 1, "bad response");

    let n = &remote_nodes[0];
    assert_eq!(n.name, "Test 0", "bad name");
    assert_eq!(n.addr, LOCALHOST_BYTES, "bad addr");
    assert_eq!(n.incarnation, 0, "bad incarnation");
    assert_eq!(n.state, NodeStateType::Suspect as i64, "bad state");

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_send_msg_piggyback() {
    let m = get_memberlist(|_| {}).await;

    // Add a message to be broadcast
    let a = Alive {
        incarnation: 10,
        node: "rand".to_string(),
        addr: vec![127, 0, 0, 255],
        meta: Vec::new(),
        vsn: vec![
            PROTOCOL_VERSION_MIN,
            PROTOCOL_VERSION_MAX,
            PROTOCOL_VERSION_MIN,
            1,
            1,
            1,
        ],
        ..Default::default()
    };
    m.encode_and_broadcast("rand", MessageType::ALIVE, &a);

    let udp = listen_udp().await;

    // Encode a ping
    let ping = ping_from(&udp);
    let buf = encode(MessageType::PING, &ping);

    // Send
    udp.send_to(&buf, socket_addr(&m))
        .await
        .expect("unexpected err");

    // Wait for response
    let inp = wait_for_response(read_packet(&udp)).await;

    let msg_type = MessageType(inp[0]);
    assert_eq!(msg_type, MessageType::COMPOUND, "bad response {inp:?}");

    // get the parts
    let (trunc, parts) =
        decode_compound_message(&Bytes::copy_from_slice(&inp[1..])).expect("unexpected err");
    assert_eq!(trunc, 0, "unexpected truncation");
    assert_eq!(parts.len(), 2, "unexpected parts {parts:?}");

    let ack: AckResp = codec::decode(&parts[0][1..]).expect("unexpected err");

    assert_eq!(ack.seq_no, 42, "bad sequence no");

    let aliveout: Alive = codec::decode(&parts[1][1..]).expect("unexpected err");

    assert!(
        aliveout.node == "rand" && aliveout.incarnation == 10,
        "bad mesg"
    );

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_raw_send_udp_crc() {
    let m = get_memberlist(|c| c.enable_compression = false).await;

    let udp = listen_udp().await;

    let a = Address {
        addr: udp.local_addr().unwrap().to_string(),
        name: "test".to_string(),
    };

    // Pass no node with no nodes registered, should result in no checksum
    let payload = [3u8, 3, 3, 3];
    m.raw_send_msg_packet(&a, None, &payload)
        .await
        .expect("err");

    let inp = wait_for_response(read_packet(&udp)).await;

    assert_eq!(inp.len(), 4, "bad: {inp:?}");

    // Pass a node with pmax >= 5, should result in a checksum
    m.raw_send_msg_packet(&a, Some(&node_with_pmax("", 5)), &payload)
        .await
        .expect("err");

    let inp = wait_for_response(read_packet(&udp)).await;

    assert_eq!(inp.len(), 9, "bad: {inp:?}");

    // Register a node with pmax >= 5 to be looked up, should result in a
    // checksum. The lookup uses the bare IP of the address as the node name.
    {
        let mut table = m.nodes.write();
        let idx = table.nodes.len();
        table.nodes.push(NodeState {
            node: node_with_pmax("127.0.0.1", 5),
            incarnation: 0,
            state: NodeStateType::Alive,
            state_change: None,
        });
        table.node_map.insert("127.0.0.1".to_string(), idx);
    }
    m.raw_send_msg_packet(&a, None, &payload)
        .await
        .expect("err");

    let inp = wait_for_response(read_packet(&udp)).await;

    assert_eq!(inp.len(), 9, "bad: {inp:?}");

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_ingest_packet_crc() {
    let m = get_memberlist(|c| c.enable_compression = false).await;

    let udp = listen_udp().await;

    let a = Address {
        addr: udp.local_addr().unwrap().to_string(),
        name: "test".to_string(),
    };

    // Get a message with a checksum
    let payload = [3u8, 3, 3, 3];
    m.raw_send_msg_packet(&a, Some(&node_with_pmax("", 5)), &payload)
        .await
        .expect("err");

    let mut inp = wait_for_response(read_packet(&udp)).await;

    assert_eq!(inp.len(), 9, "bad: {inp:?}");

    // Corrupt the checksum
    inp[1] <<= 1;

    let (_guard, logs) = capture_logs();
    m.ingest_packet(inp.into(), udp.local_addr().unwrap(), Instant::now())
        .await;

    assert!(
        logs.contents().contains("invalid checksum"),
        "bad: {}",
        logs.contents()
    );

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_handle_command() {
    let (_guard, logs) = capture_logs();
    // The Go test uses an empty Memberlist that only has a logger. Here it
    // is a real one, because the fields cannot be left out.
    let m = get_memberlist(|_| {}).await;

    let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 12345);
    m.handle_command(Bytes::new(), from, Instant::now()).await;
    assert!(
        logs.contents().contains("missing message type byte"),
        "bad: {}",
        logs.contents()
    );

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_read_remote_state_limits() {
    let mock_net = MockNetwork::new();
    let tr = Arc::new(mock_net.new_transport("node"));
    let (_guard, logs) = capture_logs();

    let m = get_memberlist(|c| {
        c.enable_compression = false;
        c.transport = Some(tr.clone());
        c.bind_addr = "127.0.0.1".to_string();
        c.bind_port = 1;
    })
    .await;

    let addr = join_host_port(&m.config.bind_addr, m.config.bind_port);

    // nodes
    {
        let msg = PushPullHeader {
            nodes: 10_000_000,
            user_state_len: 100,
            join: false,
        };
        let buf = encode(MessageType::PUSH_PULL, &msg);

        let stream = tr
            .dial_timeout(&addr, Duration::from_millis(100))
            .await
            .expect("err");
        let mut conn = Conn::new(stream);

        m.raw_send_msg_stream(&mut conn, &buf).await.expect("err");

        // conn closed: get nothing back
        let res = conn.read_u8().await;
        assert!(matches!(res, Err(Error::Eof)), "bad: {res:?}");
        assert!(
            logs.contents()
                .contains("number of nodes in header (10000000) exceeds limit"),
            "bad: {}",
            logs.contents()
        );
    }

    // user_state
    {
        let msg = PushPullHeader {
            nodes: 0,
            user_state_len: 30_000_000,
            join: false,
        };
        let buf = encode(MessageType::PUSH_PULL, &msg);

        let stream = tr
            .dial_timeout(&addr, Duration::from_millis(100))
            .await
            .expect("err");
        let mut conn = Conn::new(stream);

        m.raw_send_msg_stream(&mut conn, &buf).await.expect("err");

        // conn closed: get nothing back
        let res = conn.read_u8().await;
        assert!(matches!(res, Err(Error::Eof)), "bad: {res:?}");
        assert!(
            logs.contents()
                .contains("user state length (30000000) exceeds limit"),
            "bad: {}",
            logs.contents()
        );
    }

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_read_user_msg_limit() {
    let mock_net = MockNetwork::new();
    let tr = Arc::new(mock_net.new_transport("node"));
    let (_guard, logs) = capture_logs();

    let m = get_memberlist(|c| {
        c.enable_compression = false;
        c.transport = Some(tr.clone());
        c.bind_addr = "127.0.0.1".to_string();
        c.bind_port = 1;
    })
    .await;

    let addr = join_host_port(&m.config.bind_addr, m.config.bind_port);

    let msg = UserMsgHeader {
        user_msg_len: 30_000_000,
    };
    let buf = encode(MessageType::USER, &msg);

    let stream = tr
        .dial_timeout(&addr, Duration::from_millis(100))
        .await
        .expect("err");
    let mut conn = Conn::new(stream);

    m.raw_send_msg_stream(&mut conn, &buf).await.expect("err");

    // conn closed: get nothing back
    let err = conn.read_u8().await.expect_err("expected an error");
    assert!(err.to_string().contains("EOF"), "bad: {err}");
    assert!(
        logs.contents()
            .contains("user message length (30000000) exceeds limit"),
        "bad: {}",
        logs.contents()
    );

    m.shutdown().await.unwrap();
}

/// A connection where every read fails. It tells the test when it is closed.
/// A Rust connection is closed when it is dropped.
struct ErrorReadConn {
    closed: Option<oneshot::Sender<()>>,
}

impl AsyncRead for ErrorReadConn {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::other("test read error")))
    }
}

impl AsyncWrite for ErrorReadConn {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Connection for ErrorReadConn {}

impl Drop for ErrorReadConn {
    fn drop(&mut self) {
        if let Some(closed) = self.closed.take() {
            let _ = closed.send(());
        }
    }
}

#[tokio::test]
async fn test_handle_conn_nil_conn_after_remove_label_header_from_stream() {
    let mock_net = MockNetwork::new();

    let t1 = Arc::new(mock_net.new_transport("node1"));

    let (_guard, logs) = capture_logs();
    let m = get_memberlist(|c| c.transport = Some(t1.clone())).await;

    let (closed_tx, closed_rx) = oneshot::channel();
    let err_conn = ErrorReadConn {
        closed: Some(closed_tx),
    };
    // The Go test hands the connection to the transport with IngestStream,
    // which is not ported. The memberlist calls handle_conn for every stream
    // that its transport accepts, so call it directly.
    tokio::time::timeout(Duration::from_secs(2), m.handle_conn(Box::new(err_conn)))
        .await
        .expect("timeout");

    // The connection must be successfully closed
    closed_rx.await.expect("the connection was not closed");

    // Make sure that it was closed because the read failed.
    assert!(
        logs.contents()
            .contains("failed to receive and remove the stream label header: test read error"),
        "bad: {}",
        logs.contents()
    );

    m.shutdown().await.unwrap();
}
