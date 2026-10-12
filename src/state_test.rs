// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of state_test.go from https://github.com/hashicorp/memberlist.

use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::delegate::{
    ChannelEventDelegate, EventDelegate, NodeEvent, NodeEventType, PingDelegate,
};
use crate::error::{Error, Result};
use crate::memberlist::Memberlist;
use crate::messages::{
    Alive, Dead, MessageType, PROTOCOL_VERSION_2_COMPATIBLE, PROTOCOL_VERSION_MAX,
    PROTOCOL_VERSION_MIN, PushNodeState, Suspect,
};
use crate::net_transport::{NetTransport, NetTransportConfig};
use crate::state::{AckMessage, Node, NodeState, NodeStateType, failed_remote};
use crate::testutil::*;
use crate::transport::{Packet, Stream, Transport};

const HOUR: Duration = Duration::from_secs(3600);

/// An alive message about a node that is only known by name. Nothing is ever
/// sent to it, so it has no port. The versions are the ones of `m`.
fn alive_msg(m: &Memberlist, node: &str, addr: [u8; 4], incarnation: u32) -> Alive {
    Alive {
        node: node.to_string(),
        addr: addr.to_vec(),
        incarnation,
        vsn: m.config.build_vsn_array(),
        ..Default::default()
    }
}

/// Like `alive_of`, but without the protocol versions. A node that is added
/// this way looks like it speaks an old protocol: no TCP pings and no nacks.
fn alive_of_no_vsn(m: &Memberlist) -> Alive {
    Alive {
        vsn: Vec::new(),
        ..alive_of(m)
    }
}

/// An alive message, with incarnation 1, about a node that never gets
/// started: it has a fresh name and nothing listens on its port.
fn alive_of_missing_node(vsn: Vec<u8>) -> Alive {
    Alive {
        node: test_name(),
        addr: LOCALHOST_BYTES.to_vec(),
        port: unused_port(),
        incarnation: 1,
        vsn,
        ..Default::default()
    }
}

/// A copy of the state of the node with the given name.
fn node_state(m: &Memberlist, name: &str) -> NodeState {
    m.nodes.read().get(name).expect("should map node").clone()
}

/// Moves a time one hour into the past. If that is before the earliest time
/// that an `Instant` can hold, the result is `None`, which also counts as
/// very long ago.
fn hour_earlier(t: Option<Instant>) -> Option<Instant> {
    t.and_then(|t| t.checked_sub(HOUR))
}

fn seq_no(m: &Memberlist) -> u32 {
    m.sequence_num.load(Ordering::SeqCst)
}

/// The type of the first message in the broadcast queue.
fn first_queued_msg_type(m: &Memberlist) -> MessageType {
    MessageType(m.broadcasts.ordered_view(true)[0].b.message()[0])
}

/// Returns an event delegate that sends the events to a channel, and the
/// receiving end of that channel.
fn event_channel() -> (
    Arc<ChannelEventDelegate>,
    mpsc::UnboundedReceiver<NodeEvent>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Arc::new(ChannelEventDelegate { ch: tx }), rx)
}

/// Shuts the memberlists down, like the deferred calls in the Go tests.
async fn shutdown_all(ms: &[&Memberlist]) {
    for m in ms {
        m.shutdown().await.expect("failed to shut down");
    }
}

const DEFAULT_PAYLOAD: &[u8] = b"whatever";

/// A ping delegate that puts a fixed payload into every ack.
struct MockPing;

impl PingDelegate for MockPing {
    fn ack_payload(&self) -> Vec<u8> {
        DEFAULT_PAYLOAD.to_vec()
    }

    fn notify_ping_complete(&self, _other: &Node, _rtt: Duration, _payload: &[u8]) {}
}

/// An event delegate that only passes the events on while it is enabled.
struct ToggledEventDelegate {
    real: Arc<dyn EventDelegate>,
    enabled: Mutex<bool>,
}

impl ToggledEventDelegate {
    fn new(real: Arc<dyn EventDelegate>) -> Arc<ToggledEventDelegate> {
        Arc::new(ToggledEventDelegate {
            real,
            enabled: Mutex::new(false),
        })
    }

    fn toggle(&self, enabled: bool) {
        *self.enabled.lock() = enabled;
    }
}

impl EventDelegate for ToggledEventDelegate {
    fn notify_join(&self, n: &Node) {
        let enabled = self.enabled.lock();
        if *enabled {
            self.real.notify_join(n);
        }
    }

    fn notify_leave(&self, n: &Node) {
        let enabled = self.enabled.lock();
        if *enabled {
            self.real.notify_leave(n);
        }
    }

    fn notify_update(&self, n: &Node) {
        let enabled = self.enabled.lock();
        if *enabled {
            self.real.notify_update(n);
        }
    }
}

/// A network transport that throws away every packet that comes in. The node
/// behind it can still send packets, and streams work in both directions, so
/// it is cut off from incoming UDP traffic only.
struct NoInboundPackets {
    inner: NetTransport,
}

#[async_trait]
impl Transport for NoInboundPackets {
    fn final_advertise_addr(&self, ip: &str, port: u16) -> Result<(IpAddr, u16)> {
        self.inner.final_advertise_addr(ip, port)
    }

    async fn write_to(&self, b: &[u8], addr: &str) -> Result<Instant> {
        self.inner.write_to(b, addr).await
    }

    async fn recv_packet(&self) -> Option<Packet> {
        // Read the packets, so that the listener does not block, and drop
        // them until the transport shuts down.
        while self.inner.recv_packet().await.is_some() {}
        None
    }

    async fn dial_timeout(&self, addr: &str, timeout: Duration) -> Result<Stream> {
        self.inner.dial_timeout(addr, timeout).await
    }

    async fn accept_stream(&self) -> Option<Stream> {
        self.inner.accept_stream().await
    }

    async fn shutdown(&self) -> Result<()> {
        self.inner.shutdown().await
    }
}

/// Creates the listeners of a memberlist that never sees a packet. This is
/// how the TCP fallback tests isolate a node from UDP traffic.
async fn udp_isolated_memberlist() -> Memberlist {
    let nc = NetTransportConfig {
        bind_addrs: vec![LOCALHOST.to_string()],
        bind_port: 0, // choose a free port
        metric_labels: Vec::new(),
    };

    // The port that the kernel picks for TCP can be taken for UDP, so try a
    // few times, like `new_memberlist` does.
    let mut made = None;
    for _ in 0..10 {
        if let Ok(nt) = NetTransport::new(nc.clone()).await {
            made = Some(nt);
            break;
        }
    }
    let nt = made.expect("failed to get a transport");

    let port = nt.get_auto_bind_port();
    host_memberlist(&test_name(), |c| {
        c.bind_port = port;
        c.advertise_port = port;
        c.transport = Some(Arc::new(NoInboundPackets { inner: nt }));
    })
    .await
}

#[tokio::test]
async fn test_memberlist_probe() {
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(1);
        c.probe_interval = Duration::from_millis(10);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |_| {}).await;

    let a1 = alive_of(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of(&m2);
    m1.alive_node(&a2, None, false);

    // should ping addr2
    m1.probe().await;

    // Should not be marked suspect
    assert_eq!(
        m1.get_node_state(&a2.node),
        NodeStateType::Alive,
        "Expect node to be alive"
    );

    // Should increment seqno
    assert_eq!(seq_no(&m1), 1, "bad seqno");

    shutdown_all(&[&m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node_suspect() {
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(1);
        c.probe_interval = Duration::from_millis(10);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |_| {}).await;
    let m3 = host_memberlist(&test_name(), |_| {}).await;

    let a1 = alive_of(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of(&m2);
    m1.alive_node(&a2, None, false);
    let a3 = alive_of(&m3);
    m1.alive_node(&a3, None, false);
    // Node 4 never gets started.
    let a4 = alive_of_missing_node(m1.config.build_vsn_array());
    m1.alive_node(&a4, None, false);

    m1.probe_node_by_addr(&a4.node).await;

    // Should be marked suspect.
    assert_eq!(
        m1.get_node_state(&a4.node),
        NodeStateType::Suspect,
        "Expect node to be suspect"
    );
    sleep(Duration::from_millis(10)).await;

    // One of the peers should have attempted an indirect probe.
    let (s2, s3) = (seq_no(&m2), seq_no(&m3));
    assert!(
        s2 == 1 || s3 == 1,
        "bad seqnos, expected both to be 1: {s2}, {s3}"
    );

    shutdown_all(&[&m3, &m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node_suspect_dogpile() {
    struct Case {
        name: &'static str,
        num_peers: usize,
        confirmations: usize,
        expected: Duration,
    }
    let case = |name, num_peers, confirmations, expected_ms| Case {
        name,
        num_peers,
        confirmations,
        expected: Duration::from_millis(expected_ms),
    };
    let cases = [
        case("n=2, k=3 (max timeout disabled)", 1, 0, 500),
        case("n=3, k=3", 2, 0, 500),
        case("n=4, k=3", 3, 0, 500),
        case("n=5, k=3 (max timeout starts to take effect)", 4, 0, 1000),
        case("n=6, k=3", 5, 0, 1000),
        case("n=6, k=3 (confirmations start to lower timeout)", 5, 1, 750),
        case("n=6, k=3", 5, 2, 604),
        case("n=6, k=3 (timeout driven to nominal value)", 5, 3, 500),
        case("n=6, k=3", 5, 4, 500),
    ];

    for (i, c) in cases.iter().enumerate() {
        // Create the main memberlist under test.
        let m = host_memberlist(&test_name(), |c| {
            c.probe_timeout = Duration::from_millis(1);
            c.probe_interval = Duration::from_millis(100);
            c.suspicion_mult = 5;
            c.suspicion_max_timeout_mult = 2;
        })
        .await;

        let a = alive_of(&m);
        m.alive_node(&a, None, true);

        // Make all but one peer be an real, alive instance.
        let mut peers = Vec::new();
        for _ in 0..c.num_peers - 1 {
            let peer = host_memberlist(&test_name(), |_| {}).await;

            let a = Alive {
                vsn: m.config.build_vsn_array(),
                ..alive_of(&peer)
            };
            m.alive_node(&a, None, false);

            peers.push(peer);
        }

        // Just use a bogus address for the last peer so it doesn't respond
        // to pings, but tell the memberlist it's alive.
        let a = alive_of_missing_node(m.config.build_vsn_array());
        let bad_peer = a.node.clone();
        m.alive_node(&a, None, false);

        // Force a probe, which should start us into the suspect state.
        m.probe_node_by_addr(&bad_peer).await;

        assert_eq!(
            m.get_node_state(&bad_peer),
            NodeStateType::Suspect,
            "case {i} ({}): expected node to be suspect",
            c.name
        );

        // Add the requested number of confirmations.
        for j in 0..c.confirmations {
            let s = Suspect {
                node: bad_peer.clone(),
                incarnation: 1,
                from: format!("peer{j}"),
            };
            m.suspect_node(&s);
        }

        // Wait until right before the timeout and make sure the timer
        // hasn't fired.
        let fudge = Duration::from_millis(25);
        sleep(c.expected - fudge).await;

        assert_eq!(
            m.get_node_state(&bad_peer),
            NodeStateType::Suspect,
            "case {i} ({}): expected node to still be suspect",
            c.name
        );

        // Wait through the timeout and a little after to make sure the
        // timer fires.
        sleep(2 * fudge).await;

        assert_eq!(
            m.get_node_state(&bad_peer),
            NodeStateType::Dead,
            "case {i} ({}): expected node to be dead",
            c.name
        );

        for peer in &peers {
            peer.shutdown().await.expect("failed to shut down");
        }
        m.shutdown().await.expect("failed to shut down");
    }
}

// The three TCP fallback tests are commented out in the Go test file,
// because they swap the UDP and TCP listeners of a memberlist, and a
// memberlist does not own its listeners any more. Here the node is isolated
// with a transport that drops the packets that come in.

#[tokio::test]
async fn test_memberlist_probe_node_fallback_tcp() {
    let mut probe_time_max = Duration::ZERO;
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
        probe_time_max = c.probe_interval + Duration::from_millis(20);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |_| {}).await;
    let m3 = host_memberlist(&test_name(), |_| {}).await;

    // Isolate m4 from UDP traffic. This should force the TCP fallback path
    // to be used.
    let m4 = udp_isolated_memberlist().await;

    let a1 = alive_of_no_vsn(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of_no_vsn(&m2);
    m1.alive_node(&a2, None, false);
    let a3 = alive_of_no_vsn(&m3);
    m1.alive_node(&a3, None, false);

    // Make sure m4 is configured with the same protocol version as m1 so
    // the TCP fallback behavior is enabled.
    let a4 = Alive {
        vsn: vec![
            PROTOCOL_VERSION_MIN,
            PROTOCOL_VERSION_MAX,
            m1.config.protocol_version,
            m1.config.delegate_protocol_min,
            m1.config.delegate_protocol_max,
            m1.config.delegate_protocol_version,
        ],
        ..alive_of(&m4)
    };
    m1.alive_node(&a4, None, false);

    // Have node m1 probe m4.
    let start_probe = Instant::now();
    m1.probe_node_by_addr(&a4.node).await;
    let probe_time = start_probe.elapsed();

    // Should be marked alive because of the TCP fallback ping.
    assert_eq!(
        m1.get_node_state(&a4.node),
        NodeStateType::Alive,
        "expect node to be alive"
    );

    // Make sure TCP activity completed in a timely manner.
    assert!(
        probe_time <= probe_time_max,
        "took to long to probe, {:9.6}",
        probe_time.as_secs_f64()
    );

    // Confirm at least one of the peers attempted an indirect probe.
    sleep(probe_time_max).await;
    let (s2, s3) = (seq_no(&m2), seq_no(&m3));
    assert!(s2 == 1 || s3 == 1, "bad seqnos {s2}, {s3}");

    // Now shutdown all inbound TCP traffic to make sure the TCP fallback
    // path properly fails when the node is really unreachable. A shutdown
    // of m4 closes its TCP listener.
    m4.shutdown().await.expect("failed to shut down");

    // Probe again, this time there should be no contact.
    let start_probe = Instant::now();
    m1.probe_node_by_addr(&a4.node).await;
    let probe_time = start_probe.elapsed();

    // Node should be reported suspect.
    assert_eq!(
        m1.get_node_state(&a4.node),
        NodeStateType::Suspect,
        "expect node to be suspect"
    );

    // Make sure TCP activity didn't cause us to wait too long before
    // timing out.
    assert!(
        probe_time <= probe_time_max,
        "took to long to probe, {:9.6}",
        probe_time.as_secs_f64()
    );

    // Confirm at least one of the peers attempted an indirect probe.
    sleep(probe_time_max).await;
    let (s2, s3) = (seq_no(&m2), seq_no(&m3));
    assert!(s2 == 2 || s3 == 2, "bad seqnos {s2}, {s3}");

    shutdown_all(&[&m3, &m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node_fallback_tcp_disabled() {
    let mut probe_time_max = Duration::ZERO;
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
        probe_time_max = c.probe_interval + Duration::from_millis(20);

        // Disable the TCP pings using the config mechanism. The config of a
        // running memberlist cannot be changed, so do it here.
        c.disable_tcp_pings = true;
    })
    .await;
    let m2 = host_memberlist(&test_name(), |_| {}).await;
    let m3 = host_memberlist(&test_name(), |_| {}).await;

    // Isolate m4 from UDP traffic. This would force the TCP fallback path to
    // be used, if it was not disabled.
    let m4 = udp_isolated_memberlist().await;

    let a1 = alive_of_no_vsn(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of_no_vsn(&m2);
    m1.alive_node(&a2, None, false);
    let a3 = alive_of_no_vsn(&m3);
    m1.alive_node(&a3, None, false);

    // Make sure m4 is configured with the same protocol version as m1 so
    // the TCP fallback behavior is enabled.
    let a4 = Alive {
        vsn: vec![
            PROTOCOL_VERSION_MIN,
            PROTOCOL_VERSION_MAX,
            m1.config.protocol_version,
            m1.config.delegate_protocol_min,
            m1.config.delegate_protocol_max,
            m1.config.delegate_protocol_version,
        ],
        ..alive_of(&m4)
    };
    m1.alive_node(&a4, None, false);

    // Have node m1 probe m4.
    let start_probe = Instant::now();
    m1.probe_node_by_addr(&a4.node).await;
    let probe_time = start_probe.elapsed();

    // Node should be reported suspect.
    assert_eq!(
        m1.get_node_state(&a4.node),
        NodeStateType::Suspect,
        "expect node to be suspect"
    );

    // Make sure TCP activity didn't cause us to wait too long before
    // timing out.
    assert!(
        probe_time <= probe_time_max,
        "took to long to probe, {:9.6}",
        probe_time.as_secs_f64()
    );

    // Confirm at least one of the peers attempted an indirect probe.
    sleep(probe_time_max).await;
    let (s2, s3) = (seq_no(&m2), seq_no(&m3));
    assert!(s2 == 1 || s3 == 1, "bad seqnos {s2}, {s3}");

    shutdown_all(&[&m4, &m3, &m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node_fallback_tcp_old_protocol() {
    let mut probe_time_max = Duration::ZERO;
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
        probe_time_max = c.probe_interval + Duration::from_millis(20);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |_| {}).await;
    let m3 = host_memberlist(&test_name(), |_| {}).await;

    // Isolate m4 from UDP traffic. This would force the TCP fallback path to
    // be used, if m4 understood it.
    let m4 = udp_isolated_memberlist().await;

    let a1 = alive_of_no_vsn(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of_no_vsn(&m2);
    m1.alive_node(&a2, None, false);
    let a3 = alive_of_no_vsn(&m3);
    m1.alive_node(&a3, None, false);

    // Set up m4 so that it doesn't understand a version of the protocol
    // that supports TCP pings.
    let a4 = Alive {
        vsn: vec![
            PROTOCOL_VERSION_MIN,
            PROTOCOL_VERSION_2_COMPATIBLE,
            PROTOCOL_VERSION_2_COMPATIBLE,
            m1.config.delegate_protocol_min,
            m1.config.delegate_protocol_max,
            m1.config.delegate_protocol_version,
        ],
        ..alive_of(&m4)
    };
    m1.alive_node(&a4, None, false);

    // Have node m1 probe m4.
    let start_probe = Instant::now();
    m1.probe_node_by_addr(&a4.node).await;
    let probe_time = start_probe.elapsed();

    // Node should be reported suspect.
    assert_eq!(
        m1.get_node_state(&a4.node),
        NodeStateType::Suspect,
        "expect node to be suspect"
    );

    // Make sure TCP activity didn't cause us to wait too long before
    // timing out.
    assert!(
        probe_time <= probe_time_max,
        "took to long to probe, {:9.6}",
        probe_time.as_secs_f64()
    );

    // Confirm at least one of the peers attempted an indirect probe.
    sleep(probe_time_max).await;
    let (s2, s3) = (seq_no(&m2), seq_no(&m3));
    assert!(s2 == 1 || s3 == 1, "bad seqnos {s2}, {s3}");

    shutdown_all(&[&m4, &m3, &m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node_awareness_degraded() {
    let mut probe_time_min = Duration::ZERO;
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
        probe_time_min = 2 * c.probe_interval - Duration::from_millis(50);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
    })
    .await;
    let m3 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
    })
    .await;

    let a1 = alive_of(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of(&m2);
    m1.alive_node(&a2, None, false);
    let a3 = alive_of(&m3);
    m1.alive_node(&a3, None, false);

    let vsn4 = vec![
        PROTOCOL_VERSION_MIN,
        PROTOCOL_VERSION_MAX,
        PROTOCOL_VERSION_MIN,
        1,
        1,
        1,
    ];
    // Node 4 never gets started.
    let a4 = alive_of_missing_node(vsn4);
    m1.alive_node(&a4, None, false);

    // Start the health in a degraded state.
    m1.awareness.apply_delta(1);
    assert_eq!(m1.get_health_score(), 1);

    // Have node m1 probe m4.
    let start_probe = Instant::now();
    m1.probe_node_by_addr(&a4.node).await;
    let probe_time = start_probe.elapsed();

    // Node should be reported suspect.
    assert_eq!(
        m1.get_node_state(&a4.node),
        NodeStateType::Suspect,
        "expect node to be suspect"
    );

    // Make sure we timed out approximately on time (note that we accounted
    // for the slowed-down failure detector in the probe_time_min
    // calculation.
    assert!(
        probe_time >= probe_time_min,
        "probed too quickly, {:9.6}",
        probe_time.as_secs_f64()
    );

    // Confirm at least one of the peers attempted an indirect probe.
    let (s2, s3) = (seq_no(&m2), seq_no(&m3));
    assert!(s2 == 1 || s3 == 1, "bad seqnos {s2}, {s3}");

    // We should have gotten all the nacks, so our score should remain the
    // same, since we didn't get a successful probe.
    assert_eq!(m1.get_health_score(), 1);

    shutdown_all(&[&m3, &m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node_wrong_vsn() {
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
    })
    .await;
    let m3 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
    })
    .await;

    let a1 = alive_of(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of(&m2);
    m1.alive_node(&a2, None, false);
    let a3 = alive_of(&m3);
    m1.alive_node(&a3, None, false);

    let vsn4 = vec![0, 0, 0, 0, 0, 0];
    // Node 4 never gets started.
    let a4 = alive_of_missing_node(vsn4);
    m1.alive_node(&a4, None, false);

    // Start the health in a degraded state.
    m1.awareness.apply_delta(1);
    assert_eq!(m1.get_health_score(), 1);

    // Node 4 must not be known to m1.
    assert!(
        m1.nodes.read().get(&a4.node).is_none(),
        "expect node a4 to be not taken into account, because of its wrong version"
    );

    shutdown_all(&[&m3, &m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node_awareness_improved() {
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |_| {}).await;

    let a1 = alive_of(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of(&m2);
    m1.alive_node(&a2, None, false);

    // Start the health in a degraded state.
    m1.awareness.apply_delta(1);
    assert_eq!(m1.get_health_score(), 1);

    // Have node m1 probe m2.
    m1.probe_node_by_addr(&a2.node).await;

    // Node should be reported alive.
    assert_eq!(
        m1.get_node_state(&a2.node),
        NodeStateType::Alive,
        "expect node to be alive"
    );

    // Our score should have improved since we did a good probe.
    assert_eq!(m1.get_health_score(), 0);

    shutdown_all(&[&m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node_awareness_missed_nack() {
    let mut probe_time_max = Duration::ZERO;
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
        probe_time_max = c.probe_interval + Duration::from_millis(50);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
    })
    .await;

    let a1 = alive_of(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = Alive {
        vsn: m1.config.build_vsn_array(),
        ..alive_of(&m2)
    };
    m1.alive_node(&a2, None, false);

    let vsn = m1.config.build_vsn_array();
    // Node 3 and node 4 never get started.
    let a3 = alive_of_missing_node(vsn.clone());
    m1.alive_node(&a3, None, false);
    let a4 = alive_of_missing_node(vsn);
    m1.alive_node(&a4, None, false);

    // Make sure health looks good.
    assert_eq!(m1.get_health_score(), 0);

    // Have node m1 probe m4, which isn't up
    let start_probe = Instant::now();
    m1.probe_node_by_addr(&a4.node).await;
    let probe_time = start_probe.elapsed();

    // Node should be reported suspect.
    assert_eq!(
        m1.get_node_state(&a4.node),
        NodeStateType::Suspect,
        "expect node to be suspect"
    );

    // Make sure we timed out approximately on time.
    assert!(
        probe_time <= probe_time_max,
        "took to long to probe, {:9.6}",
        probe_time.as_secs_f64()
    );

    // We should have gotten dinged for the missed nack. Note that the code
    // under test is waiting for probe_time_max and then doing some other
    // work before it updates the awareness, so we need to wait some extra
    // time. Rather than just add longer and longer sleeps, we'll retry a
    // few times.
    let m = &m1;
    retry_default(move || async move {
        let score = m.get_health_score();
        if score != 1 {
            return Err(format!(
                "expected health score to decrement on missed nack. want 1, got: {score}"
            ));
        }
        Ok(())
    })
    .await;

    shutdown_all(&[&m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node_awareness_old_protocol() {
    let mut probe_time_max = Duration::ZERO;
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(10);
        c.probe_interval = Duration::from_millis(200);
        probe_time_max = c.probe_interval + Duration::from_millis(20);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |_| {}).await;
    let m3 = host_memberlist(&test_name(), |_| {}).await;

    let a1 = alive_of_no_vsn(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of_no_vsn(&m2);
    m1.alive_node(&a2, None, false);
    let a3 = alive_of_no_vsn(&m3);
    m1.alive_node(&a3, None, false);

    // Node 4 never gets started.
    let a4 = alive_of_missing_node(Vec::new());
    m1.alive_node(&a4, None, false);

    // Make sure health looks good.
    assert_eq!(m1.get_health_score(), 0);

    // Have node m1 probe m4.
    let start_probe = Instant::now();
    m1.probe_node_by_addr(&a4.node).await;
    let probe_time = start_probe.elapsed();

    // Node should be reported suspect.
    assert_eq!(
        m1.get_node_state(&a4.node),
        NodeStateType::Suspect,
        "expect node to be suspect"
    );

    // Make sure we timed out approximately on time.
    assert!(
        probe_time <= probe_time_max,
        "took to long to probe, {:9.6}",
        probe_time.as_secs_f64()
    );

    // Confirm at least one of the peers attempted an indirect probe.
    sleep(probe_time_max).await;
    let (s2, s3) = (seq_no(&m2), seq_no(&m3));
    assert!(s2 == 1 || s3 == 1, "bad seqnos {s2}, {s3}");

    // Since we are using the old protocol here, we should have gotten dinged
    // for a failed health check.
    assert_eq!(m1.get_health_score(), 1);

    shutdown_all(&[&m3, &m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node_buddy() {
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(1);
        c.probe_interval = Duration::from_millis(10);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |_| {}).await;

    let a1 = alive_of(&m1);
    let a2 = alive_of(&m2);

    m1.alive_node(&a1, None, true);
    m1.alive_node(&a2, None, false);
    m2.alive_node(&a2, None, true);

    // A refutation is queued under the address of the node, not under its
    // name. In the Go test the two are the same, so the refutation replaces
    // the alive message that m2 queued about itself just now. Here they
    // differ, so clear the queue to see only the refutation.
    m2.broadcasts.reset();

    // Force the state to suspect so we piggyback a suspect message with the
    // ping. We should see this get refuted later, and the ping will succeed.
    m1.change_node(&a2.node, |n| n.state = NodeStateType::Suspect);
    m1.probe_node_by_addr(&a2.node).await;

    // Make sure a ping was sent.
    assert_eq!(seq_no(&m1), 1, "bad seqno");

    // Check a broadcast is queued. The ack can arrive before m2 has handled
    // the suspect message, so wait for it.
    wait_for_condition(|| {
        let num = m2.broadcasts.num_queued();
        (num == 1, format!("expected only one queued message: {num}"))
    })
    .await;

    // Should be alive msg.
    assert_eq!(
        first_queued_msg_type(&m2),
        MessageType::ALIVE,
        "expected queued alive msg"
    );

    shutdown_all(&[&m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_probe_node() {
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_millis(1);
        c.probe_interval = Duration::from_millis(10);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |_| {}).await;

    let a1 = alive_of_no_vsn(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of_no_vsn(&m2);
    m1.alive_node(&a2, None, false);

    m1.probe_node_by_addr(&a2.node).await;

    // Should be marked alive
    assert_eq!(
        m1.get_node_state(&a2.node),
        NodeStateType::Alive,
        "Expect node to be alive"
    );

    // Should increment seqno
    assert_eq!(seq_no(&m1), 1, "bad seqno");

    shutdown_all(&[&m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_ping() {
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_secs(1);
        c.probe_interval = Duration::from_secs(10);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |_| {}).await;

    let a1 = alive_of_no_vsn(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of_no_vsn(&m2);
    m1.alive_node(&a2, None, false);

    // Do a legit ping.
    let addr = socket_addr(&m2);
    let (rtt, payload) = m1.ping(&a2.node, addr).await.expect("ping failed");
    assert!(rtt > Duration::ZERO, "bad: {rtt:?}");
    assert!(payload.is_empty(), "payload: {payload:?}");

    // This ping has a bad node name so should timeout.
    let res = m1.ping("bad", addr).await;
    assert!(matches!(res, Err(Error::NoPingResponse(_))), "bad: {res:?}");

    shutdown_all(&[&m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_ping_with_payload() {
    let m1 = host_memberlist(&test_name(), |c| {
        c.probe_timeout = Duration::from_secs(1);
        c.probe_interval = Duration::from_secs(10);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |c| {
        c.ping = Some(Arc::new(MockPing));
    })
    .await;

    let a1 = alive_of_no_vsn(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of_no_vsn(&m2);
    m1.alive_node(&a2, None, false);

    // Do a legit ping.
    let addr = socket_addr(&m2);
    let (rtt, payload) = m1.ping(&a2.node, addr).await.expect("ping failed");
    assert!(rtt > Duration::ZERO, "bad: {rtt:?}");
    assert_eq!(payload, DEFAULT_PAYLOAD, "incorrect payload");

    shutdown_all(&[&m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_reset_nodes() {
    let m = get_memberlist(|c| {
        c.gossip_to_the_dead_time = Duration::from_millis(100);
    })
    .await;

    let a1 = alive_msg(&m, "test1", [127, 0, 0, 1], 1);
    m.alive_node(&a1, None, false);
    let a2 = alive_msg(&m, "test2", [127, 0, 0, 2], 1);
    m.alive_node(&a2, None, false);
    let a3 = alive_msg(&m, "test3", [127, 0, 0, 3], 1);
    m.alive_node(&a3, None, false);
    let d = Dead {
        node: "test2".to_string(),
        incarnation: 1,
        ..Default::default()
    };
    m.dead_node(&d);

    m.reset_nodes();
    assert_eq!(m.nodes.read().nodes.len(), 3, "Bad length");
    assert!(
        m.nodes.read().node_map.contains_key("test2"),
        "test2 should not be unmapped"
    );

    sleep(Duration::from_millis(200)).await;
    m.reset_nodes();
    assert_eq!(m.nodes.read().nodes.len(), 2, "Bad length");
    assert!(
        !m.nodes.read().node_map.contains_key("test2"),
        "test2 should be unmapped"
    );

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_next_seq() {
    let m = get_memberlist(|_| {}).await;
    assert_eq!(m.next_seq_no(), 1, "bad sequence no");
    assert_eq!(m.next_seq_no(), 2, "bad sequence no");

    shutdown_all(&[&m]).await;
}

fn ack_handler_exists(m: &Memberlist) -> bool {
    m.ack_handlers.lock().contains_key(&0)
}

#[tokio::test]
async fn test_memberlist_set_probe_channels() {
    let m = get_memberlist(|_| {}).await;

    let (ch, _ack_rx) = mpsc::channel::<AckMessage>(1);
    m.set_probe_channels(0, ch, None, Duration::from_millis(10));

    assert!(ack_handler_exists(&m), "missing handler");

    sleep(Duration::from_millis(20)).await;

    assert!(!ack_handler_exists(&m), "non-reaped handler");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_set_ack_handler() {
    let m = get_memberlist(|_| {}).await;

    let f = |_: Vec<u8>, _: Instant| {};
    m.set_ack_handler(0, f, Duration::from_millis(10));

    assert!(ack_handler_exists(&m), "missing handler");

    sleep(Duration::from_millis(20)).await;

    assert!(!ack_handler_exists(&m), "non-reaped handler");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_invoke_ack_handler() {
    let m = get_memberlist(|_| {}).await;

    // Does nothing
    m.invoke_ack_handler(0, Vec::new(), Instant::now());

    let b = Arc::new(AtomicBool::new(false));
    let f = {
        let b = b.clone();
        move |_: Vec<u8>, _: Instant| b.store(true, Ordering::SeqCst)
    };
    m.set_ack_handler(0, f, Duration::from_millis(10));

    // Should set b
    m.invoke_ack_handler(0, Vec::new(), Instant::now());
    assert!(b.load(Ordering::SeqCst), "b not set");

    assert!(!ack_handler_exists(&m), "non-reaped handler");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_invoke_ack_handler_channel_ack() {
    let m = get_memberlist(|_| {}).await;

    let payload = vec![0u8, 0, 0];

    // Does nothing
    m.invoke_ack_handler(0, payload.clone(), Instant::now());

    let (ack_ch, mut ack_rx) = mpsc::channel::<AckMessage>(1);
    let (nack_ch, mut nack_rx) = mpsc::channel::<()>(1);
    m.set_probe_channels(0, ack_ch, Some(nack_ch), Duration::from_millis(10));

    // Should send message
    m.invoke_ack_handler(0, payload.clone(), Instant::now());

    assert!(nack_rx.try_recv().is_err(), "should not get a nack");
    let v = ack_rx.try_recv().expect("message not sent");
    assert!(v.complete, "Bad value");
    assert_eq!(v.payload, payload, "wrong payload");

    assert!(!ack_handler_exists(&m), "non-reaped handler");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_invoke_ack_handler_channel_nack() {
    let m = get_memberlist(|_| {}).await;

    // Does nothing.
    m.invoke_nack_handler(0);

    let (ack_ch, mut ack_rx) = mpsc::channel::<AckMessage>(1);
    let (nack_ch, mut nack_rx) = mpsc::channel::<()>(1);
    m.set_probe_channels(0, ack_ch, Some(nack_ch), Duration::from_millis(10));

    // Should send message.
    m.invoke_nack_handler(0);

    assert!(ack_rx.try_recv().is_err(), "should not get an ack");
    nack_rx.try_recv().expect("message not sent");

    // Getting a nack doesn't reap the handler so that we can still forward
    // an ack up to the reap time, if we get one.
    assert!(ack_handler_exists(&m), "handler should not be reaped");

    let payload = vec![0u8, 0, 0];
    m.invoke_ack_handler(0, payload.clone(), Instant::now());

    assert!(nack_rx.try_recv().is_err(), "should not get a nack");
    let v = ack_rx.try_recv().expect("message not sent");
    assert!(v.complete, "Bad value");
    assert_eq!(v.payload, payload, "wrong payload");

    assert!(!ack_handler_exists(&m), "non-reaped handler");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_alive_node_new_node() {
    let (events, mut ch) = event_channel();
    let m = get_memberlist(|c| {
        c.events = Some(events);
    })
    .await;

    let a = alive_msg(&m, "test", [127, 0, 0, 1], 1);
    m.alive_node(&a, None, false);

    assert_eq!(m.nodes.read().nodes.len(), 1, "should add node");

    let state = node_state(&m, "test");

    assert_eq!(state.incarnation, 1, "bad incarnation");
    assert_eq!(state.state, NodeStateType::Alive, "bad state");
    assert!(
        state.since_state_change() <= Duration::from_secs(1),
        "bad change delta"
    );

    // Check for a join message
    let e = ch.try_recv().expect("no join message");
    assert_eq!(e.node.name, "test", "bad node name");

    // Check a broad cast is queued
    assert_eq!(m.broadcasts.num_queued(), 1, "expected queued message");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_alive_node_suspect_node() {
    let (events, mut ch) = event_channel();
    let ted = ToggledEventDelegate::new(events);
    let m = get_memberlist(|c| {
        c.events = Some(ted.clone());
    })
    .await;

    let mut a = alive_msg(&m, "test", [127, 0, 0, 1], 1);
    m.alive_node(&a, None, false);

    // Listen only after first join
    ted.toggle(true);

    // Make suspect
    m.change_node("test", |state| {
        state.state = NodeStateType::Suspect;
        state.state_change = hour_earlier(state.state_change);
    });

    // Old incarnation number, should not change
    m.alive_node(&a, None, false);
    assert_eq!(
        m.get_node_state("test"),
        NodeStateType::Suspect,
        "update with old incarnation!"
    );

    // Should reset to alive now
    a.incarnation = 2;
    m.alive_node(&a, None, false);
    let state = node_state(&m, "test");
    assert_eq!(
        state.state,
        NodeStateType::Alive,
        "no update with new incarnation!"
    );

    assert!(
        state.since_state_change() <= Duration::from_secs(1),
        "bad change delta"
    );

    // Check for a no join message
    assert!(ch.try_recv().is_err(), "got bad join message");

    // Check a broad cast is queued
    assert_eq!(m.broadcasts.num_queued(), 1, "expected queued message");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_alive_node_idempotent() {
    let (events, mut ch) = event_channel();
    let ted = ToggledEventDelegate::new(events);
    let m = get_memberlist(|c| {
        c.events = Some(ted.clone());
    })
    .await;

    let mut a = alive_msg(&m, "test", [127, 0, 0, 1], 1);
    m.alive_node(&a, None, false);

    // Listen only after first join
    ted.toggle(true);

    // Remember the time of the last state change
    let state_time = m.get_node_state_change("test");
    assert!(state_time.is_some(), "the join should set the change time");

    // Should stay alive now
    a.incarnation = 2;
    m.alive_node(&a, None, false);
    let state = node_state(&m, "test");
    assert_eq!(state.state, NodeStateType::Alive, "non idempotent");

    assert_eq!(state_time, state.state_change, "should not change state");

    // Check for a no join message
    assert!(ch.try_recv().is_err(), "got bad join message");

    // Check a broad cast is queued
    assert_eq!(
        m.broadcasts.num_queued(),
        1,
        "expected only one queued message"
    );

    shutdown_all(&[&m]).await;
}

// Serf Bug: GH-58, Meta data does not update
#[tokio::test]
async fn test_memberlist_alive_node_change_meta() {
    let (events, mut ch) = event_channel();
    let ted = ToggledEventDelegate::new(events);
    let m = get_memberlist(|c| {
        c.events = Some(ted.clone());
    })
    .await;

    let mut a = alive_msg(&m, "test", [127, 0, 0, 1], 1);
    a.meta = b"val1".to_vec();
    m.alive_node(&a, None, false);

    // Listen only after first join
    ted.toggle(true);

    // Should update the meta data now
    a.incarnation = 2;
    a.meta = b"val2".to_vec();
    m.alive_node(&a, None, false);

    // Check updates
    let state = node_state(&m, "test");
    assert_eq!(state.node.meta, a.meta, "meta did not update");

    // Check for a NotifyUpdate
    let e = ch.try_recv().expect("missing event!");
    assert_eq!(e.event, NodeEventType::NodeUpdate, "bad event: {e:?}");
    assert_eq!(e.node, state.node);
    assert_eq!(e.node.meta, a.meta, "meta did not update");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_alive_node_refute() {
    let m = get_memberlist(|_| {}).await;

    let a = alive_msg(&m, &m.config.name, [127, 0, 0, 1], 1);
    m.alive_node(&a, None, true);

    // Clear queue
    m.broadcasts.reset();

    // Conflicting alive
    let mut s = alive_msg(&m, &m.config.name, [127, 0, 0, 1], 2);
    s.meta = b"foo".to_vec();
    m.alive_node(&s, None, false);

    let state = node_state(&m, &m.config.name);
    assert_eq!(state.state, NodeStateType::Alive, "should still be alive");
    assert!(state.node.meta.is_empty(), "meta should still be nil");

    // Check a broad cast is queued
    let num = m.broadcasts.num_queued();
    assert_eq!(num, 1, "expected only one queued message: {num}");

    // Should be alive mesg
    assert_eq!(
        first_queued_msg_type(&m),
        MessageType::ALIVE,
        "expected queued alive msg"
    );

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_alive_node_conflict() {
    let m = get_memberlist(|c| {
        c.dead_node_reclaim_time = Duration::from_millis(10);
    })
    .await;

    let node_name = "test";
    let other_addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    let mut a = alive_msg(&m, node_name, [127, 0, 0, 1], 1);
    a.port = 8000;
    m.alive_node(&a, None, true);

    // Clear queue
    m.broadcasts.reset();

    // Conflicting alive
    let mut s = alive_msg(&m, node_name, [127, 0, 0, 2], 2);
    s.port = 9000;
    s.meta = b"foo".to_vec();
    m.alive_node(&s, None, false);

    let state = node_state(&m, node_name);
    assert_eq!(state.state, NodeStateType::Alive, "should still be alive");
    assert!(state.node.meta.is_empty(), "meta should still be nil");
    assert_ne!(state.node.addr, other_addr, "address should not be updated");
    assert_ne!(state.node.port, 9000, "port should not be updated");

    // Check a broad cast is queued
    let num = m.broadcasts.num_queued();
    assert_eq!(num, 0, "expected 0 queued messages: {num}");

    // Change the node to dead
    let d = Dead {
        node: node_name.to_string(),
        incarnation: 2,
        ..Default::default()
    };
    m.dead_node(&d);
    m.broadcasts.reset();

    assert_eq!(
        m.get_node_state(node_name),
        NodeStateType::Dead,
        "should be dead"
    );

    sleep(m.config.dead_node_reclaim_time).await;

    // New alive node
    let mut s2 = alive_msg(&m, node_name, [127, 0, 0, 2], 3);
    s2.port = 9000;
    s2.meta = b"foo".to_vec();
    m.alive_node(&s2, None, false);

    let state = node_state(&m, node_name);
    assert_eq!(state.state, NodeStateType::Alive, "should still be alive");
    assert_eq!(state.node.meta, b"foo", "meta should be updated");
    assert_eq!(state.node.addr, other_addr, "address should be updated");
    assert_eq!(state.node.port, 9000, "port should be updated");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_suspect_node_no_node() {
    let m = get_memberlist(|_| {}).await;

    let s = Suspect {
        node: "test".to_string(),
        incarnation: 1,
        ..Default::default()
    };
    m.suspect_node(&s);
    assert!(m.nodes.read().nodes.is_empty(), "don't expect nodes");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_suspect_node() {
    let m = get_memberlist(|c| {
        c.probe_interval = Duration::from_millis(1);
        c.suspicion_mult = 1;
    })
    .await;

    let a = alive_msg(&m, "test", [127, 0, 0, 1], 1);
    m.alive_node(&a, None, false);

    m.change_node("test", |state| {
        state.state_change = hour_earlier(state.state_change);
    });

    let s = Suspect {
        node: "test".to_string(),
        incarnation: 1,
        ..Default::default()
    };
    m.suspect_node(&s);

    assert_eq!(
        m.get_node_state("test"),
        NodeStateType::Suspect,
        "Bad state"
    );

    let change = m
        .get_node_state_change("test")
        .expect("the state change should have a time");
    assert!(
        change.elapsed() <= Duration::from_secs(1),
        "bad change delta"
    );

    // Check a broad cast is queued
    assert_eq!(
        m.broadcasts.num_queued(),
        1,
        "expected only one queued message"
    );

    // Check its a suspect message
    assert_eq!(
        first_queued_msg_type(&m),
        MessageType::SUSPECT,
        "expected queued suspect msg"
    );

    // Wait for the timeout
    sleep(Duration::from_millis(10)).await;

    assert_eq!(m.get_node_state("test"), NodeStateType::Dead, "Bad state");

    let new_change = m
        .get_node_state_change("test")
        .expect("the state change should have a time");
    assert!(
        new_change.elapsed() <= Duration::from_secs(1),
        "bad change delta"
    );
    assert!(new_change > change, "should increment time");

    // Check a broad cast is queued
    assert_eq!(
        m.broadcasts.num_queued(),
        1,
        "expected only one queued message"
    );

    // Check its a dead message
    assert_eq!(
        first_queued_msg_type(&m),
        MessageType::DEAD,
        "expected queued dead msg"
    );

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_suspect_node_double_suspect() {
    let m = get_memberlist(|_| {}).await;

    let a = alive_msg(&m, "test", [127, 0, 0, 1], 1);
    m.alive_node(&a, None, false);

    m.change_node("test", |state| {
        state.state_change = hour_earlier(state.state_change);
    });

    let s = Suspect {
        node: "test".to_string(),
        incarnation: 1,
        ..Default::default()
    };
    m.suspect_node(&s);

    let state = node_state(&m, "test");
    assert_eq!(state.state, NodeStateType::Suspect, "Bad state");

    let change = state.state_change;
    assert!(
        state.since_state_change() <= Duration::from_secs(1),
        "bad change delta"
    );

    // clear the broadcast queue
    m.broadcasts.reset();

    // Suspect again
    m.suspect_node(&s);

    assert_eq!(
        m.get_node_state_change("test"),
        change,
        "unexpected state change"
    );

    // Check a broad cast is queued
    assert_eq!(m.broadcasts.num_queued(), 0, "expected no queued message");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_suspect_node_old_suspect() {
    let m = get_memberlist(|_| {}).await;

    let a = alive_msg(&m, "test", [127, 0, 0, 1], 10);
    m.alive_node(&a, None, false);

    m.change_node("test", |state| {
        state.state_change = hour_earlier(state.state_change);
    });

    // Clear queue
    m.broadcasts.reset();

    let s = Suspect {
        node: "test".to_string(),
        incarnation: 1,
        ..Default::default()
    };
    m.suspect_node(&s);

    assert_eq!(m.get_node_state("test"), NodeStateType::Alive, "Bad state");

    // Check a broad cast is queued
    assert_eq!(m.broadcasts.num_queued(), 0, "expected no queued message");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_suspect_node_refute() {
    let m = get_memberlist(|_| {}).await;

    let a = alive_msg(&m, &m.config.name, [127, 0, 0, 1], 1);
    m.alive_node(&a, None, true);

    // Clear queue
    m.broadcasts.reset();

    // Make sure health is in a good state
    assert_eq!(m.get_health_score(), 0);

    let s = Suspect {
        node: m.config.name.clone(),
        incarnation: 1,
        ..Default::default()
    };
    m.suspect_node(&s);

    assert_eq!(
        m.get_node_state(&m.config.name),
        NodeStateType::Alive,
        "should still be alive"
    );

    // Check a broad cast is queued
    assert_eq!(
        m.broadcasts.num_queued(),
        1,
        "expected only one queued message"
    );

    // Should be alive mesg
    assert_eq!(
        first_queued_msg_type(&m),
        MessageType::ALIVE,
        "expected queued alive msg"
    );

    // Health should have been dinged
    assert_eq!(m.get_health_score(), 1);

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_dead_node_no_node() {
    let m = get_memberlist(|_| {}).await;

    let d = Dead {
        node: "test".to_string(),
        incarnation: 1,
        ..Default::default()
    };
    m.dead_node(&d);
    assert!(m.nodes.read().nodes.is_empty(), "don't expect nodes");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_dead_node_left() {
    let (events, mut ch) = event_channel();
    let m = get_memberlist(|c| {
        c.events = Some(events);
    })
    .await;

    let node_name = "node1";
    let mut s1 = alive_msg(&m, node_name, [127, 0, 0, 1], 1);
    s1.port = 8000;
    m.alive_node(&s1, None, false);

    // Read the join event
    ch.try_recv().expect("no join event");

    let d = Dead {
        node: node_name.to_string(),
        from: node_name.to_string(),
        incarnation: 1,
    };
    m.dead_node(&d);

    // Read the dead event
    ch.try_recv().expect("no leave event");

    assert_eq!(
        m.get_node_state(node_name),
        NodeStateType::Left,
        "Bad state"
    );

    // Check a broad cast is queued
    assert_eq!(
        m.broadcasts.num_queued(),
        1,
        "expected only one queued message"
    );

    // Check its a dead message
    assert_eq!(
        first_queued_msg_type(&m),
        MessageType::DEAD,
        "expected queued dead msg"
    );

    // New alive node
    let mut s2 = alive_msg(&m, node_name, [127, 0, 0, 2], 3);
    s2.port = 9000;
    s2.meta = b"foo".to_vec();
    m.alive_node(&s2, None, false);

    // Read the join event
    ch.try_recv().expect("no join event");

    let state = node_state(&m, node_name);
    assert_eq!(state.state, NodeStateType::Alive, "should still be alive");
    assert_eq!(state.node.meta, b"foo", "meta should be updated");
    assert_eq!(
        state.node.addr,
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
        "address should be updated"
    );
    assert_eq!(state.node.port, 9000, "port should be updated");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_dead_node() {
    let (events, mut ch) = event_channel();
    let m = get_memberlist(|c| {
        c.events = Some(events);
    })
    .await;

    let a = alive_msg(&m, "test", [127, 0, 0, 1], 1);
    m.alive_node(&a, None, false);

    // Read the join event
    ch.try_recv().expect("no join event");

    m.change_node("test", |state| {
        state.state_change = hour_earlier(state.state_change);
    });

    let d = Dead {
        node: "test".to_string(),
        incarnation: 1,
        ..Default::default()
    };
    m.dead_node(&d);

    let state = node_state(&m, "test");
    assert_eq!(state.state, NodeStateType::Dead, "Bad state");

    assert!(
        state.since_state_change() <= Duration::from_secs(1),
        "bad change delta"
    );

    let leave = ch.try_recv().expect("no leave message");
    assert!(
        leave.event == NodeEventType::NodeLeave && leave.node.name == "test",
        "bad node name"
    );

    // Check a broad cast is queued
    assert_eq!(
        m.broadcasts.num_queued(),
        1,
        "expected only one queued message"
    );

    // Check its a dead message
    assert_eq!(
        first_queued_msg_type(&m),
        MessageType::DEAD,
        "expected queued dead msg"
    );

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_dead_node_double() {
    // The config of a running memberlist cannot be changed, so the event
    // delegate is there from the start and is turned on later.
    let (events, mut ch) = event_channel();
    let ted = ToggledEventDelegate::new(events);
    let m = get_memberlist(|c| {
        c.events = Some(ted.clone());
    })
    .await;

    let a = alive_msg(&m, "test", [127, 0, 0, 1], 1);
    m.alive_node(&a, None, false);

    m.change_node("test", |state| {
        state.state_change = hour_earlier(state.state_change);
    });

    let mut d = Dead {
        node: "test".to_string(),
        incarnation: 1,
        ..Default::default()
    };
    m.dead_node(&d);

    // Clear queue
    m.broadcasts.reset();

    // Notify after the first dead
    ted.toggle(true);

    // Should do nothing
    d.incarnation = 2;
    m.dead_node(&d);

    assert!(ch.try_recv().is_err(), "should not get leave");

    // Check a broad cast is queued
    assert_eq!(m.broadcasts.num_queued(), 0, "expected no queued message");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_dead_node_old_dead() {
    let m = get_memberlist(|_| {}).await;

    let a = alive_msg(&m, "test", [127, 0, 0, 1], 10);
    m.alive_node(&a, None, false);

    m.change_node("test", |state| {
        state.state_change = hour_earlier(state.state_change);
    });

    let d = Dead {
        node: "test".to_string(),
        incarnation: 1,
        ..Default::default()
    };
    m.dead_node(&d);

    assert_eq!(m.get_node_state("test"), NodeStateType::Alive, "Bad state");

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_dead_node_alive_replay() {
    let m = get_memberlist(|_| {}).await;

    let a = alive_msg(&m, "test", [127, 0, 0, 1], 10);
    m.alive_node(&a, None, false);

    let d = Dead {
        node: "test".to_string(),
        incarnation: 10,
        ..Default::default()
    };
    m.dead_node(&d);

    // Replay alive at same incarnation
    m.alive_node(&a, None, false);

    // Should remain dead
    if let Some(state) = m.nodes.read().get("test") {
        assert_eq!(state.state, NodeStateType::Dead, "Bad state");
    }

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_dead_node_refute() {
    let m = get_memberlist(|_| {}).await;

    let a = alive_msg(&m, &m.config.name, [127, 0, 0, 1], 1);
    m.alive_node(&a, None, true);

    // Clear queue
    m.broadcasts.reset();

    // Make sure health is in a good state
    assert_eq!(m.get_health_score(), 0);

    let d = Dead {
        node: m.config.name.clone(),
        incarnation: 1,
        ..Default::default()
    };
    m.dead_node(&d);

    assert_eq!(
        m.get_node_state(&m.config.name),
        NodeStateType::Alive,
        "should still be alive"
    );

    // Check a broad cast is queued
    assert_eq!(
        m.broadcasts.num_queued(),
        1,
        "expected only one queued message"
    );

    // Should be alive mesg
    assert_eq!(
        first_queued_msg_type(&m),
        MessageType::ALIVE,
        "expected queued alive msg"
    );

    // We should have been dinged
    assert_eq!(m.get_health_score(), 1);

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_merge_state() {
    // The config of a running memberlist cannot be changed, so the event
    // delegate is there from the start and is turned on later.
    let (events, mut event_ch) = event_channel();
    let ted = ToggledEventDelegate::new(events);
    let m = get_memberlist(|c| {
        c.events = Some(ted.clone());
    })
    .await;

    let a1 = alive_msg(&m, "test1", [127, 0, 0, 1], 1);
    m.alive_node(&a1, None, false);
    let a2 = alive_msg(&m, "test2", [127, 0, 0, 2], 1);
    m.alive_node(&a2, None, false);
    let a3 = alive_msg(&m, "test3", [127, 0, 0, 3], 1);
    m.alive_node(&a3, None, false);

    let s = Suspect {
        node: "test1".to_string(),
        incarnation: 1,
        ..Default::default()
    };
    m.suspect_node(&s);

    let push = |name: &str, addr: [u8; 4], incarnation, state: NodeStateType| PushNodeState {
        name: name.to_string(),
        addr: addr.to_vec(),
        incarnation,
        state: state as i64,
        ..Default::default()
    };
    let remote = [
        push("test1", [127, 0, 0, 1], 2, NodeStateType::Alive),
        push("test2", [127, 0, 0, 2], 1, NodeStateType::Suspect),
        push("test3", [127, 0, 0, 3], 1, NodeStateType::Dead),
        push("test4", [127, 0, 0, 4], 2, NodeStateType::Alive),
    ];

    // Listen for changes
    ted.toggle(true);

    // Merge remote state
    m.merge_state(&remote);

    // Check the states
    let state = node_state(&m, "test1");
    assert!(
        state.state == NodeStateType::Alive && state.incarnation == 2,
        "Bad state {state:?}"
    );

    let state = node_state(&m, "test2");
    assert!(
        state.state == NodeStateType::Suspect && state.incarnation == 1,
        "Bad state {state:?}"
    );

    let state = node_state(&m, "test3");
    assert_eq!(state.state, NodeStateType::Suspect, "Bad state {state:?}");

    let state = node_state(&m, "test4");
    assert!(
        state.state == NodeStateType::Alive && state.incarnation == 2,
        "Bad state {state:?}"
    );

    // Check the channels
    let e = event_ch.try_recv().expect("Expect join");
    assert!(
        e.event == NodeEventType::NodeJoin && e.node.name == "test4",
        "bad node {e:?}"
    );

    if let Ok(e) = event_ch.try_recv() {
        panic!("Unexpect event: {e:?}");
    }

    shutdown_all(&[&m]).await;
}

#[tokio::test]
async fn test_memberlist_gossip() {
    let (events, ch) = event_channel();

    let m1 = host_memberlist(&test_name(), |c| {
        // Set the gossip interval fast enough to get a reasonable test,
        // but slow enough to avoid "sendto: operation not permitted"
        c.gossip_interval = Duration::from_millis(10);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |c| {
        c.events = Some(events);
        // Set the gossip interval fast enough to get a reasonable test,
        // but slow enough to avoid "sendto: operation not permitted"
        c.gossip_interval = Duration::from_millis(10);
    })
    .await;
    let m3 = host_memberlist(&test_name(), |_| {}).await;

    let a1 = alive_of(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of(&m2);
    m1.alive_node(&a2, None, false);
    // In the Go test m3 does not listen on the address of the third node
    // either, so the third node is one that nothing can reach.
    let a3 = alive_of_missing_node(m3.config.build_vsn_array());
    m1.alive_node(&a3, None, false);

    // Gossip should send all this to m2. Retry a few times because it's UDP
    // and timing and stuff makes this flaky without.
    let (m, ch) = (&m1, &ch);
    retry(15, Duration::from_millis(250), move || async move {
        m.gossip().await;

        sleep(Duration::from_millis(3)).await;

        if ch.len() < 3 {
            return Err(format!(
                "expected 3 messages from gossip but only got {}",
                ch.len()
            ));
        }
        Ok(())
    })
    .await;

    shutdown_all(&[&m3, &m2, &m1]).await;
}

#[tokio::test]
async fn test_memberlist_gossip_to_dead() {
    let (events, mut ch) = event_channel();

    let m1 = host_memberlist(&test_name(), |c| {
        c.gossip_interval = Duration::from_millis(1);
        c.gossip_to_the_dead_time = Duration::from_millis(100);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |c| {
        c.events = Some(events);
    })
    .await;

    let a1 = alive_of(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of(&m2);
    m1.alive_node(&a2, None, false);

    // Shouldn't send anything to m2 here, node has been dead for 2x the
    // gossip_to_the_dead_time
    m1.change_node(&a2.node, |state| {
        state.state = NodeStateType::Dead;
        state.state_change = Some(Instant::now() - Duration::from_millis(200));
    });
    m1.gossip().await;

    assert!(
        tokio::time::timeout(Duration::from_millis(50), ch.recv())
            .await
            .is_err(),
        "shouldn't get gossip"
    );

    // Should gossip to m2 because its state has changed within
    // gossip_to_the_dead_time
    m1.change_node(&a2.node, |state| {
        state.state_change = Some(Instant::now() - Duration::from_millis(20));
    });

    let (m, ch) = (&m1, &ch);
    retry(5, Duration::from_millis(10), move || async move {
        m.gossip().await;

        sleep(Duration::from_millis(3)).await;

        if ch.len() < 2 {
            return Err("expected 2 messages from gossip".to_string());
        }
        Ok(())
    })
    .await;

    shutdown_all(&[&m2, &m1]).await;
}

#[test]
fn test_memberlist_failed_remote() {
    let op_error = |op, net| Error::Op {
        op,
        net,
        source: io::Error::other(""),
    };
    // The Go test also has a case for a nil error. An error always has a
    // value here, so that case does not exist.
    let tests = [
        ("normal error", Error::msg(""), false),
        ("net.OpError for file", op_error("", "file"), false),
        ("net.OpError for udp", op_error("", "udp"), false),
        ("net.OpError for udp4", op_error("", "udp4"), false),
        ("net.OpError for udp6", op_error("", "udp6"), false),
        ("net.OpError for tcp", op_error("", "tcp"), false),
        ("net.OpError for tcp4", op_error("", "tcp4"), false),
        ("net.OpError for tcp6", op_error("", "tcp6"), false),
        (
            "net.OpError for tcp with dial",
            op_error("dial", "tcp"),
            true,
        ),
        (
            "net.OpError for tcp with write",
            op_error("write", "tcp"),
            true,
        ),
        (
            "net.OpError for tcp with read",
            op_error("read", "tcp"),
            true,
        ),
        (
            "net.OpError for udp with write",
            op_error("write", "udp"),
            true,
        ),
        (
            "net.OpError for udp with read",
            op_error("read", "udp"),
            false,
        ),
    ];

    for (name, err, expected) in tests {
        let actual = failed_remote(&err);
        assert_eq!(
            actual, expected,
            "{name}: expected {expected}, got {actual}"
        );
    }
}

#[tokio::test]
async fn test_memberlist_push_pull() {
    let sink = register_in_memory_sink();

    let (events, ch) = event_channel();

    let m1 = host_memberlist(&test_name(), |c| {
        c.gossip_interval = Duration::from_secs(10);
        c.push_pull_interval = Duration::from_millis(1);
    })
    .await;
    let m2 = host_memberlist(&test_name(), |c| {
        c.gossip_interval = Duration::from_secs(10);
        c.events = Some(events);
    })
    .await;

    let a1 = alive_of(&m1);
    m1.alive_node(&a1, None, true);
    let a2 = alive_of(&m2);
    m1.alive_node(&a2, None, false);

    // Gossip should send all this to m2. It's UDP though so retry a few
    // times
    let (m, ch) = (&m1, &ch);
    retry(10, Duration::from_millis(50), move || async move {
        m.push_pull().await;

        // Wait longer for metrics to be emitted after push_pull completes
        sleep(Duration::from_millis(20)).await;

        let res = if ch.len() < 2 {
            Err("expected 2 messages from pushPull".to_string())
        } else {
            Ok(())
        };

        let instances = "memberlist.node.instances";
        verify_gauge_exists("memberlist.size.local", &[], sink);
        for state in [
            NodeStateType::Alive,
            NodeStateType::Dead,
            NodeStateType::Left,
            NodeStateType::Suspect,
        ] {
            verify_gauge_exists(instances, &[("node_state", state.metrics_string())], sink);
        }
        res
    })
    .await;

    shutdown_all(&[&m2, &m1]).await;
}

#[tokio::test]
async fn test_verify_protocol() {
    struct Case {
        a_nodes: &'static [[u8; 3]],
        b_nodes: &'static [[u8; 3]],
        expected: bool,
    }
    let cases = [
        // Both running identical everything
        Case {
            a_nodes: &[[0, 0, 0]],
            b_nodes: &[[0, 0, 0]],
            expected: true,
        },
        // One can understand newer, but speaking same protocol
        Case {
            a_nodes: &[[0, 0, 0]],
            b_nodes: &[[0, 1, 0]],
            expected: true,
        },
        // One is speaking outside the range
        Case {
            a_nodes: &[[0, 0, 0]],
            b_nodes: &[[1, 1, 1]],
            expected: false,
        },
        // Transitively outside the range
        Case {
            a_nodes: &[[0, 1, 0], [0, 2, 1]],
            b_nodes: &[[1, 3, 1]],
            expected: false,
        },
        // Multi-node
        Case {
            a_nodes: &[[0, 3, 2], [0, 2, 0]],
            b_nodes: &[[0, 2, 1], [0, 5, 0]],
            expected: true,
        },
    ];

    let core = |n: &[u8; 3]| [n[0], n[1], n[2], 0, 0, 0];
    let app = |n: &[u8; 3]| [0, 0, 0, n[0], n[1], n[2]];

    for tc in &cases {
        let a_core: Vec<[u8; 6]> = tc.a_nodes.iter().map(core).collect();
        let a_app: Vec<[u8; 6]> = tc.a_nodes.iter().map(app).collect();

        let b_core: Vec<[u8; 6]> = tc.b_nodes.iter().map(core).collect();
        let b_app: Vec<[u8; 6]> = tc.b_nodes.iter().map(app).collect();

        // Test core protocol verification
        verify_protocol_single(&a_core, &b_core, tc.expected).await;
        verify_protocol_single(&b_core, &a_core, tc.expected).await;

        //  Test app protocol verification
        verify_protocol_single(&a_app, &b_app, tc.expected).await;
        verify_protocol_single(&b_app, &a_app, tc.expected).await;
    }
}

async fn verify_protocol_single(a: &[[u8; 6]], b: &[[u8; 6]], expect: bool) {
    let m = get_memberlist(|_| {}).await;

    m.nodes.write().nodes = a
        .iter()
        .map(|n| {
            let mut node = Node::new("", LOCALHOST, 0);
            node.pmin = n[0];
            node.pmax = n[1];
            node.pcur = n[2];
            node.dmin = n[3];
            node.dmax = n[4];
            node.dcur = n[5];
            NodeState {
                node,
                incarnation: 0,
                state: NodeStateType::Alive,
                state_change: None,
            }
        })
        .collect();

    let remote: Vec<PushNodeState> = b
        .iter()
        .enumerate()
        .map(|(i, n)| PushNodeState {
            name: format!("node {i}"),
            vsn: n.to_vec(),
            ..Default::default()
        })
        .collect();

    let res = m.verify_protocol(&remote);
    assert_eq!(
        res.is_ok(),
        expect,
        "bad:\nA: {a:?}\nB: {b:?}\nErr: {:?}",
        res.err()
    );

    shutdown_all(&[&m]).await;
}

// This test is not in the Go suite. Go members that bind to 0.0.0.0 send
// their IPv4 address in the 16 byte form, and the Go code compares the
// address bytes. So we show such an address as IPv4, but pass it on in the
// form that we got it in.
#[tokio::test]
async fn test_memberlist_alive_node_ipv4_in_16_bytes() {
    let m = get_memberlist(|_| {}).await;

    let v4 = Ipv4Addr::new(127, 0, 0, 1);
    let mapped = v4.to_ipv6_mapped().octets().to_vec();
    let mut a = alive_msg(&m, "test", [0; 4], 1);
    a.addr = mapped.clone();
    a.port = 8000;
    m.alive_node(&a, None, false);

    // The user sees a plain IPv4 address.
    let state = node_state(&m, "test");
    assert_eq!(state.state, NodeStateType::Alive);
    assert_eq!(state.node.addr, IpAddr::V4(v4));
    assert_eq!(state.node.address(), "127.0.0.1:8000");

    // The state sync sends the bytes that we received.
    assert_eq!(state.node.wire_addr(), mapped);
    let queued = m.broadcasts.get_broadcasts(0, 1400);
    assert_eq!(queued.len(), 1);
    let rebroadcast: Alive = crate::codec::decode(&queued[0][1..]).unwrap();
    assert_eq!(rebroadcast.addr, mapped);

    // The same message again, with a higher incarnation, is not a conflict.
    a.incarnation = 2;
    m.alive_node(&a, None, false);
    assert_eq!(node_state(&m, "test").incarnation, 2);

    // The 4 byte form of the same address is a conflict, like in Go.
    let mut b = alive_msg(&m, "test", [127, 0, 0, 1], 3);
    b.port = 8000;
    m.alive_node(&b, None, false);
    let state = node_state(&m, "test");
    assert_eq!(state.incarnation, 2, "a conflicting alive must be ignored");
    assert_eq!(state.node.wire_addr(), mapped);

    m.shutdown().await.unwrap();
}
