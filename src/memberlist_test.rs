// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of memberlist_test.go from https://github.com/hashicorp/memberlist.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::{Config, parse_cidrs};
use crate::delegate::{
    AliveDelegate, ChannelEventDelegate, ConflictDelegate, MergeDelegate, NodeEvent, NodeEventType,
    PingDelegate,
};
use crate::error::{DelegateError, Result};
use crate::memberlist::{IpPort, Memberlist};
use crate::messages::{PROTOCOL_VERSION_2_COMPATIBLE, PROTOCOL_VERSION_MAX, PROTOCOL_VERSION_MIN};
use crate::state::{Node, NodeState, NodeStateType};
use crate::testutil::*;
use crate::transport::Address;

/// Like `test_config`, for a node in the network 127.0.<network>.0/24.
///
/// The Go tests bind such a node to an address in that network. Here every
/// node binds to 127.0.0.1, so a node in another network only advertises an
/// address in that network. When a member merges the state of a join, it
/// checks its allow-list against the address that each node advertises, so
/// a join gives the same result as in Go. Nothing can reach the advertised
/// address, but these tests only look at the result of a join, which goes
/// over a connection to 127.0.0.1.
///
/// For an alive message in a packet, a member also checks the address that
/// the packet came from. That check is not covered here, because every
/// packet comes from 127.0.0.1.
fn test_config_net(network: u8) -> Config {
    static BIND_NUM: AtomicU8 = AtomicU8::new(10);

    let mut config = test_config();
    if network != 0 {
        let num = BIND_NUM.fetch_add(1, Ordering::SeqCst);
        config.advertise_addr = format!("127.0.{network}.{num}");
    }
    config
}

/// Waits for both the number of members and the estimated number of nodes to
/// reach the expected value. Use this when you need to verify that both
/// converge (e.g., after successful joins).
async fn wait_until_size_and_estimate(m: &Memberlist, expected: usize) {
    retry(15, Duration::from_millis(500), || async {
        if m.num_members() != expected {
            return Err(format!(
                "{} expected to have {} members but had: {:?}",
                m.config.name,
                expected,
                m.members()
            ));
        }
        if m.est_num_nodes() != expected {
            return Err(format!(
                "{} expected to have {} estimated nodes but had: {}",
                m.config.name,
                expected,
                m.est_num_nodes()
            ));
        }
        Ok(())
    })
    .await;
}

/// Joins the given members and checks the number of members after that. An
/// error of the join itself is returned to the caller.
async fn join_and_test_membership(
    m: &Memberlist,
    members_to_join: &[String],
    expected_members: usize,
) -> Result<()> {
    let num = m.join(members_to_join).await?;
    assert_eq!(
        num,
        members_to_join.len(),
        "unexpected {num}, was expecting {} to be joined",
        members_to_join.len()
    );
    // Check the hosts
    assert_eq!(
        m.members().len(),
        expected_members,
        "should have {expected_members} nodes! {:?}",
        m.members()
    );
    Ok(())
}

/// Returns true if a loopback interface has the address ::1.
fn is_ipv6_loopback_available() -> bool {
    const IPV6_LOOPBACK_ADDRESS: IpAddr = IpAddr::V6(Ipv6Addr::LOCALHOST);

    let ifaces = if_addrs::get_if_addrs().expect("list the network interfaces");
    let available = ifaces
        .iter()
        .any(|iface| iface.is_loopback() && iface.ip() == IPV6_LOOPBACK_ADDRESS);
    if !available {
        eprintln!(
            "IPv6 loopback address \"{IPV6_LOOPBACK_ADDRESS}\" not found, disabling tests that require it"
        );
    }
    available
}

/// Returns two different ports on 127.0.0.1 that nothing listens on.
fn two_unused_ports() -> (u16, u16) {
    let first = unused_port();
    loop {
        let second = unused_port();
        if second != first {
            return (first, second);
        }
    }
}

#[tokio::test]
async fn test_smoke_two_nodes() {
    let (_guard, logs) = capture_logs();
    let sink = register_in_memory_sink();
    let d1 = MockDelegate::new();
    d1.set_meta(b"web");
    let mut c1 = test_config();
    c1.delegate = Some(d1.clone());
    let m1 = Memberlist::create(c1).await.unwrap();
    let m2 = Memberlist::create(test_config()).await.unwrap();

    let n = m2.join(&[join_addr(&m1)]).await.unwrap();
    assert_eq!(n, 1);
    wait_until_size(&m1, 2).await;
    wait_until_size(&m2, 2).await;
    let members = m2.members();
    let n1 = members.iter().find(|n| n.name == m1.config.name).unwrap();
    assert_eq!(n1.meta, b"web");

    m2.send_reliable(n1, b"hello").await.unwrap();
    wait_for_condition(|| {
        (
            d1.get_messages() == vec![b"hello".to_vec()],
            "no message".into(),
        )
    })
    .await;

    m1.leave(Duration::from_secs(1)).await.unwrap();
    wait_until_size(&m2, 1).await;
    m1.shutdown().await.unwrap();
    m2.shutdown().await.unwrap();
    wait_until_port_is_free(&m1).await;
    verify_gauge_exists(
        "memberlist.node.instances",
        &[("node_state", "alive")],
        sink,
    );
    assert!(
        logs.contents().contains("Initiating push/pull sync"),
        "{}",
        logs.contents()
    );
}

#[test]
fn test_default_lan_config_protocol_version() {
    let c = Config::default_lan();
    assert_eq!(
        c.protocol_version, PROTOCOL_VERSION_2_COMPATIBLE,
        "should be max: {}",
        c.protocol_version
    );
}

#[tokio::test]
async fn test_create_protocol_version() {
    let cases = [
        ("min", PROTOCOL_VERSION_MIN, false),
        ("max", PROTOCOL_VERSION_MAX, false),
        // TODO(mitchellh): uncommon when we're over 0
        // ("uncommon", PROTOCOL_VERSION_MIN - 1, true),
        ("max+1", PROTOCOL_VERSION_MAX + 1, true),
        ("min-1", PROTOCOL_VERSION_MAX - 1, false),
    ];

    for (name, version, want_err) in cases {
        let mut c = Config::default_lan();
        c.bind_addr = LOCALHOST.to_string();
        c.bind_port = 0; // choose free port
        c.protocol_version = version;

        let res = Memberlist::create(c).await;
        if let Ok(m) = &res {
            m.shutdown().await.unwrap();
        }

        match res {
            Ok(_) if want_err => panic!("{name}: Should've failed with version: {version}"),
            Err(err) if !want_err => panic!("{name}: Version '{version}' error: {err}"),
            _ => {}
        }
    }
}

#[tokio::test]
async fn test_create_check_broadcast_queue_metrics() {
    let sink = register_in_memory_sink();
    let mut c = Config::default_lan();
    c.queue_check_interval = Duration::from_secs(1);
    c.bind_addr = LOCALHOST.to_string();
    c.bind_port = 0; // choose free port

    let m = Memberlist::create(c).await.unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let sample_name = "memberlist.queue.broadcasts";
    verify_sample_exists(sample_name, sink);

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_create() {
    let mut c = test_config();
    c.protocol_version = PROTOCOL_VERSION_MIN;
    c.delegate_protocol_version = 13;
    c.delegate_protocol_min = 12;
    c.delegate_protocol_max = 24;

    let m = Memberlist::create(c.clone()).await.unwrap();

    yield_now().await;

    let members = m.members();
    assert_eq!(members.len(), 1, "bad number of members");

    assert_eq!(
        members[0].pmin, PROTOCOL_VERSION_MIN,
        "bad: {:?}",
        members[0]
    );
    assert_eq!(
        members[0].pmax, PROTOCOL_VERSION_MAX,
        "bad: {:?}",
        members[0]
    );
    assert_eq!(members[0].pcur, c.protocol_version, "bad: {:?}", members[0]);
    assert_eq!(
        members[0].dmin, c.delegate_protocol_min,
        "bad: {:?}",
        members[0]
    );
    assert_eq!(
        members[0].dmax, c.delegate_protocol_max,
        "bad: {:?}",
        members[0]
    );
    assert_eq!(
        members[0].dcur, c.delegate_protocol_version,
        "bad: {:?}",
        members[0]
    );

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_create_shutdown() {
    let m = get_memberlist(|_| {}).await;
    m.schedule();
    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_resolve_addr() {
    // Stands for the IP of the cases that do not check which IP they get.
    const NO_IP: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);

    let m = get_memberlist(|_| {}).await;

    let default_port = m.config.bind_port;

    struct BaseCase {
        name: &'static str,
        input: &'static str,
        expect_err: bool,
        ignore_expect_ip: bool,
        expect: Vec<(IpAddr, u16)>,
    }

    struct TestCase {
        name: String,
        input: String,
        expect_err: bool,
        ignore_expect_ip: bool,
        expect: Vec<IpPort>,
    }

    let ok = |name, input, expect| BaseCase {
        name,
        input,
        expect_err: false,
        ignore_expect_ip: false,
        expect,
    };
    let ok_any_ip = |name, input, port| BaseCase {
        name,
        input,
        expect_err: false,
        ignore_expect_ip: true,
        expect: vec![(NO_IP, port)],
    };
    let err = |name, input| BaseCase {
        name,
        input,
        expect_err: true,
        ignore_expect_ip: false,
        expect: Vec::new(),
    };

    let ipv4_loopback = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let ipv6_loopback = IpAddr::V6(Ipv6Addr::LOCALHOST);
    let ipv6_other = IpAddr::from([
        0x20, 0x01, 0x0d, 0xb8, 0x0a, 0x0b, 0x12, 0xf0, 0, 0, 0, 0, 0, 0, 0, 0x1,
    ]);

    let base_cases = vec![
        ok_any_ip("localhost", "localhost", default_port),
        ok("ipv6 pair", "[::1]:80", vec![(ipv6_loopback, 80)]),
        ok(
            "ipv6 non-pair",
            "[::1]",
            vec![(ipv6_loopback, default_port)],
        ),
        err("hostless port", ":80"),
        ok_any_ip("hostname port combo", "localhost:80", 80),
        err("too high port", "localhost:80000"),
        ok("ipv4 port combo", "127.0.0.1:80", vec![(ipv4_loopback, 80)]),
        ok(
            "ipv6 port combo",
            "[2001:db8:a0b:12f0::1]:80",
            vec![(ipv6_other, 80)],
        ),
        err("ipv4 port combo with empty tag", "/127.0.0.1:80"),
        ok(
            "ipv4 only",
            "127.0.0.1",
            vec![(ipv4_loopback, default_port)],
        ),
        ok(
            "ipv6 only",
            "[2001:db8:a0b:12f0::1]",
            vec![(ipv6_other, default_port)],
        ),
    ];

    // explode the cases to include tagged versions of everything
    let mut cases = Vec::new();
    for tc in base_cases {
        let with_node_name = |node_name: &str| -> Vec<IpPort> {
            tc.expect
                .iter()
                .map(|&(ip, port)| IpPort {
                    ip,
                    port,
                    node_name: node_name.to_string(),
                })
                .collect()
        };
        cases.push(TestCase {
            name: tc.name.to_string(),
            input: tc.input.to_string(),
            expect_err: tc.expect_err,
            ignore_expect_ip: tc.ignore_expect_ip,
            expect: with_node_name(""),
        });
        if !tc.input.contains('/') {
            // don't double tag already tagged cases
            cases.push(TestCase {
                name: format!("{} (tagged)", tc.name),
                input: format!("foo.bar/{}", tc.input),
                expect_err: tc.expect_err,
                ignore_expect_ip: tc.ignore_expect_ip,
                expect: with_node_name("foo.bar"),
            });
        }
    }

    for tc in cases {
        let res = m.resolve_addr(&tc.input).await;
        if tc.expect_err {
            assert!(
                res.is_err(),
                "{}: expected an error, got {:?}",
                tc.name,
                res
            );
        } else {
            let mut got = res.unwrap_or_else(|err| panic!("{}: {err}", tc.name));
            if tc.ignore_expect_ip {
                got.truncate(1);
                for ipp in &mut got {
                    ipp.ip = NO_IP;
                }
            }
            assert_eq!(got, tc.expect, "{}", tc.name);
        }
    }

    m.shutdown().await.unwrap();
}

const DNS_TYPE_A: u16 = 1;
const DNS_TYPE_AAAA: u16 = 28;
const DNS_TYPE_ANY: u16 = 255;
const DNS_CLASS_INET: u16 = 1;

/// The only name that the test DNS server answers for.
const DNS_NAME: &str = "join.service.consul.";

/// Builds the reply of the test DNS server to a query. The reply has an A
/// record and an AAAA record. Returns a message if the query is not the one
/// that the test expects.
fn dns_reply(query: &[u8]) -> std::result::Result<Vec<u8>, String> {
    let truncated = || "bad: the query is truncated".to_string();

    // The header is the id, the flags and four counts, 2 bytes each.
    if query.len() < 12 {
        return Err(truncated());
    }
    let questions = u16::from_be_bytes([query[4], query[5]]);
    if questions != 1 {
        return Err(format!("bad: {questions} questions"));
    }

    // The name of the question is a list of labels. Each label has its
    // length in front of it, and an empty label ends the name.
    let mut name = String::new();
    let mut pos = 12;
    loop {
        let len = *query.get(pos).ok_or_else(truncated)? as usize;
        pos += 1;
        if len == 0 {
            break;
        }
        let label = query.get(pos..pos + len).ok_or_else(truncated)?;
        name.push_str(&String::from_utf8_lossy(label));
        name.push('.');
        pos += len;
    }
    let encoded_name = &query[12..pos];

    // The type and the class of the question come after the name.
    let fixed = query.get(pos..pos + 4).ok_or_else(truncated)?;
    let qtype = u16::from_be_bytes([fixed[0], fixed[1]]);
    if name != DNS_NAME || qtype != DNS_TYPE_ANY {
        return Err(format!("bad: question for {name:?} with type {qtype}"));
    }
    let question = &query[12..pos + 4];

    // This is a response that is authoritative. Recursion is not available.
    // The "recursion desired" bit is copied from the query.
    let recursion_desired = u16::from_be_bytes([query[2], query[3]]) & 0x0100;
    let flags = 0x8400 | recursion_desired;

    let mut resp = Vec::new();
    resp.extend_from_slice(&query[..2]); // the id of the query
    resp.extend_from_slice(&flags.to_be_bytes());
    resp.extend_from_slice(&1u16.to_be_bytes()); // one question
    resp.extend_from_slice(&2u16.to_be_bytes()); // two answers
    resp.extend_from_slice(&[0; 4]); // no authority or additional records
    resp.extend_from_slice(question);

    let a = Ipv4Addr::new(127, 0, 0, 1).octets().to_vec();
    let aaaa = "2001:db8:a0b:12f0::1"
        .parse::<Ipv6Addr>()
        .unwrap()
        .octets()
        .to_vec();
    for (rtype, rdata) in [(DNS_TYPE_A, a), (DNS_TYPE_AAAA, aaaa)] {
        resp.extend_from_slice(encoded_name);
        resp.extend_from_slice(&rtype.to_be_bytes());
        resp.extend_from_slice(&DNS_CLASS_INET.to_be_bytes());
        resp.extend_from_slice(&0u32.to_be_bytes()); // TTL
        resp.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
        resp.extend_from_slice(&rdata);
    }
    Ok(resp)
}

/// Answers one DNS query on a TCP connection. Over TCP, a DNS message has
/// its length in front of it, as 2 bytes.
async fn serve_dns(mut conn: TcpStream) -> std::result::Result<(), String> {
    let mut len = [0u8; 2];
    conn.read_exact(&mut len)
        .await
        .map_err(|err| format!("err: {err}"))?;
    let mut query = vec![0u8; u16::from_be_bytes(len) as usize];
    conn.read_exact(&mut query)
        .await
        .map_err(|err| format!("err: {err}"))?;

    let resp = dns_reply(&query)?;

    let mut framed = Vec::with_capacity(resp.len() + 2);
    framed.extend_from_slice(&(resp.len() as u16).to_be_bytes());
    framed.extend_from_slice(&resp);
    conn.write_all(&framed)
        .await
        .map_err(|err| format!("err: {err}"))
}

/// A tiny DNS server that only speaks TCP. The Go test uses the miekg/dns
/// package for this.
struct DnsServer {
    addr: SocketAddr,
    /// The number of queries that got an answer.
    answered: Arc<AtomicUsize>,
    /// What went wrong while the queries were handled. The test fails if
    /// this is not empty.
    errors: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl DnsServer {
    async fn start() -> DnsServer {
        // Use dynamic port allocation
        let listener = TcpListener::bind((LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let answered = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(Mutex::new(Vec::new()));

        let task = tokio::spawn({
            let answered = answered.clone();
            let errors = errors.clone();
            async move {
                loop {
                    let conn = match listener.accept().await {
                        Ok((conn, _)) => conn,
                        Err(err) => {
                            errors.lock().push(format!("err: {err}"));
                            return;
                        }
                    };
                    let answered = answered.clone();
                    let errors = errors.clone();
                    tokio::spawn(async move {
                        match serve_dns(conn).await {
                            Ok(()) => {
                                answered.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(err) => errors.lock().push(err),
                        }
                    });
                }
            }
        });

        DnsServer {
            addr,
            answered,
            errors,
            task,
        }
    }
}

impl Drop for DnsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A file in the temporary directory that is removed when this is dropped.
struct TempFile(PathBuf);

impl TempFile {
    fn create(content: &str) -> TempFile {
        let path = std::env::temp_dir().join(format!(
            "memberlist-test-{}-{}",
            std::process::id(),
            test_name()
        ));
        std::fs::write(&path, content).unwrap();
        TempFile(path)
    }

    fn path(&self) -> String {
        self.0.to_str().unwrap().to_string()
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn test_memberlist_resolve_addr_tcp_first() {
    let server = DnsServer::start().await;

    // Use the actual bind address of the server
    let tmp_file = TempFile::create(&format!("nameserver {}", server.addr));

    let m = get_memberlist(|c| c.dns_config_path = tmp_file.path()).await;
    m.schedule();

    // Try with and without the trailing dot.
    let hosts = ["join.service.consul.", "join.service.consul"];
    for host in hosts {
        let ips = m.resolve_addr(host).await.unwrap();
        let port = m.config.bind_port;
        let expected = vec![
            // The A record must come back as a plain IPv4 address, and not
            // as an IPv4-mapped IPv6 address.
            IpPort {
                ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                port,
                node_name: String::new(),
            },
            IpPort {
                ip: "2001:db8:a0b:12f0::1".parse().unwrap(),
                port,
                node_name: String::new(),
            },
        ];
        assert_eq!(ips, expected, "{host}");
    }

    // The Go DNS handler fails the test when it gets a query that it does
    // not expect.
    let errors = server.errors.lock().clone();
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(server.answered.load(Ordering::SeqCst), hosts.len());

    // The Go test defers these two calls in this order.
    m.shutdown().await.unwrap();
    m.set_alive().unwrap();
}

#[tokio::test]
async fn test_memberlist_members() {
    let n1 = Node::new("test", LOCALHOST, 0);
    let n2 = Node::new("test2", LOCALHOST, 0);
    let n3 = Node::new("test3", LOCALHOST, 0);

    // The Go test uses an empty Memberlist value. Here we need a real one,
    // which has no nodes because it was not marked alive.
    let m = get_memberlist(|_| {}).await;
    let state = |node: &Node, state: NodeStateType| NodeState {
        node: node.clone(),
        incarnation: 0,
        state,
        state_change: None,
    };
    let nodes = vec![
        state(&n1, NodeStateType::Alive),
        state(&n2, NodeStateType::Dead),
        state(&n3, NodeStateType::Suspect),
    ];
    {
        let mut table = m.nodes.write();
        table.node_map = nodes
            .iter()
            .enumerate()
            .map(|(idx, n)| (n.node.name.clone(), idx))
            .collect();
        table.nodes = nodes;
    }

    let members = m.members();
    assert_eq!(members, vec![n1, n3], "bad members");

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_join() {
    let c1 = test_config();
    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second node
    let c2 = test_config();
    let m2 = Memberlist::create(c2).await.unwrap();

    let num = m2
        .join(&[join_addr(&m1)])
        .await
        .unwrap_or_else(|err| panic!("unexpected err: {err}"));
    assert_eq!(num, 1, "unexpected 1: {num}");

    // Check the hosts
    assert_eq!(
        m2.members().len(),
        2,
        "should have 2 nodes! {:?}",
        m2.members()
    );
    assert_eq!(
        m2.est_num_nodes(),
        2,
        "should have 2 nodes! {:?}",
        m2.members()
    );

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_join_with_labels() {
    let mut c1 = test_config();
    c1.label = "blah".to_string();
    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second node
    let mut c2 = test_config();
    c2.label = "blah".to_string();
    let m2 = Memberlist::create(c2).await.unwrap();

    // same label can join
    {
        let num = m2.join(&[join_addr(&m1)]).await.unwrap();
        assert_eq!(num, 1);

        // Wait for cluster convergence (both members and estimate)
        wait_until_size_and_estimate(&m2, 2).await;
        wait_until_size_and_estimate(&m1, 2).await;
    }

    // Create a third node that uses no label
    let mut c3 = test_config();
    c3.label = String::new();
    let m3 = Memberlist::create(c3).await.unwrap();

    // no label cannot join
    {
        let res = m3.join(&[join_addr(&m1)]).await;
        assert!(res.is_err(), "{res:?}");

        // Verify cluster state remains unchanged after failed join
        wait_until_size_and_estimate(&m3, 1).await;
        wait_until_size_and_estimate(&m2, 2).await;
        wait_until_size_and_estimate(&m1, 2).await;
    }

    // Create a fourth node that uses a mismatched label
    let mut c4 = test_config();
    c4.label = "not-blah".to_string();
    let m4 = Memberlist::create(c4).await.unwrap();

    // mismatched label cannot join
    {
        let res = m4.join(&[join_addr(&m1)]).await;
        assert!(res.is_err(), "{res:?}");

        // Verify cluster state remains unchanged after failed join
        wait_until_size_and_estimate(&m4, 1).await;
        wait_until_size_and_estimate(&m3, 1).await;
        wait_until_size_and_estimate(&m2, 2).await;
        wait_until_size_and_estimate(&m1, 2).await;
    }

    m4.shutdown().await.unwrap();
    m3.shutdown().await.unwrap();
    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_join_different_networks_unique_mask() {
    let mut c1 = test_config_net(0);
    c1.cidrs_allowed = parse_cidrs(&["127.0.0.0/8"]).unwrap();
    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second node
    let mut c2 = test_config_net(1);
    c2.cidrs_allowed = parse_cidrs(&["127.0.0.0/8"]).unwrap();

    let m2 = Memberlist::create(c2).await.unwrap();

    let num = m2
        .join(&[join_addr(&m1)])
        .await
        .unwrap_or_else(|err| panic!("unexpected err: {err}"));
    assert_eq!(num, 1, "unexpected 1: {num}");

    // Check the hosts
    assert_eq!(
        m2.members().len(),
        2,
        "should have 2 nodes! {:?}",
        m2.members()
    );
    assert_eq!(
        m2.est_num_nodes(),
        2,
        "should have 2 nodes! {:?}",
        m2.members()
    );

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_join_different_networks_multi_masks() {
    let mut c1 = test_config_net(0);
    c1.cidrs_allowed = parse_cidrs(&["127.0.0.0/24", "127.0.1.0/24"]).unwrap();
    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second node
    let mut c2 = test_config_net(1);
    c2.cidrs_allowed = parse_cidrs(&["127.0.0.0/24", "127.0.1.0/24"]).unwrap();

    let m2 = Memberlist::create(c2).await.unwrap();

    join_and_test_membership(&m2, &[join_addr(&m1)], 2)
        .await
        .unwrap_or_else(|err| panic!("unexpected err: {err}"));

    // Create a rogue node that allows all networks
    // It should see others, but will not be seen by others
    let mut c3 = test_config_net(2);
    c3.cidrs_allowed = parse_cidrs(&["127.0.0.0/8"]).unwrap();

    let m3 = Memberlist::create(c3).await.unwrap();
    // The rogue can see others, but others cannot see it
    join_and_test_membership(&m3, &[join_addr(&m1)], 3)
        .await
        // For the node itself, everything seems fine, it should see others
        .unwrap_or_else(|err| panic!("unexpected err: {err}"));

    // m1 and m2 should not see newcomer however
    assert_eq!(
        m1.members().len(),
        2,
        "m1 should have 2 nodes! {:?}",
        m1.members()
    );
    assert_eq!(
        m1.est_num_nodes(),
        2,
        "m1 should have 2 est. nodes! {}",
        m1.est_num_nodes()
    );

    assert_eq!(
        m2.members().len(),
        2,
        "m2 should have 2 nodes! {:?}",
        m2.members()
    );
    assert_eq!(
        m2.est_num_nodes(),
        2,
        "m2 should have 2 est. nodes! {}",
        m2.est_num_nodes()
    );

    // Another rogue, this time with a config that denies itself
    let mut c4 = test_config_net(2);
    c4.cidrs_allowed = parse_cidrs(&["127.0.0.0/24", "127.0.1.0/24"]).unwrap();

    let m4 = Memberlist::create(c4).await.unwrap();

    // This time, the node should not even see itself, so 2 expected nodes
    //
    // Like in the Go test, the addresses have no node name here. The config
    // requires node names, so this join fails and its result is ignored.
    let _ = join_and_test_membership(
        &m4,
        &[socket_addr(&m1).to_string(), socket_addr(&m2).to_string()],
        2,
    )
    .await;
    // m1 and m2 should not see newcomer however
    assert_eq!(
        m1.members().len(),
        2,
        "m1 should have 2 nodes! {:?}",
        m1.members()
    );
    assert_eq!(
        m1.est_num_nodes(),
        2,
        "m1 should have 2 est. nodes! {}",
        m1.est_num_nodes()
    );

    assert_eq!(
        m2.members().len(),
        2,
        "m2 should have 2 nodes! {:?}",
        m2.members()
    );
    assert_eq!(
        m2.est_num_nodes(),
        2,
        "m2 should have 2 est. nodes! {}",
        m2.est_num_nodes()
    );

    m4.shutdown().await.unwrap();
    m3.shutdown().await.unwrap();
    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[derive(Default)]
struct CustomMergeDelegate {
    invoked: AtomicBool,
}

impl MergeDelegate for CustomMergeDelegate {
    fn notify_merge(&self, _peers: &[Node]) -> Result<(), DelegateError> {
        eprintln!("Cancel merge");
        self.invoked.store(true, Ordering::SeqCst);
        Err("Custom merge canceled".into())
    }
}

#[tokio::test]
async fn test_memberlist_join_cancel() {
    let mut c1 = test_config();
    let merge1 = Arc::new(CustomMergeDelegate::default());
    c1.merge = Some(merge1.clone());

    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second node
    let mut c2 = test_config();
    let merge2 = Arc::new(CustomMergeDelegate::default());
    c2.merge = Some(merge2.clone());

    let m2 = Memberlist::create(c2).await.unwrap();

    // A join that reaches no node returns an error, so there is no number
    // of nodes to check here.
    let err = match m2.join(&[join_addr(&m1)]).await {
        Ok(num) => panic!("unexpected 0: {num}"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("Custom merge canceled"),
        "unexpected err: {err}"
    );

    // Check the hosts
    assert_eq!(
        m2.members().len(),
        1,
        "should have 1 nodes! {:?}",
        m2.members()
    );
    assert_eq!(
        m1.members().len(),
        1,
        "should have 1 nodes! {:?}",
        m1.members()
    );

    // Check delegate invocation. The first node calls its delegate in the
    // task that handles the connection, which may still be running.
    wait_for_condition(|| {
        (
            merge1.invoked.load(Ordering::SeqCst),
            "should invoke delegate".to_string(),
        )
    })
    .await;
    assert!(
        merge2.invoked.load(Ordering::SeqCst),
        "should invoke delegate"
    );

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

struct CustomAliveDelegate {
    ignore: String,
    count: AtomicUsize,
}

impl CustomAliveDelegate {
    fn new(ignore: &str) -> Arc<CustomAliveDelegate> {
        Arc::new(CustomAliveDelegate {
            ignore: ignore.to_string(),
            count: AtomicUsize::new(0),
        })
    }
}

impl AliveDelegate for CustomAliveDelegate {
    fn notify_alive(&self, peer: &Node) -> Result<(), DelegateError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        if peer.name == self.ignore {
            return Ok(());
        }
        eprintln!("Cancel alive");
        Err("Custom alive canceled".into())
    }
}

#[tokio::test]
async fn test_memberlist_join_cancel_passive() {
    let mut c1 = test_config();
    let alive1 = CustomAliveDelegate::new(&c1.name);
    c1.alive = Some(alive1.clone());

    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second node
    let mut c2 = test_config();
    let alive2 = CustomAliveDelegate::new(&c2.name);
    c2.alive = Some(alive2.clone());

    let m2 = Memberlist::create(c2).await.unwrap();

    let num = m2
        .join(&[join_addr(&m1)])
        .await
        .unwrap_or_else(|err| panic!("err: {err}"));
    assert_eq!(num, 1, "unexpected 1: {num}");

    // Check the hosts
    assert_eq!(
        m2.members().len(),
        1,
        "should have 1 nodes! {:?}",
        m2.members()
    );
    assert_eq!(
        m1.members().len(),
        1,
        "should have 1 nodes! {:?}",
        m1.members()
    );

    // Check delegate invocation
    let count1 = alive1.count.load(Ordering::SeqCst);
    assert_ne!(count1, 0, "should invoke delegate: {count1}");
    let count2 = alive2.count.load(Ordering::SeqCst);
    assert_ne!(count2, 0, "should invoke delegate: {count2}");

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_join_protocol_versions() {
    let c1 = test_config();

    let m1 = Memberlist::create(c1).await.unwrap();

    let c2 = test_config();

    let m2 = Memberlist::create(c2).await.unwrap();

    let mut c3 = test_config();
    c3.protocol_version = PROTOCOL_VERSION_MAX;

    let m3 = Memberlist::create(c3).await.unwrap();

    m1.join(&[join_addr(&m2)]).await.unwrap();

    yield_now().await;

    m1.join(&[join_addr(&m3)]).await.unwrap();

    m3.shutdown().await.unwrap();
    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_leave() {
    let new_config = || {
        let mut c = test_config();
        c.gossip_interval = Duration::from_millis(1);
        c
    };

    let c1 = new_config();

    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second node
    let c2 = new_config();

    let m2 = Memberlist::create(c2).await.unwrap();

    join_and_test_membership(&m2, &[join_addr(&m1)], 2)
        .await
        .unwrap_or_else(|err| panic!("unexpected err: {err}"));

    // Leave
    m1.leave(Duration::from_secs(1)).await.unwrap();

    // Wait for leave
    tokio::time::sleep(Duration::from_millis(10)).await;

    // m1 should think dead
    assert_eq!(m1.members().len(), 1, "should have 1 node");

    assert_eq!(m2.members().len(), 1, "should have 1 node");

    assert_eq!(
        m2.get_node_state(&m1.config.name),
        NodeStateType::Left,
        "bad state"
    );

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_join_shutdown() {
    let new_config = || {
        let mut c = test_config();
        c.probe_interval = Duration::from_millis(1);
        c.probe_timeout = Duration::from_micros(100);
        c.suspicion_max_timeout_mult = 1;
        c
    };

    let c1 = new_config();

    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second node
    let c2 = new_config();

    let m2 = Memberlist::create(c2).await.unwrap();

    let num = m2
        .join(&[join_addr(&m1)])
        .await
        .unwrap_or_else(|err| panic!("unexpected err: {err}"));
    assert_eq!(num, 1, "unexpected 1: {num}");

    // Check the hosts
    assert_eq!(
        m2.members().len(),
        2,
        "should have 2 nodes! {:?}",
        m2.members()
    );

    m1.shutdown().await.unwrap();

    wait_for_condition(|| {
        let n = m2.members().len();
        (n == 1, format!("expected 1 node, got {n}"))
    })
    .await;

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

/// Returns the meta data of each member as a string, by node name.
fn roles_of(members: &[Node]) -> std::collections::HashMap<String, String> {
    members
        .iter()
        .map(|m| {
            (
                m.name.clone(),
                String::from_utf8_lossy(&m.meta).into_owned(),
            )
        })
        .collect()
}

#[tokio::test]
async fn test_memberlist_delegate_meta() {
    let mut c1 = test_config();
    let mock1 = MockDelegate::new();
    mock1.set_meta(b"web");
    c1.delegate = Some(mock1);
    let name1 = c1.name.clone();

    let m1 = Memberlist::create(c1).await.unwrap();

    let mut c2 = test_config();
    let mock2 = MockDelegate::new();
    mock2.set_meta(b"lb");
    c2.delegate = Some(mock2);
    let name2 = c2.name.clone();

    let m2 = Memberlist::create(c2).await.unwrap();

    m1.join(&[join_addr(&m2)]).await.unwrap();

    yield_now().await;

    // Check the roles of members of m1
    let m1m = m1.members();
    assert_eq!(m1m.len(), 2, "bad: {m1m:?}");

    let roles = roles_of(&m1m);

    assert_eq!(
        roles[&name1], "web",
        "bad role for {name1}: {}",
        roles[&name1]
    );

    assert_eq!(
        roles[&name2], "lb",
        "bad role for {name2}: {}",
        roles[&name2]
    );

    // Check the roles of members of m2
    let m2m = m2.members();
    assert_eq!(m2m.len(), 2, "bad: {m2m:?}");

    let roles = roles_of(&m2m);

    assert_eq!(
        roles[&name1], "web",
        "bad role for {name1}: {}",
        roles[&name1]
    );

    assert_eq!(
        roles[&name2], "lb",
        "bad role for {name2}: {}",
        roles[&name2]
    );

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_delegate_meta_update() {
    let mut c1 = test_config();
    let mock1 = MockDelegate::new();
    mock1.set_meta(b"web");
    c1.delegate = Some(mock1.clone());
    let name1 = c1.name.clone();

    let m1 = Memberlist::create(c1).await.unwrap();

    let mut c2 = test_config();
    let mock2 = MockDelegate::new();
    mock2.set_meta(b"lb");
    c2.delegate = Some(mock2.clone());
    let name2 = c2.name.clone();

    let m2 = Memberlist::create(c2).await.unwrap();

    m1.join(&[join_addr(&m2)]).await.unwrap();

    yield_now().await;

    // Update the meta data roles
    mock1.set_meta(b"api");
    mock2.set_meta(b"db");

    m1.update_node(Duration::ZERO).await.unwrap();
    m2.update_node(Duration::ZERO).await.unwrap();

    yield_now().await;

    // Check the updates have propagated

    // Check the roles of members of m1
    let m1m = m1.members();
    assert_eq!(m1m.len(), 2, "bad: {m1m:?}");

    let roles = roles_of(&m1m);

    assert_eq!(
        roles[&name1], "api",
        "bad role for {name1}: {}",
        roles[&name1]
    );

    assert_eq!(
        roles[&name2], "db",
        "bad role for {name2}: {}",
        roles[&name2]
    );

    // Check the roles of members of m2
    let m2m = m2.members();
    assert_eq!(m2m.len(), 2, "bad: {m2m:?}");

    let roles = roles_of(&m2m);

    assert_eq!(
        roles[&name1], "api",
        "bad role for {name1}: {}",
        roles[&name1]
    );

    assert_eq!(
        roles[&name2], "db",
        "bad role for {name2}: {}",
        roles[&name2]
    );

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_user_data() {
    let new_config = || {
        let d = MockDelegate::new();
        let mut c = test_config();
        // Set the gossip/pushpull intervals fast enough to get a reasonable test,
        // but slow enough to avoid "sendto: operation not permitted"
        c.gossip_interval = Duration::from_millis(100);
        c.push_pull_interval = Duration::from_millis(100);
        c.delegate = Some(d.clone());
        (c, d)
    };

    let (c1, d1) = new_config();
    d1.set_state(b"something");

    let m1 = Memberlist::create(c1).await.unwrap();

    let bcasts: Vec<Bytes> = (0..256).map(|i| Bytes::from(i.to_string())).collect();

    // Create a second node
    let (c2, d2) = new_config();

    // Second delegate has things to send
    d2.set_broadcasts(bcasts.clone());
    d2.set_state(b"my state");

    let m2 = Memberlist::create(c2).await.unwrap();

    let num = m2.join(&[join_addr(&m1)]).await.unwrap();
    assert_eq!(num, 1, "unexpected 1: {num}");

    // Check the hosts
    assert_eq!(
        m2.num_members(),
        2,
        "should have 2 nodes! {:?}",
        m2.members()
    );

    let mut expected: Vec<Vec<u8>> = bcasts.iter().map(|b| b.to_vec()).collect();
    expected.sort();

    // Wait for a little while
    retry_default(|| async {
        let mut msgs1 = d1.get_messages();

        // Ensure we got the messages. Ordering of messages is not guaranteed so just
        // check we got them both in either order.
        msgs1.sort();
        if msgs1 != expected {
            return Err(format!(
                "expected the {} broadcasts, got {} messages",
                expected.len(),
                msgs1.len()
            ));
        }

        let rs1 = d1.get_remote_state();
        let rs2 = d2.get_remote_state();

        // Check the push/pull state
        if rs1 != b"my state" {
            return Err(format!("bad state {}", String::from_utf8_lossy(&rs1)));
        }
        if rs2 != b"something" {
            return Err(format!("bad state {}", String::from_utf8_lossy(&rs2)));
        }
        Ok(())
    })
    .await;

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_send_to() {
    let new_config = || {
        let d = MockDelegate::new();
        let mut c = test_config();
        c.gossip_interval = Duration::from_millis(1);
        c.push_pull_interval = Duration::from_millis(1);
        c.delegate = Some(d.clone());
        (c, d)
    };

    let (c1, d1) = new_config();

    let m1 = Memberlist::create(c1).await.unwrap();

    let (mut c2, d2) = new_config();
    // A packet that is sent to an address gets a checksum only if the
    // sender knows a node whose name is the IP of that address. In the Go
    // tests the name of every node is its IP address, so these packets have
    // a checksum. All nodes have the same IP address here, so only one node
    // can have this name. That is enough: both nodes then find it when they
    // send to 127.0.0.1.
    c2.name = LOCALHOST.to_string();

    let m2 = Memberlist::create(c2).await.unwrap();

    let num = m2.join(&[join_addr(&m1)]).await.unwrap();
    assert_eq!(num, 1);

    // Check the hosts
    assert_eq!(
        m2.num_members(),
        2,
        "should have 2 nodes! {:?}",
        m2.members()
    );

    // Try to do a direct send
    let m2_address = Address {
        addr: socket_addr(&m2).to_string(),
        name: m2.config.name.clone(),
    };
    m1.send_to_address(&m2_address, b"ping")
        .await
        .unwrap_or_else(|err| panic!("err: {err}"));

    let m1_address = Address {
        addr: socket_addr(&m1).to_string(),
        name: m1.config.name.clone(),
    };
    m2.send_to_address(&m1_address, b"pong")
        .await
        .unwrap_or_else(|err| panic!("err: {err}"));

    wait_for_condition(|| {
        let msgs = d1.get_messages();
        (
            msgs.len() == 1,
            format!("expected 1 message, got {}", msgs.len()),
        )
    })
    .await;

    let msgs1 = d1.get_messages();
    assert_eq!(msgs1[0], b"pong", "bad msg {:?}", msgs1[0]);

    wait_for_condition(|| {
        let msgs = d2.get_messages();
        (
            msgs.len() == 1,
            format!("expected 1 message, got {}", msgs.len()),
        )
    })
    .await;
    let msgs2 = d2.get_messages();
    assert_eq!(msgs2[0], b"ping", "bad msg {:?}", msgs2[0]);

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_protocol_version() {
    let mut c = test_config();
    c.protocol_version = PROTOCOL_VERSION_MAX;

    let m = Memberlist::create(c).await.unwrap();

    let result = m.protocol_version();
    assert_eq!(result, PROTOCOL_VERSION_MAX, "bad: {result}");

    m.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_memberlist_join_dead_node() {
    let mut c1 = test_config();
    c1.tcp_timeout = Duration::from_millis(50);

    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second "node", which is just a TCP listener that
    // does not ever respond. This is to test our deadlines
    let list =
        std::net::TcpListener::bind((LOCALHOST, 0)).unwrap_or_else(|err| panic!("err: {err}"));
    let addr2 = list.local_addr().unwrap();

    // Ensure we don't hang forever
    let res = tokio::time::timeout(
        Duration::from_millis(100),
        m1.join(&[format!("fake/{addr2}")]),
    )
    .await
    .expect("should have timed out by now");

    // A join that reaches no node returns an error.
    if let Ok(num) = res {
        panic!("unexpected 0: {num}");
    }

    drop(list);
    m1.shutdown().await.unwrap();
}

/// Tests that nodes running different versions of the protocol can successfully
/// discover each other and add themselves to their respective member lists.
#[tokio::test]
async fn test_memberlist_join_protocol_compatibility() {
    async fn test_protocol_version_pair(pv1: u8, pv2: u8) {
        let mut c1 = test_config();
        c1.protocol_version = pv1;

        let m1 = Memberlist::create(c1).await.unwrap();

        let mut c2 = test_config();
        c2.protocol_version = pv2;

        let m2 = Memberlist::create(c2).await.unwrap();

        let num = m2.join(&[join_addr(&m1)]).await.unwrap();
        assert_eq!(num, 1);

        // Wait for cluster convergence
        wait_until_size(&m2, 2).await;
        wait_until_size(&m1, 2).await;

        m2.shutdown().await.unwrap();
        m1.shutdown().await.unwrap();
    }

    test_protocol_version_pair(2, 1).await;
    test_protocol_version_pair(2, 3).await;
    test_protocol_version_pair(3, 2).await;
    test_protocol_version_pair(3, 1).await;
}

#[tokio::test]
async fn test_memberlist_join_ipv6() {
    if !is_ipv6_loopback_available() {
        return;
    }

    // The Go test gives "[::1]" as the bind address, which Go cannot parse,
    // so there the nodes listen on all addresses. Here the nodes listen on
    // the IPv6 loopback address only.
    let mut c1 = Config::default_lan();
    c1.name = "A".to_string();
    c1.bind_addr = "::1".to_string();
    c1.bind_port = 0; // choose free

    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second node
    let mut c2 = Config::default_lan();
    c2.name = "B".to_string();
    c2.bind_addr = "::1".to_string();
    c2.bind_port = 0; // choose free

    let m2 = Memberlist::create(c2).await.unwrap();

    let num = m2
        .join(&[format!(
            "{}/[{}]:{}",
            m1.config.name, m1.config.bind_addr, m1.config.bind_port
        )])
        .await
        .unwrap();
    assert_eq!(num, 1);

    // Check the hosts
    assert_eq!(
        m2.members().len(),
        2,
        "should have 2 nodes! {:?}",
        m2.members()
    );

    assert_eq!(
        m1.members().len(),
        2,
        "should have 2 nodes! {:?}",
        m1.members()
    );

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_advertise_addr() {
    // The Go test binds to one loopback address and advertises another one.
    // Here the advertised address is one that the node does not listen on.
    let bind_addr = LOCALHOST;
    let advertise_addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));

    let (bind_port, advertise_port) = two_unused_ports();

    let mut c = Config::default_lan();
    c.bind_addr = bind_addr.to_string();
    c.bind_port = bind_port;
    c.name = test_name();

    c.advertise_addr = advertise_addr.to_string();
    c.advertise_port = advertise_port;

    let m = Memberlist::create(c).await.unwrap();

    yield_now().await;

    let members = m.members();
    assert_eq!(members.len(), 1);

    assert_eq!(members[0].addr.to_string(), advertise_addr.to_string());
    assert_eq!(members[0].port, advertise_port);

    m.shutdown().await.unwrap();
}

#[derive(Default)]
struct MockConflict {
    /// The existing node and the other node of the last conflict.
    nodes: Mutex<Option<(Node, Node)>>,
}

impl ConflictDelegate for MockConflict {
    fn notify_conflict(&self, existing: &Node, other: &Node) {
        *self.nodes.lock() = Some((existing.clone(), other.clone()));
    }
}

#[tokio::test]
async fn test_memberlist_conflict_delegate() {
    let mut c1 = test_config();
    let mock = Arc::new(MockConflict::default());
    c1.conflict = Some(mock.clone());
    let name1 = c1.name.clone();

    let m1 = Memberlist::create(c1).await.unwrap();

    // Ensure name conflict
    let mut c2 = test_config();
    c2.name = name1;

    let m2 = Memberlist::create(c2).await.unwrap();

    let num = m1.join(&[join_addr(&m2)]).await.unwrap();
    assert_eq!(num, 1);

    yield_now().await;

    // Ensure we were notified
    let Some((existing, other)) = mock.nodes.lock().clone() else {
        panic!("should get notified");
    };
    assert_eq!(existing.name, other.name, "bad: {existing:?} {other:?}");

    m2.shutdown().await.unwrap();
    m1.shutdown().await.unwrap();
}

#[derive(Default)]
struct MockPingState {
    other: Option<Node>,
    rtt: Duration,
    payload: Vec<u8>,
}

#[derive(Default)]
struct MockPing {
    inner: Mutex<MockPingState>,
}

impl MockPing {
    fn get_contents(&self) -> (Option<Node>, Duration, Vec<u8>) {
        let inner = self.inner.lock();
        (inner.other.clone(), inner.rtt, inner.payload.clone())
    }
}

const DEFAULT_PAYLOAD: &[u8] = b"whatever";

impl PingDelegate for MockPing {
    fn ack_payload(&self) -> Vec<u8> {
        DEFAULT_PAYLOAD.to_vec()
    }

    fn notify_ping_complete(&self, other: &Node, rtt: Duration, payload: &[u8]) {
        let mut inner = self.inner.lock();
        inner.other = Some(other.clone());
        inner.rtt = rtt;
        inner.payload = payload.to_vec();
    }
}

#[tokio::test]
async fn test_memberlist_ping_delegate() {
    let new_config = || {
        let mut c = test_config();
        c.probe_interval = Duration::from_millis(100);
        let ping = Arc::new(MockPing::default());
        c.ping = Some(ping.clone());
        (c, ping)
    };

    let (c1, _) = new_config();
    let probe_interval = c1.probe_interval;

    let m1 = Memberlist::create(c1).await.unwrap();

    // Create a second node
    let (c2, mock) = new_config();

    let m2 = Memberlist::create(c2).await.unwrap();

    let num = m2.join(&[join_addr(&m1)]).await.unwrap();
    assert_eq!(num, 1);

    wait_until_size(&m1, 2).await;
    wait_until_size(&m2, 2).await;

    tokio::time::sleep(2 * probe_interval).await;

    m1.shutdown().await.unwrap();
    m2.shutdown().await.unwrap();

    let (m_other, m_rtt, m_payload) = mock.get_contents();

    // Ensure we were notified
    let Some(m_other) = m_other else {
        panic!("should get notified");
    };

    assert_eq!(
        m_other,
        m1.local_node(),
        "not notified about the correct node; expected: {:?}; actual: {:?}",
        m1.local_node(),
        m_other
    );

    assert!(m_rtt > Duration::ZERO, "rtt should be greater than 0");

    assert_eq!(
        m_payload, DEFAULT_PAYLOAD,
        "incorrect payload. expected: {DEFAULT_PAYLOAD:?}; actual: {m_payload:?}"
    );
}

/// Tests the memberlist by creating a cluster of 16 nodes
/// and checking that we get strong convergence of changes.
///
/// The Go test only runs when the INTEG_TESTS environment variable is set.
/// This test is ignored by default. Run it with:
///
///   cargo test --lib test_memberlist_integ -- --ignored
#[tokio::test]
#[ignore = "integration test, run it with `cargo test --lib test_memberlist_integ -- --ignored`"]
async fn test_memberlist_integ() {
    let num = 16;
    let mut members: Vec<Memberlist> = Vec::new();

    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<NodeEvent>();

    for i in 0..num {
        let mut c = Config::default_lan();
        c.name = test_name();
        c.bind_addr = LOCALHOST.to_string();
        c.bind_port = 0; // choose free port
        c.probe_interval = Duration::from_millis(20);
        c.probe_timeout = Duration::from_millis(100);
        c.gossip_interval = Duration::from_millis(20);
        c.push_pull_interval = Duration::from_millis(200);

        if i == 0 {
            c.events = Some(Arc::new(ChannelEventDelegate {
                ch: event_tx.clone(),
            }));
        }

        let m = Memberlist::create(c)
            .await
            .unwrap_or_else(|err| panic!("unexpected err: {err}"));

        if i > 0 {
            let last = &members[i - 1];
            let joined = m
                .join(&[join_addr(last)])
                .await
                .unwrap_or_else(|err| panic!("unexpected err: {err}"));
            assert_ne!(joined, 0, "no node was joined");
        }

        members.push(m);
    }

    // Wait and print debug info
    let break_timer = tokio::time::sleep(Duration::from_millis(250));
    tokio::pin!(break_timer);
    loop {
        tokio::select! {
            Some(e) = event_rx.recv() => {
                if e.event == NodeEventType::NodeJoin {
                    eprintln!("[DEBUG] Node join: {:?} ({})", e.node, members[0].num_members());
                } else {
                    eprintln!("[DEBUG] Node leave: {:?} ({})", e.node, members[0].num_members());
                }
            }
            _ = &mut break_timer => break,
        }
    }

    let mut failures = Vec::new();
    for (idx, m) in members.iter().enumerate() {
        let got = m.num_members();
        if got != num {
            failures.push(format!(
                "bad num members at idx {idx}. Expected {num}. Got {got}."
            ));
        }
    }

    for m in members.iter().rev() {
        m.shutdown().await.unwrap();
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// This is the part of the Go test `TestMemberlist_EncryptedGossipTransition`
// that does not need encryption: a member leaves and shuts down, a new
// member starts with the same name on the same port, and joins again.
#[tokio::test]
async fn test_memberlist_restart_on_same_port() {
    let m1 = Memberlist::create(test_config()).await.unwrap();

    let mut c2 = test_config();
    c2.gossip_interval = Duration::from_millis(20);
    let m2 = Memberlist::create(c2.clone()).await.unwrap();
    assert_eq!(m2.join(&[join_addr(&m1)]).await.unwrap(), 1);
    wait_until_size(&m1, 2).await;
    wait_until_size(&m2, 2).await;

    // Leave and shut down. After that both ports must be free again.
    m2.leave(Duration::from_secs(1)).await.unwrap();
    wait_until_size(&m1, 1).await;
    m2.shutdown().await.unwrap();
    wait_until_port_is_free(&m2).await;

    // Start again with the same name on the same port.
    c2.bind_port = m2.config.bind_port;
    c2.advertise_port = m2.config.bind_port;
    let m3 = Memberlist::create(c2).await.unwrap();
    assert_eq!(m3.config.bind_port, m2.config.bind_port);
    assert_eq!(m3.join(&[join_addr(&m1)]).await.unwrap(), 1);
    wait_until_size(&m1, 2).await;
    wait_until_size(&m3, 2).await;

    m1.shutdown().await.unwrap();
    m3.shutdown().await.unwrap();
}
