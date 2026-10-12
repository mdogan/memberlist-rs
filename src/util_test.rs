// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of util_test.go from https://github.com/hashicorp/memberlist.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::codec;
use crate::messages::{
    COMPOUND_HEADER_OVERHEAD, COMPOUND_OVERHEAD, MAX_DECOMPRESSED_BYTES, MAX_PUSH_STATE_BYTES,
    MessageType, Ping, encode,
};
use crate::state::{Node, NodeState, NodeStateType};
use crate::testutil::LOCALHOST;
use crate::util::{
    compress_payload, decode_compound_message, decompress_payload, ensure_port, has_port,
    k_random_nodes, make_compound_message, move_dead_nodes, push_pull_scale, random_offset,
    retransmit_limit, shuffle_nodes, suspicion_timeout,
};

/// Returns a node state with the given name, state and time of the last
/// state change. `age` is how long ago the state changed.
fn node_state(name: &str, state: NodeStateType, age: Option<Duration>) -> NodeState {
    NodeState {
        node: Node::new(name, LOCALHOST, 0),
        incarnation: 0,
        state,
        state_change: age.map(|age| Instant::now().checked_sub(age).expect("time before now")),
    }
}

/// Returns an encoded ping with sequence number 100.
fn encoded_ping() -> Vec<u8> {
    let msg = Ping {
        seq_no: 100,
        ..Default::default()
    };
    encode(MessageType::PING, &msg)
}

#[test]
fn test_util_port_functions() {
    let tests = [
        ("1.2.3.4", false, "1.2.3.4:8301"),
        ("1.2.3.4:1234", true, "1.2.3.4:1234"),
        (
            "2600:1f14:e22:1501:f9a:2e0c:a167:67e8",
            false,
            "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:8301",
        ),
        (
            "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]",
            false,
            "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:8301",
        ),
        (
            "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:1234",
            true,
            "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:1234",
        ),
        ("localhost", false, "localhost:8301"),
        ("localhost:1234", true, "localhost:1234"),
        ("hashicorp.com", false, "hashicorp.com:8301"),
        ("hashicorp.com:1234", true, "hashicorp.com:1234"),
    ];
    for (addr, want_has_port, want_ensure_port) in tests {
        assert_eq!(has_port(addr), want_has_port, "has_port({addr:?})");
        assert_eq!(
            ensure_port(addr, 8301),
            want_ensure_port,
            "ensure_port({addr:?})"
        );
    }
}

#[test]
fn test_encode_decode() {
    let msg = Ping {
        seq_no: 100,
        ..Default::default()
    };
    let buf = encode(MessageType::PING, &msg);
    let out: Ping = codec::decode(&buf[1..]).expect("unexpected err");
    assert_eq!(msg.seq_no, out.seq_no, "bad sequence no");
}

#[test]
fn test_random_offset() {
    let mut vals = HashSet::new();
    for _ in 0..100 {
        let offset = random_offset(2 << 30);
        assert!(vals.insert(offset), "got collision");
    }
}

#[test]
fn test_random_offset_zero() {
    let offset = random_offset(0);
    assert_eq!(offset, 0, "bad offset");
}

#[test]
fn test_suspicion_timeout() {
    let timeouts = [
        (5, Duration::from_millis(1000)),
        (10, Duration::from_millis(1000)),
        (50, Duration::from_millis(1698)),
        (100, Duration::from_millis(2000)),
        (500, Duration::from_millis(2698)),
        (1000, Duration::from_millis(3000)),
    ];
    for (n, expected) in timeouts {
        let timeout = suspicion_timeout(3, n, Duration::from_secs(1)) / 3;
        assert_eq!(timeout, expected, "bad: n = {n}");
    }
}

#[test]
fn test_retransmit_limit() {
    let lim = retransmit_limit(3, 0);
    assert_eq!(lim, 0, "bad val {lim}");
    let lim = retransmit_limit(3, 1);
    assert_eq!(lim, 3, "bad val {lim}");
    let lim = retransmit_limit(3, 99);
    assert_eq!(lim, 6, "bad val {lim}");
}

#[test]
fn test_shuffle_nodes() {
    use NodeStateType::{Alive, Dead};

    // The nodes of the Go test differ only in their state, so about one
    // shuffle in 56 gives an equal list again and fails that test. Here
    // every node also has its own name, and the order of the names is
    // compared.
    let orig: Vec<NodeState> = [Dead, Alive, Alive, Dead, Alive, Alive, Dead, Alive]
        .into_iter()
        .enumerate()
        .map(|(i, state)| node_state(&format!("test{i}"), state, None))
        .collect();
    let names = |nodes: &[NodeState]| -> Vec<String> {
        nodes.iter().map(|n| n.node.name.clone()).collect()
    };
    let mut nodes = orig.clone();

    assert_eq!(names(&nodes), names(&orig), "should match");

    shuffle_nodes(&mut nodes);

    assert_ne!(names(&nodes), names(&orig), "should not match");
}

#[test]
fn test_push_pull_scale() {
    let sec = Duration::from_secs(1);
    for i in 0..=32 {
        let s = push_pull_scale(sec, i);
        assert_eq!(s, sec, "Bad time scale: {s:?}");
    }
    for i in 33..=64 {
        let s = push_pull_scale(sec, i);
        assert_eq!(s, 2 * sec, "Bad time scale: {s:?}");
    }
    for i in 65..=128 {
        let s = push_pull_scale(sec, i);
        assert_eq!(s, 3 * sec, "Bad time scale: {s:?}");
    }
}

#[test]
fn test_move_dead_nodes() {
    use NodeStateType::{Alive, Dead, Left};

    let secs = |n| Some(Duration::from_secs(n));
    let mut nodes = vec![
        node_state("", Dead, secs(20)),
        node_state("", Alive, secs(20)),
        // This dead node should not be moved, as its state changed
        // less than the specified GossipToTheDead time ago
        node_state("", Dead, secs(10)),
        // This left node should not be moved, as its state changed
        // less than the specified GossipToTheDead time ago
        node_state("", Left, secs(10)),
        node_state("", Left, secs(20)),
        node_state("", Alive, secs(20)),
        node_state("", Dead, secs(20)),
        node_state("", Alive, secs(20)),
        node_state("", Left, secs(20)),
    ];

    let idx = move_dead_nodes(&mut nodes, Duration::from_secs(15));
    assert_eq!(idx, 5, "bad index");
    for (i, node) in nodes.iter().enumerate().take(idx) {
        match i {
            // Recently dead node remains at index 2,
            // since nodes are swapped out to move to end.
            2 => assert_eq!(node.state, Dead, "Bad state {i}"),
            // Recently left node should remain at 3
            3 => assert_eq!(node.state, Left, "Bad state {i}"),
            _ => assert_eq!(node.state, Alive, "Bad state {i}"),
        }
    }
    for (i, node) in nodes.iter().enumerate().skip(idx) {
        assert!(node.dead_or_left(), "Bad state {i}");
    }
}

#[test]
fn test_k_random_nodes() {
    let mut nodes = Vec::new();
    for i in 0..90 {
        // Only every third node is alive, the others are in a bad state
        let state = match i % 3 {
            0 => NodeStateType::Alive,
            1 => NodeStateType::Suspect,
            _ => NodeStateType::Dead,
        };
        nodes.push(node_state(&format!("test{i}"), state, None));
    }

    let filter_func = |n: &NodeState| n.node.name == "test0" || n.state != NodeStateType::Alive;

    let s1 = k_random_nodes(3, &nodes, filter_func);
    let s2 = k_random_nodes(3, &nodes, filter_func);
    let s3 = k_random_nodes(3, &nodes, filter_func);

    assert_ne!(s1, s2, "unexpected equal");
    assert_ne!(s1, s3, "unexpected equal");
    assert_ne!(s2, s3, "unexpected equal");

    for s in [&s1, &s2, &s3] {
        assert_eq!(s.len(), 3, "bad len");
        for n in s {
            assert_ne!(n.name, "test0", "Bad name");
            assert_eq!(n.state, NodeStateType::Alive, "Bad state");
        }
    }

    // make sure we test the very-small path
    let nodes = &nodes[..8];
    let s4 = k_random_nodes(3, nodes, filter_func);
    assert_eq!(s4.len(), 2, "expected 2 nodes");
    for n in &s4 {
        assert!(
            n.name == "test3" || n.name == "test6",
            "unexpected node picked"
        );
    }
}

#[test]
fn test_make_compound_message() {
    let buf = encoded_ping();

    let msgs = [&buf, &buf, &buf];
    let compound = make_compound_message(&msgs);

    assert_eq!(
        compound.len(),
        3 * buf.len() + 3 * COMPOUND_OVERHEAD + COMPOUND_HEADER_OVERHEAD,
        "bad len"
    );
}

#[test]
fn test_decode_compound_message() {
    let buf = encoded_ping();

    let msgs = [&buf, &buf, &buf];
    let compound = Bytes::from(make_compound_message(&msgs));

    let (trunc, parts) = decode_compound_message(&compound.slice(1..)).expect("unexpected err");
    assert_eq!(trunc, 0, "should not truncate");
    assert_eq!(parts.len(), 3, "bad parts");
    for p in &parts {
        assert_eq!(p.len(), buf.len(), "bad part len");
    }
}

#[test]
fn test_decode_compound_message_number_of_parts_overflow() {
    let buf = Bytes::from_static(&[0x80]);
    let err = decode_compound_message(&buf).expect_err("expected an error");
    assert_eq!(err.to_string(), "truncated len slice");
}

#[test]
fn test_decode_compound_message_trunc() {
    let buf = encoded_ping();

    let msgs = [&buf, &buf, &buf];
    let compound = Bytes::from(make_compound_message(&msgs));

    let (trunc, parts) = decode_compound_message(&compound.slice(1..38)).expect("unexpected err");
    assert_eq!(trunc, 1, "truncate: {trunc}");
    assert_eq!(parts.len(), 2, "bad parts");
    for p in &parts {
        assert_eq!(p.len(), buf.len(), "bad part len");
    }
}

#[test]
fn test_compress_decompress_payload() {
    let buf = compress_payload(b"testing").expect("unexpected err");

    let decomp = decompress_payload(&buf[1..]).expect("unexpected err");

    assert_eq!(decomp, b"testing", "bad payload: {decomp:?}");
}

#[test]
fn test_decompress_payload_limit() {
    // Over the limit: rejected.
    let buf = compress_payload(&vec![0u8; MAX_DECOMPRESSED_BYTES + 1]).expect("unexpected err");

    let err = decompress_payload(&buf[1..]).expect_err("expected an error");
    assert!(err.to_string().contains("larger than limit"), "bad: {err}");

    // Between the wire limit and the decompressed limit: accepted.
    let buf = compress_payload(&vec![0u8; MAX_PUSH_STATE_BYTES + 1]).expect("unexpected err");

    decompress_payload(&buf[1..]).expect("unexpected err");
}
