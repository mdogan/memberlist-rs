// Copyright (c) 2026 Mehmet Dogan
// SPDX-License-Identifier: MPL-2.0

//! Checks the wire encoding against bytes that the Go implementation wrote.
//!
//! The files in `tests/testdata` are made by
//! `tests/testdata/golden_gen_test.go.txt`. To make them again, copy that file
//! into the root of a checkout of `github.com/hashicorp/memberlist` as
//! `zz_golden_test.go`, run `mkdir -p testdata && go test -run TestZZGolden .`
//! there, and copy the files from its `testdata` directory back here.

use std::collections::HashMap;

use bytes::Bytes;

use crate::codec::{self, Message};
use crate::label::add_label_header_to_packet;
use crate::messages::*;
use crate::state::NodeStateType;
use crate::util::{compress_payload, decompress_payload, make_compound_message};

const GOLDEN: &str = include_str!("../tests/testdata/golden.txt");

/// What the Go side wrote for one case: the bytes themselves, or only their
/// checksum and length when they are long.
enum Golden {
    Bytes(Vec<u8>),
    Crc { crc: u32, len: usize },
}

fn golden() -> HashMap<&'static str, Golden> {
    GOLDEN
        .lines()
        .map(|line| {
            let (name, value) = line.split_once('\t').expect("name and value");
            let golden = match value.strip_prefix("crc:") {
                Some(rest) => {
                    let (crc, len) = rest.split_once(':').expect("crc and length");
                    Golden::Crc {
                        crc: u32::from_str_radix(crc, 16).unwrap(),
                        len: len.parse().unwrap(),
                    }
                }
                None => Golden::Bytes(
                    (0..value.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
                        .collect(),
                ),
            };
            (name, golden)
        })
        .collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[track_caller]
fn check(golden: &HashMap<&'static str, Golden>, name: &str, got: &[u8]) {
    match golden
        .get(name)
        .unwrap_or_else(|| panic!("no golden case {name}"))
    {
        Golden::Bytes(want) => assert_eq!(hex(got), hex(want), "case {name}"),
        Golden::Crc { crc, len } => {
            assert_eq!(got.len(), *len, "case {name}: length");
            assert_eq!(crc32fast::hash(got), *crc, "case {name}: checksum");
        }
    }
}

/// Checks that a message encodes to the golden bytes, and that the golden
/// bytes decode back to the message.
#[track_caller]
fn check_msg<T: Message + PartialEq + std::fmt::Debug>(
    golden: &HashMap<&'static str, Golden>,
    name: &str,
    msg_type: Option<MessageType>,
    msg: &T,
) {
    let got = match msg_type {
        Some(msg_type) => encode(msg_type, msg),
        None => {
            let mut out = Vec::new();
            msg.encode_to(&mut out);
            out
        }
    };
    check(golden, name, &got);

    let body = if msg_type.is_some() {
        &got[1..]
    } else {
        &got[..]
    };
    let back: T = codec::decode(body).unwrap_or_else(|err| panic!("case {name}: {err}"));
    assert_eq!(&back, msg, "case {name}: decode");
}

fn lcg_bytes(n: usize, seed: u32) -> Vec<u8> {
    let mut x = seed;
    (0..n)
        .map(|_| {
            x = x.wrapping_mul(1103515245).wrapping_add(12345);
            (x >> 16) as u8
        })
        .collect()
}

fn pat_bytes(n: usize) -> Vec<u8> {
    (0..n).map(|i| b'a' + (i % 26) as u8).collect()
}

#[test]
fn test_message_keys_are_sorted() {
    // The Go encoder writes the fields of a struct sorted by name.
    for keys in [
        Ping::KEYS,
        IndirectPingReq::KEYS,
        AckResp::KEYS,
        NackResp::KEYS,
        ErrResp::KEYS,
        Suspect::KEYS,
        Alive::KEYS,
        Dead::KEYS,
        PushPullHeader::KEYS,
        UserMsgHeader::KEYS,
        PushNodeState::KEYS,
        Compress::KEYS,
    ] {
        assert!(keys.is_sorted(), "keys are not sorted: {keys:?}");
    }
}

#[test]
fn test_golden_messages() {
    let g = golden();

    check_msg(
        &g,
        "ping_min",
        Some(MessageType::PING),
        &Ping {
            seq_no: 1,
            node: "n1".into(),
            ..Default::default()
        },
    );
    check_msg(
        &g,
        "ping_full",
        Some(MessageType::PING),
        &Ping {
            seq_no: 4000000000,
            node: "node-b".into(),
            source_addr: vec![10, 0, 0, 1],
            source_port: 7946,
            source_node: "node-a".into(),
        },
    );
    check_msg(&g, "ping_empty", Some(MessageType::PING), &Ping::default());
    check_msg(
        &g,
        "indirect_min",
        Some(MessageType::INDIRECT_PING),
        &IndirectPingReq {
            seq_no: 200,
            target: vec![127, 0, 0, 1],
            port: 80,
            node: "x".into(),
            ..Default::default()
        },
    );
    check_msg(
        &g,
        "indirect_full",
        Some(MessageType::INDIRECT_PING),
        &IndirectPingReq {
            seq_no: 70000,
            target: vec![127, 0, 0, 1],
            port: 65535,
            node: "x".into(),
            nack: true,
            source_addr: lcg_bytes(16, 7),
            source_port: 1,
            source_node: "src".into(),
        },
    );
    check_msg(
        &g,
        "ack_nil",
        Some(MessageType::ACK_RESP),
        &AckResp {
            seq_no: 42,
            payload: vec![],
        },
    );
    check_msg(
        &g,
        "ack_payload",
        Some(MessageType::ACK_RESP),
        &AckResp {
            seq_no: 255,
            payload: b"hello".to_vec(),
        },
    );
    check_msg(
        &g,
        "nack",
        Some(MessageType::NACK_RESP),
        &NackResp { seq_no: 256 },
    );
    check_msg(
        &g,
        "err",
        Some(MessageType::ERR),
        &ErrResp {
            error: "boom".into(),
        },
    );
    check_msg(
        &g,
        "suspect",
        Some(MessageType::SUSPECT),
        &Suspect {
            incarnation: 3,
            node: "a".into(),
            from: "b".into(),
        },
    );
    check_msg(
        &g,
        "dead",
        Some(MessageType::DEAD),
        &Dead {
            incarnation: 65536,
            node: "a".into(),
            from: "a".into(),
        },
    );
    check_msg(
        &g,
        "alive_min",
        Some(MessageType::ALIVE),
        &Alive {
            incarnation: 1,
            node: "a".into(),
            addr: vec![127, 0, 0, 1],
            port: 7946,
            ..Default::default()
        },
    );
    check_msg(
        &g,
        "alive_full",
        Some(MessageType::ALIVE),
        &Alive {
            incarnation: 128,
            node: "node-with-a-name-longer-than-31-bytes".into(),
            addr: lcg_bytes(16, 3),
            port: 127,
            meta: b"meta".to_vec(),
            vsn: vec![1, 5, 2, 0, 0, 0],
        },
    );

    let header = |nodes: i64, user_state_len: i64, join: bool| PushPullHeader {
        nodes,
        user_state_len,
        join,
    };
    check_msg(&g, "pp_header_0", None, &header(0, 0, false));
    check_msg(&g, "pp_header_1", None, &header(127, 128, true));
    check_msg(&g, "pp_header_2", None, &header(32767, 32768, false));
    check_msg(
        &g,
        "pp_header_3",
        None,
        &header(2147483647, 2147483648, false),
    );
    check_msg(&g, "pp_header_neg", None, &header(-1, -33, false));
    check_msg(&g, "pp_header_neg2", None, &header(-129, -32769, false));
    check_msg(
        &g,
        "user_header",
        None,
        &UserMsgHeader { user_msg_len: 300 },
    );
    check_msg(
        &g,
        "push_node_min",
        None,
        &PushNodeState {
            name: "a".into(),
            addr: vec![1, 2, 3, 4],
            port: 1,
            ..Default::default()
        },
    );
    check_msg(
        &g,
        "push_node_full",
        None,
        &PushNodeState {
            name: "a".into(),
            addr: vec![1, 2, 3, 4],
            port: 7946,
            meta: vec![0xff],
            incarnation: 9,
            state: NodeStateType::Left as i64,
            vsn: vec![1, 5, 2, 0, 0, 0],
        },
    );
    check_msg(
        &g,
        "compress",
        Some(MessageType::COMPRESS),
        &Compress {
            algo: LZW_ALGO,
            buf: vec![1, 2, 3],
        },
    );

    // The length of a string or of bytes decides which header is used.
    for n in [0usize, 1, 31, 32, 255, 256, 65535, 65536] {
        check_msg(
            &g,
            &format!("err_len_{n}"),
            Some(MessageType::ERR),
            &ErrResp {
                error: String::from_utf8(pat_bytes(n)).unwrap(),
            },
        );
        // Go writes an empty slice that is not nil as an empty string, and
        // we write nil. The decoders on both sides read both as empty.
        if n > 0 {
            check_msg(
                &g,
                &format!("ack_len_{n}"),
                Some(MessageType::ACK_RESP),
                &AckResp {
                    seq_no: 1,
                    payload: pat_bytes(n),
                },
            );
        }
    }
}

#[test]
fn test_golden_decode_empty_bytes() {
    // An empty slice that is not nil, as Go writes it.
    let g = golden();
    for name in ["ack_empty", "ack_len_0"] {
        let Some(Golden::Bytes(b)) = g.get(name) else {
            panic!("no golden case {name}");
        };
        let ack: AckResp = codec::decode(&b[1..]).unwrap();
        assert!(ack.payload.is_empty(), "case {name}");
    }
}

#[test]
fn test_golden_compound_and_label() {
    let g = golden();

    let msgs = [
        Bytes::from_static(b"abc"),
        Bytes::new(),
        Bytes::from_static(b"defgh"),
    ];
    check(&g, "compound", &make_compound_message(&msgs));

    let labeled = add_label_header_to_packet(b"payload", "blue").unwrap();
    check(&g, "label_packet", &labeled);
}

#[test]
fn test_golden_lzw() {
    // Payloads that Go compressed must decompress to the same input here.
    let mixed = [lcg_bytes(30000, 5), pat_bytes(60000)].concat();
    let cases: [(&str, &[u8], Vec<u8>); 6] = [
        (
            "empty",
            include_bytes!("../tests/testdata/lzw_empty.bin"),
            vec![],
        ),
        (
            "one",
            include_bytes!("../tests/testdata/lzw_one.bin"),
            vec![7],
        ),
        (
            "text",
            include_bytes!("../tests/testdata/lzw_text.bin"),
            b"TOBEORNOTTOBEORTOBEORNOT".to_vec(),
        ),
        (
            "zeros",
            include_bytes!("../tests/testdata/lzw_zeros.bin"),
            vec![0; 5000],
        ),
        (
            "random",
            include_bytes!("../tests/testdata/lzw_random.bin"),
            lcg_bytes(40000, 99),
        ),
        (
            "mixed",
            include_bytes!("../tests/testdata/lzw_mixed.bin"),
            mixed,
        ),
    ];

    let g = golden();
    for (name, compressed, want) in cases {
        check(&g, &format!("lzw_{name}"), compressed);
        assert_eq!(compressed[0], MessageType::COMPRESS.0, "case {name}");

        let got =
            decompress_payload(&compressed[1..]).unwrap_or_else(|err| panic!("case {name}: {err}"));
        assert_eq!(got, want, "case {name}: Go to Rust");

        // Our own output must also come back unchanged.
        let ours = compress_payload(&want).unwrap();
        let back = decompress_payload(&ours[1..]).unwrap();
        assert_eq!(back, want, "case {name}: Rust to Rust");
    }
}
