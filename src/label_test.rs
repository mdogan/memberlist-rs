// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of label_test.go from https://github.com/hashicorp/memberlist.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::label::{
    add_label_header_to_packet, add_label_header_to_stream, label_overhead,
    remove_label_header_from_packet, remove_label_header_from_stream,
};
use crate::messages::MessageType;

/// The highest encryption version of the Go implementation. Encryption is
/// not ported, but an encrypted packet or stream starts with this byte, and
/// it must not be taken for a label header.
const MAX_ENCRYPTION_VERSION: u8 = 1;

/// The size of the buffer of the in-memory streams.
const PIPE_BUF_SIZE: usize = 64 * 1024;

/// One piece of a test buffer: a single byte or a run of bytes.
enum Part<'a> {
    Byte(u8),
    Bytes(&'a [u8]),
}

impl From<u8> for Part<'_> {
    fn from(b: u8) -> Self {
        Part::Byte(b)
    }
}

impl From<MessageType> for Part<'_> {
    fn from(t: MessageType) -> Self {
        Part::Byte(t.0)
    }
}

impl<'a> From<&'a str> for Part<'a> {
    fn from(s: &'a str) -> Self {
        Part::Bytes(s.as_bytes())
    }
}

/// Joins bytes, message types and strings into one buffer.
macro_rules! build_buffer {
    ($($part:expr),* $(,)?) => {{
        #[allow(unused_mut)]
        let mut buf: Vec<u8> = Vec::new();
        $(
            match Part::from($part) {
                Part::Byte(b) => buf.push(b),
                Part::Bytes(b) => buf.extend_from_slice(b),
            }
        )*
        buf
    }};
}

#[test]
fn test_add_label_header_to_packet() {
    struct Case<'a> {
        name: &'static str,
        buf: &'a [u8],
        label: &'a str,
        /// The packet, or a part of the error message.
        expect: Result<Vec<u8>, String>,
    }

    let long_label = "a".repeat(255);
    let too_long_label = format!("{long_label}x");

    let cases = [
        Case {
            name: "nil buf with no label",
            buf: &[],
            label: "",
            expect: Ok(Vec::new()),
        },
        Case {
            name: "nil buf with label",
            buf: &[],
            label: "foo",
            expect: Ok(build_buffer!(MessageType::HAS_LABEL, 3, "foo")),
        },
        Case {
            name: "message with label",
            buf: b"something",
            label: "foo",
            expect: Ok(build_buffer!(MessageType::HAS_LABEL, 3, "foosomething")),
        },
        Case {
            name: "message with no label",
            buf: b"something",
            label: "",
            expect: Ok(b"something".to_vec()),
        },
        Case {
            name: "message with almost too long label",
            buf: b"something",
            label: &long_label,
            expect: Ok(build_buffer!(
                MessageType::HAS_LABEL,
                255,
                long_label.as_str(),
                "something"
            )),
        },
        Case {
            name: "label too long by one byte",
            buf: b"something",
            label: &too_long_label,
            expect: Err(format!("label \"{long_label}x\" is too long")),
        },
    ];

    for tc in cases {
        let name = tc.name;
        let got = add_label_header_to_packet(tc.buf, tc.label);
        match tc.expect {
            Err(expect_err) => {
                let err = got.expect_err(name);
                assert!(err.to_string().contains(&expect_err), "{name}: {err}");
            }
            Ok(expect_packet) => {
                let got = got.unwrap_or_else(|err| panic!("{name}: {err}"));
                assert_eq!(got, expect_packet, "{name}");
            }
        }
    }
}

/// The cases of the tests that remove a label header. A packet and a stream
/// with the same content must give the same result. Only the error message
/// differs: it names the `kind` of the input, "packet" or "stream".
struct RemoveCase {
    name: &'static str,
    buf: Vec<u8>,
    /// The label and the rest of the data, or a part of the error message.
    expect: Result<(&'static str, Vec<u8>), String>,
}

fn remove_cases(kind: &str) -> Vec<RemoveCase> {
    let truncated = format!("cannot decode label; {kind} has been truncated");
    let empty = "label header cannot be empty when present".to_string();
    vec![
        RemoveCase {
            name: "empty buf",
            buf: Vec::new(),
            expect: Ok(("", Vec::new())),
        },
        RemoveCase {
            name: "ping with no label",
            buf: build_buffer!(MessageType::PING, "blah"),
            expect: Ok(("", build_buffer!(MessageType::PING, "blah"))),
        },
        RemoveCase {
            // 2021-10: largest standard message type
            name: "error with no label",
            buf: build_buffer!(MessageType::ERR, "blah"),
            expect: Ok(("", build_buffer!(MessageType::ERR, "blah"))),
        },
        RemoveCase {
            // 2021-10: highest encryption version
            name: "v1 encrypt with no label",
            buf: build_buffer!(MAX_ENCRYPTION_VERSION, "blah"),
            expect: Ok(("", build_buffer!(MAX_ENCRYPTION_VERSION, "blah"))),
        },
        RemoveCase {
            name: "buf too small for label",
            buf: build_buffer!(MessageType::HAS_LABEL, "x"),
            expect: Err(truncated.clone()),
        },
        RemoveCase {
            name: "buf too small for label size",
            buf: build_buffer!(MessageType::HAS_LABEL),
            expect: Err(truncated.clone()),
        },
        RemoveCase {
            name: "label empty",
            buf: build_buffer!(MessageType::HAS_LABEL, 0, "x"),
            expect: Err(empty),
        },
        RemoveCase {
            name: "label truncated",
            buf: build_buffer!(MessageType::HAS_LABEL, 2, "x"),
            expect: Err(truncated),
        },
        RemoveCase {
            name: "ping with label",
            buf: build_buffer!(MessageType::HAS_LABEL, 3, "abc", MessageType::PING, "blah"),
            expect: Ok(("abc", build_buffer!(MessageType::PING, "blah"))),
        },
        RemoveCase {
            // 2021-10: largest standard message type
            name: "error with label",
            buf: build_buffer!(MessageType::HAS_LABEL, 3, "abc", MessageType::ERR, "blah"),
            expect: Ok(("abc", build_buffer!(MessageType::ERR, "blah"))),
        },
        RemoveCase {
            // 2021-10: highest encryption version
            name: "v1 encrypt with label",
            buf: build_buffer!(
                MessageType::HAS_LABEL,
                3,
                "abc",
                MAX_ENCRYPTION_VERSION,
                "blah"
            ),
            expect: Ok(("abc", build_buffer!(MAX_ENCRYPTION_VERSION, "blah"))),
        },
    ]
}

#[test]
fn test_remove_label_header_from_packet() {
    for tc in remove_cases("packet") {
        let name = tc.name;
        let got = remove_label_header_from_packet(&tc.buf);
        match tc.expect {
            Err(expect_err) => {
                let err = got.expect_err(name);
                assert!(err.to_string().contains(&expect_err), "{name}: {err}");
            }
            Ok((expect_label, expect_packet)) => {
                let (got_buf, got_label) = got.unwrap_or_else(|err| panic!("{name}: {err}"));
                assert_eq!(got_buf, expect_packet, "{name}");
                assert_eq!(got_label, expect_label, "{name}");
            }
        }
    }
}

#[tokio::test]
async fn test_add_label_header_to_stream() {
    const SUFFIX_DATA: &[u8] = b"EXTRA DATA";

    struct Case<'a> {
        name: &'static str,
        label: &'a str,
        /// The data that the header adds to the stream, or a part of the
        /// error message.
        expect: Result<Vec<u8>, String>,
    }

    let long_label = "a".repeat(255);
    let too_long_label = format!("{long_label}x");

    let cases = [
        Case {
            name: "no label",
            label: "",
            expect: Ok(Vec::new()),
        },
        Case {
            name: "with label",
            label: "foo",
            expect: Ok(build_buffer!(MessageType::HAS_LABEL, 3, "foo")),
        },
        Case {
            name: "almost too long label",
            label: &long_label,
            expect: Ok(build_buffer!(
                MessageType::HAS_LABEL,
                255,
                long_label.as_str()
            )),
        },
        Case {
            name: "label too long by one byte",
            label: &too_long_label,
            expect: Err(format!("label \"{long_label}x\" is too long")),
        },
    ];

    for tc in cases {
        let name = tc.name;
        let (mut server, mut client) = tokio::io::duplex(PIPE_BUF_SIZE);

        // Read everything that arrives on the other end.
        let reader = tokio::spawn(async move {
            let mut buf = Vec::new();
            server.read_to_end(&mut buf).await.map(|_| buf)
        });

        let res = add_label_header_to_stream(&mut client, tc.label).await;
        let expect_data = match tc.expect {
            Err(expect_err) => {
                let err = res.expect_err(name);
                assert!(err.to_string().contains(&expect_err), "{name}: {err}");
                continue;
            }
            Ok(expect_data) => expect_data,
        };
        res.unwrap_or_else(|err| panic!("{name}: {err}"));

        client.write_all(SUFFIX_DATA).await.unwrap();
        client.shutdown().await.unwrap();
        drop(client);

        let mut expect = expect_data;
        expect.extend_from_slice(SUFFIX_DATA);

        let got = reader
            .await
            .unwrap()
            .unwrap_or_else(|err| panic!("{name}: {err}"));
        assert_eq!(got, expect, "{name}");
    }
}

#[tokio::test]
async fn test_remove_label_header_from_stream() {
    for tc in remove_cases("stream") {
        let name = tc.name;
        let (mut server, client) = tokio::io::duplex(PIPE_BUF_SIZE);

        // Write the buffer on the other end and close it. A failed write is
        // not checked, like in the Go test.
        let buf = tc.buf;
        let writer = tokio::spawn(async move {
            let _ = server.write_all(&buf).await;
            let _ = server.shutdown().await;
        });

        let res = remove_label_header_from_stream(Box::new(client)).await;
        match tc.expect {
            Err(expect_err) => {
                let err = match res {
                    Ok(_) => panic!("{name}: expected an error"),
                    Err(err) => err,
                };
                assert!(err.to_string().contains(&expect_err), "{name}: {err}");
            }
            Ok((expect_label, expect_data)) => {
                let (mut new_conn, got_label) = match res {
                    Ok(res) => res,
                    Err(err) => panic!("{name}: {err}"),
                };

                let mut got_buf = Vec::new();
                new_conn
                    .read_to_end(&mut got_buf)
                    .await
                    .unwrap_or_else(|err| panic!("{name}: {err}"));

                assert_eq!(got_buf, expect_data, "{name}");
                assert_eq!(got_label, expect_label, "{name}");
            }
        }
        writer.await.unwrap();
    }
}

#[test]
fn test_label_overhead() {
    assert_eq!(label_overhead(""), 0);
    assert_eq!(label_overhead("a"), 3);
    assert_eq!(label_overhead("abcdefg"), 9);
}
