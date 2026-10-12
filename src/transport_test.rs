// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of transport_test.go from https://github.com/hashicorp/memberlist.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::memberlist::Memberlist;
use crate::mock_transport::MockNetwork;
use crate::net_transport::tcp_listen;
use crate::testutil::{LOCALHOST, MockDelegate, capture_logs};
use crate::transport::{Address, Stream};

#[tokio::test]
async fn test_transport_join() {
    let net = MockNetwork::new();

    let t1 = Arc::new(net.new_transport("node1"));

    let mut c1 = Config::default_lan();
    c1.name = "node1".to_string();
    c1.transport = Some(t1.clone());
    let m1 = Memberlist::create(c1.clone()).await.expect("err");
    m1.set_alive().expect("err");
    m1.schedule();

    let mut c2 = Config::default_lan();
    c2.name = "node2".to_string();
    c2.transport = Some(Arc::new(net.new_transport("node2")));
    let m2 = Memberlist::create(c2).await.expect("err");
    m2.set_alive().expect("err");
    m2.schedule();

    let num = m2
        .join(&[format!("{}/{}", c1.name, t1.addr())])
        .await
        .expect("err");
    assert_eq!(num, 1, "bad: {num}");

    assert_eq!(m2.members().len(), 2, "bad: {:?}", m2.members());
    assert_eq!(m2.est_num_nodes(), 2, "bad: {:?}", m2.members());

    m2.shutdown().await.expect("err");
    m1.shutdown().await.expect("err");
}

#[tokio::test]
async fn test_transport_send() {
    let net = MockNetwork::new();

    let t1 = Arc::new(net.new_transport("node1"));
    let d1 = MockDelegate::new();

    let mut c1 = Config::default_lan();
    c1.name = "node1".to_string();
    c1.transport = Some(t1.clone());
    c1.delegate = Some(d1.clone());
    let m1 = Memberlist::create(c1.clone()).await.expect("err");
    m1.set_alive().expect("err");
    m1.schedule();

    let mut c2 = Config::default_lan();
    c2.name = "node2".to_string();
    c2.transport = Some(Arc::new(net.new_transport("node2")));
    let m2 = Memberlist::create(c2).await.expect("err");
    m2.set_alive().expect("err");
    m2.schedule();

    let num = m2
        .join(&[format!("{}/{}", c1.name, t1.addr())])
        .await
        .expect("err");
    assert_eq!(num, 1, "bad: {num}");

    // The Go test sends with the deprecated SendTo, SendToUDP and SendToTCP
    // here. They are not ported, so this uses the calls that they forward
    // to: send_to_address without a node name, send_best_effort and
    // send_reliable.
    let a1 = Address {
        addr: t1.addr().to_string(),
        name: String::new(),
    };
    m2.send_to_address(&a1, b"SendTo").await.expect("err");

    let members = m2.members();
    let n1 = members.iter().find(|n| n.name == c1.name).expect("bad");

    m2.send_best_effort(n1, b"SendToUDP").await.expect("err");
    m2.send_reliable(n1, b"SendToTCP").await.expect("err");
    m2.send_best_effort(n1, b"SendBestEffort")
        .await
        .expect("err");
    m2.send_reliable(n1, b"SendReliable").await.expect("err");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut expected = vec![
        "SendTo",
        "SendToUDP",
        "SendToTCP",
        "SendBestEffort",
        "SendReliable",
    ];

    let mut received: Vec<String> = d1
        .get_messages()
        .iter()
        .map(|bs| String::from_utf8_lossy(bs).into_owned())
        .collect();

    // Some of these are packets so often get re-ordered making the test
    // flaky if we assert send ordering. Sort both lists to be tolerant of
    // re-ordering.
    expected.sort_unstable();
    received.sort_unstable();
    assert_eq!(received, expected);

    m2.shutdown().await.expect("err");
    m1.shutdown().await.expect("err");
}

/// Returns a listener whose accept calls always fail.
///
/// The Go test closes a listener and keeps using it. Safe Rust cannot do
/// that, because a listener is only closed when it is dropped. So this takes
/// a UDP socket and wraps it as a TCP listener: a UDP socket does not
/// support accept, so every call fails. A packet that nobody reads keeps the
/// socket readable, so the runtime does not wait before it calls accept.
fn failing_listener() -> tokio::net::TcpListener {
    let socket = std::net::UdpSocket::bind((LOCALHOST, 0)).expect("bind a UDP socket");
    let addr = socket.local_addr().unwrap();
    socket.send_to(b"x", addr).expect("send a packet");
    socket.set_nonblocking(true).unwrap();

    let listener = std::net::TcpListener::from(socket2::Socket::from(socket));
    tokio::net::TcpListener::from_std(listener).expect("wrap the listener")
}

/// Tests that accept errors in the TCP listen task of NetTransport do not
/// result in a tight loop and spam the log. We verify this here by counting
/// the number of entries logged in a given time period.
#[tokio::test]
async fn test_transport_tcp_listen_backoff() {
    // TEST_TIME is the amount of time we will allow the TCP listen task to
    // run. This needs to be long enough that to verify that the maximum
    // delay is in force, but not so long as to be obnoxious when running
    // the test suite.
    const TEST_TIME: Duration = Duration::from_secs(4);

    let (_guard, logs) = capture_logs();
    let (stream_tx, stream_rx) = mpsc::channel::<Stream>(1);
    let shutdown = CancellationToken::new();

    // create a listener that will cause accept calls to fail
    let listener = failing_listener();
    let task = tokio::spawn(tcp_listen(listener, stream_tx, shutdown.clone()));

    // sleep (+yield) for TEST_TIME before asking the accept loop to shut down
    tokio::time::sleep(TEST_TIME).await;
    shutdown.cancel();

    // Verify that the task ended (but without blocking this test). The
    // maximum delay is 1s, so we will give the task 1.25s to loop around and
    // shut down.
    let done = tokio::time::timeout(Duration::from_millis(1250), task).await;
    assert!(
        done.is_ok(),
        "timed out waiting for the TCP listen task to be done after flagging shutdown"
    );

    let logs = logs.contents();
    let mut num_calls = 0;
    for line in logs.lines() {
        num_calls += 1;
        assert!(
            line.contains("memberlist: Error accepting TCP connection"),
            "did not receive expected log message: {line}"
        );
    }

    // In TEST_TIME==4s, we expect to loop approximately 12 times (and log
    // approximately 11 errors), with the following delays (in ms):
    //   0+5+10+20+40+80+160+320+640+1000+1000+1000 == 4275 ms
    // Too few calls suggests that the minimum delay is not in force; too
    // many calls suggests that the maximum delay is not in force or that the
    // back-off isn't working at all.
    // We'll leave a little flex; the important thing here is the asymptotic
    // behavior.
    // If the base delay or the maximum delay in tcp_listen are modified,
    // this test may fail and need to be adjusted.
    assert!(num_calls > 8, "{num_calls} log entries:\n{logs}");
    assert!(num_calls < 14, "{num_calls} log entries:\n{logs}");

    // no connections should have been accepted and sent to the channel
    assert_eq!(stream_rx.len(), 0);
}
