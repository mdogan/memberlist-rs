// Copyright (c) 2026 Mehmet Dogan
// SPDX-License-Identifier: MPL-2.0

//! Checks that Rust members and Go members can be in the same cluster.
//!
//! Each test starts real Go memberlist nodes with the helper program in
//! `tests/gonode`, so the Go toolchain is needed. The tests are skipped when
//! the `go` command is not found. The helper is its own Go module that depends
//! on `github.com/hashicorp/memberlist`, so the first build needs network
//! access to download it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use memberlist_rs::{Config, Delegate, Memberlist, Node, PingDelegate};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// Builds the Go helper once. Returns `None` if there is no Go toolchain.
fn gonode_binary() -> Option<&'static PathBuf> {
    static BINARY: OnceLock<Option<PathBuf>> = OnceLock::new();
    BINARY
        .get_or_init(|| {
            let out = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("gonode");
            let status = std::process::Command::new("go")
                .args(["build", "-o"])
                .arg(&out)
                .arg(".")
                .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/gonode"))
                .status();
            match status {
                Ok(status) if status.success() => Some(out),
                Ok(status) => panic!("building the Go helper failed: {status}"),
                Err(err) => {
                    eprintln!("skipping the interop tests, cannot run `go`: {err}");
                    None
                }
            }
        })
        .as_ref()
}

#[derive(Default)]
struct GoOptions {
    label: &'static str,
    no_compression: bool,
    proto: u8,
    state_len: usize,
}

/// A Go memberlist node in a child process.
struct GoNode {
    name: String,
    port: u16,
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl GoNode {
    /// Starts a Go node. Returns `None` if there is no Go toolchain.
    async fn start(name: &str, opts: GoOptions) -> Option<GoNode> {
        let binary = gonode_binary()?;
        let mut cmd = Command::new(binary);
        cmd.arg(format!("-name={name}"))
            .arg(format!("-label={}", opts.label))
            .arg(format!("-compress={}", !opts.no_compression))
            .arg(format!("-state-len={}", opts.state_len))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        if opts.proto != 0 {
            cmd.arg(format!("-proto={}", opts.proto));
        }
        let mut child = cmd.spawn().expect("start the Go helper");
        let stdin = child.stdin.take().unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();

        let ready = stdout.next_line().await.unwrap().expect("ready line");
        let port = ready
            .strip_prefix("READY ")
            .unwrap_or_else(|| panic!("unexpected first line: {ready}"))
            .parse()
            .unwrap();
        Some(GoNode {
            name: name.to_string(),
            port,
            child,
            stdin,
            stdout,
        })
    }

    fn addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.port))
    }

    /// Sends one command and returns the text after "OK" in the reply.
    async fn cmd(&mut self, line: &str) -> Result<String, String> {
        self.stdin
            .write_all(format!("{line}\n").as_bytes())
            .await
            .unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(20), self.stdout.next_line())
            .await
            .expect("the Go helper did not reply")
            .unwrap()
            .expect("the Go helper closed its output");
        match reply.strip_prefix("OK") {
            Some(rest) => Ok(rest.trim().to_string()),
            None => Err(reply),
        }
    }

    /// Returns the members as (name, address, port, meta).
    async fn members(&mut self) -> Vec<(String, String, u16, Vec<u8>)> {
        let reply = self.cmd("members").await.unwrap();
        reply
            .split(',')
            .filter(|m| !m.is_empty())
            .map(|m| {
                let f: Vec<&str> = m.split('|').collect();
                (
                    f[0].to_string(),
                    f[1].to_string(),
                    f[2].parse().unwrap(),
                    unhex(f[3]),
                )
            })
            .collect()
    }

    async fn msgs(&mut self) -> Vec<Vec<u8>> {
        let reply = self.cmd("msgs").await.unwrap();
        reply
            .split(',')
            .filter(|m| !m.is_empty())
            .map(unhex)
            .collect()
    }

    async fn wait_members(&mut self, want: usize) {
        let start = Instant::now();
        loop {
            let got = self.members().await.len();
            if got == want {
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "{}: {got} members, want {want}",
                self.name
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn kill(&mut self) {
        self.child.kill().await.unwrap();
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

/// Makes the same bytes as `stateBytes` in the Go helper.
fn state_bytes(prefix: &str, n: usize) -> Vec<u8> {
    let mut out = prefix.as_bytes().to_vec();
    let mut x = prefix.len() as u32;
    while out.len() < n {
        x = x.wrapping_mul(1103515245).wrapping_add(12345);
        out.push((x >> 16) as u8);
    }
    out
}

#[derive(Default)]
struct TestDelegate {
    meta: Vec<u8>,
    state: Vec<u8>,
    msgs: Mutex<Vec<Vec<u8>>>,
    remote_state: Mutex<Vec<u8>>,
}

impl Delegate for TestDelegate {
    fn node_meta(&self, _limit: usize) -> Vec<u8> {
        self.meta.clone()
    }

    fn notify_msg(&self, msg: &[u8]) {
        self.msgs.lock().unwrap().push(msg.to_vec());
    }

    fn get_broadcasts(&self, _overhead: usize, _limit: usize) -> Vec<Bytes> {
        Vec::new()
    }

    fn local_state(&self, _join: bool) -> Vec<u8> {
        self.state.clone()
    }

    fn merge_remote_state(&self, buf: &[u8], _join: bool) {
        *self.remote_state.lock().unwrap() = buf.to_vec();
    }
}

struct TestPing {
    payload: Vec<u8>,
}

impl PingDelegate for TestPing {
    fn ack_payload(&self) -> Vec<u8> {
        self.payload.clone()
    }

    fn notify_ping_complete(&self, _other: &Node, _rtt: Duration, _payload: &[u8]) {}
}

/// The Rust counterpart of the configuration in the Go helper.
fn rust_config(name: &str, state_len: usize) -> (Config, Arc<TestDelegate>) {
    let delegate = Arc::new(TestDelegate {
        meta: format!("meta-{name}").into_bytes(),
        state: state_bytes(&format!("state-{name}"), state_len),
        ..Default::default()
    });
    let mut conf = Config::default_local();
    conf.name = name.to_string();
    conf.bind_addr = "127.0.0.1".to_string();
    conf.bind_port = 0;
    conf.delegate = Some(delegate.clone());
    conf.ping = Some(Arc::new(TestPing {
        payload: format!("ack-{name}").into_bytes(),
    }));
    conf.probe_interval = Duration::from_millis(200);
    conf.probe_timeout = Duration::from_millis(100);
    conf.gossip_interval = Duration::from_millis(50);
    // Gossip alone sends an update only a few times in a small cluster, so
    // a member can miss it. The periodic state sync makes up for that.
    conf.push_pull_interval = Duration::from_millis(500);
    (conf, delegate)
}

async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "timed out: {what}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn find<'a>(members: &'a [Node], name: &str) -> &'a Node {
    members
        .iter()
        .find(|n| n.name == name)
        .unwrap_or_else(|| panic!("no member {name} in {members:?}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rust_joins_go() {
    let opts = GoOptions {
        state_len: 64,
        ..Default::default()
    };
    let Some(mut go) = GoNode::start("go-a", opts).await else {
        return;
    };
    let (conf, delegate) = rust_config("rust-a", 64);
    let m = Memberlist::create(conf).await.unwrap();
    let rust_port = m.local_node().port;

    // Join. The Rust node starts the state sync and the Go node answers it.
    let n = m.join(&[go.addr().to_string()]).await.unwrap();
    assert_eq!(n, 1);

    // Both sides know both members, with the right address and meta data.
    let members = m.members();
    assert_eq!(members.len(), 2, "{members:?}");
    let go_node = find(&members, "go-a").clone();
    assert_eq!(go_node.addr.to_string(), "127.0.0.1");
    assert_eq!(go_node.port, go.port);
    assert_eq!(go_node.meta, b"meta-go-a");
    assert_eq!((go_node.pmin, go_node.pmax, go_node.pcur), (1, 5, 2));

    go.wait_members(2).await;
    let go_members = go.members().await;
    let rust_seen = go_members.iter().find(|m| m.0 == "rust-a").unwrap();
    assert_eq!(rust_seen.1, "127.0.0.1");
    assert_eq!(rust_seen.2, rust_port);
    assert_eq!(rust_seen.3, b"meta-rust-a");

    // The user state went both ways in the state sync.
    assert_eq!(
        *delegate.remote_state.lock().unwrap(),
        state_bytes("state-go-a", 64)
    );
    assert_eq!(
        unhex(&go.cmd("remote-state").await.unwrap()),
        state_bytes("state-rust-a", 64)
    );

    // User messages from Rust to Go, over TCP and over UDP.
    m.send_reliable(&go_node, b"rust-tcp").await.unwrap();
    m.send_best_effort(&go_node, b"rust-udp").await.unwrap();
    let start = Instant::now();
    loop {
        let msgs = go.msgs().await;
        if msgs.contains(&b"rust-tcp".to_vec()) && msgs.contains(&b"rust-udp".to_vec()) {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(5), "Go got {msgs:?}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // User messages from Go to Rust, over TCP and over UDP.
    go.cmd(&format!("send-reliable rust-a {}", hex(b"go-tcp")))
        .await
        .unwrap();
    go.cmd(&format!("send-best-effort rust-a {}", hex(b"go-udp")))
        .await
        .unwrap();
    wait_for("messages from Go", || {
        let msgs = delegate.msgs.lock().unwrap();
        msgs.contains(&b"go-tcp".to_vec()) && msgs.contains(&b"go-udp".to_vec())
    })
    .await;

    // A direct ping works in both directions. The Rust side also gets the
    // ack payload of the peer.
    let (_rtt, payload) = m.ping("go-a", go.addr()).await.unwrap();
    assert_eq!(payload, b"ack-go-a");
    go.cmd(&format!("ping rust-a 127.0.0.1:{rust_port}"))
        .await
        .unwrap();

    // Let both sides probe each other for a while. Nobody must be suspected
    // and both must stay healthy.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // The Go side got the ack payload of the Rust node with its probes.
    let payload = go.cmd("ack-payload rust-a").await.unwrap();
    assert_eq!(unhex(&payload), b"ack-rust-a");
    assert_eq!(m.num_members(), 2);
    assert_eq!(go.members().await.len(), 2);
    assert_eq!(m.get_health_score(), 0);
    assert_eq!(go.cmd("health").await.unwrap(), "0");

    // The Go node leaves and the Rust node sees it.
    go.cmd("leave").await.unwrap();
    wait_for("the Go node to leave", || m.num_members() == 1).await;

    m.shutdown().await.unwrap();
    go.cmd("quit").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_go_joins_rust() {
    let opts = GoOptions {
        state_len: 100,
        ..Default::default()
    };
    let Some(mut go) = GoNode::start("go-a", opts).await else {
        return;
    };
    let (conf, delegate) = rust_config("rust-a", 100);
    let m = Memberlist::create(conf).await.unwrap();
    let rust_port = m.local_node().port;

    // Join. The Go node starts the state sync and the Rust node answers it.
    let reply = go
        .cmd(&format!("join 127.0.0.1:{rust_port}"))
        .await
        .unwrap();
    assert_eq!(reply, "1");

    go.wait_members(2).await;
    wait_for("the Rust node to see the Go node", || m.num_members() == 2).await;
    assert_eq!(find(&m.members(), "go-a").meta, b"meta-go-a");

    wait_for("the state of the Go node", || {
        *delegate.remote_state.lock().unwrap() == state_bytes("state-go-a", 100)
    })
    .await;
    assert_eq!(
        unhex(&go.cmd("remote-state").await.unwrap()),
        state_bytes("state-rust-a", 100)
    );

    // The Rust node leaves and the Go node sees it.
    m.leave(Duration::from_secs(2)).await.unwrap();
    go.wait_members(1).await;

    m.shutdown().await.unwrap();
    go.cmd("quit").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failure_detection() {
    let Some(mut go_a) = GoNode::start("go-a", GoOptions::default()).await else {
        return;
    };
    let mut go_b = GoNode::start("go-b", GoOptions::default()).await.unwrap();
    let (conf, _delegate) = rust_config("rust-a", 0);
    let m = Memberlist::create(conf).await.unwrap();
    let (conf, _delegate) = rust_config("rust-b", 0);
    let m2 = Memberlist::create(conf).await.unwrap();

    // Everybody joins through go-a. The other members learn about each
    // other from gossip.
    m.join(&[go_a.addr().to_string()]).await.unwrap();
    m2.join(&[go_a.addr().to_string()]).await.unwrap();
    go_b.cmd(&format!("join {}", go_a.addr())).await.unwrap();

    wait_for("rust-a to see 4 members", || m.num_members() == 4).await;
    wait_for("rust-b to see 4 members", || m2.num_members() == 4).await;
    go_a.wait_members(4).await;
    go_b.wait_members(4).await;

    // A Go node dies without leaving. The others find out by probing: a
    // direct ping, indirect pings through the other members, and a TCP ping.
    go_b.kill().await;
    wait_for("rust-a to drop go-b", || m.num_members() == 3).await;
    wait_for("rust-b to drop go-b", || m2.num_members() == 3).await;
    go_a.wait_members(3).await;

    // A Rust node dies without leaving.
    m2.shutdown().await.unwrap();
    wait_for("rust-a to drop rust-b", || m.num_members() == 2).await;
    go_a.wait_members(2).await;

    // The members that are left did not turn on each other.
    assert!(m.members().iter().any(|n| n.name == "go-a"));

    m.shutdown().await.unwrap();
    go_a.cmd("quit").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_label() {
    let opts = GoOptions {
        label: "blue",
        ..Default::default()
    };
    let Some(mut go) = GoNode::start("go-a", opts).await else {
        return;
    };

    // The same label works.
    let (mut conf, _delegate) = rust_config("rust-a", 0);
    conf.label = "blue".to_string();
    let m = Memberlist::create(conf).await.unwrap();
    m.join(&[go.addr().to_string()]).await.unwrap();
    assert_eq!(m.num_members(), 2);
    go.wait_members(2).await;
    let (_rtt, payload) = m.ping("go-a", go.addr()).await.unwrap();
    assert_eq!(payload, b"ack-go-a");

    // Another label, or no label, is rejected.
    for label in ["green", ""] {
        let (mut conf, _delegate) = rust_config("rust-b", 0);
        conf.label = label.to_string();
        let other = Memberlist::create(conf).await.unwrap();
        assert!(
            other.join(&[go.addr().to_string()]).await.is_err(),
            "join with label {label:?} must fail"
        );
        other.shutdown().await.unwrap();
    }
    assert_eq!(go.members().await.len(), 2);

    m.shutdown().await.unwrap();
    go.cmd("quit").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compression_and_protocol_versions() {
    // (Go compresses, Rust compresses, Go protocol, Rust protocol)
    let cases = [
        (false, false, 2, 2),
        (true, false, 2, 2),
        (false, true, 2, 2),
        (true, true, 5, 5),
        (true, true, 2, 5),
        (true, true, 5, 2),
        (true, true, 1, 3),
    ];
    for (go_compress, rust_compress, go_proto, rust_proto) in cases {
        let case = format!("{go_compress}/{rust_compress}/{go_proto}/{rust_proto}");
        let opts = GoOptions {
            no_compression: !go_compress,
            proto: go_proto,
            state_len: 2000,
            ..Default::default()
        };
        let Some(mut go) = GoNode::start("go-a", opts).await else {
            return;
        };
        let (mut conf, delegate) = rust_config("rust-a", 2000);
        conf.enable_compression = rust_compress;
        conf.protocol_version = rust_proto;
        let m = Memberlist::create(conf).await.unwrap();

        m.join(&[go.addr().to_string()])
            .await
            .unwrap_or_else(|err| panic!("case {case}: {err}"));
        assert_eq!(m.num_members(), 2, "case {case}");
        go.wait_members(2).await;
        assert_eq!(
            *delegate.remote_state.lock().unwrap(),
            state_bytes("state-go-a", 2000),
            "case {case}"
        );
        assert_eq!(
            unhex(&go.cmd("remote-state").await.unwrap()),
            state_bytes("state-rust-a", 2000),
            "case {case}"
        );

        let go_node = find(&m.members(), "go-a").clone();
        assert_eq!(go_node.pcur, go_proto, "case {case}");
        m.send_best_effort(&go_node, b"hello").await.unwrap();
        let start = Instant::now();
        while !go.msgs().await.contains(&b"hello".to_vec()) {
            assert!(start.elapsed() < Duration::from_secs(5), "case {case}");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // A few rounds of probing and gossip must not break anything.
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(m.num_members(), 2, "case {case}");
        assert_eq!(go.members().await.len(), 2, "case {case}");

        m.shutdown().await.unwrap();
        go.cmd("quit").await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_large_state() {
    // A large state that does not compress well. It makes the LZW code
    // table fill up and reset many times, in both directions.
    const LEN: usize = 400_000;
    let opts = GoOptions {
        state_len: LEN,
        ..Default::default()
    };
    let Some(mut go) = GoNode::start("go-a", opts).await else {
        return;
    };
    let (conf, delegate) = rust_config("rust-a", LEN);
    let m = Memberlist::create(conf).await.unwrap();

    // The Rust node starts the sync.
    m.join(&[go.addr().to_string()]).await.unwrap();
    assert!(
        *delegate.remote_state.lock().unwrap() == state_bytes("state-go-a", LEN),
        "the state from Go is wrong"
    );
    assert!(
        unhex(&go.cmd("remote-state").await.unwrap()) == state_bytes("state-rust-a", LEN),
        "the state from Rust is wrong"
    );

    // The Go node starts the sync.
    delegate.remote_state.lock().unwrap().clear();
    let rust_port = m.local_node().port;
    go.cmd(&format!("join 127.0.0.1:{rust_port}"))
        .await
        .unwrap();
    wait_for("the state of the Go node", || {
        *delegate.remote_state.lock().unwrap() == state_bytes("state-go-a", LEN)
    })
    .await;

    m.shutdown().await.unwrap();
    go.cmd("quit").await.unwrap();
}
