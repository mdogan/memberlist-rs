// Copyright (c) 2026 Mehmet Dogan
// SPDX-License-Identifier: MPL-2.0

//! Helpers that the tests share.
//!
//! The Go tests give every node its own loopback address (127.0.0.x), which
//! needs extra setup on macOS. Here every node binds to 127.0.0.1 with a
//! port that the kernel picks, and gets a unique name. To join a node, use
//! [`join_addr`], which has the name, the address and the port.

use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use metrics_util::MetricKind;
use metrics_util::debugging::{DebuggingRecorder, Snapshotter};
use parking_lot::Mutex;

use crate::config::Config;
use crate::delegate::Delegate;
use crate::memberlist::Memberlist;
use crate::messages::Alive;

/// The address that every test node binds to.
pub(crate) const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// The bytes of [`LOCALHOST`], as they are sent on the wire.
pub(crate) const LOCALHOST_BYTES: [u8; 4] = [127, 0, 0, 1];

/// Returns a node name that no other test uses.
pub(crate) fn test_name() -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    format!("node-{}", NEXT.fetch_add(1, Ordering::SeqCst))
}

/// The LAN defaults for a node with a unique name on 127.0.0.1 with a free
/// port. Node names are required, like in the Go tests.
pub(crate) fn test_config() -> Config {
    let mut config = Config::default_lan();
    config.bind_addr = LOCALHOST.to_string();
    config.name = test_name();
    config.bind_port = 0; // choose free port
    config.require_node_names = true;
    config
}

/// Creates the listeners of a memberlist with [`test_config`], without
/// marking it alive and without scheduling the background work.
pub(crate) async fn get_memberlist(f: impl FnOnce(&mut Config)) -> Memberlist {
    let mut c = test_config();
    f(&mut c);
    Memberlist::new_memberlist(c)
        .await
        .expect("failed to get memberlist")
}

/// Like [`get_memberlist`], but with the plain LAN defaults: node names are
/// not required. The node gets the given name.
pub(crate) async fn host_memberlist(name: &str, f: impl FnOnce(&mut Config)) -> Memberlist {
    let mut c = Config::default_lan();
    c.name = name.to_string();
    c.bind_addr = LOCALHOST.to_string();
    c.bind_port = 0; // choose a free port
    f(&mut c);
    Memberlist::new_memberlist(c)
        .await
        .expect("failed to get memberlist")
}

/// The string to pass to `join` to reach this memberlist: "name/ip:port".
pub(crate) fn join_addr(m: &Memberlist) -> String {
    format!("{}/{}", m.config.name, socket_addr(m))
}

/// The address that this memberlist listens on.
pub(crate) fn socket_addr(m: &Memberlist) -> SocketAddr {
    SocketAddr::new(LOCALHOST, m.config.bind_port)
}

/// An alive message about the memberlist itself, with incarnation 1.
pub(crate) fn alive_of(m: &Memberlist) -> Alive {
    Alive {
        node: m.config.name.clone(),
        addr: LOCALHOST_BYTES.to_vec(),
        port: m.config.bind_port,
        incarnation: 1,
        vsn: m.config.build_vsn_array(),
        ..Default::default()
    }
}

/// Returns a port on 127.0.0.1 that nothing listens on, for both TCP and
/// UDP. Another process could take it later, but that is unlikely within a
/// test.
pub(crate) fn unused_port() -> u16 {
    loop {
        let tcp = std::net::TcpListener::bind((LOCALHOST, 0)).expect("bind a TCP port");
        let port = tcp.local_addr().unwrap().port();
        if std::net::UdpSocket::bind((LOCALHOST, port)).is_ok() {
            return port;
        }
    }
}

/// Gives the background tasks some time to do their work.
pub(crate) async fn yield_now() {
    tokio::time::sleep(Duration::from_millis(250)).await;
}

/// Runs the check up to `n` times, with a wait of `w` between the attempts.
/// The check returns `Err` with a message when it fails. Panics with the
/// last message if every attempt fails.
pub(crate) async fn retry<F, Fut>(n: usize, w: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let mut last = String::new();
    for _ in 0..n {
        match f().await {
            Ok(()) => return,
            Err(msg) => last = msg,
        }
        tokio::time::sleep(w).await;
    }
    panic!("{last}");
}

/// Like the default retryer of the Go tests: repeats the check for 7
/// seconds, with 25 ms between the attempts.
pub(crate) async fn retry_default<F, Fut>(mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let start = Instant::now();
    loop {
        match f().await {
            Ok(()) => return,
            Err(msg) => {
                if start.elapsed() > Duration::from_secs(7) {
                    panic!("{msg}");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Waits until the check returns true. The check also returns a message,
/// which is the panic message if it is still false after 20 seconds.
pub(crate) async fn wait_for_condition(mut f: impl FnMut() -> (bool, String)) {
    let start = Instant::now();
    loop {
        let (success, msg) = f();
        if success {
            return;
        }
        if start.elapsed() > Duration::from_secs(20) {
            panic!("timeout waiting for condition: {msg}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Waits until the memberlist has the expected number of members.
pub(crate) async fn wait_until_size(m: &Memberlist, expected: usize) {
    retry(15, Duration::from_millis(500), || async {
        if m.num_members() != expected {
            return Err(format!(
                "{} expected to have {} members but had: {:?}",
                m.config.name,
                expected,
                m.members()
            ));
        }
        Ok(())
    })
    .await;
}

/// Checks that the UDP and the TCP port of a memberlist are free again.
pub(crate) async fn wait_until_port_is_free(m: &Memberlist) {
    let addr = socket_addr(m);
    retry(15, Duration::from_millis(250), || async {
        let tcp = std::net::TcpListener::bind(addr)
            .map_err(|err| format!("TCP port {addr} is not free: {err}"))?;
        drop(tcp);
        let udp = std::net::UdpSocket::bind(addr)
            .map_err(|err| format!("UDP port {addr} is not free: {err}"))?;
        drop(udp);
        Ok(())
    })
    .await;
}

#[derive(Default)]
struct MockDelegateState {
    meta: Vec<u8>,
    msgs: Vec<Vec<u8>>,
    broadcasts: Vec<Bytes>,
    state: Vec<u8>,
    remote_state: Vec<u8>,
}

/// A delegate that records what it is given and returns what the test set.
#[derive(Default)]
pub(crate) struct MockDelegate {
    inner: Mutex<MockDelegateState>,
}

impl MockDelegate {
    pub(crate) fn new() -> Arc<MockDelegate> {
        Arc::new(MockDelegate::default())
    }

    pub(crate) fn set_meta(&self, meta: &[u8]) {
        self.inner.lock().meta = meta.to_vec();
    }

    pub(crate) fn set_state(&self, state: &[u8]) {
        self.inner.lock().state = state.to_vec();
    }

    pub(crate) fn set_broadcasts(&self, broadcasts: Vec<Bytes>) {
        self.inner.lock().broadcasts = broadcasts;
    }

    pub(crate) fn get_remote_state(&self) -> Vec<u8> {
        self.inner.lock().remote_state.clone()
    }

    pub(crate) fn get_messages(&self) -> Vec<Vec<u8>> {
        self.inner.lock().msgs.clone()
    }
}

impl Delegate for MockDelegate {
    fn node_meta(&self, _limit: usize) -> Vec<u8> {
        self.inner.lock().meta.clone()
    }

    fn notify_msg(&self, msg: &[u8]) {
        self.inner.lock().msgs.push(msg.to_vec());
    }

    fn get_broadcasts(&self, _overhead: usize, _limit: usize) -> Vec<Bytes> {
        std::mem::take(&mut self.inner.lock().broadcasts)
    }

    fn local_state(&self, _join: bool) -> Vec<u8> {
        self.inner.lock().state.clone()
    }

    fn merge_remote_state(&self, s: &[u8], _join: bool) {
        self.inner.lock().remote_state = s.to_vec();
    }
}

/// Installs a recorder that keeps all metrics in memory. The `metrics`
/// crate has one recorder per process, so all tests share it.
pub(crate) fn register_in_memory_sink() -> &'static Snapshotter {
    static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();
    SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install the metrics recorder");
        snapshotter
    })
}

/// Returns true if a metric of the given kind was recorded with this name
/// and with all of these labels.
fn metric_exists(
    sink: &Snapshotter,
    kind: MetricKind,
    name: &str,
    labels: &[(&str, &str)],
) -> bool {
    sink.snapshot().into_vec().into_iter().any(|(key, ..)| {
        let (key_kind, key) = key.into_parts();
        key_kind == kind
            && key.name() == name
            && labels
                .iter()
                .all(|(k, v)| key.labels().any(|l| l.key() == *k && l.value() == *v))
    })
}

/// Panics if no gauge was recorded with this name and these labels.
pub(crate) fn verify_gauge_exists(name: &str, labels: &[(&str, &str)], sink: &Snapshotter) {
    assert!(
        metric_exists(sink, MetricKind::Gauge, name, labels),
        "expected gauge {name} {labels:?} to exist"
    );
}

/// Panics if no sample (histogram) was recorded with this name.
pub(crate) fn verify_sample_exists(name: &str, sink: &Snapshotter) {
    assert!(
        metric_exists(sink, MetricKind::Histogram, name, &[]),
        "expected sample {name} to exist"
    );
}

/// The log lines that were written while a [`capture_logs`] guard was alive.
#[derive(Clone, Default)]
pub(crate) struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl LogBuffer {
    pub(crate) fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }
}

impl io::Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = LogBuffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Collects the log output of the current thread until the guard is
/// dropped. This only sees everything in a test that uses the default
/// single threaded runtime of `#[tokio::test]`, where all tasks run on the
/// thread of the test.
pub(crate) fn capture_logs() -> (tracing::subscriber::DefaultGuard, LogBuffer) {
    // tracing remembers for each log call site if any subscriber wants it.
    // While only one subscriber exists, it asks the subscriber of the thread
    // that gets to the call site first. If that is the thread of another
    // test, which has no subscriber, the call site is turned off and the
    // test that captures the logs never sees its lines. With more than one
    // subscriber, tracing asks all of them. So keep two subscribers that do
    // nothing alive for the whole test run.
    static KEEP_ALIVE: OnceLock<[tracing::Dispatch; 2]> = OnceLock::new();
    KEEP_ALIVE.get_or_init(|| {
        [(); 2].map(|_| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()))
    });

    let buf = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .finish();
    (tracing::subscriber::set_default(subscriber), buf)
}

// The Go suite has a test for its retry helper (`internal/retry`). This is
// the same check for the helper above: three attempts, with a wait between
// them but not before the first one.
#[tokio::test]
async fn test_retry() {
    let iters = AtomicUsize::new(0);
    let start = Instant::now();
    let res = tokio::spawn(async move {
        retry(3, Duration::from_millis(100), || async {
            iters.fetch_add(1, Ordering::SeqCst);
            Err(format!("attempt {}", iters.load(Ordering::SeqCst)))
        })
        .await
    })
    .await;
    let dur = start.elapsed();

    // The helper panics with the last message when every attempt failed.
    let panic = res.expect_err("retry must panic").into_panic();
    assert_eq!(panic.downcast_ref::<String>().unwrap(), "attempt 3");
    assert!(
        dur >= Duration::from_millis(275) && dur <= Duration::from_millis(500),
        "loop took {dur:?}"
    );

    // A check that passes ends the loop at once.
    let iters = AtomicUsize::new(0);
    retry(3, Duration::from_millis(100), || async {
        iters.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .await;
    assert_eq!(iters.load(Ordering::SeqCst), 1);
}
