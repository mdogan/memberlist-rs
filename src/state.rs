// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of state.go from https://github.com/hashicorp/memberlist.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use metrics::Label;
use rand::RngExt;
use tokio::sync::{Notify, mpsc};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::error::{Error, Result};
use crate::label::label_overhead;
use crate::memberlist::Memberlist;
use crate::messages::{
    Alive, COMPOUND_HEADER_OVERHEAD, COMPOUND_OVERHEAD, Dead, IndirectPingReq, MessageType, Ping,
    PushNodeState, Suspect, encode,
};
use crate::suspicion::Suspicion;
use crate::timer::Timer;
use crate::transport::Address;
use crate::util::{
    addr_from_wire, addr_to_wire, ip_bytes_string, ip_string, ip_to_bytes, join_host_port,
    k_random_nodes, make_compound_message, make_compound_messages, move_dead_nodes,
    push_pull_scale, random_offset, shuffle_nodes, suspicion_timeout,
};

/// The state of a node, as this member sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum NodeStateType {
    #[default]
    Alive = 0,
    Suspect = 1,
    Dead = 2,
    Left = 3,
}

impl NodeStateType {
    pub(crate) fn metrics_string(&self) -> &'static str {
        match self {
            NodeStateType::Alive => "alive",
            NodeStateType::Dead => "dead",
            NodeStateType::Suspect => "suspect",
            NodeStateType::Left => "left",
        }
    }

    /// Converts the integer that is sent on the wire.
    pub(crate) fn from_wire(v: i64) -> Option<NodeStateType> {
        match v {
            0 => Some(NodeStateType::Alive),
            1 => Some(NodeStateType::Suspect),
            2 => Some(NodeStateType::Dead),
            3 => Some(NodeStateType::Left),
            _ => None,
        }
    }
}

/// Represents a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub name: String,
    /// The IP address of the node. An IPv4 address is always in its IPv4
    /// form here, also when the node sends it in the 16 byte form.
    pub addr: IpAddr,
    pub port: u16,
    /// Metadata from the delegate for this node.
    pub meta: Vec<u8>,
    /// State of the node.
    pub state: NodeStateType,
    /// Minimum protocol version this understands
    pub pmin: u8,
    /// Maximum protocol version this understands
    pub pmax: u8,
    /// Current version node is speaking
    pub pcur: u8,
    /// Min protocol version for the delegate to understand
    pub dmin: u8,
    /// Max protocol version for the delegate to understand
    pub dmax: u8,
    /// Current version delegate is speaking
    pub dcur: u8,
    /// True if the node sends its IPv4 address in the 16 byte form
    /// (IPv4-mapped IPv6), like Go members that bind to 0.0.0.0 do. The Go
    /// code compares addresses byte by byte, so we must pass the address on
    /// in the form that we got it in.
    pub(crate) addr_is_mapped: bool,
}

impl Node {
    /// Creates a node with the given name and address. The other fields get
    /// their zero values.
    pub fn new(name: impl Into<String>, addr: IpAddr, port: u16) -> Node {
        Node {
            name: name.into(),
            addr,
            port,
            meta: Vec::new(),
            state: NodeStateType::Alive,
            pmin: 0,
            pmax: 0,
            pcur: 0,
            dmin: 0,
            dmax: 0,
            dcur: 0,
            addr_is_mapped: false,
        }
    }

    /// Creates a node from the raw address bytes of a message. Returns
    /// `None` if the bytes are not an address.
    pub(crate) fn from_wire(name: &str, addr: &[u8], port: u16) -> Option<Node> {
        let (addr, mapped) = addr_from_wire(addr)?;
        let mut node = Node::new(name, addr, port);
        node.addr_is_mapped = mapped;
        Some(node)
    }

    /// The raw bytes of the address, as this node sends them.
    pub(crate) fn wire_addr(&self) -> Vec<u8> {
        addr_to_wire(&self.addr, self.addr_is_mapped)
    }

    /// Returns the host:port form of a node's address, suitable for use with
    /// a transport.
    pub fn address(&self) -> String {
        join_host_port(&ip_string(&self.addr), self.port)
    }

    /// Returns the node name and host:port form of a node's address,
    /// suitable for use with a transport.
    pub fn full_address(&self) -> Address {
        Address {
            addr: self.address(),
            name: self.name.clone(),
        }
    }

    /// Copies the protocol versions out of the version vector of an alive
    /// message. The vector must have at least 6 entries.
    fn set_versions(&mut self, vsn: &[u8]) {
        self.pmin = vsn[0];
        self.pmax = vsn[1];
        self.pcur = vsn[2];
        self.dmin = vsn[3];
        self.dmax = vsn[4];
        self.dcur = vsn[5];
    }

    fn versions(&self) -> [u8; 6] {
        [
            self.pmin, self.pmax, self.pcur, self.dmin, self.dmax, self.dcur,
        ]
    }
}

/// Prints the node name.
impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Used to manage our state view of another node.
#[derive(Debug, Clone)]
pub(crate) struct NodeState {
    pub(crate) node: Node,
    /// Last known incarnation number
    pub(crate) incarnation: u32,
    /// Current state
    pub(crate) state: NodeStateType,
    /// Time last state change happened. `None` means it never changed, which
    /// counts as infinitely long ago.
    pub(crate) state_change: Option<Instant>,
}

impl NodeState {
    /// Returns the host:port form of a node's address, suitable for use with
    /// a transport.
    #[allow(dead_code)]
    pub(crate) fn address(&self) -> String {
        self.node.address()
    }

    /// Returns the node name and host:port form of a node's address,
    /// suitable for use with a transport.
    pub(crate) fn full_address(&self) -> Address {
        self.node.full_address()
    }

    pub(crate) fn dead_or_left(&self) -> bool {
        self.state == NodeStateType::Dead || self.state == NodeStateType::Left
    }

    /// The time that has passed since the last state change.
    pub(crate) fn since_state_change(&self) -> Duration {
        match self.state_change {
            Some(t) => t.elapsed(),
            None => Duration::MAX,
        }
    }
}

/// The nodes that we know about.
///
/// `nodes` holds the states in probe order, and `node_map` maps a node name
/// to its index in `nodes`. Every change of the order must keep the two in
/// sync.
#[derive(Default)]
pub(crate) struct NodeTable {
    /// Known nodes
    pub(crate) nodes: Vec<NodeState>,
    /// Maps Node.Name -> index in `nodes`
    pub(crate) node_map: HashMap<String, usize>,
    /// Maps Node.Name -> suspicion timer
    pub(crate) node_timers: HashMap<String, Suspicion>,
}

impl NodeTable {
    pub(crate) fn get(&self, name: &str) -> Option<&NodeState> {
        self.node_map.get(name).map(|&idx| &self.nodes[idx])
    }

    #[allow(dead_code)]
    pub(crate) fn get_mut(&mut self, name: &str) -> Option<&mut NodeState> {
        self.node_map.get(name).map(|&idx| &mut self.nodes[idx])
    }

    /// Builds the name index again after the order of `nodes` changed.
    fn reindex(&mut self) {
        self.node_map.clear();
        for (idx, state) in self.nodes.iter().enumerate() {
            self.node_map.insert(state.node.name.clone(), idx);
        }
    }
}

/// Used to register handlers for incoming acks and nacks.
pub(crate) struct AckHandler {
    ack_fn: Box<dyn FnOnce(Vec<u8>, Instant) + Send>,
    nack_fn: Option<Arc<dyn Fn() + Send + Sync>>,
    timer: Option<Timer>,
}

/// The result of waiting for an ack.
#[derive(Debug, Clone)]
pub(crate) struct AckMessage {
    pub(crate) complete: bool,
    pub(crate) payload: Vec<u8>,
    pub(crate) timestamp: Instant,
}

/// Records how long an operation took when it is dropped, like a deferred
/// `MeasureSince` in Go. The sample is in milliseconds.
pub(crate) struct MeasureSince {
    name: &'static str,
    labels: Vec<Label>,
    start: Instant,
}

impl MeasureSince {
    pub(crate) fn new(name: &'static str, labels: &[Label]) -> Self {
        MeasureSince {
            name,
            labels: labels.to_vec(),
            start: Instant::now(),
        }
    }
}

impl Drop for MeasureSince {
    fn drop(&mut self) {
        let ms = self.start.elapsed().as_secs_f64() * 1000.0;
        metrics::histogram!(self.name, std::mem::take(&mut self.labels)).record(ms);
    }
}

/// Checks the error and decides if it indicates a failure on the other end.
pub(crate) fn failed_remote(err: &Error) -> bool {
    match err {
        Error::Op { op, net, .. } if net.starts_with("tcp") => {
            matches!(*op, "dial" | "read" | "write")
        }
        Error::Op { op, net, .. } if net.starts_with("udp") => *op == "write",
        _ => false,
    }
}

impl Memberlist {
    /// Ensures the background work is performed periodically. This function
    /// is safe to call multiple times. If the memberlist is already
    /// scheduled, then it won't do anything.
    pub(crate) fn schedule(&self) {
        let mut stop_tick = self.stop_tick.lock();

        // If we already have tickers, then don't do anything, since we're
        // scheduled
        if stop_tick.is_some() {
            return;
        }

        // Create the stop token. We cancel this when we should stop the
        // tickers.
        let stop = CancellationToken::new();
        let mut scheduled = false;

        // Create a new probe ticker
        if !self.config.probe_interval.is_zero() {
            let m = self.clone();
            tokio::spawn(trigger_func(
                self.config.probe_interval,
                stop.clone(),
                move || {
                    let m = m.clone();
                    async move { m.probe().await }
                },
            ));
            scheduled = true;
        }

        // Create a push pull ticker if needed
        if !self.config.push_pull_interval.is_zero() {
            tokio::spawn(self.clone().push_pull_trigger(stop.clone()));
            scheduled = true;
        }

        // Create a gossip ticker if needed
        if !self.config.gossip_interval.is_zero() && self.config.gossip_nodes > 0 {
            let m = self.clone();
            tokio::spawn(trigger_func(
                self.config.gossip_interval,
                stop.clone(),
                move || {
                    let m = m.clone();
                    async move { m.gossip().await }
                },
            ));
            scheduled = true;
        }

        // If we started anything, then record the stop token for later.
        if scheduled {
            *stop_tick = Some(stop);
        }
    }

    /// Periodically triggers a push/pull until the stop token is cancelled.
    /// We don't use `trigger_func` since the push/pull timer is dynamically
    /// scaled based on cluster size to avoid network saturation.
    async fn push_pull_trigger(self, stop: CancellationToken) {
        let interval = self.config.push_pull_interval;

        // Use a random stagger to avoid syncronizing
        let rand_stagger = random_stagger(interval);
        tokio::select! {
            _ = tokio::time::sleep(rand_stagger) => {}
            _ = stop.cancelled() => return,
        }

        // Tick using a dynamic timer
        loop {
            let tick_time = push_pull_scale(interval, self.est_num_nodes());
            tokio::select! {
                _ = tokio::time::sleep(tick_time) => self.push_pull().await,
                _ = stop.cancelled() => return,
            }
        }
    }

    /// Stops the background maintenance. This is safe to call multiple
    /// times.
    pub(crate) fn deschedule(&self) {
        // If we have no stop token, then we aren't scheduled. Otherwise
        // cancel it so all the ticker listeners stop.
        if let Some(stop) = self.stop_tick.lock().take() {
            stop.cancel();
        }
    }

    /// Performs a single round of failure detection.
    pub(crate) async fn probe(&self) {
        // Track the number of indexes we've considered probing
        let mut num_check = 0;
        loop {
            let node = {
                let nodes = self.nodes.read();

                // Make sure we don't wrap around infinitely
                if num_check >= nodes.nodes.len() {
                    return;
                }

                // Handle the wrap around case
                let probe_index = self.probe_index.load(Ordering::SeqCst);
                if probe_index >= nodes.nodes.len() {
                    drop(nodes);
                    self.reset_nodes();
                    self.probe_index.store(0, Ordering::SeqCst);
                    num_check += 1;
                    continue;
                }

                // Determine if we should probe this node
                let node = &nodes.nodes[probe_index];
                let skip = node.node.name == self.config.name || node.dead_or_left();

                // Potentially skip
                self.probe_index.store(probe_index + 1, Ordering::SeqCst);
                if skip {
                    num_check += 1;
                    continue;
                }
                node.clone()
            };

            // Probe the specific node
            self.probe_node(&node).await;
            return;
        }
    }

    /// Safely calls `probe_node` given only the name of the node (for
    /// tests).
    #[cfg(test)]
    pub(crate) async fn probe_node_by_addr(&self, addr: &str) {
        let node = self.nodes.read().get(addr).expect("unknown node").clone();
        self.probe_node(&node).await;
    }

    /// Handles a single round of failure checking on a node.
    pub(crate) async fn probe_node(&self, node: &NodeState) {
        let _measure = MeasureSince::new("memberlist.probeNode", &self.config.metric_labels);

        // Arrange for our self-awareness to get updated.
        let awareness_delta = self.probe_node_inner(node).await;
        self.awareness.apply_delta(awareness_delta);
    }

    /// Does the work of `probe_node` and returns the change to apply to our
    /// health score.
    async fn probe_node_inner(&self, node: &NodeState) -> i64 {
        // We use our health awareness to scale the overall probe interval,
        // so we slow down if we detect problems. The ticker that calls us
        // can handle us running over the base interval, and will skip
        // missed ticks.
        let probe_interval = self.awareness.scale_timeout(self.config.probe_interval);
        if probe_interval > self.config.probe_interval {
            metrics::counter!(
                "memberlist.degraded.probe",
                self.config.metric_labels.clone()
            )
            .increment(1);
        }

        // Prepare a ping message and setup an ack handler.
        let (self_addr, self_port) = self.get_advertise();
        let ping = Ping {
            seq_no: self.next_seq_no(),
            node: node.node.name.clone(),
            source_addr: ip_to_bytes(&self_addr),
            source_port: self_port,
            source_node: self.config.name.clone(),
        };
        let (ack_tx, mut ack_rx) = mpsc::channel(self.config.indirect_checks + 1);
        let (nack_tx, mut nack_rx) = mpsc::channel(self.config.indirect_checks + 1);
        self.set_probe_channels(ping.seq_no, ack_tx.clone(), Some(nack_tx), probe_interval);

        // Mark the sent time here, which should be after any pre-processing
        // but before system calls to do the actual send. This probably
        // over-reports a bit, but it's the best we can do. We had originally
        // put this right after the I/O, but that would sometimes give
        // negative RTT measurements which was not desirable.
        let sent = Instant::now();

        // Send a ping to the node. If this node looks like it's suspect or
        // dead, also tack on a suspect message so that it has a chance to
        // refute as soon as possible.
        let deadline = sent + probe_interval;
        let addr = node.address();

        // The change to our health score. It stays zero until the ping is on
        // its way.
        let mut awareness_delta = 0;

        let send_result = if node.state == NodeStateType::Alive {
            self.encode_and_send_msg(&node.full_address(), MessageType::PING, &ping)
                .await
                .inspect_err(|err| error!("memberlist: Failed to send UDP ping: {err}"))
        } else {
            let s = Suspect {
                incarnation: node.incarnation,
                node: node.node.name.clone(),
                from: self.config.name.clone(),
            };
            let msgs = [
                encode(MessageType::PING, &ping),
                encode(MessageType::SUSPECT, &s),
            ];

            let compound = make_compound_message(&msgs);
            self.raw_send_msg_packet(&node.full_address(), Some(&node.node), &compound)
                .await
                .inspect_err(|err| {
                    error!(
                        "memberlist: Failed to send UDP compound ping and suspect message to {addr}: {err}"
                    )
                })
        };

        match send_result {
            // A failed send that points at the other end goes straight to
            // the indirect probes below.
            Err(err) if failed_remote(&err) => {}
            Err(_) => return awareness_delta,
            Ok(()) => {
                // Arrange for our self-awareness to get updated. At this
                // point we've sent the ping, so any return statement means
                // the probe succeeded which will improve our health until we
                // get to the failure scenarios at the end of this function,
                // which will alter this delta variable accordingly.
                awareness_delta = -1;

                // Wait for response or round-trip-time.
                tokio::select! {
                    v = ack_rx.recv() => {
                        if let Some(v) = v {
                            if v.complete {
                                if let Some(ping_delegate) = &self.config.ping {
                                    let rtt = v.timestamp.saturating_duration_since(sent);
                                    ping_delegate.notify_ping_complete(&node.node, rtt, &v.payload);
                                }
                                return awareness_delta;
                            }

                            // As an edge case, if we get a timeout, we need
                            // to re-enqueue it here to break out of the wait
                            // below.
                            let _ = ack_tx.try_send(v);
                        }
                    }
                    _ = tokio::time::sleep(self.config.probe_timeout) => {
                        // Note that we don't scale this timeout based on
                        // awareness and the health score. That's because we
                        // don't really expect waiting longer to help get UDP
                        // through. Since health does extend the probe
                        // interval it will give the TCP fallback more time,
                        // which is more active in dealing with lost packets,
                        // and it gives more time to wait for indirect
                        // acks/nacks.
                        debug!(
                            "memberlist: Failed UDP ping: {} (timeout reached)",
                            node.node.name
                        );
                    }
                }
            }
        }

        // Get some random live nodes.
        let k_nodes = {
            let nodes = self.nodes.read();
            k_random_nodes(self.config.indirect_checks, &nodes.nodes, |n| {
                n.node.name == self.config.name
                    || n.node.name == node.node.name
                    || n.state != NodeStateType::Alive
            })
        };

        // Attempt an indirect ping.
        let mut expected_nacks = 0;
        let (self_addr, self_port) = self.get_advertise();
        let mut ind = IndirectPingReq {
            seq_no: ping.seq_no,
            target: node.node.wire_addr(),
            port: node.node.port,
            node: node.node.name.clone(),
            nack: false,
            source_addr: ip_to_bytes(&self_addr),
            source_port: self_port,
            source_node: self.config.name.clone(),
        };
        for peer in &k_nodes {
            // We only expect nack to be sent from peers who understand
            // version 4 of the protocol.
            ind.nack = peer.pmax >= 4;
            if ind.nack {
                expected_nacks += 1;
            }

            if let Err(err) = self
                .encode_and_send_msg(&peer.full_address(), MessageType::INDIRECT_PING, &ind)
                .await
            {
                error!("memberlist: Failed to send indirect UDP ping: {err}");
            }
        }

        // Also make an attempt to contact the node directly over TCP. This
        // helps prevent confused clients who get isolated from UDP traffic
        // but can still speak TCP (which also means they can possibly report
        // misinformation to other nodes via anti-entropy), avoiding flapping
        // in the cluster.
        //
        // This is a little unusual because we will attempt a TCP ping to any
        // member who understands version 3 of the protocol, regardless of
        // which protocol version we are speaking. That's why we've included
        // a config option to turn this off if desired.
        let disable_tcp_pings = self.config.disable_tcp_pings
            || self
                .config
                .disable_tcp_pings_for_node
                .as_ref()
                .is_some_and(|f| f(&node.node.name));
        let fallback = if !disable_tcp_pings && node.node.pmax >= 3 {
            let m = self.clone();
            let a = node.full_address();
            let ping = ping.clone();
            Some(tokio::spawn(async move {
                match m.send_ping_and_wait_for_ack(&a, &ping, deadline).await {
                    Ok(did_contact) => Some(did_contact),
                    Err(err) => {
                        let to = if err.is_timeout() {
                            format!("timeout {probe_interval:?}: ")
                        } else {
                            String::new()
                        };
                        error!("memberlist: Failed fallback TCP ping: {to}{err}");
                        None
                    }
                }
            }))
        } else {
            None
        };

        // Wait for the acks or timeout. Note that we don't check the
        // fallback result here because we want to issue a warning below if
        // that's the *only* way we hear back from the peer, so we have to
        // let this time out first to allow the normal UDP-based acks to come
        // in.
        if let Some(v) = ack_rx.recv().await
            && v.complete
        {
            return awareness_delta;
        }

        // Finally, poll the fallback result. The timeouts are set such that
        // the task will have something or be done without having to wait any
        // additional time here.
        if let Some(fallback) = fallback
            && let Ok(Some(true)) = fallback.await
        {
            warn!(
                "memberlist: Was able to connect to {} over TCP but UDP probes failed, network may be misconfigured",
                node.node.name
            );
            return awareness_delta;
        }

        // Update our self-awareness based on the results of this failed
        // probe. If we don't have peers who will send nacks then we penalize
        // for any failed probe as a simple health metric. If we do have
        // peers to nack verify, then we can use that as a more sophisticated
        // measure of self-health because we assume them to be working, and
        // they can help us decide if the probed node was really dead or if
        // it was something wrong with ourselves.
        awareness_delta = 0;
        if expected_nacks > 0 {
            let mut nack_count = 0;
            while nack_rx.try_recv().is_ok() {
                nack_count += 1;
            }
            if nack_count < expected_nacks {
                awareness_delta += expected_nacks - nack_count;
            }
        } else {
            awareness_delta += 1;
        }

        // No acks received from target, suspect it as failed.
        info!(
            "memberlist: Suspect {} has failed, no acks received",
            node.node.name
        );
        let s = Suspect {
            incarnation: node.incarnation,
            node: node.node.name.clone(),
            from: self.config.name.clone(),
        };
        self.suspect_node(&s);
        awareness_delta
    }

    /// Initiates a ping to the node with the specified name. On success it
    /// returns the round trip time and the payload of the ack.
    pub async fn ping(&self, node: &str, addr: SocketAddr) -> Result<(Duration, Vec<u8>)> {
        // Prepare a ping message and setup an ack handler.
        let (self_addr, self_port) = self.get_advertise();
        let ping = Ping {
            seq_no: self.next_seq_no(),
            node: node.to_string(),
            source_addr: ip_to_bytes(&self_addr),
            source_port: self_port,
            source_node: self.config.name.clone(),
        };
        let (ack_tx, mut ack_rx) = mpsc::channel(self.config.indirect_checks + 1);
        self.set_probe_channels(ping.seq_no, ack_tx, None, self.config.probe_interval);

        let a = Address {
            addr: addr.to_string(),
            name: node.to_string(),
        };

        // Send a ping to the node.
        self.encode_and_send_msg(&a, MessageType::PING, &ping)
            .await?;

        // Mark the sent time here, which should be after any pre-processing
        // and system calls to do the actual send. This probably
        // under-reports a bit, but it's the best we can do.
        let sent = Instant::now();

        // Wait for response or timeout.
        tokio::select! {
            v = ack_rx.recv() => {
                if let Some(v) = v
                    && v.complete {
                        return Ok((v.timestamp.saturating_duration_since(sent), v.payload));
                    }
            }
            _ = tokio::time::sleep(self.config.probe_timeout) => {
                // Timeout, return an error below.
            }
        }

        debug!("memberlist: Failed UDP ping: {node} (timeout reached)");
        Err(Error::NoPingResponse(ping.node))
    }

    /// Used when the tick wraps around. It will reap the dead nodes and
    /// shuffle the node list.
    pub(crate) fn reset_nodes(&self) {
        let mut table = self.nodes.write();

        // Move dead nodes, but respect gossip to the dead interval
        let dead_idx = move_dead_nodes(&mut table.nodes, self.config.gossip_to_the_dead_time);

        // Deregister the dead nodes and trim the nodes to exclude them
        table.nodes.truncate(dead_idx);

        // Update num_nodes after we've trimmed the dead nodes
        self.num_nodes.store(dead_idx as u32, Ordering::SeqCst);

        // Shuffle live nodes
        shuffle_nodes(&mut table.nodes);
        table.reindex();
    }

    /// Invoked every `gossip_interval` period to broadcast our gossip
    /// messages to a few random nodes.
    pub(crate) async fn gossip(&self) {
        let _measure = MeasureSince::new("memberlist.gossip", &self.config.metric_labels);

        // Get some random live, suspect, or recently dead nodes
        let k_nodes = {
            let nodes = self.nodes.read();
            k_random_nodes(self.config.gossip_nodes, &nodes.nodes, |n| {
                if n.node.name == self.config.name {
                    return true;
                }

                match n.state {
                    NodeStateType::Alive | NodeStateType::Suspect => false,
                    NodeStateType::Dead => {
                        n.since_state_change() > self.config.gossip_to_the_dead_time
                    }
                    NodeStateType::Left => true,
                }
            })
        };

        // Compute the bytes available
        let bytes_avail = self
            .config
            .udp_buffer_size
            .saturating_sub(COMPOUND_HEADER_OVERHEAD + label_overhead(&self.config.label));

        for node in &k_nodes {
            // Get any pending broadcasts
            let msgs = self.get_broadcasts(COMPOUND_OVERHEAD, bytes_avail);
            if msgs.is_empty() {
                return;
            }

            let addr = node.address();
            if msgs.len() == 1 {
                // Send single message as is
                if let Err(err) = self
                    .raw_send_msg_packet(&node.full_address(), Some(node), &msgs[0])
                    .await
                {
                    error!("memberlist: Failed to send gossip to {addr}: {err}");
                }
            } else {
                // Otherwise create and send one or more compound messages
                for compound in make_compound_messages(&msgs) {
                    if let Err(err) = self
                        .raw_send_msg_packet(&node.full_address(), Some(node), &compound)
                        .await
                    {
                        error!("memberlist: Failed to send gossip to {addr}: {err}");
                    }
                }
            }
        }
    }

    /// Invoked periodically to randomly perform a complete state exchange.
    /// Used to ensure a high level of convergence, but is also reasonably
    /// expensive as the entire state of this node is exchanged with the
    /// other node.
    pub(crate) async fn push_pull(&self) {
        // Get a random live node
        let nodes = {
            let nodes = self.nodes.read();
            k_random_nodes(1, &nodes.nodes, |n| {
                n.node.name == self.config.name || n.state != NodeStateType::Alive
            })
        };

        // If no nodes, bail
        let Some(node) = nodes.first() else {
            return;
        };

        // Attempt a push pull
        if let Err(err) = self.push_pull_node(&node.full_address(), false).await {
            error!("memberlist: Push/Pull with {} failed: {err}", node.name);
        }
    }

    /// Does a complete state exchange with a specific node.
    pub(crate) async fn push_pull_node(&self, a: &Address, join: bool) -> Result<()> {
        let _measure = MeasureSince::new("memberlist.pushPullNode", &self.config.metric_labels);

        // Attempt to send and receive with the node
        let (remote, user_state) = self.send_and_receive_state(a, join).await?;

        self.merge_remote_state(join, &remote, &user_state)
    }

    /// Verifies that all the remote nodes can speak with our nodes and vice
    /// versa on both the core protocol as well as the delegate protocol
    /// level.
    ///
    /// The verification works by finding the maximum minimum and minimum
    /// maximum understood protocol and delegate versions. In other words, it
    /// finds the common denominator of protocol and delegate version ranges
    /// for the entire cluster.
    ///
    /// After this, it goes through the entire cluster (local and remote) and
    /// verifies that everyone's speaking protocol versions satisfy this
    /// range. If this passes, it means that every node can understand each
    /// other.
    pub(crate) fn verify_protocol(&self, remote: &[PushNodeState]) -> Result<()> {
        let nodes = self.nodes.read();

        // Maximum minimum understood and minimum maximum understood for both
        // the protocol and delegate versions. We use this to verify everyone
        // can be understood.
        let (mut maxpmin, mut minpmax) = (0u8, u8::MAX);
        let (mut maxdmin, mut mindmax) = (0u8, u8::MAX);

        for rn in remote {
            // If the node isn't alive, then skip it
            if rn.node_state() != Some(NodeStateType::Alive) {
                continue;
            }

            // Skip nodes that don't have proper version info
            if rn.vsn.len() < 5 {
                continue;
            }

            maxpmin = maxpmin.max(rn.vsn[0]);
            minpmax = minpmax.min(rn.vsn[1]);
            maxdmin = maxdmin.max(rn.vsn[3]);
            mindmax = mindmax.min(rn.vsn[4]);
        }

        for n in &nodes.nodes {
            // Ignore non-alive nodes
            if n.state != NodeStateType::Alive {
                continue;
            }

            maxpmin = maxpmin.max(n.node.pmin);
            minpmax = minpmax.min(n.node.pmax);
            maxdmin = maxdmin.max(n.node.dmin);
            mindmax = mindmax.min(n.node.dmax);
        }

        // Now that we definitively know the minimum and maximum understood
        // version that satisfies the whole cluster, we verify that every
        // node in the cluster satisifies this.
        let check = |name: &str, pcur: u8, dcur: u8| -> Result<()> {
            if pcur < maxpmin || pcur > minpmax {
                return Err(Error::msg(format!(
                    "Node '{name}' protocol version ({pcur}) is incompatible: [{maxpmin}, {minpmax}]"
                )));
            }

            if dcur < maxdmin || dcur > mindmax {
                return Err(Error::msg(format!(
                    "Node '{name}' delegate protocol version ({dcur}) is incompatible: [{maxdmin}, {mindmax}]"
                )));
            }
            Ok(())
        };

        for n in remote {
            let (pcur, dcur) = if n.vsn.len() >= 6 {
                (n.vsn[2], n.vsn[5])
            } else {
                (0, 0)
            };
            check(&n.name, pcur, dcur)?;
        }

        for n in &nodes.nodes {
            check(&n.node.name, n.node.pcur, n.node.dcur)?;
        }

        Ok(())
    }

    /// Returns a usable sequence number in a thread safe way.
    pub(crate) fn next_seq_no(&self) -> u32 {
        self.sequence_num
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1)
    }

    /// Returns the next incarnation number in a thread safe way.
    pub(crate) fn next_incarnation(&self) -> u32 {
        self.incarnation
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1)
    }

    /// Adds the positive offset to the incarnation number.
    pub(crate) fn skip_incarnation(&self, offset: u32) -> u32 {
        self.incarnation
            .fetch_add(offset, Ordering::SeqCst)
            .wrapping_add(offset)
    }

    /// Gets the current estimate of the number of nodes.
    pub(crate) fn est_num_nodes(&self) -> usize {
        self.num_nodes.load(Ordering::SeqCst) as usize
    }

    /// Attaches the ack channel to receive a message when an ack with a
    /// given sequence number is received. The `complete` field of the
    /// message will be false on timeout. Any nack messages will cause an
    /// empty value to be passed to the nack channel, which can be `None` if
    /// not needed.
    pub(crate) fn set_probe_channels(
        &self,
        seq_no: u32,
        ack_ch: mpsc::Sender<AckMessage>,
        nack_ch: Option<mpsc::Sender<()>>,
        timeout: Duration,
    ) {
        // Create handler functions for acks and nacks
        let ack_fn = {
            let ack_ch = ack_ch.clone();
            move |payload: Vec<u8>, timestamp: Instant| {
                let _ = ack_ch.try_send(AckMessage {
                    complete: true,
                    payload,
                    timestamp,
                });
            }
        };
        let nack_fn = move || {
            if let Some(nack_ch) = &nack_ch {
                let _ = nack_ch.try_send(());
            }
        };

        // Add the handler, with a reaping routine
        let weak = self.downgrade();
        let timer = Timer::after_func(timeout, move || {
            if let Some(m) = Memberlist::upgrade(&weak) {
                let handler = m.ack_handlers.lock().remove(&seq_no);
                drop(handler);
            }
            let _ = ack_ch.try_send(AckMessage {
                complete: false,
                payload: Vec::new(),
                timestamp: Instant::now(),
            });
        });

        self.ack_handlers.lock().insert(
            seq_no,
            AckHandler {
                ack_fn: Box::new(ack_fn),
                nack_fn: Some(Arc::new(nack_fn)),
                timer: Some(timer),
            },
        );
    }

    /// Attaches a handler to be invoked when an ack with a given sequence
    /// number is received. If a timeout is reached, the handler is deleted.
    /// This is used for indirect pings so does not configure a function for
    /// nacks.
    pub(crate) fn set_ack_handler(
        &self,
        seq_no: u32,
        ack_fn: impl FnOnce(Vec<u8>, Instant) + Send + 'static,
        timeout: Duration,
    ) {
        // Setup a reaping routing
        let weak = self.downgrade();
        let timer = Timer::after_func(timeout, move || {
            if let Some(m) = Memberlist::upgrade(&weak) {
                let handler = m.ack_handlers.lock().remove(&seq_no);
                drop(handler);
            }
        });

        // Add the handler
        self.ack_handlers.lock().insert(
            seq_no,
            AckHandler {
                ack_fn: Box::new(ack_fn),
                nack_fn: None,
                timer: Some(timer),
            },
        );
    }

    /// Invokes an ack handler if any is associated, and reaps the handler
    /// immediately.
    pub(crate) fn invoke_ack_handler(&self, seq_no: u32, payload: Vec<u8>, timestamp: Instant) {
        let handler = self.ack_handlers.lock().remove(&seq_no);
        let Some(mut handler) = handler else {
            return;
        };
        if let Some(timer) = handler.timer.take() {
            timer.stop();
        }
        (handler.ack_fn)(payload, timestamp);
    }

    /// Invokes nack handler if any is associated.
    pub(crate) fn invoke_nack_handler(&self, seq_no: u32) {
        let nack_fn = {
            let handlers = self.ack_handlers.lock();
            handlers.get(&seq_no).and_then(|ah| ah.nack_fn.clone())
        };
        if let Some(nack_fn) = nack_fn {
            nack_fn();
        }
    }

    /// Gossips an alive message in response to incoming information that we
    /// are suspect or dead. It will make sure the incarnation number beats
    /// the given accused_inc value, or you can supply 0 to just get the next
    /// incarnation number. This alters the node state that's passed in so
    /// this MUST be called while the node lock is held.
    fn refute(&self, me: &mut NodeState, accused_inc: u32) {
        // Make sure the incarnation number beats the accusation.
        let mut inc = self.next_incarnation();
        if accused_inc >= inc {
            inc = self.skip_incarnation(accused_inc - inc + 1);
        }
        me.incarnation = inc;

        // Decrease our health because we are being asked to refute a problem.
        self.awareness.apply_delta(1);

        // Format and broadcast an alive message.
        let a = Alive {
            incarnation: inc,
            node: me.node.name.clone(),
            addr: me.node.wire_addr(),
            port: me.node.port,
            meta: me.node.meta.clone(),
            vsn: me.node.versions().to_vec(),
        };
        self.encode_and_broadcast(&ip_string(&me.node.addr), MessageType::ALIVE, &a);
    }

    /// Invoked by the network layer when we get a message about a live node.
    pub(crate) fn alive_node(&self, a: &Alive, notify: Option<Arc<Notify>>, bootstrap: bool) {
        let mut table = self.nodes.write();

        // It is possible that during a leave, there is already an alive
        // message in-queue to be processed but blocked by the locks above.
        // If we let that message process, it'll cause us to re-join the
        // cluster. This ensures that we don't.
        if self.has_left() && a.node == self.config.name {
            return;
        }

        if a.vsn.len() >= 3 {
            let (pmin, pmax, pcur) = (a.vsn[0], a.vsn[1], a.vsn[2]);
            if pmin == 0 || pmax == 0 || pmin > pmax {
                warn!(
                    "memberlist: Ignoring an alive message for '{}' ({}:{}) because protocol version(s) are wrong: {} <= {} <= {} should be >0",
                    a.node,
                    ip_bytes_string(&a.addr),
                    a.port,
                    pmin,
                    pcur,
                    pmax
                );
                return;
            }
        }

        // A node needs an address that we can talk to.
        let Some(alive_node) = Node::from_wire(&a.node, &a.addr, a.port) else {
            warn!(
                "memberlist: Ignoring an alive message for '{}' ({}:{}) because the address is not valid",
                a.node,
                ip_bytes_string(&a.addr),
                a.port
            );
            return;
        };
        let addr = alive_node.addr;

        // Invoke the Alive delegate if any. This can be used to filter out
        // alive messages based on custom logic. For example, using a cluster
        // name. Using a merge delegate is not enough, as it is possible for
        // passive cluster merging to still occur.
        if let Some(alive) = &self.config.alive {
            if a.vsn.len() < 6 {
                warn!(
                    "memberlist: ignoring alive message for '{}' ({}:{}) because Vsn is not present",
                    a.node,
                    ip_bytes_string(&a.addr),
                    a.port
                );
                return;
            }
            let mut node = alive_node.clone();
            node.meta = a.meta.clone();
            node.set_versions(&a.vsn);
            if let Err(err) = alive.notify_alive(&node) {
                warn!("memberlist: ignoring alive message for '{}': {err}", a.node);
                return;
            }
        }

        // Check if we've never seen this node before, and if not, then
        // store this node in our node map.
        let mut updates_node = false;
        let idx = match table.node_map.get(&a.node).copied() {
            None => {
                if let Err(err) = self.config.ip_allowed(addr) {
                    warn!(
                        "memberlist: Rejected node {} ({}): {err}",
                        a.node,
                        ip_string(&addr)
                    );
                    return;
                }
                let mut state = NodeState {
                    node: alive_node.clone(),
                    incarnation: 0,
                    state: NodeStateType::Dead,
                    state_change: None,
                };
                state.node.meta = a.meta.clone();
                if a.vsn.len() > 5 {
                    state.node.set_versions(&a.vsn);
                }

                // Get a random offset. This is important to ensure the
                // failure detection bound is low on average. If all nodes
                // did an append, failure detection bound would be very high.
                let n = table.nodes.len();
                let offset = random_offset(n);

                // Add at the end and swap with the node at the offset
                table.nodes.push(state);
                table.nodes.swap(offset, n);

                // Add to map, and fix the index of the node that moved
                let moved = table.nodes[n].node.name.clone();
                table.node_map.insert(moved, n);
                table.node_map.insert(a.node.clone(), offset);

                // Update num_nodes after we've added a new node
                self.num_nodes.fetch_add(1, Ordering::SeqCst);
                offset
            }
            Some(idx) => {
                // Check if this address is different than the existing node
                // unless the old node is dead.
                let state = &table.nodes[idx];
                if state.node.wire_addr() != a.addr || state.node.port != a.port {
                    if let Err(err) = self.config.ip_allowed(addr) {
                        warn!(
                            "memberlist: Rejected IP update from {} to {} for node {}: {err}",
                            a.node,
                            ip_string(&state.node.addr),
                            ip_string(&addr)
                        );
                        return;
                    }
                    // If dead_node_reclaim_time is configured, check if
                    // enough time has elapsed since the node died.
                    let can_reclaim = !self.config.dead_node_reclaim_time.is_zero()
                        && state.since_state_change() > self.config.dead_node_reclaim_time;

                    // Allow the address to be updated if a dead node is
                    // being replaced.
                    if state.state == NodeStateType::Left
                        || (state.state == NodeStateType::Dead && can_reclaim)
                    {
                        info!(
                            "memberlist: Updating address for left or failed node {} from {}:{} to {}:{}",
                            state.node.name,
                            ip_string(&state.node.addr),
                            state.node.port,
                            ip_string(&addr),
                            a.port
                        );
                        updates_node = true;
                    } else {
                        error!(
                            "memberlist: Conflicting address for {}. Mine: {}:{} Theirs: {}:{} Old state: {:?}",
                            state.node.name,
                            ip_string(&state.node.addr),
                            state.node.port,
                            ip_string(&addr),
                            a.port,
                            state.state
                        );

                        // Inform the conflict delegate if provided
                        if let Some(conflict) = &self.config.conflict {
                            let mut other = alive_node.clone();
                            other.meta = a.meta.clone();
                            conflict.notify_conflict(&state.node, &other);
                        }
                        return;
                    }
                }
                idx
            }
        };

        let NodeTable {
            nodes, node_timers, ..
        } = &mut *table;
        let state = &mut nodes[idx];

        // Bail if the incarnation number is older, and this is not about us
        let is_local_node = state.node.name == self.config.name;
        if a.incarnation <= state.incarnation && !is_local_node && !updates_node {
            return;
        }

        // Bail if strictly less and this is about us
        if a.incarnation < state.incarnation && is_local_node {
            return;
        }

        // Clear out any suspicion timer that may be in effect.
        node_timers.remove(&a.node);

        // Store the old state and meta data
        let old_state = state.state;
        let old_meta = state.node.meta.clone();

        // If this is us we need to refute, otherwise re-broadcast
        if !bootstrap && is_local_node {
            // Compute the version vector
            let versions = state.node.versions();

            // If the Incarnation is the same, we need special handling,
            // since it possible for the following situation to happen:
            // 1) Start with configuration C, join cluster
            // 2) Hard fail / Kill / Shutdown
            // 3) Restart with configuration C', join cluster
            //
            // In this case, other nodes and the local node see the same
            // incarnation, but the values may not be the same. For this
            // reason, we always need to do an equality check for this
            // Incarnation. In most cases, we just ignore, but we may need to
            // refute.
            if a.incarnation == state.incarnation && a.meta == state.node.meta && a.vsn == versions
            {
                return;
            }
            self.refute(state, a.incarnation);
            warn!(
                "memberlist: Refuting an alive message for '{}' ({}:{}) meta:({:?} VS {:?}), vsn:({:?} VS {:?})",
                a.node,
                ip_string(&addr),
                a.port,
                a.meta,
                state.node.meta,
                a.vsn,
                versions
            );
        } else {
            self.encode_broadcast_notify(&a.node, MessageType::ALIVE, a, notify);

            // Update protocol versions if it arrived
            if a.vsn.len() >= 6 {
                state.node.set_versions(&a.vsn);
            }

            // Update the state and incarnation number
            state.incarnation = a.incarnation;
            state.node.meta = a.meta.clone();
            state.node.addr = addr;
            state.node.addr_is_mapped = alive_node.addr_is_mapped;
            state.node.port = a.port;
            if state.state != NodeStateType::Alive {
                state.state = NodeStateType::Alive;
                state.state_change = Some(Instant::now());
            }
        }

        // Update metrics
        metrics::counter!("memberlist.msg.alive", self.config.metric_labels.clone()).increment(1);

        // Notify the delegate of any relevant updates
        if let Some(events) = &self.config.events {
            if old_state == NodeStateType::Dead || old_state == NodeStateType::Left {
                // if Dead/Left -> Alive, notify of join
                events.notify_join(&state.node);
            } else if old_meta != state.node.meta {
                // if Meta changed, trigger an update notification
                events.notify_update(&state.node);
            }
        }
    }

    /// Invoked by the network layer when we get a message about a suspect
    /// node.
    pub(crate) fn suspect_node(&self, s: &Suspect) {
        let mut table = self.nodes.write();
        let NodeTable {
            nodes,
            node_map,
            node_timers,
        } = &mut *table;

        // If we've never heard about this node before, ignore it
        let Some(&idx) = node_map.get(&s.node) else {
            return;
        };
        let state = &mut nodes[idx];

        // Ignore old incarnation numbers
        if s.incarnation < state.incarnation {
            return;
        }

        // See if there's a suspicion timer we can confirm. If the info is
        // new to us we will go ahead and re-gossip it. This allows for
        // multiple independent confirmations to flow even when a node probes
        // a node that's already suspect.
        if let Some(timer) = node_timers.get_mut(&s.node) {
            if timer.confirm(&s.from) {
                self.encode_and_broadcast(&s.node, MessageType::SUSPECT, s);
            }
            return;
        }

        // Ignore non-alive nodes
        if state.state != NodeStateType::Alive {
            return;
        }

        // If this is us we need to refute, otherwise re-broadcast
        if state.node.name == self.config.name {
            self.refute(state, s.incarnation);
            warn!("memberlist: Refuting a suspect message (from: {})", s.from);
            return; // Do not mark ourself suspect
        } else {
            self.encode_and_broadcast(&s.node, MessageType::SUSPECT, s);
        }

        // Update metrics
        metrics::counter!("memberlist.msg.suspect", self.config.metric_labels.clone()).increment(1);

        // Update the state
        state.incarnation = s.incarnation;
        state.state = NodeStateType::Suspect;
        let change_time = Instant::now();
        state.state_change = Some(change_time);

        // Setup a suspicion timer. Given that we don't have any known phase
        // relationship with our peers, we set up k such that we hit the
        // nominal timeout two probe intervals short of what we expect given
        // the suspicion multiplier.
        let mut k = self.config.suspicion_mult as i64 - 2;

        // If there aren't enough nodes to give the expected confirmations,
        // just set k to 0 to say that we don't expect any. Note we subtract
        // 2 from n here to take out ourselves and the node being probed.
        let n = self.est_num_nodes();
        if (n as i64) - 2 < k {
            k = 0;
        }

        // Compute the timeouts based on the size of the cluster.
        let min = suspicion_timeout(self.config.suspicion_mult, n, self.config.probe_interval);
        let max = min * self.config.suspicion_max_timeout_mult as u32;
        let weak = self.downgrade();
        let node_name = s.node.clone();
        let f = move |num_confirmations: i32| {
            let Some(m) = Memberlist::upgrade(&weak) else {
                return;
            };

            let d = {
                let table = m.nodes.read();
                match table.get(&node_name) {
                    Some(state)
                        if state.state == NodeStateType::Suspect
                            && state.state_change == Some(change_time) =>
                    {
                        Some(Dead {
                            incarnation: state.incarnation,
                            node: state.node.name.clone(),
                            from: m.config.name.clone(),
                        })
                    }
                    _ => None,
                }
            };

            if let Some(d) = d {
                if k > 0 && (num_confirmations as i64) < k {
                    metrics::counter!(
                        "memberlist.degraded.timeout",
                        m.config.metric_labels.clone()
                    )
                    .increment(1);
                }

                info!(
                    "memberlist: Marking {} as failed, suspect timeout reached ({} peer confirmations)",
                    d.node, num_confirmations
                );

                m.dead_node(&d);
            }
        };
        node_timers.insert(
            s.node.clone(),
            Suspicion::new(&s.from, k as i32, min, max, f),
        );
    }

    /// Invoked by the network layer when we get a message about a dead node.
    pub(crate) fn dead_node(&self, d: &Dead) {
        let mut table = self.nodes.write();
        let NodeTable {
            nodes,
            node_map,
            node_timers,
        } = &mut *table;

        // If we've never heard about this node before, ignore it
        let Some(&idx) = node_map.get(&d.node) else {
            return;
        };
        let state = &mut nodes[idx];

        // Ignore old incarnation numbers
        if d.incarnation < state.incarnation {
            return;
        }

        // Clear out any suspicion timer that may be in effect.
        node_timers.remove(&d.node);

        // Ignore if node is already dead
        if state.dead_or_left() {
            return;
        }

        // Check if this is us
        if state.node.name == self.config.name {
            // If we are not leaving we need to refute
            if !self.has_left() {
                self.refute(state, d.incarnation);
                warn!("memberlist: Refuting a dead message (from: {})", d.from);
                return; // Do not mark ourself dead
            }

            // If we are leaving, we broadcast and wait
            self.encode_broadcast_notify(
                &d.node,
                MessageType::DEAD,
                d,
                Some(self.leave_broadcast.clone()),
            );
        } else {
            self.encode_and_broadcast(&d.node, MessageType::DEAD, d);
        }

        // Update metrics
        metrics::counter!("memberlist.msg.dead", self.config.metric_labels.clone()).increment(1);

        // Update the state
        state.incarnation = d.incarnation;

        // If the dead message was send by the node itself, mark it is left
        // instead of dead.
        if d.node == d.from {
            state.state = NodeStateType::Left;
        } else {
            state.state = NodeStateType::Dead;
        }
        state.state_change = Some(Instant::now());

        // Notify of death
        if let Some(events) = &self.config.events {
            events.notify_leave(&state.node);
        }
    }

    /// Invoked by the network layer when we get a Push/Pull state transfer.
    pub(crate) fn merge_state(&self, remote: &[PushNodeState]) {
        for r in remote {
            match r.node_state() {
                Some(NodeStateType::Alive) => {
                    let a = Alive {
                        incarnation: r.incarnation,
                        node: r.name.clone(),
                        addr: r.addr.clone(),
                        port: r.port,
                        meta: r.meta.clone(),
                        vsn: r.vsn.clone(),
                    };
                    self.alive_node(&a, None, false);
                }
                Some(NodeStateType::Left) => {
                    let d = Dead {
                        incarnation: r.incarnation,
                        node: r.name.clone(),
                        from: r.name.clone(),
                    };
                    self.dead_node(&d);
                }
                // If the remote node believes a node is dead, we prefer to
                // suspect that node instead of declaring it dead instantly
                Some(NodeStateType::Dead) | Some(NodeStateType::Suspect) => {
                    let s = Suspect {
                        incarnation: r.incarnation,
                        node: r.name.clone(),
                        from: self.config.name.clone(),
                    };
                    self.suspect_node(&s);
                }
                None => {}
            }
        }
    }
}

/// Returns a random duration that is shorter than the interval.
fn random_stagger(interval: Duration) -> Duration {
    let nanos = interval.as_nanos().max(1) as u64;
    Duration::from_nanos(rand::rng().random::<u64>() % nanos)
}

/// Triggers a function call each time the interval passes, until the stop
/// token is cancelled.
async fn trigger_func<F, Fut>(interval: Duration, stop: CancellationToken, f: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()>,
{
    // The ticker starts before the stagger, like in the Go code. Ticks that
    // we are too slow for are skipped.
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // Use a random stagger to avoid syncronizing
    let rand_stagger = random_stagger(interval);
    tokio::select! {
        _ = tokio::time::sleep(rand_stagger) => {}
        _ = stop.cancelled() => return,
    }
    loop {
        tokio::select! {
            _ = ticker.tick() => f().await,
            _ = stop.cancelled() => return,
        }
    }
}
