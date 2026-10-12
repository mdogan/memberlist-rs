// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of memberlist.go from https://github.com/hashicorp/memberlist.

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::awareness::Awareness;
use crate::config::Config;
use crate::dns::tcp_lookup_ip;
use crate::error::{Error, Result};
use crate::label::LABEL_MAX_SIZE;
use crate::messages::{
    Alive, Dead, META_MAX_SIZE, MessageType, PROTOCOL_VERSION_MAX, PROTOCOL_VERSION_MIN,
};
use crate::net::MsgHandoff;
use crate::net_transport::{NetTransport, NetTransportConfig};
use crate::queue::TransmitLimitedQueue;
use crate::sockaddr::is_rfc6890;
use crate::state::{AckHandler, Node, NodeTable};
#[cfg(test)]
use crate::state::{NodeState, NodeStateType};
use crate::transport::{Address, LabelWrappedTransport, Transport};
use crate::util::{ensure_port, ip_string, ip_to_bytes, join_host_port, split_host_port};

/// Manages cluster membership and member failure detection using a gossip
/// based protocol.
///
/// A `Memberlist` is a cheap handle: cloning it gives another handle to the
/// same member.
#[derive(Clone)]
pub struct Memberlist {
    pub(crate) inner: Arc<Inner>,
}

/// The messages that were received over the packet interface and wait to be
/// handled.
#[derive(Default)]
pub(crate) struct MsgQueues {
    pub(crate) high_priority: VecDeque<MsgHandoff>,
    pub(crate) low_priority: VecDeque<MsgHandoff>,
}

/// The shared state of a memberlist. This type is public only because
/// `Memberlist` dereferences to it. All of its fields are private.
#[doc(hidden)]
pub struct Inner {
    /// Local sequence number
    pub(crate) sequence_num: AtomicU32,
    /// Local incarnation number
    pub(crate) incarnation: AtomicU32,
    /// Number of known nodes (estimate)
    pub(crate) num_nodes: Arc<AtomicU32>,
    /// Number of push/pull requests
    pub(crate) push_pull_req: AtomicU32,

    pub(crate) advertise: RwLock<(IpAddr, u16)>,

    pub(crate) config: Config,
    pub(crate) shutdown: AtomicBool,
    pub(crate) shutdown_token: CancellationToken,
    pub(crate) leave: AtomicBool,
    pub(crate) leave_broadcast: Arc<Notify>,

    /// Serializes calls to shutdown
    pub(crate) shutdown_lock: tokio::sync::Mutex<()>,
    /// Serializes calls to leave
    pub(crate) leave_lock: tokio::sync::Mutex<()>,

    pub(crate) transport: Arc<dyn Transport>,

    pub(crate) handoff: Notify,
    pub(crate) msg_queues: Mutex<MsgQueues>,

    pub(crate) nodes: RwLock<NodeTable>,
    pub(crate) awareness: Awareness,

    /// Stops the background tasks of `schedule` when it is cancelled.
    pub(crate) stop_tick: Mutex<Option<CancellationToken>>,
    pub(crate) probe_index: AtomicUsize,

    pub(crate) ack_handlers: Mutex<HashMap<u32, AckHandler>>,

    pub(crate) broadcasts: TransmitLimitedQueue,
}

impl Deref for Memberlist {
    type Target = Inner;

    fn deref(&self) -> &Inner {
        &self.inner
    }
}

/// Holds information about a node we want to try to join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IpPort {
    pub(crate) ip: IpAddr,
    pub(crate) port: u16,
    /// optional
    pub(crate) node_name: String,
}

impl Memberlist {
    /// Returns a handle that does not keep the memberlist alive. Timers use
    /// it, so that a pending timer cannot leak the memberlist.
    pub(crate) fn downgrade(&self) -> Weak<Inner> {
        Arc::downgrade(&self.inner)
    }

    pub(crate) fn upgrade(weak: &Weak<Inner>) -> Option<Memberlist> {
        weak.upgrade().map(|inner| Memberlist { inner })
    }

    /// Creates the network listeners.
    /// Does not schedule execution of background maintenance.
    pub(crate) async fn new_memberlist(mut conf: Config) -> Result<Memberlist> {
        if conf.protocol_version < PROTOCOL_VERSION_MIN {
            return Err(Error::msg(format!(
                "protocol version '{}' too low. Must be in range: [{}, {}]",
                conf.protocol_version, PROTOCOL_VERSION_MIN, PROTOCOL_VERSION_MAX
            )));
        } else if conf.protocol_version > PROTOCOL_VERSION_MAX {
            return Err(Error::msg(format!(
                "protocol version '{}' too high. Must be in range: [{}, {}]",
                conf.protocol_version, PROTOCOL_VERSION_MIN, PROTOCOL_VERSION_MAX
            )));
        }

        if conf.label.len() > LABEL_MAX_SIZE {
            return Err(Error::msg(format!(
                "could not use {:?} as a label: too long",
                conf.label
            )));
        }

        // Set up a network transport by default if a custom one wasn't given
        // by the config.
        let mut transport: Arc<dyn Transport> = match conf.transport.clone() {
            Some(transport) => transport,
            None => {
                let nc = NetTransportConfig {
                    bind_addrs: vec![conf.bind_addr.clone()],
                    bind_port: conf.bind_port,
                    metric_labels: conf.metric_labels.clone(),
                };

                // The dynamic bind port operation is inherently racy because
                // even though we are using the kernel to find a port for us,
                // we are attempting to bind multiple protocols (and
                // potentially multiple addresses) with the same port number.
                // We build in a few retries here since this often gets
                // transient errors in busy unit tests.
                let limit = if conf.bind_port == 0 { 10 } else { 1 };

                let mut last_err = None;
                let mut made = None;
                for _ in 0..limit {
                    match NetTransport::new(nc.clone()).await {
                        Ok(nt) => {
                            made = Some(nt);
                            break;
                        }
                        Err(err) => {
                            debug!("memberlist: Got bind error: {err}");
                            last_err = Some(err);
                        }
                    }
                }
                let Some(nt) = made else {
                    let err = last_err.map(|e| e.to_string()).unwrap_or_default();
                    return Err(Error::msg(format!(
                        "could not set up network transport: failed to obtain an address: {err}"
                    )));
                };

                if conf.bind_port == 0 {
                    let port = nt.get_auto_bind_port();
                    conf.bind_port = port;
                    conf.advertise_port = port;
                    debug!("memberlist: Using dynamic bind port {port}");
                }
                Arc::new(nt)
            }
        };

        if !conf.label.is_empty() {
            transport = Arc::new(LabelWrappedTransport {
                label: conf.label.clone(),
                transport,
            });
        }

        let num_nodes = Arc::new(AtomicU32::new(0));
        let broadcasts = {
            let num_nodes = num_nodes.clone();
            TransmitLimitedQueue::new(conf.retransmit_mult, move || {
                num_nodes.load(Ordering::SeqCst) as usize
            })
        };

        let m = Memberlist {
            inner: Arc::new(Inner {
                sequence_num: AtomicU32::new(0),
                incarnation: AtomicU32::new(0),
                num_nodes,
                push_pull_req: AtomicU32::new(0),
                advertise: RwLock::new((IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)),
                awareness: Awareness::new(
                    conf.awareness_max_multiplier,
                    conf.metric_labels.clone(),
                ),
                config: conf,
                shutdown: AtomicBool::new(false),
                shutdown_token: CancellationToken::new(),
                leave: AtomicBool::new(false),
                leave_broadcast: Arc::new(Notify::new()),
                shutdown_lock: tokio::sync::Mutex::new(()),
                leave_lock: tokio::sync::Mutex::new(()),
                transport,
                handoff: Notify::new(),
                msg_queues: Mutex::new(MsgQueues::default()),
                nodes: RwLock::new(NodeTable::default()),
                stop_tick: Mutex::new(None),
                probe_index: AtomicUsize::new(0),
                ack_handlers: Mutex::new(HashMap::new()),
                broadcasts,
            }),
        };

        // Get the final advertise address from the transport, which may need
        // to see which address we bound to. We'll refresh this each time we
        // send out an alive message.
        if let Err(err) = m.refresh_advertise() {
            // Do not leave the listeners of a transport that we made open.
            if m.config.transport.is_none() {
                let _ = m.transport.shutdown().await;
            }
            return Err(err);
        }

        tokio::spawn(m.clone().stream_listen());
        tokio::spawn(m.clone().packet_listen());
        tokio::spawn(m.clone().packet_handler());
        tokio::spawn(m.clone().check_broadcast_queue_depth());
        Ok(m)
    }

    /// Creates a new Memberlist using the given configuration. This will not
    /// connect to any other node (see `join`) yet, but will start all the
    /// listeners to allow other nodes to join this memberlist.
    pub async fn create(conf: Config) -> Result<Memberlist> {
        let m = Self::new_memberlist(conf).await?;
        if let Err(err) = m.set_alive() {
            let _ = m.shutdown().await;
            return Err(err);
        }
        m.schedule();
        Ok(m)
    }

    /// The configuration of this memberlist. If the configured bind port was
    /// zero, this has the port that was picked.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Takes an existing Memberlist and attempts to join a cluster by
    /// contacting all the given hosts and performing a state sync.
    /// Initially, the Memberlist only contains our own state, so doing this
    /// will cause remote nodes to become aware of the existence of this
    /// node, effectively joining the cluster.
    ///
    /// This returns the number of hosts successfully contacted and an error
    /// if none could be reached. If an error is returned, the node did not
    /// successfully join the cluster.
    pub async fn join<S: AsRef<str>>(&self, existing: &[S]) -> Result<usize> {
        let mut num_success = 0;
        let mut errs = Vec::new();
        for exist in existing {
            let exist = exist.as_ref();
            let addrs = match self.resolve_addr(exist).await {
                Ok(addrs) => addrs,
                Err(err) => {
                    let err = Error::msg(format!("failed to resolve {exist}: {err}"));
                    warn!("memberlist: {err}");
                    errs.push(err);
                    continue;
                }
            };

            for addr in addrs {
                let hp = join_host_port(&ip_string(&addr.ip), addr.port);
                let a = Address {
                    addr: hp,
                    name: addr.node_name,
                };
                if let Err(err) = self.push_pull_node(&a, true).await {
                    let err = Error::msg(format!("failed to join {}: {}", a.addr, err));
                    debug!("memberlist: {err}");
                    errs.push(err);
                    continue;
                }
                num_success += 1;
            }
        }
        if num_success > 0 || errs.is_empty() {
            return Ok(num_success);
        }
        Err(Error::Multiple(errs))
    }

    /// Resolves the address into a list of IPs and ports. If no port is
    /// given, the default is used.
    pub(crate) async fn resolve_addr(&self, host_str: &str) -> Result<Vec<IpPort>> {
        // First peel off any leading node name. This is optional.
        let (node_name, host_str) = match host_str.find('/') {
            Some(0) => return Err(Error::msg("empty node name provided")),
            Some(slash_idx) => (&host_str[..slash_idx], &host_str[slash_idx + 1..]),
            None => ("", host_str),
        };

        // This captures the supplied port, or the default one.
        let host_str = ensure_port(host_str, self.config.bind_port);
        let (host, sport) = split_host_port(&host_str)?;
        let port: u16 = sport
            .parse()
            .map_err(|err| Error::msg(format!("parsing {sport:?}: {err}")))?;

        let ip_port = |ip: IpAddr| IpPort {
            ip,
            port,
            node_name: node_name.to_string(),
        };

        // If it looks like an IP address we are done. The split_host_port
        // above will make sure the host part is in good shape for parsing,
        // even for IPv6 addresses.
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![ip_port(ip)]);
        }

        // First try TCP so we have the best chance for the largest list of
        // hosts to join. If this fails it's not fatal since this isn't a
        // standard way to query DNS, and we have a fallback below.
        match tcp_lookup_ip(&self.config.dns_config_path, &host).await {
            Ok(ips) if !ips.is_empty() => return Ok(ips.into_iter().map(ip_port).collect()),
            Ok(_) => {}
            Err(err) => {
                debug!(
                    "memberlist: TCP-first lookup failed for '{host_str}', falling back to UDP: {err}"
                );
            }
        }

        // If TCP didn't yield anything then use the normal resolver which
        // will try UDP, then might possibly try TCP again if the UDP
        // response indicates it was truncated.
        let ans = tokio::net::lookup_host((host.as_str(), port)).await?;
        Ok(ans.map(|addr| ip_port(addr.ip())).collect())
    }

    /// Marks this node as being alive. This is the same as if we received an
    /// alive notification our own network channel for ourself.
    pub(crate) fn set_alive(&self) -> Result<()> {
        // Get the final advertise address from the transport, which may need
        // to see which address we bound to.
        let (addr, port) = self.refresh_advertise()?;

        // Check if this is a public address without encryption
        if !is_rfc6890(&addr) {
            warn!("memberlist: Binding to public address without encryption!");
        }

        // Set any metadata from the delegate.
        let meta = self.delegate_meta()?;

        let a = Alive {
            incarnation: self.next_incarnation(),
            node: self.config.name.clone(),
            addr: ip_to_bytes(&addr),
            port,
            meta,
            vsn: self.config.build_vsn_array(),
        };
        self.alive_node(&a, None, true);

        Ok(())
    }

    /// Gets the node meta data from the delegate, if there is one.
    fn delegate_meta(&self) -> Result<Vec<u8>> {
        let Some(delegate) = &self.config.delegate else {
            return Ok(Vec::new());
        };
        let meta = delegate.node_meta(META_MAX_SIZE);
        if meta.len() > META_MAX_SIZE {
            return Err(Error::msg(
                "Node meta data provided is longer than the limit",
            ));
        }
        Ok(meta)
    }

    pub(crate) fn get_advertise(&self) -> (IpAddr, u16) {
        *self.advertise.read()
    }

    pub(crate) fn set_advertise(&self, addr: IpAddr, port: u16) {
        *self.advertise.write() = (addr, port);
    }

    pub(crate) fn refresh_advertise(&self) -> Result<(IpAddr, u16)> {
        let (addr, port) = self
            .transport
            .final_advertise_addr(&self.config.advertise_addr, self.config.advertise_port)
            .map_err(|err| Error::msg(format!("failed to get final advertise address: {err}")))?;
        self.set_advertise(addr, port);
        Ok((addr, port))
    }

    /// Returns the local Node.
    pub fn local_node(&self) -> Node {
        let nodes = self.nodes.read();
        nodes
            .get(&self.config.name)
            .map(|state| state.node.clone())
            .expect("the local node is always in the node map")
    }

    /// Triggers re-advertising the local node. This is primarily used with a
    /// Delegate to support dynamic updates to the local meta data. This
    /// will block until the update message is successfully broadcasted to a
    /// member of the cluster, if any exist or until a specified timeout is
    /// reached. A zero timeout means no timeout.
    pub async fn update_node(&self, timeout: Duration) -> Result<()> {
        // Get the node meta data
        let meta = self.delegate_meta()?;

        // Get the existing node
        let (addr, port) = {
            let nodes = self.nodes.read();
            let state = nodes
                .get(&self.config.name)
                .expect("the local node is always in the node map");
            (state.node.wire_addr(), state.node.port)
        };

        // Format a new alive message
        let a = Alive {
            incarnation: self.next_incarnation(),
            node: self.config.name.clone(),
            addr,
            port,
            meta,
            vsn: self.config.build_vsn_array(),
        };
        let notify = Arc::new(Notify::new());
        self.alive_node(&a, Some(notify.clone()), true);

        // Wait for the broadcast or a timeout
        if self.any_alive() {
            if timeout.is_zero() {
                notify.notified().await;
            } else if tokio::time::timeout(timeout, notify.notified())
                .await
                .is_err()
            {
                return Err(Error::msg("timeout waiting for update broadcast"));
            }
        }
        Ok(())
    }

    /// Uses the unreliable packet-oriented interface of the transport to
    /// send a user message to the given address (this does not use the
    /// gossip mechanism).
    pub async fn send_to_address(&self, a: &Address, msg: &[u8]) -> Result<()> {
        // Encode as a user message
        let mut buf = Vec::with_capacity(msg.len() + 1);
        buf.push(MessageType::USER.0);
        buf.extend_from_slice(msg);

        // Send the message
        self.raw_send_msg_packet(a, None, &buf).await
    }

    /// Uses the unreliable packet-oriented interface of the transport to
    /// target a user message at the given node (this does not use the
    /// gossip mechanism). The maximum size of the message depends on the
    /// configured `udp_buffer_size` for this memberlist instance.
    pub async fn send_best_effort(&self, to: &Node, msg: &[u8]) -> Result<()> {
        // Encode as a user message
        let mut buf = Vec::with_capacity(msg.len() + 1);
        buf.push(MessageType::USER.0);
        buf.extend_from_slice(msg);

        // Send the message
        let a = Address {
            addr: to.address(),
            name: to.name.clone(),
        };
        self.raw_send_msg_packet(&a, Some(to), &buf).await
    }

    /// Uses the reliable stream-oriented interface of the transport to
    /// target a user message at the given node (this does not use the
    /// gossip mechanism). Delivery is guaranteed if no error is returned,
    /// and there is no limit on the size of the message.
    pub async fn send_reliable(&self, to: &Node, msg: &[u8]) -> Result<()> {
        self.send_user_msg(&to.full_address(), msg).await
    }

    /// Returns a list of all known live nodes.
    pub fn members(&self) -> Vec<Node> {
        let nodes = self.nodes.read();
        nodes
            .nodes
            .iter()
            .filter(|n| !n.dead_or_left())
            .map(|n| n.node.clone())
            .collect()
    }

    /// Returns the number of alive nodes currently known. Between the time
    /// of calling this and calling `members`, the number of alive nodes may
    /// have changed, so this shouldn't be used to determine how many members
    /// will be returned by `members`.
    pub fn num_members(&self) -> usize {
        let nodes = self.nodes.read();
        nodes.nodes.iter().filter(|n| !n.dead_or_left()).count()
    }

    /// Broadcasts a leave message but will not shutdown the background
    /// listeners, meaning the node will continue participating in gossip and
    /// state updates.
    ///
    /// This will block until the leave message is successfully broadcasted
    /// to a member of the cluster, if any exist or until a specified timeout
    /// is reached. A zero timeout means no timeout.
    ///
    /// This method is safe to call multiple times, but must not be called
    /// after the cluster is already shut down.
    pub async fn leave(&self, timeout: Duration) -> Result<()> {
        let _guard = self.leave_lock.lock().await;

        if self.has_shutdown() {
            return Err(Error::Shutdown);
        }

        if !self.has_left() {
            self.leave.store(true, Ordering::SeqCst);

            let state = {
                let nodes = self.nodes.read();
                nodes
                    .get(&self.config.name)
                    .map(|state| (state.incarnation, state.node.name.clone()))
            };
            let Some((incarnation, name)) = state else {
                warn!("memberlist: Leave but we're not in the node map.");
                return Ok(());
            };

            // This dead message is special, because Node and From are the
            // same. This helps other nodes figure out that a node left
            // intentionally. When Node equals From, other nodes know for
            // sure this node is gone.
            let d = Dead {
                incarnation,
                node: name.clone(),
                from: name,
            };
            self.dead_node(&d);

            // Block until the broadcast goes out
            if self.any_alive() {
                if timeout.is_zero() {
                    self.leave_broadcast.notified().await;
                } else if tokio::time::timeout(timeout, self.leave_broadcast.notified())
                    .await
                    .is_err()
                {
                    return Err(Error::msg("timeout waiting for leave broadcast"));
                }
            }
        }

        Ok(())
    }

    /// Check for any other alive node.
    pub(crate) fn any_alive(&self) -> bool {
        let nodes = self.nodes.read();
        nodes
            .nodes
            .iter()
            .any(|n| !n.dead_or_left() && n.node.name != self.config.name)
    }

    /// Gives this instance's idea of how well it is meeting the soft
    /// real-time requirements of the protocol. Lower numbers are better, and
    /// zero means "totally healthy".
    pub fn get_health_score(&self) -> usize {
        self.awareness.get_health_score().max(0) as usize
    }

    /// Returns the protocol version currently in use by this memberlist.
    pub fn protocol_version(&self) -> u8 {
        // NOTE: This method exists so that in the future we can control
        // any locking if necessary, if we change the protocol version at
        // runtime, etc.
        self.config.protocol_version
    }

    /// Stops any background maintenance of network activity for this
    /// memberlist, causing it to appear "dead". A leave message will not be
    /// broadcasted prior, so the cluster being left will have to detect this
    /// node's shutdown using probing. If you wish to more gracefully exit
    /// the cluster, call `leave` prior to shutting down.
    ///
    /// This method is safe to call multiple times.
    pub async fn shutdown(&self) -> Result<()> {
        let _guard = self.shutdown_lock.lock().await;

        if self.has_shutdown() {
            return Ok(());
        }

        // Shut down the transport first, which should block until it's
        // completely torn down. If we kill the memberlist-side handlers
        // those I/O handlers might get stuck.
        if let Err(err) = self.transport.shutdown().await {
            error!("Failed to shutdown transport: {err}");
        }

        // Now tear down everything else.
        self.shutdown.store(true, Ordering::SeqCst);
        self.shutdown_token.cancel();
        self.deschedule();
        Ok(())
    }

    pub(crate) fn has_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    pub(crate) fn has_left(&self) -> bool {
        self.leave.load(Ordering::SeqCst)
    }

    /// Returns the state of the node with the given name. For tests.
    #[cfg(test)]
    pub(crate) fn get_node_state(&self, name: &str) -> NodeStateType {
        self.nodes.read().get(name).expect("unknown node").state
    }

    /// Returns the time of the last state change of the node with the given
    /// name. For tests.
    #[cfg(test)]
    pub(crate) fn get_node_state_change(&self, name: &str) -> Option<std::time::Instant> {
        self.nodes
            .read()
            .get(name)
            .expect("unknown node")
            .state_change
    }

    /// Changes the node with the given name. For tests.
    #[cfg(test)]
    pub(crate) fn change_node(&self, name: &str, f: impl FnOnce(&mut NodeState)) {
        let mut nodes = self.nodes.write();
        f(nodes.get_mut(name).expect("unknown node"));
    }

    /// Periodically checks the size of the broadcast queue to see if it is
    /// too large.
    async fn check_broadcast_queue_depth(self) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.config.queue_check_interval) => {
                    let numq = self.broadcasts.num_queued();
                    metrics::histogram!(
                        "memberlist.queue.broadcasts",
                        self.config.metric_labels.clone()
                    )
                    .record(numq as f64);
                }
                _ = self.shutdown_token.cancelled() => return,
            }
        }
    }
}
