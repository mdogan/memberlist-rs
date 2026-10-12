// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of config.go from https://github.com/hashicorp/memberlist.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use ipnet::IpNet;
use metrics::Label;

use crate::delegate::{
    AliveDelegate, ConflictDelegate, Delegate, EventDelegate, MergeDelegate, PingDelegate,
};
use crate::error::{Error, Result};
use crate::messages::{PROTOCOL_VERSION_2_COMPATIBLE, PROTOCOL_VERSION_MAX, PROTOCOL_VERSION_MIN};
use crate::transport::Transport;

/// The configuration of a [`Memberlist`](crate::Memberlist).
#[derive(Clone)]
pub struct Config {
    /// The name of this node. This must be unique in the cluster.
    pub name: String,

    /// A hook for providing custom code to communicate with other nodes. If
    /// this is left as `None`, then memberlist will by default make a
    /// NetTransport using `bind_addr` and `bind_port` from this structure.
    pub transport: Option<Arc<dyn Transport>>,

    /// An optional set of bytes to include on the outside of each packet and
    /// stream.
    pub label: String,

    /// Skips the check that inbound packets and gossip streams need to be
    /// label prefixed.
    pub skip_inbound_label_check: bool,

    /// Configuration related to what address to bind to and ports to listen
    /// on. The port is used for both UDP and TCP gossip. It is assumed other
    /// nodes are running on this port, but they do not need to.
    pub bind_addr: String,
    pub bind_port: u16,

    /// Configuration related to what address to advertise to other cluster
    /// members. Used for nat traversal.
    pub advertise_addr: String,
    pub advertise_port: u16,

    /// The configured protocol version that we will _speak_. This must be
    /// between `PROTOCOL_VERSION_MIN` and `PROTOCOL_VERSION_MAX`.
    pub protocol_version: u8,

    /// The timeout for establishing a stream connection with a remote node
    /// for a full state sync, and for stream read and write operations. This
    /// is a legacy name for backwards compatibility, but should really be
    /// called stream timeout now that we have generalized the transport.
    pub tcp_timeout: Duration,

    /// The number of nodes that will be asked to perform an indirect probe
    /// of a node in the case a direct probe fails. Memberlist waits for an
    /// ack from any single indirect node, so increasing this number will
    /// increase the likelihood that an indirect probe will succeed at the
    /// expense of bandwidth.
    pub indirect_checks: usize,

    /// The multiplier for the number of retransmissions that are attempted
    /// for messages broadcasted over gossip. The actual count of
    /// retransmissions is calculated using the formula:
    ///
    ///   Retransmits = RetransmitMult * log(N+1)
    ///
    /// This allows the retransmits to scale properly with cluster size. The
    /// higher the multiplier, the more likely a failed broadcast is to
    /// converge at the expense of increased bandwidth.
    pub retransmit_mult: usize,

    /// The multiplier for determining the time an inaccessible node is
    /// considered suspect before declaring it dead. The actual timeout is
    /// calculated using the formula:
    ///
    ///   SuspicionTimeout = SuspicionMult * log(N+1) * ProbeInterval
    ///
    /// This allows the timeout to scale properly with expected propagation
    /// delay with a larger cluster size. The higher the multiplier, the
    /// longer an inaccessible node is considered part of the cluster before
    /// declaring it dead, giving that suspect node more time to refute if it
    /// is indeed still alive.
    pub suspicion_mult: usize,

    /// The multiplier applied to the SuspicionTimeout used as an upper bound
    /// on detection time. This max timeout is calculated using the formula:
    ///
    /// SuspicionMaxTimeout = SuspicionMaxTimeoutMult * SuspicionTimeout
    ///
    /// If everything is working properly, confirmations from other nodes
    /// will accelerate suspicion timers in a manner which will cause the
    /// timeout to reach the base SuspicionTimeout before that elapses, so
    /// this value will typically only come into play if a node is
    /// experiencing issues communicating with other nodes. It should be set
    /// to a something fairly large so that a node having problems will have
    /// a lot of chances to recover before falsely declaring other nodes as
    /// failed, but short enough for a legitimately isolated node to still
    /// make progress marking nodes failed in a reasonable amount of time.
    pub suspicion_max_timeout_mult: usize,

    /// The interval between complete state syncs. Complete state syncs are
    /// done with a single node over TCP and are quite expensive relative to
    /// standard gossiped messages. Setting this to zero will disable state
    /// push/pull syncs completely.
    ///
    /// Setting this interval lower (more frequent) will increase convergence
    /// speeds across larger clusters at the expense of increased bandwidth
    /// usage.
    pub push_pull_interval: Duration,

    /// `probe_interval` and `probe_timeout` are used to configure probing
    /// behavior for memberlist.
    ///
    /// `probe_interval` is the interval between random node probes. Setting
    /// this lower (more frequent) will cause the memberlist cluster to
    /// detect failed nodes more quickly at the expense of increased
    /// bandwidth usage.
    ///
    /// `probe_timeout` is the timeout to wait for an ack from a probed node
    /// before assuming it is unhealthy. This should be set to 99-percentile
    /// of RTT (round-trip time) on your network.
    pub probe_interval: Duration,
    pub probe_timeout: Duration,

    /// Turns off the fallback TCP pings that are attempted if the direct UDP
    /// ping fails. These get pipelined along with the indirect UDP pings.
    pub disable_tcp_pings: bool,

    /// Like `disable_tcp_pings`, but lets you control whether to perform TCP
    /// pings on a node-by-node basis.
    #[allow(clippy::type_complexity)]
    pub disable_tcp_pings_for_node: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,

    /// Will increase the probe interval if the node becomes aware that it
    /// might be degraded and not meeting the soft real time requirements to
    /// reliably probe other nodes.
    pub awareness_max_multiplier: usize,

    /// `gossip_interval` and `gossip_nodes` are used to configure the gossip
    /// behavior of memberlist.
    ///
    /// `gossip_interval` is the interval between sending messages that need
    /// to be gossiped that haven't been able to piggyback on probing
    /// messages. If this is set to zero, non-piggyback gossip is disabled.
    /// By lowering this value (more frequent) gossip messages are propagated
    /// across the cluster more quickly at the expense of increased
    /// bandwidth.
    ///
    /// `gossip_nodes` is the number of random nodes to send gossip messages
    /// to per `gossip_interval`. Increasing this number causes the gossip
    /// messages to propagate across the cluster more quickly at the expense
    /// of increased bandwidth.
    ///
    /// `gossip_to_the_dead_time` is the interval after which a node has died
    /// that we will still try to gossip to it. This gives it a chance to
    /// refute.
    pub gossip_interval: Duration,
    pub gossip_nodes: usize,
    pub gossip_to_the_dead_time: Duration,

    /// Used to control message compression. This can be used to reduce
    /// bandwidth usage at the cost of slightly more CPU utilization. This is
    /// only available starting at protocol version 1.
    pub enable_compression: bool,

    /// `delegate` and `events` are delegates for receiving and providing
    /// data to memberlist via callback mechanisms. For `delegate`, see the
    /// [`Delegate`] trait. For `events`, see the [`EventDelegate`] trait.
    ///
    /// The `delegate_protocol_min`/`max` are used to guarantee
    /// protocol-compatibility for any custom messages that the delegate
    /// might do (broadcasts, local/remote state, etc.). If you don't set
    /// these, then the protocol versions will just be zero, and version
    /// compliance won't be done.
    pub delegate: Option<Arc<dyn Delegate>>,
    pub delegate_protocol_version: u8,
    pub delegate_protocol_min: u8,
    pub delegate_protocol_max: u8,
    pub events: Option<Arc<dyn EventDelegate>>,
    pub conflict: Option<Arc<dyn ConflictDelegate>>,
    pub merge: Option<Arc<dyn MergeDelegate>>,
    pub ping: Option<Arc<dyn PingDelegate>>,
    pub alive: Option<Arc<dyn AliveDelegate>>,

    /// Points to the system's DNS config file, usually located at
    /// /etc/resolv.conf. It can be overridden via config for easier testing.
    pub dns_config_path: String,

    /// Size of Memberlist's internal queue which handles UDP messages. The
    /// size of this determines the size of the queue which Memberlist will
    /// keep while UDP messages are handled.
    pub handoff_queue_depth: usize,

    /// Maximum number of bytes that memberlist will put in a packet (this
    /// will be for UDP packets by default with a NetTransport). A safe value
    /// for this is typically 1400 bytes (which is the default). However,
    /// depending on your network's MTU (Maximum Transmission Unit) you may
    /// be able to increase this to get more content into each gossip packet.
    /// This is a legacy name for backward compatibility but should really be
    /// called packet buffer size now that we have generalized the transport.
    pub udp_buffer_size: usize,

    /// Controls the time before a dead node's name can be reclaimed by one
    /// with a different address or port. By default, this is 0, meaning
    /// nodes cannot be reclaimed this way.
    pub dead_node_reclaim_time: Duration,

    /// Controls if the name of a node is required when sending a message to
    /// that node.
    pub require_node_names: bool,

    /// If empty, allow any connection (default), otherwise specify all
    /// networks allowed to connect (you must specify IPv6/IPv4 separately).
    pub cidrs_allowed: Vec<IpNet>,

    /// Optional labels to apply to all metrics emitted.
    pub metric_labels: Vec<Label>,

    /// The interval at which we check the message queue to apply the warning
    /// and max depth.
    pub queue_check_interval: Duration,
}

/// Returns a possibly empty list of all networks that have been parsed. In
/// case of an error, it returns an error that lists every CIDR that is not
/// valid.
pub fn parse_cidrs<S: AsRef<str>>(v: &[S]) -> Result<Vec<IpNet>> {
    let mut nets = Vec::with_capacity(v.len());
    let mut errs = Vec::new();
    for p in v {
        match p.as_ref().trim().parse::<IpNet>() {
            Ok(net) => nets.push(net.trunc()),
            Err(_) => errs.push(Error::msg(format!("invalid cidr: {}", p.as_ref()))),
        }
    }
    if !errs.is_empty() {
        return Err(Error::Multiple(errs));
    }
    Ok(nets)
}

impl Config {
    /// Returns a sane set of configurations for Memberlist. It uses the
    /// hostname as the node name, and otherwise sets very conservative
    /// values that are sane for most LAN environments. The default
    /// configuration errs on the side of caution, choosing values that are
    /// optimized for higher convergence at the cost of higher bandwidth
    /// usage. Regardless, these values are a good starting point when
    /// getting started with memberlist.
    pub fn default_lan() -> Config {
        let hostname = gethostname::gethostname().to_string_lossy().into_owned();
        Config {
            name: hostname,
            transport: None,
            label: String::new(),
            skip_inbound_label_check: false,
            bind_addr: "0.0.0.0".to_string(),
            bind_port: 7946,
            advertise_addr: String::new(),
            advertise_port: 7946,
            protocol_version: PROTOCOL_VERSION_2_COMPATIBLE,
            tcp_timeout: Duration::from_secs(10), // Timeout after 10 seconds
            indirect_checks: 3,                   // Use 3 nodes for the indirect ping
            retransmit_mult: 4,                   // Retransmit a message 4 * log(N+1) nodes
            suspicion_mult: 4,                    // Suspect a node for 4 * log(N+1) * Interval
            suspicion_max_timeout_mult: 6, // For 10k nodes this will give a max timeout of 120 seconds
            push_pull_interval: Duration::from_secs(30), // Low frequency
            probe_timeout: Duration::from_millis(500), // Reasonable RTT time for LAN
            probe_interval: Duration::from_secs(1), // Failure check every second
            disable_tcp_pings: false,      // TCP pings are safe, even with mixed versions
            disable_tcp_pings_for_node: None,
            awareness_max_multiplier: 8, // Probe interval backs off to 8 seconds

            gossip_nodes: 3,                                  // Gossip to 3 nodes
            gossip_interval: Duration::from_millis(200),      // Gossip more rapidly
            gossip_to_the_dead_time: Duration::from_secs(30), // Same as push/pull

            enable_compression: true, // Enable compression by default

            delegate: None,
            delegate_protocol_version: 0,
            delegate_protocol_min: 0,
            delegate_protocol_max: 0,
            events: None,
            conflict: None,
            merge: None,
            ping: None,
            alive: None,

            dns_config_path: "/etc/resolv.conf".to_string(),

            handoff_queue_depth: 1024,
            udp_buffer_size: 1400,
            dead_node_reclaim_time: Duration::ZERO,
            require_node_names: false,
            cidrs_allowed: Vec::new(), // same as allow all
            metric_labels: Vec::new(),

            queue_check_interval: Duration::from_secs(30),
        }
    }

    /// Works like `default_lan`, however it returns a configuration that is
    /// optimized for most WAN environments. The default configuration is
    /// still very conservative and errs on the side of caution.
    pub fn default_wan() -> Config {
        let mut conf = Config::default_lan();
        conf.tcp_timeout = Duration::from_secs(30);
        conf.suspicion_mult = 6;
        conf.push_pull_interval = Duration::from_secs(60);
        conf.probe_timeout = Duration::from_secs(3);
        conf.probe_interval = Duration::from_secs(5);
        conf.gossip_nodes = 4; // Gossip less frequently, but to an additional node
        conf.gossip_interval = Duration::from_millis(500);
        conf.gossip_to_the_dead_time = Duration::from_secs(60);
        conf
    }

    /// Works like `default_lan`, however it returns a configuration that is
    /// optimized for a local loopback environments. The default
    /// configuration is still very conservative and errs on the side of
    /// caution.
    pub fn default_local() -> Config {
        let mut conf = Config::default_lan();
        conf.tcp_timeout = Duration::from_secs(1);
        conf.indirect_checks = 1;
        conf.retransmit_mult = 2;
        conf.suspicion_mult = 3;
        conf.push_pull_interval = Duration::from_secs(15);
        conf.probe_timeout = Duration::from_millis(200);
        conf.probe_interval = Duration::from_secs(1);
        conf.gossip_interval = Duration::from_millis(100);
        conf.gossip_to_the_dead_time = Duration::from_secs(15);
        conf
    }

    /// Creates the array of versions that is sent in alive messages.
    pub fn build_vsn_array(&self) -> Vec<u8> {
        vec![
            PROTOCOL_VERSION_MIN,
            PROTOCOL_VERSION_MAX,
            self.protocol_version,
            self.delegate_protocol_min,
            self.delegate_protocol_max,
            self.delegate_protocol_version,
        ]
    }

    /// Returns true if `ip_allowed` must be called.
    pub fn ip_must_be_checked(&self) -> bool {
        !self.cidrs_allowed.is_empty()
    }

    /// Returns an error if access to memberlist is denied.
    pub fn ip_allowed(&self, ip: IpAddr) -> Result<()> {
        if !self.ip_must_be_checked() {
            return Ok(());
        }
        let ip = ip.to_canonical();
        for n in &self.cidrs_allowed {
            if n.contains(&ip) {
                return Ok(());
            }
        }
        Err(Error::msg(format!("{ip} is not allowed")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_address_defaults() {
        let tests = [
            "127.0.0.1",
            "127.0.0.5",
            "10.0.0.9",
            "172.16.0.7",
            "192.168.2.1",
            "fe80::aede:48ff:fe00:1122",
            "::1",
        ];
        let config = Config::default_lan();
        for ip in tests {
            let local: IpAddr = ip.parse().unwrap();
            assert!(
                config.ip_allowed(local).is_ok(),
                "IP {ip} Localhost Should be accepted for LAN"
            );
        }
        let config = Config::default_wan();
        for ip in tests {
            let local: IpAddr = ip.parse().unwrap();
            assert!(
                config.ip_allowed(local).is_ok(),
                "IP {ip} Localhost Should be accepted for WAN"
            );
        }
    }

    #[test]
    fn test_is_valid_address_override() {
        struct Case {
            name: &'static str,
            allow: &'static [&'static str],
            success: &'static [&'static str],
            fail: &'static [&'static str],
        }
        let cases = [
            Case {
                name: "Default, nil allows all",
                allow: &[],
                success: &["127.0.0.5", "10.0.0.9", "192.168.1.7", "::1"],
                fail: &[],
            },
            Case {
                name: "Only IPv4",
                allow: &["0.0.0.0/0"],
                success: &["127.0.0.5", "10.0.0.9", "192.168.1.7"],
                fail: &["fe80::38bc:4dff:fe62:b1ae", "::1"],
            },
            Case {
                name: "Only IPv6",
                allow: &["::0/0"],
                success: &["fe80::38bc:4dff:fe62:b1ae", "::1"],
                fail: &["127.0.0.5", "10.0.0.9", "192.168.1.7"],
            },
            Case {
                name: "Only 127.0.0.0/8 and ::1",
                allow: &["::1/128", "127.0.0.0/8"],
                success: &["127.0.0.5", "::1"],
                fail: &[
                    "::2",
                    "178.250.0.187",
                    "10.0.0.9",
                    "192.168.1.7",
                    "fe80::38bc:4dff:fe62:b1ae",
                ],
            },
        ];

        for case in cases {
            let mut config = Config::default_lan();
            config.cidrs_allowed = parse_cidrs(case.allow)
                .unwrap_or_else(|_| panic!("{}: failed parsing {:?}", case.name, case.allow));
            for ips in case.success {
                let ip: IpAddr = ips.parse().unwrap();
                assert!(
                    config.ip_allowed(ip).is_ok(),
                    "{}: Test case with {ip} should pass",
                    case.name
                );
            }
            for ips in case.fail {
                let ip: IpAddr = ips.parse().unwrap();
                assert!(
                    config.ip_allowed(ip).is_err(),
                    "{}: Test case with {ip} should fail",
                    case.name
                );
            }
        }
    }
}
