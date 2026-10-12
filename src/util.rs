// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of util.go from https://github.com/hashicorp/memberlist.

use std::net::IpAddr;
use std::time::Duration;

use bytes::Bytes;
use rand::RngExt;
use rand::seq::SliceRandom;
use weezl::{BitOrder, LzwStatus};

use crate::codec;
use crate::error::{Error, Result};
use crate::messages::{Compress, LZW_ALGO, MAX_DECOMPRESSED_BYTES, MessageType, encode};
use crate::state::{Node, NodeState};

/// The minimum number of nodes before we start scaling the push/pull timing.
/// The scale effect is the log2(Nodes) - log2(pushPullScale). This means
/// that the 33rd node will cause us to double the interval, while the 65th
/// will triple it.
const PUSH_PULL_SCALE_THRESHOLD: usize = 32;

/// Constant litWidth 2-8.
const LZW_LIT_WIDTH: u8 = 8;

/// Returns a random offset between 0 and n.
pub(crate) fn random_offset(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    (rand::rng().random::<u32>() % n as u32) as usize
}

/// Computes the timeout that should be used when a node is suspected.
pub(crate) fn suspicion_timeout(suspicion_mult: usize, n: usize, interval: Duration) -> Duration {
    let node_scale = f64::max(1.0, f64::max(1.0, n as f64).log10());
    // multiply by 1000 to keep some precision because the duration is an
    // integer number of nanoseconds
    let scale = (node_scale * 1000.0) as u128;
    let nanos = suspicion_mult as u128 * scale * interval.as_nanos() / 1000;
    Duration::from_nanos(nanos as u64)
}

/// Computes the limit of retransmissions.
pub(crate) fn retransmit_limit(retransmit_mult: usize, n: usize) -> usize {
    let node_scale = ((n + 1) as f64).log10().ceil();
    retransmit_mult * node_scale as usize
}

/// Randomly shuffles the input nodes using the Fisher-Yates shuffle.
pub(crate) fn shuffle_nodes(nodes: &mut [NodeState]) {
    nodes.shuffle(&mut rand::rng());
}

/// Scales the time interval at which push/pull syncs take place. It is used
/// to prevent network saturation as the cluster size grows.
pub(crate) fn push_pull_scale(interval: Duration, n: usize) -> Duration {
    // Don't scale until we cross the threshold
    if n <= PUSH_PULL_SCALE_THRESHOLD {
        return interval;
    }

    let multiplier = ((n as f64).log2() - (PUSH_PULL_SCALE_THRESHOLD as f64).log2()).ceil() + 1.0;
    interval * multiplier as u32
}

/// Moves dead and left nodes that have not changed during the
/// gossip_to_the_dead_time interval to the end of the slice and returns the
/// index of the first moved node.
pub(crate) fn move_dead_nodes(nodes: &mut [NodeState], gossip_to_the_dead_time: Duration) -> usize {
    let mut num_dead = 0;
    let n = nodes.len();
    let mut i = 0;
    while i < n - num_dead {
        if !nodes[i].dead_or_left() {
            i += 1;
            continue;
        }

        // Respect the gossip to the dead interval
        if nodes[i].since_state_change() <= gossip_to_the_dead_time {
            i += 1;
            continue;
        }

        // Move this node to the end. The node that takes its place has not
        // been looked at yet, so `i` stays where it is.
        nodes.swap(i, n - num_dead - 1);
        num_dead += 1;
    }
    n - num_dead
}

/// Selects up to k random nodes, excluding any nodes where the exclude
/// function returns true. It is possible that less than k nodes are returned.
pub(crate) fn k_random_nodes(
    k: usize,
    nodes: &[NodeState],
    exclude: impl Fn(&NodeState) -> bool,
) -> Vec<Node> {
    let n = nodes.len();
    let mut k_nodes: Vec<Node> = Vec::with_capacity(k);

    // When n is very small (ex. 3-5 node Raft clusters), we can easily miss
    // non-excluded nodes with random selection, so instead shuffle the slice
    // to ensure the search is exhaustive
    if n < k * 3 {
        let mut order: Vec<usize> = (0..n).collect();
        order.shuffle(&mut rand::rng());
        for idx in order {
            if k_nodes.len() >= k {
                break;
            }
            let state = &nodes[idx];
            if exclude(state) {
                continue;
            }
            k_nodes.push(state.node.clone());
        }
        return k_nodes;
    }

    // Probe up to 3*n times, with large n this is not necessary since k << n,
    // but when n >= k*3 but still not "large", we want to give the search a
    // shot at being exhaustive
    let mut i = 0;
    'outer: while i < 3 * n && k_nodes.len() < k {
        i += 1;
        let idx = random_offset(n);
        let state = &nodes[idx];

        if exclude(state) {
            continue 'outer;
        }

        // Check if we have this node already
        for picked in &k_nodes {
            if state.node.name == picked.name {
                continue 'outer;
            }
        }

        k_nodes.push(state.node.clone());
    }
    k_nodes
}

/// Takes a list of messages and packs them into one or multiple messages
/// based on the limitations of compound messages (255 messages each).
pub(crate) fn make_compound_messages(msgs: &[Bytes]) -> Vec<Vec<u8>> {
    const MAX_MSGS: usize = 255;
    msgs.chunks(MAX_MSGS).map(make_compound_message).collect()
}

/// Takes a list of messages and generates a single compound message
/// containing all of them.
pub(crate) fn make_compound_message<B: AsRef<[u8]>>(msgs: &[B]) -> Vec<u8> {
    let total: usize = msgs.iter().map(|m| m.as_ref().len() + 2).sum();
    let mut buf = Vec::with_capacity(2 + total);

    // Write out the type
    buf.push(MessageType::COMPOUND.0);

    // Write out the number of message
    buf.push(msgs.len() as u8);

    // Add the message lengths
    for m in msgs {
        buf.extend_from_slice(&(m.as_ref().len() as u16).to_be_bytes());
    }

    // Append the messages
    for m in msgs {
        buf.extend_from_slice(m.as_ref());
    }

    buf
}

/// Splits a compound message and returns the individual messages. Also
/// returns the number of truncated messages.
pub(crate) fn decode_compound_message(buf: &Bytes) -> Result<(usize, Vec<Bytes>)> {
    if buf.is_empty() {
        return Err(Error::msg("missing compound length byte"));
    }
    let num_parts = buf[0] as usize;
    let mut buf = buf.slice(1..);

    // Check we have enough bytes
    if buf.len() < num_parts * 2 {
        return Err(Error::msg("truncated len slice"));
    }

    // Decode the lengths
    let lengths: Vec<usize> = (0..num_parts)
        .map(|i| u16::from_be_bytes([buf[i * 2], buf[i * 2 + 1]]) as usize)
        .collect();
    buf = buf.slice(num_parts * 2..);

    // Split each message
    let mut parts = Vec::with_capacity(num_parts);
    for (idx, msg_len) in lengths.into_iter().enumerate() {
        if buf.len() < msg_len {
            return Ok((num_parts - idx, parts));
        }

        // Extract the slice, seek past on the buffer
        parts.push(buf.slice(..msg_len));
        buf = buf.slice(msg_len..);
    }
    Ok((0, parts))
}

/// Takes an opaque input buffer, compresses it and wraps it in a compress
/// message that is encoded.
pub(crate) fn compress_payload(inp: &[u8]) -> Result<Vec<u8>> {
    let buf = weezl::encode::Encoder::new(BitOrder::Lsb, LZW_LIT_WIDTH)
        .encode(inp)
        .map_err(|err| Error::msg(format!("lzw: {err}")))?;

    // Create a compressed message
    let c = Compress {
        algo: LZW_ALGO,
        buf,
    };
    Ok(encode(MessageType::COMPRESS, &c))
}

/// Unpacks an encoded compress message and returns its payload uncompressed.
pub(crate) fn decompress_payload(msg: &[u8]) -> Result<Vec<u8>> {
    // Decode the message
    let c: Compress = codec::decode(msg)?;
    decompress_buffer(&c)
}

/// Decompresses the buffer of a single compress message, handling multiple
/// algorithms.
pub(crate) fn decompress_buffer(c: &Compress) -> Result<Vec<u8>> {
    // Verify the algorithm
    if c.algo != LZW_ALGO {
        return Err(Error::msg(format!(
            "cannot decompress unknown algorithm {}",
            c.algo
        )));
    }

    // Create a uncompressor
    let mut decoder = weezl::decode::Decoder::new(BitOrder::Lsb, LZW_LIT_WIDTH);
    let mut inp = &c.buf[..];
    let mut out = Vec::new();
    let mut chunk = vec![0u8; 16 * 1024];
    loop {
        let res = decoder.decode_bytes(inp, &mut chunk);
        inp = &inp[res.consumed_in..];
        out.extend_from_slice(&chunk[..res.consumed_out]);

        // Ensure a compress message can't decompress to too much data.
        if out.len() > MAX_DECOMPRESSED_BYTES {
            return Err(Error::msg(format!(
                "decompressed message is larger than limit ({MAX_DECOMPRESSED_BYTES})"
            )));
        }

        match res.status {
            Ok(LzwStatus::Done) => break,
            Ok(LzwStatus::Ok) => {}
            // The input ended before the end-of-data code.
            Ok(LzwStatus::NoProgress) => return Err(Error::msg("unexpected EOF")),
            Err(err) => return Err(Error::msg(format!("lzw: {err}"))),
        }
    }

    // Return the uncompressed bytes
    Ok(out)
}

/// Converts the raw bytes of an address, as they are sent on the wire, into
/// an IP address. Returns `None` if the length is not 4 or 16.
pub(crate) fn ip_from_bytes(b: &[u8]) -> Option<IpAddr> {
    if let Ok(v4) = <[u8; 4]>::try_from(b) {
        return Some(IpAddr::from(v4));
    }
    <[u8; 16]>::try_from(b).ok().map(IpAddr::from)
}

/// Returns the raw bytes of an IP address, as they are sent on the wire.
pub(crate) fn ip_to_bytes(ip: &IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(v4) => v4.octets().to_vec(),
        IpAddr::V6(v6) => v6.octets().to_vec(),
    }
}

/// Converts the raw bytes of an address into the form that we show to the
/// user. Go members often send an IPv4 address in its 16 byte form
/// (IPv4-mapped IPv6). We show that as a plain IPv4 address, and the second
/// value says that it came in the 16 byte form.
pub(crate) fn addr_from_wire(b: &[u8]) -> Option<(IpAddr, bool)> {
    match ip_from_bytes(b)? {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => Some((IpAddr::V4(v4), true)),
            None => Some((IpAddr::V6(v6), false)),
        },
        v4 => Some((v4, false)),
    }
}

/// The opposite of [`addr_from_wire`].
pub(crate) fn addr_to_wire(ip: &IpAddr, mapped: bool) -> Vec<u8> {
    match ip {
        IpAddr::V4(v4) if mapped => v4.to_ipv6_mapped().octets().to_vec(),
        _ => ip_to_bytes(ip),
    }
}

/// Formats an IP address for use in a "host:port" address. An IPv4 address
/// that is mapped into IPv6 is printed in its IPv4 form, like Go does.
pub(crate) fn ip_string(ip: &IpAddr) -> String {
    ip.to_canonical().to_string()
}

/// Formats the raw bytes of an address. Unlike [`ip_string`] this also
/// handles bytes that are not a valid address.
pub(crate) fn ip_bytes_string(b: &[u8]) -> String {
    match ip_from_bytes(b) {
        Some(ip) => ip_string(&ip),
        None if b.is_empty() => "<nil>".to_string(),
        None => {
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            format!("?{hex}")
        }
    }
}

/// Returns the host:port form of an address, for use with a transport.
pub(crate) fn join_host_port(host: &str, port: u16) -> String {
    // We assume that host is a literal IPv6 address if host has colons.
    if host.contains(':') || host.contains('%') {
        return format!("[{host}]:{port}");
    }
    format!("{host}:{port}")
}

/// Splits a network address of the form "host:port", "host%zone:port",
/// "[host]:port" or "[host%zone]:port" into host and port.
pub(crate) fn split_host_port(hostport: &str) -> Result<(String, String)> {
    const MISSING_PORT: &str = "missing port in address";
    const TOO_MANY_COLONS: &str = "too many colons in address";
    let addr_err = |why: &str| Err(Error::msg(format!("address {hostport}: {why}")));

    // The port starts after the last colon.
    let Some(i) = hostport.rfind(':') else {
        return addr_err(MISSING_PORT);
    };

    let (host, j, k);
    if hostport.starts_with('[') {
        // Expect the first ']' just before the last ':'.
        let Some(end) = hostport.find(']') else {
            return addr_err("missing ']' in address");
        };
        if end + 1 == hostport.len() {
            // There can't be a ':' behind the ']' now.
            return addr_err(MISSING_PORT);
        } else if end + 1 != i {
            // Either ']' isn't followed by a colon, or it is followed by a
            // colon that is not the last one.
            if hostport.as_bytes()[end + 1] == b':' {
                return addr_err(TOO_MANY_COLONS);
            }
            return addr_err(MISSING_PORT);
        }
        host = &hostport[1..end];
        // there can't be a '[' resp. ']' before these positions
        j = 1;
        k = end + 1;
    } else {
        host = &hostport[..i];
        if host.contains(':') {
            return addr_err(TOO_MANY_COLONS);
        }
        j = 0;
        k = 0;
    }
    if hostport[j..].contains('[') {
        return addr_err("unexpected '[' in address");
    }
    if hostport[k..].contains(']') {
        return addr_err("unexpected ']' in address");
    }

    Ok((host.to_string(), hostport[i + 1..].to_string()))
}

/// Given a string of the form "host", "host:port", "ipv6::address", or
/// "[ipv6::address]:port", returns true if the string includes a port.
pub(crate) fn has_port(s: &str) -> bool {
    // IPv6 address in brackets.
    if s.rfind('[') == Some(0) {
        return match (s.rfind(':'), s.rfind(']')) {
            (Some(colon), Some(bracket)) => colon > bracket,
            (Some(_), None) => true,
            (None, _) => false,
        };
    }

    // Otherwise the presence of a single colon determines if there's a port
    // since IPv6 addresses outside of brackets (count > 1) can't have a
    // port.
    s.matches(':').count() == 1
}

/// Makes sure the given string has a port number on it, otherwise it appends
/// the given port as a default.
pub(crate) fn ensure_port(s: &str, port: u16) -> String {
    if has_port(s) {
        return s.to_string();
    }

    // If this is an IPv6 address, the join call will add another set of
    // brackets, so we have to trim before we add the default port.
    let s = s.trim_matches(|c| c == '[' || c == ']');
    join_host_port(s, port)
}
