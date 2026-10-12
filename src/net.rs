// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of net.go from https://github.com/hashicorp/memberlist.

use std::borrow::Cow;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::Bytes;
use metrics::Label;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::codec::{self, Message};
use crate::conn::{Conn, StreamReader};
use crate::error::{Error, Result};
use crate::label::{remove_label_header_from_packet, remove_label_header_from_stream};
use crate::logging::{log_address, log_string_address};
use crate::memberlist::Memberlist;
use crate::messages::{
    AckResp, Alive, Compress, Dead, ErrResp, IndirectPingReq, MAX_PUSH_PULL_REQUESTS,
    MAX_PUSH_STATE_BYTES, MAX_PUSH_STATE_NODES, MAX_USER_MSG_BYTES, MessageType, NackResp, Ping,
    PushNodeState, PushPullHeader, Suspect, UserMsgHeader, encode,
};
use crate::state::{Node, NodeStateType};
use crate::transport::{Address, Stream};
use crate::util::{
    compress_payload, decode_compound_message, decompress_buffer, decompress_payload,
    ip_bytes_string, ip_from_bytes, ip_to_bytes, join_host_port, make_compound_message,
    split_host_port,
};

/// Used to transfer a message between tasks.
pub(crate) struct MsgHandoff {
    pub(crate) msg_type: MessageType,
    pub(crate) buf: Bytes,
    pub(crate) from: SocketAddr,
}

/// Counts a pending push/pull request for as long as it lives.
struct PushPullGuard<'a>(&'a Memberlist);

impl Drop for PushPullGuard<'_> {
    fn drop(&mut self) {
        self.0.push_pull_req.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Memberlist {
    /// A long running task that pulls incoming streams from the transport
    /// and hands them off for processing.
    pub(crate) async fn stream_listen(self) {
        loop {
            tokio::select! {
                conn = self.transport.accept_stream() => {
                    let Some(conn) = conn else {
                        return;
                    };
                    let m = self.clone();
                    tokio::spawn(async move { m.handle_conn(conn).await });
                }
                _ = self.shutdown_token.cancelled() => return,
            }
        }
    }

    /// Handles a single incoming stream connection from the transport.
    pub(crate) async fn handle_conn(&self, stream: Stream) {
        let remote = log_address(stream.remote_addr());
        debug!("memberlist: Stream connection {remote}");

        metrics::counter!("memberlist.tcp.accept", self.config.metric_labels.clone()).increment(1);

        let deadline = Instant::now() + self.config.tcp_timeout;

        // If reading the label header from the stream fails, the connection
        // is closed, because it is dropped.
        let removed = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            remove_label_header_from_stream(stream),
        )
        .await
        .unwrap_or_else(|_| Err(crate::error::timeout_error().into()));
        let (stream, mut stream_label) = match removed {
            Ok(res) => res,
            Err(err) => {
                error!(
                    "memberlist: failed to receive and remove the stream label header: {err} {remote}"
                );
                return;
            }
        };

        // The connection is always closed when we return, because it is
        // dropped.
        let mut conn = Conn::new(stream);
        conn.set_deadline(deadline);

        if self.config.skip_inbound_label_check {
            if !stream_label.is_empty() {
                error!(
                    "memberlist: unexpected double stream label header: {}",
                    conn.log()
                );
                return;
            }
            // Set this from config so that the label check below passes.
            stream_label = self.config.label.clone();
        }

        if self.config.label != stream_label {
            error!(
                "memberlist: discarding stream with unacceptable label {:?}: {}",
                stream_label,
                conn.log()
            );
            return;
        }

        let (msg_type, mut reader) = match self.read_stream(&mut conn).await {
            Ok(res) => res,
            Err(Error::Eof) => return,
            Err(err) => {
                error!("memberlist: failed to receive: {err} {}", conn.log());

                let resp = ErrResp {
                    error: err.to_string(),
                };
                let out = encode(MessageType::ERR, &resp);

                if let Err(err) = self.raw_send_msg_stream(&mut conn, &out).await {
                    error!("memberlist: Failed to send error: {err} {}", conn.log());
                }
                return;
            }
        };

        match msg_type {
            MessageType::USER => {
                if let Err(err) = self.read_user_msg(&mut reader).await {
                    error!("memberlist: Failed to receive user message: {err} {remote}");
                }
            }
            MessageType::PUSH_PULL => {
                // Increment counter of pending push/pulls
                let num_concurrent = self.push_pull_req.fetch_add(1, Ordering::SeqCst) + 1;
                let _guard = PushPullGuard(self);

                // Check if we have too many open push/pull requests
                if num_concurrent >= MAX_PUSH_PULL_REQUESTS {
                    error!("memberlist: Too many pending push/pull requests");
                    return;
                }

                let (join, remote_nodes, user_state) =
                    match self.read_remote_state(&mut reader).await {
                        Ok(res) => res,
                        Err(err) => {
                            error!("memberlist: Failed to read remote state: {err} {remote}");
                            return;
                        }
                    };
                drop(reader);

                if let Err(err) = self.send_local_state(&mut conn, join).await {
                    error!("memberlist: Failed to push local state: {err} {remote}");
                    return;
                }

                if let Err(err) = self.merge_remote_state(join, &remote_nodes, &user_state) {
                    error!("memberlist: Failed push/pull merge: {err} {remote}");
                }
            }
            MessageType::PING => {
                let p: Ping = match reader.decode().await {
                    Ok(p) => p,
                    Err(err) => {
                        error!("memberlist: Failed to decode ping: {err} {remote}");
                        return;
                    }
                };
                drop(reader);

                if !p.node.is_empty() && p.node != self.config.name {
                    warn!(
                        "memberlist: Got ping for unexpected node {} {remote}",
                        p.node
                    );
                    return;
                }

                let ack = AckResp {
                    seq_no: p.seq_no,
                    payload: Vec::new(),
                };
                let out = encode(MessageType::ACK_RESP, &ack);

                if let Err(err) = self.raw_send_msg_stream(&mut conn, &out).await {
                    error!("memberlist: Failed to send ack: {err} {remote}");
                }
            }
            _ => {
                error!("memberlist: Received invalid msgType ({msg_type}) {remote}");
            }
        }
    }

    /// A long running task that pulls packets out of the transport and hands
    /// them off for processing.
    pub(crate) async fn packet_listen(self) {
        loop {
            tokio::select! {
                packet = self.transport.recv_packet() => {
                    let Some(packet) = packet else {
                        return;
                    };
                    self.ingest_packet(packet.buf, packet.from, packet.timestamp).await;
                }
                _ = self.shutdown_token.cancelled() => return,
            }
        }
    }

    pub(crate) async fn ingest_packet(&self, buf: Bytes, from: SocketAddr, timestamp: Instant) {
        let (buf, mut packet_label) = match remove_label_header_from_packet(&buf) {
            Ok((rest, label)) => (buf.slice_ref(rest), label),
            Err(err) => {
                error!("memberlist: {err} {}", log_address(Some(from)));
                return;
            }
        };

        if self.config.skip_inbound_label_check {
            if !packet_label.is_empty() {
                error!(
                    "memberlist: unexpected double packet label header: {}",
                    log_address(Some(from))
                );
                return;
            }
            // Set this from config so that the label check below passes.
            packet_label = self.config.label.clone();
        }

        if self.config.label != packet_label {
            error!(
                "memberlist: discarding packet with unacceptable label {:?}: {}",
                packet_label,
                log_address(Some(from))
            );
            return;
        }

        // See if there's a checksum included to verify the contents of the
        // message
        if buf.len() >= 5 && buf[0] == MessageType::HAS_CRC.0 {
            let crc = crc32fast::hash(&buf[5..]);
            let expected = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
            if crc != expected {
                warn!("memberlist: Got invalid checksum for UDP packet: {crc:x}, {expected:x}");
                return;
            }
            self.handle_command(buf.slice(5..), from, timestamp).await;
        } else {
            self.handle_command(buf, from, timestamp).await;
        }
    }

    /// Handles one message from a packet. A compound or compressed message
    /// holds other messages, which are handled by calling this again. That
    /// is why the future is boxed.
    pub(crate) fn handle_command<'a>(
        &'a self,
        buf: Bytes,
        from: SocketAddr,
        timestamp: Instant,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if buf.is_empty() {
                error!(
                    "memberlist: missing message type byte {}",
                    log_address(Some(from))
                );
                return;
            }
            // Decode the message type
            let msg_type = MessageType(buf[0]);
            let buf = buf.slice(1..);

            // Switch on the msgType
            match msg_type {
                MessageType::COMPOUND => self.handle_compound(buf, from, timestamp).await,
                MessageType::COMPRESS => self.handle_compressed(buf, from, timestamp).await,

                MessageType::PING => self.handle_ping(&buf, from).await,
                MessageType::INDIRECT_PING => self.handle_indirect_ping(&buf, from).await,
                MessageType::ACK_RESP => self.handle_ack(&buf, from, timestamp),
                MessageType::NACK_RESP => self.handle_nack(&buf, from),

                MessageType::SUSPECT
                | MessageType::ALIVE
                | MessageType::DEAD
                | MessageType::USER => {
                    // Determine the message queue, prioritize alive
                    {
                        let mut queues = self.msg_queues.lock();
                        let queue = if msg_type == MessageType::ALIVE {
                            &mut queues.high_priority
                        } else {
                            &mut queues.low_priority
                        };

                        // Check for overflow and append if not full
                        if queue.len() >= self.config.handoff_queue_depth {
                            warn!(
                                "memberlist: handler queue full, dropping message ({msg_type}) {}",
                                log_address(Some(from))
                            );
                        } else {
                            queue.push_back(MsgHandoff {
                                msg_type,
                                buf,
                                from,
                            });
                        }
                    }

                    // Notify of pending message
                    self.handoff.notify_one();
                }

                _ => {
                    error!(
                        "memberlist: msg type ({msg_type}) not supported {}",
                        log_address(Some(from))
                    );
                }
            }
        })
    }

    /// Returns the next message to process in priority order, using LIFO.
    pub(crate) fn get_next_message(&self) -> Option<MsgHandoff> {
        let mut queues = self.msg_queues.lock();

        if let Some(msg) = queues.high_priority.pop_back() {
            return Some(msg);
        }
        queues.low_priority.pop_back()
    }

    /// A long running task that processes messages received over the packet
    /// interface, but is decoupled from the listener to avoid blocking the
    /// listener which may cause ping/ack messages to be delayed.
    pub(crate) async fn packet_handler(self) {
        loop {
            tokio::select! {
                _ = self.handoff.notified() => {
                    while let Some(msg) = self.get_next_message() {
                        let MsgHandoff { msg_type, buf, from } = msg;

                        match msg_type {
                            MessageType::SUSPECT => self.handle_suspect(&buf, from),
                            MessageType::ALIVE => self.handle_alive(&buf, from),
                            MessageType::DEAD => self.handle_dead(&buf, from),
                            MessageType::USER => self.handle_user(&buf, from),
                            _ => {
                                error!(
                                    "memberlist: Message type ({msg_type}) not supported {} (packet handler)",
                                    log_address(Some(from))
                                );
                            }
                        }

                        // Give other tasks a chance to run between messages.
                        tokio::task::yield_now().await;
                    }
                }
                _ = self.shutdown_token.cancelled() => return,
            }
        }
    }

    async fn handle_compound(&self, buf: Bytes, from: SocketAddr, timestamp: Instant) {
        // Decode the parts
        let (trunc, parts) = match decode_compound_message(&buf) {
            Ok(res) => res,
            Err(err) => {
                error!(
                    "memberlist: Failed to decode compound request: {err} {}",
                    log_address(Some(from))
                );
                return;
            }
        };

        // Log any truncation
        if trunc > 0 {
            warn!(
                "memberlist: Compound request had {trunc} truncated messages {}",
                log_address(Some(from))
            );
        }

        // Handle each message
        for part in parts {
            self.handle_command(part, from, timestamp).await;
        }
    }

    async fn handle_ping(&self, buf: &[u8], from: SocketAddr) {
        let p: Ping = match codec::decode(buf) {
            Ok(p) => p,
            Err(err) => {
                error!(
                    "memberlist: Failed to decode ping request: {err} {}",
                    log_address(Some(from))
                );
                return;
            }
        };
        // If node is provided, verify that it is for us
        if !p.node.is_empty() && p.node != self.config.name {
            warn!(
                "memberlist: Got ping for unexpected node '{}' {}",
                p.node,
                log_address(Some(from))
            );
            return;
        }
        let mut ack = AckResp {
            seq_no: p.seq_no,
            payload: Vec::new(),
        };
        if let Some(ping) = &self.config.ping {
            ack.payload = ping.ack_payload();
        }

        let addr = if !p.source_addr.is_empty() && p.source_port > 0 {
            join_host_port(&ip_bytes_string(&p.source_addr), p.source_port)
        } else {
            from.to_string()
        };

        let a = Address {
            addr,
            name: p.source_node,
        };
        if let Err(err) = self
            .encode_and_send_msg(&a, MessageType::ACK_RESP, &ack)
            .await
        {
            error!(
                "memberlist: Failed to send ack: {err} {}",
                log_address(Some(from))
            );
        }
    }

    async fn handle_indirect_ping(&self, buf: &[u8], from: SocketAddr) {
        let mut ind: IndirectPingReq = match codec::decode(buf) {
            Ok(ind) => ind,
            Err(err) => {
                error!(
                    "memberlist: Failed to decode indirect ping request: {err} {}",
                    log_address(Some(from))
                );
                return;
            }
        };

        // For proto versions < 2, there is no port provided. Mask old
        // behavior by using the configured port.
        if self.protocol_version() < 2 || ind.port == 0 {
            ind.port = self.config.bind_port;
        }

        // Send a ping to the correct host.
        let local_seq_no = self.next_seq_no();
        let (self_addr, self_port) = self.get_advertise();
        let ping = Ping {
            seq_no: local_seq_no,
            node: ind.node.clone(),
            // The outbound message is addressed FROM us.
            source_addr: ip_to_bytes(&self_addr),
            source_port: self_port,
            source_node: self.config.name.clone(),
        };

        // Forward the ack back to the requestor. If the request encodes an
        // origin use that otherwise assume that the other end of the UDP
        // socket is usable.
        let ind_addr = if !ind.source_addr.is_empty() && ind.source_port > 0 {
            join_host_port(&ip_bytes_string(&ind.source_addr), ind.source_port)
        } else {
            from.to_string()
        };
        let requestor = Address {
            addr: ind_addr.clone(),
            name: ind.source_node.clone(),
        };

        // Setup a response handler to relay the ack
        let cancel = CancellationToken::new();
        let resp_handler = {
            let cancel = cancel.clone();
            let weak = self.downgrade();
            let requestor = requestor.clone();
            let ind_addr = ind_addr.clone();
            let seq_no = ind.seq_no;
            move |_payload: Vec<u8>, _timestamp: Instant| {
                // Try to prevent the nack if we've caught it in time.
                cancel.cancel();

                let Some(m) = Memberlist::upgrade(&weak) else {
                    return;
                };
                tokio::spawn(async move {
                    let ack = AckResp {
                        seq_no,
                        payload: Vec::new(),
                    };
                    if let Err(err) = m
                        .encode_and_send_msg(&requestor, MessageType::ACK_RESP, &ack)
                        .await
                    {
                        error!(
                            "memberlist: Failed to forward ack: {err} {}",
                            log_string_address(&ind_addr)
                        );
                    }
                });
            }
        };
        self.set_ack_handler(local_seq_no, resp_handler, self.config.probe_timeout);

        // Send the ping.
        let a = Address {
            addr: join_host_port(&ip_bytes_string(&ind.target), ind.port),
            name: ind.node.clone(),
        };
        if let Err(err) = self.encode_and_send_msg(&a, MessageType::PING, &ping).await {
            error!(
                "memberlist: Failed to send indirect ping: {err} {}",
                log_string_address(&ind_addr)
            );
        }

        // Setup a timer to fire off a nack if no ack is seen in time.
        if ind.nack {
            let m = self.clone();
            let seq_no = ind.seq_no;
            tokio::spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = tokio::time::sleep(m.config.probe_timeout) => {
                        let nack = NackResp { seq_no };
                        if let Err(err) = m
                            .encode_and_send_msg(&requestor, MessageType::NACK_RESP, &nack)
                            .await
                        {
                            error!(
                                "memberlist: Failed to send nack: {err} {}",
                                log_string_address(&ind_addr)
                            );
                        }
                    }
                }
            });
        }
    }

    fn handle_ack(&self, buf: &[u8], from: SocketAddr, timestamp: Instant) {
        let ack: AckResp = match codec::decode(buf) {
            Ok(ack) => ack,
            Err(err) => {
                error!(
                    "memberlist: Failed to decode ack response: {err} {}",
                    log_address(Some(from))
                );
                return;
            }
        };
        self.invoke_ack_handler(ack.seq_no, ack.payload, timestamp);
    }

    fn handle_nack(&self, buf: &[u8], from: SocketAddr) {
        let nack: NackResp = match codec::decode(buf) {
            Ok(nack) => nack,
            Err(err) => {
                error!(
                    "memberlist: Failed to decode nack response: {err} {}",
                    log_address(Some(from))
                );
                return;
            }
        };
        self.invoke_nack_handler(nack.seq_no);
    }

    fn handle_suspect(&self, buf: &[u8], from: SocketAddr) {
        let sus: Suspect = match codec::decode(buf) {
            Ok(sus) => sus,
            Err(err) => {
                error!(
                    "memberlist: Failed to decode suspect message: {err} {}",
                    log_address(Some(from))
                );
                return;
            }
        };
        self.suspect_node(&sus);
    }

    /// Returns an error if the client at this address must not connect.
    pub(crate) fn ensure_can_connect(&self, from: SocketAddr) -> Result<()> {
        if !self.config.ip_must_be_checked() {
            return Ok(());
        }
        self.config.ip_allowed(from.ip())
    }

    pub(crate) fn handle_alive(&self, buf: &[u8], from: SocketAddr) {
        if let Err(err) = self.ensure_can_connect(from) {
            debug!(
                "memberlist: Blocked alive message: {err} {}",
                log_address(Some(from))
            );
            return;
        }
        let mut live: Alive = match codec::decode(buf) {
            Ok(live) => live,
            Err(err) => {
                error!(
                    "memberlist: Failed to decode alive message: {err} {}",
                    log_address(Some(from))
                );
                return;
            }
        };
        if self.config.ip_must_be_checked()
            && let Some(inner_ip) = ip_from_bytes(&live.addr)
            && let Err(err) = self.config.ip_allowed(inner_ip)
        {
            debug!(
                "memberlist: Blocked alive.Addr={} message from: {err} {}",
                ip_bytes_string(&live.addr),
                log_address(Some(from))
            );
            return;
        }

        // For proto versions < 2, there is no port provided. Mask old
        // behavior by using the configured port
        if self.protocol_version() < 2 || live.port == 0 {
            live.port = self.config.bind_port;
        }

        self.alive_node(&live, None, false);
    }

    fn handle_dead(&self, buf: &[u8], from: SocketAddr) {
        let d: Dead = match codec::decode(buf) {
            Ok(d) => d,
            Err(err) => {
                error!(
                    "memberlist: Failed to decode dead message: {err} {}",
                    log_address(Some(from))
                );
                return;
            }
        };
        self.dead_node(&d);
    }

    /// Notifies the delegate of incoming user data.
    fn handle_user(&self, buf: &[u8], _from: SocketAddr) {
        if let Some(d) = &self.config.delegate {
            d.notify_msg(buf);
        }
    }

    /// Unpacks a compressed message.
    async fn handle_compressed(&self, buf: Bytes, from: SocketAddr, timestamp: Instant) {
        // Try to decode the payload
        let payload = match decompress_payload(&buf) {
            Ok(payload) => payload,
            Err(err) => {
                error!(
                    "memberlist: Failed to decompress payload: {err} {}",
                    log_address(Some(from))
                );
                return;
            }
        };

        // Recursively handle the payload
        self.handle_command(payload.into(), from, timestamp).await;
    }

    /// Combines the encoding and sending steps.
    pub(crate) async fn encode_and_send_msg<T: Message>(
        &self,
        a: &Address,
        msg_type: MessageType,
        msg: &T,
    ) -> Result<()> {
        let out = encode(msg_type, msg);
        self.send_msg(a, &out).await
    }

    /// Sends a message via packet to another host. It will opportunistically
    /// create a compound message and piggy back other broadcasts.
    pub(crate) async fn send_msg(&self, a: &Address, msg: &[u8]) -> Result<()> {
        // Check if we can piggy back any messages
        let bytes_avail = self.config.udp_buffer_size.saturating_sub(
            msg.len()
                + crate::messages::COMPOUND_HEADER_OVERHEAD
                + crate::label::label_overhead(&self.config.label),
        );
        let extra = self.get_broadcasts(crate::messages::COMPOUND_OVERHEAD, bytes_avail);

        // Fast path if nothing to piggypack
        if extra.is_empty() {
            return self.raw_send_msg_packet(a, None, msg).await;
        }

        // Join all the messages
        let mut msgs: Vec<&[u8]> = Vec::with_capacity(1 + extra.len());
        msgs.push(msg);
        msgs.extend(extra.iter().map(|m| &m[..]));

        // Create a compound message
        let compound = make_compound_message(&msgs);

        // Send the message
        self.raw_send_msg_packet(a, None, &compound).await
    }

    /// Sends a message via packet to another host without modification,
    /// other than compression if enabled.
    pub(crate) async fn raw_send_msg_packet(
        &self,
        a: &Address,
        node: Option<&Node>,
        msg: &[u8],
    ) -> Result<()> {
        if a.name.is_empty() && self.config.require_node_names {
            return Err(Error::NodeNamesAreRequired);
        }

        let mut msg = Cow::Borrowed(msg);

        // Check if we have compression enabled
        if self.config.enable_compression {
            match compress_payload(&msg) {
                Err(err) => warn!("memberlist: Failed to compress payload: {err}"),
                Ok(buf) => {
                    // Only use compression if it reduced the size
                    if buf.len() < msg.len() {
                        msg = Cow::Owned(buf);
                    }
                }
            }
        }

        // Try to look up the destination node. Note this will only work if
        // the bare ip address is used as the node name, which is not
        // guaranteed.
        let pmax = match node {
            Some(node) => Some(node.pmax),
            None => {
                let (to_addr, _) = split_host_port(&a.addr).inspect_err(|err| {
                    error!("memberlist: Failed to parse address {:?}: {err}", a.addr)
                })?;
                self.nodes.read().get(&to_addr).map(|state| state.node.pmax)
            }
        };

        // Add a CRC to the end of the payload if the recipient understands
        // ProtocolVersion >= 5
        if pmax.is_some_and(|pmax| pmax >= 5) {
            let crc = crc32fast::hash(&msg);
            let mut with_crc = Vec::with_capacity(5 + msg.len());
            with_crc.push(MessageType::HAS_CRC.0);
            with_crc.extend_from_slice(&crc.to_be_bytes());
            with_crc.extend_from_slice(&msg);
            msg = Cow::Owned(with_crc);
        }

        metrics::counter!("memberlist.udp.sent", self.config.metric_labels.clone())
            .increment(msg.len() as u64);
        self.transport.write_to_address(&msg, a).await?;
        Ok(())
    }

    /// Streams a message to another host without modification, other than
    /// applying compression if enabled.
    pub(crate) async fn raw_send_msg_stream(&self, conn: &mut Conn, send_buf: &[u8]) -> Result<()> {
        let mut send_buf = Cow::Borrowed(send_buf);

        // Check if compression is enabled
        if self.config.enable_compression {
            match compress_payload(&send_buf) {
                Err(err) => error!("memberlist: Failed to compress payload: {err}"),
                Ok(comp_buf) => send_buf = Cow::Owned(comp_buf),
            }
        }

        // Write out the entire send buffer
        metrics::counter!("memberlist.tcp.sent", self.config.metric_labels.clone())
            .increment(send_buf.len() as u64);

        conn.write_all(&send_buf).await?;
        Ok(())
    }

    /// Streams a user message to another host.
    pub(crate) async fn send_user_msg(&self, a: &Address, send_buf: &[u8]) -> Result<()> {
        if a.name.is_empty() && self.config.require_node_names {
            return Err(Error::NodeNamesAreRequired);
        }

        let stream = self
            .transport
            .dial_address_timeout(a, self.config.tcp_timeout)
            .await?;
        let mut conn = Conn::new(stream);

        let mut buf = Vec::with_capacity(send_buf.len() + 16);
        buf.push(MessageType::USER.0);

        let header = UserMsgHeader {
            user_msg_len: send_buf.len() as i64,
        };
        header.encode_to(&mut buf);
        buf.extend_from_slice(send_buf);

        self.raw_send_msg_stream(&mut conn, &buf).await
    }

    /// Initiates a push/pull over a stream with a remote host.
    pub(crate) async fn send_and_receive_state(
        &self,
        a: &Address,
        join: bool,
    ) -> Result<(Vec<PushNodeState>, Vec<u8>)> {
        if a.name.is_empty() && self.config.require_node_names {
            return Err(Error::NodeNamesAreRequired);
        }

        // Attempt to connect
        let stream = self
            .transport
            .dial_address_timeout(a, self.config.tcp_timeout)
            .await?;
        let mut conn = Conn::new(stream);
        debug!(
            "memberlist: Initiating push/pull sync with: {} {}",
            a.name,
            conn.remote_string()
        );
        metrics::counter!("memberlist.tcp.connect", self.config.metric_labels.clone()).increment(1);

        // Send our state
        self.send_local_state(&mut conn, join).await?;

        conn.set_deadline(Instant::now() + self.config.tcp_timeout);
        let remote = conn.log();
        let (msg_type, mut reader) = self.read_stream(&mut conn).await?;

        if msg_type == MessageType::ERR {
            let resp: ErrResp = reader.decode().await?;
            return Err(Error::Remote(resp.error));
        }

        // Quit if not push/pull
        if msg_type != MessageType::PUSH_PULL {
            return Err(Error::msg(format!(
                "received invalid msgType ({}), expected pushPullMsg ({}) {}",
                msg_type,
                MessageType::PUSH_PULL,
                remote
            )));
        }

        // Read remote state
        let (_, remote_nodes, user_state) = self.read_remote_state(&mut reader).await?;
        Ok((remote_nodes, user_state))
    }

    /// Invoked to send our local state over a stream connection.
    pub(crate) async fn send_local_state(&self, conn: &mut Conn, join: bool) -> Result<()> {
        // Setup a deadline
        conn.set_deadline(Instant::now() + self.config.tcp_timeout);

        // Prepare the local node state
        let local_nodes: Vec<PushNodeState> = {
            let nodes = self.nodes.read();
            nodes
                .nodes
                .iter()
                .map(|n| PushNodeState {
                    name: n.node.name.clone(),
                    addr: n.node.wire_addr(),
                    port: n.node.port,
                    incarnation: n.incarnation,
                    state: n.state as i64,
                    meta: n.node.meta.clone(),
                    vsn: vec![
                        n.node.pmin,
                        n.node.pmax,
                        n.node.pcur,
                        n.node.dmin,
                        n.node.dmax,
                        n.node.dcur,
                    ],
                })
                .collect()
        };

        for state in [
            NodeStateType::Alive,
            NodeStateType::Left,
            NodeStateType::Dead,
            NodeStateType::Suspect,
        ] {
            let cnt = local_nodes
                .iter()
                .filter(|n| n.state == state as i64)
                .count();
            let mut labels = self.config.metric_labels.clone();
            labels.push(Label::new("node_state", state.metrics_string()));
            metrics::gauge!("memberlist.node.instances", labels).set(cnt as f64);
        }

        // Get the delegate state
        let user_data = match &self.config.delegate {
            Some(d) => d.local_state(join),
            None => Vec::new(),
        };

        // Create a bytes buffer writer
        let mut buf = Vec::with_capacity(64 + 64 * local_nodes.len() + user_data.len());

        // Send our node state
        let header = PushPullHeader {
            nodes: local_nodes.len() as i64,
            user_state_len: user_data.len() as i64,
            join,
        };

        // Begin state push
        buf.push(MessageType::PUSH_PULL.0);

        header.encode_to(&mut buf);
        for node in &local_nodes {
            node.encode_to(&mut buf);
        }

        // Write the user state as well
        buf.extend_from_slice(&user_data);

        metrics::gauge!("memberlist.size.local", self.config.metric_labels.clone())
            .set(buf.len() as f64);

        // Get the send buffer
        self.raw_send_msg_stream(conn, &buf).await
    }

    /// Reads the start of a message from a stream connection, decompressing
    /// the stream if necessary. Returns the message type and a reader for
    /// the rest of the message.
    pub(crate) async fn read_stream<'a>(
        &self,
        conn: &'a mut Conn,
    ) -> Result<(MessageType, StreamReader<'a>)> {
        // Read the message type
        let mut msg_type = MessageType(conn.read_u8().await?);

        // Check if the message is encrypted
        if msg_type == MessageType::ENCRYPT {
            return Err(Error::msg(
                "remote state is encrypted and encryption is not configured",
            ));
        }

        let mut reader = StreamReader::Conn(conn);

        // Check if we have a compressed message
        if msg_type == MessageType::COMPRESS {
            let c: Compress = reader.decode().await?;
            let mut decomp = decompress_buffer(&c)?;
            if decomp.is_empty() {
                return Err(Error::msg("decompressed message is empty"));
            }

            // Reset the message type
            msg_type = MessageType(decomp.remove(0));

            // Create a new reader
            reader = StreamReader::mem(decomp);
        }

        Ok((msg_type, reader))
    }

    /// Reads the remote state from a connection.
    pub(crate) async fn read_remote_state(
        &self,
        reader: &mut StreamReader<'_>,
    ) -> Result<(bool, Vec<PushNodeState>, Vec<u8>)> {
        // Read the push/pull header
        let header: PushPullHeader = reader.decode().await?;

        if header.nodes < 0 || header.nodes > MAX_PUSH_STATE_NODES as i64 {
            return Err(Error::msg(format!(
                "number of nodes in header ({}) exceeds limit",
                header.nodes
            )));
        }

        // Try to decode all the states
        let mut remote_nodes = Vec::with_capacity((header.nodes as usize).min(1024));
        for _ in 0..header.nodes {
            remote_nodes.push(reader.decode::<PushNodeState>().await?);
        }

        if header.user_state_len < 0 || header.user_state_len > MAX_PUSH_STATE_BYTES as i64 {
            return Err(Error::msg(format!(
                "user state length ({}) exceeds limit",
                header.user_state_len
            )));
        }

        // Read the remote user state into a buffer
        let user_buf = if header.user_state_len > 0 {
            reader.read_exact(header.user_state_len as usize).await?
        } else {
            Vec::new()
        };

        // For proto versions < 2, there is no port provided. Mask old
        // behavior by using the configured port
        for node in &mut remote_nodes {
            if self.protocol_version() < 2 || node.port == 0 {
                node.port = self.config.bind_port;
            }
        }

        Ok((header.join, remote_nodes, user_buf))
    }

    /// Merges the remote state with our local state.
    pub(crate) fn merge_remote_state(
        &self,
        join: bool,
        remote_nodes: &[PushNodeState],
        user_buf: &[u8],
    ) -> Result<()> {
        self.verify_protocol(remote_nodes)?;

        // Invoke the merge delegate if any
        if join && let Some(merge) = &self.config.merge {
            let mut nodes = Vec::with_capacity(remote_nodes.len());
            for n in remote_nodes {
                let Some(mut node) = Node::from_wire(&n.name, &n.addr, n.port) else {
                    return Err(Error::msg(format!(
                        "Node '{}' has an address that is not valid: {}",
                        n.name,
                        ip_bytes_string(&n.addr)
                    )));
                };
                node.meta = n.meta.clone();
                node.state = n.node_state().unwrap_or_default();
                if n.vsn.len() >= 6 {
                    node.pmin = n.vsn[0];
                    node.pmax = n.vsn[1];
                    node.pcur = n.vsn[2];
                    node.dmin = n.vsn[3];
                    node.dmax = n.vsn[4];
                    node.dcur = n.vsn[5];
                }
                nodes.push(node);
            }
            merge
                .notify_merge(&nodes)
                .map_err(|err| Error::msg(err.to_string()))?;
        }

        // Merge the membership state
        self.merge_state(remote_nodes);

        // Invoke the delegate for user state
        if !user_buf.is_empty()
            && let Some(d) = &self.config.delegate
        {
            d.merge_remote_state(user_buf, join);
        }
        Ok(())
    }

    /// Decodes a user message from a stream.
    pub(crate) async fn read_user_msg(&self, reader: &mut StreamReader<'_>) -> Result<()> {
        // Read the user message header
        let header: UserMsgHeader = reader.decode().await?;

        if header.user_msg_len < 0 || header.user_msg_len > MAX_USER_MSG_BYTES as i64 {
            return Err(Error::msg(format!(
                "user message length ({}) exceeds limit",
                header.user_msg_len
            )));
        }

        // Read the user message into a buffer
        if header.user_msg_len > 0 {
            let user_buf = reader.read_exact(header.user_msg_len as usize).await?;

            if let Some(d) = &self.config.delegate {
                d.notify_msg(&user_buf);
            }
        }

        Ok(())
    }

    /// Makes a stream connection to the given address, sends a ping, and
    /// waits for an ack. All of this is done as a series of blocking
    /// operations, given the deadline. The bool return parameter is true if
    /// we were able to round trip a ping to the other node.
    pub(crate) async fn send_ping_and_wait_for_ack(
        &self,
        a: &Address,
        ping: &Ping,
        deadline: Instant,
    ) -> Result<bool> {
        if a.name.is_empty() && self.config.require_node_names {
            return Err(Error::NodeNamesAreRequired);
        }

        // If the node is actually dead we expect the dial to fail, so we
        // shouldn't spam the logs with it. After this point, errors with the
        // connection are real, unexpected errors and should get propagated
        // up.
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Ok(false);
        }
        let Ok(stream) = self.transport.dial_address_timeout(a, timeout).await else {
            return Ok(false);
        };
        let mut conn = Conn::new(stream);
        conn.set_deadline(deadline);

        let out = encode(MessageType::PING, ping);

        self.raw_send_msg_stream(&mut conn, &out).await?;

        let remote = conn.log();
        let (msg_type, mut reader) = self.read_stream(&mut conn).await?;

        if msg_type != MessageType::ACK_RESP {
            return Err(Error::msg(format!(
                "unexpected msgType ({msg_type}) from ping {remote}"
            )));
        }

        let ack: AckResp = reader.decode().await?;

        if ack.seq_no != ping.seq_no {
            return Err(Error::msg(format!(
                "sequence number from ack ({}) doesn't match ping ({})",
                ack.seq_no, ping.seq_no
            )));
        }

        Ok(true)
    }
}
