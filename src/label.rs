// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of label.go from https://github.com/hashicorp/memberlist.

//! General approach is to prefix all packets and streams with the same
//! structure:
//!
//! ```text
//! magic type byte (244): uint8
//! length of label name:  uint8 (because labels can't be longer than 255 bytes)
//! label name:            []uint8
//! ```

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::error::{Error, Result};
use crate::messages::MessageType;
use crate::transport::{Connection, Stream};

/// The maximum length of a packet or stream label.
pub const LABEL_MAX_SIZE: usize = 255;

/// Prefixes outgoing packets with the correct header if the label is not
/// empty.
pub fn add_label_header_to_packet(buf: &[u8], label: &str) -> Result<Vec<u8>> {
    if label.is_empty() {
        return Ok(buf.to_vec());
    }
    if label.len() > LABEL_MAX_SIZE {
        return Err(Error::msg(format!("label {label:?} is too long")));
    }

    Ok(make_label_header(label, buf))
}

/// Removes any label header from the provided packet and returns it along
/// with the remaining packet contents.
pub fn remove_label_header_from_packet(buf: &[u8]) -> Result<(&[u8], String)> {
    if buf.is_empty() {
        return Ok((buf, String::new())); // can't possibly be labeled
    }

    // [type:byte] [size:byte] [size bytes]

    if buf[0] != MessageType::HAS_LABEL.0 {
        return Ok((buf, String::new()));
    }

    if buf.len() < 2 {
        return Err(Error::msg("cannot decode label; packet has been truncated"));
    }

    let size = buf[1] as usize;
    if size < 1 {
        return Err(Error::msg("label header cannot be empty when present"));
    }

    if buf.len() < 2 + size {
        return Err(Error::msg("cannot decode label; packet has been truncated"));
    }

    let label = String::from_utf8_lossy(&buf[2..2 + size]).into_owned();
    Ok((&buf[2 + size..], label))
}

/// Prefixes outgoing streams with the correct header if the label is not
/// empty.
pub async fn add_label_header_to_stream<W>(conn: &mut W, label: &str) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    if label.is_empty() {
        return Ok(());
    }
    if label.len() > LABEL_MAX_SIZE {
        return Err(Error::msg(format!("label {label:?} is too long")));
    }

    let header = make_label_header(label, &[]);

    conn.write_all(&header).await?;
    Ok(())
}

/// Removes any label header from the beginning of the stream if present and
/// returns it along with an updated conn with that header removed.
///
/// Note that on error the connection is closed, because it is dropped.
pub async fn remove_label_header_from_stream(mut conn: Stream) -> Result<(Stream, String)> {
    let mut buf = BytesMut::with_capacity(4096);

    // First check for the type byte.
    if !fill(&mut conn, &mut buf, 1).await? {
        // It is safe to return the original conn at this point because it
        // never contained any data in the first place so we don't have to
        // splice the buffer into the conn because both are empty.
        return Ok((conn, String::new()));
    }

    if buf[0] != MessageType::HAS_LABEL.0 {
        return Ok((PeekedConn::boxed(buf.freeze(), conn), String::new()));
    }

    // We are guaranteed to get a size byte as well.
    if !fill(&mut conn, &mut buf, 2).await? {
        return Err(Error::msg("cannot decode label; stream has been truncated"));
    }

    let size = buf[1] as usize;
    if size < 1 {
        return Err(Error::msg("label header cannot be empty when present"));
    }
    // NOTE: we don't have to check this against LABEL_MAX_SIZE because a
    // byte already has a max value of 255.

    // Once we know the size we can peek the label as well.
    if !fill(&mut conn, &mut buf, 2 + size).await? {
        return Err(Error::msg("cannot decode label; stream has been truncated"));
    }

    let label = String::from_utf8_lossy(&buf[2..2 + size]).into_owned();

    buf.advance(2 + size);
    Ok((PeekedConn::boxed(buf.freeze(), conn), label))
}

/// Reads from the conn until the buffer holds at least `n` bytes. Returns
/// false if the stream ended before that.
async fn fill(conn: &mut Stream, buf: &mut BytesMut, n: usize) -> io::Result<bool> {
    while buf.len() < n {
        if conn.read_buf(buf).await? == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// An incoming connection that has had some bytes read from it to look for
/// the label header. Reading stitches the peeked bytes and the unread bytes
/// back together.
struct PeekedConn {
    /// The bytes that have been read from the conn, but have not yet been
    /// consumed by read calls.
    peeked: Bytes,

    /// The underlying connection.
    conn: Stream,
}

impl PeekedConn {
    fn boxed(peeked: Bytes, conn: Stream) -> Stream {
        if peeked.is_empty() {
            return conn;
        }
        Box::new(PeekedConn { peeked, conn })
    }
}

impl AsyncRead for PeekedConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.peeked.is_empty() {
            let n = self.peeked.len().min(buf.remaining());
            buf.put_slice(&self.peeked.split_to(n));
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.conn).poll_read(cx, buf)
    }
}

impl AsyncWrite for PeekedConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.conn).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.conn).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.conn).poll_shutdown(cx)
    }
}

impl Connection for PeekedConn {
    fn remote_addr(&self) -> Option<SocketAddr> {
        self.conn.remote_addr()
    }
}

fn make_label_header(label: &str, rest: &[u8]) -> Vec<u8> {
    let mut new_buf = Vec::with_capacity(2 + label.len() + rest.len());
    new_buf.push(MessageType::HAS_LABEL.0);
    new_buf.push(label.len() as u8);
    new_buf.extend_from_slice(label.as_bytes());
    new_buf.extend_from_slice(rest);
    new_buf
}

pub(crate) fn label_overhead(label: &str) -> usize {
    if label.is_empty() {
        return 0;
    }
    2 + label.len()
}
