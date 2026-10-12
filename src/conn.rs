// Copyright (c) 2026 Mehmet Dogan
// SPDX-License-Identifier: MPL-2.0

//! Helpers for reading and writing messages on a stream connection.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Instant;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::codec::{DecodeError, Decoder, Message};
use crate::error::{Error, Result, timeout_error};
use crate::logging::log_address;
use crate::messages::MAX_DECOMPRESSED_BYTES;
use crate::transport::{Connection, Stream};

/// The most bytes that we buffer while we wait for the rest of a single
/// value. A peer that announces a larger value is rejected, so that it
/// cannot make us allocate memory without limit.
const MAX_STREAM_BUFFER: usize = 2 * MAX_DECOMPRESSED_BYTES;

/// A stream connection with a deadline and a read buffer.
///
/// The deadline works like `SetDeadline` on a Go connection: once it has
/// passed, every read and write fails with a timeout error.
pub(crate) struct Conn {
    stream: Stream,
    rbuf: BytesMut,
    deadline: Option<tokio::time::Instant>,
    remote: Option<SocketAddr>,
}

async fn with_deadline<T>(
    deadline: Option<tokio::time::Instant>,
    fut: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    match deadline {
        None => fut.await,
        Some(deadline) => match tokio::time::timeout_at(deadline, fut).await {
            Ok(res) => res,
            Err(_) => Err(timeout_error()),
        },
    }
}

impl Conn {
    pub(crate) fn new(stream: Stream) -> Self {
        let remote = stream.remote_addr();
        Conn {
            stream,
            rbuf: BytesMut::new(),
            deadline: None,
            remote,
        }
    }

    /// Sets the time after which reads and writes fail.
    pub(crate) fn set_deadline(&mut self, deadline: Instant) {
        self.deadline = Some(tokio::time::Instant::from_std(deadline));
    }

    /// Formats the remote address for a log line.
    pub(crate) fn log(&self) -> String {
        log_address(self.remote)
    }

    /// The remote address as a string, or an empty string if it is unknown.
    pub(crate) fn remote_string(&self) -> String {
        self.remote.map(|a| a.to_string()).unwrap_or_default()
    }

    /// Writes the whole buffer to the stream.
    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let stream = &mut self.stream;
        with_deadline(self.deadline, async move {
            stream.write_all(buf).await?;
            stream.flush().await
        })
        .await
    }

    /// Reads some more bytes from the stream into the read buffer. Returns
    /// the number of bytes that were read, which is zero at the end of the
    /// stream.
    async fn read_more(&mut self) -> io::Result<usize> {
        with_deadline(self.deadline, self.stream.read_buf(&mut self.rbuf)).await
    }

    /// Reads one byte. Returns `Error::Eof` if the stream ended before it.
    pub(crate) async fn read_u8(&mut self) -> Result<u8> {
        if self.rbuf.is_empty() && self.read_more().await? == 0 {
            return Err(Error::Eof);
        }
        Ok(self.rbuf.get_u8())
    }
}

/// Reads the body of a message. The body comes either straight from the
/// connection, or from memory when the message was compressed.
pub(crate) enum StreamReader<'a> {
    Conn(&'a mut Conn),
    Mem { buf: Vec<u8>, pos: usize },
}

impl StreamReader<'_> {
    /// Returns a reader over bytes that are already in memory.
    pub(crate) fn mem(buf: Vec<u8>) -> StreamReader<'static> {
        StreamReader::Mem { buf, pos: 0 }
    }

    fn buffered(&self) -> &[u8] {
        match self {
            StreamReader::Conn(conn) => &conn.rbuf,
            StreamReader::Mem { buf, pos } => &buf[*pos..],
        }
    }

    fn consume(&mut self, n: usize) {
        match self {
            StreamReader::Conn(conn) => conn.rbuf.advance(n),
            StreamReader::Mem { pos, .. } => *pos += n,
        }
    }

    /// Makes at least `need` more bytes available.
    async fn fill(&mut self, need: usize) -> Result<()> {
        let unexpected_eof = || {
            Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ))
        };
        match self {
            StreamReader::Mem { .. } => Err(unexpected_eof()),
            StreamReader::Conn(conn) => {
                let target = conn.rbuf.len().saturating_add(need);
                if target > MAX_STREAM_BUFFER {
                    return Err(Error::msg(format!(
                        "stream message is larger than limit ({MAX_STREAM_BUFFER})"
                    )));
                }
                while conn.rbuf.len() < target {
                    if conn.read_more().await? == 0 {
                        return Err(unexpected_eof());
                    }
                }
                Ok(())
            }
        }
    }

    /// Decodes the next msgpack message.
    pub(crate) async fn decode<T: Message>(&mut self) -> Result<T> {
        loop {
            let mut d = Decoder::new(self.buffered());
            match T::decode_from(&mut d) {
                Ok(v) => {
                    let n = d.position();
                    self.consume(n);
                    return Ok(v);
                }
                Err(DecodeError::Incomplete(need)) => self.fill(need).await?,
                Err(err) => return Err(err.into()),
            }
        }
    }

    /// Reads exactly `n` raw bytes.
    pub(crate) async fn read_exact(&mut self, n: usize) -> Result<Vec<u8>> {
        let have = self.buffered().len();
        if have < n {
            self.fill(n - have).await?;
        }
        let out = self.buffered()[..n].to_vec();
        self.consume(n);
        Ok(out)
    }
}
