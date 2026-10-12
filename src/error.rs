// Copyright (c) 2026 Mehmet Dogan
// SPDX-License-Identifier: MPL-2.0

use std::fmt;
use std::io;

use crate::codec::DecodeError;

/// A specialized `Result` type for memberlist operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The error type that a delegate returns to reject a node or a merge.
pub type DelegateError = Box<dyn std::error::Error + Send + Sync>;

/// The errors that memberlist operations can return.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The configuration requires node names, but a message was addressed
    /// without one.
    #[error("memberlist: node names are required by configuration but one was not provided")]
    NodeNamesAreRequired,

    /// A ping packet was sent, but no response was received.
    #[error("No response from node {0}")]
    NoPingResponse(String),

    /// A network operation failed. `op` is the operation ("dial", "read" or
    /// "write") and `net` is the kind of network ("tcp" or "udp").
    #[error("{op} {net}: {source}")]
    Op {
        op: &'static str,
        net: &'static str,
        #[source]
        source: io::Error,
    },

    /// An I/O error that is not tied to a network operation.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// The peer closed the stream before it sent anything.
    #[error("EOF")]
    Eof,

    /// The remote side reported an error.
    #[error("remote error: {0}")]
    Remote(String),

    /// The memberlist has been shut down.
    #[error("memberlist has been shut down")]
    Shutdown,

    /// Several things failed, for example every address given to `join`.
    #[error("{}", format_multiple(.0))]
    Multiple(Vec<Error>),

    /// Any other error, described by its message.
    #[error("{0}")]
    Other(String),
}

impl Error {
    pub(crate) fn msg(msg: impl Into<String>) -> Self {
        Error::Other(msg.into())
    }

    /// Returns true if the error is an I/O timeout.
    pub fn is_timeout(&self) -> bool {
        match self {
            Error::Op { source, .. } | Error::Io(source) => {
                source.kind() == io::ErrorKind::TimedOut
            }
            _ => false,
        }
    }
}

impl From<DecodeError> for Error {
    fn from(err: DecodeError) -> Self {
        match err {
            DecodeError::Incomplete(_) => Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
            DecodeError::Invalid(_) => Error::Other(err.to_string()),
        }
    }
}

fn format_multiple(errors: &[Error]) -> String {
    use fmt::Write;

    let mut out = String::new();
    if errors.len() == 1 {
        let _ = write!(out, "1 error occurred:\n\t* {}\n\n", errors[0]);
        return out;
    }
    let _ = writeln!(out, "{} errors occurred:", errors.len());
    for err in errors {
        let _ = writeln!(out, "\t* {err}");
    }
    out.push('\n');
    out
}

/// Returns an I/O timeout error, like the one Go returns when a deadline
/// passes.
pub(crate) fn timeout_error() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "i/o timeout")
}
