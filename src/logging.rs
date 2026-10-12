// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0
//
// This file is a Rust port of logging.go from https://github.com/hashicorp/memberlist.

use std::net::SocketAddr;

use crate::transport::Connection;

/// Formats the address of a peer for a log line.
pub fn log_address(addr: Option<SocketAddr>) -> String {
    match addr {
        None => "from=<unknown address>".to_string(),
        Some(addr) => format!("from={addr}"),
    }
}

/// Formats the address of a peer for a log line, when the address is only
/// known as a string.
pub fn log_string_address(addr: &str) -> String {
    if addr.is_empty() {
        return "from=<unknown address>".to_string();
    }

    format!("from={addr}")
}

/// Formats the remote address of a stream connection for a log line.
pub fn log_conn(conn: Option<&dyn Connection>) -> String {
    match conn {
        None => log_address(None),
        Some(conn) => log_address(conn.remote_addr()),
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    #[test]
    fn test_log_address() {
        assert_eq!(log_address(None), "from=<unknown address>", "nil address");

        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        assert_eq!(
            log_address(Some(addr)),
            "from=127.0.0.1:9999",
            "valid address"
        );
    }

    #[test]
    fn test_log_string_address() {
        assert_eq!(
            log_string_address(""),
            "from=<unknown address>",
            "empty string address"
        );
        assert_eq!(
            log_string_address("127.0.0.1"),
            "from=127.0.0.1",
            "valid address"
        );
    }

    #[tokio::test]
    async fn test_log_conn() {
        assert_eq!(log_conn(None), "from=<unknown address>", "nil connection");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let conn = TcpStream::connect(addr).await.unwrap();
        assert_eq!(
            log_conn(Some(&conn)),
            format!("from={addr}"),
            "valid connection"
        );
    }
}
