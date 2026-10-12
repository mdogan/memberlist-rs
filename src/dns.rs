// Copyright (c) 2026 Mehmet Dogan
// SPDX-License-Identifier: MPL-2.0

//! A minimal DNS client for the TCP-first lookup that `join` does.

use std::net::IpAddr;
use std::time::Duration;

use rand::RngExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::error::{Error, Result, timeout_error};
use crate::util::{has_port, join_host_port};

const TYPE_A: u16 = 1;
const TYPE_CNAME: u16 = 5;
const TYPE_AAAA: u16 = 28;
const TYPE_ANY: u16 = 255;
const CLASS_INET: u16 = 1;

/// The time limit for each step of a lookup (connect, write and read).
const DNS_TIMEOUT: Duration = Duration::from_secs(2);

/// Returns the name servers that are listed in a resolv.conf file.
fn name_servers(conf: &str) -> Vec<String> {
    conf.lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#') && !line.starts_with(';'))
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            match (fields.next(), fields.next()) {
                (Some("nameserver"), Some(server)) => Some(server.to_string()),
                _ => None,
            }
        })
        .collect()
}

fn build_query(id: u16, name: &str) -> Result<Vec<u8>> {
    let mut msg = Vec::with_capacity(18 + name.len());
    msg.extend_from_slice(&id.to_be_bytes());
    msg.extend_from_slice(&0x0100u16.to_be_bytes()); // recursion desired
    msg.extend_from_slice(&1u16.to_be_bytes()); // one question
    msg.extend_from_slice(&[0; 6]); // no answer, authority or additional records
    for label in name.split('.').filter(|label| !label.is_empty()) {
        if label.len() > 63 {
            return Err(Error::msg(format!("dns: label {label:?} is too long")));
        }
        msg.push(label.len() as u8);
        msg.extend_from_slice(label.as_bytes());
    }
    msg.push(0);
    msg.extend_from_slice(&TYPE_ANY.to_be_bytes());
    msg.extend_from_slice(&CLASS_INET.to_be_bytes());
    Ok(msg)
}

fn malformed() -> Error {
    Error::msg("dns: malformed message")
}

fn read_u16(buf: &[u8], pos: usize) -> Result<u16> {
    let b = buf.get(pos..pos + 2).ok_or_else(malformed)?;
    Ok(u16::from_be_bytes([b[0], b[1]]))
}

/// Returns the position right after the domain name that starts at `pos`.
fn skip_name(buf: &[u8], mut pos: usize) -> Result<usize> {
    loop {
        let len = *buf.get(pos).ok_or_else(malformed)? as usize;
        if len == 0 {
            return Ok(pos + 1);
        }
        if len & 0xc0 == 0xc0 {
            // A pointer to an earlier name ends this name.
            return Ok(pos + 2);
        }
        pos += 1 + len;
    }
}

/// The records of a DNS answer that we care about.
#[derive(Debug, Default, PartialEq, Eq)]
struct Answer {
    ips: Vec<IpAddr>,
    cnames: usize,
}

fn parse_response(id: u16, buf: &[u8]) -> Result<Answer> {
    if buf.len() < 12 {
        return Err(malformed());
    }
    if read_u16(buf, 0)? != id {
        return Err(Error::msg("dns: id mismatch"));
    }
    let questions = read_u16(buf, 4)?;
    let answers = read_u16(buf, 6)?;

    let mut pos = 12;
    for _ in 0..questions {
        pos = skip_name(buf, pos)? + 4;
    }

    let mut out = Answer::default();
    for _ in 0..answers {
        pos = skip_name(buf, pos)?;
        let rtype = read_u16(buf, pos)?;
        let rdlen = read_u16(buf, pos + 8)? as usize;
        pos += 10;
        let rdata = buf.get(pos..pos + rdlen).ok_or_else(malformed)?;
        pos += rdlen;
        match rtype {
            TYPE_A => {
                let ip: [u8; 4] = rdata.try_into().map_err(|_| malformed())?;
                out.ips.push(IpAddr::from(ip));
            }
            TYPE_AAAA => {
                let ip: [u8; 16] = rdata.try_into().map_err(|_| malformed())?;
                out.ips.push(IpAddr::from(ip));
            }
            TYPE_CNAME => out.cnames += 1,
            _ => {}
        }
    }
    Ok(out)
}

async fn with_timeout<T>(fut: impl Future<Output = std::io::Result<T>>) -> Result<T> {
    match tokio::time::timeout(DNS_TIMEOUT, fut).await {
        Ok(res) => Ok(res?),
        Err(_) => Err(timeout_error().into()),
    }
}

/// A helper to initiate a TCP-based DNS lookup for the given host.
///
/// The system resolver will do a UDP lookup first, and will only use TCP if
/// the response has the truncate bit set, which isn't common on DNS servers
/// like Consul's. By doing the TCP lookup directly, we get the best chance
/// for the largest list of hosts to join. Since joins are relatively rare
/// events, it's ok to do this rather expensive operation.
pub(crate) async fn tcp_lookup_ip(dns_config_path: &str, host: &str) -> Result<Vec<IpAddr>> {
    // Don't attempt any TCP lookups against non-fully qualified domain
    // names, since those will likely come from the resolv.conf file.
    if !host.contains('.') {
        return Ok(Vec::new());
    }

    // See if we can find a server to try.
    let conf = std::fs::read_to_string(dns_config_path)?;
    let Some(server) = name_servers(&conf).into_iter().next() else {
        return Ok(Vec::new());
    };

    // We support host:port in the DNS config, but need to add the default
    // port if one is not supplied.
    let server = if has_port(&server) {
        server
    } else {
        join_host_port(&server, 53)
    };

    // Do the lookup.
    let id: u16 = rand::rng().random();
    let query = build_query(id, host)?;
    let mut conn = with_timeout(TcpStream::connect(&server)).await?;
    let mut framed = Vec::with_capacity(query.len() + 2);
    framed.extend_from_slice(&(query.len() as u16).to_be_bytes());
    framed.extend_from_slice(&query);
    with_timeout(conn.write_all(&framed)).await?;

    let mut len = [0u8; 2];
    with_timeout(conn.read_exact(&mut len)).await?;
    let mut resp = vec![0u8; u16::from_be_bytes(len) as usize];
    with_timeout(conn.read_exact(&mut resp)).await?;

    // Handle any IPs we get back that we can attempt to join.
    let answer = parse_response(id, &resp)?;
    if answer.cnames > 0 {
        debug!("memberlist: Ignoring CNAME RR in TCP-first answer for '{host}'");
    }
    Ok(answer.ips)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name_servers() {
        let conf = "# comment\n; other comment\nsearch example.com\nnameserver 10.0.0.1\n  nameserver   [::1]:5353  \noptions ndots:2\n";
        assert_eq!(name_servers(conf), vec!["10.0.0.1", "[::1]:5353"]);
        assert!(name_servers("").is_empty());
    }

    #[test]
    fn test_query_and_response() {
        let query = build_query(0x1234, "join.service.consul.").unwrap();
        assert_eq!(&query[..2], &[0x12, 0x34]);
        assert_eq!(&query[12..17], b"\x04join");
        assert_eq!(&query[query.len() - 4..], &[0, 255, 0, 1]);

        // A response with the question, an A record that uses a name
        // pointer, a CNAME record and an AAAA record.
        let mut resp = query.clone();
        resp[2] = 0x81;
        resp[7] = 3; // three answers
        for (rtype, rdata) in [
            (TYPE_A, vec![10, 0, 0, 7]),
            (TYPE_CNAME, vec![1, b'x', 0]),
            (
                TYPE_AAAA,
                "fd00::7"
                    .parse::<std::net::Ipv6Addr>()
                    .unwrap()
                    .octets()
                    .to_vec(),
            ),
        ] {
            resp.extend_from_slice(&[0xc0, 12]); // pointer to the question name
            resp.extend_from_slice(&rtype.to_be_bytes());
            resp.extend_from_slice(&CLASS_INET.to_be_bytes());
            resp.extend_from_slice(&60u32.to_be_bytes());
            resp.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
            resp.extend_from_slice(&rdata);
        }

        let answer = parse_response(0x1234, &resp).unwrap();
        assert_eq!(
            answer.ips,
            vec![
                "10.0.0.7".parse::<IpAddr>().unwrap(),
                "fd00::7".parse::<IpAddr>().unwrap()
            ]
        );
        assert_eq!(answer.cnames, 1);

        assert!(parse_response(0x4321, &resp).is_err(), "id mismatch");
        assert!(
            parse_response(0x1234, &resp[..resp.len() - 3]).is_err(),
            "truncated"
        );
    }
}
