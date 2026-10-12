// Copyright (c) 2026 Mehmet Dogan
// SPDX-License-Identifier: MPL-2.0

//! The few address helpers that the Go code takes from `go-sockaddr`.

use std::net::IpAddr;
use std::sync::LazyLock;

use ipnet::IpNet;

fn parse_nets(nets: &[&str]) -> Vec<IpNet> {
    nets.iter()
        .map(|n| n.parse().expect("valid network literal"))
        .collect()
}

/// The special-purpose address blocks of RFC 6890.
static RFC_6890: LazyLock<Vec<IpNet>> = LazyLock::new(|| {
    parse_nets(&[
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.0.0.0/24",
        "192.0.0.0/29",
        "192.0.2.0/24",
        "192.88.99.0/24",
        "192.168.0.0/16",
        "198.18.0.0/15",
        "198.51.100.0/24",
        "203.0.113.0/24",
        "240.0.0.0/4",
        "255.255.255.255/32",
        "::1/128",
        "::/128",
        "64:ff9b::/96",
        "::ffff:0:0/96",
        "100::/64",
        "2001::/16",
        "2001:2::/48",
        "2001:db8::/32",
        "2001:10::/28",
        "2002::/16",
        "fc00::/7",
        "fe80::/10",
    ])
});

/// The blocks of RFC 6890 that are not forwardable.
static FORWARDING_BLACKLIST: LazyLock<Vec<IpNet>> = LazyLock::new(|| {
    parse_nets(&[
        "0.0.0.0/8",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "192.0.0.0/24",
        "192.0.2.0/24",
        "198.51.100.0/24",
        "203.0.113.0/24",
        "240.0.0.0/4",
        "255.255.255.255/32",
        "::1/128",
        "::/128",
        "::ffff:0:0/96",
        "2001:db8::/32",
        "2001:10::/28",
        "fe80::/10",
    ])
});

fn in_any(nets: &[IpNet], ip: &IpAddr) -> bool {
    nets.iter().any(|n| n.contains(ip))
}

/// Returns true if the address is in one of the special-purpose blocks of
/// RFC 6890. An address outside of them is a public address.
pub(crate) fn is_rfc6890(ip: &IpAddr) -> bool {
    in_any(&RFC_6890, &ip.to_canonical())
}

/// Returns a single IP address that is part of RFC 6890, is forwardable and
/// belongs to an interface that is up. Returns `None` if the system has no
/// such address.
pub(crate) fn get_private_ip() -> std::io::Result<Option<IpAddr>> {
    let mut candidates: Vec<(bool, u8, IpAddr)> = if_addrs::get_if_addrs()?
        .into_iter()
        .filter(|iface| iface.is_oper_up())
        .map(|iface| {
            let prefixlen = match &iface.addr {
                if_addrs::IfAddr::V4(a) => a.prefixlen,
                if_addrs::IfAddr::V6(a) => a.prefixlen,
            };
            let ip = iface.ip();
            (ip.is_ipv6(), prefixlen, ip)
        })
        .filter(|(_, _, ip)| !in_any(&FORWARDING_BLACKLIST, ip) && in_any(&RFC_6890, ip))
        .collect();

    // IPv4 before IPv6, then larger networks first.
    candidates.sort_by_key(|(v6, prefixlen, _)| (*v6, *prefixlen));
    Ok(candidates.first().map(|(_, _, ip)| *ip))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_rfc6890() {
        for ip in [
            "10.1.2.3",
            "127.0.0.1",
            "192.168.1.1",
            "172.16.5.4",
            "::1",
            "fd00::1",
        ] {
            assert!(is_rfc6890(&ip.parse().unwrap()), "{ip} should be private");
        }
        for ip in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"] {
            assert!(!is_rfc6890(&ip.parse().unwrap()), "{ip} should be public");
        }
    }
}
