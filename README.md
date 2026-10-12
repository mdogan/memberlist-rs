# memberlist-rs

A Rust port of [hashicorp/memberlist](https://github.com/hashicorp/memberlist), a library that manages cluster membership 
and member failure detection using a gossip based protocol.

## Compatibility with the Go library

The port follows hashicorp/memberlist v0.7.0. It uses the same wire protocol, so Rust members and Go members can be in the same cluster. 
Encryption is not supported. A Rust member cannot join a cluster that uses encryption.

## Usage

```rust
use memberlist::{Config, Memberlist};

#[tokio::main]
async fn main() -> Result<(), memberlist::Error> {
    // Create the initial memberlist from a safe configuration.
    let list = Memberlist::create(Config::default_local()).await?;

    // Join an existing cluster by specifying at least one known member.
    let n = list.join(&["1.2.3.4"]).await?;
    println!("joined {n} members");

    // Ask for members of the cluster
    for member in list.members() {
        println!("Member: {} {}", member.name, member.addr);
    }

    Ok(())
}
```

## Differences from the Go library

* **No encryption.** The keyring, `SecretKey` and the `GossipVerify*` options are not ported.
* **No deprecated API.** `SendTo`, `SendToUDP`, `SendToTCP` and `IngestionAwareTransport` are not ported.
* **Delegates are sync traits.** Their methods must not block, like in Go. `ChannelEventDelegate` sends to an unbounded tokio channel.
* **One `Transport` trait.** `write_to_address` and `dial_address_timeout` are methods with a default, so there is no separate `NodeAwareTransport`.
  Incoming packets and streams come from `recv_packet` and `accept_stream` instead of channels.
* **Logging** uses the [`tracing`](https://docs.rs/tracing) crate, so there is no `LogOutput` or `Logger` option. 
* **Metrics** use the [`metrics`](https://docs.rs/metrics) crate. The names are the Go keys joined with a dot, for example `memberlist.udp.sent`.
* **`ping` returns the ack payload** together with the round trip time.
* **Advertise address.** When the bind address is `0.0.0.0` and no advertise address is set, a private address of the host is used, IPv4 first. 
  Unlike Go, the interface with the default route is not preferred. Set `advertise_addr` on a host with several private addresses.
* `MsgpackUseNewTimeFormat` is not ported. No message has a time value, so the option has no effect on the wire.

## Building and testing

```
make build          # debug build
make release        # release build
make test           # all tests
make test-unit      # unit tests only
make test-interop   # Go interop tests only
make lint           # clippy, warnings are errors
make fmt            # format the code
make check          # fmt, lint and test
```

The targets are thin wrappers around `cargo`, so `cargo build` and `cargo test` work too. Run `make help` to list all targets.

The interop tests start real Go memberlist nodes, so they need the Go toolchain. They are skipped when the `go` command is not found.
The first run downloads `hashicorp/memberlist`, so it needs network access.

All tests bind to `127.0.0.1` only.

## Protocol

memberlist is based on ["SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol"](http://ieeexplore.ieee.org/document/1028914/),
with a number of extensions to increase propagation speed and convergence rate, and the Lifeguard extensions that make it more robust in the presence
of slow message processing. For details, read the paper "[Lifeguard : SWIM-ing with Situational Awareness](https://arxiv.org/abs/1707.00788)".

## License

This is a derived work of [hashicorp/memberlist](https://github.com/hashicorp/memberlist)
(Copyright IBM Corp.) and has the same license, the Mozilla Public License 2.0. See [LICENSE](LICENSE).
