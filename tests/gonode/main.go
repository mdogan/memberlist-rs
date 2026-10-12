// Copyright (c) 2026 Mehmet Dogan
// SPDX-License-Identifier: MPL-2.0

// gonode runs one Go memberlist node that is driven by commands on stdin.
// The Rust interop tests use it to check that Rust and Go members can be in
// the same cluster. Each command gets exactly one reply line on stdout.
package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

type delegate struct {
	mu          sync.Mutex
	meta        []byte
	state       []byte
	msgs        [][]byte
	remoteState []byte
}

func (d *delegate) NodeMeta(limit int) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.meta
}

func (d *delegate) NotifyMsg(msg []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.msgs = append(d.msgs, append([]byte(nil), msg...))
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

func (d *delegate) LocalState(join bool) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.remoteState = append([]byte(nil), buf...)
}

type pingDelegate struct {
	payload []byte

	mu   sync.Mutex
	acks map[string][]byte
}

func (p *pingDelegate) AckPayload() []byte { return p.payload }

// NotifyPingComplete keeps the payload of the last ack from each node. The
// acks come from the probes that memberlist does in the background.
func (p *pingDelegate) NotifyPingComplete(other *memberlist.Node, rtt time.Duration, payload []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.acks[other.Name] = append([]byte(nil), payload...)
}

// stateBytes makes the same bytes as the Rust side of the test, so each side
// can check what it received.
func stateBytes(prefix string, n int) []byte {
	out := []byte(prefix)
	x := uint32(len(prefix))
	for len(out) < n {
		x = x*1103515245 + 12345
		out = append(out, byte(x>>16))
	}
	return out
}

func main() {
	name := flag.String("name", "go-node", "node name")
	port := flag.Int("port", 0, "bind port")
	label := flag.String("label", "", "label")
	compress := flag.Bool("compress", true, "enable compression")
	proto := flag.Int("proto", 2, "protocol version")
	stateLen := flag.Int("state-len", 0, "length of the user state")
	flag.Parse()

	d := &delegate{
		meta:  []byte("meta-" + *name),
		state: stateBytes("state-"+*name, *stateLen),
	}

	conf := memberlist.DefaultLocalConfig()
	conf.Name = *name
	conf.BindAddr = "127.0.0.1"
	conf.BindPort = *port
	conf.AdvertisePort = *port
	conf.Label = *label
	conf.EnableCompression = *compress
	conf.ProtocolVersion = uint8(*proto)
	conf.Delegate = d
	pd := &pingDelegate{payload: []byte("ack-" + *name), acks: map[string][]byte{}}
	conf.Ping = pd
	conf.ProbeInterval = 200 * time.Millisecond
	conf.ProbeTimeout = 100 * time.Millisecond
	conf.GossipInterval = 50 * time.Millisecond
	// Gossip alone sends an update only a few times in a small cluster, so
	// a member can miss it. The periodic state sync makes up for that.
	conf.PushPullInterval = 500 * time.Millisecond
	conf.LogOutput = os.Stderr

	m, err := memberlist.Create(conf)
	if err != nil {
		fmt.Printf("ERR %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("READY %d\n", m.LocalNode().Port)

	findNode := func(name string) *memberlist.Node {
		for _, n := range m.Members() {
			if n.Name == name {
				return n
			}
		}
		return nil
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "join":
			n, err := m.Join(fields[1:])
			if err != nil {
				fmt.Printf("ERR %v\n", strings.ReplaceAll(err.Error(), "\n", " "))
			} else {
				fmt.Printf("OK %d\n", n)
			}
		case "members":
			var out []string
			for _, n := range m.Members() {
				out = append(out, fmt.Sprintf("%s|%s|%d|%s", n.Name, n.Addr, n.Port, hex.EncodeToString(n.Meta)))
			}
			fmt.Printf("OK %s\n", strings.Join(out, ","))
		case "msgs":
			d.mu.Lock()
			var out []string
			for _, msg := range d.msgs {
				out = append(out, hex.EncodeToString(msg))
			}
			d.mu.Unlock()
			fmt.Printf("OK %s\n", strings.Join(out, ","))
		case "remote-state":
			d.mu.Lock()
			out := hex.EncodeToString(d.remoteState)
			d.mu.Unlock()
			fmt.Printf("OK %s\n", out)
		case "send-reliable", "send-best-effort":
			node := findNode(fields[1])
			if node == nil {
				fmt.Printf("ERR unknown node %s\n", fields[1])
				continue
			}
			msg, _ := hex.DecodeString(fields[2])
			if fields[0] == "send-reliable" {
				err = m.SendReliable(node, msg)
			} else {
				err = m.SendBestEffort(node, msg)
			}
			if err != nil {
				fmt.Printf("ERR %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "ping":
			addr, err := net.ResolveUDPAddr("udp", fields[2])
			if err != nil {
				fmt.Printf("ERR %v\n", err)
				continue
			}
			if _, err := m.Ping(fields[1], addr); err != nil {
				fmt.Printf("ERR %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "ack-payload":
			pd.mu.Lock()
			out := hex.EncodeToString(pd.acks[fields[1]])
			pd.mu.Unlock()
			fmt.Printf("OK %s\n", out)
		case "health":
			fmt.Printf("OK %d\n", m.GetHealthScore())
		case "leave":
			if err := m.Leave(2 * time.Second); err != nil {
				fmt.Printf("ERR %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "shutdown":
			if err := m.Shutdown(); err != nil {
				fmt.Printf("ERR %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "quit":
			_ = m.Shutdown()
			fmt.Println("OK")
			return
		default:
			fmt.Printf("ERR unknown command %s\n", fields[0])
		}
	}
}
