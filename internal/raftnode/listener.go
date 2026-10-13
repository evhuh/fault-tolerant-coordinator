package raftnode

import (
	"net"
)

// newListener binds eagerly so a bring-up failure (port already held by a
// stale process) is reported by Start rather than disappearing into a
// background goroutine. Day 3's demo lost an hour to exactly that.
func newListener(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}
