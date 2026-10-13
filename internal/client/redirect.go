// Package client holds the NOT_LEADER redirect logic shared by the worker and
// schedctl.
//
// Workers and clients both dial a coordinator that may not be the leader, or
// may stop being one mid-request. Rather than a discovery service, the
// coordinator answers with the leader's address and the caller re-dials.
package client

import (
	"fmt"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// notLeaderPrefix is how the coordinator marks a redirect. Matching on the
// message is crude; the alternative is a typed error detail, which is more
// machinery than a fixed cluster needs.
const notLeaderPrefix = "NOT_LEADER"

// IsNotLeader reports whether an RPC failed because this node does not lead,
// and extracts the leader's address when the node knew one.
func IsNotLeader(err error) (leaderAddr string, ok bool) {
	st, isStatus := status.FromError(err)
	if !isStatus {
		return "", false
	}
	if st.Code() != codes.FailedPrecondition && st.Code() != codes.Unavailable {
		return "", false
	}
	msg := st.Message()
	if !strings.Contains(msg, notLeaderPrefix) {
		return "", false
	}
	// "NOT_LEADER: leader is at http://127.0.0.1:9021"
	if i := strings.LastIndex(msg, "leader is at "); i >= 0 {
		return strings.TrimSpace(msg[i+len("leader is at "):]), true
	}
	return "", true // no leader known yet; the caller retries where it is
}

// Pool keeps one connection per coordinator address and tracks which one
// currently leads, so a redirect is paid once rather than on every call.
type Pool struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	current string
	seeds   []string
}

// NewPool takes one or more coordinator gRPC addresses. Any one will do: a
// non-leader answers NOT_LEADER with the leader's address, so a single seed
// is enough to find the cluster.
func NewPool(seeds ...string) *Pool {
	norm := make([]string, 0, len(seeds))
	for _, s := range seeds {
		norm = append(norm, normalize(s))
	}
	return &Pool{conns: map[string]*grpc.ClientConn{}, seeds: norm, current: norm[0]}
}

// Client returns a client for the address believed to be the leader.
func (p *Pool) Client() (schedulerpb.SchedulerClient, error) {
	p.mu.Lock()
	addr := p.current
	p.mu.Unlock()
	conn, err := p.connTo(addr)
	if err != nil {
		return nil, err
	}
	return schedulerpb.NewSchedulerClient(conn), nil
}

// Redirect points the pool at a new leader. An empty address means "no leader
// known" — the pool rotates to the next seed rather than hammering a node
// that just told us it cannot help.
func (p *Pool) Redirect(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if addr != "" {
		p.current = normalize(addr)
		return
	}
	for i, s := range p.seeds {
		if s == p.current {
			p.current = p.seeds[(i+1)%len(p.seeds)]
			return
		}
	}
	p.current = p.seeds[0]
}

func (p *Pool) connTo(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	p.conns[addr] = c
	return c, nil
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[string]*grpc.ClientConn{}
}

// normalize strips a scheme: the raft peer set carries URLs, but gRPC dials
// host:port. The two address spaces differ and the redirect crosses them.
func normalize(addr string) string {
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	return strings.TrimSuffix(addr, "/")
}

// maxAttempts bounds one call's chase. A cluster mid-election can bounce a
// caller around; past this the call fails and the caller retries at its own
// pace rather than spinning inside a single RPC.
const maxAttempts = 6

// isUnreachable reports a transport-level failure: the node is down, or did
// not answer in time.
//
// This is a SEPARATE case from NOT_LEADER and it is the one that actually
// matters on failover. A killed leader does not answer "NOT_LEADER" — it does
// not answer at all. A pool that only handled redirects would retry the
// corpse forever, which is precisely what stranded the worker pool the first
// time this was wired up.
func isUnreachable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}

// Do runs fn against the node currently believed to lead, following
// NOT_LEADER redirects.
//
// This is the whole point of the package: without it the coordinator's
// redirect is advice nobody takes. A worker that ignores it re-registers
// against a deposed node forever, and a failover strands the worker pool even
// though the cluster recovered fine.
//
// fn may run more than once, so it must be safe to retry. Every RPC in the
// scheduler API is: writes carry (clientID, requestID) or are idempotent on
// (taskID, attempt), and reads have no effect.
func (p *Pool) Do(fn func(schedulerpb.SchedulerClient) error) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		c, err := p.Client()
		if err != nil {
			return err
		}
		err = fn(c)
		if err == nil {
			return nil
		}
		lastErr = err

		// Two distinct ways to be talking to the wrong node:
		if addr, ok := IsNotLeader(err); ok {
			// It answered, and told us where to go. An empty addr means it
			// knows of no leader either, and Redirect rotates instead.
			p.Redirect(addr)
			continue
		}
		if isUnreachable(err) {
			// It did not answer at all — killed, partitioned, or still
			// starting. Nobody is going to send us a redirect, so move on.
			p.Redirect("")
			continue
		}
		return err // a real error; the caller decides what to do
	}
	return lastErr
}

// Current reports the address the pool is currently aimed at. For logging and
// tests.
func (p *Pool) Current() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}
