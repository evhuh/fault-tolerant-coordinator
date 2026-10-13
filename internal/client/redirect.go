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

func NewPool(seeds ...string) *Pool {
	return &Pool{conns: map[string]*grpc.ClientConn{}, seeds: seeds, current: seeds[0]}
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
