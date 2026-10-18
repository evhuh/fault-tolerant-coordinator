package raftnode

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Transport is fire-and-forget HTTP POSTs of marshaled raftpb.Messages.
//
// No retries, no acks, no ordering — Raft tolerates drop, reorder, and
// duplication by design, and layering reliability underneath it would only
// add latency. The one rule that is NOT optional: never Step a corrupt
// message. A malformed message is dropped, because feeding garbage into the
// consensus module is how you turn a network fault into a safety violation.
//
// Day 8 replaces this with a shim that can drop, delay, and partition on a
// seeded schedule. That is why sending goes through a struct rather than a
// package-level function.
type Transport interface {
	// Listen starts receiving, handing every well-formed message to step.
	Listen(selfURL string, step func(context.Context, *raftpb.Message) error) error
	// Send delivers a message to m.To, best effort.
	Send(m *raftpb.Message)
	Close()
}

// httpTransport is the real one, and the default.
type httpTransport struct {
	peers  map[uint64]string
	client *http.Client
	srv    *http.Server
}

func newHTTPTransport(peers map[uint64]string) *httpTransport {
	return &httpTransport{
		peers: peers,
		client: &http.Client{
			// Bounded: a hung peer must not pin a sender goroutine forever.
			Timeout: 2 * time.Second,
		},
	}
}

// listen starts the receiver for this node.
//
// A dedicated mux, never http.DefaultServeMux: the global one would panic on
// double registration the moment two nodes share a process, which is exactly
// what Day 8's simulator does.
func (t *httpTransport) Listen(selfURL string, step func(context.Context, *raftpb.Message) error) error {
	u, err := url.Parse(selfURL)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/raft", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		m := &raftpb.Message{}
		if proto.Unmarshal(body, m) != nil {
			return // corrupt → drop, never Step
		}
		_ = step(r.Context(), m)
	})
	t.srv = &http.Server{Addr: u.Host, Handler: mux}

	ln, err := newListener(u.Host)
	if err != nil {
		return err
	}
	go func() { _ = t.srv.Serve(ln) }()
	return nil
}

func (t *httpTransport) Send(m *raftpb.Message) {
	addr, ok := t.peers[m.GetTo()]
	if !ok {
		return
	}
	b, err := proto.Marshal(m)
	if err != nil {
		return
	}
	resp, err := t.client.Post(addr+"/raft", "application/octet-stream", bytes.NewReader(b))
	if err != nil {
		return // peer down or partitioned; raft's own protocol retries
	}
	_ = resp.Body.Close()
}

func (t *httpTransport) Close() {
	if t.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = t.srv.Shutdown(ctx)
	}
}
