package raftnode

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// The HTTP transport's "corrupt → drop, never Step" branch.
//
// It had never executed anywhere: the real network does not corrupt frames in
// a loopback test, and simnet marshaled cleanly until a corruption fault was
// added. This drives the branch directly, because the rule it enforces — never
// hand a malformed message to the consensus module — is the one transport
// invariant that protects safety rather than liveness.

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type stepRecorder struct {
	mu   sync.Mutex
	msgs []*raftpb.Message
}

func (r *stepRecorder) step(_ context.Context, m *raftpb.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
	return nil
}

func (r *stepRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

// post sends a raw body to the transport's /raft endpoint, bypassing Send so
// the bytes can be anything.
func post(t *testing.T, addr string, body []byte) {
	t.Helper()
	resp, err := http.Post(addr+"/raft", "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = resp.Body.Close()
}

func TestHTTPTransportDiscardsMalformedBodies(t *testing.T) {
	addr := fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
	tp := newHTTPTransport(1, map[uint64]string{1: addr, 2: "http://peer-2"})
	rec := &stepRecorder{}
	if err := tp.Listen(addr, rec.step); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer tp.Close()

	// A well-formed message first, to prove the endpoint works at all —
	// otherwise "nothing was Stepped" would be satisfied by a broken server.
	// Addressed to self (1), from a known peer (2).
	// A non-local type matters: Type 0 is MsgHup, which the guard refuses.
	var from, to uint64 = 2, 1
	goodType := raftpb.MsgApp
	good, err := proto.Marshal(&raftpb.Message{From: &from, To: &to, Type: &goodType})
	if err != nil {
		t.Fatal(err)
	}
	post(t, addr, good)

	deadline := time.Now().Add(2 * time.Second)
	for rec.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if rec.count() != 1 {
		t.Fatalf("a valid message was not Stepped (count=%d)", rec.count())
	}

	// Now the malformed ones. None may reach Step, and none may panic the
	// handler — a panicking receiver is a denial of service a peer can
	// trigger at will.
	bad := [][]byte{
		{0xff, 0xff, 0xff, 0xff},         // not protobuf at all
		append([]byte(nil), good[:1]...), // truncated frame
		{0x08},                           // a field tag with no value
		bytes.Repeat([]byte{0x7f}, 64),   // plausible-looking garbage
		{},                               // empty body
	}
	for _, b := range bad {
		post(t, addr, b)
	}
	time.Sleep(200 * time.Millisecond)

	if got := rec.count(); got != 1 {
		t.Errorf("Step was called %d times; only the one valid message should have reached it", got)
	}

	// And the endpoint still works afterwards: a corrupt message must not
	// wedge the receiver for everyone behind it.
	post(t, addr, good)
	deadline = time.Now().Add(2 * time.Second)
	for rec.count() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if rec.count() != 2 {
		t.Errorf("the transport stopped accepting valid messages after corrupt ones (count=%d)", rec.count())
	}
}

// THE ONE THAT PARSES. An empty body is valid protobuf: it decodes to
// Message{Type: MsgHup}, and MsgHup makes a node start an election.
//
// Unmarshal succeeding is therefore not sufficient to Step a message. Without
// the plausibility guard, one empty HTTP POST forces the receiver to campaign,
// and a loop of them keeps the cluster from ever settling — at the cost of one
// request each.
func TestEmptyBodyDoesNotForceAnElection(t *testing.T) {
	// The decode itself succeeds; that is the whole problem.
	m := &raftpb.Message{}
	if err := proto.Unmarshal(nil, m); err != nil {
		t.Fatalf("precondition: empty body no longer parses: %v", err)
	}
	if m.GetType() != raftpb.MsgHup {
		t.Fatalf("precondition: empty body decodes to %v, not MsgHup", m.GetType())
	}

	addr := fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
	tp := newHTTPTransport(1, map[uint64]string{1: addr, 2: "http://peer-2"})
	rec := &stepRecorder{}
	if err := tp.Listen(addr, rec.step); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer tp.Close()

	for i := 0; i < 20; i++ {
		post(t, addr, nil)
	}
	time.Sleep(200 * time.Millisecond)
	if got := rec.count(); got != 0 {
		t.Errorf("%d empty bodies reached Step as MsgHup — each one forces an election", got)
	}
}

// A message addressed to someone else reached the wrong node.
func TestMessageForAnotherNodeIsDropped(t *testing.T) {
	addr := fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
	tp := newHTTPTransport(1, map[uint64]string{1: addr, 2: "http://peer-2"})
	rec := &stepRecorder{}
	if err := tp.Listen(addr, rec.step); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer tp.Close()

	var from, elsewhere uint64 = 2, 9
	typ := raftpb.MsgApp
	body, err := proto.Marshal(&raftpb.Message{From: &from, To: &elsewhere, Type: &typ})
	if err != nil {
		t.Fatal(err)
	}
	post(t, addr, body)
	time.Sleep(150 * time.Millisecond)
	if got := rec.count(); got != 0 {
		t.Errorf("a message addressed to node 9 was Stepped by node 1 (%d)", got)
	}
}

// A local message type arriving over a socket is not a peer talking.
//
// raft already refuses these (see TestRaftIgnoresLocalMessagesArrivingFromThe
// Network), so this layer is belt and braces — but it is the layer that can
// say so explicitly rather than relying on a library internal.
func TestLocalMessageTypesFromTheNetworkAreDropped(t *testing.T) {
	addr := fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
	tp := newHTTPTransport(1, map[uint64]string{1: addr, 2: "http://peer-2"})
	rec := &stepRecorder{}
	if err := tp.Listen(addr, rec.step); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer tp.Close()

	var from, to uint64 = 2, 1 // otherwise entirely well-formed
	for _, typ := range []raftpb.MessageType{raftpb.MsgHup, raftpb.MsgBeat} {
		ty := typ
		body, err := proto.Marshal(&raftpb.Message{From: &from, To: &to, Type: &ty})
		if err != nil {
			t.Fatal(err)
		}
		post(t, addr, body)
	}
	time.Sleep(150 * time.Millisecond)
	if got := rec.count(); got != 0 {
		t.Errorf("%d local-type messages from the network were Stepped", got)
	}
}

// A message from an id outside the configured cluster is not from this
// cluster. Membership is fixed at startup, so this is exactly decidable.
func TestMessageFromAnUnknownPeerIsDropped(t *testing.T) {
	addr := fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
	tp := newHTTPTransport(1, map[uint64]string{1: addr, 2: "http://peer-2"})
	rec := &stepRecorder{}
	if err := tp.Listen(addr, rec.step); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer tp.Close()

	var stranger, to uint64 = 99, 1
	typ := raftpb.MsgApp
	body, err := proto.Marshal(&raftpb.Message{From: &stranger, To: &to, Type: &typ})
	if err != nil {
		t.Fatal(err)
	}
	post(t, addr, body)
	time.Sleep(150 * time.Millisecond)
	if got := rec.count(); got != 0 {
		t.Errorf("a message from unknown node 99 was Stepped (%d)", got)
	}
}
