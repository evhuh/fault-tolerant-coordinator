// Package simnet is an in-memory raft transport whose faults come from a
// seeded RNG.
//
// It replaces HTTP with channels, which is what makes a long chaotic run
// cheap: no ports to exhaust, no real latency to wait out, and a partition is
// a set membership test rather than a firewall rule.
//
// It still MARSHALS AND UNMARSHALS every message. That is deliberate — the
// one rule the real transport has (never Step a corrupt message) stays
// exercised, and a serialization bug would still surface here rather than
// hiding behind a Go pointer handed across a channel.
//
// # What the seed reproduces
//
// Every fault decision — drop, delay, partition — draws from one seeded RNG,
// so a given seed produces the same FAULT SCHEDULE. It does not produce the
// same execution: Go's goroutine scheduling decides who wins a race, and no
// amount of seeding changes that. See internal/clock for the same caveat.
package simnet

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/evazheng/fault-tolerant-coordinator/internal/raftnode"
)

// Faults describes how hostile the network is.
type Faults struct {
	// DropRate is the probability a message is discarded outright, 0..1.
	// Raft tolerates this by design; the point is to make it happen often
	// enough to matter.
	DropRate float64
	// CorruptRate is the probability a message's bytes are mangled in
	// flight, 0..1.
	//
	// This exists to execute a branch that otherwise never runs. Clean
	// marshaling every time means proto.Unmarshal always succeeds, so the
	// "corrupt → drop, never Step" rule — the one transport invariant that
	// actually protects safety — has no coverage at all. Feeding garbage into
	// a consensus module is how a network fault becomes a safety violation,
	// and an untested guard against that is a comment.
	//
	// Corruption is applied to the SERIALIZED BYTES, which is the only place
	// it can happen for real: an in-process transport cannot corrupt a Go
	// struct, but a network corrupts frames.
	CorruptRate float64
	// MinDelay and MaxDelay bound per-message latency. Delivery stays
	// asynchronous, so messages can REORDER, which is the more interesting
	// fault: a stale append arriving after a newer one is exactly the case
	// term checks exist for.
	MinDelay, MaxDelay time.Duration
}

// Network carries messages between in-process nodes.
type Network struct {
	mu   sync.Mutex
	rng  *rand.Rand
	f    Faults
	recv map[uint64]func(context.Context, *raftpb.Message) error
	// partitioned holds node ids that are cut off from everyone else. A
	// message is dropped when exactly one of its endpoints is in the set.
	partitioned map[uint64]bool
	// down holds nodes that are stopped. Distinct from partitioned: a
	// partitioned node is running and will catch up; a down node is not
	// there at all.
	down map[uint64]bool

	closed bool
	wg     sync.WaitGroup

	// Counters, so a run can report how much chaos it actually induced
	// rather than how much it configured.
	//
	// The three drop reasons are counted SEPARATELY on purpose. Folding them
	// together produces a "drop rate" that is mostly messages to a node that
	// is switched off, which is not a network fault at all — and a metric
	// that reports 33% when 2% was configured is worse than no metric.
	sent, delivered  int
	droppedRandom    int // the configured drop rate
	droppedPartition int // crossed a partition
	droppedDown      int // addressed to a stopped node
	corrupted        int // bytes mangled at send
	corruptArrived   int // ...and actually reached a receiver
	// Why each arrival was refused, kept apart because they test DIFFERENT
	// things: one proves the decoder rejects garbage, the other proves the
	// plausibility guard rejects garbage that decoded fine. A single
	// "discarded" number cannot tell you whether the second ever ran.
	corruptRejectedByDecoder int
	corruptRejectedByGuard   int
}

func New(seed int64, f Faults) *Network {
	return &Network{
		rng:         rand.New(rand.NewSource(seed)),
		f:           f,
		recv:        map[uint64]func(context.Context, *raftpb.Message) error{},
		partitioned: map[uint64]bool{},
		down:        map[uint64]bool{},
	}
}

// Transport returns this node's view of the network, for raftnode.Config.
func (n *Network) Transport(id uint64) raftnode.Transport {
	return &transport{net: n, id: id}
}

// Partition cuts ids off from every node not in the set. Calling it with no
// arguments heals the network.
//
// One-sided by construction: a message is dropped when exactly ONE endpoint
// is partitioned, so nodes inside the set still talk to each other. That is
// what makes a minority partition able to elect nothing while the majority
// carries on.
func (n *Network) Partition(ids ...uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned = map[uint64]bool{}
	for _, id := range ids {
		n.partitioned[id] = true
	}
}

// Heal removes every partition.
func (n *Network) Heal() { n.Partition() }

// SetDown marks a node stopped or started. A down node neither sends nor
// receives, and unlike a partition it is not coming back on its own.
func (n *Network) SetDown(id uint64, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = down
}

// Stats reports induced chaos, with each drop reason kept apart.
func (n *Network) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Stats{
		Sent: n.sent, Delivered: n.delivered,
		DroppedRandom:            n.droppedRandom,
		DroppedPartition:         n.droppedPartition,
		DroppedDown:              n.droppedDown,
		Corrupted:                n.corrupted,
		CorruptArrived:           n.corruptArrived,
		CorruptRejectedByDecoder: n.corruptRejectedByDecoder,
		CorruptRejectedByGuard:   n.corruptRejectedByGuard,
	}
}

// Stats is what a run actually induced.
type Stats struct {
	Sent, Delivered  int
	DroppedRandom    int
	DroppedPartition int
	DroppedDown      int
	// Corrupted counts messages mangled at send. Some are then dropped en
	// route for an unrelated reason — a partition opening, the target going
	// down — and never reach a receiver at all.
	//
	// The invariant is CorruptArrived == CorruptRejectedByDecoder +
	// CorruptRejectedByGuard. Comparing against Corrupted instead reports a
	// phantom leak every time a corrupt message is dropped by a partition,
	// which reads exactly like a corrupt message reaching Step.
	//
	// The two rejection reasons are separate because they exercise different
	// code: the decoder refusing malformed bytes, and the plausibility guard
	// refusing bytes that decoded into something a peer could not have sent.
	// Summing them would hide a run in which the guard never ran at all.
	Corrupted                int
	CorruptArrived           int
	CorruptRejectedByDecoder int
	CorruptRejectedByGuard   int
}

// Close stops delivery and waits for in-flight messages to finish, so a test
// does not leak goroutines into the next one.
func (n *Network) Close() {
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
	n.wg.Wait()
}

// dropReason classifies why a message from->to cannot be delivered, or
// returns reasonNone.
//
// Caller must hold n.mu.
type dropReason int

const (
	reasonNone dropReason = iota
	reasonDown
	reasonPartition
)

func (n *Network) dropReason(from, to uint64) dropReason {
	if n.down[from] || n.down[to] {
		return reasonDown
	}
	if n.partitioned[from] != n.partitioned[to] {
		return reasonPartition // exactly one side is cut off
	}
	return reasonNone
}

// countDrop records a drop under its own reason. Caller must hold n.mu.
func (n *Network) countDrop(r dropReason) {
	switch r {
	case reasonDown:
		n.droppedDown++
	case reasonPartition:
		n.droppedPartition++
	}
}

type transport struct {
	net *Network
	id  uint64
}

func (t *transport) Listen(_ string, step func(context.Context, *raftpb.Message) error) error {
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	if _, exists := t.net.recv[t.id]; exists {
		return fmt.Errorf("simnet: node %d already listening", t.id)
	}
	t.net.recv[t.id] = step
	return nil
}

// Send marshals, decides this message's fate from the seeded RNG, and
// delivers it asynchronously.
func (t *transport) Send(m *raftpb.Message) {
	// Marshal on the SENDER's goroutine, before any fault decision — the
	// real transport does, and keeping it here means the receiver still
	// unmarshals and still drops anything corrupt.
	b, err := proto.Marshal(m)
	if err != nil {
		return
	}

	n := t.net
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.sent++
	if r := n.dropReason(t.id, m.GetTo()); r != reasonNone {
		n.countDrop(r)
		n.mu.Unlock()
		return
	}
	if n.f.DropRate > 0 && n.rng.Float64() < n.f.DropRate {
		n.droppedRandom++
		n.mu.Unlock()
		return
	}
	// Corrupt AFTER the drop decisions, so the two faults stay independent
	// and each count means what it says.
	corrupt := n.f.CorruptRate > 0 && n.rng.Float64() < n.f.CorruptRate
	if corrupt {
		b = corruptBytes(n.rng, b)
		n.corrupted++
	}
	delay := n.pickDelayLocked()
	step := n.recv[m.GetTo()]
	n.wg.Add(1)
	n.mu.Unlock()

	if step == nil {
		n.wg.Done()
		return
	}

	go func() {
		defer n.wg.Done()
		if delay > 0 {
			time.Sleep(delay)
		}

		// Re-check on arrival, not just on send. A partition that opened
		// while this message was in flight must still swallow it, or a
		// delayed message would tunnel through a partition that exists.
		n.mu.Lock()
		if n.closed {
			n.mu.Unlock()
			return
		}
		if r := n.dropReason(t.id, m.GetTo()); r != reasonNone {
			n.countDrop(r)
			n.mu.Unlock()
			return
		}
		n.delivered++
		n.mu.Unlock()

		if corrupt {
			n.mu.Lock()
			n.corruptArrived++
			n.mu.Unlock()
		}

		msg := &raftpb.Message{}
		if proto.Unmarshal(b, msg) != nil {
			// THE RULE: corrupt → drop, never Step. Handing a malformed
			// message to the consensus module is how a network fault turns
			// into a safety violation.
			n.mu.Lock()
			n.corruptRejectedByDecoder++
			n.mu.Unlock()
			return
		}
		if corrupt {
			// Mangled bytes that still PARSED. Protobuf is not
			// self-validating, so this is common — a flipped byte often
			// lands in a field value rather than in the framing. Counted
			// separately because it is the case the decoder cannot catch and
			// something above it must.
			n.mu.Lock()
			n.corruptRejectedByGuard++
			n.mu.Unlock()
			return
		}
		_ = step(context.Background(), msg)
	}()
}

// pickDelayLocked draws this message's latency. Caller must hold n.mu, so the
// RNG is only ever touched under the lock — otherwise concurrent sends would
// race it and the seed would stop meaning anything.
func (n *Network) pickDelayLocked() time.Duration {
	if n.f.MaxDelay <= 0 {
		return 0
	}
	lo, hi := n.f.MinDelay, n.f.MaxDelay
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(n.rng.Int63n(int64(hi-lo)))
}

func (t *transport) Close() {
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	delete(t.net.recv, t.id)
}

// corruptBytes mangles a serialized message the way a bad link would: flip a
// byte, or truncate the frame. Drawn from the seeded RNG, so a given seed
// corrupts the same messages the same way.
func corruptBytes(rng *rand.Rand, b []byte) []byte {
	if len(b) == 0 {
		return b
	}
	out := make([]byte, len(b))
	copy(out, b)
	if rng.Intn(2) == 0 {
		i := rng.Intn(len(out))
		out[i] ^= byte(1 + rng.Intn(255))
		return out
	}
	// Truncation is the more realistic failure and the one most likely to
	// produce a partially-valid parse.
	return out[:rng.Intn(len(out))]
}
