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
	// DeliverCorrupt controls what happens to a mangled frame. OFF by
	// default, because the default has to stay inside raft's fault model.
	//
	// Raft assumes a message arrives INTACT OR NOT AT ALL. It has no message
	// checksums, so a frame that is mangled but still decodes into something
	// plausible — a wrong index or term in an otherwise well-formed MsgApp
	// from a real peer — is a BYZANTINE fault, and raft makes no claim to
	// survive one. A corrupted MsgApp payload can be appended to a log.
	//
	// So by default a corrupted frame is dropped at the link, which is what
	// production actually gets from TCP checksums and TLS. That keeps "zero
	// violations across N seeds" a statement about raft's real guarantees
	// rather than about an adversary it never promised to handle.
	//
	// Turning this on delivers them. Any violation found that way is a
	// harness artifact unless the run is specifically a study of
	// undetectable corruption; correctness runs leave it off.
	DeliverCorrupt bool
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
	// held holds nodes that are PAUSED, and queued is what arrived for them
	// while they were.
	//
	// A pause is asymmetric, and getting that wrong made the fault useless.
	// INBOUND is queued: the kernel buffers packets for a process that is
	// not scheduled, and delivering them on resume is what keeps the node
	// unaware that it was deposed. OUTBOUND is SUPPRESSED, because a
	// descheduled process does not generate messages at all.
	//
	// The first version only queued inbound. The paused node's goroutines
	// still ran, so client submits still reached it, it still proposed, and
	// its MsgApp still went out — which RESET every follower's election
	// timer. Diagnosed from the node states at wake: the followers sat at
	// term 2 pointing at the paused leader, having never become candidates,
	// so the majority never elected and there was nothing newer for the
	// woken leader to be stale about.
	//
	// A pause is neither of the other two, which is the whole reason it
	// needs its own state. A partitioned node's messages are DROPPED; a
	// paused node's are QUEUED, because the kernel buffers packets for a
	// process that is not scheduled and delivers them when it resumes.
	//
	// That difference is the fault's entire point. Dropping would let the
	// paused node simply miss the new leader's higher-term heartbeat and
	// carry on believing it leads — which is the same end state by accident
	// — while queueing preserves the real sequence: it stays convinced it
	// leads, and finds out it was deposed only once it drains the queue.
	held   map[uint64]bool
	queued map[uint64][]*raftpb.Message

	closed bool
	wg     sync.WaitGroup

	// Counters, so a run can report how much chaos it actually induced
	// rather than how much it configured.
	//
	// The three drop reasons are counted SEPARATELY on purpose. Folding them
	// together produces a "drop rate" that is mostly messages to a node that
	// is switched off, which is not a network fault at all — and a metric
	// that reports 33% when 2% was configured is worse than no metric.
	sent, delivered int
	droppedRandom   int // the configured drop rate
	// MsgSnap accounting, separate from everything else.
	//
	// A DROPPED MsgSnap IS NOT LIKE A DROPPED MsgApp. raft retries an append
	// on its own, but it holds the follower in StateSnapshot until the
	// APPLICATION reports the snapshot's outcome — and until Day 10 nothing
	// in this build ever did. The follower then acks nothing while its
	// heartbeat responses keep it RecentActive, so the leader keeps a term it
	// cannot commit in. Counting these separately is what lets a frozen-clock
	// window be traced to a cause instead of correlated with a symptom.
	snapSent  int
	snapDrops []SnapDrop
	// dropSnapTo is a countdown of MsgSnap messages to swallow per
	// destination, for DropSnapshotsTo.
	dropSnapTo           map[uint64]int
	droppedPartition     int // crossed a partition
	droppedDown          int // addressed to a stopped node
	corrupted            int // bytes mangled at send
	corruptDroppedAtLink int // ...and discarded by link integrity (the default)
	corruptDelivered     int // ...or handed to a receiver, under DeliverCorrupt
	// Of those delivered: refused by the decoder, versus decoded cleanly and
	// Stepped. The second is the Byzantine case and must be zero in any run
	// whose results are being claimed.
	corruptRejectedByDecoder int
	// heldCount and releasedCount report the pause fault's effect: how many
	// messages were queued for a paused node, and how many were handed over
	// when it resumed. They must agree by the end of a run; a gap means
	// messages were queued for a node that never came back.
	heldCount     int
	releasedCount int
	// suppressedOut counts messages a paused node would have sent but,
	// being descheduled, never generated.
	suppressedOut      int
	corruptReachedStep int
}

func New(seed int64, f Faults) *Network {
	return &Network{
		rng:         rand.New(rand.NewSource(seed)),
		f:           f,
		recv:        map[uint64]func(context.Context, *raftpb.Message) error{},
		partitioned: map[uint64]bool{},
		down:        map[uint64]bool{},
		held:        map[uint64]bool{},
		queued:      map[uint64][]*raftpb.Message{},
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
// DECIDED BUT NOT YET IMPLEMENTED: this REPLACES the isolated set, and the
// agreed semantics are additive — see "Partition semantics" in
// docs/architecture.md.
//
// Replacing is why a held leader partition can be cancelled one step into its
// hold by the schedule's next partition, while holdHeal goes on suppressing
// heals, leaving the simulator believing it holds a partition it released.
// The change waits behind the snapshot-reporting fix because it makes the
// adversary strictly harsher and voids the calibrated floors.
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

// DropSnapshotsTo drops the next n MsgSnap messages addressed to id.
//
// A TARGETED FAULT, because the defect it exposes needs one specific message
// lost and nothing else. etcd/raft holds a follower in StateSnapshot until
// the application reports the snapshot's outcome, so a single dropped MsgSnap
// parks that follower permanently when nothing ever reports — and that is not
// reachable by raising the random drop rate without disturbing everything
// else in the run.
//
// The ordinary adversary drops MsgSnap too, at the configured rate; this
// exists so a deterministic test can show the causal chain rather than sample
// for it. Day 10 sampled 24 seeds for a frozen-clock window and found none,
// which at a ~6% per-run rate is a one-in-four outcome and settles nothing.
func (n *Network) DropSnapshotsTo(id uint64, count int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dropSnapTo == nil {
		n.dropSnapTo = map[uint64]int{}
	}
	n.dropSnapTo[id] = count
}

// Hold pauses delivery to a node, queueing what arrives instead of dropping
// it. Models a process that is not being scheduled.
func (n *Network) Hold(id uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.held[id] = true
}

// Release delivers everything that queued while the node was held, in
// arrival order, and resumes normal delivery.
//
// Order matters: these are raft messages, and replaying an append out of
// order would be a fault raft does not promise to survive. The queue is a
// FIFO for the same reason the kernel's is.
func (n *Network) Release(id uint64) {
	n.mu.Lock()
	if !n.held[id] {
		n.mu.Unlock()
		return
	}
	delete(n.held, id)
	pending := n.queued[id]
	delete(n.queued, id)
	step := n.recv[id]
	n.releasedCount += len(pending)
	n.delivered += len(pending)
	n.mu.Unlock()

	if step == nil {
		return
	}
	for _, msg := range pending {
		_ = step(context.Background(), msg)
	}
}

// IsHeld reports whether this node is currently paused.
func (n *Network) IsHeld(id uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.held[id]
}

// IsPartitioned reports whether this node is currently cut off.
//
// Exported for COVERAGE, not for routing. A linearizability verdict over a
// history in which no operation ever reached a cut-off node is a verdict
// about a healthy cluster, which is trivially linearizable; the harness has
// to be able to say how many operations actually landed on one.
func (n *Network) IsPartitioned(id uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.partitioned[id]
}

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
		CorruptDroppedAtLink:     n.corruptDroppedAtLink,
		CorruptDelivered:         n.corruptDelivered,
		CorruptRejectedByDecoder: n.corruptRejectedByDecoder,
		CorruptReachedStep:       n.corruptReachedStep,
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
	// Corrupted counts frames mangled at send. By DEFAULT every one is then
	// dropped at the link, so CorruptDroppedAtLink == Corrupted and
	// CorruptDelivered == 0 — which is what keeps a run inside the fault
	// model raft actually claims to handle.
	//
	// CorruptReachedStep is the number that decides whether a run's results
	// can be claimed: frames that were mangled, decoded anyway, and were
	// handed to raft. That is a Byzantine fault, and a correctness run must
	// report ZERO.
	Corrupted            int
	CorruptDroppedAtLink int
	CorruptDelivered     int

	CorruptRejectedByDecoder int
	CorruptReachedStep       int
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

// SnapDrop is one MsgSnap that never arrived.
type SnapDrop struct {
	AtMs   int64
	From   uint64
	To     uint64
	Reason string
}

// noteSnapDropLocked records a dropped MsgSnap. Caller must hold n.mu.
func (n *Network) noteSnapDropLocked(isSnap bool, from, to uint64, reason string) {
	if !isSnap {
		return
	}
	n.snapDrops = append(n.snapDrops, SnapDrop{
		AtMs: time.Now().UnixMilli(), From: from, To: to, Reason: reason,
	})
}

// SnapTraffic reports how many snapshots were sent and which were dropped.
func (n *Network) SnapTraffic() (sent int, drops []SnapDrop) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapSent, append([]SnapDrop(nil), n.snapDrops...)
}

// name is the drop reason as a word, for diagnostics.
func (r dropReason) name() string {
	switch r {
	case reasonDown:
		return "node-down"
	case reasonPartition:
		return "partition"
	}
	return "none"
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
// Send reports whether the SEND ATTEMPT completed, which in an in-memory
// network is almost always yes.
//
// A DROPPED MESSAGE RETURNS nil, deliberately: a real network drop looks like
// success to the sender too, and a simnet that reported drops as send errors
// would hand raft information no production sender has — making the harness
// easier on the system than reality is.
func (t *transport) Send(m *raftpb.Message) error {
	// Marshal on the SENDER's goroutine, before any fault decision — the
	// real transport does, and keeping it here means the receiver still
	// unmarshals and still drops anything corrupt.
	b, err := proto.Marshal(m)
	if err != nil {
		return nil
	}

	n := t.net
	isSnap := m.GetType() == raftpb.MsgSnap

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.sent++
	if isSnap {
		n.snapSent++
	}
	// The targeted snapshot drop goes FIRST, so a test can lose a MsgSnap
	// without also arranging a partition or a kill to do it.
	if isSnap && n.dropSnapTo[m.GetTo()] > 0 {
		n.dropSnapTo[m.GetTo()]--
		n.droppedRandom++
		n.noteSnapDropLocked(isSnap, t.id, m.GetTo(), "targeted")
		n.mu.Unlock()
		return nil
	}
	if r := n.dropReason(t.id, m.GetTo()); r != reasonNone {
		n.countDrop(r)
		n.noteSnapDropLocked(isSnap, t.id, m.GetTo(), r.name())
		n.mu.Unlock()
		return nil
	}
	if n.f.DropRate > 0 && n.rng.Float64() < n.f.DropRate {
		n.droppedRandom++
		n.noteSnapDropLocked(isSnap, t.id, m.GetTo(), "random")
		n.mu.Unlock()
		return nil
	}
	// Corrupt AFTER the drop decisions, so the two faults stay independent
	// and each count means what it says.
	corrupt := n.f.CorruptRate > 0 && n.rng.Float64() < n.f.CorruptRate
	if corrupt {
		b = corruptBytes(n.rng, b)
		n.corrupted++
		if !n.f.DeliverCorrupt {
			// LINK INTEGRITY, the default. A failed checksum discards the
			// frame before anything upstream sees it — what TCP and TLS give
			// production, and what keeps this run inside the fault model
			// raft actually claims to handle.
			n.corruptDroppedAtLink++
			n.mu.Unlock()
			return nil
		}
		n.corruptDelivered++
	}
	delay := n.pickDelayLocked()
	step := n.recv[m.GetTo()]
	n.wg.Add(1)
	n.mu.Unlock()

	if step == nil {
		n.wg.Done()
		return nil
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
		// OUTBOUND from a paused node never existed: a descheduled process
		// produces no messages, so this one is discarded rather than queued.
		// Queueing it would deliver a burst of stale heartbeats on resume
		// that the real process would never have sent.
		if n.held[t.id] {
			n.suppressedOut++
			n.mu.Unlock()
			return
		}
		// HELD, not dropped: queue it for a paused process.
		if n.held[m.GetTo()] {
			msg := &raftpb.Message{}
			if proto.Unmarshal(b, msg) == nil {
				n.queued[m.GetTo()] = append(n.queued[m.GetTo()], msg)
				n.heldCount++
			} else {
				// Corrupt frames never queue; the rule is the same whether
				// the destination is running or paused.
				n.corruptRejectedByDecoder++
			}
			n.mu.Unlock()
			return
		}
		n.delivered++
		n.mu.Unlock()

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
			// Mangled bytes that still PARSED, and DeliverCorrupt is on, so
			// they go to raft. Protobuf is not self-validating — a flipped
			// byte often lands in a field value rather than in the framing —
			// and raft has no checksum to catch it.
			//
			// THE BYZANTINE CASE. Counted loudly, because a run with a
			// nonzero value here cannot support a correctness claim.
			n.mu.Lock()
			n.corruptReachedStep++
			n.mu.Unlock()
		}
		_ = step(context.Background(), msg)
	}()
	// The ATTEMPT completed. Whether the message arrives is decided on the
	// goroutine above, and a sender does not learn that here any more than
	// it would over a real network.
	return nil
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
