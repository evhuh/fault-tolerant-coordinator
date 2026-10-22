package raftnode

// The ReadIndex barrier: a linearizable read without writing to the log.
//
// # What it is for
//
// Day 9 found, and a test now pins, that a leader on the minority side of a
// partition answers reads from its own state and so serves data that is
// already stale. The leader believes it leads because nothing told it
// otherwise — raft only revokes leadership when a higher term shows up, and a
// cut-off leader never hears one.
//
// ReadIndex fixes that by asking the quorum instead of asking itself. The
// sequence etcd/raft implements under ReadOnlySafe:
//
//  1. The leader records its current commit index.
//  2. It sends heartbeats and waits for a QUORUM of acknowledgements.
//  3. Only then does it hand back a ReadState carrying that index.
//
// A cut-off leader cannot reach step 3. Its heartbeats go nowhere, no quorum
// acknowledges, and the read never completes — so it returns a timeout
// instead of a wrong answer. That is the whole point: the stale read becomes
// an UNAVAILABLE read, which a client can retry and a linearizability checker
// accepts, because a read that returns no value makes no claim.
//
// The index is then a lower bound the local state machine has to catch up to.
// A read served at an applied index below it would be reading a prefix of
// what the quorum had already committed when the read began, which is the
// same staleness by a different route.
//
// # Why this is not CheckQuorum
//
// CheckQuorum makes a leader step down when it stops hearing from a majority,
// which NARROWS this window — but it closes it only after an election
// timeout, and reads inside that timeout are still stale. ReadIndex closes it
// outright, per read. They are complementary and they are separate changes;
// conflating them would make a clean linearizability result impossible to
// attribute.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

// ErrReadBarrierLost means the barrier did not complete: no quorum confirmed
// this node still leads, or the node stopped waiting.
//
// It is deliberately NOT a "not leader" error. The node may well still be the
// leader; what is unknown is whether it is the leader a majority agrees on,
// and the honest report of that is "I could not confirm", not "ask someone
// else".
var ErrReadBarrierLost = errors.New("raftnode: read barrier not confirmed by a quorum")

// readBarrier tracks in-flight ReadIndex requests and applied-index waiters.
type readBarrier struct {
	mu sync.Mutex
	// pending maps a request token to the channel its ReadState arrives on.
	pending map[uint64]chan uint64
	nextTok uint64

	// appliedCh is closed and replaced every time the applied index
	// advances, so any number of waiters can block on one channel without
	// the Ready loop tracking them individually.
	//
	// A condition variable would also work and would be worse: the loop
	// would have to take a lock on every applied entry, and the Ready loop
	// is the one goroutine in this program that must never wait on anything
	// it also serves.
	appliedCh chan struct{}
}

func newReadBarrier() *readBarrier {
	return &readBarrier{
		pending:   map[uint64]chan uint64{},
		appliedCh: make(chan struct{}),
	}
}

// register allocates a token and the channel its ReadState will arrive on.
func (b *readBarrier) register() (uint64, chan uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextTok++
	tok := b.nextTok
	ch := make(chan uint64, 1)
	b.pending[tok] = ch
	return tok, ch
}

func (b *readBarrier) forget(tok uint64) {
	b.mu.Lock()
	delete(b.pending, tok)
	b.mu.Unlock()
}

// deliver hands a ReadState's index to whoever is waiting for that token.
//
// Non-blocking, because it runs ON THE READY LOOP. A waiter that has already
// given up leaves a buffered channel nobody reads, and blocking here to tell
// it so would stall the loop — the same shape as the leadership-revocation
// deadlock Day 8 found.
func (b *readBarrier) deliver(tok, index uint64) {
	b.mu.Lock()
	ch, ok := b.pending[tok]
	b.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- index:
	default:
	}
}

// notifyApplied wakes every applied-index waiter. Also called from the Ready
// loop, and also non-blocking for the same reason.
func (b *readBarrier) notifyApplied() {
	b.mu.Lock()
	close(b.appliedCh)
	b.appliedCh = make(chan struct{})
	b.mu.Unlock()
}

func (b *readBarrier) appliedWait() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.appliedCh
}

// failAll fails every in-flight barrier, by delivering the sentinel index 0.
//
// Called when the Ready loop is shutting down. Without it a caller blocked on
// a barrier waits out its whole context against a loop that will never answer
// — which is a hang where a prompt error belongs.
func (b *readBarrier) failAll() {
	b.mu.Lock()
	for tok, ch := range b.pending {
		select {
		case ch <- 0:
		default:
		}
		delete(b.pending, tok)
	}
	b.mu.Unlock()
}

// ReadIndex returns a log index that a quorum confirmed was committed at the
// time of the call, after which this node's state machine has caught up to it.
//
// On return, a read of local state is linearizable: every operation that had
// completed before the call is reflected in it. The caller does not need the
// index, so it is not returned — asking for the barrier and asking where it
// landed are the same request, and handing back an index invites someone to
// use it without waiting.
func (n *Node) ReadIndex(ctx context.Context) error {
	if !n.IsLeader() {
		return ErrNotLeaderNode
	}

	tok, ch := n.rb.register()
	defer n.rb.forget(tok)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], tok)
	if err := n.node.ReadIndex(ctx, buf[:]); err != nil {
		return fmt.Errorf("read index: %w", err)
	}

	var index uint64
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", ErrReadBarrierLost, ctx.Err())
	case <-n.done:
		return ErrReadBarrierLost
	case index = <-ch:
		// Index 0 is the shutdown sentinel: no quorum ever confirmed this.
		if index == 0 {
			return ErrReadBarrierLost
		}
	}

	return n.waitApplied(ctx, index)
}

// waitApplied blocks until this node has applied up to index.
//
// The check comes BEFORE the wait on every iteration, not after. The applied
// index may already be past it — commonly is, on a busy leader — and a
// wait-then-check would park until the next unrelated entry committed, which
// on an idle cluster is until the next Tick.
func (n *Node) waitApplied(ctx context.Context, index uint64) error {
	for {
		if n.appliedAtomic.Load() >= index {
			return nil
		}
		// Taken before re-reading the index, so an advance between the two
		// cannot be missed: the channel this returns is only closed by an
		// advance that happens after this call.
		wait := n.rb.appliedWait()
		if n.appliedAtomic.Load() >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: applied %d, need %d: %w",
				ErrReadBarrierLost, n.appliedAtomic.Load(), index, ctx.Err())
		case <-n.done:
			return ErrReadBarrierLost
		case <-wait:
		}
	}
}

// ErrNotLeaderNode is returned when a barrier is asked of a node that does
// not believe it leads. The server layer maps it onto its own ErrNotLeader.
var ErrNotLeaderNode = errors.New("raftnode: not leader")
