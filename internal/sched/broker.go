// Package sched is the Nomad-shaped scheduling pipeline: a priority eval
// broker, N scheduler workers deciding on snapshots in parallel, and a
// single-threaded planner that verifies plans under optimistic concurrency
// before they commit.
//
// Everything in this package is LEADER-ONLY and IN-MEMORY. None of it is
// replicated. Under Raft it starts on election and stops on leadership loss.
package sched

import (
	"container/heap"
	"context"
	"sync"

	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
)

// Eval is "something changed; someone should think about this job."
type Eval struct {
	ID          string
	JobID       string
	Priority    int32
	TriggeredBy state.EvalReason
	CreatedMs   int64
}

// Broker is a priority-ordered queue of pending evaluations.
//
// IT IS DERIVED, NOT REPLICATED, and that is the most interesting decision in
// this package. Replicating a work queue would make every pop a consensus
// round-trip, and it would need rebuilding after failover anyway — the new
// leader cannot trust an in-flight dequeue from the old one. So instead the
// queue is reconstructed from committed state on leadership acquisition
// (Rebuild), and losing it costs nothing but a rescan.
//
// # Blocked evaluations
//
// A plan that could not place every task leaves work undone, and the eval
// that produced it is already consumed. Without something more, those tasks
// sit PENDING forever: nothing creates a new eval when a worker merely
// FINISHES something, because CompleteTask emits no trigger.
//
// So an eval that failed to place everything is BLOCKED rather than dropped,
// and capacity-freeing commands release the blocked set back into the ready
// queue. This is Nomad's design, and it is what makes the broker more than a
// queue.
type Broker struct {
	mu     sync.Mutex
	ready  evalHeap
	seq    uint64          // tiebreak for equal-priority evals: FIFO within a priority
	queued map[string]bool // evals in `ready`, so a duplicate id is not enqueued twice

	// blocked holds evals that ran out of capacity. They are NOT in priority
	// order because they are not candidates until something frees up; on
	// release they go back through the heap and re-sort.
	blocked map[string]Eval

	notify chan struct{} // buffered(1); signals "the ready queue is non-empty"
	closed bool
}

func NewBroker() *Broker {
	return &Broker{
		queued:  make(map[string]bool),
		blocked: make(map[string]Eval),
		notify:  make(chan struct{}, 1),
	}
}

// Enqueue admits an eval to the ready queue. A repeated id is ignored, which
// makes it safe for the leader to re-derive the same eval from a replayed
// command.
func (b *Broker) Enqueue(e Eval) {
	b.mu.Lock()
	if b.closed || b.queued[e.ID] {
		b.mu.Unlock()
		return
	}
	// An eval being re-enqueued while blocked is no longer blocked.
	delete(b.blocked, e.ID)
	b.queued[e.ID] = true
	b.seq++
	heap.Push(&b.ready, &evalItem{eval: e, seq: b.seq})
	b.mu.Unlock()
	b.signal()
}

// Block parks an eval whose plan could not place every task. It waits there
// until Unblock, so capacity that frees later gets used.
func (b *Broker) Block(e Eval) {
	b.mu.Lock()
	if !b.closed {
		b.blocked[e.ID] = e
	}
	b.mu.Unlock()
}

// Unblock releases every blocked eval back into the ready queue. It is called
// when committed state frees capacity — a task completing, or failing
// terminally.
//
// Releasing ALL of them rather than one is deliberate: the broker does not
// know which blocked eval the freed slot suits, and re-planning a job with
// nothing to place is cheap (the scheduler finds no pending tasks and drops
// it). Being wrong here costs a wasted snapshot, not correctness.
func (b *Broker) Unblock() {
	b.mu.Lock()
	if b.closed || len(b.blocked) == 0 {
		b.mu.Unlock()
		return
	}
	for id, e := range b.blocked {
		delete(b.blocked, id)
		if b.queued[id] {
			continue
		}
		b.queued[id] = true
		b.seq++
		heap.Push(&b.ready, &evalItem{eval: e, seq: b.seq})
	}
	b.mu.Unlock()
	b.signal()
}

// Dequeue blocks until an eval is ready or ctx ends.
func (b *Broker) Dequeue(ctx context.Context) (Eval, bool) {
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return Eval{}, false
		}
		if b.ready.Len() > 0 {
			item := heap.Pop(&b.ready).(*evalItem)
			delete(b.queued, item.eval.ID)
			b.mu.Unlock()
			return item.eval, true
		}
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return Eval{}, false
		case <-b.notify:
		}
	}
}

func (b *Broker) signal() {
	select {
	case b.notify <- struct{}{}:
	default: // already signalled; a waiter will re-check the queue
	}
}

// Rebuild reconstructs the queue from committed state. This is what makes
// "derived, not replicated" correct: a new leader calls it on election and
// rediscovers every job with dispatchable work, without any of the old
// leader's memory.
//
// It scans for PENDING TASKS rather than for Eval objects, so an eval the old
// leader never got to propose costs nothing.
func (b *Broker) Rebuild(snap *state.Snapshot, idFor func(jobID string) string) int {
	b.mu.Lock()
	b.ready = nil
	b.queued = make(map[string]bool)
	b.blocked = make(map[string]Eval)
	b.mu.Unlock()

	// One eval per job with pending work, in sorted job order.
	seen := make(map[string]bool)
	n := 0
	for _, taskID := range snap.PendingTaskIDs("") {
		t, _ := snap.Task(taskID)
		if seen[t.JobID] {
			continue
		}
		seen[t.JobID] = true
		job, ok := snap.Job(t.JobID)
		if !ok {
			continue
		}
		b.Enqueue(Eval{
			ID:          idFor(t.JobID),
			JobID:       t.JobID,
			Priority:    job.Priority,
			TriggeredBy: state.EvalJobSubmitted,
			CreatedMs:   snap.NowMs,
		})
		n++
	}
	return n
}

// Close wakes every waiting scheduler worker. It is called on leadership
// loss: stopping cleanly is what prevents two leaders scheduling at once.
func (b *Broker) Close() {
	b.mu.Lock()
	b.closed = true
	b.ready = nil
	b.queued = make(map[string]bool)
	b.blocked = make(map[string]Eval)
	b.mu.Unlock()
	close(b.notify)
}

// Stats reports queue depth for metrics and tests.
func (b *Broker) Stats() (ready, blocked int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ready.Len(), len(b.blocked)
}

// ---------------------------------------------------------------------------
// Priority ordering. Higher priority first; within a priority, FIFO by
// insertion sequence. The sequence tiebreak matters: without it, equal
// priority evals would come out in heap-internal order, which is arbitrary
// and would make the scheduler's behaviour hard to reason about in a test.
// ---------------------------------------------------------------------------

type evalItem struct {
	eval Eval
	seq  uint64
}

type evalHeap []*evalItem

func (h evalHeap) Len() int { return len(h) }
func (h evalHeap) Less(i, j int) bool {
	if h[i].eval.Priority != h[j].eval.Priority {
		return h[i].eval.Priority > h[j].eval.Priority
	}
	return h[i].seq < h[j].seq
}
func (h evalHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *evalHeap) Push(x any)   { *h = append(*h, x.(*evalItem)) }
func (h *evalHeap) Pop() any     { old := *h; n := len(old); it := old[n-1]; *h = old[:n-1]; return it }
