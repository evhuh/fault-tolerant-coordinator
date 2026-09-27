// Package server hosts the StateStore behind gRPC.
//
// The load-bearing property of this package is that there is exactly ONE
// writer to the StateStore, and exactly one function that writes: propose.
// StateStore itself has no lock, and is correct only under a single writer;
// a gRPC server puts N concurrent handler goroutines in front of it, so the
// serialization has to live here.
//
// propose is also the Day 6 seam. Today its body is a direct Apply. On Day 6
// the body becomes node.Propose() plus a wait for the entry to come back
// through rd.CommittedEntries — and no caller moves.
package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
)

// Config carries the timing budget. The defaults are the ones Day 11 has to
// validate: a worker missing 4 heartbeats loses its lease, and the reclaim is
// visible within one Tick after that.
type Config struct {
	// LeaseTTL is how long a worker's lease survives without renewal.
	LeaseTTL time.Duration
	// TickPeriod is how often the leader proposes Tick{nowMs}. It bounds how
	// stale the state machine's notion of time can be.
	TickPeriod time.Duration
	// CommandVersion is stamped on every command this build proposes.
	CommandVersion uint32
}

func DefaultConfig() Config {
	return Config{
		LeaseTTL:       2000 * time.Millisecond, // 4 missed heartbeats at H=500ms
		TickPeriod:     200 * time.Millisecond,
		CommandVersion: 1,
	}
}

type Coordinator struct {
	cfg Config

	// mu guards store for BOTH reads and writes. The read accessors return
	// copies, but they still walk the same maps a concurrent Apply is
	// mutating, so an unguarded read is a data race -race will find under
	// any real load.
	mu    sync.Mutex
	store *state.StateStore

	// now is the only wall clock in the system. Everything downstream reads
	// time from Tick-carried timestamps instead. Held as a field so Day 8 can
	// replace it with an injected clock without touching call sites.
	now func() time.Time
}

func New(cfg Config) *Coordinator {
	return &Coordinator{
		cfg:   cfg,
		store: state.New(),
		now:   time.Now,
	}
}

func (c *Coordinator) nowMs() int64 { return c.now().UnixMilli() }

// propose is the single mutation path. Every gRPC handler funnels through it.
//
// Day 6: replace the body with node.Propose(encoded) and block until the
// entry is applied from rd.CommittedEntries. The signature stays.
func (c *Coordinator) propose(cmd *commandpb.Command) (state.Result, error) {
	cmd.Version = c.cfg.CommandVersion
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.Apply(cmd)
}

// read runs fn under the same lock as propose. Every read of the store goes
// through it, so there is one place to change on Day 6 if reads need a
// ReadIndex barrier (see Day 9: a partitioned leader serving local reads is a
// linearizability violation, and this is where the fix would land).
func (c *Coordinator) read(fn func(s *state.StateStore)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn(c.store)
}

// RunTicker proposes Tick{nowMs} until ctx is cancelled. It is the ONLY
// producer of time inside the state machine: lease expiry reads the timestamp
// carried in the committed entry, never time.Now(), so on Day 6 every replica
// expires the same leases at the same log position.
//
// Day 6 moves this under establishLeadership/revokeLeadership. Two leaders
// ticking concurrently is exactly the split-brain that stopping cleanly
// prevents.
func (c *Coordinator) RunTicker(ctx context.Context) {
	t := time.NewTicker(c.cfg.TickPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// A Tick whose lease expiries produce EvalTriggers is the normal
			// case; Day 3 drops them, because assign() rediscovers pending
			// work by scanning state. Day 4's broker consumes them.
			if _, err := c.propose(tickCmd(c.nowMs())); err != nil {
				// Tick has no failure mode other than a malformed payload,
				// which would be a bug in this file.
				panic(fmt.Sprintf("coordinator: tick rejected: %v", err))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// The assignment placeholder — DELETE ALL OF THIS ON DAY 4.
//
// This is the mechanism ("task, meet worker") with none of the policy. It is
// deliberately naive:
//
//   - It scans the StateStore instead of popping a queue, because the source
//     of truth for "what is pending" is committed state. A mutable side list
//     would be a wrong proto-eval-broker; the real broker is DERIVED from
//     committed state and rebuilt on leadership change.
//   - It has no priority ordering, no batching, and no conflict checking
//     beyond what applyPlan already does.
//   - It does not referee races. If two workers reach for the same task, the
//     second ApplyPlan fails its PENDING check and that worker polls again.
//     The state machine is already the backstop, so the placeholder is
//     allowed to be dumb.
//
// Day 4 replaces the body with the eval-broker -> scheduler -> planner
// pipeline and deletes the call from RequestTask. The worker binary, the
// executor, ApplyPlan/applyPlan, and the proto surface must not move.
// ---------------------------------------------------------------------------

// assign finds the first dispatchable task and returns a one-assignment plan.
// The caller must hold c.mu.
func (c *Coordinator) assign(workerID string) (*commandpb.Command, bool) {
	w, ok := c.store.Worker(workerID)
	if !ok || w.State != state.WorkerAlive {
		// An unregistered or lease-lapsed worker gets nothing; it must
		// re-register first. Assigning to it would only be rejected by
		// applyPlan's ALIVE check anyway.
		return nil, false
	}

	for _, id := range c.store.TaskIDs() {
		t, ok := c.store.Task(id)
		if !ok || t.State != state.TaskPending {
			continue
		}
		// Respect the attempt budget applyPlan enforces, or we would propose
		// a plan that can only ever be rejected and spin on it. Tick's
		// reclaim path already takes exhausted tasks terminal; an ApplyPlan
		// stop does not, which is a Day 4 gap noted in the roadmap.
		if t.Attempt >= t.Spec.MaxAttempts {
			continue
		}

		attempt := t.Attempt + 1
		return applyPlanCmd(placeholderEvalID(t.ID, attempt), &commandpb.Assignment{
			TaskId:        t.ID,
			WorkerId:      workerID,
			Attempt:       attempt,
			LeaseExpiryMs: c.nowMs() + c.cfg.LeaseTTL.Milliseconds(),
		}), true
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// Command constructors. Every one of these is a DECIDED FACT: ids assigned,
// timestamps read, nothing left for Apply to compute.
// ---------------------------------------------------------------------------

func tickCmd(nowMs int64) *commandpb.Command {
	return &commandpb.Command{
		Kind: &commandpb.Command_Tick{Tick: &commandpb.Tick{NowMs: nowMs}},
	}
}

func applyPlanCmd(evalID string, assignments ...*commandpb.Assignment) *commandpb.Command {
	return &commandpb.Command{
		Kind: &commandpb.Command_ApplyPlan{
			ApplyPlan: &commandpb.ApplyPlan{EvalId: evalID, Assignments: assignments},
		},
	}
}

// registerWorkerCmd doubles as the heartbeat: there is no Heartbeat command,
// so renewal is re-registration with a fresh lease, and runningTaskIDs is the
// worker's acknowledgement that promotes ASSIGNED -> RUNNING.
func (c *Coordinator) registerWorkerCmd(workerID string, runningTaskIDs []string) *commandpb.Command {
	now := c.nowMs()
	return &commandpb.Command{
		Kind: &commandpb.Command_RegisterWorker{
			RegisterWorker: &commandpb.RegisterWorker{
				WorkerId:       workerID,
				RegisteredMs:   now,
				LeaseExpiryMs:  now + c.cfg.LeaseTTL.Milliseconds(),
				RunningTaskIds: runningTaskIDs,
			},
		},
	}
}
