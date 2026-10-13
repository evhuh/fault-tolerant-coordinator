package sched

import (
	"context"
	"sync"

	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
)

// Assignment is one task-meets-worker decision.
type Assignment struct {
	TaskID        string
	WorkerID      string
	Attempt       int32
	LeaseExpiryMs int64
}

// Stop retracts a task from the worker holding it.
type Stop struct {
	TaskID string
	Reason string
}

// Plan is what a scheduler worker produces: a batch of decisions made against
// one snapshot, to be verified and committed as a single entry.
type Plan struct {
	EvalID      string
	JobID       string
	Assignments []Assignment
	Stops       []Stop
	// Unplaced counts tasks the scheduler wanted to place but had no room
	// for. A nonzero value is what makes the eval BLOCKED rather than done.
	Unplaced int
}

// PlanResult is the planner's verdict.
type PlanResult struct {
	Accepted int
	Rejected int
	// Err is set only if the commit itself failed, not if assignments were
	// rejected — a partial accept is a normal outcome, not an error.
	Err error
}

// Planner verifies plans against current state and commits them.
type Planner interface {
	Submit(p *Plan) PlanResult
}

// Snapshotter hands out coherent point-in-time views of the state machine.
type Snapshotter interface {
	Snapshot() *state.Snapshot
}

// Config tunes the scheduler workers.
type Config struct {
	// Workers is how many evals are decided concurrently. This is the knob
	// Day 11's contention sweep moves: more schedulers help at low
	// contention and manufacture rejections at high contention.
	Workers int
	// LeaseTTLMs is how long an assignment's lease runs.
	LeaseTTLMs int64
	// MaxPlanSize caps assignments per plan, and therefore per committed
	// entry. Batching is the whole throughput argument, so this is large.
	MaxPlanSize int
}

func DefaultConfig() Config {
	return Config{Workers: 4, LeaseTTLMs: 2000, MaxPlanSize: 1000}
}

// Scheduler runs N workers that dequeue evals, decide against a snapshot, and
// hand plans to the planner.
type Scheduler struct {
	cfg     Config
	broker  *Broker
	snaps   Snapshotter
	planner Planner

	wg sync.WaitGroup
}

func NewScheduler(cfg Config, b *Broker, s Snapshotter, p Planner) *Scheduler {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.MaxPlanSize < 1 {
		cfg.MaxPlanSize = 1
	}
	return &Scheduler{cfg: cfg, broker: b, snaps: s, planner: p}
}

// Start launches the worker goroutines. Day 6 calls this from
// establishLeadership.
func (s *Scheduler) Start(ctx context.Context) {
	for i := 0; i < s.cfg.Workers; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.run(ctx)
		}()
	}
}

// Wait blocks until every scheduler worker has stopped.
func (s *Scheduler) Wait() { s.wg.Wait() }

func (s *Scheduler) run(ctx context.Context) {
	for {
		eval, ok := s.broker.Dequeue(ctx)
		if !ok {
			return
		}

		// The expensive part: read a snapshot and decide. N workers do this
		// CONCURRENTLY, touching nothing shared. That is the throughput
		// argument — only the commit below serializes.
		plan := s.computePlan(s.snaps.Snapshot(), eval)

		if len(plan.Assignments) == 0 && len(plan.Stops) == 0 {
			// Nothing to do. If work remains but there was no room, the eval
			// waits for capacity rather than disappearing.
			if plan.Unplaced > 0 {
				s.broker.Block(eval)
			}
			continue
		}

		res := s.planner.Submit(plan)

		// Two reasons to block: we knowingly ran out of room, or the planner
		// rejected assignments we thought were good. Both mean work remains
		// and no other eval is coming to pick it up.
		if plan.Unplaced > 0 || res.Rejected > 0 {
			s.broker.Block(eval)
		}
	}
}

// computePlan places this job's pending tasks onto workers with room.
//
// It reads ONLY the snapshot, never live state, which is what lets it run in
// parallel. Everything it decides is provisional: the planner re-verifies
// against current state, and applyPlan checks again on every replica.
func (s *Scheduler) computePlan(snap *state.Snapshot, eval Eval) *Plan {
	plan := &Plan{EvalID: eval.ID, JobID: eval.JobID}

	// Remaining capacity per worker, computed once from the snapshot. As the
	// plan places tasks it decrements locally, so one plan never oversubscribes
	// a worker even though nothing is committed yet.
	free := make(map[string]int32, len(snap.WorkerIDs()))
	order := make([]string, 0, len(snap.WorkerIDs()))
	for _, id := range snap.WorkerIDs() {
		w, _ := snap.Worker(id)
		if w.State != state.WorkerAlive {
			continue
		}
		room := w.Capacity - snap.InFlight(id)
		if room <= 0 {
			continue
		}
		free[id] = room
		order = append(order, id) // WorkerIDs() is sorted, so this is too
	}

	next := 0
	for _, taskID := range snap.PendingTaskIDs(eval.JobID) {
		if len(plan.Assignments) >= s.cfg.MaxPlanSize {
			plan.Unplaced++
			continue
		}
		worker, ok := pickWorker(order, free, &next)
		if !ok {
			// No room anywhere. Everything left is unplaced, and the eval
			// blocks until capacity frees.
			plan.Unplaced++
			continue
		}
		t, _ := snap.Task(taskID)
		plan.Assignments = append(plan.Assignments, Assignment{
			TaskID:   taskID,
			WorkerID: worker,
			// The attempt is a strict dispatch counter. applyPlan validates
			// it is exactly Attempt+1, which is what makes a reclaimed task's
			// next dispatch distinguishable from a slow worker's stale report.
			Attempt:       t.Attempt + 1,
			LeaseExpiryMs: snap.NowMs + s.cfg.LeaseTTLMs,
		})
		free[worker]--
	}
	return plan
}

// pickWorker round-robins over workers that still have room, so one plan
// spreads across the pool instead of filling the first worker.
func pickWorker(order []string, free map[string]int32, next *int) (string, bool) {
	for i := 0; i < len(order); i++ {
		idx := (*next + i) % len(order)
		id := order[idx]
		if free[id] > 0 {
			*next = idx + 1
			return id, true
		}
	}
	return "", false
}
