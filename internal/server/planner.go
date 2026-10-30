package server

import (
	"sync"

	"github.com/evazheng/fault-tolerant-coordinator/internal/sched"
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"

	"github.com/evazheng/fault-tolerant-coordinator/internal/metrics"
)

// Planner is the serialization point of the whole design.
//
// Scheduler workers decide in parallel against snapshots that may already be
// out of date. The planner is the referee: it re-verifies every assignment
// against CURRENT state, drops the ones that no longer hold, and commits the
// survivors as ONE ApplyPlan entry.
//
// That split is the throughput argument. Deciding — the expensive part — runs
// concurrently; only verify-and-commit serializes, and one entry carries many
// assignments, so a single fsync is amortized across all of them.
//
// Single-threaded by construction: plannerMu admits one plan at a time, so
// two plans can never both believe they got the same worker's last free slot.
type Planner struct {
	c *Coordinator

	plannerMu sync.Mutex

	// Counters for Day 11. A rejection is an optimistic-concurrency conflict:
	// two schedulers reached for the same task from the same snapshot, and
	// the second one lost. Watching this rise with contention is the second
	// benchmark experiment.
	statsMu             sync.Mutex
	plansSubmitted      int
	assignmentsAccepted int
	assignmentsRejected int
	largestPlan         int
}

func NewPlanner(c *Coordinator) *Planner { return &Planner{c: c} }

// Submit verifies and commits one plan.
func (p *Planner) Submit(plan *sched.Plan) sched.PlanResult {
	if plan == nil || (len(plan.Assignments) == 0 && len(plan.Stops) == 0) {
		return sched.PlanResult{}
	}

	// One plan at a time. Everything below is the optimistic-concurrency
	// check the architecture rests on.
	p.plannerMu.Lock()
	defer p.plannerMu.Unlock()

	// Verification reads CURRENT state under the store lock, then the lock is
	// released and the plan is proposed.
	//
	// Under Raft there is a REAL GAP between those two steps — a Tick or a
	// CompleteTask can commit in between — and that gap cannot be closed:
	// holding the store lock across a consensus round-trip would stop the
	// apply loop that the round-trip depends on. So the gap is accepted, and
	// applyPlan re-validates each assignment on every replica, skipping the
	// ones that went stale.
	//
	// That is the division of labour the whole design rests on: the planner's
	// job is to make conflicts RARE, the state machine's job is to make them
	// SAFE.
	p.c.mu.Lock()
	now := p.c.store.NowMs()
	leaseExpiry := now + p.c.cfg.LeaseTTL.Milliseconds()

	// Room remaining per worker, decremented as this plan consumes it, so one
	// plan cannot oversubscribe a worker it is itself filling.
	room := make(map[string]int32)
	roomFor := func(workerID string) int32 {
		if r, ok := room[workerID]; ok {
			return r
		}
		w, ok := p.c.store.Worker(workerID)
		if !ok {
			return 0
		}
		var inFlight int32
		for _, id := range p.c.store.TaskIDs() {
			t, _ := p.c.store.Task(id)
			if t.WorkerID == workerID && (t.State == state.TaskAssigned || t.State == state.TaskRunning) {
				inFlight++
			}
		}
		r := w.Capacity - inFlight
		room[workerID] = r
		return r
	}

	accepted := make([]*commandpb.Assignment, 0, len(plan.Assignments))
	rejected := 0
	seen := make(map[string]bool, len(plan.Assignments))

	for _, a := range plan.Assignments {
		if seen[a.TaskID] {
			rejected++ // the same task twice in one plan is a scheduler bug
			continue
		}
		t, ok := p.c.store.Task(a.TaskID)
		if !ok {
			rejected++
			continue
		}
		// The task moved since the snapshot: already assigned, already
		// running, already terminal. Someone else got there first.
		if t.State != state.TaskPending {
			rejected++
			continue
		}
		// The attempt the scheduler computed is no longer the next one, so
		// the snapshot predates a reclaim.
		if a.Attempt != t.Attempt+1 || a.Attempt > t.Spec.MaxAttempts {
			rejected++
			continue
		}
		// The task picked up a retry backoff after the snapshot was taken.
		if t.NotBeforeMs > now {
			rejected++
			continue
		}
		w, ok := p.c.store.Worker(a.WorkerID)
		if !ok || w.State != state.WorkerAlive {
			rejected++ // the worker died between snapshot and commit
			continue
		}
		if roomFor(a.WorkerID) <= 0 {
			rejected++ // the worker filled up
			continue
		}

		room[a.WorkerID]--
		seen[a.TaskID] = true
		accepted = append(accepted, &commandpb.Assignment{
			TaskId:   a.TaskID,
			WorkerId: a.WorkerID,
			Attempt:  a.Attempt,
			// Recomputed against CURRENT time, not the snapshot's. A plan
			// that waited would otherwise carry an already-expired lease and
			// be rejected wholesale by applyPlan.
			LeaseExpiryMs: leaseExpiry,
		})
	}

	stops := make([]*commandpb.Stop, 0, len(plan.Stops))
	for _, st := range plan.Stops {
		t, ok := p.c.store.Task(st.TaskID)
		if !ok || (t.State != state.TaskAssigned && t.State != state.TaskRunning) {
			rejected++
			continue
		}
		stops = append(stops, &commandpb.Stop{TaskId: st.TaskID, Reason: st.Reason})
	}

	// Lock released BEFORE proposing. Everything below races other commands,
	// by design.
	p.c.mu.Unlock()

	if len(accepted) == 0 && len(stops) == 0 {
		p.record(0, rejected, 0)
		return sched.PlanResult{Rejected: rejected}
	}

	// One entry, however many assignments it carries. This is the batching
	// that amortizes fsync across many scheduling decisions — and the reason
	// paying one consensus round-trip per PLAN rather than per decision is
	// the whole throughput argument.
	cmd := &commandpb.Command{
		Kind: &commandpb.Command_ApplyPlan{
			ApplyPlan: &commandpb.ApplyPlan{
				EvalId:      plan.EvalID,
				Assignments: accepted,
				Stops:       stops,
			},
		},
	}

	// propose() waits for the entry to be applied, and applyPlan is what
	// decides how much of it survived the gap. plannerMu is still held, so
	// no second plan verifies against state this one has not landed in yet.
	res, err := p.c.propose(cmd)
	if err != nil {
		// Leadership loss or a lost proposal. The assignments did not happen;
		// the eval blocks and a later leader re-plans them.
		p.record(0, rejected+len(accepted), 0)
		return sched.PlanResult{Rejected: rejected + len(accepted), Err: err}
	}

	// The state machine's own skips are optimistic-concurrency conflicts that
	// opened AFTER verification — the ones the gap above makes unavoidable.
	// Counting them here is what makes the rejection metric honest.
	rejected += res.PlanSkipped
	p.record(res.PlanApplied, rejected, res.PlanApplied)
	return sched.PlanResult{Accepted: res.PlanApplied, Rejected: rejected}
}

func (p *Planner) record(accepted, rejected, planSize int) {
	// The plan-batching experiment's independent variable, MEASURED rather
	// than assumed from -max-plan-size: a run configured to batch but offered
	// too little load to ever find two assignable tasks at once would
	// otherwise read as a disproof of batching.
	//
	// Only non-empty plans are observed. A plan of zero assignments is the
	// broker having manufactured work that placed nothing, which is a real
	// phenomenon worth its own counter, but folding it into the size
	// histogram drags the reported batch size toward zero.
	if planSize > 0 {
		metrics.PlanSize.Observe(float64(planSize))
	}
	if accepted > 0 {
		metrics.Assignments.Add(float64(accepted))
	}
	if rejected > 0 {
		metrics.PlanRejections.Add(float64(rejected))
	}
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.plansSubmitted++
	p.assignmentsAccepted += accepted
	p.assignmentsRejected += rejected
	if planSize > p.largestPlan {
		p.largestPlan = planSize
	}
}

// Stats reports the counters Day 11 turns into Prometheus metrics.
func (p *Planner) Stats() (plans, accepted, rejected, largest int) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.plansSubmitted, p.assignmentsAccepted, p.assignmentsRejected, p.largestPlan
}
