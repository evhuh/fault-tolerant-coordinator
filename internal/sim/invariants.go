package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
)

// Invariants checked after every fault step.
//
// Day 10 grows this into the full checker over an effect ledger. What is here
// is the subset that can be read straight off coordinator state, and the
// point of checking it EVERY STEP is attribution: a violation names the seed
// and the step, which is the difference between a bug you can reproduce and
// one you can only describe.
//
// Everything here must hold MID-CHAOS, on whatever node is currently leading.
// Properties that only hold once the cluster has converged go in
// checkConverged instead — asserting those during a partition would just be
// wrong, and a test that fails for being correct is worse than no test.

func (s *Sim) checkInvariants(step int) []Violation {
	lead := s.leader()
	if lead == nil {
		return nil // no leader right now; nothing authoritative to check
	}

	var vs []Violation
	add := func(format string, args ...any) {
		vs = append(vs, Violation{Seed: s.cfg.Seed, Step: step, What: fmt.Sprintf(format, args...)})
	}

	// A stalled Ready loop is a VIOLATION, not a log line. It means something
	// on that goroutine blocked on work the goroutine itself serves, which
	// self-heals after a timeout and leaves only an unexplained election
	// behind. Checked across every node, not just the leader: a follower's
	// loop stalling is the same bug.
	for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
		n, up := s.nodeAt(id)
		if !up {
			continue
		}
		for _, d := range n.coord.Stalls() {
			add("node %d Ready loop stalled %s — a blocking call on the loop's own goroutine",
				id, d.Round(time.Millisecond))
		}
	}

	// The clock must be ADVANCING. Dropped ticks are tolerated by design, so
	// a node that fails every single one looks perfectly healthy while its
	// state-machine clock is frozen — no lease ever expires, nothing is ever
	// reclaimed, and every check above still passes.
	//
	// NO BYZANTINE DELIVERIES. Raft assumes messages arrive intact or not at
	// all, so a mangled frame that decoded and reached Step is outside the
	// fault model this run's results are claimed against. If any got
	// through, a violation found here could be a harness artifact rather
	// than a bug — so the run is reported INCONCLUSIVE rather than passing
	// or failing on evidence it cannot support.
	if n := s.net.Stats().CorruptReachedStep; n > 0 {
		vs = append(vs, Violation{
			Seed: s.cfg.Seed, Step: step, Inconclusive: true,
			What: fmt.Sprintf(
				"%d corrupted frames were delivered to raft (DeliverCorrupt is on); "+
					"this run is outside raft's fault model and cannot support a correctness claim", n),
		})
	}

	// Calibrated on a STREAK, not a single step. Ticks proposed into a
	// partition or across a leadership change legitimately fail, so one
	// stalled step is ordinary; several in a row with a leader present is
	// not. At ~3 ticks per step, this is roughly 15 consecutive missed ticks.
	//
	// A CUT-OFF leader is exempt, and Day 9 is what made that necessary. The
	// streak was calibrated against partitions that the next quiet step
	// healed; held leader partitions keep a minority leader in place for
	// several steps, and a minority leader's clock is frozen because its
	// ticks cannot commit. That is the system behaving correctly — the
	// frozen clock is the SYMPTOM of a working quorum rule — and firing here
	// would mean the harness reporting a violation for an induced fault
	// doing exactly what it was induced to do.
	//
	// The streak is reset rather than merely not reported, so the steps
	// either side of the partition are not stitched into one false streak.
	switch nowMs := s.stateClock(lead); {
	case s.net.IsPartitioned(lead.id):
		s.clockFrozenSteps = 0
		s.lastClock = nowMs
	case nowMs == s.lastClock && s.lastClock > 0:
		s.clockFrozenSteps++
		if s.clockFrozenSteps >= frozenClockSteps {
			add("leader's state-machine clock stuck at %d for %d consecutive steps "+
				"(%d tick failures) — ticks are being dropped silently, so no lease will ever expire",
				nowMs, s.clockFrozenSteps, lead.coord.TickFailures())
		}
	default:
		s.clockFrozenSteps = 0
		s.lastClock = nowMs
	}

	lead.coord.ReadState(func(st *state.StateStore) {
		// A task holds at most one worker, and only while it is live. The
		// coordinator-side version of "never running on two workers": the
		// state machine makes it structurally true, so a failure here means
		// the state machine itself is broken.
		for _, id := range st.TaskIDs() {
			t, _ := st.Task(id)
			switch t.State {
			case state.TaskAssigned, state.TaskRunning:
				if t.WorkerID == "" {
					add("task %s is %s with no worker", id, t.State)
				}
				if t.LeaseExpiryMs == 0 {
					add("task %s is %s with no lease", id, t.State)
				}
			case state.TaskPending:
				if t.WorkerID != "" {
					add("task %s is PENDING but still held by %s", id, t.WorkerID)
				}
			}

			// The attempt budget is never exceeded. Every path back to
			// PENDING respects it; a violation means one of them stopped.
			if t.Attempt > t.Spec.MaxAttempts {
				add("task %s attempt %d exceeds max %d", id, t.Attempt, t.Spec.MaxAttempts)
			}

			// A completed task carries the evidence of its completion.
			if t.State == state.TaskComplete && t.ResultRef == "" {
				add("task %s is COMPLETE with no result_ref", id)
			}

			// Every task belongs to a job that exists.
			if _, ok := st.Job(t.JobID); !ok {
				add("task %s references missing job %s", id, t.JobID)
			}
		}

		// A job's derived state agrees with its tasks. This is the one most
		// likely to catch a real bug, because job state is computed rather
		// than stored and a missed recompute is invisible otherwise.
		for _, jid := range st.JobIDs() {
			j, _ := st.Job(jid)
			allTerminal, anyFailed := true, false
			for _, tid := range j.TaskIDs {
				t, ok := st.Task(tid)
				if !ok {
					add("job %s lists missing task %s", jid, tid)
					continue
				}
				if t.State != state.TaskComplete && t.State != state.TaskFailed {
					allTerminal = false
				}
				if t.State == state.TaskFailed {
					anyFailed = true
				}
			}
			switch {
			case allTerminal && anyFailed && j.State != state.JobFailed:
				add("job %s has a failed task but is %s", jid, j.State)
			case allTerminal && !anyFailed && j.State != state.JobComplete:
				add("job %s is fully complete but is %s", jid, j.State)
			case !allTerminal && (j.State == state.JobComplete || j.State == state.JobFailed):
				add("job %s is %s but still has live tasks", jid, j.State)
			}
		}
	})
	return vs
}

// checkConverged runs once at the end, after healing and restarting
// everything.
//
// These are the properties that are allowed to be false mid-partition and
// must be true once the cluster settles. Asserting them during chaos would
// fail the run for behaving correctly.
func (s *Sim) checkConverged() []Violation {
	var vs []Violation
	add := func(format string, args ...any) {
		vs = append(vs, Violation{Seed: s.cfg.Seed, Step: s.result.Steps, What: fmt.Sprintf(format, args...)})
	}

	// WAIT for a stable leader rather than sampling at one instant.
	//
	// leader() requires exactly one, and during an election there are zero —
	// so a single-instant check fails whenever the restarts this function
	// just performed trigger one, which is most of the time. That is the
	// harness being unable to see, not the cluster being broken.
	//
	// A genuinely leaderless cluster still fails here; it just has to stay
	// that way for the whole window.
	lead := s.awaitLeader(15 * time.Second)
	if lead == nil {
		add("no leader 15s after healing and restarting every node")
		return vs
	}

	// REPLICAS AGREE AT THE SAME LOG POSITION.
	//
	// The qualifier is the whole assertion. Two replicas at DIFFERENT applied
	// indices legitimately hold different state — the Tick proposer commits
	// constantly, so a node one entry behind is normal, not broken. Comparing
	// them blind reports a divergence on every healthy run, which is exactly
	// what the first version of this check did.
	//
	// So: sample (appliedIndex, state) from every live node repeatedly, and
	// compare only where two nodes were seen at the SAME index. A mismatch
	// there means apply stopped being a pure function of (state, command),
	// which is the most serious bug this harness can find.
	diffs, comparisons := s.sampleDivergence(2 * time.Second)
	for _, d := range diffs {
		add("%s", d)
	}
	// A run that structurally compared almost nothing must not pass green.
	if min := s.cfg.MinComparisons; min > 0 && comparisons < min {
		vs = append(vs, Violation{
			Seed: s.cfg.Seed, Step: s.result.Steps, Inconclusive: true,
			What: fmt.Sprintf(
				"replica agreement made only %d cross-node comparisons (floor %d); "+
					"the run proved nothing about convergence", comparisons, min),
		})
	}

	// No task is stranded: nothing sits ASSIGNED or RUNNING forever against a
	// worker that is gone, and nothing is PENDING past its attempt budget.
	lead.coord.ReadState(func(st *state.StateStore) {
		for _, id := range st.TaskIDs() {
			t, _ := st.Task(id)
			if t.State == state.TaskPending && t.Attempt >= t.Spec.MaxAttempts {
				add("task %s is PENDING at the attempt cap (%d) and can never be dispatched", id, t.Attempt)
			}
		}
	})
	return vs
}

// sampleDivergence repeatedly reads every live node's (appliedIndex, state)
// and reports any index at which two nodes disagree.
//
// Sampling rather than synchronizing because there is no moment when every
// node is guaranteed to pause at the same index — the log keeps moving. Over
// enough samples the nodes pass through common indices, and those are the
// only comparisons that carry meaning.
func (s *Sim) sampleDivergence(within time.Duration) (diffs []string, comparisons int) {
	// node id -> applied index -> state hash
	seen := map[uint64]map[uint64]string{}
	deadline := time.Now().Add(within)

	for time.Now().Before(deadline) {
		for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
			n, up := s.nodeAt(id)
			if !up {
				continue
			}
			idx := n.coord.AppliedIndex()
			var enc []byte
			var err error
			n.coord.ReadState(func(st *state.StateStore) { enc, err = st.Encode() })
			if err != nil || idx == 0 {
				continue
			}
			// Re-read the index AFTER encoding: if it moved, the snapshot
			// straddles two positions and cannot be attributed to either.
			if n.coord.AppliedIndex() != idx {
				continue
			}
			sum := sha256.Sum256(enc)
			if seen[id] == nil {
				seen[id] = map[uint64]string{}
			}
			seen[id][idx] = hex.EncodeToString(sum[:8])
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Compare every pair at every shared index, in sorted order so a failure
	// message is stable run to run.
	var out []string //nolint:prealloc
	ids := make([]uint64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var shared int
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			a, b := ids[i], ids[j]
			idxs := make([]uint64, 0, len(seen[a]))
			for idx := range seen[a] {
				if _, ok := seen[b][idx]; ok {
					idxs = append(idxs, idx)
				}
			}
			sort.Slice(idxs, func(x, y int) bool { return idxs[x] < idxs[y] })
			for _, idx := range idxs {
				shared++
				if seen[a][idx] != seen[b][idx] {
					out = append(out, fmt.Sprintf(
						"nodes %d and %d DIVERGED at applied index %d (%s vs %s)",
						a, b, idx, seen[a][idx], seen[b][idx]))
				}
			}
		}
	}

	return out, shared
}

// recordCoverage samples committed state for evidence that each path ran.
//
// Sampled rather than counted at the source because the interesting paths are
// in the STATE MACHINE, not the workload: a task can be redispatched by lease
// reclaim without any worker reporting anything, and the session sweep has no
// caller at all — Tick does it.
//
// Called between steps on the Run goroutine, so the maps need no lock.
func (s *Sim) recordCoverage() {
	lead := s.leader()
	if lead == nil {
		return
	}

	// Under s.mu because this now runs from the background sampler as well
	// as from the Run goroutine between steps.
	s.mu.Lock()
	defer s.mu.Unlock()

	lead.coord.ReadState(func(st *state.StateStore) {
		now := st.NowMs()
		for _, id := range st.TaskIDs() {
			t, _ := st.Task(id)
			if t.Attempt > 1 {
				s.seenRetried[id] = true
			}
			if t.NotBeforeMs > now {
				s.seenBackoff[id] = true
			}
			if t.State == state.TaskFailed {
				s.seenFailed[id] = true
			}
		}

		// A DROP in the session count means the Tick TTL sweep removed
		// something. Counting it this way rather than instrumenting the state
		// machine keeps apply free of bookkeeping that is only for tests.
		n := st.SessionCount()
		if s.lastSessions >= 0 && n < s.lastSessions {
			s.result.Coverage.SessionsExpired += s.lastSessions - n
		}
		s.lastSessions = n
	})

	s.result.Coverage.TasksRedispatched = len(s.seenRetried)
	s.result.Coverage.TasksBackedOff = len(s.seenBackoff)
	s.result.Coverage.TasksTerminalFail = len(s.seenFailed)
}

// sampleCoverage polls for coverage continuously until ctx is done.
//
// A live retry backoff is the one thing here that cannot be observed after
// the fact: NotBeforeMs is CLEARED the moment the Tick sweep wakes the task,
// so a task that backed off and resumed leaves no trace in committed state.
// Sampling between steps therefore measures luck — at a 25ms base backoff
// against steps that take hundreds of milliseconds, two looks per step miss
// it almost every time, and the aggregate across four seeds sat at 0-5,
// which is a coverage assertion that fails on timing rather than on a
// regression. Polling fast enough to be shorter than the shortest backoff
// makes the number mean what it says.
func (s *Sim) sampleCoverage(ctx context.Context) {
	iv := s.cfg.CoverageSampleInterval
	if iv <= 0 {
		return
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.recordCoverage()
		}
	}
}

// frozenClockSteps is how many consecutive steps a leader's clock may fail to
// advance before that counts as broken rather than unlucky.
const frozenClockSteps = 6

// stateClock reads a node's state-machine clock.
func (s *Sim) stateClock(n *node) int64 {
	var ms int64
	n.coord.ReadState(func(st *state.StateStore) { ms = st.NowMs() })
	return ms
}
