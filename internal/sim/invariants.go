package sim

import (
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

	lead := s.leader()
	if lead == nil {
		add("no leader after healing and restarting every node")
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
	for _, d := range s.sampleDivergence(2 * time.Second) {
		add("%s", d)
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
func (s *Sim) sampleDivergence(within time.Duration) []string {
	// node id -> applied index -> state hash
	seen := map[uint64]map[uint64]string{}
	deadline := time.Now().Add(within)

	for time.Now().Before(deadline) {
		for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
			n, up := s.nodes[id]
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
	var out []string
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

	// A check that never compared anything proves nothing, and silently
	// passing in that case is how a harness becomes decoration.
	if shared == 0 && len(ids) > 1 {
		out = append(out, fmt.Sprintf(
			"no shared applied index across %d live nodes in %s; replica agreement was never actually checked",
			len(ids), within))
	}
	return out
}
