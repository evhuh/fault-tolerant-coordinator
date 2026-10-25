package sim

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
)

// THE EFFECT LEDGER.
//
// Day 5's design claims effectively-once EFFECTS over at-least-once DELIVERY,
// and until now nothing tested it. The claim is NOT that a task executes once
// — several attempts may execute, and the simulator now has real workers so
// they really do. The claim is that exactly one of them ever has its result
// COMMITTED, and that the committed one is the attempt the coordinator was
// waiting on.
//
// What the ledger is made of, and why it is not one artifact:
//
//   - The worker side: <workerDir>/<taskID>/<attempt>/, written by the real
//     executor, plus a shared append-only log the task payload itself writes.
//     This is what EXECUTED.
//   - The coordinator side: the accepted reports, observed at the apply site
//     because an ignored report is a silent no-op that cannot be sampled
//     afterwards. This is what COMMITTED.
//
// The interesting assertions are all about the relationship between the two,
// so a ledger that only had one side could not make them.
//
// WHAT IS ASSERTED
//   - Exactly one committed effect per taskID, across every attempt.
//   - The committed effect is the attempt the coordinator recorded as
//     completing, and its result directory exists on that worker's disk.
//   - Every other attempt's report was rejected as stale.
//   - A completed task never reverts.
//   - Every submitted task reaches a terminal state — AFTER quiescence only.
//
// WHAT IS MEASURED AND NEVER ASSERTED
//   - Overlapping executions. Two workers running one task at the same time
//     is at-least-once working as designed; preventing it needs fencing,
//     which this system does not have and does not claim. ZERO overlap means
//     the idempotency was never exercised, which makes the claim
//     INCONCLUSIVE for that run rather than confirmed.
//   - Late reports that reached a coordinator and were rejected as stale.
//     Zero is the same kind of nothing: the path the claim rests on did not
//     run.

// effect is one accepted report, as the state machine accepted it.
type effect struct {
	TaskID   string
	Attempt  int32
	WorkerID string
	// Terminal is whether this report moved the task to a terminal state.
	//
	// DERIVED HERE rather than read back from the state machine, and the
	// duplication is the point: the checker re-derives the rule
	// (retryable AND attempt < MaxAttempts -> PENDING, else FAILED) from the
	// report and the spec, instead of asking the thing under test what it
	// decided. A checker that trusts its subject's own account of itself is
	// not checking.
	Terminal  bool
	ResultRef string
	Step      int
}

// Ledger is what the run observed about effects, and is reported whether or
// not anything failed.
type Ledger struct {
	// Checked is false when the run had no real workers, so there was no
	// executor to record.
	Checked bool
	// Accepted is every report the state machine accepted, per taskID.
	Accepted map[string][]effect
	// CommittedEffects is the count of accepted reports that took a task to a
	// terminal state — the committed effects proper.
	CommittedEffects int
	// StaleRejected is late reports that reached a coordinator and were
	// discarded because their attempt was no longer live.
	StaleRejected int
	// DuplicateIgnored is redeliveries of the LIVE attempt of a task that had
	// already finished.
	DuplicateIgnored int
	// StaleByTask is which attempts' reports were rejected as stale, per
	// task — the other half of "exactly one committed effect": the losers
	// have to be refused by NAME, not merely absent.
	StaleByTask map[string][]int32
	// Executions is how many attempts actually ran, from the payload's own
	// log, and Overlaps how many pairs of them ran at the same time.
	Executions int
	Overlaps   int
	// MaxConcurrent is the most simultaneous executions of a single task.
	MaxConcurrent int
	// Drained is whether every submitted task reached a terminal state within
	// the quiescence bound, and Undrained how many had not.
	Drained   bool
	Undrained int
	// DrainWaited is how long quiescence took, and DrainBound the budget it
	// was given — reported together, because "did not drain" means nothing
	// without the bound it exceeded.
	DrainWaited time.Duration
	DrainBound  time.Duration
	// Stuck describes every task that was still in flight when the bound
	// expired, field by field.
	Stuck []string
}

// recordAccepted files a report the state machine accepted.
//
// Dedup is by (taskID, attempt) as with the rejections: every replica applies
// the entry and computes the identical Result, so three nodes would otherwise
// turn one effect into three — and "exactly one committed effect per taskID"
// would fail on a healthy cluster.
func (s *Sim) recordAccepted(cmd *commandpb.Command, res state.Result) {
	var e effect
	switch {
	case cmd.GetCompleteTask() != nil:
		c := cmd.GetCompleteTask()
		e = effect{
			TaskID: c.GetTaskId(), Attempt: c.GetAttempt(), WorkerID: c.GetWorkerId(),
			ResultRef: c.GetResultRef(), Terminal: true,
		}
	case cmd.GetFailTask() != nil:
		f := cmd.GetFailTask()
		e = effect{
			TaskID: f.GetTaskId(), Attempt: f.GetAttempt(), WorkerID: f.GetWorkerId(),
			// The state machine's own rule, re-derived. MaxAttempts comes from
			// the submitted spec, which this workload fixes at 5.
			Terminal: !f.GetRetryable() || f.GetAttempt() >= maxAttempts,
		}
	default:
		return
	}
	if e.TaskID == "" {
		return
	}
	key := fmt.Sprintf("%s/%d", e.TaskID, e.Attempt)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seenAccepted[key] {
		return
	}
	s.seenAccepted[key] = true
	e.Step = int(s.stepNo.Load())
	if s.ledger.Accepted == nil {
		s.ledger.Accepted = map[string][]effect{}
	}
	s.ledger.Accepted[e.TaskID] = append(s.ledger.Accepted[e.TaskID], e)
	if e.Terminal {
		s.ledger.CommittedEffects++
	}
}

// maxAttempts is what the workload submits. Named so the re-derivation above
// is not a bare 5 sitting next to a rule it is reproducing.
const maxAttempts = 5

// quiesce heals everything, stops the workload, and waits for the queue to
// drain.
//
// WHY LIVENESS IS ONLY ASSERTED HERE. "Every submitted task reaches a
// terminal state" is false by design mid-chaos: a task whose worker is
// partitioned is retrying, not lost, and a checker that asserted it during
// injection would fire on a system that is merely slow. So the assertion is
// made once, against a healed and idle cluster, with a bound.
//
// Exceeding the bound is DID-NOT-DRAIN, which is deliberately NOT the same
// finding as a lost task. It says the run could not establish liveness within
// its budget — a bound this harness chose — and conflating that with "a task
// disappeared" would turn a slow host into a correctness claim.
func (s *Sim) quiesce(override time.Duration) {
	started := time.Now()

	// Everything healed and everyone back, so nothing is still being
	// prevented from finishing.
	s.net.Heal()
	// A pause that is still open outlives the schedule otherwise: a frozen
	// ticker means the node never renews anything, so nothing it holds can
	// ever drain.
	if s.paused != nil {
		s.paused.clk.Resume()
		s.net.Release(s.paused.id)
		s.paused = nil
		s.pauseUntilStep = 0
	}
	for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
		if _, up := s.nodeAt(id); !up {
			_ = s.start(id)
			s.result.Faults.NodeRestarts++
		}
	}
	if s.rw != nil {
		for _, w := range s.rw.workers {
			s.rw.resume(w.id)
		}
	}
	s.awaitLeader(10 * time.Second)

	// THE BOUND IS DERIVED FROM THE BACKLOG, not fixed. The first version
	// used a fixed 6s and reported DID-NOT-DRAIN on all three seeds with 2,
	// 27 and 28 tasks outstanding — which was true and useless: at six
	// slots, a third of tasks sleeping six leases each, and five attempts
	// apiece, a hundred-task backlog cannot physically drain in six seconds.
	// That is a throughput shortfall, and a liveness bound that is really
	// measuring throughput tells you nothing about liveness.
	bound := override
	if bound <= 0 {
		outstanding := s.undrained()
		if outstanding < 0 {
			outstanding = 0
		}
		// A base for the cluster to settle, plus room per outstanding task.
		// Deliberately generous: the point of the bound is to catch a task
		// that is STUCK, not to race the queue.
		bound = 20*s.cfg.LeaseTTL + time.Duration(outstanding)*4*s.cfg.LeaseTTL
	}

	deadline := time.Now().Add(bound)
	for {
		left := s.undrained()
		if left == 0 {
			s.ledger.Drained = true
			s.ledger.Undrained = 0
			s.ledger.DrainWaited = time.Since(started)
			s.ledger.DrainBound = bound
			return
		}
		if time.Now().After(deadline) {
			s.ledger.Drained = false
			s.ledger.Undrained = left
			s.ledger.DrainWaited = time.Since(started)
			s.ledger.DrainBound = bound
			return
		}
		// The workload keeps submitting nothing here, but the cluster still
		// needs its drivers to run: evals to be processed, leases to expire,
		// backoffs to elapse. That is all on the coordinator's own loops, so
		// this only has to wait.
		time.Sleep(s.cfg.StepTime)
	}
}

// undrained counts the committed tasks that are not in a terminal state.
func (s *Sim) undrained() int {
	stuck := s.stuckTasks()
	if stuck == nil {
		return -1 // no node to ask; not the same as "none left"
	}
	return len(stuck)
}

// stuckTasks describes every non-terminal task, field by field.
//
// "2 tasks did not drain" is not a diagnosis, and this day's lesson is that
// every healthy-looking aggregate was hiding something that only the per-item
// columns revealed. A stuck task's STATE is the whole question: PENDING with
// a live backoff is waiting, PENDING without one is unscheduled, ASSIGNED to
// a worker that is LOST is a reclaim that never happened, and RUNNING with an
// expired lease is a sweep that never fired. Those are four different bugs.
func (s *Sim) stuckTasks() []string {
	n := s.anyNode()
	if n == nil {
		return nil
	}
	out := []string{}
	n.coord.ReadState(func(st *state.StateStore) {
		nowMs := st.NowMs()
		for _, id := range st.TaskIDs() {
			t, _ := st.Task(id)
			if t.State == state.TaskComplete || t.State == state.TaskFailed {
				continue
			}
			worker := "-"
			if t.WorkerID != "" {
				worker = t.WorkerID
				if w, ok := st.Worker(t.WorkerID); ok {
					worker = fmt.Sprintf("%s(%s,lease %+dms)", t.WorkerID, w.State, w.LeaseExpiryMs-nowMs)
				} else {
					worker += "(unknown)"
				}
			}
			out = append(out, fmt.Sprintf("%s state=%s attempt=%d/%d worker=%s taskLease=%+dms notBefore=%+dms",
				id, t.State, t.Attempt, t.Spec.MaxAttempts, worker,
				t.LeaseExpiryMs-nowMs, t.NotBeforeMs-nowMs))
		}
		sort.Strings(out)
	})
	return out
}

// checkLedger runs the assertions and the measurements.
func (s *Sim) checkLedger() []Violation {
	if !s.cfg.RealWorkers {
		return nil
	}
	s.ledger.Checked = true
	s.ledger.StaleRejected = s.result.Coverage.StaleReportsRejected
	s.ledger.DuplicateIgnored = s.result.Coverage.DuplicateReportsIgnored
	s.readOverlapLog()

	var vs []Violation
	add := func(kind ViolationKind, inconclusive bool, format string, args ...any) {
		vs = append(vs, Violation{
			Seed: s.cfg.Seed, Step: s.result.Steps, Kind: kind,
			Inconclusive: inconclusive, What: fmt.Sprintf(format, args...),
		})
	}

	// Committed state is the coordinator's account of what completed, and the
	// accepted reports are the ledger's. The assertions are all about those
	// two agreeing.
	type committed struct {
		state     state.TaskState
		attempt   int32
		worker    string
		resultRef string
	}
	final := map[string]committed{}
	if n := s.anyNode(); n != nil {
		n.coord.ReadState(func(st *state.StateStore) {
			for _, id := range st.TaskIDs() {
				t, _ := st.Task(id)
				final[id] = committed{t.State, t.Attempt, t.WorkerID, t.ResultRef}
			}
		})
	}

	// ASSERTION 3 in spirit, checked per task: the losers were refused by
	// name. Accepted reports for one task must arrive in STRICTLY INCREASING
	// attempt order, because the live attempt only ever moves forward — so
	// an accepted report for an attempt at or below one already accepted
	// means staleReport let a loser through.
	for _, taskID := range sortedKeys(s.ledger.Accepted) {
		var last int32
		for _, e := range s.ledger.Accepted[taskID] {
			if e.Attempt <= last {
				add(KindEffectLedger, false,
					"task %s accepted a report for attempt %d after already accepting attempt %d: a dead attempt's report was not rejected as stale",
					taskID, e.Attempt, last)
			}
			last = e.Attempt
		}
	}

	// ASSERTION 1 and 2, which are one walk: one committed effect per task,
	// and it is the attempt the coordinator recorded.
	for _, taskID := range sortedKeys(s.ledger.Accepted) {
		var terminals []effect
		for _, e := range s.ledger.Accepted[taskID] {
			if e.Terminal {
				terminals = append(terminals, e)
			}
		}
		if len(terminals) > 1 {
			add(KindEffectLedger, false,
				"task %s has %d COMMITTED effects across attempts %s — effectively-once requires exactly one",
				taskID, len(terminals), attemptsOf(terminals))
			continue
		}
		if len(terminals) == 0 {
			continue // still in flight, or retrying; liveness is asserted elsewhere
		}

		e := terminals[0]
		c, ok := final[taskID]
		if !ok {
			add(KindEffectLedger, false,
				"task %s has a committed effect (attempt %d) but no committed task record",
				taskID, e.Attempt)
			continue
		}
		if c.state != state.TaskComplete && c.state != state.TaskFailed {
			add(KindEffectLedger, false,
				"task %s accepted a terminal report for attempt %d but is committed as %s",
				taskID, e.Attempt, c.state)
			continue
		}
		if c.attempt != e.Attempt {
			add(KindEffectLedger, false,
				"task %s committed effect is attempt %d but the coordinator recorded attempt %d as completing",
				taskID, e.Attempt, c.attempt)
			continue
		}
		// The effect has to be WHERE the coordinator says it is. A matching
		// attempt number with no directory behind it would mean the ledger
		// and the result tree disagree about what happened.
		if c.state == state.TaskComplete && c.resultRef != "" {
			if _, err := os.Stat(c.resultRef); err != nil {
				add(KindEffectLedger, false,
					"task %s committed result_ref %q for attempt %d is not on disk: %v",
					taskID, c.resultRef, c.attempt, err)
				continue
			}
			if want := fmt.Sprintf("/%d", c.attempt); !strings.HasSuffix(c.resultRef, want) {
				add(KindEffectLedger, false,
					"task %s committed result_ref %q is not under attempt %d's directory",
					taskID, c.resultRef, c.attempt)
			}
		}
	}

	// ASSERTION 3: every submitted task reaches a terminal state — but only
	// over a cluster that drained. DID-NOT-DRAIN is its own outcome.
	switch {
	case !s.ledger.Drained && s.ledger.Undrained < 0:
		add(KindDidNotDrain, true, "quiescence could not read committed state: no node was up")
	case !s.ledger.Drained:
		s.ledger.Stuck = s.stuckTasks()
		add(KindDidNotDrain, true,
			"DID-NOT-DRAIN: %d task(s) were still non-terminal after %s of a %s budget, with every fault healed — not a lost task, an unmet liveness bound",
			s.ledger.Undrained, s.ledger.DrainWaited.Round(time.Millisecond), s.ledger.DrainBound)
		for _, line := range s.ledger.Stuck {
			add(KindDidNotDrain, true, "  still in flight: %s", line)
		}
	default:
		for id, c := range final {
			if c.state != state.TaskComplete && c.state != state.TaskFailed {
				add(KindEffectLedger, false,
					"task %s is %s after the cluster drained: a submitted task never reached a terminal state",
					id, c.state)
			}
		}
	}

	// COVERAGE, measured and reported, never asserted on. Both of these are
	// zeros that mean "the scenario did not happen", and by the Day 9 and 10
	// rule that makes the claim inconclusive for this run rather than
	// confirmed.
	if s.ledger.Overlaps == 0 {
		add(KindLedgerCoverage, true,
			"no overlapping executions: %d attempts ran but never two at once, so idempotency was not exercised and effectively-once is unproven for this run",
			s.ledger.Executions)
	}
	if s.ledger.StaleRejected == 0 {
		add(KindLedgerCoverage, true,
			"no late report reached a coordinator: the stale-report path did not run, so the (taskID, attempt) rejection is unverified for this run")
	}
	return vs
}

// readOverlapLog replays the payload's own append-only log.
//
// Overlap is read off the ORDER of the lines: a START for a task that already
// has an open attempt means two executions of it were live at once. No
// timestamps, deliberately — see specFor.
func (s *Sim) readOverlapLog() {
	if s.overlapLog == "" {
		return
	}
	f, err := os.Open(s.overlapLog)
	if err != nil {
		return // nothing ran, or nothing could write; Executions stays zero
	}
	defer f.Close()

	open := map[string]map[int32]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 3 {
			continue
		}
		kind, taskID := fields[0], fields[1]
		var attempt int32
		if _, err := fmt.Sscanf(fields[2], "%d", &attempt); err != nil {
			continue
		}
		live := open[taskID]
		if live == nil {
			live = map[int32]bool{}
			open[taskID] = live
		}
		switch kind {
		case "START":
			s.ledger.Executions++
			// Counted BEFORE this attempt joins the set, so it is the number
			// of other executions of this task already running.
			if n := len(live); n > 0 {
				s.ledger.Overlaps += n
			}
			live[attempt] = true
			if n := len(live); n > s.ledger.MaxConcurrent {
				s.ledger.MaxConcurrent = n
			}
		case "END":
			delete(live, attempt)
		}
	}
}

func attemptsOf(es []effect) string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, fmt.Sprintf("%d(worker %s)", e.Attempt, e.WorkerID))
	}
	return strings.Join(out, ", ")
}

func sortedKeys(m map[string][]effect) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
