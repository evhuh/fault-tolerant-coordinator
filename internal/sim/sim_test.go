package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/evazheng/fault-tolerant-coordinator/internal/simnet"
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
)

func dirsFor(t *testing.T, n int) map[uint64]string {
	t.Helper()
	out := map[uint64]string{}
	for i := 1; i <= n; i++ {
		out[uint64(i)] = t.TempDir()
	}
	return out
}

// EVERY TEST ASSERTS ONLY ITS OWN PROPERTY.
//
// Run checks several unrelated things, so a bare "err != nil" turns a test
// red for a reason it was not written to detect — and then reports the wrong
// cause. When the linearizability check landed, the comparison-floor test
// announced "a healthy run failed its own floor" over a run whose floor was
// fine, and the schedule-reproducibility test went red over a violation that
// has nothing to do with schedules. Both sent the reader to the wrong code.
//
// So the tests below select on KIND. Only the dedicated linearizability tests
// may fail on an illegal history: the stale-read finding under ReadLocal, its
// ReadIndexBarrier counterpart, and TestSeedSweep, which is kept deliberately
// as a broad detector.

// ownViolations drops the violations a test does not own, so what remains is
// what that test is actually about.
func ownViolations(res Result, ignore func(ViolationKind) bool) []Violation {
	var out []Violation
	for _, v := range res.Violations {
		if ignore(v.Kind) {
			continue
		}
		// A stall DOWNGRADED to inconclusive is the host's doing, not the
		// system's: the same run waited on its disk past its own election
		// timeout, so the duration this stall is measured against was never
		// valid. See downgradeStallsOnBrokenPremise. A stall that is still a
		// VIOLATION is kept, because that is the case worth catching.
		//
		// UNLESS THE RUN IS SUPPORTING A CLAIM. Under FTC_STRICT_STALLS — set
		// by release-check — an unattributable stall fails, because "we could
		// not tell" is not a basis for a release. The fsync gate runs first
		// so this should never trip; if it does, the host degraded mid-run
		// and the numbers are not quotable.
		if v.Inconclusive && v.Kind.IsStall() && !strictStalls() {
			continue
		}
		out = append(out, v)
	}
	return out
}

// notLinearizability is the usual filter: this test is about something else.
// Environment kinds are dropped alongside, since "the host's disk stalled"
// and "the run left the fault model" are nobody's property.
//
// LEDGER kinds too, since real workers landed. TestEffectLedger owns them,
// and without this the transport tests went red on
// "no overlapping executions" — a finding about a property they do not make a
// claim about, reported against a test that cannot act on it. That is the
// exact failure mode the each-test-asserts-only-its-own-property rule exists
// to prevent, arriving from a new direction.
func notLinearizability(k ViolationKind) bool {
	return k.IsLinearizability() || k.IsEnvironment() || k.IsLedger()
}

// onlyLinearizabilityCoverage reports whether the sole complaint was that the
// history could not have exposed a stale read.
func onlyLinearizabilityCoverage(res Result) bool {
	if len(res.Violations) == 0 {
		return false
	}
	for _, v := range res.Violations {
		if v.Kind != KindLinearizabilityCoverage {
			return false
		}
	}
	return true
}

// notMine is the same filter as an error predicate.
func notMine(err error) bool {
	var v Violation
	if !errors.As(err, &v) {
		return false
	}
	if v.Inconclusive && v.Kind.IsStall() && !strictStalls() {
		return true // the host broke the premise; see ownViolations
	}
	return v.Kind.IsLinearizability() || v.Kind.IsEnvironment() || v.Kind.IsLedger()
}

// inconclusiveOnly reports whether every violation recorded was an
// INCONCLUSIVE one, which is how the detector sweep tells "nothing was found"
// from "nothing could be looked at".
//
// Those are opposite claims and only one of them is a defect. A short run
// that never rolled a partition, a host whose fsyncs blew past the run's
// compressed timings, a run that left raft's fault model — none of them
// found anything wrong, and failing the sweep on them is how a detector
// becomes nondeterministically red.
func inconclusiveOnly(res Result) bool {
	if len(res.Violations) == 0 {
		return false
	}
	for _, v := range res.Violations {
		if !v.Inconclusive {
			return false
		}
	}
	return true
}

// strictStalls reports whether an UNATTRIBUTABLE stall should fail rather
// than be tolerated.
//
// Off for ordinary development, where a laptop's disk hiccup turning six
// tests red teaches nothing. On for release-check, where the run's whole
// purpose is to support a claim and "the host broke the premise" means the
// claim is unsupported rather than met.
func strictStalls() bool { return os.Getenv("FTC_STRICT_STALLS") == "1" }

// findKind returns the first violation of a kind, which is how a test asserts
// on the check it owns rather than on whatever failed first.
func findKind(res Result, kind ViolationKind) (Violation, bool) {
	for _, v := range res.Violations {
		if v.Kind == kind {
			return v, true
		}
	}
	return Violation{}, false
}

func quickConfig(seed int64, steps int) Config {
	cfg := DefaultConfig()
	cfg.Seed = seed
	cfg.Steps = steps
	cfg.StepTime = 60 * time.Millisecond
	cfg.Faults = simnet.Faults{DropRate: 0.02, MaxDelay: 2 * time.Millisecond}
	return cfg
}

// A short run must finish, and finish CLEAN. Anything that wedges shows up
// here as a test timeout with a full goroutine dump, which is how the
// leadership-revocation deadlock was found.
func TestShortRunCompletes(t *testing.T) {
	cfg := quickConfig(42, 10)
	res, err := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
	// This test is about completing without wedging, not about
	// linearizability; the dedicated tests own that.
	if err != nil && !notMine(err) {
		t.Fatalf("seed %d: %v", cfg.Seed, err)
	}
	if vs := ownViolations(res, notLinearizability); len(vs) != 0 {
		t.Errorf("violations: %v", vs)
	}
	t.Logf("seed=%d kills=%d restarts=%d partitions=%d heals=%d jobs=%d tasks=%d",
		res.Seed, res.Faults.NodeKills, res.Faults.NodeRestarts,
		res.Faults.Partitions, res.Faults.Heals, res.Jobs, res.Tasks)
}

// THE REPRODUCIBILITY CHECKPOINT: a seed reproduces the same fault schedule.
//
// Note precisely what is asserted — the SCHEDULE, not the execution. Two runs
// of one seed kill the same nodes at the same steps, partition the same
// nodes, and heal at the same points. They do not produce identical state:
// goroutine scheduling decides who wins each race, and no seeding changes
// that in Go.
//
// That weaker claim is still the one that matters. A failure you can put the
// same adversary back in front of is investigable; one you cannot is an
// anecdote.
func TestSameSeedReproducesTheFaultSchedule(t *testing.T) {
	const steps = 14
	run := func(seed int64) (string, FaultCounts) {
		cfg := quickConfig(seed, steps)
		var log bytes.Buffer
		cfg.Out = &log
		res, err := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
		// Schedule reproducibility is the only claim here. A linearizability
		// violation says nothing about whether the seed drove the same
		// adversary, and failing on one made this test red for seed 5678
		// over an entirely unrelated finding.
		if err != nil && !notMine(err) {
			t.Fatalf("seed %d: %v", seed, err)
		}
		// A RUN THAT ENDED EARLY LOGS A PREFIX, and comparing a prefix
		// against a complete log is not a reproducibility failure — it is a
		// different question entirely, reported as the wrong answer.
		//
		// This test did exactly that under load: the first run of seed 1234
		// stopped at step 5 on an INCONCLUSIVE host-io stall (the host's
		// fsync blew past the run's compressed timings), the second reached
		// step 9, and the output read "seed 1234 produced different fault
		// schedules" — which would send the next reader hunting for
		// nondeterminism in the fault RNG that is not there.
		//
		// notMine already keeps the environment kinds from failing the test;
		// what was missing is that a notMine violation can still TRUNCATE
		// the run, and the comparison has to know that.
		if res.Steps < steps {
			t.Skipf("seed %d ended at step %d of %d, so its schedule log is a prefix and the "+
				"checkpoint cannot be evaluated: %v", seed, res.Steps, steps, err)
		}
		return log.String(), res.Faults
	}

	scheduleA, countsA := run(1234)
	scheduleB, countsB := run(1234)

	if scheduleA != scheduleB {
		t.Errorf("seed 1234 produced different fault schedules\n--- first ---\n%s\n--- second ---\n%s",
			scheduleA, scheduleB)
	}
	if countsA.NodeKills != countsB.NodeKills ||
		countsA.Partitions != countsB.Partitions ||
		countsA.Heals != countsB.Heals ||
		countsA.NodeRestarts != countsB.NodeRestarts {
		t.Errorf("fault counts differ: %+v vs %+v", countsA, countsB)
	}
	if scheduleA == "" {
		t.Fatal("no faults were scheduled at all; the test proves nothing")
	}

	// A different seed must actually differ, or the assertion above is
	// satisfied by every run being empty.
	scheduleC, _ := run(5678)
	if scheduleC == scheduleA {
		t.Error("seeds 1234 and 5678 produced identical schedules; the seed is not driving anything")
	}
	t.Logf("seed 1234 schedule:\n%s", scheduleA)
}

// A sweep of seeds, each short. This is the shape the chaos checkpoint scales up: breadth
// across adversaries beats depth on one.
//
// Unlike every other test here, this one is a DETECTOR and fails on any kind
// of violation, linearizability included. It is the broad net; the tests
// above each own one property. Note what that costs: detection here is
// seed-and-timing dependent, since the seed reproduces the adversary and not
// the execution, so this catching something is informative and this passing
// is not a guarantee. The deterministic scenario tests are the reliable
// artifact.
// How wide the sweep is, and how much of it must have been able to find
// something for the result to count.
//
// Recalibrated after Covered was tightened: see onlyLinearizabilityCoverage.
// The counts below predate that change but the floor still holds, because a
// coverage-only complaint no longer disqualifies a run here.
//
// "CHECKED" throughout means invariants and convergence. The linearizability
// word is "COVERED" and belongs to the targeted sweeps alone.
//
// CALIBRATED, not chosen. Measured CHECKED counts over six sweeps of six
// 8-step seeds were 1, 4, 2, 2, 2, 4 — 15 of 36, a rate near 42%, with enough
// variance that one sweep managed a single conclusive run. At six seeds no
// floor above one is stable (a floor of two would fail roughly one sweep in
// six), so the sweep is WIDER rather than the floor lower: at sixteen seeds
// the expected conclusive count is near seven and a floor of three sits about
// two standard deviations below it. A first run at that width measured nine.
//
// The real remedy is more STEPS per run, not more seeds — 12-step runs are
// conclusive about 87% of the time against 47% here, because a short run
// often never rolls a leader partition at all. That is left to the chaos
// checkpoint, and this is a wide-and-shallow detector that says so.
var (
	seedSweepRuns       int64 = 16
	seedSweepMinChecked       = 3
)

func TestSeedSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("sweep is slow; -short runs the single-seed test only")
	}
	// NAMED "CHECKED", NOT "CONCLUSIVE".
	//
	// This sweep's property is that the per-step invariants and the
	// convergence check ran and found nothing. That is a different claim
	// from the linearizability notion of COVERED — a history containing a
	// read that could have been stale — and the two were briefly sharing one
	// word, which is how a general fault sweep gets quoted as
	// linearizability evidence. It is not, and under the shipped
	// configuration it structurally cannot be: the window never opens by
	// chance. Only the targeted sweeps speak to linearizability.
	var checked, unchecked, illegal int
	// IN-PROCESS DRIFT, LOGGED PER SEED.
	//
	// This sweep runs sixteen clusters back to back in ONE process, so a
	// cluster that does not fully shut down leaves its goroutines and timers
	// behind and the later seeds run against a busier runtime. That is not a
	// slow host, it is this test leaking, and the two have identical
	// symptoms: an apply stall late in the sweep, classified as a blocking
	// call because no disk stall accompanied it.
	//
	// Seed 4 failed on a ready-loop-stall at 2 of 9 runs with no mechanism
	// in the code, and a rising series here would say the cause is in
	// process rather than on the machine. Logged rather than asserted,
	// because the threshold at which drift matters is not yet known.
	var goroutineSeries, heapSeries []string
	for seed := int64(1); seed <= seedSweepRuns; seed++ {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		goroutineSeries = append(goroutineSeries, fmt.Sprintf("%d", runtime.NumGoroutine()))
		heapSeries = append(heapSeries, fmt.Sprintf("%.0f", float64(ms.HeapInuse)/(1<<20)))
		cfg := quickConfig(seed, 8)
		res, err := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
		if res.Lin.Verdict == porcupine.Illegal {
			illegal++
		}
		// Inconclusive outcomes are REPORTED, not failed.
		//
		// LINEARIZABILITY COVERAGE IS NOT THIS TEST'S PROPERTY. Once Covered
		// was tightened to require a targeted split-brain read, every run of
		// this sweep became coverage-inconclusive — 0 of 16 conclusive —
		// because under the shipped configuration that window essentially
		// never opens without an injected pause. That is a true statement
		// about the sweep and a useless one about the system: these runs
		// still exercised every invariant and the convergence check in full.
		//
		// So a coverage-inconclusive run still counts as conclusive HERE.
		// The linearizability claim belongs to the targeted tests, which
		// open the window deliberately and carry their own calibrated
		// floors.
		switch {
		case onlyLinearizabilityCoverage(res):
			// CHECKED for everything this test owns. The run checked
			// every invariant and converged; all it could not do was expose
			// a stale read, which is another test's job.
			checked++
		case inconclusiveOnly(res):
			unchecked++
			t.Logf("seed %d inconclusive, not a finding: %v", seed, res.Violations)
		case err != nil:
			t.Errorf("seed %d FAILED: %v  (reproduce: simulator -seed %d -steps 8 -v)",
				seed, err, seed)
		case len(res.Violations) > 0:
			t.Errorf("seed %d: %v", seed, res.Violations)
		default:
			checked++
		}
	}

	// A MINIMUM, not "at least one".
	//
	// "At least one" is the floor this project already rejected for
	// MinComparisons, and for the same reason: a sweep that managed a single
	// conclusive run out of many has barely looked, and passing it reports
	// evidence that does not exist.
	if checked < seedSweepMinChecked {
		t.Errorf("sweep of %d seeds fully CHECKED only %d (floor %d): %d checked, "+
			"%d unchecked, %d NOT LINEARIZABLE — too little of this sweep could have found "+
			"anything for its result to mean much. NB: 'checked' here means invariants and "+
			"convergence, never linearizability coverage",
			seedSweepRuns, checked, seedSweepMinChecked, checked, unchecked, illegal)
	}
	t.Logf("GOROUTINES at the start of each seed: %s", strings.Join(goroutineSeries, " "))
	t.Logf("HEAP IN USE (MB) at the start of each seed: %s", strings.Join(heapSeries, " "))
	t.Logf("sweep of %d seeds (invariants + convergence): %d checked, %d unchecked, %d illegal — "+
		"this is NOT linearizability evidence; see the targeted sweeps",
		seedSweepRuns, checked, unchecked, illegal)
}

// The invariant checker has to be able to FAIL, or a green run means nothing.
// This plants a contradiction directly in a node's state machine and asserts
// the checker notices.

func TestInvariantCheckerDetectsCorruptedState(t *testing.T) {
	cfg := quickConfig(99, 3)
	s := New(cfg, dirsFor(t, cfg.Nodes))
	defer s.shutdown()

	for i := 1; i <= cfg.Nodes; i++ {
		if err := s.start(uint64(i)); err != nil {
			t.Fatalf("start: %v", err)
		}
	}
	lead := s.awaitLeader(10 * time.Second)
	if lead == nil {
		t.Fatal("no leader")
	}
	s.registerWorkers()
	s.offerWork(1)
	time.Sleep(200 * time.Millisecond)

	if vs := s.checkInvariants(1); len(vs) > 0 {
		t.Fatalf("healthy cluster already violating: %v", vs)
	}

	// Corrupt it: mark a task PENDING while leaving a worker attached, which
	// no command path can produce.
	var corrupted bool
	lead.coord.ReadState(func(st *state.StateStore) {
		for _, id := range st.TaskIDs() {
			if tk, _ := st.Task(id); tk.State == state.TaskAssigned || tk.State == state.TaskRunning {
				st.CorruptForTesting(id)
				corrupted = true
				return
			}
		}
	})
	if !corrupted {
		t.Skip("no assigned task to corrupt in this run")
	}

	vs := s.checkInvariants(1)
	if len(vs) == 0 {
		t.Error("checker passed a state no command path can produce")
	}
	for _, v := range vs {
		if v.Seed != cfg.Seed || v.Step != 1 {
			t.Errorf("violation lost its attribution: %+v", v)
		}
	}
	t.Logf("checker caught: %v", vs)
}

// A run that structurally compared almost nothing must fail as INCONCLUSIVE,
// not pass green.
//
// "At least one comparison" was too weak: a heavily partitioned run can
// compare a handful of indices and come back clean, which looks like evidence
// about convergence and is not. The distinction matters — a violation says
// the system is broken, inconclusive says the harness could not see.
func TestImpossibleComparisonFloorIsInconclusiveNotAPass(t *testing.T) {
	cfg := quickConfig(2024, 4)
	cfg.MinComparisons = 1 << 30 // unreachable on purpose
	res, err := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())

	if err == nil {
		t.Fatal("a run that compared far below the floor passed")
	}
	// The floor violation specifically, not "whatever failed first": with an
	// unreachable floor a linearizability finding can sort ahead of it, and
	// then this test would be asserting on the wrong violation.
	v, ok := findKind(res, KindComparisonFloor)
	if !ok {
		t.Fatalf("no comparison-floor violation recorded; got %v", res.Violations)
	}
	if !v.Inconclusive {
		t.Errorf("recorded as a violation, want INCONCLUSIVE: %v", v)
	}
	if !strings.Contains(v.Error(), "INCONCLUSIVE") {
		t.Errorf("message does not distinguish itself: %q", v.Error())
	}
	t.Logf("%v", v)
}

// The default floor must be comfortably reachable, or every run fails.
// Measured on this config: healthy runs make 100–300 comparisons.
func TestDefaultComparisonFloorIsReachable(t *testing.T) {
	cfg := quickConfig(31337, 10)
	if cfg.MinComparisons == 0 {
		t.Fatal("default MinComparisons is 0 — the floor is disabled")
	}
	res, _ := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())

	// The FLOOR is this test's subject. It used to fail reporting "a healthy
	// run failed its own floor" over a run that made 115 comparisons and
	// failed its linearizability check instead.
	if v, ok := findKind(res, KindComparisonFloor); ok {
		t.Fatalf("a healthy run failed its own floor (%d): %v", cfg.MinComparisons, v)
	}
	for _, v := range ownViolations(res, notLinearizability) {
		if v.Inconclusive {
			t.Errorf("healthy run reported inconclusive: %v", v)
		}
	}
}

// COVERAGE IS PART OF THE RESULT. "Zero violations" is only evidence about
// code that executed, and before the workload failed any tasks the entire
// retry path was dark while every run came back green.
//
// This asserts the paths are reachable, not that they fire every time — a
// seed that happens not to fail a task is legitimate. The simulator prints
// these so a sweep that never reaches a path is visible.
func TestRunExercisesTheFailurePaths(t *testing.T) {
	var agg Coverage
	for seed := int64(100); seed < 104; seed++ {
		cfg := quickConfig(seed, 20)
		res, err := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
		// A violation does not invalidate the COVERAGE this run measured,
		// and coverage is this test's subject. Aborting the sweep on a
		// linearizability finding reported nothing about the paths already
		// exercised, which is the one thing a coverage test must not do.
		// Invariant and convergence failures still abort: those mean the
		// state the coverage was read from is not trustworthy.
		if err != nil && !notMine(err) {
			t.Fatalf("seed %d: %v", seed, err)
		}
		c := res.Coverage
		agg.ReportedOK += c.ReportedOK
		agg.ReportedRetryable += c.ReportedRetryable
		agg.ReportedTerminal += c.ReportedTerminal
		agg.TasksRedispatched += c.TasksRedispatched
		agg.TasksBackedOff += c.TasksBackedOff
		agg.TasksTerminalFail += c.TasksTerminalFail
		agg.SessionsExpired += c.SessionsExpired
		t.Logf("seed %d: ok=%d retryable=%d terminal=%d | redispatched=%d backed-off=%d FAILED=%d sessions-expired=%d",
			seed, c.ReportedOK, c.ReportedRetryable, c.ReportedTerminal,
			c.TasksRedispatched, c.TasksBackedOff, c.TasksTerminalFail, c.SessionsExpired)
	}

	for _, tc := range []struct {
		name string
		got  int
	}{
		{"successful executions", agg.ReportedOK},
		{"retryable failures reported", agg.ReportedRetryable},
		{"terminal failures reported", agg.ReportedTerminal},
		{"tasks redispatched (attempt > 1)", agg.TasksRedispatched},
		{"tasks observed backing off", agg.TasksBackedOff},
		{"tasks reaching FAILED", agg.TasksTerminalFail},
		{"dedup sessions expired by Tick", agg.SessionsExpired},
	} {
		if tc.got == 0 {
			t.Errorf("across 4 seeds, %s never happened — that path is unverified", tc.name)
		}
	}
	t.Logf("TOTAL across 4 seeds: %+v", agg)
}

// A blocking-call stall is not attributable in a run whose disk already
// broke the premise.
//
// Both judgements measure TIME against a compressed election timeout. A
// host-io stall says that timescale was not valid in this run, so a
// "stalled 2m43s in send" from the same run cannot be charged to the system
// — it is the pileup behind the disk. The chaos checkpoint measured exactly that pair: a
// schedule checkpoint failing on a 2m43s send-phase stall during a sweep
// whose other runs were recording 13-minute fsyncs.
//
// A deterministic unit test over the classifier rather than an attempt to
// reproduce a 13-minute fsync, which is not a thing a test can ask for.
func TestStallIsNotAttributableWhenTheHostBrokeThePremise(t *testing.T) {
	stall := Violation{Seed: 1, Step: 12, Kind: KindReadyLoopStall,
		What: "node 1 Ready loop stalled 2m43s in send (blocking call)"}
	hostIO := Violation{Seed: 1, Step: 3, Kind: KindHostIO, Inconclusive: true,
		What: "node 3 Ready loop waited 13m21s in persist (host I/O)"}

	// On its own it stays a violation: this is the case worth catching, and
	// the leadership-revocation deadlock presented exactly this way.
	only, hostIOCount, downgraded := downgradeStallsOnBrokenPremise([]Violation{stall})
	if len(only) != 1 || only[0].Inconclusive {
		t.Errorf("a stall in a healthy-disk run must stay a VIOLATION, got %+v", only)
	}
	if hostIOCount != 0 || downgraded != 0 {
		t.Errorf("counts = host-io %d downgraded %d, want 0 and 0", hostIOCount, downgraded)
	}

	// Alongside a host-io stall it is not attributable.
	both, hostIOCount, downgraded := downgradeStallsOnBrokenPremise([]Violation{stall, hostIO})
	if hostIOCount != 1 || downgraded != 1 {
		t.Errorf("counts = host-io %d downgraded %d, want 1 and 1 — an unattributable stall that is "+
			"not counted reads as a clean pass", hostIOCount, downgraded)
	}
	if len(both) != 2 {
		t.Fatalf("downgrade changed the violation count: %+v", both)
	}
	if !both[0].Inconclusive {
		t.Errorf("the stall is still reported as a defect over a run whose disk broke its own timings: %+v", both[0])
	}
	if !strings.Contains(both[0].What, "not attributable") {
		t.Errorf("the downgrade must say WHY, or the next reader re-investigates it: %q", both[0].What)
	}
	// The host-io finding itself is untouched — it is the evidence, not the
	// thing being excused.
	if both[1].Kind != KindHostIO || !both[1].Inconclusive {
		t.Errorf("host-io violation altered: %+v", both[1])
	}

	// Other kinds are never touched. An invariant failure is not a timing
	// judgement and a slow disk is no excuse for it.
	inv := Violation{Seed: 1, Step: 4, Kind: KindInvariant, What: "task x is RUNNING with no worker"}
	kept, _, _ := downgradeStallsOnBrokenPremise([]Violation{inv, hostIO})
	if kept[0].Inconclusive {
		t.Errorf("a state invariant was downgraded by a slow disk: %+v", kept[0])
	}
}

// ONE SEED, named by the environment, classified exactly as TestSeedSweep
// classifies it.
//
// It exists for an A/B: TestSeedSweep's seed 4 failed on a ready-loop-stall
// at 2 of 9 runs with no mechanism anyone could name, and the question is
// whether it fails in ISOLATION or only alongside the other fifteen seeds'
// load. Answering that needs the same code path as the sweep, not a
// differently-configured approximation — hence quickConfig(seed, 8) and the
// same switch below.
//
// Skipped unless FTC_ONE_SEED is set, so it costs nothing in the normal run.
func TestOneSeedFromEnv(t *testing.T) {
	raw := os.Getenv("FTC_ONE_SEED")
	if raw == "" {
		t.Skip("set FTC_ONE_SEED=<n> to run a single seed with the sweep's classification")
	}
	seed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("FTC_ONE_SEED=%q: %v", raw, err)
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("before seed %d: goroutines=%d heapInUse=%.0fMB",
		seed, runtime.NumGoroutine(), float64(ms.HeapInuse)/(1<<20))

	cfg := quickConfig(seed, 8)
	res, runErr := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())

	runtime.ReadMemStats(&ms)
	t.Logf("after seed %d: goroutines=%d heapInUse=%.0fMB",
		seed, runtime.NumGoroutine(), float64(ms.HeapInuse)/(1<<20))

	switch {
	case onlyLinearizabilityCoverage(res):
		t.Logf("seed %d: checked", seed)
	case inconclusiveOnly(res):
		t.Logf("seed %d inconclusive, not a finding: %v", seed, res.Violations)
	case runErr != nil:
		t.Errorf("seed %d FAILED: %v", seed, runErr)
	case len(res.Violations) > 0:
		t.Errorf("seed %d: %v", seed, res.Violations)
	default:
		t.Logf("seed %d: checked", seed)
	}
}
