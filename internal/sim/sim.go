// Package sim runs a whole coordinator cluster in one process and attacks it
// on a schedule drawn from a single seed.
//
// # What a seed reproduces, precisely
//
// Every random decision in a run — which fault fires, when, against which
// node, which message is dropped, which id is minted — draws from ONE seeded
// RNG. So a given seed replays the same FAULT SCHEDULE.
//
// It does not replay the same execution. Go's goroutine scheduling, its
// network poller, and the OS decide who wins a race, and no amount of seeding
// changes that. This is seeded fault injection, NOT deterministic simulation
// testing: real DST needs control over the scheduler itself, which in Go
// means compiling to WASM and forking the runtime.
//
// The distinction matters because the honest claim is strong enough: a
// failing seed reproduces the same adversary, which is what makes a failure
// investigable. Claiming byte-identical replay would be false, and a false
// claim about verification is worse than no claim.
package sim

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/evazheng/fault-tolerant-coordinator/internal/clock"
	"github.com/evazheng/fault-tolerant-coordinator/internal/raftnode"
	"github.com/evazheng/fault-tolerant-coordinator/internal/server"
	"github.com/evazheng/fault-tolerant-coordinator/internal/simnet"
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
)

// Config describes one simulated run.
type Config struct {
	Seed  int64
	Nodes int

	// Steps is how many fault steps to take. Each step picks a fault, applies
	// it, lets the cluster run, then checks invariants.
	Steps    int
	StepTime time.Duration

	// Timings are compressed hard relative to production: the point is to
	// cover many lease expiries and elections per second of wall time.
	TickInterval  time.Duration
	ElectionTick  int
	LeaseTTL      time.Duration
	TickPeriod    time.Duration
	SnapshotEvery uint64
	// MinComparisons is how many real cross-node state comparisons the
	// convergence check must make before a run is allowed to pass.
	//
	// "At least one" is too weak: a heavily partitioned run can compare a
	// handful and still come back green, which looks like evidence and is
	// not. A run below this floor fails as INCONCLUSIVE — a distinct outcome
	// from a violation, because nothing was found wrong; nothing was looked
	// at.
	//
	// Healthy runs on this config measure 100–300, so 20 leaves wide margin.
	MinComparisons int
	// StatePolicy compresses the durations the STATE MACHINE measures.
	// Without this, a 10-minute session TTL and a 30-second backoff cap
	// against a sub-second run mean those paths never execute at all, and a
	// green run says nothing about them.
	StatePolicy state.Policy
	// ReadMode is how client reads are served. The simulator runs
	// ReadIndexBarrier by default, because the linearizability claim is
	// about the configuration that is shipped; ReadLocal is selected
	// explicitly by the regression test that asserts the bug is still
	// visible.
	ReadMode server.ReadMode
	// ReadTimeout bounds a read barrier. Compressed like ProposalTimeout:
	// at the production default a read into a partition parks for seconds,
	// which in simulated time is the whole run.
	ReadTimeout time.Duration
	// ProposalTimeout is compressed with everything else. At the production
	// default a single leaderless moment parks every caller for 5s, which in
	// simulated time is an eternity and makes a run mostly waiting.
	ProposalTimeout time.Duration

	Faults simnet.Faults

	// Workload
	JobsPerStep int
	TasksPerJob int
	WorkerCount int
	WorkerSlots int32

	// FailureRate is the fraction of task executions that report failure
	// rather than success.
	//
	// Without it the workload only ever reports ExitCode 0, so FailTask never
	// fires and the whole retry path — backoff deadlines, BACKOFF_ELAPSED
	// evals, terminal FAILED — is unreachable. A verifier that cannot reach a
	// code path provides no evidence about it.
	FailureRate float64
	// RetryableFraction splits those failures. A retryable failure exercises
	// backoff and redispatch; a non-retryable one exercises the immediate
	// terminal path, which is a different branch.
	RetryableFraction float64

	// LinClients is how many history-recording clients run alongside the
	// workload. Zero disables linearizability checking entirely.
	//
	// Small on purpose. porcupine's search is exponential in how many
	// operations overlap, so more clients do not buy more checking — past a
	// handful they buy a history the checker cannot finish, which reports
	// Unknown and is worth nothing.
	LinClients int
	// ReplayWindow forces the injected window to fire at a given step
	// against a given node, instead of choosing at runtime.
	//
	// This is what makes a sweep failure investigable. Take the WindowPlan
	// the failing run reported, feed it back, and the adversary is the same
	// one — which is as close to replay as a reactive fault can get.
	ReplayWindow *WindowPlan
	// DriveWindow makes the run OPEN a split-brain window on purpose — an
	// injected leader partition or pause at a seeded step — and protects it
	// from the schedule's kills and partitions until it has been used.
	//
	// OFF BY DEFAULT, for two separate reasons, and it took both to get the
	// boundary right.
	//
	// First, protecting the window narrows the adversary. Below-quorum
	// states, mid-pause kills and unlucky timing are real fault states the
	// claim has to cover, and suppressing them to keep an experiment tidy is
	// a selection effect. This was briefly folded into TargetStaleReads,
	// which defaults ON — so "confined to targeted mode" confined it to
	// nothing and every ordinary run had its kills suppressed.
	//
	// Second, and worse, INJECTING the window breaks schedule
	// reproducibility, which is the Day 8 checkpoint. The injection has to
	// find the current leader and defers when there is none, so whether and
	// when it fires depends on timing rather than on the seed — two runs of
	// one seed logged different schedules, intermittently.
	//
	// What stays on by default is TargetStaleReads: the clients AIM at a
	// window when the seeded schedule happens to produce one. That changes
	// no faults and logs nothing, so the schedule stays byte-reproducible.
	// Driving the window is the validation sweeps' business.
	DriveWindow bool
	// TargetStaleReads aims the clients at the split-brain window instead of
	// sampling for it: whenever a cut-off leader and a majority leader both
	// exist, write through the majority and read through the minority.
	//
	// On by default. Sampling was shown not to reach the window at all under
	// the shipped configuration — a 24-seed sweep recorded zero split-brain
	// operations — so a workload that only samples cannot support the claim.
	TargetStaleReads bool
	// LinOpsPerStep is how many operations each client issues per FAULT
	// STEP. Pacing by step rather than by clock is what makes the history
	// span the whole schedule instead of its first few seconds; see
	// linBudget for why the clock does not work here.
	LinOpsPerStep int
	// LinMaxOps caps the recorded history. It is a safety valve, not the
	// pacing mechanism: with LinOpsPerStep the total is already bounded by
	// Steps x LinClients x LinOpsPerStep. A run that hits the cap anyway is
	// still checked, and the cap is reported, because a verdict over a
	// prefix is not a verdict over the run.
	LinMaxOps int
	// LinCheckTimeout bounds the search. A timeout yields porcupine's
	// Unknown, which this harness reports as INCONCLUSIVE — the same
	// distinction MinComparisons draws: nothing was found wrong because
	// nothing was finished being looked at.
	LinCheckTimeout time.Duration
	// LinVizDir is where the HTML timeline is written on a failure. Empty
	// writes none.
	LinVizDir string
	// DisableCheckQuorum reproduces the pre-liveness configuration, in which
	// a cut-off leader keeps its term and keeps answering reads
	// indefinitely. Set by the negative control only; see raftnode.Config.
	DisableCheckQuorum bool
	// DisablePreVote likewise, used only to prove the PreVote test can
	// fail. A test that passes with the option off is not testing it.
	DisablePreVote bool
	// StallThreshold overrides raftnode's derived Ready-loop stall budget.
	//
	// Normally left zero so the simulator inherits the same "strictly below
	// the election timeout" rule production uses. It is settable because
	// LOWERING it is a measurement tool: a rare tail stall cannot be
	// attributed by waiting for it (2 in 88 runs), but turning the threshold
	// down until stalls are common shows which phase owns the slow tail,
	// which is the same question answered cheaply.
	StallThreshold time.Duration
	// CoverageSampleInterval is how often the background sampler looks at
	// committed state. It must be shorter than BaseBackoffMs, or live retry
	// backoffs are observed only by luck.
	CoverageSampleInterval time.Duration
	// PauseLeader injects a PROCESS PAUSE of the leader in targeted mode:
	// tickers frozen, inbound raft messages queued rather than dropped.
	//
	// This is the adversary that defeats CheckQuorum, which is driven by
	// ticks a paused process does not run. Off by default; the targeted
	// validation turns it on.
	PauseLeader bool
	// PauseSteps is how long a pause lasts. It must exceed an election
	// timeout, or the majority never elects a successor and there is
	// nothing newer for the woken leader to be stale about.
	PauseSteps int
	// RealWorkers runs the REAL worker binary's code in process — the
	// executor, the heartbeat loop, the local claim dedup — against the real
	// client.Pool over bufconn, instead of the simulator playing the worker
	// pool by calling the service directly.
	//
	// Off by default, for cost rather than doubt: every attempt is a
	// fork/exec, which is wall-clock time that clock compression cannot
	// remove. It is what Day 10's effect ledger needs, because a ledger
	// written by a simulated worker is evidence about the simulator.
	RealWorkers bool
	// ReplayWorkerPause forces the worker pause onto a recorded victim at a
	// recorded step, instead of aiming at whoever is executing.
	//
	// Same reason ReplayWindow exists: the injection is reactive, so the seed
	// reproduces the adversary's intent and the realization has to be fed
	// back for the aim to be the same.
	ReplayWorkerPause *WorkerPauseDiag
	// PauseWorker injects a PROCESS PAUSE of a worker that is currently
	// executing a task: its clock freezes and its outbound RPCs are held.
	//
	// WITHOUT IT OVERLAP IS UNREACHABLE, and the first ledger run proved it
	// — 62 executions, zero overlaps, max concurrency 1. A slow task alone
	// is not enough, because a LIVE worker heartbeats: heartbeat is
	// re-registration, it republishes the running claim, and it renews the
	// lease every quarter TTL for as long as the process is up. A task whose
	// worker is still running is never reclaimed however long it takes.
	//
	// So the condition the ledger exists to measure needs the worker to stop
	// renewing while its CHILD PROCESS keeps running — which is exactly a GC
	// pause, and exactly what a frozen clock plus held RPCs produces. The
	// subprocess is a real OS process and is not frozen with it, so it keeps
	// executing after the lease is reclaimed. That is the overlap.
	//
	// Only read when RealWorkers is set, so it cannot alter an ordinary run.
	PauseWorker bool
	// SlowTaskFraction is the share of tasks whose payload sleeps past its
	// lease, so the coordinator reclaims the task while it is still running.
	//
	// It exists because overlap is otherwise unreachable: a task that
	// finishes inside its lease is never redispatched mid-execution, so the
	// run measures zero overlapping executions and the effectively-once
	// claim stays inconclusive no matter how long the run is.
	SlowTaskFraction float64
	// DrainBound is how long quiescence waits for the queue to empty after
	// every fault is healed. Zero means a default derived from the run.
	DrainBound time.Duration
	// WorkerDir roots the real workers' result trees: one subdirectory per
	// worker, which is where the executor writes <taskID>/<attempt>/.
	//
	// When it is empty the simulator makes a temporary root and removes it on
	// shutdown. When it is set, the tree SURVIVES the run — which is what the
	// effect ledger has to read, since the ledger is that tree plus what the
	// coordinator committed, and a checker cannot read a directory the run
	// deleted on its way out.
	WorkerDir string
	// LeaderPartitionSteps is how many steps a leader partition is held
	// against healing. It must span an election timeout plus enough time
	// for the new leader to commit, or the window closes before a read of
	// anything newly committed could be stale.
	LeaderPartitionSteps int

	Out io.Writer
	// WorkerOut receives the real workers' transient-failure notices, which
	// mid-chaos are the EXPECTED outcome and arrive in the thousands.
	//
	// A SEPARATE writer from Out, and that separation is load-bearing: Out
	// carries the fault schedule, which the Day 8 checkpoint compares byte
	// for byte across two runs of one seed. Worker chatter is timing
	// dependent by nature, so mixing it into that log would make the
	// reproducibility claim fail for a reason that has nothing to do with
	// reproducibility. Nil discards it.
	WorkerOut io.Writer
}

func DefaultConfig() Config {
	return Config{
		Nodes:    3,
		Steps:    40,
		StepTime: 150 * time.Millisecond,
		// 25ms, not 10ms: ElectionTick x TickInterval must stay above the
		// non-compressible disk floor. A snapshot plus WAL compaction costs
		// 23-40ms of real fsync time no matter how far the clock is
		// compressed, and at a 100ms election timeout the derived stall
		// threshold (50ms) sat below it — so routine snapshots registered as
		// Ready-loop stalls. 250ms election, 125ms threshold, ~40ms worst
		// batch.
		TickInterval: 25 * time.Millisecond,
		ElectionTick: 10,
		LeaseTTL:     300 * time.Millisecond,
		TickPeriod:   20 * time.Millisecond,
		// Frequent enough that compaction and Restore are genuinely
		// exercised, rare enough that the run is not dominated by fsync.
		SnapshotEvery:   150,
		ProposalTimeout: 250 * time.Millisecond,
		ReadMode:        server.ReadIndexBarrier,
		ReadTimeout:     250 * time.Millisecond,
		MinComparisons:  20,
		// Scaled to the run, not to production. A run spans well under a
		// second of state-machine time, so these are set so that a retry
		// backoff elapses within a step or two and a session outlives a few
		// steps but not the run.
		StatePolicy: state.Policy{
			BaseBackoffMs: 25,
			MaxBackoffMs:  150,
			SessionTTLMs:  400,
		},
		Faults:                 simnet.Faults{DropRate: 0.02, MinDelay: 0, MaxDelay: 5 * time.Millisecond},
		JobsPerStep:            1,
		TasksPerJob:            3,
		WorkerCount:            3,
		WorkerSlots:            2,
		FailureRate:            0.15,
		RetryableFraction:      0.7,
		LinClients:             3,
		LinOpsPerStep:          4,
		LinMaxOps:              4000,
		LinCheckTimeout:        20 * time.Second,
		CoverageSampleInterval: 5 * time.Millisecond,
		// Three steps, not one. A step is ~150ms of StepTime against a 250ms
		// election timeout, so a one-step hold can end before the majority
		// has even elected a successor.
		LeaderPartitionSteps: 3,
		PauseSteps:           3,
		// A third of tasks outlive their lease, so the coordinator reclaims
		// them mid-execution. High enough that a short run reaches overlap
		// at all, low enough that the run is not mostly sleeping. Read only
		// when RealWorkers is set.
		SlowTaskFraction: 0.34,
		PauseWorker:      true,
	}
}

// WindowPlan is a realized injected window: when it actually fired, against
// which node, and whether it was a pause or a partition.
//
// It exists because a DriveWindow run is NOT reproducible from its seed. The
// injection has to find the current leader and defers when there is none, so
// the step it lands on and the node it hits depend on timing. A failure found
// in a sweep therefore cannot be replayed from the seed alone — the seed
// reproduces the adversary's INTENT, not its realization.
//
// So the realization is recorded and can be fed back in. Replay(plan) forces
// the same step and the same victim, which is the difference between a
// failure you can put back in front of the system and one you can only
// describe.
type WindowPlan struct {
	// Fired is false when the injection never managed to open a window.
	Fired bool
	// Step is the step it fired on, which may be later than the step drawn
	// from the seed if it had to defer.
	Step int
	// NodeID is the leader it targeted.
	NodeID uint64
	// Paused is true for a process pause, false for a network partition.
	Paused bool
}

func (w WindowPlan) String() string {
	if !w.Fired {
		return "window never opened"
	}
	kind := "partition"
	if w.Paused {
		kind = "pause"
	}
	return fmt.Sprintf("%s of node %d at step %d", kind, w.NodeID, w.Step)
}

// PauseDiag records why a pause run did or did not reach the window.
//
// "The pause covers 4 of 8" is not a diagnosis, and the first attempt to fix
// that coverage by adding seeds would have buried three real defects. Each
// way the sequence can fall through is recorded here instead.
type PauseDiag struct {
	// Woke is the step the paused node was resumed on.
	Woke int
	// MajorityElected is whether a leader OTHER than the paused node existed
	// at wake. Without one there is nothing newer to be stale about.
	MajorityElected bool
	// MajorityCommitted is whether the post-wake submit through that leader
	// was acknowledged. An unacknowledged write proves nothing when read
	// back.
	MajorityCommitted bool
	// WokenStillLeader is whether the woken node still believed it led when
	// asked. If it had already stepped down it refuses the read instead of
	// answering staly.
	WokenStillLeader bool
	// ReadLandedInGap is whether the targeted read was served before the
	// queued raft messages were released. False means the window closed
	// before the read.
	ReadLandedInGap bool
}

// Result is what a run produces.
type Result struct {
	Seed       int64
	Steps      int
	Violations []Violation
	Faults     FaultCounts
	Coverage   Coverage
	Jobs       int
	Tasks      int
	Lin        LinResult
	// Window is the injected window as REALIZED, for replay.
	Window WindowPlan
	// Pause explains the pause sequence when one was injected.
	Pause PauseDiag
	// LeadershipChanges is what each leadership transition cost the Ready
	// loop, with the job count it was carrying — the curve that says whether
	// the broker rebuild scales with state.
	LeadershipChanges []server.LeadershipChange
	// Snapshots is what each snapshot cost the Ready loop that took it, in
	// order, across every node.
	//
	// A TREND, not a total, is the useful form: the snapshot grows with
	// state and blocks the loop for all of it, so a long run's real question
	// is whether that cost is climbing toward the stall threshold. A run
	// that ends with snapshots near the threshold was about to start
	// truncating, and would have looked fine until it did.
	Snapshots []raftnode.SnapshotStat
	// FrozenClock is the rendered timeline of a frozen-clock window, set
	// only when that invariant fired. The window reports itself so the
	// diagnosis does not depend on reproducing a 6%-of-runs finding.
	FrozenClock []string
	// HostIOStalls and DowngradedStalls are the stall accounting, reported
	// on every run.
	//
	// A DOWNGRADED stall is one this run could not attribute: a blocking
	// call on the Ready loop, in a run whose disk also blew past its own
	// election timeout. It is neither a clean pass nor a finding, and it has
	// to be COUNTED or it reads as the former — the whole reason the
	// downgrade is a reporting hazard rather than a fix.
	HostIOStalls     int
	DowngradedStalls int
	// Ledger is what the effect ledger observed, reported whether or not it
	// found anything.
	Ledger Ledger
	// WorkerPause explains the worker pause when one was injected.
	WorkerPause WorkerPauseDiag
}

// Coverage is which code paths a run actually reached.
//
// It exists because "zero violations" is only evidence about paths that
// EXECUTED. A run that never failed a task says nothing about retries, and
// reporting that honestly is the difference between verification and
// decoration.
type Coverage struct {
	// Reported by the simulated workers.
	ReportedOK        int
	ReportedRetryable int
	ReportedTerminal  int

	// Observed in committed state, by sampling each step.
	TasksRedispatched int // attempt > 1: a reclaim or retry actually happened
	TasksBackedOff    int // distinct tasks seen holding a live NotBeforeMs
	TasksTerminalFail int // distinct tasks that reached FAILED
	SessionsExpired   int // the Tick TTL sweep actually removed something

	// Observed AT THE APPLY SITE, because these are silent no-ops that
	// cannot be sampled afterwards: once the entry has been applied, a report
	// that was ignored looks exactly like a report that never arrived.
	//
	// StaleReportsRejected is the one the effectively-once claim rests on: a
	// report that REACHED a coordinator naming an attempt that was no longer
	// live, and was discarded. ZERO MEANS THE SCENARIO DID NOT HAPPEN, which
	// makes the claim inconclusive for that run rather than confirmed — the
	// same rule as zero overlap and zero split-brain reads.
	StaleReportsRejected    int
	DuplicateReportsIgnored int // the live attempt reported twice
}

// FaultCounts is the chaos actually induced, not the chaos configured.
type FaultCounts struct {
	NodeKills    int
	NodeRestarts int
	Partitions   int
	// LeaderPartitions is how many of those cut off the node that was
	// leading, which is the only kind that can produce a stale read.
	LeaderPartitions int
	// Pauses is how many times a leader's process was frozen.
	Pauses int
	Heals  int
	Net    simnet.Stats
}

// ViolationKind says WHICH check failed.
//
// It exists because a bare error from Run is ambiguous, and the ambiguity was
// not harmless. When the linearizability check started failing runs, four
// unrelated tests went red: a comparison-floor test reported "a healthy run
// failed its own floor" over a run whose floor was fine and whose history was
// illegal, and a schedule-reproducibility test went red for a violation that
// has nothing to do with schedules. A test that names the wrong cause sends
// the next person to the wrong code.
//
// The rule this enforces: every test asserts only its OWN property. A
// detector fails on its own kind deliberately; everything else ignores kinds
// it does not own.
type ViolationKind string

const (
	// KindInvariant is a per-step state invariant, checked mid-chaos.
	KindInvariant ViolationKind = "invariant"
	// KindConvergence is a post-heal disagreement between replicas.
	KindConvergence ViolationKind = "convergence"
	// KindComparisonFloor means the convergence check did not compare
	// enough to conclude anything. Inconclusive, not a violation.
	KindComparisonFloor ViolationKind = "comparison-floor"
	// KindFaultModel means the run left raft's fault model, so its results
	// cannot support a correctness claim. Inconclusive.
	KindFaultModel ViolationKind = "fault-model"
	// KindHostIO means the Ready loop stalled waiting on the DISK, long
	// enough that the run's compressed timings stopped holding.
	// Inconclusive: the host could not meet the premise, so the run proved
	// nothing — it does not say the system is wrong.
	KindHostIO ViolationKind = "host-io"
	// KindLinearizability is an illegal client history.
	KindLinearizability ViolationKind = "linearizability"
	// KindLinearizabilityTimeout means the search did not finish, or there
	// was nothing to search, so there is no verdict. Inconclusive.
	KindLinearizabilityTimeout ViolationKind = "linearizability-timeout"
	// KindLinearizabilityCoverage means the history never reached a state
	// that could produce a violation: no operation landed on a cut-off node
	// and none ran while two nodes claimed leadership.
	//
	// Inconclusive, on the MinComparisons precedent. Such a run is green
	// about a HEALTHY cluster, which is trivially linearizable and needs no
	// checker, so counting it toward "N histories, zero violations" inflates
	// the claim with runs that could not have failed.
	KindLinearizabilityCoverage ViolationKind = "linearizability-coverage"
	// KindReadyLoopStall is a Ready loop that blocked on something other
	// than the disk — a blocking call on the goroutine that serves it.
	//
	// Its own kind rather than a plain invariant because it shares a PREMISE
	// with KindHostIO: both are judgements about time, measured against a
	// compressed election timeout. When the host breaks that premise, every
	// timing judgement in the run has to be downgraded with it, and
	// downgrading by kind is the only way to do that without matching on
	// message text. See downgradeStallsOnBrokenPremise.
	KindReadyLoopStall ViolationKind = "ready-loop-stall"
	// KindEffectLedger is an effect-ledger violation: more than one committed
	// effect for a task, a committed effect that does not match the attempt
	// the coordinator recorded, or a task that never reached a terminal state
	// over a cluster that had drained.
	KindEffectLedger ViolationKind = "effect-ledger"
	// KindDidNotDrain means the quiescence phase ran out of budget with
	// tasks still in flight.
	//
	// DELIBERATELY NOT a lost task, and deliberately not an effect-ledger
	// violation. A task still retrying has not been lost; what failed is a
	// liveness bound this harness chose, so it is inconclusive. Conflating
	// the two turns a slow host into a correctness claim.
	KindDidNotDrain ViolationKind = "did-not-drain"
	// KindLedgerCoverage means the ledger ran but the run never reached the
	// situation it exists to check: no two executions overlapped, or no late
	// report arrived. Inconclusive, on the same rule as linearizability
	// coverage — a zero there means the path did not run, not that it worked.
	KindLedgerCoverage ViolationKind = "ledger-coverage"
)

// IsEnvironment reports whether this kind says the HOST or the harness could
// not support a claim, as opposed to saying the system is wrong.
//
// Both members are "we could not look", not "we looked and it was broken": a
// run whose fsyncs blew past its compressed timings, and a run that left
// raft's fault model. Neither is any individual property's business, so the
// per-property tests ignore them — otherwise a slow disk turns six unrelated
// tests red and every one of them names the wrong cause.
func (k ViolationKind) IsEnvironment() bool {
	return k == KindHostIO || k == KindFaultModel
}

// IsStall reports whether this kind is a Ready-loop timing judgement. Tests
// that own neither the stall nor the host's disk ignore both.
func (k ViolationKind) IsStall() bool {
	return k == KindReadyLoopStall || k == KindHostIO
}

// IsLedger reports whether this kind comes from the effect ledger. Tests
// that are about something else use it to ignore exactly those, the same way
// they ignore the linearizability kinds.
func (k ViolationKind) IsLedger() bool {
	return k == KindEffectLedger ||
		k == KindDidNotDrain ||
		k == KindLedgerCoverage
}

// IsLinearizability reports whether this kind is one of the two the
// linearizability check produces. Tests that are not about linearizability
// use it to ignore exactly those.
func (k ViolationKind) IsLinearizability() bool {
	return k == KindLinearizability ||
		k == KindLinearizabilityTimeout ||
		k == KindLinearizabilityCoverage
}

// Violation is an invariant that failed, with the seed and step needed to get
// back to it.
type Violation struct {
	Seed int64
	Step int
	Kind ViolationKind
	What string
	// Inconclusive marks a run that could not check something, as opposed to
	// one that found something wrong. The distinction matters: a violation
	// says the system is broken, inconclusive says the HARNESS could not see.
	// Reporting the second as a pass is how a verifier quietly stops
	// verifying.
	Inconclusive bool
}

func (v Violation) Error() string {
	label := "VIOLATION"
	if v.Inconclusive {
		label = "INCONCLUSIVE"
	}
	return fmt.Sprintf("%s [%s] seed %d step %d: %s", label, v.Kind, v.Seed, v.Step, v.What)
}

// IsKind reports whether err is a Violation of this kind.
func IsKind(err error, kind ViolationKind) bool {
	var v Violation
	if !errors.As(err, &v) {
		return false
	}
	return v.Kind == kind
}

// IsLinearizabilityErr reports whether err is a linearizability finding —
// either an illegal history or a search that reached no verdict.
func IsLinearizabilityErr(err error) bool {
	var v Violation
	if !errors.As(err, &v) {
		return false
	}
	return v.Kind.IsLinearizability()
}

type node struct {
	id    uint64
	coord *server.Coordinator
	svc   *server.Service
	dir   string
	down  bool
	// clk is this node's pausable clock, so the fault schedule can freeze
	// its tickers without touching any other node's.
	clk *clock.Pausable
}

// Sim is a running simulation.
type Sim struct {
	cfg   Config
	rng   *rand.Rand
	net   *simnet.Network
	dirs  map[uint64]string
	peers map[uint64]string
	// apiAddrs are the CLIENT-facing addresses, a different address space
	// from peers: peers are the URLs raft speaks between nodes, and a client
	// dialing one would reach a port that does not serve the scheduler API.
	// They are what NOT_LEADER redirects carry.
	apiAddrs map[uint64]string
	// api serves the scheduler API over bufconn when RealWorkers is set, and
	// is nil otherwise.
	api *apiNet
	// rw is the in-process worker pool, nil unless RealWorkers is set.
	rw *realWorkerPool

	// nodesMu guards nodes.
	//
	// Until Day 9 the map was touched only by the Run goroutine and needed
	// no lock. The recorded clients changed that: they run continuously, in
	// parallel with applyFault, which is the whole point — an operation that
	// straddles a kill or a partition is the interesting one.
	//
	// start and stop hold the WRITE lock across the coordinator's own
	// start/stop, not just across the map mutation. Narrowing it would let a
	// client call into a coordinator that is mid-teardown; the cost is that
	// clients block for a few milliseconds per fault, which is the right
	// trade.
	nodesMu sync.RWMutex
	nodes   map[uint64]*node

	// The recorded history and the job ids its clients have learned.
	history   *history
	knownJobs knownJobs
	stopLin   func()
	linOnce   sync.Once
	// stepNo is the fault step currently running, read by the recorded
	// clients to pace themselves against the schedule.
	stepNo atomic.Int64
	// heldUntil maps a deliberately-isolated node to the step at which its
	// hold expires, so a held partition is not undone by the next quiet step
	// and is not cancelled by the next partition of somebody else either.
	//
	// Per node, and a step DEADLINE rather than a countdown, because both
	// alternatives were measured wrong. One shared counter made any heal
	// anywhere shorten an unrelated hold, and a countdown decremented only on
	// heal attempts meant "held for 3 steps" actually meant three heal rolls,
	// which is about five steps and varies with the seed.
	heldUntil map[uint64]int
	// targetedPartitionStep is the step at which targeted mode INJECTS a
	// held leader partition, drawn once from the run's seed.
	targetedPartitionStep int
	// windowPending is true while targeted mode is still trying to open, or
	// still inside, its injected window.
	//
	// While it is set, the fault schedule's KILLS and PARTITIONS are
	// suppressed. Those were destroying the thing the run exists to
	// measure: one seed had a node killed mid-pause, leaving a single
	// reachable node that could not elect, and another never opened a
	// window at all because membership never recovered between kills.
	//
	// This is a narrowing of the adversary and would be a selection effect
	// in the default sweep — which keeps the full schedule. It is confined
	// to TARGETED mode, which is opt-in validation whose entire purpose is
	// the injected window; letting an unrelated kill eat it measures
	// nothing and reports it as coverage.
	windowPending bool
	// paused is the node currently frozen by the pause fault, and
	// pauseUntilStep the step at which it must be woken. Zero means none.
	paused         *node
	pauseUntilStep int
	// wakeSeq numbers the post-wake targeted reads.
	wakeSeq int

	mu     sync.Mutex
	result Result

	// Coverage bookkeeping, sampled between steps on the Run goroutine.
	seenBackoff map[string]bool
	seenRetried map[string]bool
	// obs is everything recorded from the apply site and from the gRPC
	// handlers, behind its OWN mutex. It is not guarded by s.mu on purpose —
	// see the observations type.
	obs *observations
	// ledger accumulates the effect ledger during the run.
	ledger Ledger
	// overlapLog is the shared append-only file the task payloads write, and
	// is empty unless real workers are running.
	overlapLog string
	// cmdShell is this host's /bin/sh, resolved like the other commands.
	cmdShell string
	// overlapLogTemp is set when the simulator had to make its own directory
	// for the log, and is removed on shutdown.
	overlapLogTemp string
	// The worker pause: the step it may fire on, who is frozen, and when to
	// wake them.
	workerPauseStep int
	workerPaused    string
	workerResumeAt  int
	seenFailed      map[string]bool
	lastSessions    int
	// cmdOK and cmdFail are the task commands, resolved against THIS HOST's
	// PATH rather than hardcoded.
	//
	// The workload shipped "/bin/true" from Day 3, and nothing ever ran it:
	// the simulator played the worker pool and decided outcomes by hashing,
	// so the command string was inert. The first real worker reported
	// "fork/exec /bin/true: no such file or directory" on every task —
	// /bin/true does not exist on this machine, only /usr/bin/true. Three
	// days of runs had been submitting a command the host cannot execute,
	// which is exactly the class of gap "the simulator does not run the
	// worker" was always going to be hiding.
	cmdOK   string
	cmdFail string
	// faultLog is every injected fault with the wall time it landed, so a
	// frozen-clock window can be asked WHICH fault explains the churn inside
	// it rather than being correlated by eye against a step number.
	faultLog []faultEvent
	// clockLog is one sample per invariant check: the committed clock, who
	// led, in which term, and whether the exemptions applied.
	clockLog []clockSample
	// lastClock is the leader's state-machine clock at the previous check,
	// and clockFrozenSteps how many checks it has failed to move.
	lastClock        int64
	clockFrozenSteps int
}

// New builds a simulation. dirs must be writable; the caller owns cleanup.
func New(cfg Config, dirs map[uint64]string) *Sim {
	if cfg.Out == nil {
		cfg.Out = io.Discard
	}
	s := &Sim{
		cfg:   cfg,
		rng:   rand.New(rand.NewSource(cfg.Seed)),
		net:   simnet.New(cfg.Seed, cfg.Faults),
		nodes: map[uint64]*node{},
		dirs:  dirs,
		peers: map[uint64]string{},

		apiAddrs: map[uint64]string{},

		obs: newObservations(),

		seenBackoff:  map[string]bool{},
		seenRetried:  map[string]bool{},
		seenFailed:   map[string]bool{},
		lastSessions: -1,

		history: newHistory(cfg.LinMaxOps),
	}
	s.result.Seed = cfg.Seed

	// Draw the injected window once, here, so it is reproducible from the
	// seed and costs the fault sequence exactly one draw at a fixed point.
	// (Rule 3 forbids workload draws from the fault RNG because a
	// TIMING-DEPENDENT number of them destroys reproducibility; a single
	// draw at construction is deterministic and does not.)
	//
	// Placed EARLY — in the first quarter — and never at step 1, where the
	// cluster may still be electing its first leader.
	//
	// The first version spread it over the first half, which looked more
	// varied and truncated the window on late draws: the hold needs
	// LeaderPartitionSteps after it, so an injection past Steps-hold leaves
	// the run ending mid-window. Keeping it early is what lets the sweeps be
	// short enough to fit the package's test timeout.
	s.windowPending = cfg.DriveWindow
	if cfg.ReplayWindow != nil && cfg.ReplayWindow.Fired {
		s.targetedPartitionStep = cfg.ReplayWindow.Step
	} else if cfg.DriveWindow && cfg.Steps > 2 {
		// The window must have room to CLOSE before the run ends, so the
		// latest usable step is Steps minus however long it is held.
		//
		// The first version used Steps/4, which at seven steps is a span of
		// ONE: every seed injected at step 2 and woke at step 5, so eight
		// seeds were eight samples of a single timing rather than eight
		// timings. The per-run diagnosis is what showed it — the step and
		// wake columns were identical down the whole table.
		hold := cfg.PauseSteps
		if cfg.LeaderPartitionSteps > hold {
			hold = cfg.LeaderPartitionSteps
		}
		span := cfg.Steps - hold - 1
		if span < 1 {
			span = 1
		}
		s.targetedPartitionStep = 2 + s.rng.Intn(span)
	}
	// Drawn at construction for the same reason the injected window is: one
	// draw at a fixed point is reproducible from the seed, while a
	// timing-dependent number of draws is not. Gated on RealWorkers so an
	// ordinary run's fault sequence is untouched.
	if cfg.RealWorkers && cfg.PauseWorker && cfg.Steps > 3 {
		s.workerPauseStep = 2 + s.rng.Intn(maxInt(1, cfg.Steps/2))
	}
	if r := cfg.ReplayWorkerPause; r != nil && r.Fired {
		s.workerPauseStep = r.Step
	}
	for i := 1; i <= cfg.Nodes; i++ {
		// Addresses are labels only: simnet routes by node id, so nothing
		// binds a port and a long run cannot exhaust them.
		s.peers[uint64(i)] = fmt.Sprintf("sim://node-%d", i)
		// Client-facing names, resolved by the bufconn dialer rather than by
		// DNS. Labels here too: nothing binds a port, so parallel runs cannot
		// collide and a long one cannot exhaust anything.
		s.apiAddrs[uint64(i)] = fmt.Sprintf("sim-api-%d", i)
	}
	s.cmdOK = lookCommand("true")
	s.cmdFail = lookCommand("false")
	s.cmdShell = lookCommand("sh")
	if cfg.RealWorkers {
		s.api = newAPINet(s.observeReport)
	}
	return s
}

// lookCommand resolves a coreutils-style command to an absolute path on this
// host. The fallback is the conventional location, which at least fails with
// a legible path rather than an empty one.
func lookCommand(name string) string {
	if p, err := exec.LookPath(name); err == nil {
		return p
	}
	return "/usr/bin/" + name
}

// seededReader turns the run's RNG into the id source, so a reproduced run
// mints the same job and proposal ids and two runs can be diffed.
type seededReader struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func (r *seededReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rng.Read(p)
}

func (s *Sim) start(id uint64) error {
	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()

	cfg := server.DefaultConfig()
	cfg.LeaseTTL = s.cfg.LeaseTTL
	cfg.TickPeriod = s.cfg.TickPeriod
	cfg.Sched.LeaseTTLMs = s.cfg.LeaseTTL.Milliseconds()
	cfg.Sched.Workers = 2
	cfg.ProposalTimeout = s.cfg.ProposalTimeout
	cfg.ReadMode = s.cfg.ReadMode
	cfg.ReadTimeout = s.cfg.ReadTimeout
	// IDENTICAL on every node, which is the contract Policy carries.
	cfg.StatePolicy = s.cfg.StatePolicy
	// So NOT_LEADER carries an address the caller can actually dial, which is
	// what production does: cmd/coordinator takes -api-peers and sets this
	// map, and without it every redirect says "no leader elected" and a
	// client rotates seeds blindly.
	//
	// Set on EVERY run, not just the real-worker ones. The simulator's
	// default configuration is supposed to be the shipped configuration, and
	// two address-space behaviours would mean the ordinary runs were
	// verifying something production does not do.
	//
	// It does not change how ambiguity is recorded. Knowing a leader's
	// address changes a redirect's code from Unavailable to
	// FailedPrecondition, and classifySubmit puts BOTH in the Unknown bucket
	// — only nil and InvalidArgument are definitive, precisely because
	// NOT_LEADER can be returned after an entry was already proposed.
	cfg.APIAddrs = s.apiAddrs
	// Observe the apply site, so the silent no-ops are countable.
	cfg.OnApply = s.observeApply
	// Each node gets its own stream off the run's seed, so the ids a node
	// mints are reproducible without every node drawing from one shared
	// sequence whose order depends on goroutine scheduling.
	cfg.Rand = &seededReader{rng: rand.New(rand.NewSource(s.cfg.Seed + int64(id)))}
	// One pausable clock per node, shared by the coordinator's drivers and
	// the raft ticker, so a pause freezes both.
	pclk := clock.NewPausable(nil)
	cfg.Clock = pclk

	coord := server.New(cfg)
	rcfg := raftnode.DefaultConfig()
	rcfg.ID = id
	rcfg.Peers = s.peers
	rcfg.DataDir = s.dirs[id]
	rcfg.TickInterval = s.cfg.TickInterval
	rcfg.ElectionTick = s.cfg.ElectionTick
	rcfg.HeartbeatTick = 1
	rcfg.SnapshotEntries = s.cfg.SnapshotEvery
	rcfg.SnapshotCatchUpEntries = 5
	rcfg.StallThreshold = s.cfg.StallThreshold
	rcfg.DisableCheckQuorum = s.cfg.DisableCheckQuorum
	rcfg.DisablePreVote = s.cfg.DisablePreVote
	// Derived from the election timeout by raftnode when zero; left zero
	// here so the simulator inherits the same "strictly below the election
	// timeout" rule the production default uses.
	rcfg.Transport = s.net.Transport(id)
	rcfg.Clock = pclk

	if err := coord.StartRaft(rcfg); err != nil {
		return fmt.Errorf("start node %d: %w", id, err)
	}
	s.net.SetDown(id, false)
	svc := server.NewService(coord)
	s.nodes[id] = &node{id: id, coord: coord, svc: svc, dir: s.dirs[id], clk: pclk}
	if s.api != nil {
		s.api.serve(s.apiAddrs[id], svc)
	}
	return nil
}

func (s *Sim) stop(id uint64) {
	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()

	n, ok := s.nodes[id]
	if !ok {
		return
	}
	// Mark it down FIRST: a node that is tearing down must not still be
	// receiving messages, or its peers see a half-dead node rather than a
	// cleanly absent one.
	s.net.SetDown(id, true)
	// The API goes down with it. A killed coordinator must not answer
	// NOT_LEADER — it must not answer at all, which is the case that strands
	// a pool that only handles redirects.
	if s.api != nil {
		s.api.shut(s.apiAddrs[id])
	}
	n.coord.StopRaft()
	n.coord.StopScheduling()
	delete(s.nodes, id)
}

// leader returns the current leader, or nil if there is none. An election in
// progress is a normal state, not an error — the simulator spends a lot of
// its time there on purpose.
func (s *Sim) leader() *node {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()

	var leaders []*node
	for _, n := range s.nodes {
		if n.coord.IsLeader() {
			leaders = append(leaders, n)
		}
	}
	if len(leaders) != 1 {
		return nil
	}
	return leaders[0]
}

// awaitLeader waits for exactly one leader.
func (s *Sim) awaitLeader(within time.Duration) *node {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if n := s.leader(); n != nil {
			return n
		}
		time.Sleep(2 * time.Millisecond)
	}
	return nil
}

// faultEvent is one injected fault, timestamped.
type faultEvent struct {
	AtMs int64
	Step int
	What string
}

// clockSample is one invariant check's view of the committed clock.
type clockSample struct {
	AtMs              int64
	Step              int
	ClockMs           int64
	LeaderID          uint64
	Term              uint64
	Quorum            bool
	LeaderPartitioned bool
	Paused            bool
	// Raft is the LEADER'S OWN view at this sample: whether a majority could
	// actually acknowledge a proposal, and the per-follower progress behind
	// that answer. The simulator's Quorum field above counts nodes that are
	// up and unpartitioned, which a follower in StateSnapshot satisfies
	// while acking nothing.
	Raft server.RaftInfo
}

// noteFault logs a fault AND records it with a timestamp.
//
// Both, because the two readers want different things: a human reading a
// verbose run wants the step-ordered narrative, and the frozen-clock window
// wants to know which faults landed inside a 900ms span. Correlating a step
// number against raft's own timestamps by eye is how an inference gets
// reported as a measurement.
func (s *Sim) noteFault(step int, format string, args ...any) {
	what := fmt.Sprintf(format, args...)
	s.mu.Lock()
	s.faultLog = append(s.faultLog, faultEvent{AtMs: time.Now().UnixMilli(), Step: step, What: what})
	s.mu.Unlock()
	s.logf("step %d: %s", step, what)
}

func (s *Sim) logf(format string, args ...any) {
	fmt.Fprintf(s.cfg.Out, format+"\n", args...)
}

// Run executes the whole schedule and returns what happened.
//
// Every step: pick a fault from the seeded RNG, apply it, offer some work,
// let the cluster run, then CHECK INVARIANTS. Checking after every step is
// what turns "it crashed eventually" into "it broke at step 17 of seed 42".
func (s *Sim) Run(ctx context.Context) (Result, error) {
	// FIRST, before a single node starts: can this host execute the tasks
	// this run will submit? A bad command path makes a run green and empty,
	// so it fails here in milliseconds rather than after a quiet hour. See
	// validateCommands.
	if err := s.validateCommands(); err != nil {
		return s.result, fmt.Errorf("seed %d: %w", s.cfg.Seed, err)
	}

	defer s.shutdown()

	for i := 1; i <= s.cfg.Nodes; i++ {
		if err := s.start(uint64(i)); err != nil {
			return s.result, err
		}
	}
	if s.awaitLeader(10*time.Second) == nil {
		return s.result, fmt.Errorf("seed %d: no leader within 10s of bringing the cluster up", s.cfg.Seed)
	}
	if s.cfg.RealWorkers {
		// The shared append-only log the task payloads write. It lives beside
		// the worker result trees, so a caller that named WorkerDir keeps it
		// and one that did not has it cleaned up with the rest.
		if err := s.openOverlapLog(); err != nil {
			return s.result, err
		}
		rw, err := s.startRealWorkers(ctx)
		if err != nil {
			return s.result, err
		}
		s.rw = rw
	} else {
		s.registerWorkers()
	}

	// The recorded clients run for the whole fault schedule, concurrently
	// with it. They are stopped by collect(), on EVERY return path, so a run
	// that ends on an invariant violation still leaves a closed history.
	bgCtx, stopBg := context.WithCancel(ctx)
	wait := s.runLinClients(bgCtx)

	// The coverage sampler runs alongside them, for the same reason: what it
	// measures only exists mid-step.
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		s.sampleCoverage(bgCtx)
	}()

	s.stopLin = func() {
		stopBg()
		wait()
		sampler.Wait()
	}
	defer s.stopLin()

	for step := 1; step <= s.cfg.Steps; step++ {
		select {
		case <-ctx.Done():
			s.result.Steps = step - 1
			return s.collect(), ctx.Err()
		default:
		}

		s.stepNo.Store(int64(step))
		// BEFORE the fault roll: a hold that is due ends on its own schedule,
		// not when the schedule next happens to roll a heal.
		s.releaseExpiredHolds(step)
		s.applyFault(step)
		// The worker pause rides along with the fault step but is its own
		// injection: it aims at a worker that is executing right now, which
		// is a condition only the live cluster can report.
		s.injectWorkerPause(step)
		// AFTER applyFault, so a quiet step's heal cannot undo it in the
		// same breath.
		s.injectTargetedWindow(step)
		s.wakeIfDue(step)
		s.offerWork(step)
		// With real workers the pool polls, executes and reports on its own
		// goroutines; the simulator only submits. Driving both would have two
		// claimants for one worker id.
		if !s.cfg.RealWorkers {
			s.drainSomeWork()
		}
		// Sample IMMEDIATELY after the reports land. A retry backoff is
		// deliberately shorter than a step, so a once-per-step sampler
		// watches it elapse between looks and concludes it never happened —
		// which is how a coverage number lies in the safe-looking direction.
		s.recordCoverage()
		time.Sleep(s.cfg.StepTime)
		s.recordCoverage()

		s.result.Steps = step
		if vs := s.checkInvariants(step); len(vs) > 0 {
			s.result.Violations = append(s.result.Violations, vs...)
			return s.finish()
		}
	}

	// QUIESCENCE, for a run with an effect ledger. Heal everything, wake
	// everyone, stop injecting, and wait for the queue to drain within a
	// bound — because "every submitted task reaches a terminal state" is
	// false by design mid-chaos and can only be asserted over an idle
	// cluster. Exceeding the bound is DID-NOT-DRAIN, not a lost task.
	if s.cfg.RealWorkers {
		// The config's override, which is normally zero: quiesce derives its
		// own bound from the BACKLOG it finds, which a caller here cannot
		// know. Passing drainBound() made the derivation dead code and
		// reported DID-NOT-DRAIN on every seed against a fixed 6s.
		s.quiesce(s.cfg.DrainBound)
	}

	// Heal everything and give the cluster a chance to settle, so the final
	// check is against a converged cluster rather than mid-chaos. A system
	// that is briefly inconsistent under partition is expected; one that
	// stays inconsistent after healing is broken.
	s.healAll()
	for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
		if _, up := s.nodeAt(id); !up {
			_ = s.start(id)
			s.result.Faults.NodeRestarts++
		}
	}
	s.awaitLeader(10 * time.Second)
	time.Sleep(10 * s.cfg.StepTime)
	s.recordCoverage()

	if vs := s.checkConverged(); len(vs) > 0 {
		s.result.Violations = append(s.result.Violations, vs...)
	}
	return s.finish()
}

// downgradeStallsOnBrokenPremise turns blocking-call stalls INCONCLUSIVE when
// the same run also saw the host's disk blow past its compressed timings.
//
// The two share one premise. "This loop blocked for 2m43s" is only evidence
// about the system if 2m43s was ever a meaningful duration in this run, and a
// host-io stall says it was not: at a 250ms election timeout, an fsync of
// minutes means every node was seeing spurious elections and every goroutine
// was queued behind the disk. Day 10 measured exactly that pair — a schedule
// checkpoint reporting "stalled 2m43.149s in send (blocking call)" in a sweep
// whose other runs were recording 13-MINUTE fsyncs in persist.
//
// Attributing the first to the system while excusing the second is the Day 9
// mistake in reverse: there the invariant called a slow disk a concurrency
// bug directly, here it does it one step removed, by blaming the pileup the
// slow disk caused.
//
// Not a blanket exemption. A blocking-call stall in a run whose disk kept up
// stays a VIOLATION, which is the case worth catching — the leadership
// revocation deadlock that Day 8 found presented exactly that way.
func downgradeStallsOnBrokenPremise(vs []Violation) (out []Violation, hostIO, downgraded int) {
	for _, v := range vs {
		if v.Kind == KindHostIO {
			hostIO++
		}
	}
	if hostIO == 0 {
		return vs, 0, 0
	}
	out = make([]Violation, 0, len(vs))
	for _, v := range vs {
		if v.Kind == KindReadyLoopStall {
			v.Inconclusive = true
			v.What += " — but this run also waited on the disk past its own election timeout, " +
				"so the premise that makes this duration meaningful was already broken; not attributable"
			downgraded++
		}
		out = append(out, v)
	}
	return out, hostIO, downgraded
}

// finish collects the result and turns whatever was found into Run's error.
//
// It exists because collect() can ADD a violation: the linearizability check
// runs there, after the clients have been stopped, so a run that reached the
// end with every step-wise invariant intact can still have an illegal
// history. The first version returned nil on that path and printed "3/3 runs
// passed" over three Illegal verdicts — the finding was recorded in the
// Result and thrown away by the exit code, which is the one thing a verifier
// must never do.
//
// Real violations are reported ahead of inconclusive ones. Both are failures;
// a violation is the more useful thing to put in front of someone.
func (s *Sim) finish() (Result, error) {
	res := s.collect()
	vs, hostIO, downgraded := downgradeStallsOnBrokenPremise(res.Violations)
	res.Violations, res.HostIOStalls, res.DowngradedStalls = vs, hostIO, downgraded
	s.result.Violations = vs
	s.result.HostIOStalls, s.result.DowngradedStalls = hostIO, downgraded
	for _, v := range res.Violations {
		if !v.Inconclusive {
			return res, v
		}
	}
	if len(res.Violations) > 0 {
		return res, res.Violations[0]
	}
	return res, nil
}

// stopBackground halts the recorded clients and the coverage sampler, once.
//
// Everything that reads s.result off the Run goroutine depends on this having
// happened: those goroutines write to it under s.mu for the whole run, and
// the final read is not taken under that lock.
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// openOverlapLog creates the shared log the task payloads append to.
func (s *Sim) openOverlapLog() error {
	dir := s.cfg.WorkerDir
	if dir == "" {
		var err error
		if dir, err = os.MkdirTemp("", "sim-overlap-"); err != nil {
			return fmt.Errorf("overlap log dir: %w", err)
		}
		s.overlapLogTemp = dir
	}
	s.overlapLog = filepath.Join(dir, "executions.log")
	f, err := os.Create(s.overlapLog)
	if err != nil {
		return fmt.Errorf("overlap log: %w", err)
	}
	return f.Close()
}

func (s *Sim) stopBackground() {
	s.linOnce.Do(func() {
		if s.stopLin != nil {
			s.stopLin()
		}
	})
}

func (s *Sim) collect() Result {
	// STOP THE BACKGROUND WORK FIRST, unconditionally.
	//
	// This used to happen only inside checkLinearizable, which returns early
	// when LinClients is zero — so a run with the recorded clients switched
	// off read s.result here while the coverage sampler was still writing to
	// it. The race detector found it the moment a test configured
	// LinClients=0, and it had been latent since the sampler was added: the
	// linearizability path was stopping the sampler as a side effect.
	s.stopBackground()

	// The apply site's observations become part of the Result here, on this
	// goroutine, rather than being written into it as they happen.
	s.foldObservations()

	// Snapshot costs, from every node that is still up. Collected here
	// rather than streamed, because the trend is read at the end.
	for _, id := range s.liveIDs() {
		if n, ok := s.nodeAt(id); ok {
			s.result.Snapshots = append(s.result.Snapshots, n.coord.Snapshots()...)
			s.result.LeadershipChanges = append(s.result.LeadershipChanges, n.coord.LeadershipChanges()...)
		}
	}

	s.result.Faults.Net = s.net.Stats()
	if n := s.anyNode(); n != nil {
		n.coord.ReadState(func(st *state.StateStore) {
			s.result.Jobs = st.JobCount()
			s.result.Tasks = st.TaskCount()
		})
	}
	s.result.Lin = s.checkLinearizable()
	// The ledger LAST: it reads committed state and the worker result trees,
	// both of which need the clients stopped and the cluster settled.
	s.result.Violations = append(s.result.Violations, s.checkLedger()...)
	s.result.Ledger = s.ledger
	return s.result
}

// checkLinearizable closes the history and runs porcupine over it.
//
// It stops the clients FIRST. Checking a history while operations are still
// landing in it would race the recorder, and worse, an operation invoked and
// not yet returned would be checked as if it had returned — which is the
// pending-op mistake rule 6 exists to prevent, arriving through the back door.
//
// Illegal is a VIOLATION. Unknown — the search hit its timeout — is
// INCONCLUSIVE, the same distinction MinComparisons draws on the convergence
// side: the checker did not find the system correct, it ran out of time
// deciding. Reporting that as a pass is how a verifier quietly stops
// verifying.
func (s *Sim) checkLinearizable() LinResult {
	if s.cfg.LinClients <= 0 {
		return LinResult{}
	}
	s.stopBackground()

	res := s.history.check(s.cfg.LinCheckTimeout, s.cfg.LinVizDir)

	// COVERAGE FIRST, and coverage means a SPLIT-BRAIN READ.
	//
	// This used to accept `partitioned || splitBrain`, which overstated
	// itself the moment CheckQuorum landed. An operation sent to a node that
	// has already stepped down comes back NOT_LEADER: it constrains nothing,
	// it cannot expose a stale answer, and counting it marked runs covered
	// that had no chance of failing. The first sweep under the shipped
	// configuration recorded 205 operations on partitioned nodes and ZERO
	// split-brain operations, and was reported as 20 covered runs.
	//
	// Only one shape can expose the bug: a READ answered by a node that
	// believed it led while another node also did. A run without one is a
	// partition test — worth having under its own name, and not evidence
	// about linearizability.
	res.Covered = res.TargetedReads > 0
	if res.Ops > 0 && !res.Covered {
		s.result.Violations = append(s.result.Violations, Violation{
			Seed: s.cfg.Seed, Step: s.result.Steps,
			Kind: KindLinearizabilityCoverage, Inconclusive: true,
			What: fmt.Sprintf(
				"history of %d operations contained no read that could have been stale "+
					"(%d ops on a partitioned node, %d under split brain, %d split-brain reads, "+
					"%d of them targeted) — this run cannot count toward a linearizability claim",
				res.Ops, res.Partitioned, res.SplitBrain, res.SplitBrainReads, res.TargetedReads),
		})
		return res
	}

	switch res.Verdict {
	case porcupine.Illegal:
		what := fmt.Sprintf("history of %d client operations (%d submits, %d gets, %d ambiguous, %.0f%%) is NOT LINEARIZABLE",
			res.Ops, res.Submits, res.Gets, res.Ambiguous, res.AmbiguousShare()*100)
		if res.VizPath != "" {
			what += " — timeline: " + res.VizPath
		}
		s.result.Violations = append(s.result.Violations,
			Violation{Seed: s.cfg.Seed, Step: s.result.Steps, Kind: KindLinearizability, What: what})
	case porcupine.Unknown:
		what := fmt.Sprintf("linearizability search did not finish in %s over %d operations — "+
			"no verdict, not a pass", s.cfg.LinCheckTimeout, res.Ops)
		if res.Empty {
			what = "no client operations were recorded, so linearizability was not checked at all — " +
				"no verdict, not a pass"
		}
		s.result.Violations = append(s.result.Violations, Violation{
			Seed: s.cfg.Seed, Step: s.result.Steps,
			Kind: KindLinearizabilityTimeout, Inconclusive: true, What: what,
		})
	}
	return res
}

func (s *Sim) shutdown() {
	// Workers first: they hold RPCs against coordinators that are about to
	// stop answering.
	if s.rw != nil {
		s.rw.stop()
	}
	for _, id := range s.liveIDs() {
		s.stop(id)
	}
	if s.api != nil {
		s.api.close()
	}
	if s.overlapLogTemp != "" {
		_ = os.RemoveAll(s.overlapLogTemp)
	}
	s.net.Close()
}

func (s *Sim) anyNode() *node {
	if n := s.leader(); n != nil {
		return n
	}
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	for _, n := range s.nodes {
		return n
	}
	return nil
}

// liveIDs is the sorted ids of the nodes that are up.
//
// Sorted and id-ordered, never a map range: the fault schedule is derived
// from the seed, and a choice made over Go's map iteration order is a choice
// the seed does not determine.
func (s *Sim) liveIDs() []uint64 {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	return s.liveIDsLocked()
}

func (s *Sim) liveIDsLocked() []uint64 {
	live := make([]uint64, 0, len(s.nodes))
	for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
		if _, up := s.nodes[id]; up {
			live = append(live, id)
		}
	}
	return live
}

// nodeAt returns a live node by id.
func (s *Sim) nodeAt(id uint64) (*node, bool) {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	n, ok := s.nodes[id]
	return n, ok
}

// pickLeaderClaimant chooses, at random, a node that BELIEVES it leads.
//
// This is the routing decision Day 9 turns on, and it sits between the two
// obvious choices, both of which are wrong here.
//
// s.leader() — what the rest of the workload uses — returns nil unless
// exactly one node claims leadership. That is right for offering work and
// blind for this: raft here runs without CheckQuorum and without PreVote, so
// a leader cut off from the majority never learns it, keeps its term, and
// keeps passing its own requireLeader guard while the majority elects a new
// leader at a higher term. s.leader() sees two claimants, returns nil, and
// stands down for the exact window the bug lives in.
//
// Picking uniformly from ALL live nodes is the other extreme, and the first
// run showed why it is wasteful: two thirds of operations hit followers,
// which refuse with NOT_LEADER, which the recorder must file as ambiguous
// because it cannot tell that refusal from one returned after a proposal.
// Six thousand of eleven thousand operations constrained nothing.
//
// Claimants only is what a real client pool does — it follows the leader
// hint — and it keeps the stale leader in the target set, because the stale
// leader is exactly a node that claims to lead. During a split it is a coin
// flip between the real leader and the stale one, which is the sampling this
// needs.
//
// It also returns whether more than one node claimed leadership at the
// moment of the choice, which is the coverage number that decides whether an
// Ok verdict is worth anything.
// splitBrainPair returns the two nodes a stale read needs: a claimant the
// network has CUT OFF, and a claimant it has not.
//
// Random claimant selection is the right default and the wrong tool for
// aiming. During a split brain it lands on the stale leader about half the
// time, and it has no way to write through the majority and read through the
// minority IN THAT ORDER — which is the only sequence that produces a
// provably stale answer rather than a possibly stale one.
//
// Returns nils unless both exist, which is most of the time.
func (s *Sim) splitBrainPair() (majority, cutOff *node) {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	for _, id := range s.liveIDsLocked() {
		n := s.nodes[id]
		if !n.coord.IsLeader() {
			continue
		}
		if s.net.IsPartitioned(id) {
			cutOff = n
		} else {
			majority = n
		}
	}
	if majority == nil || cutOff == nil {
		return nil, nil
	}
	return majority, cutOff
}

func (s *Sim) pickLeaderClaimant(rng *rand.Rand) (*node, bool) {
	s.nodesMu.RLock()
	var claimants []*node
	for _, id := range s.liveIDsLocked() {
		if n := s.nodes[id]; n.coord.IsLeader() {
			claimants = append(claimants, n)
		}
	}
	s.nodesMu.RUnlock()

	if len(claimants) == 0 {
		return nil, false
	}
	return claimants[rng.Intn(len(claimants))], len(claimants) > 1
}

// applyFault picks one fault from the seeded RNG and applies it.
//
// The weights matter: most steps should be quiet, because a cluster that is
// never allowed to make progress proves nothing. The interesting bugs live
// in RECOVERY, which needs the system to get far enough to have something to
// recover.
// Day 9 changed this mix, and therefore changed what every seed replays. A
// seed from before Day 9 no longer reproduces the schedule it used to; the
// property that survives is the one that matters, that a seed reproduces ITS
// OWN schedule.
func (s *Sim) applyFault(step int) {
	// Protect the injected window, when the caller asked for that. The roll
	// is still drawn so the fault sequence stays reproducible; only the
	// destructive branches are skipped.
	protect := s.cfg.DriveWindow && s.windowPending

	switch roll := s.rng.Intn(100); {
	case roll < 45:
		// Quiet step. Let the cluster converge and do work.
		s.heal(step)

	case roll < 58:
		// Partition a minority. A minority CANNOT elect or commit, so this
		// tests that the majority carries on and the minority rejoins.
		if protect {
			return
		}
		victim := s.pickNode()
		if victim == 0 {
			return
		}
		s.net.Partition(victim)
		s.result.Faults.Partitions++
		s.noteFault(step, "partition node %d", victim)

	case roll < 70:
		if protect {
			return
		}
		// PARTITION THE LEADER, and hold it. This is the targeted adversary
		// Day 9 needs, and the reason it had to be added: a random victim is
		// the leader one time in three, and the first linearizability runs
		// recorded ZERO operations against a cut-off leader across a whole
		// 25-step schedule. The verdict was Ok and meant nothing.
		//
		// The hold matters as much as the target. Raft here runs without
		// CheckQuorum and without PreVote, so the cut-off leader keeps its
		// term and keeps answering reads, while the majority needs a full
		// election timeout to elect a successor and then has to commit
		// something new before any read of it could be stale. A partition
		// that the next quiet step heals is over before that sequence can
		// finish.
		lead := s.leader()
		if lead == nil {
			return
		}
		s.net.Partition(lead.id)
		s.hold(lead.id, step)
		s.result.Faults.Partitions++
		s.result.Faults.LeaderPartitions++
		s.noteFault(step, "partition LEADER node %d, held until step %d",
			lead.id, s.heldUntil[lead.id])

	case roll < 80:
		// Heal, and count it: a partition that is never healed is just a
		// smaller cluster.
		if s.heal(step) {
			s.result.Faults.Heals++
			s.noteFault(step, "heal")
		}

	case roll < 93:
		// Kill a node, but never below quorum: with 3 nodes at most one may
		// be down, or the cluster stops being able to commit at all and the
		// rest of the run tests nothing.
		//
		// Counted over LIVE nodes deliberately, not reachable ones. A kill
		// plus a partition CAN leave the cluster without a quorum, and that
		// is a real fault state the claim has to cover — guarding against
		// it here would narrow the adversary to make an invariant quieter,
		// which is a selection effect, not a fix. The invariants are what
		// know that no quorum means no progress is expected.
		if protect || len(s.liveIDs()) <= quorum(s.cfg.Nodes) {
			return
		}
		victim := s.pickNode()
		if victim == 0 {
			return
		}
		s.stop(victim)
		s.result.Faults.NodeKills++
		s.noteFault(step, "kill node %d", victim)

	default:
		// Restart something that is down, from its own disk.
		for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
			if _, up := s.nodeAt(id); !up {
				if err := s.start(id); err == nil {
					s.result.Faults.NodeRestarts++
					s.noteFault(step, "restart node %d", id)
				}
				return
			}
		}
	}
}

// hold marks a node's partition as deliberately held, until
// LeaderPartitionSteps steps from now.
//
// The hold exists because the quiet step heals, and quiet is the most common
// step. A leader partition that is not protected lasts, on average, under two
// steps and frequently exactly none — which is how the first runs managed two
// partitions and zero operations against a cut-off leader.
func (s *Sim) hold(id uint64, step int) {
	if s.heldUntil == nil {
		s.heldUntil = map[uint64]int{}
	}
	s.heldUntil[id] = step + s.cfg.LeaderPartitionSteps
}

// heal releases every partition that is not being deliberately held, and
// reports whether it released anything.
//
// It names the nodes it releases rather than clearing the set, which is the
// whole point of additive isolation: a heal rolled by the schedule must not
// end a hold that has steps left to run.
func (s *Sim) heal(step int) bool {
	var release, holding []uint64
	for _, id := range s.net.PartitionedIDs() {
		if until, ok := s.heldUntil[id]; ok && step < until {
			holding = append(holding, id)
			continue
		}
		delete(s.heldUntil, id)
		release = append(release, id)
	}
	if len(holding) > 0 {
		s.logf("step %d: holding partition of %v", step, holding)
	}
	if len(release) == 0 {
		return false
	}
	s.net.HealNodes(release...)
	return true
}

// healAll releases every partition and abandons every hold.
//
// This is the explicit whole-network heal the decided semantics ask for, and
// its callers are the ones entitled to it: quiescence, the end-of-run
// convergence check, and targeted mode restoring full membership before it
// opens its window. A fault schedule never calls it.
func (s *Sim) healAll() {
	s.net.HealAll()
	s.heldUntil = map[uint64]int{}
}

// releaseExpiredHolds ends a hold at its own scheduled step, independently of
// whether the schedule happens to roll a heal.
//
// Called once per step, before the fault roll, and it draws no randomness —
// the schedule's draw order is what makes a seed replay, so a new event in
// the loop must not consume from the RNG.
func (s *Sim) releaseExpiredHolds(step int) {
	var expired []uint64
	for id, until := range s.heldUntil {
		if step >= until {
			expired = append(expired, id)
		}
	}
	slices.Sort(expired)
	for _, id := range expired {
		delete(s.heldUntil, id)
	}
	if len(expired) == 0 {
		return
	}
	s.net.HealNodes(expired...)
	s.result.Faults.Heals++
	s.noteFault(step, "hold expired, healing node(s) %v", expired)
}

// pauseLeader freezes the leader: its tickers stop and its inbound raft
// messages queue instead of arriving.
//
// This is the fault that defeats CheckQuorum, and it defeats it for a reason
// worth stating: CheckQuorum is driven by TICKS, so a leader that is not
// running never performs the check and is still convinced it leads when it
// wakes. Nothing here is a partition — the messages are held, not dropped —
// because a process that is merely descheduled has its packets buffered by
// the kernel, and that queueing is what preserves the deposed-but-unaware
// state the stale read needs.
// replayTarget returns the node a replay plan names, if it is up.
func (s *Sim) replayTarget() *node {
	if s.cfg.ReplayWindow == nil || !s.cfg.ReplayWindow.Fired {
		return nil
	}
	n, _ := s.nodeAt(s.cfg.ReplayWindow.NodeID)
	return n
}

func (s *Sim) pauseLeader(step int) bool {
	lead := s.replayTarget()
	if lead == nil {
		lead = s.leader()
	}
	if lead == nil {
		s.logf("step %d: pause deferred, no single leader", step)
		return false
	}
	lead.clk.Pause()
	s.net.Hold(lead.id)
	s.paused = lead
	s.pauseUntilStep = step + s.cfg.PauseSteps
	s.result.Window = WindowPlan{Fired: true, Step: step, NodeID: lead.id, Paused: true}
	s.result.Faults.Pauses++
	s.noteFault(step, "PAUSE leader node %d (ticker frozen, inbound held) until step %d",
		lead.id, s.pauseUntilStep)
	return true
}

// wakeIfDue resumes a paused node once its scheduled pause has elapsed.
//
// THE ORDER IS THE EXPERIMENT. The ticker is unfrozen and the targeted read
// is issued BEFORE the queued raft messages are released, because that gap is
// the GC-pause window: the process is running again, has not yet drained the
// queue that would tell it a newer leader exists, and answers from state it
// still believes current. Releasing first would hand it the higher term, step
// it down, and close the window before the read.
func (s *Sim) wakeIfDue(step int) {
	if s.paused == nil || step < s.pauseUntilStep {
		return
	}
	woken := s.paused
	s.paused = nil
	s.pauseUntilStep = 0

	// 1. The process is scheduled again.
	woken.clk.Resume()
	s.noteFault(step, "WAKE node %d — reading before its queued messages are released", woken.id)

	// 2. Read from it while it is still unaware. One targeted read against
	// the freshest job the majority committed during the pause.
	//
	// Each way this can fall through is logged, because "the pause covered
	// 4 of 8" is not a diagnosis.
	lead := s.majorityLeaderExcept(woken.id)
	s.result.Pause.Woke = step
	s.result.Pause.MajorityElected = lead != nil
	s.result.Pause.WokenStillLeader = woken.coord.IsLeader()

	switch {
	case lead == nil:
		s.logf("step %d: MISS no majority leader at wake — the majority never elected during the pause", step)
		for _, id := range s.liveIDs() {
			if n, ok := s.nodeAt(id); ok {
				st := n.coord.RaftStatus()
				s.logf("    node %d: leader=%v term=%d raftState=%v lead=%d",
					id, n.coord.IsLeader(), st.Term, st.State, st.Lead)
			}
		}
	case !woken.coord.IsLeader():
		s.logf("step %d: MISS node %d no longer claims leadership at wake", step, woken.id)
	default:
		s.targetedReadAfterWake(woken, lead)
	}

	// The window is spent; the schedule may resume killing.
	s.windowPending = false

	// 3. Only now does it learn what happened.
	s.net.Release(woken.id)
	s.logf("step %d: released node %d's queued messages", step, woken.id)
}

// pausedNode reports whether this node is inside its scheduled pause.
//
// Scoped to the SCHEDULED pause, not to "has a frozen clock". That is what
// bounds the invariant's exemption: a node whose clock is frozen without the
// schedule having paused it is still a violation.
func (s *Sim) pausedNode(id uint64) bool {
	return s.paused != nil && s.paused.id == id
}

// majorityLeaderExcept finds a leader other than the given node.
func (s *Sim) majorityLeaderExcept(except uint64) *node {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	for _, id := range s.liveIDsLocked() {
		if id == except {
			continue
		}
		if n := s.nodes[id]; n.coord.IsLeader() {
			return n
		}
	}
	return nil
}

// injectTargetedWindow opens the split-brain window on purpose.
//
// A targeted workload that waits for the schedule to roll a leader partition
// is not targeted. Measured: against the pinned adversary the workload caught
// the bug in every run that reached the window and in none that did not — six
// of twelve seeds, the other six never having opened a window at all. The
// detector was perfect and the OPPORTUNITY was the limit, so the opportunity
// is what gets fixed, here rather than by hoping longer runs stumble into it.
//
// Held for the same count as a scheduled leader partition, which must span an
// election timeout plus enough time for the new leader to commit — otherwise
// there is nothing committed on the majority side for a stale read to miss.
func (s *Sim) injectTargetedWindow(step int) {
	if s.targetedPartitionStep == 0 || step != s.targetedPartitionStep {
		return
	}

	// THE MAJORITY SIDE MUST BE ABLE TO ELECT, or there is no window.
	//
	// Cutting the leader off only produces two claimants if what remains
	// can form a quorum and choose a successor. With three nodes that needs
	// all three up: the schedule kills one often enough that this was the
	// whole difference between a run that covered the window and one that
	// did not. Seeds 2, 5 and 6 each killed a node first, so partitioning
	// the leader left ONE reachable node, which can never elect — and the
	// run recorded zero targeted reads while looking like the injection had
	// worked.
	//
	// So targeted mode restores full membership first and opens the window
	// on the step after. This overrides the fault schedule's kill, which is
	// the point: the injected window is the run's one guaranteed
	// opportunity, and a schedule that happens to have killed a node must
	// not silently consume it.
	if len(s.liveIDs()) < s.cfg.Nodes {
		for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
			if _, up := s.nodeAt(id); !up {
				if err := s.start(id); err == nil {
					s.result.Faults.NodeRestarts++
					s.logf("step %d: targeted mode restarting node %d to make a window possible", step, id)
				}
			}
		}
		s.healAll()
		s.targetedPartitionStep = step + 1
		return
	}

	// A PAUSE instead of a partition, when asked for. The pause is the
	// adversary that works against the shipped configuration, because
	// CheckQuorum cannot step down a process that is not running its ticks.
	if s.cfg.PauseLeader {
		if !s.pauseLeader(step) {
			s.targetedPartitionStep = step + 1
		}
		return
	}

	lead := s.replayTarget()
	if lead == nil {
		lead = s.leader()
	}
	if lead == nil {
		// Mid-election. Try again next step rather than giving up on the
		// run's only guaranteed window.
		s.targetedPartitionStep = step + 1
		return
	}
	s.net.Partition(lead.id)
	s.hold(lead.id, step)
	s.windowPending = false
	s.result.Window = WindowPlan{Fired: true, Step: step, NodeID: lead.id}
	s.result.Faults.Partitions++
	s.result.Faults.LeaderPartitions++
	s.noteFault(step, "TARGETED partition of LEADER node %d, held until step %d",
		lead.id, s.heldUntil[lead.id])
}

func quorum(n int) int { return n/2 + 1 }

// reachableCount is how many nodes can currently form a quorum together:
// live AND not partitioned.
//
// Used by the INVARIANTS, never by fault selection. Holding a leader
// partition for three steps made "live" and "available" different things,
// and one seed then killed the partitioned leader and partitioned a second
// node, leaving one reachable node of three. Nothing could commit and the
// frozen-clock invariant fired on a cluster that was behaving correctly.
//
// The first fix was to stop the adversary creating that state, which was
// wrong: below quorum is a real fault state and the claim has to cover it.
// A cluster with no quorum makes no progress BY DESIGN, so what needed
// changing is the invariant that read a correctly-stalled clock as a defect.
func (s *Sim) reachableCount() int {
	n := 0
	for _, id := range s.liveIDs() {
		if !s.net.IsPartitioned(id) {
			n++
		}
	}
	return n
}

// hasQuorum reports whether enough nodes can currently reach each other to
// commit anything at all.
func (s *Sim) hasQuorum() bool {
	return s.reachableCount() >= quorum(s.cfg.Nodes)
}

// pickNode chooses a live node deterministically from the seed. Sorted ids,
// because ranging a map would make the choice depend on Go's map iteration
// order and the seed would stop determining the schedule.
func (s *Sim) pickNode() uint64 {
	live := s.liveIDs()
	if len(live) == 0 {
		return 0
	}
	return live[s.rng.Intn(len(live))]
}
