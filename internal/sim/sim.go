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
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"

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
	// LeaderPartitionSteps is how many steps a leader partition is held
	// against healing. It must span an election timeout plus enough time
	// for the new leader to commit, or the window closes before a read of
	// anything newly committed could be stale.
	LeaderPartitionSteps int

	Out io.Writer
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
	}
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
}

// FaultCounts is the chaos actually induced, not the chaos configured.
type FaultCounts struct {
	NodeKills    int
	NodeRestarts int
	Partitions   int
	// LeaderPartitions is how many of those cut off the node that was
	// leading, which is the only kind that can produce a stale read.
	LeaderPartitions int
	Heals            int
	Net              simnet.Stats
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

// IsLinearizability reports whether this kind is one of the two the
// linearizability check produces. Tests that are not about linearizability
// use it to ignore exactly those.
func (k ViolationKind) IsLinearizability() bool {
	return k == KindLinearizability || k == KindLinearizabilityTimeout
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
}

// Sim is a running simulation.
type Sim struct {
	cfg   Config
	rng   *rand.Rand
	net   *simnet.Network
	dirs  map[uint64]string
	peers map[uint64]string

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
	// holdHeal counts down the steps during which healing is refused, so a
	// deliberately-held partition is not undone by the next quiet step.
	holdHeal int

	mu     sync.Mutex
	result Result

	// Coverage bookkeeping, sampled between steps on the Run goroutine.
	seenBackoff  map[string]bool
	seenRetried  map[string]bool
	seenFailed   map[string]bool
	lastSessions int
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

		seenBackoff:  map[string]bool{},
		seenRetried:  map[string]bool{},
		seenFailed:   map[string]bool{},
		lastSessions: -1,

		history: newHistory(cfg.LinMaxOps),
	}
	s.result.Seed = cfg.Seed
	for i := 1; i <= cfg.Nodes; i++ {
		// Addresses are labels only: simnet routes by node id, so nothing
		// binds a port and a long run cannot exhaust them.
		s.peers[uint64(i)] = fmt.Sprintf("sim://node-%d", i)
	}
	return s
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
	// Each node gets its own stream off the run's seed, so the ids a node
	// mints are reproducible without every node drawing from one shared
	// sequence whose order depends on goroutine scheduling.
	cfg.Rand = &seededReader{rng: rand.New(rand.NewSource(s.cfg.Seed + int64(id)))}

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
	// Derived from the election timeout by raftnode when zero; left zero
	// here so the simulator inherits the same "strictly below the election
	// timeout" rule the production default uses.
	rcfg.Transport = s.net.Transport(id)

	if err := coord.StartRaft(rcfg); err != nil {
		return fmt.Errorf("start node %d: %w", id, err)
	}
	s.net.SetDown(id, false)
	s.nodes[id] = &node{id: id, coord: coord, svc: server.NewService(coord), dir: s.dirs[id]}
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

func (s *Sim) logf(format string, args ...any) {
	fmt.Fprintf(s.cfg.Out, format+"\n", args...)
}

// Run executes the whole schedule and returns what happened.
//
// Every step: pick a fault from the seeded RNG, apply it, offer some work,
// let the cluster run, then CHECK INVARIANTS. Checking after every step is
// what turns "it crashed eventually" into "it broke at step 17 of seed 42".
func (s *Sim) Run(ctx context.Context) (Result, error) {
	defer s.shutdown()

	for i := 1; i <= s.cfg.Nodes; i++ {
		if err := s.start(uint64(i)); err != nil {
			return s.result, err
		}
	}
	if s.awaitLeader(10*time.Second) == nil {
		return s.result, fmt.Errorf("seed %d: no leader within 10s of bringing the cluster up", s.cfg.Seed)
	}
	s.registerWorkers()

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
		s.applyFault(step)
		s.offerWork(step)
		s.drainSomeWork()
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

	// Heal everything and give the cluster a chance to settle, so the final
	// check is against a converged cluster rather than mid-chaos. A system
	// that is briefly inconsistent under partition is expected; one that
	// stays inconsistent after healing is broken.
	s.net.Heal()
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

func (s *Sim) collect() Result {
	s.result.Faults.Net = s.net.Stats()
	if n := s.anyNode(); n != nil {
		n.coord.ReadState(func(st *state.StateStore) {
			s.result.Jobs = st.JobCount()
			s.result.Tasks = st.TaskCount()
		})
	}
	s.result.Lin = s.checkLinearizable()
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
	s.linOnce.Do(func() {
		if s.stopLin != nil {
			s.stopLin()
		}
	})

	res := s.history.check(s.cfg.LinCheckTimeout, s.cfg.LinVizDir)
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
	for _, id := range s.liveIDs() {
		s.stop(id)
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
	switch roll := s.rng.Intn(100); {
	case roll < 45:
		// Quiet step. Let the cluster converge and do work.
		s.heal(step)

	case roll < 58:
		// Partition a minority. A minority CANNOT elect or commit, so this
		// tests that the majority carries on and the minority rejoins.
		//
		// Refused when it would leave no quorum: combined with a kill or an
		// existing partition, one more cut can take the reachable set below
		// a majority, and then nothing commits for the rest of the run.
		if s.wouldBreakQuorum() {
			return
		}
		victim := s.pickNode()
		if victim == 0 {
			return
		}
		s.net.Partition(victim)
		s.result.Faults.Partitions++
		s.logf("step %d: partition node %d", step, victim)

	case roll < 70:
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
		if s.wouldBreakQuorum() {
			return
		}
		lead := s.leader()
		if lead == nil {
			return
		}
		s.net.Partition(lead.id)
		s.holdHeal = s.cfg.LeaderPartitionSteps
		s.result.Faults.Partitions++
		s.result.Faults.LeaderPartitions++
		s.logf("step %d: partition LEADER node %d, held for %d steps", step, lead.id, s.holdHeal)

	case roll < 80:
		// Heal, and count it: a partition that is never healed is just a
		// smaller cluster.
		if s.heal(step) {
			s.result.Faults.Heals++
			s.logf("step %d: heal", step)
		}

	case roll < 93:
		// Kill a node, but never below quorum: with 3 nodes at most one may
		// be unavailable, or the cluster stops being able to commit at all
		// and the rest of the run tests nothing.
		//
		// Counted over REACHABLE nodes, not live ones. A node that is live
		// but partitioned cannot help form a quorum, so killing a second
		// node while one is cut off breaks the very guarantee this guard
		// exists to provide.
		if s.wouldBreakQuorum() {
			return
		}
		victim := s.pickNode()
		if victim == 0 {
			return
		}
		s.stop(victim)
		s.result.Faults.NodeKills++
		s.logf("step %d: kill node %d", step, victim)

	default:
		// Restart something that is down, from its own disk.
		for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
			if _, up := s.nodeAt(id); !up {
				if err := s.start(id); err == nil {
					s.result.Faults.NodeRestarts++
					s.logf("step %d: restart node %d", step, id)
				}
				return
			}
		}
	}
}

// heal removes every partition unless one is being deliberately held, and
// reports whether it actually healed.
//
// The hold exists because the quiet step heals, and quiet is the most common
// step. A leader partition that is not protected lasts, on average, under two
// steps and frequently exactly none — which is how the first runs managed two
// partitions and zero operations against a cut-off leader.
func (s *Sim) heal(step int) bool {
	if s.holdHeal > 0 {
		s.holdHeal--
		s.logf("step %d: holding partition (%d steps left)", step, s.holdHeal)
		return false
	}
	s.net.Heal()
	return true
}

func quorum(n int) int { return n/2 + 1 }

// reachableCount is how many nodes can currently form a quorum together:
// live AND not partitioned.
//
// Liveness alone is not the right count, and Day 9 is what exposed that. The
// kill branch guarded on live nodes, which was correct while partitions
// lasted less than a step; holding a leader partition for three steps made
// "live" and "available" different things. Seed 9005 then killed the
// partitioned leader and partitioned a second node, leaving ONE reachable
// node out of three — below quorum, nothing could commit, and the
// frozen-clock invariant fired on a cluster that was behaving correctly.
//
// The harness documents that it never drives the cluster below quorum,
// because a cluster that cannot commit stops testing anything. This is the
// count that makes that true.
func (s *Sim) reachableCount() int {
	n := 0
	for _, id := range s.liveIDs() {
		if !s.net.IsPartitioned(id) {
			n++
		}
	}
	return n
}

// wouldBreakQuorum reports whether removing one more node from the reachable
// set would leave the cluster unable to commit.
func (s *Sim) wouldBreakQuorum() bool {
	return s.reachableCount()-1 < quorum(s.cfg.Nodes)
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
