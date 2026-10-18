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
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

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
	Out         io.Writer
}

func DefaultConfig() Config {
	return Config{
		Nodes:           3,
		Steps:           40,
		StepTime:        150 * time.Millisecond,
		TickInterval:    10 * time.Millisecond,
		ElectionTick:    10,
		LeaseTTL:        300 * time.Millisecond,
		TickPeriod:      20 * time.Millisecond,
		SnapshotEvery:   50,
		ProposalTimeout: 250 * time.Millisecond,
		Faults:          simnet.Faults{DropRate: 0.02, MinDelay: 0, MaxDelay: 5 * time.Millisecond},
		JobsPerStep:     1,
		TasksPerJob:     3,
		WorkerCount:     3,
		WorkerSlots:     2,
	}
}

// Result is what a run produces.
type Result struct {
	Seed       int64
	Steps      int
	Violations []Violation
	Faults     FaultCounts
	Jobs       int
	Tasks      int
}

// FaultCounts is the chaos actually induced, not the chaos configured.
type FaultCounts struct {
	NodeKills    int
	NodeRestarts int
	Partitions   int
	Heals        int
	Net          simnet.Stats
}

// Violation is an invariant that failed, with the seed and step needed to get
// back to it.
type Violation struct {
	Seed int64
	Step int
	What string
}

func (v Violation) Error() string {
	return fmt.Sprintf("seed %d step %d: %s", v.Seed, v.Step, v.What)
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
	nodes map[uint64]*node
	dirs  map[uint64]string
	peers map[uint64]string

	mu     sync.Mutex
	result Result
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
	cfg := server.DefaultConfig()
	cfg.LeaseTTL = s.cfg.LeaseTTL
	cfg.TickPeriod = s.cfg.TickPeriod
	cfg.Sched.LeaseTTLMs = s.cfg.LeaseTTL.Milliseconds()
	cfg.Sched.Workers = 2
	cfg.ProposalTimeout = s.cfg.ProposalTimeout
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
	rcfg.Transport = s.net.Transport(id)

	if err := coord.StartRaft(rcfg); err != nil {
		return fmt.Errorf("start node %d: %w", id, err)
	}
	s.net.SetDown(id, false)
	s.nodes[id] = &node{id: id, coord: coord, svc: server.NewService(coord), dir: s.dirs[id]}
	return nil
}

func (s *Sim) stop(id uint64) {
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

	for step := 1; step <= s.cfg.Steps; step++ {
		select {
		case <-ctx.Done():
			s.result.Steps = step - 1
			return s.collect(), ctx.Err()
		default:
		}

		s.applyFault(step)
		s.offerWork(step)
		s.drainSomeWork()
		time.Sleep(s.cfg.StepTime)

		s.result.Steps = step
		if vs := s.checkInvariants(step); len(vs) > 0 {
			s.result.Violations = append(s.result.Violations, vs...)
			return s.collect(), vs[0]
		}
	}

	// Heal everything and give the cluster a chance to settle, so the final
	// check is against a converged cluster rather than mid-chaos. A system
	// that is briefly inconsistent under partition is expected; one that
	// stays inconsistent after healing is broken.
	s.net.Heal()
	for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
		if _, up := s.nodes[id]; !up {
			_ = s.start(id)
			s.result.Faults.NodeRestarts++
		}
	}
	s.awaitLeader(10 * time.Second)
	time.Sleep(10 * s.cfg.StepTime)

	if vs := s.checkConverged(); len(vs) > 0 {
		s.result.Violations = append(s.result.Violations, vs...)
		return s.collect(), vs[0]
	}
	return s.collect(), nil
}

func (s *Sim) collect() Result {
	s.result.Faults.Net = s.net.Stats()
	if n := s.anyNode(); n != nil {
		n.coord.ReadState(func(st *state.StateStore) {
			s.result.Jobs = st.JobCount()
			s.result.Tasks = st.TaskCount()
		})
	}
	return s.result
}

func (s *Sim) shutdown() {
	for id := range s.nodes {
		s.stop(id)
	}
	s.net.Close()
}

func (s *Sim) anyNode() *node {
	if n := s.leader(); n != nil {
		return n
	}
	for _, n := range s.nodes {
		return n
	}
	return nil
}

// applyFault picks one fault from the seeded RNG and applies it.
//
// The weights matter: most steps should be quiet, because a cluster that is
// never allowed to make progress proves nothing. The interesting bugs live
// in RECOVERY, which needs the system to get far enough to have something to
// recover.
func (s *Sim) applyFault(step int) {
	switch roll := s.rng.Intn(100); {
	case roll < 45:
		// Quiet step. Let the cluster converge and do work.
		s.net.Heal()

	case roll < 65:
		// Partition a minority. A minority CANNOT elect or commit, so this
		// tests that the majority carries on and the minority rejoins.
		victim := s.pickNode()
		if victim == 0 {
			return
		}
		s.net.Partition(victim)
		s.result.Faults.Partitions++
		s.logf("step %d: partition node %d", step, victim)

	case roll < 80:
		// Heal, and count it: a partition that is never healed is just a
		// smaller cluster.
		s.net.Heal()
		s.result.Faults.Heals++
		s.logf("step %d: heal", step)

	case roll < 93:
		// Kill a node, but never below quorum: with 3 nodes at most one may
		// be down, or the cluster stops being able to commit at all and the
		// rest of the run tests nothing.
		if len(s.nodes) <= quorum(s.cfg.Nodes) {
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
			if _, up := s.nodes[id]; !up {
				if err := s.start(id); err == nil {
					s.result.Faults.NodeRestarts++
					s.logf("step %d: restart node %d", step, id)
				}
				return
			}
		}
	}
}

func quorum(n int) int { return n/2 + 1 }

// pickNode chooses a live node deterministically from the seed. Sorted ids,
// because ranging a map would make the choice depend on Go's map iteration
// order and the seed would stop determining the schedule.
func (s *Sim) pickNode() uint64 {
	live := make([]uint64, 0, len(s.nodes))
	for id := uint64(1); id <= uint64(s.cfg.Nodes); id++ {
		if _, up := s.nodes[id]; up {
			live = append(live, id)
		}
	}
	if len(live) == 0 {
		return 0
	}
	return live[s.rng.Intn(len(live))]
}
