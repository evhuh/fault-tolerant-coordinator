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
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/clock"
	"github.com/evazheng/fault-tolerant-coordinator/internal/raftnode"
	"github.com/evazheng/fault-tolerant-coordinator/internal/sched"
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
	// Sched tunes the scheduling pipeline.
	Sched sched.Config
	// ProposalTimeout bounds a caller's wait for its entry to commit. Zero
	// means the default.
	ProposalTimeout time.Duration
	// StatePolicy holds the durations the state machine measures against its
	// own clock. ONE PER CLUSTER, identical on every replica — see
	// state.Policy. Zero means production defaults.
	//
	// Deliberately not a coordinator flag: the simulator compresses it so
	// session expiry and retry backoff actually occur inside a sub-second
	// run, and nothing else should be changing it.
	StatePolicy state.Policy
	// Rand is the source for generated ids. Nil means crypto/rand. The
	// simulator seeds it so a reproduced run yields the same ids and two
	// runs can be diffed.
	//
	// It is NOT a security boundary: ids only need to be unique, and in a
	// simulator they need to be repeatable, which crypto/rand cannot be.
	Rand io.Reader
	// Clock reads wall time for the DRIVERS: the Tick proposer, long-poll
	// deadlines. Nil means the real one. The apply path has no clock at all
	// and never will — that is what Tick is for.
	Clock clock.Clock
	// APIAddrs maps a raft node id to that node's gRPC address.
	//
	// It exists because the two address spaces are DIFFERENT: the raft peer
	// set holds transport URLs (http://host:9021) that only Raft speaks, while
	// a redirected client needs host:8080 to dial gRPC. Handing a client the
	// raft address would point it at a port that does not serve its API.
	APIAddrs map[uint64]string
}

func DefaultConfig() Config {
	return Config{
		LeaseTTL:       2000 * time.Millisecond, // 4 missed heartbeats at H=500ms
		TickPeriod:     200 * time.Millisecond,
		CommandVersion: state.CommandVersionPartialPlan,
		Sched:          sched.DefaultConfig(),
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

	rand io.Reader
	// clk is this node's view of wall time. Everything inside the state
	// machine reads Tick-carried timestamps instead.
	clk clock.Clock
	// now is what the Tick proposer stamps into commands. Kept separate from
	// clk so tests can drive the state machine's clock without also
	// controlling tickers and timeouts.
	now func() time.Time

	// The scheduling pipeline. LEADER-ONLY and in-memory: none of it is
	// replicated, and it starts on election and stops on leadership loss.
	// Nil until StartScheduling, which is what keeps the state machine
	// usable on its own in tests.
	//
	// schedMu guards these three POINTERS, not the objects behind them.
	// StartScheduling and StopScheduling run on the Ready-loop goroutine
	// (leadership changes are delivered from there), while BrokerStats and
	// Planner are read by RPC handlers and, from Day 11, a metrics endpoint.
	// Different goroutines, so the pointers need a lock even though nothing
	// in the current tests happens to exercise both paths at once.
	schedMu   sync.Mutex
	broker    *sched.Broker
	planner   *Planner
	scheduler *sched.Scheduler

	// Raft. Nil until StartRaft, which keeps the whole stack runnable as a
	// single node — every test below Day 6 depends on that.
	raft     *raftnode.Node
	proposer *raftProposer
	// leaderCancel stops the leader-only goroutines on revocation.
	leaderCancel context.CancelFunc

	// stalls records Ready-loop stalls. A non-empty list means something on
	// that goroutine blocked on work the goroutine itself serves.
	stallMu sync.Mutex
	stalls  []time.Duration

	// fatalErr records an unrecoverable failure that halted this node.
	fatalOnce sync.Once
	fatalMu   sync.Mutex
	fatalErr  error
}

func New(cfg Config) *Coordinator {
	clk := clock.OrReal(cfg.Clock)
	rnd := cfg.Rand
	if rnd == nil {
		rnd = cryptorand.Reader
	}
	return &Coordinator{
		cfg:   cfg,
		store: state.NewWithPolicy(cfg.StatePolicy),
		clk:   clk,
		rand:  rnd,
		now:   clk.Now,
	}
}

func (c *Coordinator) nowMs() int64 { return c.now().UnixMilli() }

// propose is the single mutation path. Every gRPC handler funnels through it.
//
// Day 6: replace the body with node.Propose(encoded) and block until the
// entry is applied from rd.CommittedEntries. The signature stays.
func (c *Coordinator) propose(cmd *commandpb.Command) (state.Result, error) {
	if c.raft != nil {
		// Through consensus: the entry commits, comes back via
		// CommittedEntries, and applyCommitted hands the Result back here.
		return c.proposeRaft(cmd)
	}
	return c.proposeLocal(cmd)
}

// proposeLocal applies directly, with no consensus.
//
// This is what propose() was through Day 5, and it stays because the state
// machine, the scheduler, and every test below Day 6 are meaningful without
// Raft. Day 6 did not delete the single-node path; it added a second one.
func (c *Coordinator) proposeLocal(cmd *commandpb.Command) (state.Result, error) {
	cmd.Version = c.cfg.CommandVersion
	c.mu.Lock()
	res, err := c.store.Apply(cmd)
	c.mu.Unlock()
	if err != nil {
		return res, err
	}
	// Outside the lock, deliberately: afterApply proposes further commands
	// and touches the broker, and doing either under c.mu would deadlock.
	c.afterApply(cmd, res)
	return res, nil
}

// afterApply is the leader's reaction to state it just committed.
//
// Two things happen here, and both are LEADER-ONLY. Followers compute the
// identical Result and discard it — that is why the state machine returns
// EvalTriggers instead of acting on them.
//
// Day 6 moves this to the Ready loop, where it runs on committed entries
// rather than on a direct Apply, guarded by whether this node leads.
func (c *Coordinator) afterApply(cmd *commandpb.Command, res state.Result) {
	broker := c.brokerRef()
	if broker == nil {
		return // scheduling pipeline not started (tests, or a follower)
	}

	// (1) Every trigger becomes an evaluation. The id is DERIVED from what
	// caused it, never random: applyCreateEval is idempotent only on a
	// repeated eval_id, so a random one would duplicate on re-propose.
	for _, trig := range res.EvalTriggers {
		id := evalIDFor(trig)
		prio := c.priorityOf(trig.JobID)
		// FIRE-AND-FORGET, and it has to be: under Raft, afterApply runs
		// inside the apply loop. Waiting for this entry to commit would wait
		// on the very loop that is sitting here — a guaranteed deadlock.
		//
		// Losing one is survivable: the broker is derived, so a new leader
		// rediscovers the work by scanning committed state.
		c.proposeAsync(createEvalCmd(id, trig, prio, c.storeNowMs()))
		broker.Enqueue(sched.Eval{
			ID:          id,
			JobID:       trig.JobID,
			Priority:    prio,
			TriggeredBy: trig.Reason,
			CreatedMs:   c.storeNowMs(),
		})
	}

	// (2) Capacity changed. CompleteTask emits no trigger — finishing a task
	// is not a reason to re-think a job — but it IS a reason to re-think the
	// jobs that had nowhere to run. Without this, blocked evals never wake
	// and pending tasks starve.
	//
	// A worker JOINING is new capacity too, so it unblocks as well. But only
	// a genuine admission: at one heartbeat per worker per 500ms, unblocking
	// on every lease renewal would make blocked evals a polling loop.
	switch cmd.GetKind().(type) {
	case *commandpb.Command_CompleteTask, *commandpb.Command_FailTask:
		broker.Unblock()
	case *commandpb.Command_RegisterWorker:
		if res.WorkerAdmitted {
			broker.Unblock()
		}
	}
}

func (c *Coordinator) storeNowMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.NowMs()
}

// priorityOf reads a job's priority so an eval inherits it. A missing job
// sorts at zero rather than erroring: the eval is about to be rejected by
// applyCreateEval anyway.
func (c *Coordinator) priorityOf(jobID string) int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if j, ok := c.store.Job(jobID); ok {
		return j.Priority
	}
	return 0
}

// Snapshot satisfies sched.Snapshotter.
func (c *Coordinator) Snapshot() *state.Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.Snapshot()
}

// ReadState runs fn against this node's state machine under the apply lock.
//
// Exported for the simulator and the invariant checker, which must inspect
// FOLLOWERS too — a divergence between replicas is only visible by reading
// each one directly, and the gRPC surface deliberately refuses a follower.
//
// It is not a client API and never should be: it bypasses the NOT_LEADER
// guard, so anything using it is explicitly asking for one node's local view.
func (c *Coordinator) ReadState(fn func(s *state.StateStore)) { c.read(fn) }

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
	t := c.clk.NewTicker(c.cfg.TickPeriod)
	defer t.Stop()

	// Tick IMMEDIATELY, before waiting out the first interval.
	//
	// A new leader's state machine may still have a zero clock, and anything
	// it serves before the first Tick commits is timestamped with "no time
	// yet". Establishing the time base up front shrinks that window to the
	// commit itself; applyTick's rebase covers what is left.
	tick := func() bool {
		if _, err := c.propose(tickCmd(c.nowMs())); err != nil {
			// Losing leadership mid-tick is NORMAL, not a bug. The ticker is
			// cancelled on revocation, but a tick already in flight races
			// that cancellation and the window cannot be closed from here.
			// Same for a proposal abandoned during the transition.
			//
			// Treating these as fatal crashed the node on every failover —
			// the one moment it most needs to survive.
			if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrProposalLost) {
				return true
			}
			// Anything else means a malformed Tick, a bug in this file.
			panic(fmt.Sprintf("coordinator: tick rejected: %v", err))
		}
		return true
	}
	tick()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			// A Tick whose lease expiries produce EvalTriggers is the
			// normal case: afterApply turns each into a CreateEval and
			// hands it to the broker, which is how a reclaimed task gets
			// rescheduled.
			tick()
		}
	}
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
func (c *Coordinator) registerWorkerCmd(workerID string, runningTaskIDs []string, capacity int32) *commandpb.Command {
	now := c.nowMs()
	return &commandpb.Command{
		Kind: &commandpb.Command_RegisterWorker{
			RegisterWorker: &commandpb.RegisterWorker{
				WorkerId:       workerID,
				RegisteredMs:   now,
				LeaseExpiryMs:  now + c.cfg.LeaseTTL.Milliseconds(),
				RunningTaskIds: runningTaskIDs,
				Capacity:       capacity,
			},
		},
	}
}

// createEvalCmd turns a trigger into the one command that puts an Eval in
// state. The eval id is derived, never random — see evalIDFor.
func createEvalCmd(evalID string, trig state.EvalTrigger, priority int32, nowMs int64) *commandpb.Command {
	return &commandpb.Command{
		Kind: &commandpb.Command_CreateEval{
			CreateEval: &commandpb.CreateEval{
				EvalId:      evalID,
				JobId:       trig.JobID,
				Priority:    priority,
				TriggeredBy: evalReasonToProto(trig.Reason),
				CreatedMs:   nowMs,
			},
		},
	}
}

func evalReasonToProto(r state.EvalReason) commandpb.EvalTrigger {
	switch r {
	case state.EvalJobSubmitted:
		return commandpb.EvalTrigger_EVAL_TRIGGER_JOB_SUBMITTED
	case state.EvalTaskFailed:
		return commandpb.EvalTrigger_EVAL_TRIGGER_TASK_FAILED
	case state.EvalWorkerLost:
		return commandpb.EvalTrigger_EVAL_TRIGGER_WORKER_LOST
	case state.EvalLeaseExpired:
		return commandpb.EvalTrigger_EVAL_TRIGGER_LEASE_EXPIRED
	case state.EvalBackoffElapsed:
		return commandpb.EvalTrigger_EVAL_TRIGGER_BACKOFF_ELAPSED
	}
	return commandpb.EvalTrigger_EVAL_TRIGGER_UNSPECIFIED
}

// ---------------------------------------------------------------------------
// Leader-only lifecycle. Day 6 calls these from establishLeadership and
// revokeLeadership; stopping cleanly is what prevents split-brain scheduling.
// ---------------------------------------------------------------------------

// StartScheduling builds the broker, rebuilds it from committed state, and
// launches the planner and scheduler workers.
//
// The rebuild is the point: a brand-new leader reconstructs its queue by
// SCANNING STATE, with none of the previous leader's memory. That is what
// makes derived-not-replicated correct.
func (c *Coordinator) StartScheduling(ctx context.Context) {
	broker := sched.NewBroker()
	planner := NewPlanner(c)
	scheduler := sched.NewScheduler(c.cfg.Sched, broker, c, planner)

	c.schedMu.Lock()
	c.broker, c.planner, c.scheduler = broker, planner, scheduler
	c.schedMu.Unlock()

	broker.Rebuild(c.Snapshot(), func(jobID string) string {
		return "eval/" + jobID + "/rebuild"
	})
	scheduler.Start(ctx)
}

// brokerRef reads the broker pointer under the lock.
func (c *Coordinator) brokerRef() *sched.Broker {
	c.schedMu.Lock()
	defer c.schedMu.Unlock()
	return c.broker
}

// StopScheduling tears the pipeline down and waits for the workers to exit.
func (c *Coordinator) StopScheduling() {
	c.schedMu.Lock()
	broker, scheduler := c.broker, c.scheduler
	c.broker, c.scheduler, c.planner = nil, nil, nil
	c.schedMu.Unlock()

	if broker == nil {
		return
	}
	broker.Close()
	scheduler.Wait()
}

// Planner exposes the planner's counters for tests and metrics.
func (c *Coordinator) Planner() *Planner {
	c.schedMu.Lock()
	defer c.schedMu.Unlock()
	return c.planner
}

// BrokerStats reports queue depth: ready evals and blocked ones.
func (c *Coordinator) BrokerStats() (ready, blocked int) {
	broker := c.brokerRef()
	if broker == nil {
		return 0, 0
	}
	return broker.Stats()
}
