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
	"fmt"
	"sync"
	"time"

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
		CommandVersion: 1,
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

	// now is the only wall clock in the system. Everything downstream reads
	// time from Tick-carried timestamps instead. Held as a field so Day 8 can
	// replace it with an injected clock without touching call sites.
	now func() time.Time

	// The scheduling pipeline. LEADER-ONLY and in-memory: none of it is
	// replicated, and on Day 6 it starts on election and stops on loss.
	// Nil until StartScheduling, which is what keeps the state machine
	// usable on its own in tests.
	broker    *sched.Broker
	planner   *Planner
	scheduler *sched.Scheduler

	// Raft. Nil until StartRaft, which keeps the whole stack runnable as a
	// single node — every test below Day 6 depends on that.
	raft     *raftnode.Node
	proposer *raftProposer
	// leaderCancel stops the leader-only goroutines on revocation.
	leaderCancel context.CancelFunc
}

func New(cfg Config) *Coordinator {
	return &Coordinator{
		cfg:   cfg,
		store: state.New(),
		now:   time.Now,
	}
}

func (c *Coordinator) nowMs() int64 { return c.now().UnixMilli() }

// SetConfig replaces the config before the node starts serving. It exists so
// cmd/coordinator can fill in APIAddrs once the peer flags are parsed.
func (c *Coordinator) SetConfig(cfg Config) { c.cfg = cfg }

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
	if c.broker == nil {
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
		c.broker.Enqueue(sched.Eval{
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
		c.broker.Unblock()
	case *commandpb.Command_RegisterWorker:
		if res.WorkerAdmitted {
			c.broker.Unblock()
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
	t := time.NewTicker(c.cfg.TickPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// A Tick whose lease expiries produce EvalTriggers is the
			// normal case: afterApply turns each into a CreateEval and
			// hands it to the broker, which is how a reclaimed task gets
			// rescheduled.
			if _, err := c.propose(tickCmd(c.nowMs())); err != nil {
				// Tick has no failure mode other than a malformed payload,
				// which would be a bug in this file.
				panic(fmt.Sprintf("coordinator: tick rejected: %v", err))
			}
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
	c.broker = sched.NewBroker()
	c.planner = NewPlanner(c)
	c.scheduler = sched.NewScheduler(c.cfg.Sched, c.broker, c, c.planner)

	c.broker.Rebuild(c.Snapshot(), func(jobID string) string {
		return "eval/" + jobID + "/rebuild"
	})
	c.scheduler.Start(ctx)
}

// StopScheduling tears the pipeline down and waits for the workers to exit.
func (c *Coordinator) StopScheduling() {
	if c.broker == nil {
		return
	}
	c.broker.Close()
	c.scheduler.Wait()
	c.broker, c.scheduler, c.planner = nil, nil, nil
}

// Planner exposes the planner's counters for tests and metrics.
func (c *Coordinator) Planner() *Planner { return c.planner }

// BrokerStats reports queue depth: ready evals and blocked ones.
func (c *Coordinator) BrokerStats() (ready, blocked int) {
	if c.broker == nil {
		return 0, 0
	}
	return c.broker.Stats()
}
