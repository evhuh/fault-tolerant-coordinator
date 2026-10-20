package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"go.etcd.io/raft/v3"

	"github.com/evazheng/fault-tolerant-coordinator/internal/raftnode"
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	"google.golang.org/protobuf/proto"
)

// Errors the gRPC layer turns into status codes.
var (
	// ErrNotLeader means this node cannot accept writes. The caller is told
	// where the leader is, if this node knows.
	ErrNotLeader = errors.New("not leader")
	// ErrProposalLost means the entry was accepted into the pipeline but never
	// came back. A leader change or a partition does this. The caller RETRIES,
	// and (clientID, requestID) dedup is what makes the retry safe.
	ErrProposalLost = errors.New("proposal lost")
)

// defaultProposalTimeout bounds how long a caller waits for its entry to
// commit. Past it the proposal is abandoned, not cancelled — it may still
// commit later, which is exactly why every command must be idempotent.
//
// Overridable because the simulator compresses every other timing by ~20×,
// and a 5s wait against a 10ms raft tick means a single leaderless moment
// costs five hundred ticks. Leaving it fixed made simulated runs spend
// almost all their wall time parked on proposals that were never going to
// land.
const defaultProposalTimeout = 5 * time.Second

func (c *Coordinator) proposalTimeout() time.Duration {
	if c.cfg.ProposalTimeout > 0 {
		return c.cfg.ProposalTimeout
	}
	return defaultProposalTimeout
}

// waiter is one caller blocked on a proposal.
type waiter struct {
	ch chan proposalOutcome
}

type proposalOutcome struct {
	result state.Result
	err    error
}

// raftProposer turns propose() from a direct Apply into a real consensus
// round-trip.
//
// The shape that matters: Propose is FIRE-AND-FORGET. Returning nil means the
// entry was accepted into the pipeline, not that it committed — commitment is
// only observable when the entry re-emerges through CommittedEntries. So each
// command carries a proposal id, the caller parks on a channel, and the apply
// loop hands back the Result when the entry finally lands.
type raftProposer struct {
	node *raftnode.Node

	mu      sync.Mutex
	waiters map[string]*waiter
}

func newRaftProposer() *raftProposer {
	return &raftProposer{waiters: make(map[string]*waiter)}
}

func (c *Coordinator) newProposalID() string {
	var b [12]byte
	if _, err := io.ReadFull(c.rand, b[:]); err != nil {
		panic("server: id randomness unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

func (p *raftProposer) register(id string) *waiter {
	w := &waiter{ch: make(chan proposalOutcome, 1)}
	p.mu.Lock()
	p.waiters[id] = w
	p.mu.Unlock()
	return w
}

func (p *raftProposer) forget(id string) {
	p.mu.Lock()
	delete(p.waiters, id)
	p.mu.Unlock()
}

// fulfill hands a result to whoever is waiting. On a follower — or for an
// entry this node proposed in a previous life — there is no waiter, and that
// is normal: the entry still applied, nobody is listening.
func (p *raftProposer) fulfill(id string, out proposalOutcome) {
	if id == "" {
		return
	}
	p.mu.Lock()
	w, ok := p.waiters[id]
	delete(p.waiters, id)
	p.mu.Unlock()
	if ok {
		w.ch <- out
	}
}

// failAll releases every pending waiter. Called on leadership loss: those
// proposals may or may not commit under the new leader, and a caller blocked
// forever is worse than one told to retry.
func (p *raftProposer) failAll(err error) {
	p.mu.Lock()
	pending := p.waiters
	p.waiters = make(map[string]*waiter)
	p.mu.Unlock()
	for _, w := range pending {
		w.ch <- proposalOutcome{err: err}
	}
}

// ---------------------------------------------------------------------------
// Coordinator integration
// ---------------------------------------------------------------------------

// StartRaft wires the coordinator to a Raft node. After this, propose() goes
// through consensus and apply is driven by the Ready loop on every replica.
func (c *Coordinator) StartRaft(cfg raftnode.Config) error {
	c.proposer = newRaftProposer()

	// Construct WITHOUT running. The Ready loop calls straight back into
	// applyCommitted, which reads c.raft via IsLeader — so the field has to
	// be published before any goroutine can observe it.
	//
	// Starting first and assigning after is a data race, and a quiet one: it
	// needs a node to be restarted while entries are already committing,
	// which is rare enough that the cluster tests never hit it and the
	// simulator hit it within ten steps.
	node, err := raftnode.New(cfg, raftnode.Handler{
		Apply:             c.applyCommitted,
		LeadershipChanged: c.onLeadershipChanged,
		Fatal:             c.fatal,
		Snapshot:          c.snapshotState,
		Restore:           c.restoreState,
		Stalled:           c.onStalled,
	})
	if err != nil {
		c.proposer = nil
		return err
	}
	c.proposer.node = node
	c.raft = node

	node.Run()
	return nil
}

// StopRaft halts this node's consensus.
//
// It deliberately does NOT nil c.raft or c.proposer. Those fields are read
// without a lock by propose, IsLeader and applyCommitted, including from the
// Tick proposer goroutine — which is cancelled by context but can be mid
// propose when this runs. Clearing them underneath a live reader is a data
// race, and it is the exact mirror of publishing them after the Ready loop
// had already started.
//
// Nothing is gained by clearing them. A stopped node reports IsLeader()
// false (the Ready loop gives up leadership on its way out), so propose
// returns ErrNotLeader, LeaderAddr reports none, and Propose on a stopped
// node fails. Every reader already does the right thing against a stopped
// node; it only has to be able to read the pointer safely.
func (c *Coordinator) StopRaft() {
	if c.raft == nil {
		return
	}
	c.raft.Stop()
}

// applyCommitted runs on EVERY replica, in log order, for every committed
// entry. It is the only writer to the StateStore once Raft is wired.
func (c *Coordinator) applyCommitted(data []byte) {
	cmd := &commandpb.Command{}
	if err := proto.Unmarshal(data, cmd); err != nil {
		// A command this build cannot decode is a schema-evolution failure,
		// which Day 7 exists to prevent. Halting this node is correct:
		// skipping the entry would silently diverge it from every replica
		// that could decode it, and a diverged replica is worse than a dead
		// one. It halts ALONE — see Coordinator.fatal.
		c.fatal(fmt.Errorf("undecodable committed entry: %w", err))
		return
	}
	if c.FatalErr() != nil {
		return // already halted; do not apply further entries
	}

	c.mu.Lock()
	res, err := c.store.Apply(cmd)
	c.mu.Unlock()

	// The waiter is leader-side only; followers have none and skip this.
	if c.proposer != nil {
		c.proposer.fulfill(cmd.GetProposalId(), proposalOutcome{result: res, err: err})
	}
	if err != nil {
		return
	}

	// LEADER-ONLY reactions. Followers compute the identical Result and
	// discard it — that is why the state machine returns EvalTriggers rather
	// than acting on them.
	if c.IsLeader() {
		c.afterApply(cmd, res)
	}
}

// proposeRaft submits a command and waits for it to be applied.
func (c *Coordinator) proposeRaft(cmd *commandpb.Command) (state.Result, error) {
	if !c.IsLeader() {
		return state.Result{}, ErrNotLeader
	}
	cmd.Version = c.cfg.CommandVersion
	cmd.ProposalId = c.newProposalID()

	data, err := proto.Marshal(cmd)
	if err != nil {
		return state.Result{}, err
	}

	w := c.proposer.register(cmd.ProposalId)
	defer c.proposer.forget(cmd.ProposalId)

	ctx, cancel := context.WithTimeout(context.Background(), c.proposalTimeout())
	defer cancel()
	if err := c.proposer.node.Propose(ctx, data); err != nil {
		// A proposal can fail to LAND for the same reasons it can fail to
		// commit, and callers must not have to tell them apart.
		//
		// Propose itself blocks until raft accepts the entry, so a busy or
		// stalled node makes it return the context deadline — which is
		// exactly ErrProposalLost's meaning: abandoned, possibly committed
		// later, safe to retry because every command is idempotent.
		//
		// Returning a bare wrapped error here instead meant the Tick
		// proposer, which tolerates ErrProposalLost, PANICKED the process
		// on a slow node. Classifying at the source rather than widening
		// every caller's tolerance is what keeps that from recurring.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return state.Result{}, ErrProposalLost
		}
		if errors.Is(err, raft.ErrStopped) {
			// The node is shutting down; it will never lead again.
			return state.Result{}, ErrNotLeader
		}
		return state.Result{}, fmt.Errorf("propose: %w", err)
	}

	select {
	case out := <-w.ch:
		return out.result, out.err
	case <-ctx.Done():
		// Abandoned, NOT cancelled: the entry may still commit. Every command
		// is idempotent precisely so this is safe to retry.
		return state.Result{}, ErrProposalLost
	}
}

// proposeAsync submits a command without waiting.
//
// This exists for one specific reason: afterApply runs INSIDE the apply loop,
// and anything it proposes cannot be waited on. Commitment requires the Ready
// loop to make progress, and the Ready loop is what is sitting in afterApply —
// waiting there is a guaranteed deadlock.
//
// Nothing waits on a CreateEval's result anyway, and losing one is survivable:
// the broker is derived, so a new leader rediscovers the work by scanning.
func (c *Coordinator) proposeAsync(cmd *commandpb.Command) {
	if c.proposer == nil || c.proposer.node == nil {
		// Raft not wired (tests, Day 5 and earlier): apply directly.
		if _, err := c.proposeLocal(cmd); err != nil {
			return
		}
		return
	}
	if !c.IsLeader() {
		return
	}
	cmd.Version = c.cfg.CommandVersion
	data, err := proto.Marshal(cmd)
	if err != nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.proposalTimeout())
		defer cancel()
		_ = c.proposer.node.Propose(ctx, data)
	}()
}

// onLeadershipChanged starts and stops the leader-only components.
//
// Nomad calls these establishLeadership and revokeLeadership. Stopping
// cleanly is what prevents split-brain scheduling: a deposed leader whose
// planner kept running would keep proposing plans that a new leader has
// already superseded.
func (c *Coordinator) onLeadershipChanged(isLeader bool) {
	if isLeader {
		log.Printf("coordinator: became leader, establishing leadership")
		ctx, cancel := context.WithCancel(context.Background())
		c.leaderCancel = cancel
		c.StartScheduling(ctx)
		go c.RunTicker(ctx)
		return
	}

	log.Printf("coordinator: lost leadership, revoking")

	// FAIL PENDING PROPOSALS FIRST. Order is load-bearing here, and getting
	// it wrong deadlocks the node.
	//
	// This function runs ON THE READY LOOP GOROUTINE. StopScheduling waits
	// for the scheduler workers to exit, and a scheduler worker may be parked
	// inside planner.Submit waiting for a proposal to commit — a proposal
	// only the Ready loop can deliver. So waiting for the scheduler before
	// releasing those waiters means the Ready loop waits on the scheduler
	// while the scheduler waits on the Ready loop.
	//
	// It self-heals after proposalTimeout, which is what hid it: the symptom
	// is not a hang but a node that processes NO raft messages for five
	// seconds on every leadership change — long enough to trigger the next
	// election and do it all again.
	if c.proposer != nil {
		c.proposer.failAll(ErrNotLeader)
	}
	if c.leaderCancel != nil {
		c.leaderCancel()
		c.leaderCancel = nil
	}
	c.StopScheduling()
}

// IsLeader reports whether this node may accept writes. Without Raft wired
// there is one node and it always leads.
func (c *Coordinator) IsLeader() bool {
	if c.raft == nil {
		return true
	}
	return c.raft.IsLeader()
}

// LeaderAddr is the gRPC address a client should redirect to, if known.
//
// It resolves through APIAddrs, not through the raft peer set: those are the
// transport URLs raft itself speaks, and a client dialing one would reach a
// port that does not serve the scheduler API. During an election there is no
// leader at all, and saying so beats guessing.
func (c *Coordinator) LeaderAddr() (string, bool) {
	if c.raft == nil {
		return "", false
	}
	id, ok := c.raft.Leader()
	if !ok {
		return "", false
	}
	addr, ok := c.cfg.APIAddrs[id]
	return addr, ok
}

// fatal marks this coordinator unusable and halts its Raft node.
//
// It does NOT exit the process. A standalone binary wants to exit, and
// cmd/coordinator does that by watching RaftDone — but the cluster tests and
// Day 8's simulator run every node in one process, and there a single node's
// failure must not take down its peers and the harness with it.
//
// The stop is asynchronous because fatal can be called from inside the Ready
// loop, and Stop waits for that loop to exit.
func (c *Coordinator) fatal(err error) {
	c.fatalOnce.Do(func() {
		c.fatalMu.Lock()
		c.fatalErr = err
		c.fatalMu.Unlock()
		log.Printf("coordinator: fatal: %v", err)
		if c.raft != nil {
			go c.raft.Stop()
		}
	})
}

// FatalErr reports the error that halted this node, if any.
func (c *Coordinator) FatalErr() error {
	c.fatalMu.Lock()
	defer c.fatalMu.Unlock()
	return c.fatalErr
}

// RaftDone closes when this node's Ready loop has exited, for any reason. A
// standalone binary watches it and exits.
func (c *Coordinator) RaftDone() <-chan struct{} {
	if c.raft == nil {
		return nil
	}
	return c.raft.Done()
}

// snapshotState serializes the state machine for a Raft snapshot.
//
// Taken under the same lock as apply, so the bytes correspond to a single log
// position rather than a smear across several.
func (c *Coordinator) snapshotState() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.Encode()
}

// restoreState REPLACES the state machine from a snapshot.
//
// Called at startup from disk, and when the leader ships one to a follower
// that fell further behind than its log could repair.
//
// The scheduling pipeline is deliberately NOT rebuilt here. A node receiving
// a snapshot is by definition a follower, so it has none running; and when it
// later wins an election, establishLeadership rebuilds the broker by scanning
// the state this restore just installed. Derived-not-replicated pays off
// exactly here — there is no in-memory queue to reconcile with a snapshot.
func (c *Coordinator) restoreState(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.Restore(data)
}

// AppliedIndex is the highest log index this node has applied, or 0 without
// Raft. The invariant checker uses it to compare replicas at the same log
// position — comparing two nodes at DIFFERENT positions is meaningless, since
// a node a few entries behind legitimately holds different state.
func (c *Coordinator) AppliedIndex() uint64 {
	if c.raft == nil {
		return 0
	}
	return c.raft.AppliedIndex()
}

// onStalled records that this node's Ready loop blocked too long.
//
// Nothing is recovered here — the loop is already wedged and will unwedge
// when whatever it waits on times out. The point is that the stall becomes
// EVIDENCE instead of an unexplained election: the simulator reads it as an
// invariant violation and fails the run with its seed.
func (c *Coordinator) onStalled(d time.Duration) {
	c.stallMu.Lock()
	c.stalls = append(c.stalls, d)
	c.stallMu.Unlock()
}

// Stalls reports Ready-loop stalls observed on this node.
func (c *Coordinator) Stalls() []time.Duration {
	c.stallMu.Lock()
	defer c.stallMu.Unlock()
	return append([]time.Duration(nil), c.stalls...)
}
