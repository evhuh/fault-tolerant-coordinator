package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

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

// proposalTimeout bounds how long a caller waits for its entry to commit.
// Past it the proposal is abandoned, not cancelled — it may still commit
// later, which is exactly why every command must be idempotent.
const proposalTimeout = 5 * time.Second

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

func newProposalID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("server: crypto/rand unavailable: " + err.Error())
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

	node, err := raftnode.Start(cfg, raftnode.Handler{
		Apply:             c.applyCommitted,
		LeadershipChanged: c.onLeadershipChanged,
	})
	if err != nil {
		c.proposer = nil
		return err
	}
	c.proposer.node = node
	c.raft = node
	return nil
}

func (c *Coordinator) StopRaft() {
	if c.raft == nil {
		return
	}
	c.raft.Stop()
	c.raft = nil
	c.proposer = nil
}

// applyCommitted runs on EVERY replica, in log order, for every committed
// entry. It is the only writer to the StateStore once Raft is wired.
func (c *Coordinator) applyCommitted(data []byte) {
	cmd := &commandpb.Command{}
	if err := proto.Unmarshal(data, cmd); err != nil {
		// A command this build cannot decode is a schema-evolution failure,
		// which Day 7 exists to prevent. Crashing is correct: silently
		// skipping it would diverge this replica from the others.
		log.Fatalf("coordinator: undecodable committed entry: %v", err)
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
	cmd.ProposalId = newProposalID()

	data, err := proto.Marshal(cmd)
	if err != nil {
		return state.Result{}, err
	}

	w := c.proposer.register(cmd.ProposalId)
	defer c.proposer.forget(cmd.ProposalId)

	ctx, cancel := context.WithTimeout(context.Background(), proposalTimeout)
	defer cancel()
	if err := c.proposer.node.Propose(ctx, data); err != nil {
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
		ctx, cancel := context.WithTimeout(context.Background(), proposalTimeout)
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
	if c.leaderCancel != nil {
		c.leaderCancel()
		c.leaderCancel = nil
	}
	c.StopScheduling()
	// Anyone blocked on a proposal this node will never commit must be told,
	// or their RPC hangs until the timeout for no reason.
	if c.proposer != nil {
		c.proposer.failAll(ErrNotLeader)
	}
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
