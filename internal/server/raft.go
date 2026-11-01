package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"time"

	"go.etcd.io/raft/v3"

	"github.com/evazheng/fault-tolerant-coordinator/internal/metrics"
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
	// ErrReadBarrier means a linearizable read could not confirm, with a
	// quorum, that this node still leads. The read was NOT served.
	//
	// It is distinct from ErrNotLeader on purpose: this node may well still
	// be the leader, and telling the client to go elsewhere would send it
	// looking for a leader that does not exist. The caller RETRIES, like a
	// lost proposal, and unlike a lost proposal there is nothing to be
	// idempotent about, because a refused read has no effect.
	ErrReadBarrier = errors.New("read barrier not confirmed")
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
//
// WHAT IT PASSES MATTERS AS MUCH AS THAT IT RUNS. These entries were already
// PROPOSED — some of them replicated — so whether they commit is unknown, and
// the error has to say so. It used to pass ErrNotLeader, which the gRPC layer
// maps to NOT_LEADER, whose documented meaning is "re-aim at the real leader"
// — a definitive "this did not happen". A client that believes that and then
// reads the committed entry has been lied to.
//
// ErrProposalLost is the error that means "abandoned, may still commit, safe
// to retry because every command is idempotent", which is exactly this
// situation and exactly what rule 6 requires an ambiguous outcome to be
// recorded as.
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
		RestoredAt:        c.restoredAt,
		LeadershipChanged: c.onLeadershipChanged,
		Fatal:             c.fatal,
		Snapshot:          c.snapshotState,
		Restore:           c.restoreState,
		Stalled:           c.onStalled,
		Snapshotted:       c.onSnapshotted,
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
// applyCommitted applies one committed entry and records its log position in
// the SAME critical section as the mutation.
//
// The index has to move with the state. raftnode advances its own applied
// index after this returns, so a reader that sampled the index first and the
// state second could encode state containing this entry while the index still
// reported the previous one — and the simulator's divergence detector, which
// compares two replicas only at indices they share, then compares different
// positions and calls it divergence. It did: a false "nodes 1 and 3 DIVERGED
// at applied index 555" on one run in twenty-two.
func (c *Coordinator) applyCommitted(index uint64, data []byte) {
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
	// AN ENTRY FROM A NEWER BUILD HALTS THIS NODE, before it is applied and
	// before the applied index moves.
	//
	// Same treatment as an undecodable entry, for the same reason: this
	// replica cannot know what the entry means, every up-to-date replica
	// does, and the two alternatives are both silent. Skipping it leaves
	// this replica missing a mutation the others made; applying it under an
	// older build's rules produces a different state while reporting
	// success. Divergence is the one failure this system has no recovery
	// from, and it is only ever noticed when something reads from the odd
	// replica.
	//
	// Halting alone is also what frees the ROLLOUT ORDER. A build that stops
	// on a version it does not know can be upgraded in any order, because
	// the worst case is a node that refuses to continue — loud, local, and
	// recoverable by upgrading it. See architecture.md.
	if v := cmd.GetVersion(); v > state.CommandVersionCurrent {
		c.fatal(fmt.Errorf("committed entry at command version %d, this build understands %d: "+
			"halting rather than guessing at its meaning", v, state.CommandVersionCurrent))
		return
	}

	metrics.RaftEntries.WithLabelValues(commandKind(cmd)).Inc()

	c.mu.Lock()
	res, err := c.store.Apply(cmd)
	// Inside the lock, next to the mutation, so (state, appliedIndex) is
	// never observed skewed. Advanced even when Apply returns an error: the
	// entry is committed and consumed either way, and a replica that
	// rejected it is at this position too.
	if index > c.appliedIndex {
		c.appliedIndex = index
	}
	c.mu.Unlock()

	// The waiter is leader-side only; followers have none and skip this.
	if c.proposer != nil {
		c.proposer.fulfill(cmd.GetProposalId(), proposalOutcome{result: res, err: err})
	}
	if err != nil {
		return
	}

	// Observed before the leader-only reactions and regardless of whether
	// this node leads: the hook's subject is what the STATE MACHINE decided,
	// which every replica decides identically. Only applies that SUCCEEDED
	// are observed; a rejected entry has no Result to report.
	if c.cfg.OnApply != nil {
		c.cfg.OnApply(cmd, res)
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

	// COMMIT LATENCY is measured only for proposals that actually committed.
	// A lost or abandoned proposal would otherwise contribute its full
	// timeout to the histogram, which turns the fsync cost the plan-batching
	// experiment is about into a measure of how often leadership moved.
	proposedAt := time.Now()

	// THE PROPOSAL'S CONTEXT HANGS OFF LEADERSHIP, not off Background.
	//
	// node.Propose blocks until raft ACCEPTS the entry, and the goroutine
	// that accepts it is the Ready loop. So a proposal in flight when
	// leadership is lost is waiting on the very loop that is inside
	// revokeLeadership waiting for the scheduler worker that is waiting for
	// this proposal. failAll does not help: it releases registered WAITERS,
	// and this caller never reached its waiter.
	//
	// With Background() the cycle broke only when the proposal timeout fired
	// — measured as 131ms and 187ms Ready-loop stalls in Day 10's
	// calibration against a 250ms timeout, and at the 5s production default
	// it would be a node that processes no raft messages for five seconds on
	// every leadership change. Deriving from the leader context makes
	// revocation cancel it immediately; Propose then returns
	// context.Canceled, which the branch below already classifies as
	// ErrProposalLost — "abandoned, may still commit, safe to retry" — which
	// is precisely what such a proposal is.
	//
	// TestRevocationReleasesAnInFlightPropose pins it, and fails with the
	// full timeout against the old context.
	base := c.leaderContext()
	if base == nil {
		// Leadership went away between the check above and here. Nothing has
		// been proposed yet, so this is definitive rather than ambiguous.
		return state.Result{}, ErrNotLeader
	}
	ctx, cancel := context.WithTimeout(base, c.proposalTimeout())
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
			// The node is shutting down. ErrProposalLost rather than
			// ErrNotLeader for the same reason as failAll: Propose has
			// already been called, and a proposal the run loop picked up
			// immediately before stopping may have been appended and
			// replicated. Unlikely is not the same as impossible, and the
			// only honest answer about an entry that may commit is that it
			// may commit. Both errors move the client off this node anyway —
			// Unavailable makes the pool rotate — so the ambiguity costs no
			// liveness.
			return state.Result{}, ErrProposalLost
		}
		return state.Result{}, fmt.Errorf("propose: %w", err)
	}

	select {
	case out := <-w.ch:
		if out.err == nil {
			metrics.CommitLatency.Observe(time.Since(proposedAt).Seconds())
		}
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
	// TIMED, because this runs on the Ready loop and its cost is not
	// constant. StartScheduling rebuilds the eval broker by SCANNING
	// COMMITTED STATE — it serializes the whole state machine through
	// c.Snapshot() — so the loop is blocked for as long as that takes, and
	// that grows with the number of jobs. Day 10's 600-step calibration
	// recorded 131ms and 187ms stalls in this phase against a 125ms
	// threshold, on a healthy disk, which is what sent anyone looking.
	//
	// The measurement is kept rather than logged so the question "does it
	// scale with state" can be answered with a curve instead of an anecdote.
	start := time.Now()
	defer func() {
		took := time.Since(start)
		var jobs int
		c.read(func(st *state.StateStore) { jobs = st.JobCount() })
		c.stallMu.Lock()
		c.leadershipChanges = append(c.leadershipChanges, LeadershipChange{
			IsLeader: isLeader, Duration: took, Jobs: jobs,
			AtMs: time.Now().UnixMilli(), Term: c.RaftStatus().Term,
		})
		c.stallMu.Unlock()
		// Only a transition TO leadership is a failover: the time from
		// learning we lead to having rebuilt the broker and resumed. Counting
		// the step-down side as well would halve the reported latency by
		// averaging in an event that does no work.
		if isLeader {
			metrics.LeaderElections.Inc()
			metrics.FailoverLatency.Observe(took.Seconds())
		}
	}()

	if isLeader {
		log.Printf("coordinator: became leader, establishing leadership")
		ctx, cancel := context.WithCancel(context.Background())
		c.leaderMu.Lock()
		c.leaderCtx, c.leaderCancel = ctx, cancel
		c.leaderMu.Unlock()
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
		c.proposer.failAll(ErrProposalLost)
	}
	c.cancelLeader()
	c.StopScheduling()
}

// leaderContext returns the context that lives exactly as long as this node's
// leadership, or nil when it does not lead.
func (c *Coordinator) leaderContext() context.Context {
	c.leaderMu.Lock()
	defer c.leaderMu.Unlock()
	return c.leaderCtx
}

// cancelLeader cancels the leader-only context, which is the first thing
// revocation does. Separated out so a test can exercise the revocation
// mechanism without racing the Ready loop for the handler.
func (c *Coordinator) cancelLeader() {
	c.leaderMu.Lock()
	cancel := c.leaderCancel
	c.leaderCancel = nil
	c.leaderCtx = nil
	c.leaderMu.Unlock()
	if cancel != nil {
		cancel()
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
// RaftStatus exposes raft's view of this node, for tests and diagnostics.
func (c *Coordinator) RaftStatus() RaftInfo {
	if c.raft == nil {
		return RaftInfo{}
	}
	st := c.raft.Status()
	info := RaftInfo{
		// GetTerm, never st.Term. See RaftInfo.
		Term:     st.HardState.GetTerm(),
		Lead:     st.Lead,
		State:    st.RaftState.String(),
		IsLeader: st.RaftState == raft.StateLeader,
		Commit:   st.HardState.GetCommit(),
	}
	// Sorted ids, never a map range: a diagnosis that depends on Go's map
	// iteration order cannot be diffed between two runs.
	ids := make([]uint64, 0, len(st.Progress))
	for id := range st.Progress {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// The leader itself always acks.
	acking := 0
	if info.IsLeader {
		acking = 1
	}
	for _, id := range ids {
		pr := st.Progress[id]
		if id == st.ID {
			continue
		}
		f := FollowerProgress{
			ID: id, State: pr.State.String(), Match: pr.Match, Next: pr.Next,
			PendingSnapshot: pr.PendingSnapshot, RecentActive: pr.RecentActive,
			Paused: pr.IsPaused(),
		}
		info.Followers = append(info.Followers, f)
		// Paused covers StateSnapshot unconditionally, which is the limbo
		// case: raft will send this follower nothing until the application
		// reports the snapshot's outcome.
		if f.RecentActive && !f.Paused {
			acking++
		}
	}
	info.AckingQuorum = acking >= len(st.Progress)/2+1
	return info
}

// FollowerProgress is raft's own view of one follower, flattened.
//
// IT ANSWERS A QUESTION THE SIMULATOR'S CONNECTIVITY VIEW CANNOT. The
// frozen-clock invariant exempts itself when the simulator thinks quorum is
// available, which counts nodes that are UP AND UNPARTITIONED — not nodes
// that can actually ACK. A follower in StateSnapshot acks nothing: the
// leader has paused log probes to it and will not resume until the
// application calls ReportSnapshot. Its heartbeat responses still keep
// RecentActive true, so CheckQuorum is satisfied and the leader keeps its
// term while committing nothing.
//
// That matches the shape of every frozen-clock window Day 10's checkpoint
// recorded: one leader, one term, quorum "available", 2.6 seconds of
// proposals lost. Which of the two a given window was — a follower in limbo
// or something else — is only visible here, and no window captured before
// this type existed can answer it.
type FollowerProgress struct {
	ID              uint64
	State           string
	Match           uint64
	Next            uint64
	PendingSnapshot uint64
	RecentActive    bool
	Paused          bool
}

// RaftInfo is raft's view of this node, flattened into plain values.
//
// THE TYPE EXISTS TO DELETE A TRAP. raft.Status reaches Term through an
// embedded *raftpb.HardState, and the field behind it is itself a *uint64, so
// `st.Term` both nil-dereferences when there is no hard state and compares
// pointer addresses when two are compared. That trap has now been walked into
// three times in this repo: Day 8's MsgHup experiment compared addresses, and
// Day 10 segfaulted a diagnostic run on the nil pointer — after the fix for
// the first one had written the correct idiom in a comment directly above
// Status().
//
// A comment did not stop it twice, so the type does: nothing outside this
// function handles a raft.Status, and TestNothingReadsRaftStatusTermDirectly
// fails if that changes.
type RaftInfo struct {
	Term     uint64
	Lead     uint64
	State    string
	IsLeader bool
	// Commit is the index this node believes committed, and Followers raft's
	// progress table when this node leads.
	Commit    uint64
	Followers []FollowerProgress
	// AckingQuorum is whether a MAJORITY of the cluster could acknowledge a
	// proposal right now: the leader itself plus every follower that is not
	// paused and is recently active. This is the quorum test the frozen-clock
	// invariant should have been using.
	AckingQuorum bool
}

// AppliedIndex is the log position of the state the store currently holds.
//
// Read under the apply lock, and advanced under it, so it is COHERENT with
// what ReadState would encode. raftnode keeps its own applied index for
// compaction; that one is allowed to lag behind this by the width of an
// Apply, and is not what a replica comparison may use.
func (c *Coordinator) AppliedIndex() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appliedIndex
}

// restoredAt resets the recorded position to the snapshot a Restore installed.
func (c *Coordinator) restoredAt(index uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.appliedIndex = index
}

// onStalled records that this node's Ready loop blocked too long.
//
// Nothing is recovered here — the loop is already wedged and will unwedge
// when whatever it waits on times out. The point is that the stall becomes
// EVIDENCE instead of an unexplained election: the simulator reads it as an
// invariant violation and fails the run with its seed.
func (c *Coordinator) onStalled(st raftnode.Stall) {
	// TWO REPORTS PER STALL, and only the second is a real duration.
	//
	// The at-detection report is censored by the watchdog's poll interval —
	// every one landed in 500-748ms whatever the true blockage — so feeding
	// it to a histogram makes the upper buckets unreachable by construction.
	// The completed report carries the batch's full blocked time.
	//
	// The early warning is still kept in the ring, because a stall that never
	// ends never produces a completed report and a genuine hang must not be
	// invisible. When the completion arrives it REPLACES that entry by ID, so
	// counts stay exact and durations become true.
	if st.Completed {
		metrics.ObserveStall(st.Phase, st.Duration)
	}
	c.stallMu.Lock()
	for i := range c.stalls {
		if c.stalls[i].ID == st.ID && st.Completed {
			c.stalls[i] = st // the real duration replaces the floor
			c.stallMu.Unlock()
			return
		}
	}
	c.stalls = append(c.stalls, st)
	c.stallMu.Unlock()
}

// onSnapshotted records what a snapshot cost this node's Ready loop.
//
// Kept, not just logged, because the question a long run has to answer is a
// TREND: the snapshot grows with state and the loop is blocked for all of
// it, so what matters is whether the cost is climbing toward the stall
// threshold, which no single log line can say.
func (c *Coordinator) onSnapshotted(st raftnode.SnapshotStat) {
	// Cost AND size together, so a slow snapshot of a small state — a disk
	// tail, which is what Day 10 found — is distinguishable from a slow
	// snapshot of a large one.
	metrics.ObserveSnapshot(st.Bytes, st.Duration)
	c.stallMu.Lock()
	c.snapshots = append(c.snapshots, st)
	c.stallMu.Unlock()
}

// LeadershipChange is what one leadership transition cost the Ready loop.
type LeadershipChange struct {
	IsLeader bool
	Duration time.Duration
	Jobs     int
	// AtMs is when it happened, and Term the raft term it happened in.
	//
	// Both are here so a frozen-clock window can COUNT the transitions
	// inside it instead of inferring a rate from a log tail — and so
	// transitions closer together than an election timeout, which PreVote is
	// supposed to prevent, are visible as such.
	AtMs int64
	Term uint64
}

// LeadershipChanges reports the transitions this node went through, in order.
func (c *Coordinator) LeadershipChanges() []LeadershipChange {
	c.stallMu.Lock()
	defer c.stallMu.Unlock()
	return append([]LeadershipChange(nil), c.leadershipChanges...)
}

// Snapshots reports the snapshots this node took, in order.
func (c *Coordinator) Snapshots() []raftnode.SnapshotStat {
	c.stallMu.Lock()
	defer c.stallMu.Unlock()
	return append([]raftnode.SnapshotStat(nil), c.snapshots...)
}

// Stalls reports Ready-loop stalls observed on this node.
func (c *Coordinator) Stalls() []raftnode.Stall {
	c.stallMu.Lock()
	defer c.stallMu.Unlock()
	return append([]raftnode.Stall(nil), c.stalls...)
}

// commandKind names a command's oneof variant for the entry breakdown.
//
// A fixed set of strings rather than reflection on the oneof, so the label's
// cardinality is bounded by this function and an unknown variant is visible
// as "other" instead of inventing a new time series.
func commandKind(cmd *commandpb.Command) string {
	switch cmd.GetKind().(type) {
	case *commandpb.Command_SubmitJob:
		return "submit_job"
	case *commandpb.Command_CreateEval:
		return "create_eval"
	case *commandpb.Command_ApplyPlan:
		return "apply_plan"
	case *commandpb.Command_CompleteTask:
		return "complete_task"
	case *commandpb.Command_FailTask:
		return "fail_task"
	case *commandpb.Command_RegisterWorker:
		return "register_worker"
	case *commandpb.Command_Tick:
		return "tick"
	default:
		return "other"
	}
}
