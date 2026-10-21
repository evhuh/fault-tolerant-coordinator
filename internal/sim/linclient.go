package sim

// The clients whose operations become the history.
//
// # Why these do not use s.leader()
//
// Every other part of the workload calls s.leader(), which returns nil unless
// exactly one node believes it leads. That is the right behaviour for
// offering work — there is no point submitting into an election — and it is
// precisely WRONG for this.
//
// The bug being hunted exists only while two nodes both believe they lead:
// raft is configured without CheckQuorum and without PreVote, so a leader cut
// off from the majority never learns it, keeps its term, and keeps passing its
// own requireLeader guard, while the majority elects a new leader at a higher
// term. s.leader() sees two leaders, returns nil, and the old workload stands
// down for the exact duration of the only window that matters.
//
// So these clients address NODES, not "the leader". Each picks a node and
// calls it; the node's own NOT_LEADER guard decides whether to answer. That is
// what a real client pool does, and it is the only way the stale leader ever
// gets asked a question.
//
// # Why they run continuously rather than per step
//
// The fault schedule advances between steps, and an operation that straddles
// a partition is the interesting one. Clients that only ran between steps
// would record histories in which every operation is neatly inside one fault
// regime, and a stale read lives exactly at the boundary.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// knownJobs is the ids the clients have been told exist.
//
// Shared across clients on purpose: a job submitted by client 0 and read by
// client 2 is where linearizability has teeth. A single client reading only
// its own writes can be satisfied by much weaker guarantees.
type knownJobs struct {
	mu  sync.Mutex
	ids []string
}

func (k *knownJobs) add(id string) {
	if id == "" {
		return
	}
	k.mu.Lock()
	k.ids = append(k.ids, id)
	k.mu.Unlock()
}

// pick returns a job id, biased hard towards the most recent ones.
//
// Recency is the whole point. A job submitted long ago has replicated
// everywhere, so reading it from a stale leader tells you nothing; a job
// committed on the majority side one millisecond ago is one a partitioned
// leader cannot possibly have. Uniform sampling over a growing id set spends
// almost all its reads on jobs too old to discriminate.
const recentWindow = 24

func (k *knownJobs) pick(rng *rand.Rand) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.ids) == 0 {
		return "", false
	}
	lo := 0
	if len(k.ids) > recentWindow {
		lo = len(k.ids) - recentWindow
	}
	return k.ids[lo+rng.Intn(len(k.ids)-lo)], true
}

// runLinClients starts the recorded clients and returns a function that waits
// for them to stop.
func (s *Sim) runLinClients(ctx context.Context) func() {
	if s.cfg.LinClients <= 0 {
		return func() {}
	}
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.LinClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.linClient(ctx, i)
		}()
	}
	return wg.Wait
}

// linClient issues one operation at a time, recording every one.
//
// Its randomness comes from a PER-CLIENT stream seeded off the run's seed,
// never from the fault RNG. Same rule as the workload's outcome hashing and
// for the same reason: these clients draw a number of times that depends on
// goroutine timing, so sharing the fault RNG would make the fault schedule
// irreproducible, which is the one property the harness exists to provide.
func (s *Sim) linClient(ctx context.Context, client int) {
	rng := rand.New(rand.NewSource(s.cfg.Seed + 7919 + int64(client)))
	seq := 0
	budget := linBudget{step: -1}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !s.awaitLinBudget(ctx, &budget) {
			return
		}

		n, split := s.pickLeaderClaimant(rng)
		if n == nil {
			// Mid-election: nobody will answer, so nothing happened and
			// NOTHING IS SPENT. Consuming the budget here produced empty
			// histories — on short steps the cluster is between leaders
			// whenever the budget refreshes, all of it burned in
			// microseconds against a leaderless cluster, and the run
			// recorded zero operations while looking like it had checked
			// something.
			if !s.linIdle(ctx) {
				return
			}
			continue
		}
		budget.spend()
		// Sampled at invocation, before the call: by the time it returns the
		// partition may have healed, and the question is where the operation
		// LANDED.
		where := linWhere{partitioned: s.net.IsPartitioned(n.id), splitBrain: split}

		// Reads outnumber writes: a read is what catches a stale leader, and
		// a write only sets one up.
		if rng.Intn(100) < 35 {
			seq++
			s.linSubmit(ctx, n, client, seq, where)
		} else {
			s.linGet(ctx, n, client, rng, where)
		}
	}
}

// linBudget paces a client off the FAULT STEP COUNTER rather than the clock.
//
// Pacing by wall time does not work here, and the first attempt proved it: a
// 25-step run took 63 seconds, because the steps are dominated by draining
// work, checking invariants and 23-40ms snapshot fsyncs rather than by
// StepTime. Clients ticking every few milliseconds recorded eleven thousand
// operations, blew through the history cap in the first nine seconds, and
// produced a green verdict over a PREFIX of the run — the quiet early steps,
// before most of the faults had even fired. A verdict over the wrong nine
// seconds is worse than no verdict, because it looks like one.
//
// Tying the budget to the step counter makes the history span the schedule by
// construction: at most OpsPerStep operations per client per step, however
// long or short a step turns out to take. The total is then bounded by
// Steps x Clients x OpsPerStep and known before the run starts.
//
// The budget is SPENT on an operation issued, not on an attempt. Those are
// different, and conflating them is what made short runs record nothing at
// all.
type linBudget struct {
	step int64
	left int
}

func (b *linBudget) spend() { b.left-- }

// linIdle waits out a moment when there is nothing to talk to. Reports false
// when the run is over.
func (s *Sim) linIdle(ctx context.Context) bool {
	t := time.NewTimer(2 * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// awaitLinBudget blocks until this client may issue another operation. It
// does NOT consume it; the caller spends it once it has something to call.
func (s *Sim) awaitLinBudget(ctx context.Context, b *linBudget) bool {
	for {
		if step := s.stepNo.Load(); step != b.step {
			b.step = step
			b.left = s.cfg.LinOpsPerStep
		}
		if b.left > 0 {
			return true
		}
		// Budget spent for this step. Wait for the next one.
		t := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return false
		case <-t.C:
		}
	}
}

// linWhere records the fault conditions an operation was issued under. It
// feeds coverage only; the model never sees it, because the model must not be
// told anything the client could not observe.
type linWhere struct {
	partitioned bool
	splitBrain  bool
}

func (s *Sim) linSubmit(ctx context.Context, n *node, client, seq int, w linWhere) {
	key := fmt.Sprintf("lin-%d/%d", client, seq)
	in := linInput{Kind: opSubmit, Key: key}
	op := s.history.begin(client, in, w.partitioned, w.splitBrain)

	resp, err := n.svc.SubmitJob(ctx, &schedulerpb.SubmitJobRequest{
		ClientId:  fmt.Sprintf("lin-%d", client),
		RequestId: fmt.Sprintf("%d", seq),
		Tasks:     []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 1}},
	})

	out := s.classifySubmit(resp, err)
	op.done(out)
	if !out.Unknown {
		s.knownJobs.add(out.JobID)
	}
}

func (s *Sim) linGet(ctx context.Context, n *node, client int, rng *rand.Rand, w linWhere) {
	jobID, ok := s.knownJobs.pick(rng)
	if !ok {
		return // nothing has been successfully submitted yet
	}
	in := linInput{Kind: opGet, JobID: jobID}
	op := s.history.begin(client, in, w.partitioned, w.splitBrain)
	_, err := n.svc.GetJob(ctx, &schedulerpb.GetJobRequest{JobId: jobID})
	op.done(s.classifyGet(err))
}

// classifySubmit turns an RPC result into what the client LEARNED.
//
// The three cases are not three error codes, they are three epistemic
// positions, and conflating them is how linearizability checking goes quietly
// wrong.
//
//	nil                  → it committed, and this is the job id.
//	InvalidArgument      → the state machine rejected it. Definitive, and in
//	                       this workload impossible: every request sent is
//	                       valid. Counted and reported rather than modelled,
//	                       because a nonzero count is a finding in itself.
//	everything else      → UNKNOWN. NOT_LEADER can be returned after the
//	                       entry was already proposed; ErrProposalLost says
//	                       outright that the entry may still commit. The
//	                       client does not know, so the history must not
//	                       claim to.
func (s *Sim) classifySubmit(resp *schedulerpb.SubmitJobResponse, err error) linOutput {
	if err == nil {
		return linOutput{JobID: resp.GetJobId()}
	}
	if codeOf(err) == codes.InvalidArgument {
		s.history.noteRejected()
	}
	return linOutput{Unknown: true}
}

// classifyGet does the same for a read.
//
// NOT_FOUND is DEFINITIVE and it is the whole hunt. It is returned only after
// requireLeader passed, which means a node that believes it leads looked in
// its own state machine and did not find the job. If the job was submitted
// successfully before that read began, no ordering of the history makes that
// answer legal — and a leader on the minority side of a partition gives
// exactly that answer.
//
// Every other error is a refusal to answer, not an answer: NOT_LEADER from a
// follower tells the client nothing about any job.
func (s *Sim) classifyGet(err error) linOutput {
	switch {
	case err == nil:
		return linOutput{Found: true}
	case codeOf(err) == codes.NotFound:
		return linOutput{Found: false}
	default:
		return linOutput{Unknown: true}
	}
}

func codeOf(err error) codes.Code {
	var se interface{ GRPCStatus() *status.Status }
	if errors.As(err, &se) {
		return se.GRPCStatus().Code()
	}
	return codes.Unknown
}
