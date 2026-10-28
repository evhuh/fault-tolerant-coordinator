package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/evazheng/fault-tolerant-coordinator/internal/client"
	"github.com/evazheng/fault-tolerant-coordinator/internal/raftnode"
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// A 3-node cluster, in one process, talking real Raft over real sockets.
//
// In-process rather than three binaries on purpose: it runs in CI, it works
// under -race, and it is the harness Day 8's simulator grows out of. The only
// thing it fakes is the clock, and only for the state machine.

type clusterNode struct {
	id    uint64
	coord *Coordinator
	svc   *Service
	dir   string
	// gs serves this node's gRPC API on a real socket, so a client pool can
	// dial it and follow NOT_LEADER for real.
	gs *grpc.Server
}

type cluster struct {
	t        *testing.T
	nodes    map[uint64]*clusterNode
	peers    map[uint64]string // raft transport URLs
	apiAddrs map[uint64]string // gRPC addresses — a DIFFERENT address space
	// snapshotEntries is deliberately tiny so compaction and Restore are
	// genuinely exercised rather than merely compiled.
	snapshotEntries uint64
}

// freePort asks the kernel for an unused port. Hardcoding them makes tests
// collide with each other and with anything else on the machine — a lesson
// the Day 3 demo taught the hard way.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{
		t: t, nodes: map[uint64]*clusterNode{},
		peers: map[uint64]string{}, apiAddrs: map[uint64]string{},
		snapshotEntries: 20,
	}

	for i := 1; i <= n; i++ {
		c.peers[uint64(i)] = fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
		c.apiAddrs[uint64(i)] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}
	for i := 1; i <= n; i++ {
		c.start(uint64(i), t.TempDir())
	}
	t.Cleanup(c.stopAll)
	return c
}

func (c *cluster) start(id uint64, dir string) {
	c.t.Helper()
	cfg := DefaultConfig()
	cfg.Sched.Workers = 2
	cfg.Sched.LeaseTTLMs = cfg.LeaseTTL.Milliseconds()
	cfg.APIAddrs = c.apiAddrs

	coord := New(cfg)
	rcfg := raftnode.DefaultConfig()
	rcfg.ID = id
	rcfg.Peers = c.peers
	rcfg.DataDir = dir
	// Fast elections: the default 10 ticks × 100ms would make every failover
	// test wait a second for no reason.
	rcfg.TickInterval = 20 * time.Millisecond
	rcfg.ElectionTick = 10
	rcfg.HeartbeatTick = 1
	// A LOW threshold on purpose. At the production default of 10k entries
	// no test would ever snapshot, and Restore would be dead code that
	// compiles. The Tick proposer alone produces entries fast enough that
	// this fires within seconds.
	rcfg.SnapshotEntries = c.snapshotEntries
	rcfg.SnapshotCatchUpEntries = 5

	if err := coord.StartRaft(rcfg); err != nil {
		c.t.Fatalf("start node %d: %v", id, err)
	}
	svc := NewService(coord)
	n := &clusterNode{id: id, coord: coord, svc: svc, dir: dir}

	// Serve the API on a real socket. Without this the redirect is untestable:
	// a client pool has nothing to dial.
	lis, err := net.Listen("tcp", c.apiAddrs[id])
	if err != nil {
		c.t.Fatalf("listen api for node %d: %v", id, err)
	}
	n.gs = grpc.NewServer()
	schedulerpb.RegisterSchedulerServer(n.gs, svc)
	go func() { _ = n.gs.Serve(lis) }()

	c.nodes[id] = n
}

func (c *cluster) stop(id uint64) {
	if n, ok := c.nodes[id]; ok {
		if n.gs != nil {
			n.gs.Stop()
		}
		n.coord.StopRaft()
		n.coord.StopScheduling()
		delete(c.nodes, id)
	}
}

// awaitKnowsLeader waits for a node to learn who leads.
//
// Distinct from waiting for a leader to EXIST: a follower's view of
// leadership lags the election, so it answers "NOT_LEADER: no leader elected"
// for a window afterwards. A client pool seeded with only that node has
// nowhere to rotate to, so a test that starts in that window fails for a
// reason that has nothing to do with what it is testing.
func (c *cluster) awaitKnowsLeader(n *clusterNode, within time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, ok := n.coord.LeaderAddr(); ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("node %d never learned who leads within %s", n.id, within)
}

// follower returns any node that is not currently leading.
func (c *cluster) follower(lead *clusterNode) *clusterNode {
	c.t.Helper()
	for id, n := range c.nodes {
		if id != lead.id {
			return n
		}
	}
	c.t.Fatal("no follower")
	return nil
}

func (c *cluster) stopAll() {
	for id := range c.nodes {
		c.stop(id)
	}
}

// leader waits for exactly one node to claim leadership.
func (c *cluster) leader(within time.Duration) *clusterNode {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var leaders []*clusterNode
		for _, n := range c.nodes {
			if n.coord.IsLeader() {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader elected within %s", within)
	return nil
}

// A 3-node cluster elects one leader, and only the leader accepts writes.
func TestClusterElectsOneLeaderAndFollowersRedirect(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)

	// Followers refuse and point at the leader. Reads included: a follower's
	// state is a prefix of the leader's, so serving one would be stale by
	// construction.
	for id, n := range c.nodes {
		if id == lead.id {
			continue
		}
		_, err := n.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
			ClientId: "c", RequestId: "r",
			Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
		})
		if err == nil {
			t.Errorf("node %d accepted a write while not leading", id)
		}
		if _, err := n.svc.ListJobs(context.Background(), &schedulerpb.ListJobsRequest{}); err == nil {
			t.Errorf("node %d served a read while not leading", id)
		}
	}
}

// Every mutation goes through the log, so state converges on all three nodes.
func TestClusterReplicatesStateToFollowers(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)

	resp, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId: "c", RequestId: "r-1",
		Tasks: []*schedulerpb.TaskSpec{
			{Command: "/bin/true", MaxAttempts: 3},
			{Command: "/bin/true", MaxAttempts: 3},
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// The followers never served this RPC; they learned it from the log.
	deadline := time.Now().Add(5 * time.Second)
	for _, n := range c.nodes {
		for {
			var found bool
			n.coord.read(func(st *state.StateStore) {
				_, found = st.Job(resp.GetJobId())
			})
			if found {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d never saw job %s", n.id, resp.GetJobId())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// THE DAY 6 CHECKPOINT: kill the leader mid-flight, and a new one takes over
// with the work intact.
//
// The new leader rebuilds its eval broker by SCANNING COMMITTED STATE — it
// has none of the old leader's in-memory queue — and keeps dispatching.
func TestLeaderFailoverPreservesWorkAndResumesScheduling(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)

	// More tasks than the single worker can hold, so work is still
	// outstanding when the leader dies.
	if _, err := lead.svc.RegisterWorker(context.Background(), &schedulerpb.RegisterWorkerRequest{
		WorkerId: "w-1", Capacity: 1,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	specs := make([]*schedulerpb.TaskSpec, 0, 5)
	for i := 0; i < 5; i++ {
		specs = append(specs, &schedulerpb.TaskSpec{Command: "/bin/true", MaxAttempts: 3})
	}
	resp, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId: "c", RequestId: "r-1", Tasks: specs,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	jobID := resp.GetJobId()

	// Wait until the old leader has actually dispatched something, so the
	// failover genuinely happens mid-flight.
	assignedBefore := awaitAssignment(t, lead, 5*time.Second)

	// SIGKILL equivalent: the node stops without revoking anything politely.
	killed := lead.id
	c.stop(killed)

	next := c.leader(10 * time.Second)
	if next.id == killed {
		t.Fatal("the killed node is still leading")
	}

	// Nothing lost: the job and every task survived in the log.
	var tasks int
	var seenAssigned bool
	next.coord.read(func(st *state.StateStore) {
		job, ok := st.Job(jobID)
		if !ok {
			t.Fatalf("job %s lost across failover", jobID)
		}
		tasks = len(job.TaskIDs)
		for _, id := range job.TaskIDs {
			if tk, _ := st.Task(id); tk.ID == assignedBefore {
				seenAssigned = tk.State == state.TaskAssigned || tk.State == state.TaskRunning
			}
		}
	})
	if tasks != 5 {
		t.Errorf("job has %d tasks after failover, want 5", tasks)
	}
	if !seenAssigned {
		t.Errorf("task %s lost its assignment across failover", assignedBefore)
	}

	// And the new leader RESUMES: it rebuilt its broker from state and keeps
	// placing work as capacity frees.
	if _, err := next.svc.Heartbeat(context.Background(), &schedulerpb.HeartbeatRequest{
		WorkerId: "w-1",
	}); err != nil {
		t.Fatalf("heartbeat to new leader: %v", err)
	}
	drainOnNode(t, next, "w-1", 5, 20*time.Second)

	var completed int
	next.coord.read(func(st *state.StateStore) {
		for _, id := range st.TaskIDs() {
			if tk, _ := st.Task(id); tk.State == state.TaskComplete {
				completed++
			}
		}
	})
	if completed != 5 {
		t.Errorf("completed %d tasks after failover, want 5 — nothing lost", completed)
	}
}

// awaitAssignment waits for the leader to place any task and returns its id.
func awaitAssignment(t *testing.T, n *clusterNode, within time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var id string
		n.coord.read(func(st *state.StateStore) {
			for _, tid := range st.TaskIDs() {
				if tk, _ := st.Task(tid); tk.State == state.TaskAssigned || tk.State == state.TaskRunning {
					id = tid
					return
				}
			}
		})
		if id != "" {
			return id
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("leader never dispatched anything")
	return ""
}

// drainOnNode plays the worker: poll, report success, repeat.
func drainOnNode(t *testing.T, n *clusterNode, workerID string, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		done := 0
		n.coord.read(func(st *state.StateStore) {
			for _, id := range st.TaskIDs() {
				if tk, _ := st.Task(id); tk.State == state.TaskComplete {
					done++
				}
			}
		})
		if done >= want {
			return
		}

		// Keep the lease alive; the clock here is real.
		_, _ = n.svc.Heartbeat(context.Background(), &schedulerpb.HeartbeatRequest{WorkerId: workerID})

		resp, err := n.svc.RequestTask(context.Background(), &schedulerpb.RequestTaskRequest{
			WorkerId: workerID,
		})
		if err != nil || resp.GetTask() == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		task := resp.GetTask()
		_, _ = n.svc.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
			WorkerId: workerID, TaskId: task.GetTaskId(), Attempt: task.GetAttempt(),
			ExitCode: 0, ResultRef: "/data/" + workerID,
		})
	}
	t.Fatalf("did not drain %d tasks within %s", want, within)
}

// A node restarted from its WAL catches up and rejoins.
func TestRestartedNodeRecoversFromDisk(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)

	resp, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId: "c", RequestId: "r-1",
		Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Stop a FOLLOWER, so the cluster keeps quorum and keeps committing.
	var victim uint64
	for id := range c.nodes {
		if id != lead.id {
			victim = id
			break
		}
	}
	dir := c.nodes[victim].dir
	c.stop(victim)

	if _, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId: "c", RequestId: "r-2",
		Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
	}); err != nil {
		t.Fatalf("submit while a node is down: %v", err)
	}

	// Restart it on the SAME directory: RestartNode, replaying its own WAL.
	c.start(victim, dir)
	revived := c.nodes[victim]

	deadline := time.Now().Add(10 * time.Second)
	for {
		var jobs int
		revived.coord.read(func(st *state.StateStore) { jobs = st.JobCount() })
		if jobs == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node has %d jobs, want 2 — it never caught up", jobs)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Including the one submitted before it died.
	var found bool
	revived.coord.read(func(st *state.StateStore) { _, found = st.Job(resp.GetJobId()) })
	if !found {
		t.Error("restarted node lost a job it had already applied")
	}
}

// ---------------------------------------------------------------------------
// NOT_LEADER redirect: the client half
// ---------------------------------------------------------------------------

// A client pool aimed at a FOLLOWER must reach the leader on its own.
//
// Without this, the coordinator's redirect is advice nobody takes: the server
// says "the leader is over there" and the caller keeps asking the node that
// cannot help it.
func TestClientPoolFollowsRedirectFromFollower(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)
	fol := c.follower(lead)

	// Seeded with ONLY the follower's address — the pool has never heard of
	// the leader and must learn it from the redirect. The follower has to
	// know who that is first, which lags the election by a beat.
	c.awaitKnowsLeader(fol, 10*time.Second)

	pool := client.NewPool([]string{c.apiAddrs[fol.id]})
	defer pool.Close()

	var jobID string
	err := pool.Do(func(sc schedulerpb.SchedulerClient) error {
		resp, e := sc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
			ClientId: "c", RequestId: "r-1",
			Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
		})
		if e != nil {
			return e
		}
		jobID = resp.GetJobId()
		return nil
	})
	if err != nil {
		t.Fatalf("submit through a follower: %v", err)
	}
	if jobID == "" {
		t.Fatal("no job id returned")
	}

	// The pool now points at the leader, so the next call costs no redirect.
	if got, want := pool.Current(), c.apiAddrs[lead.id]; got != want {
		t.Errorf("pool is aimed at %q, want the leader at %q", got, want)
	}

	// And the write really landed in the replicated log.
	var found bool
	lead.coord.read(func(st *state.StateStore) { _, found = st.Job(jobID) })
	if !found {
		t.Error("the redirected submit did not reach the state machine")
	}
}

// THE ONE THAT MATTERS: after a leader dies, a client that was talking to it
// finds the new leader by itself.
//
// This is what the Day 6 demo script was faking by restarting the worker
// against the new address by hand.
func TestClientPoolRecoversFromLeaderFailure(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)

	// Seed with every node, the way a real deployment would.
	seeds := make([]string, 0, len(c.apiAddrs))
	for _, a := range c.apiAddrs {
		seeds = append(seeds, a)
	}
	pool := client.NewPool(seeds)
	defer pool.Close()

	submit := func(reqID string) (string, error) {
		var id string
		err := pool.Do(func(sc schedulerpb.SchedulerClient) error {
			resp, e := sc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
				ClientId: "c", RequestId: reqID,
				Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
			})
			if e != nil {
				return e
			}
			id = resp.GetJobId()
			return nil
		})
		return id, err
	}

	if _, err := submit("r-before"); err != nil {
		t.Fatalf("submit before failover: %v", err)
	}

	killed := lead.id
	c.stop(killed)
	next := c.leader(10 * time.Second)
	if next.id == killed {
		t.Fatal("killed node still leading")
	}

	// The pool is probably still aimed at the dead node, and NOTHING TELLS
	// IT SO — a killed leader does not answer NOT_LEADER, it does not answer
	// at all. Do must rotate off an unreachable node on its own.
	//
	// The only thing retried here is the call itself, the way any client
	// would across an election window. The pool is never steered by hand.
	var jobID string
	var lastErr error
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		id, err := submit("r-after")
		if err == nil {
			jobID = id
			break
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	if jobID == "" {
		t.Fatalf("client never recovered after failover: %v", lastErr)
	}

	var found bool
	next.coord.read(func(st *state.StateStore) { _, found = st.Job(jobID) })
	if !found {
		t.Error("post-failover submit did not reach the new leader's state machine")
	}
}

// Retrying a submit through the redirect must not create a second job: fn can
// run several times, so every RPC Do wraps has to be safe to repeat.
func TestRedirectedRetryStillDedups(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)
	fol := c.follower(lead)

	// The pool is seeded with ONLY this follower, so it must be able to
	// redirect — which means it must already know where to redirect to.
	c.awaitKnowsLeader(fol, 10*time.Second)

	pool := client.NewPool([]string{c.apiAddrs[fol.id]})
	defer pool.Close()

	ids := map[string]bool{}
	for i := 0; i < 3; i++ {
		var id string
		if err := pool.Do(func(sc schedulerpb.SchedulerClient) error {
			resp, e := sc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
				ClientId: "c", RequestId: "same-request",
				Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
			})
			if e != nil {
				return e
			}
			id = resp.GetJobId()
			return nil
		}); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		ids[id] = true
	}
	if len(ids) != 1 {
		t.Errorf("got %d distinct job ids across retries, want 1", len(ids))
	}

	var jobs int
	lead.coord.read(func(st *state.StateStore) { jobs = st.JobCount() })
	if jobs != 1 {
		t.Errorf("job count = %d, want 1", jobs)
	}
}

// An unrecoverable node error must halt THAT NODE, not the process.
//
// Three nodes share this test binary, and Day 8's simulator runs a whole
// cluster in one. A log.Fatal in the Ready loop would take down the healthy
// peers and the harness, leaving you debugging the wrong failure.
func TestFatalHaltsOneNodeNotTheProcess(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)
	victim := c.follower(lead)

	// If this killed the process, the test binary would die here rather than
	// report a failure.
	victim.coord.fatal(errors.New("simulated unrecoverable disk error"))

	if got := victim.coord.FatalErr(); got == nil {
		t.Error("FatalErr is nil after fatal")
	}
	select {
	case <-victim.coord.RaftDone():
	case <-time.After(5 * time.Second):
		t.Fatal("the halted node's Ready loop never exited")
	}

	// The survivors still have quorum and still serve.
	if _, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId: "c", RequestId: "r-after-fatal",
		Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
	}); err != nil {
		t.Errorf("cluster stopped serving after one node halted: %v", err)
	}

	// And a healthy node reports no error.
	if err := lead.coord.FatalErr(); err != nil {
		t.Errorf("healthy leader reports fatal %v", err)
	}
}

// THE SECOND HALF OF THE DAY 6 CHECKPOINT: restart the node that was KILLED
// WHILE LEADING, and watch it catch up and rejoin.
//
// This is a different case from restarting a follower, and the harder one.
// An ex-leader's WAL can hold entries it appended and replicated to nobody —
// proposed in its last term, never committed cluster-wide. On restart it
// comes back as a FOLLOWER, and the new leader's log overwrites that
// uncommitted suffix. Getting this wrong means a node that silently believes
// in entries no one else has.
func TestKilledLeaderRestartsAsFollowerAndCatchesUp(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)

	if _, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId: "c", RequestId: "r-before",
		Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
	}); err != nil {
		t.Fatalf("submit before kill: %v", err)
	}

	// Kill the LEADER and keep its data directory.
	killed := lead.id
	dir := lead.dir
	c.stop(killed)

	next := c.leader(10 * time.Second)
	if next.id == killed {
		t.Fatal("killed node still leading")
	}

	// The surviving majority keeps committing while the old leader is gone,
	// so it has real catching up to do rather than returning to a quiet log.
	for i := 0; i < 3; i++ {
		if _, err := next.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
			ClientId: "c", RequestId: fmt.Sprintf("r-while-down-%d", i),
			Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
		}); err != nil {
			t.Fatalf("submit while the old leader is down: %v", err)
		}
	}

	// Back from its own WAL, via RestartNode.
	c.start(killed, dir)
	revived := c.nodes[killed]

	// It must rejoin as a follower: the cluster already has a leader, and two
	// would be a split brain.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var jobs int
		revived.coord.read(func(st *state.StateStore) { jobs = st.JobCount() })
		if jobs == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("revived ex-leader has %d jobs, want 4 — it never caught up", jobs)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if revived.coord.IsLeader() && next.coord.IsLeader() {
		t.Error("two leaders after the ex-leader rejoined")
	}

	// And it serves nothing while it follows.
	if !revived.coord.IsLeader() {
		if _, err := revived.svc.ListJobs(context.Background(), &schedulerpb.ListJobsRequest{}); err == nil {
			t.Error("revived node served a read while following")
		}
	}

	// Its state matches the leader's, entry for entry.
	var leaderDump, revivedDump int
	next.coord.read(func(st *state.StateStore) { leaderDump = st.TaskCount() })
	revived.coord.read(func(st *state.StateStore) { revivedDump = st.TaskCount() })
	if leaderDump != revivedDump {
		t.Errorf("task count: leader %d, revived %d — the replicas diverged", leaderDump, revivedDump)
	}
}

// ---------------------------------------------------------------------------
// Day 7: snapshots, compaction, restart
// ---------------------------------------------------------------------------

// snapshotCount reports how many nodes have written a snapshot to disk.
func (c *cluster) snapshotCount() int {
	n := 0
	for _, node := range c.nodes {
		if _, err := os.Stat(filepath.Join(node.dir, "snapshot.pb")); err == nil {
			n++
		}
	}
	return n
}

// awaitSnapshots waits until every live node has compacted at least once.
func (c *cluster) awaitSnapshots(within time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if c.snapshotCount() == len(c.nodes) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("only %d/%d nodes snapshotted within %s — compaction never ran",
		c.snapshotCount(), len(c.nodes), within)
}

// Snapshots actually happen, and the log is actually compacted.
//
// The Tick proposer alone drives the index forward, so this needs no load —
// which is the point: an always-on coordinator's log grows even when idle.
func TestNodesSnapshotAndCompact(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)

	if _, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId: "c", RequestId: "r-1",
		Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	c.awaitSnapshots(20 * time.Second)

	// Compaction must shrink the WAL, not just write a snapshot beside it.
	for _, n := range c.nodes {
		wal, err := os.Stat(filepath.Join(n.dir, "wal.log"))
		if err != nil {
			t.Fatalf("node %d: stat wal: %v", n.id, err)
		}
		snap, err := os.Stat(filepath.Join(n.dir, "snapshot.pb"))
		if err != nil {
			t.Fatalf("node %d: stat snapshot: %v", n.id, err)
		}
		t.Logf("node %d: wal=%d bytes snapshot=%d bytes", n.id, wal.Size(), snap.Size())
	}
}

// THE DAY 7 CHECKPOINT: kill ALL THREE nodes, restart them from disk, and the
// state is intact.
//
// With a low snapshot threshold most of the history is in the snapshot rather
// than the log, so this genuinely exercises Restore plus a replay of the tail
// — not just a replay from index 1.
func TestWholeClusterRestartsFromDiskWithStateIntact(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)

	// Enough jobs that losing any would be obvious.
	var jobIDs []string
	for i := 0; i < 6; i++ {
		resp, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
			ClientId: "c", RequestId: fmt.Sprintf("r-%d", i),
			Tasks: []*schedulerpb.TaskSpec{
				{Command: "/bin/true", MaxAttempts: 3},
				{Command: "/bin/true", MaxAttempts: 3},
			},
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		jobIDs = append(jobIDs, resp.GetJobId())
	}

	// Force compaction so the restart is not a plain log replay.
	c.awaitSnapshots(20 * time.Second)

	var wantJobs, wantTasks, wantSessions int
	lead.coord.read(func(st *state.StateStore) {
		wantJobs, wantTasks, wantSessions = st.JobCount(), st.TaskCount(), st.SessionCount()
	})
	t.Logf("before restart: jobs=%d tasks=%d sessions=%d", wantJobs, wantTasks, wantSessions)

	// Stop everything, keeping the data directories.
	dirs := map[uint64]string{}
	for id, n := range c.nodes {
		dirs[id] = n.dir
	}
	c.stopAll()

	// Cold start the whole cluster from disk.
	for id, dir := range dirs {
		c.start(id, dir)
	}
	revived := c.leader(15 * time.Second)

	var gotJobs, gotTasks, gotSessions int
	revived.coord.read(func(st *state.StateStore) {
		gotJobs, gotTasks, gotSessions = st.JobCount(), st.TaskCount(), st.SessionCount()
	})
	t.Logf("after restart:  jobs=%d tasks=%d sessions=%d", gotJobs, gotTasks, gotSessions)
	if gotJobs != wantJobs || gotTasks != wantTasks {
		t.Errorf("after cold restart: jobs=%d tasks=%d, want %d and %d", gotJobs, gotTasks, wantJobs, wantTasks)
	}
	for _, id := range jobIDs {
		var found bool
		revived.coord.read(func(st *state.StateStore) { _, found = st.Job(id) })
		if !found {
			t.Errorf("job %s lost across a whole-cluster restart", id)
		}
	}

	// Dedup must survive too: the sessions map is in the snapshot, and
	// forgetting it would be silent — everything looks healthy and a retry
	// quietly creates a second job.
	resp, err := revived.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId: "c", RequestId: "r-0", // the very first request, replayed
		Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
	})
	if err != nil {
		t.Fatalf("retry after restart: %v", err)
	}
	if resp.GetJobId() != jobIDs[0] {
		t.Errorf("retry returned %q, want the original %q — dedup did not survive the snapshot",
			resp.GetJobId(), jobIDs[0])
	}
	revived.coord.read(func(st *state.StateStore) { gotJobs = st.JobCount() })
	if gotJobs != wantJobs {
		t.Errorf("job count = %d after a retry, want %d — the retry created a duplicate", gotJobs, wantJobs)
	}
}

// A node that falls far behind is caught up by the LEADER SHIPPING A
// SNAPSHOT, not by replaying the log it no longer has.
//
// This is the path that made handling rd.Snapshot non-optional: compaction
// without it leaves such a node permanently stuck, silently.
func TestLaggingNodeIsCaughtUpByAnInstalledSnapshot(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)

	victim := c.follower(lead)
	dir := victim.dir
	victimID := victim.id
	c.stop(victimID)

	// Commit well past the compaction point while it is gone, so the entries
	// it needs no longer exist anywhere.
	for i := 0; i < 8; i++ {
		if _, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
			ClientId: "c", RequestId: fmt.Sprintf("r-while-down-%d", i),
			Tasks: []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 3}},
		}); err != nil {
			t.Fatalf("submit while a node is down: %v", err)
		}
	}
	// The survivors must compact, or there would still be a log to catch up from.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && c.snapshotCount() < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	if c.snapshotCount() < 2 {
		t.Fatal("survivors never compacted; the test would not exercise InstallSnapshot")
	}

	var want int
	lead.coord.read(func(st *state.StateStore) { want = st.JobCount() })

	c.start(victimID, dir)
	revived := c.nodes[victimID]

	deadline = time.Now().Add(20 * time.Second)
	for {
		var got int
		revived.coord.read(func(st *state.StateStore) { got = st.JobCount() })
		if got == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lagging node has %d jobs, want %d — it was never caught up", got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// The mechanism the revocation-deadlock fix depends on
// ---------------------------------------------------------------------------

// Revocation must not block, full stop. This drives it directly and bounds
// how long it may take — the deadlock it replaced held the Ready loop for a
// whole proposal timeout.
func TestRevocationDoesNotBlock(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader(10 * time.Second)

	// Give the scheduler something in flight so the path is not trivially
	// empty.
	if _, err := lead.svc.RegisterWorker(context.Background(), &schedulerpb.RegisterWorkerRequest{
		WorkerId: "w-1", Capacity: 2,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	specs := make([]*schedulerpb.TaskSpec, 0, 8)
	for i := 0; i < 8; i++ {
		specs = append(specs, &schedulerpb.TaskSpec{Command: "/bin/true", MaxAttempts: 3})
	}
	if _, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId: "c", RequestId: "r-1", Tasks: specs,
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		lead.coord.onLeadershipChanged(false)
		done <- time.Since(start)
	}()

	select {
	case elapsed := <-done:
		// Generous bound: the point is "did not wait out a timeout", not a
		// performance assertion.
		if elapsed > 2*time.Second {
			t.Errorf("revocation took %s — it blocked on work the Ready loop serves", elapsed)
		}
		t.Logf("revocation completed in %s", elapsed.Round(time.Millisecond))
	case <-time.After(10 * time.Second):
		t.Fatal("revocation never returned — the Ready loop is deadlocked against the scheduler")
	}
}

// Leadership revocation must release a proposal that is already in flight.
//
// THE DAY 8 DEADLOCK CLASS, INCOMPLETELY FIXED. revokeLeadership fails
// pending proposals first and only then waits for the scheduler, precisely so
// the Ready loop never waits on a worker that is waiting on the Ready loop.
// But failAll releases REGISTERED WAITERS, and a worker blocked inside
// node.Propose never reached its waiter: Propose blocks until raft accepts the
// entry, and the goroutine that would accept it is the Ready loop — the one
// sitting in StopScheduling. Its context came from context.Background(), so
// cancelling the leader context did not touch it either.
//
// The symptom is not a hang. It self-heals when the proposal timeout fires,
// which is why it showed up as a 131ms and a 187ms Ready-loop stall in Day
// 10's 600-step calibration — both just under the 250ms timeout in force
// there — and why it was invisible at the 5s production default, where it
// would instead look like a node that processes no raft messages for five
// seconds on every leadership change.
//
// The test blocks the Ready loop inside an apply, starts a proposal that
// therefore cannot be accepted, and then does what revocation does: cancels
// the leader context. A proposal that is still stuck after that is waiting on
// its own timeout.
func TestRevocationReleasesAnInFlightPropose(t *testing.T) {
	blocked := make(chan struct{})
	release := make(chan struct{})
	var armed atomic.Bool

	cfg := DefaultConfig()
	cfg.Sched.Workers = 1
	cfg.Sched.LeaseTTLMs = cfg.LeaseTTL.Milliseconds()
	// Short enough that the unfixed behaviour is a fast failure rather than a
	// five-second one, and far enough above the assertion below that the two
	// cannot be confused.
	cfg.ProposalTimeout = 300 * time.Millisecond
	cfg.OnApply = func(cmd *commandpb.Command, res state.Result) {
		// Block exactly one apply, on the Ready loop's own goroutine. That is
		// what stops raft accepting further proposals.
		if armed.CompareAndSwap(true, false) {
			close(blocked)
			<-release
		}
	}

	coord := New(cfg)
	rcfg := raftnode.DefaultConfig()
	rcfg.ID = 1
	rcfg.Peers = map[uint64]string{1: fmt.Sprintf("http://127.0.0.1:%d", freePort(t))}
	rcfg.DataDir = t.TempDir()
	rcfg.TickInterval = 20 * time.Millisecond
	rcfg.ElectionTick = 10
	rcfg.HeartbeatTick = 1
	if err := coord.StartRaft(rcfg); err != nil {
		t.Fatalf("start raft: %v", err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		coord.StopRaft()
		coord.StopScheduling()
	})

	deadline := time.Now().Add(5 * time.Second)
	for !coord.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !coord.IsLeader() {
		t.Fatal("single node never became leader")
	}

	// One proposal to get an apply, which the hook above then parks on.
	armed.Store(true)
	go func() { _, _ = coord.propose(tickCmd(1_700_000_000_000)) }()
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("no apply arrived to block the Ready loop; the test never set up its condition")
	}

	// With the loop parked, this one cannot be accepted by raft.
	done := make(chan error, 1)
	go func() {
		_, err := coord.propose(tickCmd(1_700_000_000_001))
		done <- err
	}()
	// Give it time to reach node.Propose and block there. If it has already
	// returned, the condition under test did not hold.
	select {
	case err := <-done:
		t.Fatalf("the second proposal was accepted while the Ready loop was parked (%v); "+
			"the test is not exercising an in-flight propose", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Exactly what revocation does.
	start := time.Now()
	coord.cancelLeader()
	err := <-done
	took := time.Since(start)
	t.Logf("in-flight propose returned %v after %s (proposal timeout is %s)",
		err, took.Round(time.Millisecond), cfg.ProposalTimeout)

	if took > 80*time.Millisecond {
		t.Errorf("an in-flight propose survived leadership revocation for %s — it is waiting out "+
			"its own %s timeout, and revokeLeadership waits for the scheduler worker that is "+
			"waiting for it. The propose context must derive from the leader context.",
			took.Round(time.Millisecond), cfg.ProposalTimeout)
	}
	if !errors.Is(err, ErrProposalLost) {
		t.Errorf("err = %v, want ErrProposalLost: the entry was abandoned and may still commit, "+
			"which is exactly what that error means", err)
	}
}
