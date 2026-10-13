package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/evazheng/fault-tolerant-coordinator/internal/client"
	"github.com/evazheng/fault-tolerant-coordinator/internal/raftnode"
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
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
	// the leader and must learn it from the redirect.
	pool := client.NewPool(c.apiAddrs[fol.id])
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
	pool := client.NewPool(seeds...)
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

	pool := client.NewPool(c.apiAddrs[fol.id])
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
