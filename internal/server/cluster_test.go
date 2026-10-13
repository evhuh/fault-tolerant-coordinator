package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

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
}

type cluster struct {
	t     *testing.T
	nodes map[uint64]*clusterNode
	peers map[uint64]string
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
	c := &cluster{t: t, nodes: map[uint64]*clusterNode{}, peers: map[uint64]string{}}

	for i := 1; i <= n; i++ {
		c.peers[uint64(i)] = fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
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
	c.nodes[id] = &clusterNode{id: id, coord: coord, svc: NewService(coord), dir: dir}
}

func (c *cluster) stop(id uint64) {
	if n, ok := c.nodes[id]; ok {
		n.coord.StopRaft()
		n.coord.StopScheduling()
		delete(c.nodes, id)
	}
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
