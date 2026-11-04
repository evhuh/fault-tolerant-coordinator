// Command coordinator serves the scheduler API over gRPC.
//
// Day 3: one node, no Raft. The StateStore is mutated directly through
// Coordinator.propose. Day 6 replaces that body with node.Propose() and adds
// the Ready loop, the peer transport, and NOT_LEADER redirection — this
// binary grows flags for peers and a data directory then.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/evazheng/fault-tolerant-coordinator/internal/metrics"
	"github.com/evazheng/fault-tolerant-coordinator/internal/raftnode"
	"github.com/evazheng/fault-tolerant-coordinator/internal/server"
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

func main() {
	addr := flag.String("addr", ":8080", "gRPC listen address")
	leaseTTL := flag.Duration("lease-ttl", 2000*time.Millisecond, "worker lease TTL (4 missed heartbeats at H=500ms)")
	tickPeriod := flag.Duration("tick", 200*time.Millisecond, "how often to propose Tick{nowMs}")
	schedWorkers := flag.Int("scheduler-workers", 4, "evals decided concurrently")
	maxPlanSize := flag.Int("max-plan-size", 1000, "assignments per committed plan")
	raftID := flag.Uint64("id", 0, "this node's raft id; 0 runs single-node with no Raft")
	peerSpec := flag.String("peers", "", "comma-separated id=url raft peers, including this node")
	dataDir := flag.String("data-dir", "", "WAL directory (default ./raft-<id>)")
	apiPeerSpec := flag.String("api-peers", "", "comma-separated id=host:port gRPC addresses, for NOT_LEADER redirects")
	raftTick := flag.Duration("raft-tick", 100*time.Millisecond, "raft tick interval; the time unit for election/heartbeat ticks")
	electionTick := flag.Int("election-tick", 10, "election timeout in raft ticks")
	snapEntries := flag.Uint64("snapshot-entries", 10000, "applied entries between snapshots; low values exercise Restore")
	snapCatchUp := flag.Uint64("snapshot-catchup-entries", 1000, "entries kept after compaction so a lagging follower can catch up from the log")
	metricsAddr := flag.String("metrics-addr", "", "Prometheus listen address, e.g. :9090; empty disables it")
	metricsSample := flag.Duration("metrics-sample", time.Second, "how often queue-depth gauges are resampled from committed state")
	flag.Parse()

	cfg := server.DefaultConfig()
	cfg.LeaseTTL = *leaseTTL
	cfg.TickPeriod = *tickPeriod
	cfg.Sched.Workers = *schedWorkers
	cfg.Sched.MaxPlanSize = *maxPlanSize
	cfg.Sched.LeaseTTLMs = leaseTTL.Milliseconds()

	// Raft is opt-in by -id, so the single-node path every earlier day
	// depended on still works unchanged.
	raftEnabled := *raftID != 0
	var peers map[uint64]string
	var dir string
	if raftEnabled {
		var err error
		peers, err = parsePeers(*peerSpec)
		if err != nil {
			log.Fatalf("peers: %v", err)
		}
		if _, ok := peers[*raftID]; !ok {
			log.Fatalf("node id %d is not in -peers", *raftID)
		}
		dir = *dataDir
		if dir == "" {
			dir = fmt.Sprintf("./raft-%d", *raftID)
		}
		// REQUIRED with -id. The gRPC address space is separate from raft's:
		// a redirected client dials host:8080, not the raft transport URL.
		// Without this map every NOT_LEADER says "no leader elected" even
		// when one exists, and clients never find the cluster.
		if *apiPeerSpec == "" {
			log.Fatal("-api-peers is required with -id: " +
				"clients are redirected to gRPC addresses, which are not the raft peer URLs")
		}
		apiPeers, err := parsePeers(*apiPeerSpec)
		if err != nil {
			log.Fatalf("api-peers: %v", err)
		}
		if _, ok := apiPeers[*raftID]; !ok {
			log.Fatalf("node id %d is not in -api-peers", *raftID)
		}
		cfg.APIAddrs = apiPeers
	}

	// Config is complete before the coordinator is built; nothing mutates it
	// afterwards.
	coord := server.New(cfg)
	svc := server.NewService(coord)

	// METRICS ON THEIR OWN LISTENER, not the gRPC port. A scrape that
	// contends with the API it is measuring changes the measurement, and
	// during a benchmark the scraper is the one client guaranteed to be
	// running.
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		// PPROF ON THE SAME PRIVATE LISTENER as the metrics, never on the
		// client API port. A profile endpoint is a debugging surface, and the
		// reason it is here at all is that a Ready-loop stall under load
		// cannot be diagnosed from counters: they say the apply phase blocked
		// for 600ms, not which function spent the time.
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		// /debug/workers exists so a BENCHMARK can define t=0 exactly.
		//
		// Worker reclaim latency is measured from the last heartbeat
		// RECEIVED, which is `lease_expiry_ms - leaseTTL` in committed state.
		// Nothing outside the process could read that, so the Day 11 trial
		// would have had to infer t=0 instead of knowing it.
		//
		// Read-only, and on the private listener rather than the client API:
		// it bypasses the NOT_LEADER guard and reports one node's local view,
		// so it is a debugging surface in the same category as pprof and must
		// never be mistaken for an API. It reports the committed clock
		// alongside the leases, because a lease expiry means nothing without
		// the clock it is compared against.
		// /debug/tasks is the companion to /debug/workers, and exists for the
		// same reason: a benchmark that cannot see why it failed has to be
		// re-run with print statements, which is how two diagnoses in this
		// project cost a cycle each.
		//
		// A SUMMARY, not a dump. Per-job counts by state plus how many tasks
		// are one attempt from their budget, because "the harness had nothing
		// to kill" and "every task had exhausted its attempts" look identical
		// from outside and have different fixes.
		mux.HandleFunc("/debug/tasks", func(w http.ResponseWriter, r *http.Request) {
			type jobView struct {
				JobID      string `json:"job_id"`
				Pending    int    `json:"pending"`
				Assigned   int    `json:"assigned"`
				Running    int    `json:"running"`
				Complete   int    `json:"complete"`
				Failed     int    `json:"failed"`
				NearBudget int    `json:"near_budget"`
				Backoff    int    `json:"in_backoff"`
			}
			out := struct {
				NowMs        int64     `json:"now_ms"`
				IsLeader     bool      `json:"is_leader"`
				EvalsReady   int       `json:"evals_ready"`
				EvalsBlocked int       `json:"evals_blocked"`
				Jobs         []jobView `json:"jobs"`
			}{IsLeader: coord.IsLeader()}
			out.EvalsReady, out.EvalsBlocked = coord.BrokerStats()
			byJob := map[string]*jobView{}
			coord.ReadState(func(st *state.StateStore) {
				out.NowMs = st.NowMs()
				for _, id := range st.TaskIDs() {
					t, ok := st.Task(id)
					if !ok {
						continue
					}
					j := byJob[t.JobID]
					if j == nil {
						j = &jobView{JobID: t.JobID}
						byJob[t.JobID] = j
					}
					switch t.State {
					case state.TaskPending:
						j.Pending++
						if t.NotBeforeMs > st.NowMs() {
							j.Backoff++
						}
					case state.TaskAssigned:
						j.Assigned++
					case state.TaskRunning:
						j.Running++
					case state.TaskComplete:
						j.Complete++
					case state.TaskFailed:
						j.Failed++
					}
					if !terminalState(t.State) && t.Attempt+1 >= t.Spec.MaxAttempts {
						j.NearBudget++
					}
				}
			})
			for _, j := range byJob {
				out.Jobs = append(out.Jobs, *j)
			}
			sort.Slice(out.Jobs, func(i, k int) bool { return out.Jobs[i].JobID < out.Jobs[k].JobID })
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
		})
		mux.HandleFunc("/debug/workers", func(w http.ResponseWriter, r *http.Request) {
			type heldView struct {
				TaskID  string `json:"task_id"`
				State   string `json:"state"`
				Attempt int32  `json:"attempt"`
			}
			type workerView struct {
				ID            string   `json:"worker_id"`
				State         string   `json:"state"`
				RegisteredMs  int64    `json:"registered_ms"`
				LeaseExpiryMs int64    `json:"lease_expiry_ms"`
				Capacity      int32    `json:"capacity"`
				HeldTaskIDs   []string `json:"held_task_ids"`
				// Held carries each task's STATE, because ASSIGNED and
				// RUNNING are different facts: a worker that has been handed
				// a task has not necessarily started it, and a reclaim
				// benchmark that kills the first case measures a dispatch
				// race rather than a lease expiry.
				Held []heldView `json:"held"`
			}
			out := struct {
				NowMs    int64        `json:"now_ms"`
				WallMs   int64        `json:"wall_ms"`
				IsLeader bool         `json:"is_leader"`
				Workers  []workerView `json:"workers"`
			}{WallMs: time.Now().UnixMilli(), IsLeader: coord.IsLeader()}
			coord.ReadState(func(st *state.StateStore) {
				out.NowMs = st.NowMs()
				for _, id := range st.WorkerIDs() {
					wk, ok := st.Worker(id)
					if !ok {
						continue
					}
					heldIDs := st.TasksHeldBy(id)
					held := make([]heldView, 0, len(heldIDs))
					for _, tid := range heldIDs {
						t, ok := st.Task(tid)
						if !ok {
							continue
						}
						held = append(held, heldView{
							TaskID: tid, State: t.State.String(), Attempt: t.Attempt,
						})
					}
					out.Workers = append(out.Workers, workerView{
						ID: wk.ID, State: wk.State.String(),
						RegisteredMs: wk.RegisteredMs, LeaseExpiryMs: wk.LeaseExpiryMs,
						Capacity: wk.Capacity, HeldTaskIDs: heldIDs, Held: held,
					})
				}
			})
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
		})
		ms := &http.Server{Addr: *metricsAddr, Handler: mux}
		go func() {
			if err := ms.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				// NOT fatal. Losing instrumentation must not take down a
				// coordinator that is serving clients correctly.
				log.Printf("metrics listener: %v", err)
			}
		}()
		defer ms.Close()
		go sampleGauges(coord, *metricsSample)
		// The listener carries pprof and /debug/workers, so a non-loopback
		// bind is worth saying out loud rather than discovering later.
		if h, _, err := net.SplitHostPort(*metricsAddr); err != nil || !isLoopback(h) {
			log.Printf("WARNING: metrics listener %s is not loopback-bound, and it serves "+
				"pprof and /debug/workers — bind it to 127.0.0.1 unless the network is trusted",
				*metricsAddr)
		}
		log.Printf("metrics on %s/metrics and pprof on %s/debug/pprof/, gauges resampled every %s",
			*metricsAddr, *metricsAddr, *metricsSample)
	}

	if raftEnabled {
		rcfg := raftnode.DefaultConfig()
		rcfg.ID = *raftID
		rcfg.Peers = peers
		rcfg.DataDir = dir
		rcfg.TickInterval = *raftTick
		rcfg.ElectionTick = *electionTick
		rcfg.SnapshotEntries = *snapEntries
		rcfg.SnapshotCatchUpEntries = *snapCatchUp
		if err := coord.StartRaft(rcfg); err != nil {
			log.Fatalf("start raft: %v", err)
		}
		defer coord.StopRaft()

		// A standalone binary DOES exit on an unrecoverable node error. The
		// library halts the node and reports; deciding to die is the host's
		// call, because in-process clusters must not.
		go func() {
			<-coord.RaftDone()
			if err := coord.FatalErr(); err != nil {
				log.Fatalf("coordinator: halted: %v", err)
			}
		}()
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	gs := grpc.NewServer()
	schedulerpb.RegisterSchedulerServer(gs, svc)

	// The ticker is the state machine's only clock. On Day 6 it starts on
	// election and stops on loss, alongside the eval broker and the planner.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// With Raft, the leader-only components (eval broker, scheduler workers,
	// planner, Tick proposer) are started by establishLeadership and stopped
	// by revokeLeadership. Without it, this node always leads, so they start
	// here and never stop.
	if !raftEnabled {
		go coord.RunTicker(ctx)
		coord.StartScheduling(ctx)
		defer coord.StopScheduling()
	}

	go func() {
		<-ctx.Done()
		log.Print("coordinator: shutting down")
		gs.GracefulStop()
	}()

	mode := "single-node (no raft)"
	if raftEnabled {
		mode = fmt.Sprintf("raft id=%d", *raftID)
	}
	log.Printf("coordinator: listening on %s [%s] (lease=%s tick=%s schedulers=%d max-plan=%d)",
		*addr, mode, cfg.LeaseTTL, cfg.TickPeriod, cfg.Sched.Workers, cfg.Sched.MaxPlanSize)
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// parsePeers reads a "1=http://host:port,2=..." spec. Membership is fixed at
// startup — cluster changes are deliberately out of scope.
func parsePeers(spec string) (map[uint64]string, error) {
	if spec == "" {
		return nil, fmt.Errorf("-peers is required with -id")
	}
	out := map[uint64]string{}
	for _, kv := range strings.Split(spec, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("bad peer spec %q, want id=url", kv)
		}
		id, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad peer id %q: %w", k, err)
		}
		out[id] = v
	}
	return out, nil
}

// sampleGauges refreshes the queue-depth and pending-eval gauges.
//
// Reads committed state through the coordinator's own read path, so it sees
// exactly what a client would and cannot invent a depth the state machine
// does not have. Runs on every node, not just the leader: a follower's queue
// depth is a real measurement — it is the same committed state — and a gauge
// that vanished on failover would look like the queue draining.
func sampleGauges(coord *server.Coordinator, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for range t.C {
		var pending, assigned, running int
		coord.ReadState(func(st *state.StateStore) {
			counts := st.TaskStateCounts()
			pending = counts[state.TaskPending]
			assigned = counts[state.TaskAssigned]
			running = counts[state.TaskRunning]
		})
		ready, blocked := coord.BrokerStats()
		metrics.Sample(pending, assigned, running, ready+blocked)
	}
}

// isLoopback reports whether a listen host is unambiguously local. An empty
// host means "all interfaces", which is the case the warning exists for.
func isLoopback(host string) bool {
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// terminalState mirrors the state machine's notion of terminal for the debug
// summary. Duplicated rather than exported from state, because a debug
// endpoint must not be a reason to widen that package's API.
func terminalState(s state.TaskState) bool {
	return s == state.TaskComplete || s == state.TaskFailed
}
