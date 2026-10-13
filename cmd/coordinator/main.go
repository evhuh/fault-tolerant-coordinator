// Command coordinator serves the scheduler API over gRPC.
//
// Day 3: one node, no Raft. The StateStore is mutated directly through
// Coordinator.propose. Day 6 replaces that body with node.Propose() and adds
// the Ready loop, the peer transport, and NOT_LEADER redirection — this
// binary grows flags for peers and a data directory then.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/evazheng/fault-tolerant-coordinator/internal/raftnode"
	"github.com/evazheng/fault-tolerant-coordinator/internal/server"
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

	if raftEnabled {
		rcfg := raftnode.DefaultConfig()
		rcfg.ID = *raftID
		rcfg.Peers = peers
		rcfg.DataDir = dir
		rcfg.TickInterval = *raftTick
		rcfg.ElectionTick = *electionTick
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
