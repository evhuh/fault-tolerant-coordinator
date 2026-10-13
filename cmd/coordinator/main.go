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
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/evazheng/fault-tolerant-coordinator/internal/server"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

func main() {
	addr := flag.String("addr", ":8080", "gRPC listen address")
	leaseTTL := flag.Duration("lease-ttl", 2000*time.Millisecond, "worker lease TTL (4 missed heartbeats at H=500ms)")
	tickPeriod := flag.Duration("tick", 200*time.Millisecond, "how often to propose Tick{nowMs}")
	schedWorkers := flag.Int("scheduler-workers", 4, "evals decided concurrently")
	maxPlanSize := flag.Int("max-plan-size", 1000, "assignments per committed plan")
	flag.Parse()

	cfg := server.DefaultConfig()
	cfg.LeaseTTL = *leaseTTL
	cfg.TickPeriod = *tickPeriod
	cfg.Sched.Workers = *schedWorkers
	cfg.Sched.MaxPlanSize = *maxPlanSize
	cfg.Sched.LeaseTTLMs = leaseTTL.Milliseconds()

	coord := server.New(cfg)
	svc := server.NewService(coord)

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
	go coord.RunTicker(ctx)

	// Leader-only components. On Day 6 these move under establishLeadership,
	// and StopScheduling runs on leadership loss — stopping cleanly is what
	// prevents two leaders scheduling against the same state.
	coord.StartScheduling(ctx)
	defer coord.StopScheduling()

	go func() {
		<-ctx.Done()
		log.Print("coordinator: shutting down")
		gs.GracefulStop()
	}()

	log.Printf("coordinator: listening on %s (lease=%s tick=%s schedulers=%d max-plan=%d)",
		*addr, cfg.LeaseTTL, cfg.TickPeriod, cfg.Sched.Workers, cfg.Sched.MaxPlanSize)
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
