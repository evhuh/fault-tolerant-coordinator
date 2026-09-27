// Command worker registers with a coordinator, long-polls for tasks, executes
// them as subprocesses, and reports results.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/evazheng/fault-tolerant-coordinator/internal/worker"
)

func main() {
	coordAddr := flag.String("coordinator", "localhost:8080", "coordinator address")
	id := flag.String("id", "", "worker id (required, stable across restarts)")
	dataDir := flag.String("data-dir", "", "where task stdout/stderr are written (default ./data-<id>)")
	concurrency := flag.Int("concurrency", 1, "tasks executed at once")
	heartbeat := flag.Duration("heartbeat", 500*time.Millisecond, "lease renewal interval")
	flag.Parse()

	if *id == "" {
		log.Fatal("-id is required")
	}
	dir := *dataDir
	if dir == "" {
		dir = "./data-" + *id
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatalf("data dir: %v", err)
	}

	conn, err := grpc.NewClient(*coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *coordAddr, err)
	}
	defer conn.Close()

	cfg := worker.DefaultConfig(*id, dir)
	cfg.Concurrency = *concurrency
	cfg.HeartbeatInterval = *heartbeat

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("worker %s: coordinator=%s data=%s concurrency=%d", *id, *coordAddr, dir, *concurrency)
	if err := worker.New(cfg, conn).Run(ctx); err != nil {
		log.Fatalf("worker %s: %v", *id, err)
	}
}
