// Command worker registers with a coordinator, long-polls for tasks, executes
// them as subprocesses, and reports results.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/client"
	"github.com/evazheng/fault-tolerant-coordinator/internal/worker"
)

func main() {
	coordAddr := flag.String("coordinator", "localhost:8080", "comma-separated coordinator addresses; any one will do, NOT_LEADER redirects find the rest")
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

	// A pool, not a connection: the coordinator answers NOT_LEADER with the
	// leader's address, and the worker has to actually follow it or a
	// failover strands it against a deposed node.
	pool := client.NewPool(strings.Split(*coordAddr, ",")...)
	defer pool.Close()

	cfg := worker.DefaultConfig(*id, dir)
	cfg.Concurrency = *concurrency
	cfg.HeartbeatInterval = *heartbeat

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("worker %s: coordinator=%s data=%s concurrency=%d", *id, *coordAddr, dir, *concurrency)
	if err := worker.New(cfg, pool).Run(ctx); err != nil {
		log.Fatalf("worker %s: %v", *id, err)
	}
}
