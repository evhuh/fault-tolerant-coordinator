// Command simulator runs a coordinator cluster under seeded fault injection.
//
//	simulator                    # one random seed, printed
//	simulator -seed 42           # reproduce exactly that schedule
//	simulator -runs 50           # sweep 50 consecutive seeds
//	SCHED_SEED=42 simulator      # same as -seed
//
// THE SEED IS PRINTED ON EVERY RUN AND ON EVERY FAILURE. That is the whole
// point: a failure you cannot get back to is an anecdote.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/server"
	"github.com/evazheng/fault-tolerant-coordinator/internal/sim"
	"github.com/evazheng/fault-tolerant-coordinator/internal/simnet"
)

func main() {
	seedFlag := flag.Int64("seed", 0, "seed to run; 0 picks one from the clock and prints it")
	runs := flag.Int("runs", 1, "how many consecutive seeds to run")
	steps := flag.Int("steps", 40, "fault steps per run")
	nodes := flag.Int("nodes", 3, "coordinator nodes")
	dropRate := flag.Float64("drop-rate", 0.02, "probability a raft message is dropped")
	corruptRate := flag.Float64("corrupt-rate", 0.01, "probability a raft message's bytes are mangled in flight")
	deliverCorrupt := flag.Bool("deliver-corrupt", false,
		"deliver mangled frames instead of dropping them at the link; OUTSIDE raft's fault model, "+
			"excluded from correctness claims")
	failureRate := flag.Float64("failure-rate", 0.15, "fraction of task executions that report failure")
	maxDelay := flag.Duration("max-delay", 5*time.Millisecond, "upper bound on message delay")
	stepTime := flag.Duration("step-time", 150*time.Millisecond, "wall time the cluster runs per step")
	linClients := flag.Int("lin-clients", 3,
		"history-recording clients for the linearizability check; 0 disables it")
	linVizDir := flag.String("lin-viz-dir", "",
		"directory for the HTML timeline written when a history is not linearizable")
	readMode := flag.String("read-mode", "readindex",
		"how client reads are served: readindex (quorum barrier) or local (NOT linearizable)")
	stallThreshold := flag.Duration("stall-threshold", 0,
		"override the Ready-loop stall budget; LOWER it to profile which phase owns the slow tail")
	verbose := flag.Bool("v", false, "log every fault as it is applied")
	flag.Parse()

	seed := *seedFlag
	if env := os.Getenv("SCHED_SEED"); env != "" && seed == 0 {
		v, err := strconv.ParseInt(env, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad SCHED_SEED %q: %v\n", env, err)
			os.Exit(2)
		}
		seed = v
	}
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	mode, err := server.ParseReadMode(*readMode)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	failures := 0
	for i := 0; i < *runs; i++ {
		s := seed + int64(i)
		if err := runOne(ctx, s, *steps, *nodes, *dropRate, *corruptRate, *failureRate,
			*deliverCorrupt, *maxDelay, *stepTime, *verbose, *linClients, *linVizDir, mode, *stallThreshold); err != nil {
			failures++
			fmt.Printf("\nFAILED  seed=%d\n", s)
			fmt.Printf("  %v\n", err)
			fmt.Printf("  reproduce with:  simulator -seed %d -steps %d -nodes %d -v\n", s, *steps, *nodes)
		}
		if ctx.Err() != nil {
			break
		}
	}

	if failures > 0 {
		fmt.Printf("\n%d/%d runs failed\n", failures, *runs)
		os.Exit(1)
	}
	fmt.Printf("\n%d/%d runs passed\n", *runs, *runs)
}

func runOne(ctx context.Context, seed int64, steps, nodes int, dropRate, corruptRate, failureRate float64,
	deliverCorrupt bool, maxDelay, stepTime time.Duration, verbose bool,
	linClients int, linVizDir string, mode server.ReadMode, stallThreshold time.Duration) error {

	dir, err := os.MkdirTemp("", fmt.Sprintf("sim-%d-", seed))
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	dirs := map[uint64]string{}
	for i := 1; i <= nodes; i++ {
		d := fmt.Sprintf("%s/node-%d", dir, i)
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
		dirs[uint64(i)] = d
	}

	cfg := sim.DefaultConfig()
	cfg.Seed = seed
	cfg.Nodes = nodes
	cfg.Steps = steps
	cfg.StepTime = stepTime
	cfg.Faults = simnet.Faults{
		DropRate: dropRate, CorruptRate: corruptRate,
		DeliverCorrupt: deliverCorrupt, MaxDelay: maxDelay,
	}
	cfg.FailureRate = failureRate
	cfg.ReadMode = mode
	cfg.StallThreshold = stallThreshold
	cfg.LinClients = linClients
	cfg.LinVizDir = linVizDir
	cfg.Out = io.Discard
	if verbose {
		cfg.Out = os.Stdout
	}

	fmt.Printf("seed=%-20d nodes=%d steps=%d drop=%.0f%% reads=%s ... ",
		seed, nodes, steps, dropRate*100, mode)
	start := time.Now()
	res, err := sim.New(cfg, dirs).Run(ctx)
	elapsed := time.Since(start).Round(time.Millisecond)

	if err != nil {
		fmt.Printf("FAIL (%s)\n", elapsed)
		return err
	}
	n, c := res.Faults.Net, res.Coverage
	fmt.Printf("ok (%s)\n", elapsed)
	fmt.Printf("    faults:   kills=%d restarts=%d partitions=%d (%d of the leader) heals=%d\n",
		res.Faults.NodeKills, res.Faults.NodeRestarts, res.Faults.Partitions,
		res.Faults.LeaderPartitions, res.Faults.Heals)
	fmt.Printf("    messages: sent=%d delivered=%d  dropped: random=%d partition=%d node-down=%d  corrupt: %d mangled, %d dropped at link, %d delivered (%d refused by decoder, %d reached raft)\n",
		n.Sent, n.Delivered, n.DroppedRandom, n.DroppedPartition, n.DroppedDown,
		n.Corrupted, n.CorruptDroppedAtLink, n.CorruptDelivered,
		n.CorruptRejectedByDecoder, n.CorruptReachedStep)
	// Coverage, not configuration: which paths this run actually reached.
	// "Zero violations" is only evidence about code that executed.
	fmt.Printf("    reported: ok=%d retryable-fail=%d terminal-fail=%d\n",
		c.ReportedOK, c.ReportedRetryable, c.ReportedTerminal)
	fmt.Printf("    exercised: redispatched=%d backed-off=%d reached-FAILED=%d sessions-expired=%d\n",
		c.TasksRedispatched, c.TasksBackedOff, c.TasksTerminalFail, c.SessionsExpired)
	fmt.Printf("    state:    jobs=%d tasks=%d\n", res.Jobs, res.Tasks)
	if l := res.Lin; l.Checked {
		// The verdict is printed with the history's shape next to it on
		// purpose. "Ok" over 12 operations is not the same evidence as "Ok"
		// over 900, and a history that is mostly ambiguous checked very
		// little regardless of its verdict.
		fmt.Printf("    linearizable: %s  (%d ops: %d submits, %d gets; checked in %s)\n",
			l.Verdict, l.Ops, l.Submits, l.Gets, l.Elapsed.Round(time.Millisecond))
		// The ambiguous SHARE, not just the count. An ambiguous operation
		// constrains nothing, so this is how much of the verdict is real.
		fmt.Printf("      ambiguous: %d of %d ops (%.0f%%) returned without telling the client whether they took effect\n",
			l.Ambiguous, l.Ops, l.AmbiguousShare()*100)
		// Where the operations landed. A verdict over a history that never
		// touched a cut-off node is a verdict about a healthy cluster.
		fmt.Printf("      reached:  %d ops on a partitioned node, %d while two nodes claimed leadership\n",
			l.Partitioned, l.SplitBrain)
		if l.Dropped > 0 {
			fmt.Printf("      NOTE: %d operations past the history cap were not recorded; "+
				"the verdict covers the recorded prefix only\n", l.Dropped)
		}
		if l.Rejected > 0 {
			fmt.Printf("      NOTE: %d submits were rejected as invalid; this workload only sends valid requests\n",
				l.Rejected)
		}
		if l.VizPath != "" {
			fmt.Printf("      timeline: %s\n", l.VizPath)
		}
	}
	return nil
}
