// Command fsyncprobe measures this host's fsync latency and fails if it is
// too slow for the simulator's compressed timings to mean anything.
//
// WHY THIS GATES A RELEASE. Every timing judgement the harness makes — a
// Ready-loop stall, an election that should have happened, a lease that
// should have lapsed — is measured against a clock compressed to a 250ms
// election timeout and a 25ms tick. Disk I/O does not compress with it. When
// the host's fsync latency climbs, runs stop completing: Day 10 measured
// fsyncs of 1 to 13 MINUTES in `persist` on a machine running back-to-back
// race suites, and 20 of 25 reproducibility checkpoints skipped because every
// run logged only a prefix of its schedule.
//
// Those skips are the honest outcome for such a run, and they are also how a
// claim quietly stops being supported: `go test` exits 0 for a skip, and a
// stall that is INCONCLUSIVE because the disk broke the premise looks the same
// as no stall at all. So before a release asserts anything, the host has to be
// shown capable of supporting the premise. If this fails, the answer is a
// quieter machine, not a bigger threshold.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/hostio"
)

func main() {
	dir := flag.String("dir", os.TempDir(), "directory to probe; use the data dir the cluster will write to")
	n := flag.Int("n", 40, "number of write+fsync pairs to time")
	// The budgets come from the PREMISE, not from the best measurement seen.
	// Mean under half a 25ms tick, so a loop that fsyncs can still keep up;
	// worst under the 125ms stall threshold, so one slow sync cannot end a
	// run early.
	maxMean := flag.Duration("max-mean", 12*time.Millisecond, "fail if mean fsync exceeds this")
	maxWorst := flag.Duration("max-worst", 100*time.Millisecond, "fail if the worst fsync exceeds this")
	asJSON := flag.Bool("json", false, "emit fsync AND cpu probes as JSON, and never fail on the budget")
	flag.Parse()

	p, err := hostio.Fsync(*dir, *n)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fsyncprobe: %v\n", err)
		os.Exit(2)
	}

	// JSON MODE EXISTS FOR THE BENCHMARK HARNESS, which brackets every trial
	// with a probe so a quoted number carries host attribution.
	//
	// It reports rather than judges: a bracketing probe that exited non-zero
	// would abort the trial it was only meant to describe, and the decision
	// about whether a trial is usable belongs to whatever analyses the
	// trials. The gate's own behaviour, below, is unchanged.
	//
	// Day 11 needed this because the batching sweep had no probes: when one
	// run came in at 93.9s against 60.2s and 62.1s, its commit latency could
	// be reported but the slower disk behind it could not be attributed.
	if *asJSON {
		c := hostio.ProbeCPU()
		out := struct {
			FsyncMeanUs     int64   `json:"fsync_mean_us"`
			FsyncWorstUs    int64   `json:"fsync_worst_us"`
			FsyncN          int     `json:"fsync_n"`
			ComputeUs       int64   `json:"compute_us"`
			SchedLatencyUs  int64   `json:"sched_latency_us"`
			HeapMB          float64 `json:"heap_mb"`
			SysMB           float64 `json:"sys_mb"`
			NumGC           uint32  `json:"num_gc"`
			WithinBudget    bool    `json:"within_budget"`
			BudgetViolation string  `json:"budget_violation,omitempty"`
		}{
			FsyncMeanUs:    p.Mean.Microseconds(),
			FsyncWorstUs:   p.Worst.Microseconds(),
			FsyncN:         *n,
			ComputeUs:      c.Compute.Microseconds(),
			SchedLatencyUs: c.SchedLatency.Microseconds(),
			HeapMB:         c.HeapMB,
			SysMB:          c.SysMB,
			NumGC:          c.NumGC,
		}
		bad, why := p.Exceeds(*maxMean, *maxWorst)
		out.WithinBudget = !bad
		if bad {
			out.BudgetViolation = why
		}
		b, err := json.Marshal(out)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fsyncprobe: %v\n", err)
			os.Exit(2)
		}
		fmt.Println(string(b))
		return
	}

	fmt.Printf("%s (budget: mean %s, worst %s)\n", p, *maxMean, *maxWorst)

	if bad, why := p.Exceeds(*maxMean, *maxWorst); bad {
		fmt.Fprintf(os.Stderr, "FAILED: %s — this host cannot sustain the simulator's compressed "+
			"timings, so runs will truncate on host-io stalls and the checkpoints will skip "+
			"rather than check\n", why)
		fmt.Fprintln(os.Stderr, "Wait for the machine to go quiet and probe again. Raising the "+
			"budget does not make the measurement valid; it makes the claim unsupported.")
		os.Exit(1)
	}
}
