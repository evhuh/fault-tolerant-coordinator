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
	flag.Parse()

	p, err := hostio.Fsync(*dir, *n)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fsyncprobe: %v\n", err)
		os.Exit(2)
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
