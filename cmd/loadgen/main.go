// Command loadgen offers a FIXED RATE of job submissions to a real
// coordinator cluster and reports what it cost.
//
// OPEN LOOP, which is the only reason this program is worth running. A
// closed-loop generator — submit, wait for the response, submit again —
// cannot offer more load than the system is currently completing, so it
// measures the client and the server together and hides queueing entirely.
// Here a ticker decides when a request SHOULD go out, a pool of submitters
// sends it, and nothing waits for a response before the next one is due.
//
// COORDINATED OMISSION is handled by recording the INTENDED send time rather
// than the actual one. Latency is completedAt - intendedAt, computed from the
// schedule (start + n/rate) and never from time.Now(). The difference is the
// whole measurement: when the cluster stalls for a second at 500/s, the 500
// requests that should have gone out during the stall carry their full
// queueing delay instead of quietly not existing. A generator that timestamps
// at send time reports that stall as "a few slow requests" and publishes a
// p99 that is off by orders of magnitude.
//
// It also REFUSES to publish a number it cannot stand behind. If the
// generator itself falls behind the offered rate, the run aborts rather than
// silently becoming closed-loop — see -max-backlog. The same discipline as
// the fsync gate: a harness that cannot meet its own premise says so instead
// of producing a green result.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"

	"github.com/evazheng/fault-tolerant-coordinator/internal/client"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

func main() {
	addrs := flag.String("addrs", "localhost:8080", "comma-separated coordinator gRPC addresses")
	rate := flag.Float64("rate", 100, "target submissions per second, offered regardless of completion")
	duration := flag.Duration("duration", 30*time.Second, "measured duration, after warmup")
	warmup := flag.Duration("warmup", 10*time.Second, "discarded leading period")
	tasksPerJob := flag.Int("tasks-per-job", 1, "tasks in each submitted job")
	submitters := flag.Int("submitters", 64, "concurrent submitter goroutines")
	command := flag.String("command", "", "task command; default resolves true(1) on this host")
	taskArgs := flag.String("task-args", "", "comma-separated args for the task command, e.g. 30 for sleep")
	taskTimeout := flag.Duration("task-timeout", 5*time.Second, "per-task timeout the coordinator enforces")
	timeout := flag.Duration("timeout", 5*time.Second, "per-request deadline")
	maxBacklog := flag.Float64("max-backlog", 2, "abort if undispatched slots exceed this many seconds of offered rate")
	out := flag.String("out", "", "write the result as JSON to this path")
	label := flag.String("label", "", "a name for this run, recorded in the result")
	prefill := flag.Int("prefill", 0, "submit this many jobs as fast as possible, then exit; no rate, no measurement")
	flag.Parse()

	if *rate <= 0 {
		log.Fatal("-rate must be positive")
	}
	// THE COMMAND IS RESOLVED BEFORE ANY LOAD IS OFFERED. /bin/true does not
	// exist on every host — it does not exist on this one — and a benchmark
	// whose tasks all fail to exec is a green run that measured nothing but
	// the dispatch path.
	cmdPath := *command
	if cmdPath == "" {
		p, err := exec.LookPath("true")
		if err != nil {
			log.Fatalf("cannot resolve true(1) on this host; pass -command: %v", err)
		}
		cmdPath = p
	}
	if _, err := exec.LookPath(cmdPath); err != nil {
		log.Fatalf("-command %q is not executable here: %v", cmdPath, err)
	}

	seeds := splitAddrs(*addrs)
	if len(seeds) == 0 {
		log.Fatal("-addrs named no coordinator")
	}
	pool := client.NewPool(seeds)
	defer pool.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// PREFILL is a separate mode, not a rate. The plan-batching experiment
	// needs a large backlog of PENDING tasks waiting when a large pool of
	// idle worker slots appears, so that a plan has many assignable tasks and
	// many free slots at the same moment. Offering load at a rate cannot
	// produce that: with workers already running, slots free up one or two at
	// a time and every plan carries one assignment — measured, 2782 plans of
	// size exactly 1.0 against 7094 pending tasks.
	//
	// So: submit the backlog with NO workers connected, then start the pool.
	if *prefill > 0 {
		n := prefillJobs(ctx, pool, cmdPath, splitAddrs(*taskArgs), *taskTimeout,
			*prefill, *tasksPerJob, *submitters, *timeout)
		log.Printf("prefill: %d of %d jobs submitted", n, *prefill)
		if n < *prefill {
			os.Exit(1)
		}
		return
	}

	r := &run{
		pool:           pool,
		rate:           *rate,
		warmup:         *warmup,
		duration:       *duration,
		tasksPerJob:    *tasksPerJob,
		command:        cmdPath,
		args:           splitAddrs(*taskArgs),
		taskTimeout:    *taskTimeout,
		timeout:        *timeout,
		maxBacklog:     int64(*maxBacklog * *rate),
		label:          *label,
		submitterCount: *submitters,
		seeds:          seeds,
		// 1µs to 60s at 3 significant figures. The floor is below anything
		// this system can achieve and the ceiling is past the request
		// deadline, so nothing lands outside the recordable range — a value
		// hdrhistogram would otherwise reject, losing exactly the
		// observations that matter.
		latency: hdr.New(1, 60_000_000, 3),
		queued:  hdr.New(1, 60_000_000, 3),
	}
	res := r.execute(ctx)

	report(res)
	if *out != "" {
		b, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			log.Fatalf("marshal: %v", err)
		}
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			log.Fatalf("write %s: %v", *out, err)
		}
		log.Printf("result written to %s", *out)
	}
	if res.Aborted != "" {
		// NON-ZERO EXIT on an aborted run, so a script that sweeps configs
		// cannot collect a number the generator disowned.
		os.Exit(1)
	}
}

type run struct {
	pool           *client.Pool
	rate           float64
	warmup         time.Duration
	duration       time.Duration
	tasksPerJob    int
	command        string
	args           []string
	taskTimeout    time.Duration
	timeout        time.Duration
	maxBacklog     int64
	label          string
	submitterCount int
	seeds          []string

	mu      sync.Mutex
	latency *hdr.Histogram // intended send -> response, the honest latency
	queued  *hdr.Histogram // intended send -> actual send, the generator's own lag

	// MEASURED-WINDOW COUNTERS ONLY. The warmup's submissions are discarded
	// from these as well as from the histograms: dividing a total that
	// includes the warmup by the measured duration reports a rate higher
	// than the one that was offered, which is how a 200/s run first
	// announced 250/s here.
	submitted atomic.Int64
	ok        atomic.Int64
	failed    atomic.Int64
	notLeader atomic.Int64
	// Warmup totals, kept so the report can say what was discarded rather
	// than leaving a reader to wonder where the requests went.
	warmSubmitted atomic.Int64
	warmOK        atomic.Int64
	backlog       atomic.Int64
	worstLag      atomic.Int64
}

// Result is what the run reports, and it carries its own disclosure. A
// throughput figure without the hardware, the Go version, the cluster size
// and the fsync setting is not reproducible, so they travel together rather
// than relying on someone to paste them into a README later.
type Result struct {
	Label      string `json:"label,omitempty"`
	StartedAt  string `json:"started_at"`
	Disclosure struct {
		CPU         string   `json:"cpu"`
		Cores       int      `json:"cores"`
		GoVersion   string   `json:"go_version"`
		OS          string   `json:"os_arch"`
		Addrs       []string `json:"coordinator_addrs"`
		ClusterSize int      `json:"cluster_size"`
		// FSYNC IS UNCONDITIONAL in this build: the WAL syncs once per batch
		// and every snapshot calls File.Sync, with no unsafe mode to turn
		// off. Stated rather than left implicit, because a throughput number
		// from a no-fsync build is a different measurement and the two are
		// routinely confused.
		Fsync string `json:"fsync"`
		// TRANSPORT, stated because the brief and the code disagree about
		// it: load is offered over gRPC. HTTP is the raft PEER transport
		// between coordinators, not the client API.
		Transport string `json:"transport"`
		Harness   string `json:"harness"`
	} `json:"disclosure"`

	TargetRate  float64 `json:"target_rate"`
	WarmupMs    int64   `json:"warmup_ms"`
	DurationMs  int64   `json:"duration_ms"`
	TasksPerJob int     `json:"tasks_per_job"`

	// Submitted, OK and Failed cover the MEASURED window only.
	Submitted int64 `json:"submitted"`
	OK        int64 `json:"ok"`
	Failed    int64 `json:"failed"`
	NotLeader int64 `json:"not_leader"`
	// Discarded by the warmup, reported so the totals reconcile.
	WarmupSubmitted int64 `json:"warmup_submitted"`
	WarmupOK        int64 `json:"warmup_ok"`

	AchievedRate float64 `json:"achieved_rate"`
	// Latency percentiles in milliseconds, measured from the INTENDED send
	// time. The p50/p99 gap is the queueing the open loop exists to expose.
	P50  float64 `json:"p50_ms"`
	P90  float64 `json:"p90_ms"`
	P99  float64 `json:"p99_ms"`
	P999 float64 `json:"p999_ms"`
	Max  float64 `json:"max_ms"`
	// GeneratorLagP99Ms is intended-send to actual-send: the generator's own
	// contribution. If this is not far below the latency percentiles, the
	// numbers describe this program rather than the cluster.
	GeneratorLagP99Ms float64 `json:"generator_lag_p99_ms"`
	GeneratorLagMaxMs float64 `json:"generator_lag_max_ms"`
	WorstBacklog      int64   `json:"worst_backlog_slots"`

	// Aborted is empty on a run whose numbers are usable, and otherwise says
	// why they are not.
	Aborted string `json:"aborted,omitempty"`
}

func (r *run) execute(ctx context.Context) *Result {
	res := &Result{
		Label:       r.label,
		StartedAt:   time.Now().Format(time.RFC3339),
		TargetRate:  r.rate,
		WarmupMs:    r.warmup.Milliseconds(),
		DurationMs:  r.duration.Milliseconds(),
		TasksPerJob: r.tasksPerJob,
	}
	res.Disclosure.CPU = cpuName()
	res.Disclosure.Cores = runtime.NumCPU()
	res.Disclosure.GoVersion = runtime.Version()
	res.Disclosure.OS = runtime.GOOS + "/" + runtime.GOARCH
	res.Disclosure.Addrs = r.seeds
	res.Disclosure.ClusterSize = len(res.Disclosure.Addrs)
	res.Disclosure.Fsync = "always; WAL synced per batch and every snapshot File.Sync'd, no unsafe mode in this build"
	res.Disclosure.Transport = "client load over gRPC; raft peer traffic over HTTP"
	res.Disclosure.Harness = "real cmd/coordinator cluster, no simulator and no simnet in the path"

	// ONE SCHEDULE for warmup and measurement together, so the measured
	// window begins against an already-loaded cluster. Restarting the clock
	// after warmup would hand the first measured request an idle system.
	interval := time.Duration(float64(time.Second) / r.rate)
	start := time.Now()
	measureFrom := start.Add(r.warmup)
	deadline := measureFrom.Add(r.duration)

	slots := make(chan slot, int(r.rate)+1024)
	var wg sync.WaitGroup
	for i := 0; i < r.submitterCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r.submitLoop(ctx, id, slots, measureFrom)
		}(i)
	}

	aborted := r.dispatch(ctx, start, interval, deadline, slots)
	close(slots)
	wg.Wait()

	elapsed := time.Since(measureFrom)
	if elapsed > r.duration {
		elapsed = r.duration
	}
	res.Submitted = r.submitted.Load()
	res.OK = r.ok.Load()
	res.Failed = r.failed.Load()
	res.NotLeader = r.notLeader.Load()
	res.WarmupSubmitted = r.warmSubmitted.Load()
	res.WarmupOK = r.warmOK.Load()
	if elapsed > 0 {
		res.AchievedRate = float64(res.OK) / elapsed.Seconds()
	}
	res.WorstBacklog = r.worstLag.Load()
	res.Aborted = aborted

	r.mu.Lock()
	defer r.mu.Unlock()
	res.P50 = usToMs(r.latency.ValueAtQuantile(50))
	res.P90 = usToMs(r.latency.ValueAtQuantile(90))
	res.P99 = usToMs(r.latency.ValueAtQuantile(99))
	res.P999 = usToMs(r.latency.ValueAtQuantile(99.9))
	res.Max = usToMs(r.latency.Max())
	res.GeneratorLagP99Ms = usToMs(r.queued.ValueAtQuantile(99))
	res.GeneratorLagMaxMs = usToMs(r.queued.Max())
	return res
}

type slot struct {
	seq        int64
	intendedAt time.Time
	measured   bool
}

// dispatch walks the schedule and hands each slot to a submitter. It never
// waits for a response; its only job is to be on time.
func (r *run) dispatch(ctx context.Context, start time.Time, interval time.Duration,
	deadline time.Time, slots chan<- slot) string {

	var seq int64
	for {
		intended := start.Add(time.Duration(seq) * interval)
		if intended.After(deadline) {
			return ""
		}
		if d := time.Until(intended); d > 0 {
			select {
			case <-ctx.Done():
				return "interrupted"
			case <-time.After(d):
			}
		}
		select {
		case <-ctx.Done():
			return "interrupted"
		default:
		}

		s := slot{seq: seq, intendedAt: intended, measured: !intended.Before(start.Add(r.warmup))}
		select {
		case slots <- s:
			n := r.backlog.Add(1)
			if n > r.worstLag.Load() {
				r.worstLag.Store(n)
			}
			if n > r.maxBacklog {
				// THE REFUSAL. Past this point the generator is no longer
				// offering the configured rate, so whatever it reports is a
				// property of this program.
				return fmt.Sprintf("generator could not sustain %.0f/s: %d slots undispatched "+
					"(limit %d); the offered rate was not actually offered, so these numbers "+
					"describe the load generator", r.rate, n, r.maxBacklog)
			}
		case <-ctx.Done():
			return "interrupted"
		}
		seq++
	}
}

func (r *run) submitLoop(ctx context.Context, id int, slots <-chan slot, measureFrom time.Time) {
	clientID := fmt.Sprintf("loadgen-%d", id)
	for s := range slots {
		sentAt := time.Now()
		r.backlog.Add(-1)
		if s.measured {
			r.submitted.Add(1)
		} else {
			r.warmSubmitted.Add(1)
		}

		req := &schedulerpb.SubmitJobRequest{
			ClientId: clientID,
			// DETERMINISTIC request id, so a retry — Pool.Do retries on
			// redirect — dedups server-side instead of inflating throughput.
			RequestId: fmt.Sprintf("%s-%d", clientID, s.seq),
			Tasks:     r.taskSpecs(),
		}
		var redirected bool
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.timeout)
		err := r.pool.Do(func(c schedulerpb.SchedulerClient) error {
			_, err := c.SubmitJob(rctx, req)
			if _, isNL := client.IsNotLeader(err); isNL {
				redirected = true
			}
			return err
		})
		cancel()
		done := time.Now()

		if !s.measured {
			// Warmup: the cluster still did the work, and it still matters
			// that it succeeded, but none of it reaches the reported rate.
			if err == nil {
				r.warmOK.Add(1)
			}
			continue
		}
		if redirected {
			// Counted even when the retry then succeeded: a redirect is real
			// work the cluster did, and a run with many of them was measured
			// across a failover rather than in steady state.
			r.notLeader.Add(1)
		}
		if err != nil {
			r.failed.Add(1)
			continue
		}
		r.ok.Add(1)
		// FROM THE INTENDED TIME, not from sentAt. This is the line that
		// makes the p99 honest.
		r.observe(done.Sub(s.intendedAt), sentAt.Sub(s.intendedAt))
	}
}

func (r *run) observe(total, generatorLag time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = r.latency.RecordValue(total.Microseconds())
	if generatorLag < 0 {
		generatorLag = 0
	}
	_ = r.queued.RecordValue(generatorLag.Microseconds())
}

func (r *run) taskSpecs() []*schedulerpb.TaskSpec {
	specs := make([]*schedulerpb.TaskSpec, r.tasksPerJob)
	for i := range specs {
		specs[i] = &schedulerpb.TaskSpec{
			Command:     r.command,
			Args:        r.args,
			TimeoutMs:   r.taskTimeout.Milliseconds(),
			MaxAttempts: 3,
		}
	}
	return specs
}

func report(res *Result) {
	fmt.Printf("\n=== loadgen %s ===\n", res.Label)
	fmt.Printf("harness:    %s\n", res.Disclosure.Harness)
	fmt.Printf("transport:  %s\n", res.Disclosure.Transport)
	fmt.Printf("host:       %s, %d cores, %s, %s\n",
		res.Disclosure.CPU, res.Disclosure.Cores, res.Disclosure.GoVersion, res.Disclosure.OS)
	fmt.Printf("cluster:    %d node(s) %v\n", res.Disclosure.ClusterSize, res.Disclosure.Addrs)
	fmt.Printf("fsync:      %s\n", res.Disclosure.Fsync)
	fmt.Printf("offered:    %.0f/s for %s after %s discarded warmup, %d task(s) per job\n",
		res.TargetRate, time.Duration(res.DurationMs)*time.Millisecond,
		time.Duration(res.WarmupMs)*time.Millisecond, res.TasksPerJob)
	fmt.Printf("submitted:  %d measured (ok %d, failed %d, not-leader %d); %d discarded as warmup\n",
		res.Submitted, res.OK, res.Failed, res.NotLeader, res.WarmupSubmitted)
	fmt.Printf("achieved:   %.1f/s accepted over the measured window\n", res.AchievedRate)
	// A SHORTFALL IS REPORTED, not left to be noticed. Achieving materially
	// less than the offered rate means the cluster could not keep up, which
	// is a result; achieving materially MORE means the accounting is wrong,
	// which is a bug.
	if d := res.AchievedRate - res.TargetRate; d > res.TargetRate*0.02 {
		fmt.Printf("            NOTE: that is ABOVE the offered rate, which is an accounting bug, not a result\n")
	} else if -d > res.TargetRate*0.05 {
		fmt.Printf("            NOTE: %.0f%% below the offered rate — the cluster did not keep up\n",
			-d/res.TargetRate*100)
	}
	fmt.Printf("latency:    p50 %.1fms  p90 %.1fms  p99 %.1fms  p99.9 %.1fms  max %.1fms  (from INTENDED send)\n",
		res.P50, res.P90, res.P99, res.P999, res.Max)
	fmt.Printf("generator:  lag p99 %.1fms, max %.1fms, worst backlog %d slots\n",
		res.GeneratorLagP99Ms, res.GeneratorLagMaxMs, res.WorstBacklog)
	if res.Aborted != "" {
		fmt.Printf("\nABORTED — THESE NUMBERS ARE NOT USABLE: %s\n", res.Aborted)
		return
	}
	// The generator's own lag is reported next to the latency it claims to
	// measure, because the comparison is the only thing that says whether
	// the measurement is about the cluster.
	if res.GeneratorLagP99Ms*4 > res.P99 {
		fmt.Printf("\nWARNING: generator lag p99 (%.1fms) is within 4x of request p99 (%.1fms); "+
			"this run is measuring the generator as much as the cluster\n",
			res.GeneratorLagP99Ms, res.P99)
	}
}

func usToMs(us int64) float64 { return float64(us) / 1000 }

func cpuName() string {
	out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
	if err != nil {
		return runtime.GOARCH
	}
	return string(trimNewline(out))
}

func trimNewline(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

func splitAddrs(s string) []string {
	var out []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(c)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

// prefillJobs submits as fast as the cluster will accept, with no schedule and
// no latency measurement: the only output that matters is how many landed.
func prefillJobs(ctx context.Context, pool *client.Pool, command string, args []string,
	taskTimeout time.Duration, jobs, tasksPerJob, submitters int, timeout time.Duration) int {

	var ok atomic.Int64
	work := make(chan int, submitters*2)
	var wg sync.WaitGroup
	for i := 0; i < submitters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clientID := fmt.Sprintf("prefill-%d", id)
			specs := make([]*schedulerpb.TaskSpec, tasksPerJob)
			for j := range specs {
				specs[j] = &schedulerpb.TaskSpec{Command: command, Args: args,
					TimeoutMs: taskTimeout.Milliseconds(), MaxAttempts: 3}
			}
			for seq := range work {
				rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
				err := pool.Do(func(c schedulerpb.SchedulerClient) error {
					_, err := c.SubmitJob(rctx, &schedulerpb.SubmitJobRequest{
						ClientId:  clientID,
						RequestId: fmt.Sprintf("%s-%d", clientID, seq),
						Tasks:     specs,
					})
					return err
				})
				cancel()
				if err == nil {
					ok.Add(1)
				}
			}
		}(i)
	}
	for i := 0; i < jobs; i++ {
		select {
		case work <- i:
		case <-ctx.Done():
			close(work)
			wg.Wait()
			return int(ok.Load())
		}
	}
	close(work)
	wg.Wait()
	return int(ok.Load())
}
