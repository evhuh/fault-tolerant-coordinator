package sim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/client"
	"github.com/evazheng/fault-tolerant-coordinator/internal/clock"
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	"github.com/evazheng/fault-tolerant-coordinator/internal/worker"
	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// The real worker, in process.
//
// Until now the simulator played the worker pool itself: it called
// RequestTask, Heartbeat and ReportResult on a *server.Service directly and
// decided each task's outcome by hashing. That drives the coordinator fine,
// and it is why the worker's own code — the executor's (taskID, attempt)
// result tree, the local claim dedup, heartbeat-as-re-registration — has been
// covered only by its unit tests since Day 8.
//
// Day 10's effect ledger is a claim about precisely that code: one committed
// effect per taskID, across attempts. Checking it against a simulated worker
// would check the simulator. So the real worker runs here, against the real
// client.Pool, over bufconn.
//
// OPT-IN (Config.RealWorkers). It costs a fork/exec per attempt, which is
// wall-clock time that no amount of clock compression removes — the same
// constraint the fsync floor imposes on TickInterval. The default sweeps keep
// the simulated pool.

type realWorker struct {
	id   string
	clk  *clock.Pausable
	pool *client.Pool
	dir  string
}

// realWorkerPool owns the in-process workers and their lifecycles.
type realWorkerPool struct {
	s       *Sim
	workers []*realWorker
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	// keepDirs is set when the caller named a root, and the result trees are
	// left behind for it to read.
	keepDirs bool
}

// startRealWorkers brings up WorkerCount real workers, each with its own
// Pool, its own pausable clock and its own result directory.
func (s *Sim) startRealWorkers(ctx context.Context) (*realWorkerPool, error) {
	seeds := make([]string, 0, s.cfg.Nodes)
	for i := 1; i <= s.cfg.Nodes; i++ {
		seeds = append(seeds, s.apiAddrs[uint64(i)])
	}

	ctx, cancel := context.WithCancel(ctx)
	p := &realWorkerPool{s: s, cancel: cancel, keepDirs: s.cfg.WorkerDir != ""}

	// THE PAUSE MUST NOT OUTLAST THE REPORT DEADLINE.
	//
	// A worker's ReportResult runs under a context deadline, which is
	// WALL-CLOCK: the injected clock compresses the heartbeat and poll loops,
	// but a context deadline does not compress with it. If a held report's
	// deadline expires during the pause, the call is cancelled on wake, the
	// late report never reaches the coordinator, and the stale-report
	// path — the one the effectively-once claim rests on — silently never
	// runs. The run would come back green about a case it never exercised.
	//
	// So the deadline is derived from the pause rather than inherited, and
	// the relationship is checked here rather than assumed. Four times the
	// pause: enough that a held report survives it with room for the wake
	// sequence, short enough that a worker is not parked for most of a run.
	pause := time.Duration(s.cfg.PauseSteps) * s.cfg.StepTime
	reportTimeout := 4 * pause
	if reportTimeout < time.Second {
		reportTimeout = time.Second
	}
	if reportTimeout <= pause {
		cancel()
		return nil, fmt.Errorf("worker report deadline %s does not outlast the pause %s: a held report would be cancelled on wake and the stale-report path could never run",
			reportTimeout, pause)
	}

	for i := 1; i <= s.cfg.WorkerCount; i++ {
		id := fmt.Sprintf("w-%d", i)
		dir := filepath.Join(s.cfg.WorkerDir, id)
		if s.cfg.WorkerDir == "" {
			var err error
			if dir, err = os.MkdirTemp("", "sim-worker-"+id+"-"); err != nil {
				cancel()
				return nil, fmt.Errorf("worker %s result dir: %w", id, err)
			}
		} else if err := os.MkdirAll(dir, 0o755); err != nil {
			cancel()
			return nil, fmt.Errorf("worker %s result dir: %w", id, err)
		}
		// One pausable clock per worker, so the pause fault can freeze a
		// worker's heartbeat and poll timers without touching any other
		// worker or any coordinator — a worker's own GC pause.
		clk := clock.NewPausable(nil)
		// Every seed, so the pool can find the leader from wherever it
		// starts, and the dialer is this worker's: a hold applies to the
		// process, not to one address.
		pool := client.NewPool(seeds, client.WithDialer(s.api.dialerFor(id)))

		rw := &realWorker{id: id, clk: clk, pool: pool, dir: dir}
		p.workers = append(p.workers, rw)

		cfg := worker.Config{
			WorkerID: id,
			// Four heartbeats to a lease, the ratio the production default
			// uses, against the simulator's compressed TTL.
			HeartbeatInterval: s.cfg.LeaseTTL / 4,
			// Short: a long park would hold the call across the faults this
			// run exists to inject, and the planner assigns proactively
			// anyway, so the first look usually succeeds.
			PollWait:      s.cfg.StepTime,
			Concurrency:   int(s.cfg.WorkerSlots),
			ReportTimeout: reportTimeout,
			DataDir:       dir,
			Clock:         clk,
			Logf:          s.workerLogf,
		}
		w := worker.New(cfg, pool)

		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.supervise(ctx, id, w)
		}()
	}
	return p, nil
}

// supervise restarts a worker whose Run returned.
//
// Run returns when registration fails, which mid-chaos is routine: there may
// be no leader at all for several steps. Without this the first leaderless
// moment would silently retire the worker pool and the rest of the run would
// drain nothing — the kind of harness failure that reads as a quiet, passing
// run.
func (p *realWorkerPool) supervise(ctx context.Context, id string, w *worker.Worker) {
	for ctx.Err() == nil {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			p.s.workerLogf("worker %s: run: %v", id, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.s.cfg.LeaseTTL / 4):
		}
	}
}

// pause freezes a worker's tickers and HOLDS its outbound RPCs.
//
// Both halves are needed and they do different things. The frozen clock stops
// the heartbeats, so the coordinator's lease expires and the task is
// reclaimed and redispatched as a later attempt. The held RPCs are what make
// the late report late rather than lost: whatever the worker had in flight
// leaves it after the release, by which time the coordinator has moved on.
// Dropping would be a partition, and a partition is not a GC pause.
func (p *realWorkerPool) pause(id string) {
	for _, w := range p.workers {
		if w.id == id {
			w.clk.Pause()
			p.s.api.hold(id)
			return
		}
	}
}

// resume wakes a worker. The clock first, then the queue: the worker should
// be running again as its held calls land, which is the ordering a
// descheduled process actually experiences.
func (p *realWorkerPool) resume(id string) {
	for _, w := range p.workers {
		if w.id == id {
			w.clk.Resume()
			p.s.api.release(id)
			return
		}
	}
}

func (p *realWorkerPool) stop() {
	p.cancel()
	// Release every hold before waiting: a worker blocked on a held write
	// would otherwise never notice the cancellation, and stop would hang for
	// the length of the RPC's own timeout.
	for _, w := range p.workers {
		p.s.api.release(w.id)
	}
	p.wg.Wait()
	for _, w := range p.workers {
		w.pool.Close()
		if !p.keepDirs {
			_ = os.RemoveAll(w.dir)
		}
	}
}

// workerLogf routes the workers' transient-failure notices into the run's own
// output, where -v shows them and a quiet run does not.
// It goes to WorkerOut rather than the standard logger or Out: on stderr
// these bury the finding, and in Out they would break the byte-for-byte
// schedule comparison the Day 8 checkpoint makes.
func (s *Sim) workerLogf(format string, args ...any) {
	if s.cfg.WorkerOut == nil {
		return
	}
	fmt.Fprintf(s.cfg.WorkerOut, format+"\n", args...)
}

// observeReport keeps the failure-path coverage counters honest once the
// simulator has stopped deciding outcomes.
//
// With the simulated pool these were counted where the outcome was chosen. A
// real worker chooses for itself, so they are counted where the report
// ARRIVES — and only when the coordinator accepted it, because a report that
// failed mid-partition exercised nothing on the state machine.
func (s *Sim) observeReport(req *schedulerpb.ReportResultRequest, err error) {
	// Logged whether or not it was accepted. A rejected report is the
	// interesting one — it is where a late attempt's effect is supposed to be
	// discarded — and the ledger checker will assert on exactly this
	// sequence, so being able to read it off a verbose run is worth the line.
	s.workerLogf("REPORT %s attempt=%d exit=%d retryable=%v err=%q rpc=%v",
		req.GetTaskId(), req.GetAttempt(), req.GetExitCode(), req.GetRetryable(), req.GetError(), err)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case req.GetExitCode() == 0:
		s.result.Coverage.ReportedOK++
	case req.GetRetryable():
		s.result.Coverage.ReportedRetryable++
	default:
		s.result.Coverage.ReportedTerminal++
	}
}

// observeApply counts the state machine's SILENT no-ops.
//
// Dedup by (taskID, attempt) because every replica applies the entry and
// computes the identical Result — three nodes would otherwise report one late
// report as three. The dedup is also why this is keyed on the report's own
// identity rather than counted per call.
func (s *Sim) observeApply(cmd *commandpb.Command, res state.Result) {
	if !res.StaleReport && !res.DuplicateReport {
		// Accepted. Only the ledger wants these, and only a real-worker run
		// has a ledger.
		if s.cfg.RealWorkers {
			s.recordAccepted(cmd, res)
		}
		return
	}
	taskID, attempt := reportedTask(cmd)
	if taskID == "" {
		return
	}
	key := fmt.Sprintf("%s/%d", taskID, attempt)

	s.mu.Lock()
	defer s.mu.Unlock()
	if res.StaleReport {
		if s.seenStaleReport[key] {
			return
		}
		s.seenStaleReport[key] = true
		s.result.Coverage.StaleReportsRejected++
		// Kept per task as well as counted, so the ledger can say WHICH
		// attempt was refused rather than only how many were.
		if s.ledger.StaleByTask == nil {
			s.ledger.StaleByTask = map[string][]int32{}
		}
		s.ledger.StaleByTask[taskID] = append(s.ledger.StaleByTask[taskID], attempt)
		s.workerLogf("  STALE REJECTED %s attempt %d", taskID, attempt)
		return
	}
	if s.seenDuplicateReport[key] {
		return
	}
	s.seenDuplicateReport[key] = true
	s.result.Coverage.DuplicateReportsIgnored++
}

// reportedTask pulls the (taskID, attempt) out of whichever report this is.
func reportedTask(cmd *commandpb.Command) (string, int32) {
	if c := cmd.GetCompleteTask(); c != nil {
		return c.GetTaskId(), c.GetAttempt()
	}
	if f := cmd.GetFailTask(); f != nil {
		return f.GetTaskId(), f.GetAttempt()
	}
	return "", 0
}

// WorkerPauseDiag records the worker pause as it was REALIZED, for the same
// reason the leader pause has one: "no overlap" is not a diagnosis, and each
// way this sequence can fall through has to be visible.
type WorkerPauseDiag struct {
	// Fired is false when no worker was executing anything at the scheduled
	// step, or at any step after it, so the pause never opened.
	Fired bool
	Step  int
	// Worker is who was frozen, and Task what they were executing when the
	// freeze landed — the thing that should then be reclaimed under them.
	Worker string
	Task   string
	// Attempt is the attempt the frozen worker was running.
	Attempt int32
	// Resumed is the step the worker was released on.
	Resumed int
	// Reclaimed is whether the task was redispatched to a LATER attempt while
	// the worker was frozen. False means the lease never lapsed, and no
	// overlap was possible.
	Reclaimed bool
}

// injectWorkerPause freezes a worker that is currently executing, and wakes
// it once its lease has had time to lapse and the task to be redispatched.
//
// It AIMS rather than waits: pausing a worker that happens to be idle freezes
// nothing interesting, so the victim is chosen from committed state — a task
// in ASSIGNED or RUNNING with a worker holding it — and the injection defers
// to a later step when there is none.
func (s *Sim) injectWorkerPause(step int) {
	if !s.cfg.RealWorkers || !s.cfg.PauseWorker || s.rw == nil {
		return
	}
	if s.workerPaused != "" {
		if step >= s.workerResumeAt {
			// The clock first, then the held queue, the same ordering the
			// leader pause uses: the process is running again as its own
			// calls land.
			s.rw.resume(s.workerPaused)
			s.result.WorkerPause.Resumed = step
			s.result.WorkerPause.Reclaimed = s.attemptOf(s.result.WorkerPause.Task) > s.result.WorkerPause.Attempt
			// workerLogf, NOT logf: this goes to WorkerOut rather than into
			// the schedule log. See the note on the injection below.
			s.workerLogf("step %d: WAKE worker %s — its held report lands now, naming attempt %d",
				step, s.workerPaused, s.result.WorkerPause.Attempt)
			s.workerPaused = ""
			s.workerResumeAt = 0
		}
		return
	}
	if s.workerPauseStep == 0 || step < s.workerPauseStep || s.result.WorkerPause.Fired {
		return
	}

	victim, taskID, attempt := s.busyWorker()
	if victim == "" {
		return // nobody is executing; try again next step
	}
	s.rw.pause(victim)
	s.workerPaused = victim
	// Long enough for the lease to lapse and the planner to redispatch. The
	// lease is sized at four missed heartbeats, so one TTL is the floor and
	// two gives the reclaim somewhere to happen.
	s.workerResumeAt = step + stepsFor(2*s.cfg.LeaseTTL, s.cfg.StepTime) + 1
	s.result.WorkerPause = WorkerPauseDiag{
		Fired: true, Step: step, Worker: victim, Task: taskID, Attempt: attempt,
	}
	// KEPT OUT OF THE SCHEDULE LOG, deliberately, and this is the second
	// time this lesson has been paid for.
	//
	// The injection is REACTIVE: it looks for a worker that is executing
	// right now and defers when there is none, so the step it lands on and
	// the victim it picks depend on timing rather than on the seed. Logging
	// that into Out made two runs of seed 1234 print different schedules and
	// turned the real-worker reproducibility test red — exactly what
	// DriveWindow did, which is why DriveWindow is off by default.
	//
	// So what the seed reproduces here is the pause's INTENT, and the
	// realization is reported in Result.WorkerPause instead, where a failing
	// run can be read and replayed from it.
	s.workerLogf("step %d: PAUSE worker %s mid-execution of %s attempt %d (resume at step %d)",
		step, victim, taskID, attempt, s.workerResumeAt)
}

// busyWorker finds a worker that is executing a task right now, and what it
// is executing.
func (s *Sim) busyWorker() (workerID, taskID string, attempt int32) {
	n := s.leader()
	if n == nil {
		return "", "", 0
	}
	n.coord.ReadState(func(st *state.StateStore) {
		// Sorted ids, never a map range: a choice made over Go's map
		// iteration order is a choice the seed does not determine.
		for _, id := range st.TaskIDs() {
			t, _ := st.Task(id)
			if t.WorkerID == "" {
				continue
			}
			if t.State != state.TaskRunning && t.State != state.TaskAssigned {
				continue
			}
			workerID, taskID, attempt = t.WorkerID, id, t.Attempt
			return
		}
	})
	return workerID, taskID, attempt
}

// attemptOf reads a task's current attempt from committed state.
func (s *Sim) attemptOf(taskID string) int32 {
	n := s.anyNode()
	if n == nil || taskID == "" {
		return 0
	}
	var attempt int32
	n.coord.ReadState(func(st *state.StateStore) {
		if t, ok := st.Task(taskID); ok {
			attempt = t.Attempt
		}
	})
	return attempt
}

// stepsFor converts a duration into whole fault steps, rounding up.
func stepsFor(d, step time.Duration) int {
	if step <= 0 {
		return 1
	}
	n := int((d + step - 1) / step)
	if n < 1 {
		return 1
	}
	return n
}
