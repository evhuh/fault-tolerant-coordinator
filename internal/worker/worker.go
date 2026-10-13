// Package worker is the thing that actually runs tasks.
//
// Workers PULL. They dial the coordinator and long-poll for work; the
// coordinator never dials out. That removes address tracking, reconnection,
// and NAT handling, and it gives backpressure for free — a saturated worker
// simply does not ask for more. Heartbeats flow worker -> coordinator, which
// is also what failure detection reads.
package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"

	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

type Config struct {
	WorkerID string
	// HeartbeatInterval must divide the coordinator's lease TTL several times
	// over: the lease is deliberately sized at 4 missed heartbeats, so one
	// slow RPC never costs a healthy worker its tasks.
	HeartbeatInterval time.Duration
	// PollWait is how long a RequestTask call parks server-side before
	// returning empty.
	PollWait time.Duration
	// Concurrency is how many tasks this worker executes at once.
	Concurrency int
	DataDir     string
}

func DefaultConfig(workerID, dataDir string) Config {
	return Config{
		WorkerID:          workerID,
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          2 * time.Second,
		Concurrency:       1,
		DataDir:           dataDir,
	}
}

type Worker struct {
	cfg    Config
	client schedulerpb.SchedulerClient
	exec   *Executor

	// running tracks what this worker is currently executing, and is what the
	// heartbeat carries up as running_task_ids. That claim is what promotes
	// ASSIGNED -> RUNNING in the state machine; there is no separate ack RPC,
	// because RequestTask only delivers a decision already committed.
	mu      sync.Mutex
	running map[string]bool
}

func New(cfg Config, conn *grpc.ClientConn) *Worker {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	return &Worker{
		cfg:     cfg,
		client:  schedulerpb.NewSchedulerClient(conn),
		exec:    &Executor{DataDir: cfg.DataDir},
		running: make(map[string]bool),
	}
}

// Run registers, then runs the heartbeat loop and N poll loops until ctx ends.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.register(ctx); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.heartbeatLoop(ctx)
	}()
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.pollLoop(ctx)
		}()
	}
	wg.Wait()
	return nil
}

func (w *Worker) register(ctx context.Context) error {
	// Declaring capacity is what lets the planner assign proactively. Day 3
	// did not need it: workers pulled, so they self-limited by construction.
	_, err := w.client.RegisterWorker(ctx, &schedulerpb.RegisterWorkerRequest{
		WorkerId: w.cfg.WorkerID,
		Capacity: int32(w.cfg.Concurrency),
	})
	return err
}

// heartbeatLoop renews the lease and republishes the running claim.
//
// Renewal is re-registration: a worker whose lease lapsed (a long GC pause, a
// partition) is marked LOST and its tasks reclaimed, and the same call
// revives it. That is why RegisterWorker is create-OR-renew rather than
// create-only.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, err := w.client.Heartbeat(ctx, &schedulerpb.HeartbeatRequest{
				WorkerId:       w.cfg.WorkerID,
				RunningTaskIds: w.runningIDs(),
			})
			if err != nil && ctx.Err() == nil {
				log.Printf("worker %s: heartbeat: %v", w.cfg.WorkerID, err)
			}
		}
	}
}

func (w *Worker) runningIDs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := make([]string, 0, len(w.running))
	for id := range w.running {
		ids = append(ids, id)
	}
	return ids
}

func (w *Worker) pollLoop(ctx context.Context) {
	for ctx.Err() == nil {
		resp, err := w.client.RequestTask(ctx, &schedulerpb.RequestTaskRequest{
			WorkerId: w.cfg.WorkerID,
			WaitMs:   w.cfg.PollWait.Milliseconds(),
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// A coordinator restart or a lapsed lease both surface here.
			// Re-register and back off; there is nothing else to do.
			log.Printf("worker %s: request task: %v", w.cfg.WorkerID, err)
			time.Sleep(w.cfg.HeartbeatInterval)
			_ = w.register(ctx)
			continue
		}
		task := resp.GetTask()
		if task == nil {
			continue // nothing available within wait_ms; poll again
		}
		// RequestTask's first clause returns any task already assigned to
		// this WORKER, which is correct and necessary — a restarted worker
		// has to be handed its assignment back. But it means every poll
		// goroutine in this process sees the same task, so with concurrency
		// > 1 two of them would execute it side by side, into the same
		// result directory, keyed on the same (taskID, attempt).
		//
		// Dedup belongs here rather than in the coordinator: the coordinator
		// cannot tell one poller from another, and must keep re-offering the
		// task in case the worker restarted.
		if !w.claim(task.GetTaskId()) {
			// Another goroutine in this process already has it. Back off so
			// this does not spin: the poll returns the held task instantly.
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.cfg.HeartbeatInterval):
			}
			continue
		}
		w.execute(ctx, task)
	}
}

// claim reserves a task for this goroutine. It returns false if another
// goroutine in this process is already executing it, which is what keeps one
// worker from running the same (taskID, attempt) twice.
func (w *Worker) claim(taskID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running[taskID] {
		return false
	}
	w.running[taskID] = true
	return true
}

// execute runs a task the caller has already claimed, and releases the claim
// when it finishes.
func (w *Worker) execute(ctx context.Context, task *schedulerpb.Task) {
	defer func() {
		w.mu.Lock()
		delete(w.running, task.GetTaskId())
		w.mu.Unlock()
	}()

	spec := task.GetSpec()
	res := w.exec.Run(ctx, task.GetTaskId(), task.GetAttempt(),
		spec.GetCommand(), spec.GetArgs(),
		time.Duration(spec.GetTimeoutMs())*time.Millisecond)

	// The attempt number travels back unchanged. Together with the task id it
	// is the idempotency key: if this worker went slow, lost its lease, and
	// the task was already redispatched as attempt N+1, the state machine
	// sees an older attempt and ignores this report — silently and on every
	// replica. That is what turns at-least-once DELIVERY into effectively-once
	// EFFECTS.
	req := &schedulerpb.ReportResultRequest{
		WorkerId:  w.cfg.WorkerID,
		TaskId:    task.GetTaskId(),
		Attempt:   task.GetAttempt(),
		ExitCode:  res.ExitCode,
		ResultRef: res.ResultRef,
		Error:     res.Err,
		Retryable: res.Retryable,
	}

	// Report on a context that outlives cancellation: if we are shutting down
	// mid-task, a delivered result is strictly better than a lease timeout.
	// Delivery is at-least-once, so retrying here is safe by construction.
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := w.client.ReportResult(reportCtx, req); err != nil {
		// Dropping the report is survivable: the lease expires and the task
		// is redispatched. That is the failure mode the whole lease design
		// exists to absorb.
		log.Printf("worker %s: report %s attempt %d: %v",
			w.cfg.WorkerID, task.GetTaskId(), task.GetAttempt(), err)
	}
}
