// Command schedctl is the client CLI.
//
//	schedctl submit -n 100 -- /bin/sh -c 'echo hi'
//	schedctl status <jobID>
//	schedctl list
//
// It has submit and status; `watch` was never added, since polling status
// demonstrates the same thing.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/client"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  schedctl [-coordinator ADDR] submit [-n N] [-timeout D] [-max-attempts N] -- COMMAND [ARGS...]
  schedctl [-coordinator ADDR] status JOBID
  schedctl [-coordinator ADDR] list
`)
	os.Exit(2)
}

func main() {
	addr := flag.String("coordinator", "localhost:8080", "comma-separated coordinator addresses; any one will do, NOT_LEADER redirects find the rest")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() < 1 {
		usage()
	}

	pool := client.NewPool(strings.Split(*addr, ","))
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch cmd := flag.Arg(0); cmd {
	case "submit":
		submit(ctx, pool, flag.Args()[1:])
	case "status":
		if flag.NArg() != 2 {
			usage()
		}
		status(ctx, pool, flag.Arg(1))
	case "list":
		list(ctx, pool)
	default:
		fatalf("unknown command %q", cmd)
	}
}

func submit(ctx context.Context, pool *client.Pool, args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	n := fs.Int("n", 1, "number of identical tasks to fan out")
	timeout := fs.Duration("timeout", 30*time.Second, "per-task timeout")
	maxAttempts := fs.Int("max-attempts", 3, "attempts before a task is terminally FAILED")
	requestID := fs.String("request-id", "", "idempotency key; a retry with the same value returns the original job")
	clientID := fs.String("client-id", "schedctl", "client identity for dedup")
	_ = fs.Parse(args)

	argv := fs.Args()
	if len(argv) == 0 {
		fatalf("submit: no command given (did you forget `--`?)")
	}

	// A request id makes the submission idempotent: retrying it returns the
	// ORIGINAL job rather than creating a second one. Defaulting it to a
	// timestamp keeps distinct invocations distinct.
	if *requestID == "" {
		*requestID = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	specs := make([]*schedulerpb.TaskSpec, 0, *n)
	for i := 0; i < *n; i++ {
		specs = append(specs, &schedulerpb.TaskSpec{
			Command:     argv[0],
			Args:        argv[1:],
			TimeoutMs:   timeout.Milliseconds(),
			MaxAttempts: int32(*maxAttempts),
		})
	}

	var resp *schedulerpb.SubmitJobResponse
	err := pool.Do(func(c schedulerpb.SchedulerClient) error {
		var e error
		resp, e = c.SubmitJob(ctx, &schedulerpb.SubmitJobRequest{
			ClientId:  *clientID,
			RequestId: *requestID,
			Tasks:     specs,
		})
		return e
	})
	if err != nil {
		fatalf("submit: %v", err)
	}
	fmt.Println(resp.GetJobId())
}

func status(ctx context.Context, pool *client.Pool, jobID string) {
	var resp *schedulerpb.GetJobResponse
	err := pool.Do(func(c schedulerpb.SchedulerClient) error {
		var e error
		resp, e = c.GetJob(ctx, &schedulerpb.GetJobRequest{JobId: jobID})
		return e
	})
	if err != nil {
		fatalf("status: %v", err)
	}
	printJob(resp.GetJob(), true)
}

func list(ctx context.Context, pool *client.Pool) {
	var resp *schedulerpb.ListJobsResponse
	err := pool.Do(func(c schedulerpb.SchedulerClient) error {
		var e error
		resp, e = c.ListJobs(ctx, &schedulerpb.ListJobsRequest{})
		return e
	})
	if err != nil {
		fatalf("list: %v", err)
	}
	for _, j := range resp.GetJobs() {
		printJob(j, false)
	}
}

func printJob(j *schedulerpb.Job, verbose bool) {
	counts := map[schedulerpb.TaskState]int{}
	for _, t := range j.GetTasks() {
		counts[t.GetState()]++
	}
	fmt.Printf("%s  %s  tasks=%d  pending=%d assigned=%d running=%d complete=%d failed=%d\n",
		j.GetJobId(), short(j.GetState().String()), len(j.GetTasks()),
		counts[schedulerpb.TaskState_TASK_STATE_PENDING],
		counts[schedulerpb.TaskState_TASK_STATE_ASSIGNED],
		counts[schedulerpb.TaskState_TASK_STATE_RUNNING],
		counts[schedulerpb.TaskState_TASK_STATE_COMPLETE],
		counts[schedulerpb.TaskState_TASK_STATE_FAILED])

	if !verbose {
		return
	}
	for _, t := range j.GetTasks() {
		detail := t.GetResultRef()
		if t.GetLastError() != "" {
			detail = t.GetLastError()
		}
		fmt.Printf("  %-28s %-9s attempt=%d worker=%-10s exit=%d %s\n",
			t.GetTaskId(), short(t.GetState().String()), t.GetAttempt(),
			orDash(t.GetWorkerId()), t.GetExitCode(), detail)
	}
}

// short trims the proto enum prefix: TASK_STATE_RUNNING -> RUNNING.
func short(s string) string {
	for _, p := range []string{"TASK_STATE_", "JOB_STATE_"} {
		if len(s) > len(p) && s[:len(p)] == p {
			return s[len(p):]
		}
	}
	return s
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
