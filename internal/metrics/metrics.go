// Package metrics is the coordinator's Prometheus instrumentation.
//
// Day 11's benchmarks need numbers that come from the SERVER rather than from
// the load generator, for the reason the roadmap gives about closed-loop
// clients: a generator can only report what it observed, and what it observed
// includes itself. Throughput measured as "completed tasks per second at the
// coordinator" is a property of the system; the same figure measured as
// "responses my client received" is a property of the pair.
//
// Everything here is a package-level collector registered on the default
// registry, because there is exactly one coordinator per process and threading
// a registry through five layers to support a second one would be scaffolding
// for a case that does not exist. The simulator runs many coordinators in one
// process, which is why every Observe and Inc here must be safe to call from
// several at once and must never be the thing that fails a run — a metric
// cannot be allowed to break the system it measures.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Buckets chosen from what this system has actually measured rather than from
// the client library's defaults, which start at 5ms and would put every raft
// commit in the first bucket.
//
// Commit latency on a laptop SSD is fsync-bound in the low tens of
// milliseconds, so the range runs from 1ms to 8s: the bottom captures a
// batched commit that amortized its fsync, and the top is past the 5s
// proposal timeout, so a proposal that died waiting is visible rather than
// clamped into +Inf alongside everything else slow.
var commitBuckets = prometheus.ExponentialBuckets(0.001, 2, 14)

// Stall buckets start at 1ms and reach ~16s. The 8-to-31-second Ready-loop
// stalls Day 10 measured under -race are the reason the top is not 1s: a
// histogram whose last finite bucket is below the worst observation reports
// every disaster identically.
var stallBuckets = prometheus.ExponentialBuckets(0.001, 2, 15)

// Snapshot sizes run from 16KB to ~1GB. Measured so far: 37KB early in a run
// to 1.6MB at 600 steps.
var snapshotBytesBuckets = prometheus.ExponentialBuckets(16*1024, 4, 9)

var (
	// TasksTotal is the throughput numerator. Labelled by what happened to
	// the task, because "completed" and "retried" moving together is a very
	// different system from "completed" rising alone.
	TasksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sched_tasks_total",
		Help: "Tasks that reached a terminal disposition, by state.",
	}, []string{"state"})

	// QueueDepth is sampled from committed state rather than counted
	// incrementally, so it cannot drift out of agreement with the state
	// machine. See Sampler.
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sched_queue_depth",
		Help: "Tasks in each non-terminal state.",
	}, []string{"state"})

	// EvalsPending answers "is the broker draining or growing", which is the
	// question that separates a saturated system from a broken one.
	EvalsPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sched_evals_pending",
		Help: "Evaluations waiting in the broker, ready plus blocked.",
	})

	// PlanSize is assignments per committed raft entry — the independent
	// variable of the plan-batching experiment, measured rather than assumed.
	// A run configured for batching that did not batch, because offered load
	// was too low to ever find two assignable tasks at once, would otherwise
	// look like a disproof of batching.
	PlanSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sched_plan_size",
		Help:    "Assignments per committed plan.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	})

	// PlanRejections counts optimistic-concurrency losses: a plan computed
	// against a state that had moved by the time it applied.
	PlanRejections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sched_plan_rejections_total",
		Help: "Plans rejected at apply because the state they were computed against had changed.",
	})

	FailoverLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sched_failover_latency_seconds",
		Help:    "Leadership loss to the new leader having rebuilt its broker and resumed.",
		Buckets: commitBuckets,
	})

	WorkerReclaimLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sched_worker_reclaim_latency_seconds",
		Help:    "Lease expiry to the task being requeued.",
		Buckets: commitBuckets,
	})

	// CommitLatency is proposal to apply, which is the fsync cost the
	// plan-batching experiment is about amortizing.
	CommitLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "raft_commit_latency_seconds",
		Help:    "Propose to applied, measured on the proposing node.",
		Buckets: commitBuckets,
	})

	LeaderElections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_leader_elections_total",
		Help: "Times this node became leader.",
	})

	// ReadyStall is PHASE-LABELLED, which is the whole value of it. Day 10
	// found a 133ms stall in snapshot+compact on a healthy disk, and an
	// unlabelled stall histogram would have reported that identically to a
	// blocking call in the apply path — the conflation that cost Day 9 an
	// afternoon. The phases are raftnode's own: persist, apply, send,
	// leadership-change, snapshot+compact, snapshot-in.
	ReadyStall = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "raft_ready_stall_seconds",
		Help:    "Ready-loop iterations that exceeded the stall threshold, by phase.",
		Buckets: stallBuckets,
	}, []string{"phase"})

	// SnapshotDuration exists because the synchronous snapshot is this
	// program's known exposure to disk tail latency, and a benchmark is
	// exactly where that should be visible under load rather than only in the
	// simulator.
	SnapshotDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "raft_snapshot_duration_seconds",
		Help:    "Time spent in maybeSnapshot: serialize, fsync and compact, inside the Ready loop.",
		Buckets: stallBuckets,
	})

	// RaftEntries breaks the committed log down by command type, which is
	// what keeps plan batching from being oversold. Each task completion is
	// its own CompleteTask entry and each worker heartbeat a RegisterWorker,
	// so batching N assignments into one ApplyPlan removes N-1 entries out of
	// a log that still carries one entry per completion: the smoke run
	// committed 18,246 entries against 2,782 plans. End-to-end task
	// throughput can therefore gain at most about 2x from batching, and the
	// breakdown is how a reader sees that rather than taking it on trust.
	//
	// Counted on EVERY node, because applying the log is what every replica
	// does; the figure is per-node and comparable across them.
	RaftEntries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "raft_entries_total",
		Help: "Committed entries applied, by command type.",
	}, []string{"command"})

	// Assignments is the plan-batching experiment's HEADLINE numerator:
	// assignments committed per second. Task completions are the wrong
	// headline for it, being bounded by the per-completion entry above.
	Assignments = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sched_assignments_total",
		Help: "Assignments that took effect, summed over committed plans.",
	})

	SnapshotBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "raft_snapshot_bytes",
		Help:    "Serialized snapshot size.",
		Buckets: snapshotBytesBuckets,
	})
)

// Handler serves the metrics endpoint.
//
// On its OWN listener, not the gRPC port: a scrape that contends with the API
// it is measuring changes the measurement, and during a benchmark the scraper
// is the one client guaranteed to be running.
func Handler() http.Handler { return promhttp.Handler() }

// ObserveStall records a Ready-loop stall under its phase.
//
// Takes the phase as a string because that is what raftnode.Stall carries,
// and deliberately does not validate it: an unexpected phase label is a
// cardinality nuisance, while dropping it loses the finding.
func ObserveStall(phase string, d time.Duration) {
	ReadyStall.WithLabelValues(phase).Observe(d.Seconds())
}

// ObserveSnapshot records one snapshot's cost and size together, so a reader
// can tell a slow snapshot of a small state (a disk tail) from a slow
// snapshot of a large one (serialization).
func ObserveSnapshot(bytes int, d time.Duration) {
	SnapshotDuration.Observe(d.Seconds())
	SnapshotBytes.Observe(float64(bytes))
}

// Sample refreshes the gauges from committed state.
//
// Gauges are SAMPLED rather than tracked incrementally so they cannot drift
// out of agreement with the state machine: an incrementally-maintained depth
// and the state it claims to describe are two sources of truth, and this
// project has already paid for one counter that disagreed with reality.
//
// Cheap enough to call every second — it walks the task map once — and the
// caller decides how often, because the benchmark's scrape interval is the
// thing that actually needs it.
func Sample(pending, assigned, running, evalsPending int) {
	QueueDepth.WithLabelValues("pending").Set(float64(pending))
	QueueDepth.WithLabelValues("assigned").Set(float64(assigned))
	QueueDepth.WithLabelValues("running").Set(float64(running))
	EvalsPending.Set(float64(evalsPending))
}
