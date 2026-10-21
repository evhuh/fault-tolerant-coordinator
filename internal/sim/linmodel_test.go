package sim

// Tests for the model itself, separately from any cluster.
//
// A linearizability checker is a test oracle, and an oracle nobody tested is
// a test that passes for free. Every one of these is a history hand-built so
// that the right answer is known by construction — including the one the
// whole of Day 9 exists to catch.

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func submit(client int, key, jobID string, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		ClientId: client,
		Input:    linInput{Kind: opSubmit, Key: key},
		Output:   linOutput{JobID: jobID},
		Call:     call, Return: ret,
	}
}

func submitAmbiguous(client int, key string, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		ClientId: client,
		Input:    linInput{Kind: opSubmit, Key: key},
		Output:   linOutput{Unknown: true},
		Call:     call, Return: ret,
	}
}

func get(client int, jobID string, found bool, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		ClientId: client,
		Input:    linInput{Kind: opGet, JobID: jobID},
		Output:   linOutput{Found: found},
		Call:     call, Return: ret,
	}
}

func check(t *testing.T, ops []porcupine.Operation) bool {
	t.Helper()
	res := porcupine.CheckOperationsTimeout(jobsModel(), ops, 10*time.Second)
	if res == porcupine.Unknown {
		t.Fatalf("checker timed out on a %d-operation history; the test is too big", len(ops))
	}
	return res == porcupine.Ok
}

func TestModelAcceptsSequentialHistory(t *testing.T) {
	ops := []porcupine.Operation{
		submit(0, "c0/1", "job-a", 0, 10),
		get(0, "job-a", true, 20, 30),
		submit(0, "c0/2", "job-b", 40, 50),
		get(0, "job-b", true, 60, 70),
		get(0, "job-a", true, 80, 90),
	}
	if !check(t, ops) {
		t.Fatal("a plain sequential history must be linearizable")
	}
}

// THE BUG. A job is submitted and acknowledged, then a later read — strictly
// after the submit returned, so there is no ordering freedom at all — says it
// does not exist. This is what a partitioned leader answering from its own
// stale state produces.
func TestModelRejectsStaleReadAfterAcknowledgedSubmit(t *testing.T) {
	ops := []porcupine.Operation{
		submit(0, "c0/1", "job-a", 0, 10),
		get(1, "job-a", false, 20, 30),
	}
	if check(t, ops) {
		t.Fatal("a NOT_FOUND strictly after an acknowledged submit of that job is not linearizable")
	}
}

// Found-then-not-found is the same violation in the other direction: state
// that went backwards. A follower cannot produce it (it refuses reads), so
// this is a leader that lost committed state or a second leader answering
// from an older log.
func TestModelRejectsJobDisappearing(t *testing.T) {
	ops := []porcupine.Operation{
		submit(0, "c0/1", "job-a", 0, 10),
		get(0, "job-a", true, 20, 30),
		get(1, "job-a", false, 40, 50),
	}
	if check(t, ops) {
		t.Fatal("a job that exists and then does not is not linearizable")
	}
}

// A read CONCURRENT with the submit may legally miss the job: the submit had
// not returned, so the client had no acknowledgement to be contradicted. This
// is the case that separates a real checker from an assertion that reads
// always see writes.
func TestModelAcceptsConcurrentReadMissingTheJob(t *testing.T) {
	ops := []porcupine.Operation{
		submit(0, "c0/1", "job-a", 0, 100),
		get(1, "job-a", false, 10, 20),
	}
	if !check(t, ops) {
		t.Fatal("a read concurrent with the submit may miss the job")
	}
}

// Dedup: the same (clientID, requestID) must come back with the same job id.
func TestModelAcceptsDedupedRetry(t *testing.T) {
	ops := []porcupine.Operation{
		submit(0, "c0/1", "job-a", 0, 10),
		submit(0, "c0/1", "job-a", 20, 30),
		get(0, "job-a", true, 40, 50),
	}
	if !check(t, ops) {
		t.Fatal("a retry of the same request key returning the same job is correct")
	}
}

func TestModelRejectsDedupReturningADifferentJob(t *testing.T) {
	ops := []porcupine.Operation{
		submit(0, "c0/1", "job-a", 0, 10),
		submit(0, "c0/1", "job-b", 20, 30),
	}
	if check(t, ops) {
		t.Fatal("one request key minting two jobs means dedup failed")
	}
}

func TestModelRejectsTwoKeysSharingAJobID(t *testing.T) {
	ops := []porcupine.Operation{
		submit(0, "c0/1", "job-a", 0, 10),
		submit(1, "c1/1", "job-a", 20, 30),
	}
	if check(t, ops) {
		t.Fatal("two distinct requests collapsing onto one job id is a violation")
	}
}

// An ambiguous submit constrains nothing, and must not make an otherwise
// legal history illegal. This is rule 6's cost: ambiguity is recorded and
// then permits everything, which is correct — the client observed nothing.
func TestModelAcceptsAmbiguousSubmit(t *testing.T) {
	ops := []porcupine.Operation{
		submitAmbiguous(0, "c0/1", 0, 10),
		submit(0, "c0/1", "job-a", 20, 30),
		get(0, "job-a", true, 40, 50),
	}
	if !check(t, ops) {
		t.Fatal("an ambiguous submit followed by a successful retry is legal")
	}
}

// And an ambiguous outcome must not LAUNDER a violation either: the stale
// read is still a violation when an ambiguous operation sits next to it.
func TestModelStillRejectsStaleReadAlongsideAmbiguity(t *testing.T) {
	ops := []porcupine.Operation{
		submitAmbiguous(0, "c0/9", 0, 10),
		submit(0, "c0/1", "job-a", 20, 30),
		get(1, "job-a", false, 40, 50),
	}
	if check(t, ops) {
		t.Fatal("an unrelated ambiguous operation does not excuse a stale read")
	}
}

// Reordering freedom: two concurrent submits can linearize either way, and a
// read that sees only one of them is legal.
func TestModelAcceptsConcurrentSubmits(t *testing.T) {
	ops := []porcupine.Operation{
		submit(0, "c0/1", "job-a", 0, 50),
		submit(1, "c1/1", "job-b", 10, 60),
		get(2, "job-a", true, 70, 80),
		get(2, "job-b", true, 90, 100),
	}
	if !check(t, ops) {
		t.Fatal("concurrent submits followed by reads of both are linearizable")
	}
}

func TestModelStateIsComparable(t *testing.T) {
	// porcupine falls back to == when Equal is nil, and silently explores an
	// exponentially larger space if two equal states compare unequal. Pin the
	// property the model relies on.
	a := linState{jobs: linAdd(linAdd("", "b"), "a")}
	b := linState{jobs: linAdd(linAdd("", "a"), "b")}
	if a != b {
		t.Fatalf("insertion order must not change the state: %q vs %q", a.jobs, b.jobs)
	}
}

func TestLinSetHelpers(t *testing.T) {
	set := linAdd(linAdd(linAdd("", "x"), "y"), "z")
	for _, e := range []string{"x", "y", "z"} {
		if !linHas(set, e) {
			t.Fatalf("%q should contain %q", set, e)
		}
	}
	if linHas(set, "w") {
		t.Fatalf("%q should not contain w", set)
	}
	if linHas("", "x") {
		t.Fatal("the empty set contains nothing")
	}
	// A prefix of an element is not an element.
	if linHas(linAdd("", "job-ab"), "job-a") {
		t.Fatal("membership must be exact, not by prefix")
	}
	bound, ok := linLookup(linAdd("", "c0/1=job-a"), "c0/1")
	if !ok || bound != "job-a" {
		t.Fatalf("lookup returned (%q, %v)", bound, ok)
	}
	if _, ok := linLookup(linAdd("", "c0/1=job-a"), "c0/2"); ok {
		t.Fatal("lookup of an unbound key must miss")
	}
}

// The recorder's classification is where epistemics turn into history, so pin
// it directly rather than only through a whole run.
func TestClassifyRecordsAmbiguityAsUnknown(t *testing.T) {
	s := &Sim{history: newHistory(0)}

	if out := s.classifyGet(nil); !out.Found || out.Unknown {
		t.Fatalf("a successful read is a definitive found: %+v", out)
	}
	if out := s.classifyGet(notFoundErr()); out.Found || out.Unknown {
		t.Fatalf("NOT_FOUND is a definitive absence, not ambiguity: %+v", out)
	}
	if out := s.classifyGet(unavailableErr()); !out.Unknown {
		t.Fatalf("a refusal to answer is not an answer: %+v", out)
	}
	if out := s.classifySubmit(nil, unavailableErr()); !out.Unknown {
		t.Fatalf("a lost proposal may still commit: %+v", out)
	}
	if out := s.classifySubmit(nil, notLeaderErr()); !out.Unknown {
		t.Fatalf("NOT_LEADER can arrive after the entry was proposed: %+v", out)
	}
	if s.classifySubmit(nil, invalidArgErr()); s.history.rejected != 1 {
		t.Fatalf("a definitive rejection must be counted, got %d", s.history.rejected)
	}
}

func notFoundErr() error    { return status.Error(codes.NotFound, "unknown job") }
func unavailableErr() error { return status.Error(codes.Unavailable, "proposal lost, retry") }
func notLeaderErr() error   { return status.Error(codes.FailedPrecondition, "NOT_LEADER") }
func invalidArgErr() error  { return status.Error(codes.InvalidArgument, "rejected") }
