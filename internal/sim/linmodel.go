package sim

// The sequential specification the recorded history is checked against.
//
// # What is modelled, and why only this
//
// Two client operations: SubmitJob and GetJob.
// Those are the whole client-visible surface that touches job existence.
// RequestTask delivers a decision the log already committed, and the worker
// RPCs (RegisterWorker, Heartbeat, ReportResult) are not client operations at
// all — they are the execution substrate reporting in.
// Modelling them would mean modelling the scheduler, which is a concurrent
// actor inside the system rather than a caller of it, and a sequential spec
// cannot express "and then, at some point, a worker reported".
//
// State is the jobs map, exactly as the roadmap says, plus the dedup
// bindings: SubmitJob is idempotent on (clientID, requestID), and a spec that
// ignores that would call every correct retry a violation.
//
// # What a state is
//
// Two sorted, delimited strings rather than two maps, which buys three
// things that matter more here than elegance.
// The state is comparable, so porcupine's default equality and the
// `interface{}` boxing both work with no Equal or Hash function to get
// subtly wrong.
// It is immutable, which porcupine REQUIRES — Step is called during a
// backtracking search and a state it mutates corrupts every branch that
// shares it.
// And it is cheap to copy, which is the operation the search performs most.
// Job ids and request keys contain neither '|' nor '=', so the encoding is
// unambiguous.

import (
	"fmt"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

type linOpKind string

const (
	opSubmit linOpKind = "submit"
	opGet    linOpKind = "get"
)

// linInput is one client operation's arguments.
type linInput struct {
	Kind linOpKind
	// Key is "clientID/requestID" — the dedup key, for a submit.
	Key string
	// JobID is the id being read, for a get.
	JobID string
}

// linOutput is what the client observed.
//
// Unknown is the honest third answer and the reason this type is not a bare
// (string, bool).
// A client that sees a timeout, NOT_LEADER after proposing, or a lost
// proposal learns NOTHING about whether the operation took effect, and the
// entry may still commit after the RPC has returned.
// Recording that as "it failed" is how a failing history turns into a passing
// one, because the checker is then told something the client never observed.
type linOutput struct {
	JobID   string
	Found   bool
	Unknown bool
}

// linState is the sequential state: which jobs exist, and which dedup key
// minted each one.
type linState struct {
	// jobs is the sorted set of job ids that exist.
	jobs string
	// keys is the sorted set of "clientID/requestID=jobID" bindings.
	keys string
}

// jobsModel is the model handed to porcupine.
func jobsModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} { return linState{} },
		Step: func(st, in, out interface{}) (bool, interface{}) {
			return linStep(st.(linState), in.(linInput), out.(linOutput))
		},
		// PARTITIONING IS WHAT MAKES A LONG HISTORY CHECKABLE.
		//
		// porcupine's search is exponential in how many operations overlap,
		// so one 6,000-step history is not a bigger version of a 600-step
		// one — it is a search that does not finish, and an unfinished search
		// reports Unknown, which this harness reports as INCONCLUSIVE. The
		// headline property would come back "we could not tell" precisely as
		// the run got long enough to be interesting.
		//
		// Partitioning is sound only if operations in different parts cannot
		// constrain each other, so the parts are the CONNECTED COMPONENTS of
		// the coupling this model expresses, not simply one part per job id.
		// See linPartitions.
		Partition:         linPartitions,
		DescribeOperation: describeLinOp,
		DescribeState: func(st interface{}) string {
			s := st.(linState)
			n := 0
			if s.jobs != "" {
				n = strings.Count(s.jobs, "|") + 1
			}
			return fmt.Sprintf("%d jobs {%s}", n, s.jobs)
		},
	}
}

// linStep is the spec itself.
//
// # The ambiguous submit
//
// An ambiguous SubmitJob steps the state nowhere, and that is sound here for
// a specific reason rather than for convenience.
// The job id is minted by the SERVER, so a client whose submit returned
// ambiguously never learns the id of the job it may have created — which
// means it can never pass that id to GetJob, and no operation in this model
// can observe the job's existence.
// If the commit did happen, the only way it ever becomes visible is a retry
// of the same (clientID, requestID), and dedup makes that retry return the
// original id, which the model then treats as the submit that created it.
// Both branches — committed or not — end in the same observable state, so
// splitting them (a porcupine NondeterministicModel) would double the search
// space to distinguish two things no operation can tell apart.
//
// THIS STOPS BEING SOUND the moment an operation enumerates jobs without
// naming one. Add ListJobs to the history and the branches diverge
// immediately: the committed branch has a job the unchanged branch does not,
// and a list would see it. At that point this must become a
// NondeterministicModel. That trigger is the thing to remember, not the
// shortcut.
func linStep(s linState, in linInput, out linOutput) (bool, interface{}) {
	switch in.Kind {
	case opSubmit:
		if out.Unknown {
			return true, s
		}
		// A repeat of a key already bound MUST return the same job. A
		// different id means dedup failed and there are now two jobs where
		// the client asked for one.
		if bound, ok := linLookup(s.keys, in.Key); ok {
			return bound == out.JobID, s
		}
		// A fresh key that returns an id already in use means two distinct
		// requests collided on one job.
		if linHas(s.jobs, out.JobID) {
			return false, s
		}
		return true, linState{
			jobs: linAdd(s.jobs, out.JobID),
			keys: linAdd(s.keys, in.Key+"="+out.JobID),
		}

	case opGet:
		if out.Unknown {
			return true, s
		}
		// The whole hunt lands here. A job the history has already seen
		// submitted must be found; a job that does not exist must not be.
		// A partitioned leader answering from its own stale state returns
		// NOT_FOUND for a job the client was told was created, and no
		// ordering of these operations makes that legal.
		return out.Found == linHas(s.jobs, in.JobID), s
	}
	return false, s
}

func describeLinOp(in, out interface{}) string {
	i := in.(linInput)
	o := out.(linOutput)
	switch i.Kind {
	case opSubmit:
		if o.Unknown {
			return fmt.Sprintf("Submit(%s) -> AMBIGUOUS", i.Key)
		}
		return fmt.Sprintf("Submit(%s) -> %s", i.Key, o.JobID)
	case opGet:
		switch {
		case o.Unknown:
			return fmt.Sprintf("Get(%s) -> AMBIGUOUS", i.JobID)
		case o.Found:
			return fmt.Sprintf("Get(%s) -> found", i.JobID)
		default:
			return fmt.Sprintf("Get(%s) -> NOT_FOUND", i.JobID)
		}
	}
	return "?"
}

// Set helpers over the delimited encoding. Sorted on insert, so two states
// that hold the same elements are the same string and compare equal.

func linHas(set, elem string) bool {
	if set == "" {
		return false
	}
	for _, e := range strings.Split(set, "|") {
		if e == elem {
			return true
		}
	}
	return false
}

func linAdd(set, elem string) string {
	if set == "" {
		return elem
	}
	parts := append(strings.Split(set, "|"), elem)
	sort.Strings(parts)
	return strings.Join(parts, "|")
}

// linLookup finds a "key=value" binding.
func linLookup(set, key string) (string, bool) {
	if set == "" {
		return "", false
	}
	prefix := key + "="
	for _, e := range strings.Split(set, "|") {
		if strings.HasPrefix(e, prefix) {
			return e[len(prefix):], true
		}
	}
	return "", false
}

// linPartitions splits a history into independently checkable parts.
//
// # Why not simply one part per job id
//
// Partitioning by job id alone would lose a real check. Two submits sharing a
// dedup key MUST return the same job — that is the whole of
// effectively-once at the client boundary — and a system that got it wrong
// would return two different ids, which job-id keying would file in two
// different parts where neither could see the other. The violation would
// vanish exactly when it happened.
//
// So the parts are the connected components over BOTH kinds of name this
// model knows: dedup keys and job ids. Two operations land together when they
// share either. For a correct system that is almost always one part per job;
// for an incorrect one it is still one part per *coupling*, which is what
// soundness needs.
//
// An AMBIGUOUS submit carries no job id and is identity in the spec, so it
// could be dropped — but it is kept, grouped by its key, because a later
// retry of that key is exactly how such a submit becomes observable, and the
// two have to be checked together.
//
// # What makes this sound
//
// Every operation here names the one job it concerns: a get names it in its
// input, a submit in its output. No operation observes the set of jobs
// without naming one. Add ListJobs and this is immediately wrong — the same
// trigger that breaks the ambiguous-submit shortcut above, for the same
// reason: a list couples every job to every other.
func linPartitions(ops []porcupine.Operation) [][]porcupine.Operation {
	uf := newUnionFind()
	// First pass: every operation declares the names it touches, and names
	// touched together are joined.
	names := make([][]string, len(ops))
	for i, op := range ops {
		in := op.Input.(linInput)
		out := op.Output.(linOutput)
		var ns []string
		if in.Key != "" {
			ns = append(ns, "key:"+in.Key)
		}
		if in.JobID != "" {
			ns = append(ns, "job:"+in.JobID)
		}
		if out.JobID != "" {
			ns = append(ns, "job:"+out.JobID)
		}
		if len(ns) == 0 {
			// Nothing named: cannot constrain anything, so it is its own
			// part. Only reachable if an operation is recorded with neither
			// a key nor a job, which the clients do not do.
			ns = append(ns, fmt.Sprintf("orphan:%d", i))
		}
		for _, n := range ns[1:] {
			uf.union(ns[0], n)
		}
		names[i] = ns
	}

	// Second pass: group by component. Insertion order is preserved so the
	// parts are deterministic — porcupine does not require it, but a verdict
	// that depends on map iteration order is not one you can diff.
	index := map[string]int{}
	var parts [][]porcupine.Operation
	for i, op := range ops {
		root := uf.find(names[i][0])
		at, ok := index[root]
		if !ok {
			at = len(parts)
			index[root] = at
			parts = append(parts, nil)
		}
		parts[at] = append(parts[at], op)
	}
	return parts
}

// unionFind over string names, which is all this needs: a few thousand names
// and no deletions.
type unionFind struct{ parent map[string]string }

func newUnionFind() *unionFind { return &unionFind{parent: map[string]string{}} }

func (u *unionFind) find(x string) string {
	p, ok := u.parent[x]
	if !ok {
		u.parent[x] = x
		return x
	}
	if p == x {
		return x
	}
	root := u.find(p)
	u.parent[x] = root // path compression
	return root
}

func (u *unionFind) union(a, b string) {
	ra, rb := u.find(a), u.find(b)
	if ra != rb {
		u.parent[ra] = rb
	}
}
