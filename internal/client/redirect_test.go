package client

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

func TestIsNotLeaderRecognizesRedirects(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantAddr string
		wantOK   bool
	}{
		{
			name:     "redirect with a known leader",
			err:      status.Error(codes.FailedPrecondition, "NOT_LEADER: leader is at 127.0.0.1:8081"),
			wantAddr: "127.0.0.1:8081",
			wantOK:   true,
		},
		{
			// Mid-election there is no leader. The caller still needs to know
			// this node cannot serve it, so it can try elsewhere.
			name:     "redirect with no leader elected",
			err:      status.Error(codes.Unavailable, "NOT_LEADER: no leader elected"),
			wantAddr: "",
			wantOK:   true,
		},
		{
			name:   "an ordinary error is not a redirect",
			err:    status.Error(codes.InvalidArgument, "job has no tasks"),
			wantOK: false,
		},
		{
			// Unavailable alone means the node is DOWN, not that it is a
			// follower. It is not a redirect — but Do must still rotate off
			// it, which TestDoRotatesOffAnUnreachableNode covers.
			name:   "plain unavailable is not a redirect",
			err:    status.Error(codes.Unavailable, "connection refused"),
			wantOK: false,
		},
		{name: "nil", err: nil, wantOK: false},
		{name: "non-grpc error", err: errors.New("boom"), wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addr, ok := IsNotLeader(tc.err)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if addr != tc.wantAddr {
				t.Errorf("addr = %q, want %q", addr, tc.wantAddr)
			}
		})
	}
}

// The raft peer set holds transport URLs; gRPC dials host:port. The redirect
// crosses those two address spaces, so the scheme has to come off.
func TestRedirectNormalizesAddresses(t *testing.T) {
	p := NewPool([]string{"127.0.0.1:1", "127.0.0.1:2"})
	for _, in := range []string{
		"http://127.0.0.1:9090",
		"https://127.0.0.1:9090",
		"127.0.0.1:9090",
		"http://127.0.0.1:9090/",
	} {
		p.Redirect(in)
		if got := p.Current(); got != "127.0.0.1:9090" {
			t.Errorf("Redirect(%q) → %q, want 127.0.0.1:9090", in, got)
		}
	}
}

// An empty address means "this node knows of no leader". Rotating beats
// hammering the node that just said it cannot help.
func TestRedirectWithNoLeaderRotatesSeeds(t *testing.T) {
	p := NewPool([]string{"a:1", "b:2", "c:3"})
	if got := p.Current(); got != "a:1" {
		t.Fatalf("initial = %q, want a:1", got)
	}
	want := []string{"b:2", "c:3", "a:1"}
	for i, w := range want {
		p.Redirect("")
		if got := p.Current(); got != w {
			t.Fatalf("rotation %d = %q, want %q", i, got, w)
		}
	}
}

// Do must give up rather than chase a redirect loop forever — a cluster
// mid-election can bounce a caller indefinitely.
func TestDoBoundsTheRedirectChase(t *testing.T) {
	p := NewPool([]string{"127.0.0.1:1", "127.0.0.1:2"})
	calls := 0
	err := p.Do(func(schedulerpb.SchedulerClient) error {
		calls++
		return status.Error(codes.FailedPrecondition, "NOT_LEADER: leader is at 127.0.0.1:9999")
	})
	if err == nil {
		t.Fatal("Do succeeded against an endless redirect")
	}
	if calls != maxAttempts {
		t.Errorf("fn ran %d times, want %d", calls, maxAttempts)
	}
}

// A real error must come straight back, not be retried as a redirect.
func TestDoDoesNotRetryOrdinaryErrors(t *testing.T) {
	p := NewPool([]string{"127.0.0.1:1"})
	calls := 0
	want := status.Error(codes.InvalidArgument, "job has no tasks")
	err := p.Do(func(schedulerpb.SchedulerClient) error {
		calls++
		return want
	})
	if calls != 1 {
		t.Errorf("fn ran %d times, want 1 — an InvalidArgument is not a redirect", calls)
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("err = %v, want the original InvalidArgument", err)
	}
}

func TestDoReturnsNilOnSuccess(t *testing.T) {
	p := NewPool([]string{"127.0.0.1:1"})
	calls := 0
	if err := p.Do(func(schedulerpb.SchedulerClient) error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("Do = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("fn ran %d times, want 1", calls)
	}
}

// THE CASE THAT STRANDED THE WORKER POOL.
//
// A killed leader does not answer NOT_LEADER — it does not answer at all. If
// Do only handled redirects it would retry the dead node forever, and a
// coordinator failover would take out every worker even though the cluster
// recovered fine.
func TestDoRotatesOffAnUnreachableNode(t *testing.T) {
	p := NewPool([]string{"a:1", "b:2", "c:3"})
	var seen []string
	err := p.Do(func(schedulerpb.SchedulerClient) error {
		seen = append(seen, p.Current())
		return status.Error(codes.Unavailable, "connection refused")
	})
	if err == nil {
		t.Fatal("Do succeeded against an unreachable cluster")
	}
	if len(seen) != maxAttempts {
		t.Fatalf("fn ran %d times, want %d", len(seen), maxAttempts)
	}
	// It must actually have moved, not retried one address.
	distinct := map[string]bool{}
	for _, a := range seen {
		distinct[a] = true
	}
	if len(distinct) < 3 {
		t.Errorf("only tried %v; a dead node must not be retried forever", distinct)
	}
}

// A deadline exceeded is also "this node is not answering", and rotates.
//
// Asserted over the addresses actually TRIED, not the one left at the end:
// with an even number of seeds the rotation can land back where it started,
// which would make a final-position check silently vacuous.
func TestDoRotatesOnTimeout(t *testing.T) {
	p := NewPool([]string{"a:1", "b:2"})
	var seen []string
	_ = p.Do(func(schedulerpb.SchedulerClient) error {
		seen = append(seen, p.Current())
		return status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	})
	distinct := map[string]bool{}
	for _, a := range seen {
		distinct[a] = true
	}
	if len(distinct) != 2 {
		t.Errorf("tried %v, want both seeds — a timing-out node must not be retried forever", distinct)
	}
}
