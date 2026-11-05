package sim

// Replay of a driven window.
//
// A DriveWindow run is not reproducible from its seed: the injection has to
// find the current leader and defers when there is none, so the step it lands
// on and the node it hits depend on timing. That is a real weakening of the
// reproducibility checkpoint and the reason Result.Window exists — the seed reproduces
// the adversary's INTENT, the plan reproduces its REALIZATION.
//
// This pins the part that must hold: feeding a realized plan back produces
// the same window. Without it, "replay" is an unverified claim in a comment,
// and a failure found in a sweep could not be put back in front of the
// system.

import (
	"context"
	"testing"

	"github.com/evazheng/fault-tolerant-coordinator/internal/server"
)

func TestReplayWindowReproducesTheRealizedWindow(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause bool
	}{
		{"partition", false},
		{"pause", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := func() Config {
				cfg := quickConfig(4242, targetedSteps)
				cfg.TargetStaleReads = true
				cfg.DriveWindow = true
				cfg.PauseLeader = tc.pause
				cfg.ReadMode = server.ReadLocal
				return cfg
			}

			first, _ := New(base(), dirsFor(t, 3)).Run(context.Background())
			if !first.Window.Fired {
				t.Skipf("the window never opened, so there is nothing to replay: %s", first.Window)
			}
			t.Logf("realized: %s", first.Window)

			// Feed the realization back.
			replayCfg := base()
			plan := first.Window
			replayCfg.ReplayWindow = &plan
			second, _ := New(replayCfg, dirsFor(t, 3)).Run(context.Background())

			if second.Window != first.Window {
				t.Errorf("replay produced a different window:\n  first:  %+v\n  second: %+v\n"+
					"a plan that does not reproduce its own window cannot put a sweep failure "+
					"back in front of the system", first.Window, second.Window)
			}
			if second.Window.Paused != tc.pause {
				t.Errorf("replayed window kind = paused:%v, want paused:%v",
					second.Window.Paused, tc.pause)
			}
		})
	}
}
