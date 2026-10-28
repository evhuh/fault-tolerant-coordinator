.PHONY: proto test race vet build sweeps repro fsync-gate release-check

# Regenerate both schemas. Run from proto/ so `source:` in the generated headers stays stable; --go_opt=module strips the package prefix so files land in proto/commandpb and proto/schedulerpb rather than a nested tree
PROTO_MODULE := github.com/evazheng/fault-tolerant-coordinator/proto

proto:
	cd proto && protoc \
		--go_out=. --go_opt=module=$(PROTO_MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(PROTO_MODULE) \
		command.proto scheduler.proto snapshot.proto

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

# The adversary sweeps: targeted stale-read validation against each adversary.
#
# NOT part of `make test`. They take minutes, and they are VALIDATION of the
# targeted workload — proof that it can still catch a bug that is definitely
# present — rather than per-commit regression guards. What guards the
# behaviour on every commit is the sub-second deterministic stale-read
# scenario under both read modes, plus the liveness tests, all in `make test`.
#
# REQUIRED BEFORE:
#   - tagging a release, since the tag asserts the claim these measure;
#   - quoting any catch rate, coverage figure or "zero violations" number in
#     the README, the roadmap or anywhere else.
#
# EVERY QUOTED RATE CARRIES ITS n AND SEED RANGE. "8/8 Illegal" means nothing
# on its own; "8/8 Illegal over seeds 1-8 at 7 steps" can be reproduced and
# argued with. The tests print exactly that, and a number copied out of them
# without its n is not a result.
#
# REPRODUCIBILITY IS NARROWER HERE THAN IN `make test`. These sweeps DRIVE
# their window — they inject a leader pause or partition — and the injection
# has to find the current leader, deferring when there is none. So the step it
# lands on and the node it hits depend on timing, not on the seed: a sweep
# seed reproduces the adversary's INTENT, not its realization. Each run
# reports the realization (Result.Window) and Config.ReplayWindow feeds it
# back to force the same step and victim.
#
# Consequence for anything quoted: a failure found here MUST be reduced to a
# deterministic scenario test before it is called fixed. Ordinary runs do
# reproduce their schedule from the seed — that claim is unaffected and is
# pinned by TestSameSeedReproducesTheFaultSchedule.
#
# -timeout is explicit because the default 10m fails the whole package with no
# test failing, which is the least informative failure Go can produce.
sweeps:
	FTC_SWEEPS=1 go test -timeout 30m -v ./internal/sim/ \
		-run 'TestTargetedWorkload|TestPauseDefeats|TestShippedConfigWithoutPause|TestReadIndexFindsNoStale|TestEffectLedger'

# The reproducibility checkpoints, REPEATED, with a skip budget.
#
# Repetition is the point. These are timing-sensitive, and a single green pass
# is not evidence about them: one clean `go test -race` pass was reported as
# "race clean" during Day 10, and eight runs then put the real failure rate
# near 2 in 8. One pass is a sample.
#
# The skip budget is the other half. A checkpoint skips when it could not be
# evaluated — a run cut short by a host fsync logs only a prefix of its
# schedule — and `go test` exits 0 for a skip exactly as it does for a pass.
# Without counting them, a checkpoint can quietly stop checking anything and
# still look green twenty-five times in a row.
#
# The budgets are TRIPWIRES, not calibrated floors, and the two measurements
# behind them say why. On a QUIET disk (fsync mean 3.1ms, worst 9ms) the skip
# rate is 0 of 25 for both schedule checkpoints and 0 of 10 for both replay
# tests. On a disk being hammered by back-to-back race suites and sweeps —
# fsyncs of 1 to 13 MINUTES in persist — the same checkpoint measured 20 of 25
# skipped, because a run that stops early on a host-io stall logs only a
# prefix of its schedule and cannot be compared.
#
# So the skip rate is a property of the HOST, not of the code, and the budget
# is there to say "this machine cannot evaluate this right now" rather than to
# be raised until it passes. If it trips, measure the disk (scripts have no
# probe; a 40-iteration write+fsync loop is enough) before touching the
# number.
repro:
	scripts/loop-test.sh ./internal/sim/ 'TestSameSeedReproducesTheFaultSchedule$$' 25 10
	scripts/loop-test.sh ./internal/sim/ 'TestSameSeedReproducesTheFaultScheduleWithRealWorkers' 25 10
	scripts/loop-test.sh ./internal/sim/ 'TestReplayWindowReproducesTheRealizedWindow' 10 20
	FTC_SWEEPS=1 scripts/loop-test.sh ./internal/sim/ 'TestReplayWorkerPauseReproducesTheRealizedPause' 10 20

# Is this host capable of supporting the claim at all?
#
# Every timing judgement in the harness is measured against a clock compressed
# to a 250ms election timeout, and disk I/O does not compress with it. When
# fsync latency climbs, runs stop completing: the checkpoints SKIP and the
# stalls go INCONCLUSIVE, both of which exit 0. A release would then assert
# over runs that checked nothing.
#
# The budgets come from the premise, not from the best measurement. Mean 12ms
# is under half a 25ms tick, so a loop that fsyncs can still keep up; worst
# 100ms is under the 125ms stall threshold, so one slow sync cannot end a run.
# Measured on a quiet machine: mean 3.1-3.8ms, worst 5-13ms, which is
# comfortable margin. Measured during back-to-back race suites: fsyncs of 1 to
# 13 MINUTES, which is what this gate exists to refuse.
fsync-gate:
	go run ./cmd/fsyncprobe

# Everything a tag asserts: a host that can support the premise, then the
# default suite, the race detector, the reproducibility checkpoints under
# repetition, and the sweeps whose numbers the claim rests on.
#
# FTC_STRICT_STALLS makes an UNATTRIBUTABLE stall fail rather than be
# tolerated. Ordinary development tolerates one — a laptop hiccup turning six
# tests red teaches nothing — but a run that is supporting a claim cannot pass
# on "the harness could not tell". The gate above runs first, so this should
# never trip; if it does, the host degraded mid-run and nothing from it is
# quotable.
release-check: fsync-gate
	FTC_STRICT_STALLS=1 $(MAKE) vet test race repro sweeps

vet:
	go vet ./...
