#!/usr/bin/env bash
#
# Run one test N times and tally PASS / SKIP / FAIL.
#
# WHY A SKIP BUDGET. The reproducibility checkpoints skip rather than fail
# when a run could not be evaluated — a run cut short by a host-io stall logs
# only a prefix of its schedule, and comparing a prefix against a complete log
# reports nondeterminism that is not there. That skip is the honest outcome
# for that run and it is also a way for a checkpoint to stop checking
# anything: twenty-five green iterations that all skipped prove exactly
# nothing, and `go test` exits 0 for every one of them.
#
# So repetition alone is not a result. This counts the skips, prints them next
# to the passes, and fails if they take over.
#
# Usage: loop-test.sh <pkg> <test-regex> <iterations> <max-skip-percent>
set -u -o pipefail

pkg=${1:?package}
run=${2:?test regex}
iters=${3:-25}
budget=${4:-10}

pass=0
skip=0
fail=0
log_dir=$(mktemp -d)
trap 'rm -rf "$log_dir"' EXIT

for i in $(seq 1 "$iters"); do
	# -count=1 defeats the test cache: a cached PASS is not another run, and
	# the whole point here is repetition.
	if ! go test -count=1 -v "$pkg" -run "$run" >"$log_dir/$i.log" 2>&1; then
		fail=$((fail + 1))
		echo "FAIL on iteration $i:"
		grep -E -e '^--- FAIL' -e '^    [a-z_]+_test\.go:' "$log_dir/$i.log" | head -20
		continue
	fi
	# A skip and a pass both exit 0, which is exactly the problem.
	if grep -q '^--- SKIP' "$log_dir/$i.log"; then
		skip=$((skip + 1))
		# -B1: go test -v prints the skip REASON on the line before the
		# --- SKIP marker, and the reason is the whole point of showing it.
		grep -B1 '^--- SKIP' "$log_dir/$i.log" | head -1 | sed 's/^/    skipped:/'
	else
		pass=$((pass + 1))
	fi
done

pct=$((skip * 100 / iters))
echo "$run over $iters iterations: $pass passed, $skip skipped ($pct%), $fail failed"

if [ "$fail" -gt 0 ]; then
	echo "FAILED: $fail of $iters iterations failed"
	exit 1
fi
if [ "$pass" -eq 0 ]; then
	echo "FAILED: every iteration skipped, so this checkpoint asserted nothing"
	exit 1
fi
if [ "$pct" -gt "$budget" ]; then
	echo "FAILED: skip rate $pct% exceeds the $budget% budget — the checkpoint is evaluating too few of its runs to be worth quoting"
	exit 1
fi
