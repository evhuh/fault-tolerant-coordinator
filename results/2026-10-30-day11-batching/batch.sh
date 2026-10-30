#!/bin/zsh
# The plan-batching experiment, one config.
#
# A plan can only assign as many tasks as there are FREE WORKER SLOTS at that
# instant, so a big plan needs a big backlog AND many idle slots at the same
# moment. A rate-limited submitter running alongside live workers cannot
# produce that: slots free one or two at a time and every plan carries one
# assignment (measured: 2782 plans at exactly 1.00). So:
#
#   1. cluster up at -max-plan-size P, NO workers
#   2. prefill a backlog of pending tasks (nothing assignable yet)
#   3. scrape every node
#   4. start a large idle pool at once
#   5. wait, scrape every node, diff
#
# THE BACKLOG MUST BE FEW JOBS WITH MANY TASKS. computePlan iterates
# snap.PendingTaskIDs(eval.JobID) — scoped to the eval's OWN job — so 2,000
# jobs of one task each pins plan size at 1 whatever -max-plan-size says.
# Measured that way twice, at max-plan-size 1 and 100: both 1.00.
#
# The backlog is also kept MODEST in total tasks. A 20,000-job prefill grew
# the state to 5.9MB, and snapshotting that synchronously inside the Ready
# loop cost leadership 12 times in 30s — that run measured failover.
#
# Usage: batch.sh <plan-size> <backlog> <seconds> <label>
set -e
D=/tmp/ftc-bench
P=$1; JOBS=$2; SECS=$3; LABEL=$4; TPJ=${TPJ:-500}
WORKERS=8; CONC=32   # 256 slots, all idle when the pool starts
ADDRS=127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083

scrape() { for i in 1 2 3; do curl -s 127.0.0.1:909$i/metrics | grep -E "^(sched_|raft_)" > $D/$1-$P-$i.txt; done }

$D/down.sh >/dev/null 2>&1 || true
rm -f $D/t0-$P-1.txt $D/t0-$P-2.txt $D/t0-$P-3.txt $D/t1-$P-1.txt $D/t1-$P-2.txt $D/t1-$P-3.txt
MAXPLAN=$P $D/up.sh
sleep 6
$D/loadgen -addrs $ADDRS -prefill $JOBS -tasks-per-job $TPJ -submitters 16 2>&1 | tail -1
# SETTLE before t0. SubmitJob returns once the entry commits on the leader,
# but followers apply a moment later, so scraping immediately puts the
# prefill's own entries inside the measured window on whichever replica was
# behind — and the per-node figures are read from the busiest node.
sleep 4
scrape t0
for i in $(seq 1 $WORKERS); do
  $D/worker -id w-$i -coordinator $ADDRS -concurrency $CONC -data-dir $D/w-$i > $D/worker-$i.log 2>&1 &
  echo $! >> $D/wpids
done
sleep $SECS
scrape t1
$D/down.sh >/dev/null 2>&1 || true
cd $D && python3 -I analyze.py "$LABEL" "$P" "$SECS"
