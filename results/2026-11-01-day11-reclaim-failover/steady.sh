#!/bin/zsh
# Steady-state control: shipped config, same workload, NO KILLS.
#
# Every stall observed here is unambiguously steady state, because nothing
# restarts. The pooled "36 stalls beyond 10s of a restart" figure could not
# distinguish steady state from a replay longer than the 10s cutoff.
#
# ALL artifacts go in a RUN-STAMPED directory, logs included: the coordinator
# logs were previously opened in append mode in a shared directory, so one
# analysis mixed four runs and divided a four-run stall count by a one-run
# restart count.
set -e
D=/tmp/ftc-bench
RUN=${RUN:-steady-$(date -u +%Y%m%d-%H%M%S)}
R=$D/$RUN
mkdir -p $R
MINUTES=${MINUTES:-10}
ADDRS=127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
peers="1=http://127.0.0.1:7001,2=http://127.0.0.1:7002,3=http://127.0.0.1:7003"
apis="1=127.0.0.1:8081,2=127.0.0.1:8082,3=127.0.0.1:8083"

echo "run_id=$RUN" > $R/RUN_ID
echo "snapshot_entries=10000" >> $R/RUN_ID
echo "kills=0" >> $R/RUN_ID
echo "minutes=$MINUTES" >> $R/RUN_ID

pkill -x worker 2>/dev/null || true; pkill -x coordinator 2>/dev/null || true; sleep 1
for i in 1 2 3; do rm -rf $R/raft-$i; done
for i in 1 2 3; do
  $D/coordinator -id $i -addr 127.0.0.1:808$i -peers "$peers" -api-peers "$apis" \
    -data-dir $R/raft-$i -metrics-addr 127.0.0.1:909$i -max-plan-size 100 \
    -snapshot-entries 10000 > $R/coord-$i.log 2>&1 &
done
sleep 6
for k in 0 1 2 3; do
  $D/worker -id sw-$k -coordinator $ADDRS -concurrency 8 -data-dir $R/w-$k \
    > $R/worker-$k.log 2>&1 &
done
sleep 3
TRUEB=$(command -v true)
END=$((SECONDS + MINUTES*60))
n=0
while [ $SECONDS -lt $END ]; do
  $D/loadgen -addrs $ADDRS -prefill 6 -tasks-per-job 60 -command $TRUEB -submitters 8 \
    >> $R/loadgen.log 2>&1 || echo "loadgen failed" >> $R/loadgen.log
  n=$((n+1))
  sleep 8
done
echo "submitted $n batches" >> $R/RUN_ID
for i in 1 2 3; do curl -s 127.0.0.1:909$i/debug/tasks > $R/tasks-$i.json 2>/dev/null || true; done
pkill -x worker 2>/dev/null || true; pkill -x coordinator 2>/dev/null || true
echo "$R"
