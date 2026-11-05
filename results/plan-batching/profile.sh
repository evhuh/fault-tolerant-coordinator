#!/bin/zsh
# Reproduce the 10k-task destabilization and CPU-profile the LEADER during it.
#
# The leader is identified at profile time rather than assumed: the stall
# causes leadership to move, so "node 2" is not a stable answer.
set -e
D=/tmp/ftc-bench
JOBS=${JOBS:-20}; TPJ=${TPJ:-500}; SECS=${SECS:-30}
ADDRS=127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083

$D/down.sh >/dev/null 2>&1 || true
MAXPLAN=1 SNAPENTRIES=500000 $D/up.sh
sleep 6
$D/loadgen -addrs $ADDRS -prefill $JOBS -tasks-per-job $TPJ -submitters 16 2>&1 | tail -1
sleep 4
for i in $(seq 1 8); do
  $D/worker -id w-$i -coordinator $ADDRS -concurrency 32 -data-dir $D/w-$i > $D/worker-$i.log 2>&1 &
  echo $! >> $D/wpids
done
# Let the stall establish before profiling: the first seconds are the pool
# registering, which is not what we are looking for.
sleep 8
leader=""
for i in 1 2 3; do
  if grep -q "became leader" $D/coord-$i.log && [ -n "$(tail -40 $D/coord-$i.log | grep 'became leader')" ]; then leader=$i; fi
done
[ -z "$leader" ] && leader=2
echo "profiling node $leader (most recent leader) for 20s"
curl -s "127.0.0.1:909$leader/debug/pprof/profile?seconds=20" -o $D/cpu-$JOBS-$TPJ.pprof
sleep $SECS
$D/down.sh >/dev/null 2>&1 || true
echo "profile: $D/cpu-$JOBS-$TPJ.pprof"
grep -chE "READY LOOP STALLED" $D/coord-*.log | paste -sd' ' -
