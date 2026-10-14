#!/bin/bash
# Day 7: snapshots, compaction, and a COLD RESTART of the whole cluster.
# A deliberately low snapshot threshold so Restore is genuinely exercised.
source "$(dirname "$0")/common.sh"
build

RAFT="1=http://127.0.0.1:19021,2=http://127.0.0.1:19022,3=http://127.0.0.1:19023"
API="1=127.0.0.1:18081,2=127.0.0.1:18082,3=127.0.0.1:18083"
ALL_API="127.0.0.1:18081,127.0.0.1:18082,127.0.0.1:18083"

pkill -x coordinator 2>/dev/null; pkill -x worker 2>/dev/null; sleep 0.5
rm -rf raft-* data-w-* ./*.log

boot() {
  for i in 1 2 3; do
    ./coordinator -id $i -addr ":1808$i" -peers "$RAFT" -api-peers "$API" \
      -data-dir "./raft-$i" -snapshot-entries 25 -snapshot-catchup-entries 5 \
      >> "coord$i.log" 2>&1 &
  done
  sleep 3
}

boot
JOB=$(./schedctl -coordinator "$ALL_API" submit -n 4 -- /bin/sh -c 'echo ok')
echo "JOB=$JOB"
echo
echo "--- waiting for compaction (Tick alone drives the index forward) ---"
for i in $(seq 1 30); do
  n=$(ls raft-*/snapshot.pb 2>/dev/null | wc -l | tr -d ' ')
  [ "$n" = "3" ] && { echo "all 3 nodes snapshotted after ${i}s"; break; }
  sleep 1
done

echo
echo "--- on-disk state per node ---"
for i in 1 2 3; do
  w=$(stat -f%z "raft-$i/wal.log" 2>/dev/null || echo 0)
  s=$(stat -f%z "raft-$i/snapshot.pb" 2>/dev/null || echo 0)
  echo "  node $i: wal=${w}B snapshot=${s}B"
done
echo "(the WAL is compacted; the snapshot carries the history)"

echo
echo "--- state before the cold restart ---"
./schedctl -coordinator "$ALL_API" status "$JOB" | head -1

echo
echo ">>> SIGKILL ALL THREE NODES"
pkill -9 -x coordinator; sleep 2

echo ">>> cold start from disk"
boot

echo
echo "--- state after the cold restart ---"
./schedctl -coordinator "$ALL_API" status "$JOB" | head -1
echo
echo "--- dedup survived the snapshot? resubmitting the SAME request id ---"
A=$(./schedctl -coordinator "$ALL_API" submit -n 4 -request-id "dedup-probe" -- /bin/sh -c 'echo ok')
B=$(./schedctl -coordinator "$ALL_API" submit -n 4 -request-id "dedup-probe" -- /bin/sh -c 'echo ok')
if [ "$A" = "$B" ]; then echo "  same job id both times: $A  ✓"; else echo "  DUPLICATE: $A vs $B  ✗"; fi
echo
echo "--- total jobs (should be the original + one probe) ---"
./schedctl -coordinator "$ALL_API" list | wc -l | tr -d ' '
