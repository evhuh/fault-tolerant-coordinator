#!/bin/zsh
# Bring up a real 3-node coordinator cluster: raft over HTTP between nodes,
# gRPC for clients, metrics on their own listeners.
set -e
D=/tmp/ftc-bench
PEERS="1=http://127.0.0.1:7001,2=http://127.0.0.1:7002,3=http://127.0.0.1:7003"
APIS="1=127.0.0.1:8081,2=127.0.0.1:8082,3=127.0.0.1:8083"
MAXPLAN=${MAXPLAN:-1000}
# SNAPSHOTS PUSHED OUT OF THE WINDOW, and disclosed as such.
#
# This build snapshots synchronously inside the Ready loop. At 10,000 tasks of
# state that cost leadership 9 times in a 30s window and drove commit latency
# to 322ms, so a batching measurement taken across it measures failover. The
# threshold is raised so no snapshot occurs during a measurement; the snapshot
# exposure itself is a separate, already-documented finding, and conflating
# the two would answer neither question.
SNAPENTRIES=${SNAPENTRIES:-500000}
for i in 1 2 3; do
  rm -rf $D/raft-$i
  $D/coordinator -id $i -addr 127.0.0.1:808$i -peers "$PEERS" -api-peers "$APIS" \
    -data-dir $D/raft-$i -metrics-addr 127.0.0.1:909$i -max-plan-size $MAXPLAN \
    -snapshot-entries $SNAPENTRIES \
    > $D/coord-$i.log 2>&1 &
  echo $! >> $D/pids
done
