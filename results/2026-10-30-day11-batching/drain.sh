#!/bin/zsh
# Time-to-drain a FIXED backlog, sweeping -max-plan-size.
#
# Drain time rather than rate-in-a-window, because a window measurement is
# floor-limited the moment a config finishes early: the P=100 pilot drained
# all 1,000 tasks and then sat idle, so its 33.3/s was the backlog's rate and
# not the system's. A fixed backlog cannot be gamed that way.
#
# Usage: drain.sh <plan-size> <jobs> <tasks-per-job> <run-label>
set -e
D=/tmp/ftc-bench
P=$1; JOBS=$2; TPJ=$3; TAG=$4
TOTAL=$((JOBS*TPJ))
ADDRS=127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083

$D/down.sh >/dev/null 2>&1 || true
rm -f $D/d0-$P-$TAG-1.txt $D/d0-$P-$TAG-2.txt $D/d0-$P-$TAG-3.txt
MAXPLAN=$P SNAPENTRIES=500000 $D/up.sh
sleep 6
$D/loadgen -addrs $ADDRS -prefill $JOBS -tasks-per-job $TPJ -submitters 16 >/dev/null 2>&1
sleep 4
for i in 1 2 3; do curl -s 127.0.0.1:909$i/metrics | grep -E "^(sched_|raft_)" > $D/d0-$P-$TAG-$i.txt; done

START=$(python3 -c 'import time; print(time.time())')
for i in $(seq 1 8); do
  $D/worker -id w-$i -coordinator $ADDRS -concurrency 32 -data-dir $D/w-$i > $D/worker-$i.log 2>&1 &
  echo $! >> $D/wpids
done
# DRAINED = every task terminal. Polled from committed state on the leader,
# which is the same thing a client would see.
DEADLINE=$((SECONDS+300))
while [ $SECONDS -lt $DEADLINE ]; do
  depth=0
  for i in 1 2 3; do
    v=$(curl -s --max-time 2 127.0.0.1:909$i/metrics 2>/dev/null | grep -E '^sched_queue_depth' | awk '{s+=$NF} END {print s+0}')
    [ -n "$v" ] && [ "$v" != "0" ] && depth=$v
  done
  [ "$depth" = "0" ] && break
  sleep 0.5
done
END=$(python3 -c 'import time; print(time.time())')
for i in 1 2 3; do curl -s 127.0.0.1:909$i/metrics | grep -E "^(sched_|raft_)" > $D/d1-$P-$TAG-$i.txt; done
$D/down.sh >/dev/null 2>&1 || true
python3 -I -c "
import sys,re,glob,os
P,TAG,TOTAL=sys.argv[1],sys.argv[2],int(sys.argv[3])
elapsed=float(sys.argv[5])-float(sys.argv[4])
def load(pat):
    out={}
    for p in sorted(glob.glob(pat)):
        d={}
        for line in open(p):
            m=re.match(r'^([a-z_]+(?:\{[^}]*\})?)\s+([0-9.e+-]+)\$',line.strip())
            if m: d[m.group(1)]=float(m.group(2))
        out[os.path.basename(p)]=d
    return out
A,B=load(f'/tmp/ftc-bench/d0-{P}-{TAG}-*.txt'),load(f'/tmp/ftc-bench/d1-{P}-{TAG}-*.txt')
def s(k): return sum(B[n].get(k,0)-A.get(n.replace('d1','d0'),{}).get(k,0) for n in B)
def pn(k): return max((B[n].get(k,0)-A.get(n.replace('d1','d0'),{}).get(k,0) for n in B),default=0)
plans,assigns=s('sched_plan_size_count'),s('sched_plan_size_sum')
rej,comp=s('sched_plan_rejections_total'),s('sched_tasks_total{state=\"completed\"}')
el=s('raft_leader_elections_total')
ents={k:pn(k) for k in set().union(*[b.keys() for b in B.values()]) if k.startswith('raft_entries_total{')}
tot=sum(v for v in ents.values() if v>0)
print(f'RUN P={P} {TAG}: drain={elapsed:.1f}s planSize={assigns/plans if plans else 0:.2f} '
      f'plans={plans:.0f} assigns={assigns:.0f} completed={comp:.0f}/{TOTAL} rej={rej:.0f} '
      f'entries={tot:.0f} elections={el:.0f}')
print('   entries: '+' '.join(f'{re.search(chr(34)+chr(91)+chr(97)+chr(45)+chr(122)+chr(95)+chr(93)+chr(43)+chr(34),k).group(0).strip(chr(34))}={v:.0f}' for k,v in sorted(ents.items(),key=lambda kv:-kv[1]) if v>0))
" "$P" "$TAG" "$TOTAL" "$START" "$END"
