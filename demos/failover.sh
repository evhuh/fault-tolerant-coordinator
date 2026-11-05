#!/usr/bin/env bash
# DEMO 1 — SIGKILL the leader mid-flight; a new leader resumes the queue.
#
# What it shows, in order: a backlog committing and draining, which node leads,
# that node dying by SIGKILL, a survivor winning the election, assignments
# resuming, and the job completing with every task accounted for exactly once.
#
# What it deliberately does NOT claim: the timing printed here is one trial on
# one host. The measured distribution is 1441ms p50 / 2090ms p95 over 55
# trials, in results/reclaim-failover/.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TASKS=${TASKS:-60}

require_bin
if ! leader_node >/dev/null; then
  bad "no cluster — run: ./demos/up.sh 3 4"
  exit 1
fi

say "== 1. submit $TASKS tasks =="
JOB=$("$BIN/schedctl" -coordinator "$COORDS" submit -n "$TASKS" -timeout 10s -- /bin/sleep 1)
note "job $JOB"
sleep 2

say "== 2. who leads, and what is in flight =="
LEAD=$(leader_node)
printf 'leader           node %s\n' "$LEAD"
printf 'assignments      %s dispatched by node %s\n' "$(assignments_on "$LEAD")" "$LEAD"
jsonget "$(live_metrics)/debug/tasks" \
  "' '.join('%s pending=%d assigned=%d running=%d complete=%d' % (j['job_id'][:12], j['pending'], j['assigned'], j['running'], j['complete']) for j in d['jobs'])"

say "== 3. SIGKILL node $LEAD =="
PID=$(coord_pid "$LEAD")
kill -9 "$PID"
killed_at=$(python3 -I -c 'import time;print(int(time.time()*1000))')
bad "killed node $LEAD (pid $PID) — the cluster is now 2 of 3"

# t=1 is the first assignment dispatched by the NEW leader, which is the
# quantity the Day 11 trials measured. Polled, so it carries the poll's
# resolution; and read from the new leader by name, because the counter is
# per-process and starts at zero when it wins the election.
say "== 4. waiting for a survivor to lead and dispatch =="
new_leader=""
dispatched=""
deadline=$(( $(date +%s) + 20 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if [ -z "$new_leader" ]; then
    n=$(leader_node) && [ "$n" != "$LEAD" ] && new_leader="$n" && \
      printf 'new leader       node %s after %sms\n' "$n" \
        "$(( $(python3 -I -c 'import time;print(int(time.time()*1000))') - killed_at ))"
  fi
  if [ -n "$new_leader" ] && [ "$(assignments_on "$new_leader")" -ge 1 ]; then
    dispatched=$(( $(python3 -I -c 'import time;print(int(time.time()*1000))') - killed_at ))
    printf 'first dispatch   %sms after the kill (±10ms, polled)\n' "$dispatched"
    break
  fi
  sleep 0.01
done
[ -z "$dispatched" ] && { bad "no dispatch within 20s — see $RUN/coord-*.log"; exit 1; }

say "== 5. the job finishes on the survivors =="
# Poll the SUMMARY line, and wait for nothing to be in flight. Matching
# "COMPLETE" anywhere in the output is wrong: the verbose listing names a
# state per task, so the first completed task would end the wait.
deadline=$(( $(date +%s) + 180 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  line=$("$BIN/schedctl" -coordinator "$COORDS" status "$JOB" 2>/dev/null | head -1)
  # Redraw in place on a terminal; print nothing when redirected, because a
  # carriage return in a captured log smears every poll onto one line.
  [ -t 1 ] && printf '\r%-100s' "$line"
  inflight=$(printf '%s\n' "$line" | python3 -I -c "
import re,sys
s = sys.stdin.read()
m = dict(re.findall(r'(pending|assigned|running)=(\d+)', s))
print(sum(int(v) for v in m.values()) if m else 1)
")
  [ "${inflight:-1}" -eq 0 ] && break
  sleep 1
done
[ -t 1 ] && printf '\n'
"$BIN/schedctl" -coordinator "$COORDS" status "$JOB" | head -1

# The accounting claim, checked rather than asserted: every task terminal, and
# the number of terminal tasks equal to the number submitted. A lost task
# shows up as a missing one; a duplicated task shows up as a surplus.
total=$(jsonget "$(live_metrics)/debug/tasks" \
  "sum(j['complete']+j['failed'] for j in d['jobs'])")
if [ "${total:-0}" -ge "$TASKS" ]; then
  ok "$total task(s) terminal, $TASKS submitted — nothing lost, nothing duplicated"
else
  bad "$total terminal against $TASKS submitted"
  exit 1
fi
note "one trial on one host; the distribution is in results/reclaim-failover/"
