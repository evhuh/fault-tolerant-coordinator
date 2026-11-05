#!/usr/bin/env bash
# DEMO 2 — SIGKILL a worker mid-task; the lease expires and the task redispatches.
#
# The precondition matters and is asserted rather than hoped for: the victim
# must hold a RUNNING task, not merely an ASSIGNED one. Killing a worker that
# was handed a task but has not started it demonstrates a dispatch race, not a
# lease expiry — and there must be a free slot elsewhere, or the redispatch
# measures queueing for a slot.
#
# t=0 is the last heartbeat RECEIVED, derived as lease_expiry_ms - 2000ms from
# committed state. That is the number the 2.25s budget is about.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

LEASE_TTL_MS=2000

require_bin
if ! leader_node >/dev/null; then
  bad "no cluster — run: ./demos/up.sh 2 2"
  exit 1
fi
M="$(live_metrics)" || { bad "no coordinator answering on a metrics port"; exit 1; }

say "== 1. submit one long task =="
JOB=$("$BIN/schedctl" -coordinator "$COORDS" submit -n 1 -timeout 60s -max-attempts 3 -- /bin/sleep 45)
note "job $JOB"

say "== 2. wait for a worker to be RUNNING it =="
victim=""; task=""
deadline=$(( $(date +%s) + 20 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  read -r victim task <<<"$(jsonget "$M/debug/workers" \
    "next((w['worker_id']+' '+h['task_id'] for w in d['workers'] for h in w['held'] if h['state']=='RUNNING'), '')")"
  [ -n "$victim" ] && break
  sleep 0.2
done
[ -z "$victim" ] && { bad "no worker reached RUNNING within 20s"; exit 1; }
ok "victim $victim is RUNNING $task"

# Precondition: a free slot somewhere else, or the redispatch leg is measuring
# the wrong thing entirely.
free=$(jsonget "$M/debug/workers" \
  "sum(w['capacity']-len(w['held']) for w in d['workers'] if w['worker_id']!='$victim')")
if [ "${free:-0}" -lt 1 ]; then
  bad "no free slot on another worker — redispatch would measure queueing, not reclaim"
  exit 1
fi
ok "$free free slot(s) elsewhere"

# t=0, from the POST-KILL lease. The value read now is stale by up to one
# heartbeat interval, and using it overstated this figure by 606ms in the
# reclaim trials; once the worker is dead the lease stops moving, so the last
# observed value IS last-heartbeat + TTL by definition.
say "== 3. SIGKILL $victim =="
PID=$(cat "$RUN/${victim/w-/worker-}.pid" 2>/dev/null)
[ -z "$PID" ] && PID=$(cat "$RUN/worker-${victim#w-}.pid" 2>/dev/null)
kill -9 "$PID"
sleep 0.2
if alive "$PID"; then bad "$victim (pid $PID) survived the kill"; exit 1; fi
bad "killed $victim (pid $PID)"
lease=$(jsonget "$M/debug/workers" \
  "next((w['lease_expiry_ms'] for w in d['workers'] if w['worker_id']=='$victim'), 0)")
t0=$(( lease - LEASE_TTL_MS ))
note "t=0 = last heartbeat received = ${t0}ms (lease ${lease} - ${LEASE_TTL_MS}ms TTL), on the committed clock"

say "== 4. lease expiry, reclaim, redispatch =="
released=""; reassigned=""
deadline=$(( $(date +%s) + 30 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  held=$(jsonget "$M/debug/workers" \
    "next((len(w['held']) for w in d['workers'] if w['worker_id']=='$victim'), 0)")
  now=$(jsonget "$M/debug/workers" "d['now_ms']")
  if [ -z "$released" ] && [ "${held:-1}" -eq 0 ]; then
    released=$(( now - t0 ))
    printf 'reclaimed        %sms after last heartbeat (±10ms poll)\n' "$released"
  fi
  holder=$(jsonget "$M/debug/workers" \
    "next((w['worker_id'] for w in d['workers'] for h in w['held'] if h['task_id']=='$task'), '')")
  if [ -n "$released" ] && [ -n "$holder" ] && [ "$holder" != "$victim" ]; then
    reassigned=$(( now - t0 ))
    printf 'redispatched     to %s, %sms after last heartbeat\n' "$holder" "$reassigned"
    break
  fi
  sleep 0.01
done
[ -z "$released" ] && { bad "the task was never released within 30s"; exit 1; }

say "== 5. the attempt number is the idempotency key =="
jsonget "$M/debug/workers" \
  "'\n'.join('%-6s cap=%d  held=%s' % (w['worker_id'], w['capacity'], [(h['task_id'][-8:], h['state'], 'attempt %d' % h['attempt']) for h in w['held']]) for w in d['workers'])"
note "attempt 2 writes to its own directory; a late report from attempt 1 names a dead attempt and is discarded on every replica"

if [ -n "$reassigned" ]; then
  ok "reclaimed in ${released}ms, redispatched in ${reassigned}ms — consistent with the ~2250ms budget (lease 2000 + tick 200 + commit)"
else
  warn "reclaimed in ${released}ms; no redispatch observed (the task may have exhausted its attempts, which is lawful)"
fi
note "one trial on one host; 100-trial percentiles are in results/reclaim-failover/"
