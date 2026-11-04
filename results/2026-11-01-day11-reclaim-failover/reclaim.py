#!/usr/bin/env python3
"""Worker reclaim latency: SIGKILL a worker mid-task, 100 trials.

t=0 is the LAST HEARTBEAT RECEIVED, read exactly from /debug/workers as
lease_expiry_ms - leaseTTL. Reported two ways, because they answer different
questions:

  from last heartbeat  -- what the ~2.25s budget is about (lease 2000ms +
                          tick 200ms + commit). The clock starts when the
                          cluster last heard from the worker, which is where
                          a lease-based claim has to start.
  from the kill        -- what a user sees. Up to one heartbeat period
                          (500ms) shorter, because the worker was alive for
                          part of the lease it had already renewed.

Both carry 10ms measurement error: the state is polled, not pushed.

Reclaim-to-reassigned needs a FREE SLOT at reclaim time. With four
concurrency-1 workers all running sleep-30, every survivor is busy and the
reassignment waits on a slot rather than on the reclaim — so a spare idle
worker is kept with free capacity, and each trial records whether a free slot
actually existed at t=1.
"""
import json, os, signal, subprocess, sys, time, urllib.request

D = "/tmp/ftc-bench"
NODES = [1, 2, 3]
ADDRS = ",".join(f"127.0.0.1:808{i}" for i in NODES)
LEASE_TTL_MS = 2000
# The worker heartbeats every 500ms (-heartbeat default), which is the
# interval the kill-took-effect check waits out.
HEARTBEAT_S = 0.5
# Sorts after every "v-*" victim, which is the point. See start-up comment.
SPARE = "z-spare"
# Total non-terminal tasks allowed in the cluster: one per victim plus a
# margin, so the spare always has room for a reassignment.
CEILING = 6
TRIALS = int(os.environ.get("TRIALS", "100"))
POLL = 0.01

def get(url, timeout=2):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)

def leader_debug():
    for i in NODES:
        try:
            d = get(f"http://127.0.0.1:909{i}/debug/workers")
            if d.get("is_leader"):
                return i, d
        except Exception:
            pass
    return None, None

def probe():
    out = subprocess.run([f"{D}/fsyncprobe", "-json", "-n", "20", "-dir", D],
                         capture_output=True, text=True)
    try:
        return json.loads(out.stdout)
    except Exception:
        return {"error": out.stderr.strip()[:200]}

def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)

def start_worker(wid, conc, cmdpath):
    # The pid goes into wpids as well, so down.sh can clean up workers this
    # script started. Without that they SURVIVE the run, stay registered, and
    # the next run's victim selection picks a process this script does not
    # control — so the kill lands on the wrong pid, the real worker keeps
    # heartbeating, the lease never expires and nothing is ever reclaimed.
    p = subprocess.Popen(
        [f"{D}/worker", "-id", wid, "-coordinator", ADDRS, "-concurrency", str(conc),
         "-data-dir", f"{D}/wr-{wid}"],
        stdout=open(f"{D}/reclaim-{wid}.log", "a"), stderr=subprocess.STDOUT)
    with open(f"{D}/wpids", "a") as f:
        f.write(f"{p.pid}\n")
    return p

def main():
    # STALE WORKERS FIRST. A registration left over from an earlier run is
    # indistinguishable from a live one in committed state, and it takes the
    # tasks the victims were supposed to hold.
    # MATCH THE PROCESS NAME, not a path. pkill -f "ftc-bench/worker" matched
    # NOTHING, because workers are launched as ./worker from that directory
    # and the pattern never appears in the command line. The consequence was
    # not a tidiness problem: stale workers from earlier runs stayed
    # registered and kept heartbeating under the same ids, so the kill landed
    # on a process this script controlled while another renewed the lease.
    # Every trial then timed out waiting for a reclaim that could not happen.
    # A SWEEP, not the kill mechanism. Trials kill the pid they recorded;
    # this only clears what an earlier run leaked, and it says what it found
    # rather than clearing silently — a leaked worker keeps heartbeating
    # under its old id and quietly invalidates every trial that reuses it.
    leaked = sh('pgrep -x worker').stdout.split()
    if leaked:
        print(f"WARNING: {len(leaked)} leaked worker(s) from an earlier run: {leaked}", flush=True)
    sh('pkill -x worker')
    sh(f"{D}/down.sh")
    time.sleep(1)
    # A long task, so the victim is RUNNING something when it dies. With
    # /bin/true there is nothing to reclaim and the trial measures nothing.
    sleep_bin = sh("command -v sleep").stdout.strip() or "/bin/sleep"
    env = dict(os.environ, MAXPLAN="100", SNAPENTRIES="500000")
    subprocess.run([f"{D}/up.sh"], env=env, check=True)
    time.sleep(6)

    # Victims are concurrency-1 so a kill always loses exactly one task.
    # The SPARE is concurrency-4 and deliberately kept idle-ish, so a
    # reclaimed task has somewhere to go.
    victims = [f"v-{i}" for i in range(4)]
    procs = {}
    for wid in victims:
        procs[wid] = start_worker(wid, 1, sleep_bin)
    # The SPARE holds the free capacity a reassignment needs, and it is named
    # to sort AFTER the victims.
    #
    # pickWorker starts at index 0 of the sorted worker ids on every plan, so
    # with one assignment per plan the first id with room always wins. Named
    # "spare" it sorted before "v-0" and absorbed every task: 36 of 57 trials
    # were INVALID for having no victim holding anything.
    procs[SPARE] = start_worker(SPARE, 16, sleep_bin)
    time.sleep(3)

    rows = []
    for trial in range(1, TRIALS + 1):
        # Keep a backlog of long tasks so victims always hold one.
        # THE IN-FLIGHT SET IS BOUNDED, and topped up only when it has
        # drained. Submitting a fixed batch per trial fills the spare with
        # 30-second tasks within two trials, after which a reassignment waits
        # on a slot instead of on the reclaim: measured as 27s and 28s with
        # free_slots_at_pending = 0.
        #
        # sleep also NEEDS AN ARGUMENT. Without one it exits non-zero
        # instantly, the task fails instead of running, and no victim ever
        # holds anything — which excluded every trial of the first version.
        # Topped up on whether VICTIMS are busy, not on total in-flight. The
        # total includes whatever the spare is holding, so a spare with a few
        # 30-second tasks satisfies the bound while every victim sits idle —
        # which excluded a trial for having no task to lose.
        # IN-FLIGHT WORK IS BOUNDED, not merely topped up.
        #
        # Submitting per idle victim over-submits: the tasks sleep 30s, so
        # they accumulate on the spare and its free capacity decays. Measured
        # that way: free slots fell 14, 11, 8, 5, 2, 0 across eight trials and
        # a reassignment took 13.7s waiting for a slot rather than for the
        # reclaim. The ceiling keeps the spare's room available, which is the
        # condition the reassignment leg depends on.
        node, d = leader_debug()
        inflight = -1
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:909{node}/debug/tasks", timeout=3) as r:
                inflight = sum(j.get("pending", 0) + j.get("assigned", 0) + j.get("running", 0)
                               for j in (json.load(r).get("jobs") or []))
        except Exception:
            pass
        busy_victims = 0
        if d:
            byid = {w["worker_id"]: w for w in d["workers"]}
            busy_victims = sum(1 for v in victims if (byid.get(v, {}).get("held_task_ids") or []))
        topup = None
        if 0 <= inflight < CEILING:
            # Enough for every idle victim plus headroom. Submitting only
            # when fewer than two are busy left 56 of 100 trials with no
            # victim holding anything: a task that goes terminal on its
            # attempt budget is consumed, and the condition stayed satisfied
            # while the victims emptied one by one.
            want = CEILING - inflight
            t_sub = time.time() * 1000
            out = sh(f"{D}/loadgen -addrs {ADDRS} -prefill 1 -tasks-per-job {want} "
                     f"-command {sleep_bin} -task-args 30 -task-timeout 60s "
                     f"-submitters 4 2>&1")
            topup = {"at_wall_ms": t_sub, "tasks_requested": want,
                     "busy_victims_before": busy_victims, "inflight_before": inflight,
                     "exit": out.returncode, "tail": out.stdout.strip()[-200:]}
            if out.returncode != 0:
                # loadgen exits non-zero when a submission created no new
                # work, so the harness stops attributing its own no-op to the
                # system.
                print(f"trial {trial}/{TRIALS}: top-up FAILED: {topup['tail']}", flush=True)
        # Wait for a victim to hold a RUNNING task, and note the spare's room.
        # Victim, t=0 and the task are all taken from ONE read. Re-reading
        # after choosing is a race: the held list can empty between the two
        # reads, and the first version of this harness crashed on exactly
        # that.
        victim = t0 = task = None
        spare_room = 0
        deadline = time.time() + 20
        while time.time() < deadline:
            node, d = leader_debug()
            if not d:
                time.sleep(POLL); continue
            byid = {w["worker_id"]: w for w in d["workers"]}
            spare = byid.get(SPARE) or {}
            spare_room = (spare.get("capacity") or 0) - len(spare.get("held_task_ids") or [])
            for wid in victims:
                w = byid.get(wid)
                if not w or w["state"] != "ALIVE":
                    continue
                # PRECONDITION: a RUNNING task, not merely an assigned one.
                # Killing a worker that has been handed a task but has not
                # started it measures a dispatch race, not a lease expiry.
                running = [h for h in (w.get("held") or []) if h["state"] == "RUNNING"]
                if not running:
                    continue
                victim = wid
                t0 = w["lease_expiry_ms"] - LEASE_TTL_MS
                task = running[0]["task_id"]
                break
            if victim:
                break
            time.sleep(POLL)
        if not victim:
            # THE EXCLUSION EXPLAINS ITSELF. "The harness had nothing to kill"
            # and "every task had exhausted its attempts" look identical from
            # outside and have different fixes, so record both.
            node, dw = leader_debug()
            procs_alive = {w: (procs[w].poll() is None) for w in victims + [SPARE]}
            tasks_view = None
            try:
                with urllib.request.urlopen(
                        f"http://127.0.0.1:909{node}/debug/tasks", timeout=3) as r:
                    tasks_view = json.load(r)
            except Exception as e:
                tasks_view = {"error": str(e)[:200]}
            workers_view = []
            for w in (dw or {}).get("workers", []):
                held = w["held_task_ids"] or []
                workers_view.append({"id": w["worker_id"], "state": w["state"],
                                     "held": held, "capacity": w["capacity"],
                                     "free": max(0, w["capacity"] - len(held))})
            row = {"trial": trial, "excluded": "no victim held a task within 20s",
                   "topup": topup,
                   "process_alive": procs_alive,
                   "leader": node,
                   "now_ms": (dw or {}).get("now_ms"),
                   "workers": workers_view,
                   "tasks": tasks_view}
            with open(f"{D}/reclaim-trials.jsonl", "a") as f:
                f.write(json.dumps(row) + "\n")
            print(f"trial {trial}/{TRIALS}: EXCLUDED, no victim held a task", flush=True)
            continue

        # PRECONDITION: somewhere other than the victim has room, or the
        # reassignment leg measures queueing for a slot rather than the
        # reclaim itself.
        free_elsewhere = 0
        for w in (d or {}).get("workers", []):
            if w["state"] == "ALIVE" and w["worker_id"] != victim:
                free_elsewhere += max(0, w["capacity"] - len(w["held_task_ids"] or []))
        if free_elsewhere < 1:
            row = {"trial": trial, "victim": victim, "free_elsewhere": free_elsewhere,
                   "invalid": "precondition: no free slot anywhere but the victim, so a "
                              "reassignment would measure queueing rather than reclaim"}
            with open(f"{D}/reclaim-trials.jsonl", "a") as f:
                f.write(json.dumps(row) + "\n")
            print(f"trial {trial}/{TRIALS}: INVALID — no spare slot elsewhere", flush=True)
            time.sleep(1.0)
            continue

        before = probe()

        # SIGKILL, not SIGTERM: a clean shutdown would deregister and skip the
        # lease path entirely, which is the path under test.
        victim_pid = procs[victim].pid
        procs[victim].send_signal(signal.SIGKILL)
        procs[victim].wait()
        kill_wall = time.time() * 1000

        # THE KILL MUST BE PROVEN, not assumed. Two independent checks, each
        # of which the pkill bug would have failed:
        #
        #   (a) no process is still running with this worker's -id. The
        #       recorded pid is dead by construction after wait(), but a
        #       STALE process under the same id is exactly what went wrong.
        #   (b) the lease stops advancing. One heartbeat interval after the
        #       kill, lease_expiry_ms must be unchanged; if it moved, someone
        #       is still renewing and the trial is measuring nothing.
        #
        # A trial failing either is INVALID — reported with its reason, never
        # dropped quietly.
        survivors = sh(f'pgrep -f "[-]id {victim}"').stdout.split()
        _, d_a = leader_debug()
        lease_a = next((w["lease_expiry_ms"] for w in (d_a or {}).get("workers", [])
                        if w["worker_id"] == victim), None)
        time.sleep(HEARTBEAT_S)
        _, d_b = leader_debug()
        lease_b = next((w["lease_expiry_ms"] for w in (d_b or {}).get("workers", [])
                        if w["worker_id"] == victim), None)
        # t=0 IS RE-DERIVED FROM THE POST-KILL LEASE, which is exact.
        #
        # The lease read at victim-selection time is stale: the worker
        # heartbeats every 500ms and the probe between selection and kill
        # takes time, so it renewed again before dying. Using that reading
        # overstated reclaim-from-last-heartbeat by up to one heartbeat
        # interval — measured as p50 2522ms against a from-kill p50 of
        # 1916ms, a 606ms gap that is one heartbeat plus poll.
        #
        # Once the worker is dead the lease stops moving, so the last value
        # observed IS last-heartbeat + leaseTTL, by definition.
        final_lease = lease_b if lease_b is not None else lease_a
        if final_lease is not None:
            t0 = final_lease - LEASE_TTL_MS

        invalid = None
        if survivors:
            invalid = f"a process with -id {victim} survived the kill: pids {survivors}"
        elif lease_a is not None and lease_b is not None and lease_b > lease_a:
            invalid = (f"lease still advancing after the kill "
                       f"({lease_a} -> {lease_b}); something is renewing it")
        if invalid:
            row = {"trial": trial, "victim": victim, "invalid": invalid,
                   "killed_pid": victim_pid, "probe_before": before}
            with open(f"{D}/reclaim-trials.jsonl", "a") as f:
                f.write(json.dumps(row) + "\n")
            print(f"trial {trial}/{TRIALS}: INVALID — {invalid}", flush=True)
            procs[victim] = start_worker(victim, 1, sleep_bin)
            time.sleep(1.0)
            continue

        # t=1a: the task is PENDING again (the state-machine event the budget
        # is about). t=1b: it has been reassigned (what a user sees).
        # DETECT THE RELEASE, not the PENDING state.
        #
        # The first version waited to observe the task held by nobody, which
        # requires catching the gap between reclaim and reassignment. That gap
        # is about 15ms — measured as 1687ms vs 1702ms on a trial that did
        # catch it — so at a 10ms poll it is missed more often than not, and
        # every trial recorded None.
        #
        # "Reclaimed" is therefore defined as the task no longer being
        # attributed to the dead worker, which is observable whatever the
        # planner does next. The two timestamps can legitimately coincide
        # within one poll.
        t_released = t_reassigned = None
        free_slot_at_release = None
        deadline = time.time() + 30
        while time.time() < deadline and (t_released is None or t_reassigned is None):
            node, d = leader_debug()
            if not d:
                time.sleep(POLL); continue
            now = time.time() * 1000
            holders = [w["worker_id"] for w in d["workers"] if task in (w["held_task_ids"] or [])]
            if t_released is None and victim not in holders:
                t_released = now
                room = 0
                for w in d["workers"]:
                    if w["state"] == "ALIVE" and w["worker_id"] != victim:
                        room += max(0, w["capacity"] - len(w["held_task_ids"] or []))
                free_slot_at_release = room
            if t_reassigned is None and [h for h in holders if h != victim]:
                t_reassigned = now
            time.sleep(POLL)
        t_pending = t_released
        free_slot_at_pending = free_slot_at_release

        after = probe()
        # A TIMEOUT MUST EXPLAIN ITSELF. Capturing the leader's view and the
        # queue depths at the moment of failure is what turns "reclaim=None"
        # into a diagnosis, instead of another run with print statements
        # bolted on.
        timeout_state = None
        if t_released is None or t_reassigned is None:
            n2, d2 = leader_debug()
            depth = ""
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:909{n2}/metrics", timeout=2) as r:
                    depth = "; ".join(l for l in r.read().decode().splitlines()
                                      if l.startswith("sched_queue_depth")
                                      or l.startswith("sched_tasks_total"))
            except Exception as e:
                depth = f"metrics unavailable: {e}"
            timeout_state = {"leader": n2, "workers": (d2 or {}).get("workers"),
                             "now_ms": (d2 or {}).get("now_ms"), "metrics": depth}
        rows.append({
            "trial": trial, "victim": victim, "task": task, "killed_pid": victim_pid,
            "last_heartbeat_committed_ms": t0,
            "kill_wall_ms": kill_wall,
            "pending_wall_ms": t_pending, "reassigned_wall_ms": t_reassigned,
            # Both clocks are Unix ms: Tick proposes time.Now().UnixMilli(),
            # so the committed clock and wall clock are the same scale,
            # differing by at most one tick period.
            "reclaim_from_last_heartbeat_ms": (t_pending - t0) if t_pending else None,
            "reassign_from_last_heartbeat_ms": (t_reassigned - t0) if t_reassigned else None,
            "reclaim_from_kill_ms": (t_pending - kill_wall) if t_pending else None,
            "reassign_from_kill_ms": (t_reassigned - kill_wall) if t_reassigned else None,
            "free_slots_at_release": free_slot_at_pending,
            "free_elsewhere_at_kill": free_elsewhere,
            "spare_room_at_kill": spare_room,
            "probe_before": before, "probe_after": after,
            "timeout_state": timeout_state,
        })
        with open(f"{D}/reclaim-trials.jsonl", "a") as f:
            f.write(json.dumps(rows[-1]) + "\n")
        print(f"trial {trial}/{TRIALS}: victim={victim} "
              f"reclaim={rows[-1]['reclaim_from_kill_ms']} "
              f"reassign={rows[-1]['reassign_from_kill_ms']} "
              f"freeSlots={free_slot_at_pending}", flush=True)
        procs[victim] = start_worker(victim, 1, sleep_bin)
        time.sleep(1.0)

    for p in procs.values():
        try:
            p.send_signal(signal.SIGKILL)
        except Exception:
            pass
    leaked = sh('pgrep -x worker').stdout.split()
    if leaked:
        print(f"WARNING: {len(leaked)} worker(s) leaked by this run: {leaked}", flush=True)
    sh('pkill -x worker')
    sh(f"{D}/down.sh")
    print(f"wrote {D}/reclaim-trials.jsonl")

main()
