import sys, re, glob, os
label, P, secs = sys.argv[1], sys.argv[2], float(sys.argv[3])
def load(pat):
    out = {}
    for p in sorted(glob.glob(pat)):
        d = {}
        for line in open(p):
            m = re.match(r'^([a-z_]+(?:\{[^}]*\})?)\s+([0-9.e+-]+)$', line.strip())
            if m: d[m.group(1)] = float(m.group(2))
        out[os.path.basename(p)] = d
    return out
A, B = load(f't0-{P}-*.txt'), load(f't1-{P}-*.txt')
# LEADER-ONLY metrics live on whichever node was leading, and leadership can
# move mid-run, so they are SUMMED across nodes. PER-NODE log metrics are the
# same work on every replica, so summing them would triple-count: those are
# read from the busiest node and labelled as per-node.
def s(k):
    return sum(B[n].get(k,0)-A.get(n,{}).get(k,0) for n in B)
def per_node(k):
    return max((B[n].get(k,0)-A.get(n,{}).get(k,0) for n in B), default=0)
plans, assigns = s('sched_plan_size_count'), s('sched_plan_size_sum')
acc, rej = s('sched_assignments_total'), s('sched_plan_rejections_total')
comp = s('sched_tasks_total{state="completed"}')
elections = s('raft_leader_elections_total')
print(f"\n=== plan-batching: {label} (max-plan-size={P}, {secs:.0f}s window) ===")
if elections > 1:
    print(f"*** CONTAMINATED: {elections:.0f} leadership changes inside the window. ***")
    print("    A run that re-elected mid-measurement measured failover, not batching.")
print(f"MEASURED plan size:     {assigns/plans if plans else 0:.2f} assignments/plan   <- the independent variable, proven")
print(f"ApplyPlan entries:      {plans:8.0f}   {plans/secs:9.1f}/s")
print(f"assignments committed:  {assigns:8.0f}   {assigns/secs:9.1f}/s   [HEADLINE]")
print(f"  took effect:          {acc:8.0f}   {acc/secs:9.1f}/s")
print(f"plan rejections:        {rej:8.0f}   {100*rej/(acc+rej) if acc+rej else 0:5.1f}% of assignments attempted")
print(f"task completions:       {comp:8.0f}   {comp/secs:9.1f}/s   [end-to-end; bounded by one CompleteTask entry each]")
print(f"leadership changes:     {elections:8.0f}")
ents = {k: per_node(k) for k in set().union(*[b.keys() for b in B.values()]) if k.startswith('raft_entries_total{')}
tot = sum(v for v in ents.values() if v>0)
print(f"raft entries, PER NODE  ({tot:.0f} total, {tot/secs:.1f}/s):")
for k,v in sorted(ents.items(), key=lambda kv:-kv[1]):
    if v>0:
        name = re.search(r'"([a-z_]+)"', k).group(1)
        print(f"    {name:<17}{v:8.0f}  {100*v/tot:5.1f}%")
c,t = per_node('raft_commit_latency_seconds_count'), per_node('raft_commit_latency_seconds_sum')
if c: print(f"commit latency mean:    {1000*t/c:.1f}ms over {c:.0f} commits (per node)")
