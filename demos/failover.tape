# DEMO 1 — SIGKILL the leader mid-flight.
#
#   vhs demos/failover.tape        (run from the repository root)
#
# Needs vhs >= 0.7 for the `Wait` command, which holds the recording until the
# shell prompt returns. The alternative is a fixed `Sleep`, and a fixed sleep
# either truncates the demo on a slow run or pads every GIF with dead air.

Output demos/failover.gif

Require bash
Require curl
Require python3

Set Shell bash
Set FontSize 15
Set Width 1300
Set Height 860
Set Padding 18
Set TypingSpeed 25ms
Set WaitTimeout 240s
Set WaitPattern /\$ $/

# Setup is real but not interesting: build, start three coordinators, start
# three workers. Hidden so the recording opens on the demo itself.
Hide
Type "cd $(git rev-parse --show-toplevel) && clear" Enter
Type "make bin >/dev/null && ./demos/down.sh >/dev/null 2>&1; ./demos/up.sh 3 4" Enter
Wait
Type "clear" Enter
Show

Type "# 3 coordinators, 3 workers, 12 slots. Kill the leader mid-flight."
Enter
Sleep 2s

Type "TASKS=24 ./demos/failover.sh"
Enter
Wait
Sleep 4s

Hide
Type "./demos/down.sh" Enter
Wait
