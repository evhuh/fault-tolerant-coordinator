# DEMO 2 — SIGKILL a worker mid-task.
#
#   vhs demos/reclaim.tape        (run from the repository root)
#
# Two workers, not three: the demo asserts that a free slot exists somewhere
# other than the victim, and with two workers that assertion is visibly about
# the one worker left rather than about a crowd.

Output demos/reclaim.gif

Require bash
Require curl
Require python3

Set Shell bash
Set FontSize 15
Set Width 1300
Set Height 760
Set Padding 18
Set TypingSpeed 25ms
Set WaitTimeout 180s
Set WaitPattern /\$ $/

Hide
Type "cd $(git rev-parse --show-toplevel) && clear" Enter
Type "make bin >/dev/null && ./demos/down.sh >/dev/null 2>&1; ./demos/up.sh 2 2" Enter
Wait
Type "clear" Enter
Show

Type "# A worker is RUNNING a 45s task. SIGKILL it and watch the lease expire."
Enter
Sleep 2s

Type "./demos/reclaim.sh"
Enter
Wait
Sleep 4s

Hide
Type "./demos/down.sh" Enter
Wait
