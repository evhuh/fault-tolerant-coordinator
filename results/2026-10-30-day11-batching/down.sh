#!/bin/zsh
D=/tmp/ftc-bench
[ -f $D/pids ] && while read p; do kill $p 2>/dev/null; done < $D/pids
[ -f $D/wpids ] && while read p; do kill $p 2>/dev/null; done < $D/wpids
rm -f $D/pids $D/wpids
sleep 1
