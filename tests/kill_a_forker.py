"""Kill a process that keeps forking, the way the memory watchdog kills a branch.

Run by ``test_memory.py`` as a script in a process of its own: it becomes a
child subreaper, as a sandbox's init adopts orphans, starts a forker, samples
it, lets it fork on, kills the branch, and prints how many processes were
sampled, how many were killed and how many of its descendants still live.
"""

from __future__ import annotations

import ctypes
import os
import subprocess
import sys
import time

from hpc_sandbox import memory, procfs

PR_SET_CHILD_SUBREAPER = 36
FORKER = """
import os, time
while True:
    if os.fork() == 0:
        time.sleep(60)
        os._exit(0)
    time.sleep(0.002)
"""


def main() -> None:
    ctypes.CDLL(None, use_errno=True).prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
    forker = subprocess.Popen([sys.executable, "-c", FORKER])
    try:
        time.sleep(0.2)
        since = memory._now_ticks()
        processes = memory.sample(None, [forker.pid])
        time.sleep(0.2)  # it keeps forking
        sampled = {process.identity for process in processes}
        owners = {os.getpid(): procfs.start_time(os.getpid())}
        sessions = {process.session for process in processes}
        killed = memory._kill_branch(processes, sampled, owners, since, sessions)
        forker.wait(timeout=10)
        time.sleep(0.5)
        left = [
            pid for pid in procfs.descendants([os.getpid()]) if procfs.owned_live_start_time(pid)
        ]
        print(len(processes), killed, len(left))
    finally:
        forker.kill()
        for pid in procfs.descendants([os.getpid()]):
            try:
                os.kill(pid, 9)
            except ProcessLookupError:
                pass
        while True:
            try:
                os.waitpid(-1, 0)
            except ChildProcessError:
                break


if __name__ == "__main__":
    main()
