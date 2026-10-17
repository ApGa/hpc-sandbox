"""The memory watchdog's choice: the subtree the memory leads down to, so a harness survives the command that ran
away however that command spread its memory, and a small process beside the memory survives a small excess."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import make_provider

from hpc_sandbox import SandboxSpec
from hpc_sandbox.memory import MemoryKill, _Process, choose_victim

MB = 1024 * 1024
PYTHON = "/usr/bin/python3"


def tree(*rows: tuple[int, int | None, int], protected: tuple[int, ...] = ()) -> list[_Process]:
    """Processes from (pid, parent, MB) rows, parents first."""
    return [
        _Process(pid, str(pid), size * MB, None, pid in protected, parent)
        for pid, parent, size in rows
    ]


def test_a_pool_of_workers_is_killed_with_its_parent_and_the_harness_survives():
    """The G3 shape: a 257 MB harness ran a command that started 192 workers of 240 MB, each smaller than the
    harness. The largest process is the harness; the subtree that holds the excess is the command's."""
    workers = [(100 + i, 3, 240) for i in range(192)]
    processes = tree((1, None, 1), (2, 1, 257), (3, 2, 60), *workers, protected=(1,))
    kill = choose_victim(processes, 3072 * MB)
    assert kill is not None and kill.pid == 3
    assert kill.pids == (3, *(100 + i for i in range(192)))
    assert kill.pss_bytes == (60 + 192 * 240) * MB
    assert "killed pid 3 and its 192 descendants" in kill.message()


def test_a_single_hog_is_killed_alone():
    processes = tree((1, None, 1), (2, 1, 250), (3, 2, 4000), (4, 2, 30), protected=(1,))
    kill = choose_victim(processes, 3072 * MB)
    assert kill is not None and (kill.pid, kill.pids) == (3, (3,))
    assert kill.message().endswith(f"killed pid 3 using {4000 * MB} bytes")


def test_a_harness_that_holds_the_excess_itself_is_killed_with_what_it_started():
    processes = tree((1, None, 1), (2, 1, 4000), (3, 2, 30), protected=(1,))
    kill = choose_victim(processes, 3072 * MB)
    assert kill is not None and kill.pids == (2, 3)


def test_memory_spread_over_independent_trees_kills_the_largest_and_samples_again():
    # Orphans under the protected init: no one subtree holds the 1301 MB excess.
    processes = tree((1, None, 1), (2, 1, 1000), (3, 1, 1200), (4, 1, 1100), protected=(1,))
    kill = choose_victim(processes, 2000 * MB)
    assert kill is not None and kill.pids == (3,)
    # With a smaller excess, which each of them holds, still the largest.
    kill = choose_victim(processes, 3000 * MB)
    assert kill is not None and kill.pids == (3,)


def test_a_small_excess_kills_the_process_that_holds_the_memory():
    """At the first sample over the limit the excess is often small, and fits in any small process: the keep-alive
    loop's sleep, a sleep in the command's pipeline. The memory is in the hog. (A real Singularity instance and exec:
    sinit and the keep-alive loop are protected; PSS in KiB.)"""
    rows = [
        (1, None, 19348, None, True),  # sinit
        (2, 1, 289, None, True),  # /bin/sh /.singularity.d/startscript
        (3, 2, 259, None, False),  # sleep 86400
        (10, None, 9416, 10, False),  # Singularity runtime parent (the exec)
        (11, 10, 19870, 10, False),  # the command, a shell holding 20 MB
        (12, 11, 252, 10, False),  # sleep 25, in its pipeline
        (13, 11, 97951, 10, False),  # tail -c 100000000, the hog
    ]
    processes = [
        _Process(pid, str(pid), kib * 1024, exec_root, protected, parent)
        for pid, parent, kib, exec_root, protected in rows
    ]
    total = sum(process.pss_bytes for process in processes)
    for excess_kib in (4, 64, 256, 1024, 65536):
        kill = choose_victim(processes, total - excess_kib * 1024)
        assert kill is not None and (kill.pids, kill.exec_root) == ((13,), 10), excess_kib


def test_a_bystander_smaller_than_the_hog_survives_a_small_excess():
    # A hog growing under a shell, next to an independent 300 MB process, first sampled 50 MB over the limit.
    processes = tree((1, None, 1), (2, 1, 2), (3, 2, 2800), (4, 1, 300), protected=(1,))
    kill = choose_victim(processes, 3053 * MB)
    assert kill is not None and kill.pids == (3,)


def test_of_two_hogs_under_a_harness_one_goes_and_the_harness_stays():
    processes = tree((1, None, 1), (2, 1, 200), (3, 2, 2000), (4, 2, 1900), protected=(1,))
    kill = choose_victim(processes, 4000 * MB)
    assert kill is not None and kill.pids == (3,)


def test_protected_processes_are_never_chosen_and_nothing_under_the_limit_is():
    assert choose_victim(tree((1, None, 9000), protected=(1,)), 100 * MB) is None
    assert choose_victim(tree((1, None, 1), (2, 1, 50)), 100 * MB) is None
    processes = tree((1, None, 9000), (2, 1, 10), protected=(1,))
    kill = choose_victim(processes, 100 * MB)
    assert kill is not None and kill.pids == (2,)


def test_a_kill_reports_its_exec():
    processes = [
        _Process(10, "10", 1 * MB, 10, False, None),
        _Process(11, "11", 900 * MB, 10, False, 10),
    ]
    kill = choose_victim(processes, 500 * MB)
    assert kill == MemoryKill(11, 900 * MB, 901 * MB, 500 * MB, 10, (11,))


HARNESS = """
import os, subprocess, sys, time
print("harness", os.getpid(), flush=True)
held = bytearray(120 * 1024 * 1024)  # the harness: larger than any one worker below
for i in range(0, len(held), 4096):
    held[i] = 1
command = subprocess.run([sys.executable, "-c", sys.argv[1]])
print("harness alive, command exit", command.returncode, flush=True)
"""
POOL = """
import os, time
pids = []
for _ in range(4):
    pid = os.fork()
    if pid == 0:
        data = bytearray(60 * 1024 * 1024)
        for i in range(0, len(data), 4096):
            data[i] = 1
        time.sleep(60)
        os._exit(0)
    pids.append(pid)
for pid in pids:
    os.waitpid(pid, 0)
"""


@pytest.mark.skipif(not Path(PYTHON).exists(), reason=f"needs {PYTHON}")
async def test_a_harness_survives_the_pool_its_command_started(tmp_path, fake_runtime, sif):
    provider = make_provider(tmp_path)
    sandbox = await provider.start(SandboxSpec(image=str(sif), memory_mb=200))
    try:
        result = await sandbox.exec([PYTHON, "-c", HARNESS, POOL], timeout_s=60)
        assert result.exit_code == 0, (
            result.stdout,
            result.stderr,
        )  # its own exit: it was not killed
        assert "harness alive, command exit" in result.stdout
        assert "memory limit exceeded" in result.stderr
        harness = int(result.stdout.split()[1])
        # The pool's processes went (all at once, or a worker at a time while it grew); the harness never did.
        assert sandbox.memory_kills
        assert all(harness not in kill.pids for kill in sandbox.memory_kills)
    finally:
        await sandbox.stop()
