"""Which branch of a sandbox's processes the memory watchdog kills."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path

from hpc_sandbox import procfs
from hpc_sandbox.memory import _Process, choose_branch, choose_victim, sample

MB = 1024 * 1024
LIMIT = 1000 * MB


def proc(pid, parent, mb, *, adj=0, protected=False, exec_root=None, command=""):
    return _Process(pid, parent, "0", mb * MB, exec_root, protected, adj, command)


def sandbox(*processes):
    """The instance (1) and the container's init (2), protected, then ``processes``."""
    return [proc(1, None, 1, protected=True), proc(2, 1, 1, protected=True), *processes]


def killed(processes, limit=LIMIT):
    return sorted(process.pid for process in choose_branch(processes, limit))


def pool(parent, first_pid, workers, mb, **options):
    return [proc(first_pid + index, parent, mb, **options) for index in range(workers)]


def test_nothing_dies_within_the_limit():
    assert killed(sandbox(proc(10, 2, 900))) == []


def test_a_runaway_pool_dies_with_its_parent_and_the_harness_survives():
    # The harness (the largest single process) started a tool whose workers hold the memory.
    processes = sandbox(proc(10, 2, 257), proc(20, 10, 10), *pool(20, 100, 190, 250))
    assert killed(processes) == [20, *range(100, 290)]
    report = choose_victim(processes, LIMIT)
    assert (report.pid, report.processes) == (20, 191)
    assert report.pss_bytes == (10 + 190 * 250) * MB and report.exec_root is None


def test_a_pool_run_as_an_exec_dies_whole():
    # nsenter (50) -> nsenter (51) -> the command (52) -> its workers.
    command = [proc(50, None, 1, exec_root=50), proc(51, 50, 1, exec_root=50)]
    command += [proc(52, 51, 100, exec_root=50), *pool(52, 100, 10, 250, exec_root=50)]
    assert killed(sandbox(proc(10, 2, 257), *command)) == [52, *range(100, 110)]  # nsenter follows


def test_a_harness_that_leaks_itself_is_killed_with_its_children():
    assert killed(sandbox(proc(10, 2, 5000), proc(20, 10, 50), proc(21, 10, 60))) == [10, 20, 21]


def test_one_runaway_child_dies_alone():
    assert killed(sandbox(proc(10, 2, 257), proc(20, 10, 3000), proc(21, 10, 50))) == [20]


def test_tool_commands_marked_with_oom_score_adj_go_before_the_harness():
    tools = pool(10, 100, 15, 200, adj=1000)
    tools[3] = proc(103, 10, 300, adj=1000)
    assert killed(sandbox(proc(10, 2, 257), *tools)) == [103]  # the heaviest, alone


def test_unmarked_tool_commands_spread_evenly_take_their_parent_with_them():
    # Nothing tells the harness from a pool's parent here; mark tool commands to spare it.
    processes = sandbox(proc(10, 2, 257), *pool(10, 100, 15, 200))
    assert killed(processes) == [10, *range(100, 115)]


def test_oom_score_adj_minus_1000_is_never_killed():
    processes = sandbox(proc(10, 2, 3000, adj=-1000), proc(20, 10, 200), proc(30, 2, 100))
    assert killed(processes) == [20]  # within its branch, only what may die
    assert killed(sandbox(proc(10, 2, 3000, adj=-1000))) == []


def test_the_sandboxs_own_processes_are_never_killed():
    assert killed([proc(1, None, 3000, protected=True), proc(2, 1, 3000, protected=True)]) == []


def test_the_heaviest_top_level_branch_is_searched_first():
    processes = sandbox(proc(10, 2, 400), proc(20, 2, 300), proc(21, 20, 300), proc(30, 2, 100))
    assert killed(processes, limit=700 * MB) == [20, 21]


def test_a_runaway_exec_dies_before_a_harness_left_running_in_the_background():
    # A harness started with nohup and daemons of earlier commands, all below the
    # container's init, weigh more together than the runaway exec; each is its own branch.
    background = [proc(10, 2, 400), *pool(2, 11, 3, 300)]
    command = [proc(50, None, 1, exec_root=50), proc(51, 50, 1, exec_root=50)]
    command.append(proc(52, 51, 1000, exec_root=50))
    assert killed(sandbox(*background, *command), limit=2000 * MB) == [52]


def test_orphans_of_the_sandboxs_own_processes_may_be_killed():
    # Only the processes recorded at the start are protected, not what later
    # commands leave behind below them.
    processes = sandbox(proc(3, 2, 1), proc(4, 1, 3000), proc(50, None, 500, exec_root=50))
    assert killed(processes) == [4]


def test_a_runaway_tool_dies_alone_next_to_a_large_idle_one():
    # Killing the larger one alone is enough; the harness survives.
    command = [proc(60, None, 1, exec_root=60), proc(61, 60, 257, exec_root=60)]
    command += [proc(62, 61, 2000, exec_root=60), proc(63, 61, 1900, exec_root=60)]
    assert killed(sandbox(*command), 4000 * MB) == [62]


def test_a_tool_holding_the_excess_dies_before_a_larger_harness():
    processes = sandbox(proc(10, 2, 800), proc(20, 10, 300, command="tool"))
    assert killed(processes) == [20]
    # When it does not hold the excess, the harness's whole branch goes.
    processes = sandbox(proc(10, 2, 1000), proc(20, 10, 50, command="tool"))  # 52 MB over
    assert killed(processes) == [10, 20]


def test_like_the_kernel_a_larger_independent_command_goes_first_unless_marked():
    harness = [proc(50, None, 1, exec_root=50), proc(51, 50, 800, exec_root=50)]
    tool = [proc(60, None, 1, exec_root=60), proc(61, 60, 300, exec_root=60)]
    assert killed(sandbox(*harness, *tool)) == [51]
    marked = [proc(60, None, 1, exec_root=60), proc(61, 60, 300, exec_root=60, adj=1000)]
    assert killed(sandbox(*harness, *marked)) == [61]


POOL = "python3 pool.py"  # fork workers are copies of their parent
SPAWNED = "python3 -c from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=5, pipe_handle=7) --multiprocessing-fork"
FORKSERVER = (
    "python3 -c from multiprocessing.forkserver import main; main(10, 11, ['__main__'], **{})"
)


def test_pools_of_two_or_uneven_pools_die_whole():
    parent = proc(20, 10, 10, command=POOL)
    two = sandbox(proc(10, 2, 257), parent, *pool(20, 100, 2, 600, command=POOL))
    assert killed(two) == [20, 100, 101]
    uneven = [proc(100, 20, 900, command=POOL), proc(101, 20, 100, command=POOL)]
    processes = sandbox(proc(10, 2, 257), parent, *uneven)
    assert killed(processes) == [20, 100, 101]


def test_pools_of_one_and_spawned_or_forkserver_pools_die_with_their_owner():
    owner = proc(20, 10, 30, command=POOL)
    forked = sandbox(proc(10, 2, 257), owner, proc(100, 20, 1500, command=POOL))
    assert killed(forked) == [20, 100]
    spawned = sandbox(proc(10, 2, 257), owner, proc(100, 20, 1500, command=SPAWNED))
    assert killed(spawned) == [20, 100]
    # The fork server forks the workers; its owner waits on them.
    server = [proc(30, 20, 10, command=FORKSERVER), *pool(30, 100, 3, 500, command=FORKSERVER)]
    tracker = proc(
        40, 20, 5, command="python3 -c from multiprocessing.resource_tracker import main;main(8)"
    )
    processes = sandbox(proc(10, 2, 257), owner, *server, tracker)
    assert killed(processes) == [20, 30, 40, 100, 101, 102]


def test_a_subshell_is_no_pool_worker():
    # sh -c '(pool); echo $?': the subshell is a copy of its shell, not a worker.
    shell = proc(52, 51, 1, exec_root=50, command="/bin/sh -c (pool); echo $?")
    subshell = proc(53, 52, 1, exec_root=50, command="/bin/sh -c (pool); echo $?")
    workers = pool(53, 100, 4, 40, exec_root=50, command="tail -c 40000000")
    exec_tree = [proc(50, None, 1, exec_root=50), proc(51, 50, 1, exec_root=50)]
    processes = sandbox(*exec_tree, shell, subshell, *workers)
    assert killed(processes, 100 * MB) == [53, *range(100, 104)]


def test_children_running_the_very_same_command_are_a_pool():
    # sh -c 'for i in 1 2 3 4; do tail ... & done; wait', or xargs -P.
    shell = proc(20, 10, 1, command="sh -c for i in 1 2 3 4; do tail -c 60000000 & done; wait")
    workers = pool(20, 100, 4, 50, command="tail -c 60000000")
    processes = sandbox(proc(10, 2, 100, command="harness"), shell, *workers)
    assert killed(processes, 280 * MB) == [20, *range(100, 104)]


def test_tool_commands_that_differ_only_in_numbers_are_no_pool():
    harness = proc(10, 2, 257, command="python3 harness.py")
    tools = [proc(20, 10, 900, command="python3 /tmp/sol.py 1")]
    tools.append(proc(21, 10, 100, command="python3 /tmp/sol.py 2"))
    processes = sandbox(harness, *tools)
    assert killed(processes) == [20]


def test_like_the_kernel_a_larger_idle_sibling_goes_first_unless_the_runaway_is_marked():
    harness = proc(10, 2, 50, command="python3 harness.py")
    server = proc(20, 10, 600, command="python3 server.py")
    leak = proc(21, 10, 460, command="python3 leak.py")
    assert killed(sandbox(harness, server, leak), 1100 * MB) == [20]
    marked = proc(21, 10, 460, command="python3 leak.py", adj=500)
    assert killed(sandbox(harness, server, marked), 1100 * MB) == [21]


def test_a_runaway_tool_in_a_test_worker_dies_alone():
    # pytest-xdist: identical workers; one test's command ran away.
    workers = pool(20, 30, 4, 50, command="python3 -c import sys;exec(eval(sys.stdin.readline()))")
    tool = proc(40, 30, 900, command="./cli --big")
    processes = sandbox(proc(10, 2, 100, command="pytest -n 4"), proc(20, 10, 50), *workers, tool)
    assert killed(processes) == [40]


def test_a_pool_dies_whole_even_when_one_worker_would_do():
    workers = pool(20, 100, 3, 400, command=POOL)
    processes = sandbox(proc(10, 2, 257), proc(20, 10, 10, command=POOL), *workers)
    assert killed(processes, limit=1200 * MB) == [20, 100, 101, 102]


def test_an_oom_score_adj_every_process_inherited_changes_nothing():
    for adj in (1000, 500, -997):
        processes = [proc(1, None, 1, adj=adj, protected=True)]
        processes += [proc(2, 1, 1, adj=adj, protected=True), proc(10, 2, 257, adj=adj)]
        processes.append(proc(20, 10, 10, adj=adj))
        processes += pool(20, 100, 190, 250, adj=adj)
        assert killed(processes) == [20, *range(100, 290)], adj


def test_a_marked_pool_counts_its_mark_once():
    # Workers inherit their tool's oom_score_adj; the heavier marked tool goes first.
    tools = [proc(20, 10, 10, adj=1000), *pool(20, 100, 15, 100, adj=1000)]
    tools.append(proc(30, 10, 2000, adj=1000))
    assert killed(sandbox(proc(10, 2, 257), *tools)) == [30]


def test_sample_protects_the_instance_and_the_recorded_processes():
    child = subprocess.Popen(["sleep", "30"])
    other = subprocess.Popen(["sleep", "30"])
    try:
        protected = {child.pid: procfs.start_time(child.pid)}
        processes = {process.pid: process for process in sample(os.getpid(), [], protected)}
        assert processes[os.getpid()].protected and processes[child.pid].protected
        assert not processes[other.pid].protected
        assert processes[child.pid].parent == os.getpid()
        replaced = {child.pid: "not its start time"}
        assert not {p.pid: p for p in sample(os.getpid(), [], replaced)}[child.pid].protected
    finally:
        for process in (child, other):
            process.kill()
            process.wait()


def test_a_marked_leak_goes_before_a_marked_build_of_many_processes():
    leak = [proc(20, 10, 1, adj=1000), proc(21, 20, 3000, adj=1000)]
    build = [proc(30, 10, 20, adj=1000), *pool(30, 31, 9, 20, adj=1000)]
    assert killed(sandbox(proc(10, 2, 257), *leak, *build), limit=3000 * MB) == [21]


def test_a_kill_takes_what_the_branch_forked_after_it_was_sampled():
    # In a process of its own: it adopts orphans, as a sandbox's init does.
    helper = Path(__file__).with_name("kill_a_forker.py")
    src = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [sys.executable, str(helper)],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "PYTHONPATH": src},
        check=False,
    )
    assert result.returncode == 0, result.stderr
    sampled, killed, left = map(int, result.stdout.split())
    assert killed > sampled and left == 0, result.stdout


def test_sample_skips_an_instance_whose_pid_was_reused():
    child = subprocess.Popen(["sleep", "30"])
    try:
        assert sample(child.pid, [], {child.pid: "1"}) == []
        recorded = {child.pid: procfs.start_time(child.pid)}
        assert [p.pid for p in sample(child.pid, [], recorded)] == [child.pid]
    finally:
        child.kill()
        child.wait()


def test_sample_skips_exec_roots_whose_pid_was_reused():
    child = subprocess.Popen(["sleep", "30"])
    try:
        assert {p.pid for p in sample(None, [(child.pid, "1")])} == set()
        started = procfs.start_time(child.pid)
        assert {p.pid for p in sample(None, [(child.pid, started)])} == {child.pid}
    finally:
        child.kill()
        child.wait()


def test_a_process_that_vanishes_mid_sample_counts_as_inheriting(monkeypatch):
    child = subprocess.Popen(["sleep", "30"])
    real = procfs.oom_score_adj
    monkeypatch.setattr(
        procfs, "oom_score_adj", lambda pid: None if pid == child.pid else real(pid)
    )
    try:
        processes = {p.pid: p for p in sample(os.getpid(), [])}
        assert processes[child.pid].adj == processes[os.getpid()].adj
    finally:
        child.kill()
        child.wait()


def test_signals_the_kernel_refuses_are_skipped():
    from hpc_sandbox.memory import _signal

    assert not _signal(1, procfs.start_time(1), signal.SIGCONT)  # init is root's


def test_tmpfs_counts_whole_file_systems_only():
    mountinfo = """\
22 1 0:20 / / rw - ext4 /dev/sda1 rw
30 22 0:31 / /dev rw,nosuid - tmpfs tmpfs rw
31 30 0:32 / /dev/shm rw,nosuid,nodev - tmpfs tmpfs rw
32 31 0:33 / /dev/shm rw,nosuid,nodev - tmpfs tmpfs rw,size=65536k
33 22 0:40 /scratch/hpcsb-1/tmp /tmp rw,nosuid - tmpfs tmpfs rw
34 22 0:41 / /run/lock rw - tmpfs tmpfs rw
35 22 0:31 / /var/lock rw - tmpfs tmpfs rw
36 22 0:42 / /data rw - tmpfs tmpfs rw
"""
    # /tmp is a directory of the host's tmpfs; /var/lock is /dev's file system again;
    # /data is not one of the sandbox's own; /dev/shm counts once, as statvfs sees it.
    assert procfs.sandbox_tmpfs(mountinfo) == ["/dev", "/dev/shm", "/run/lock"]
