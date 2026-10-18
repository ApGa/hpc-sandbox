"""Which branch of a sandbox's processes the memory watchdog kills."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import make_provider

from hpc_sandbox import SandboxSpec, memory, procfs
from hpc_sandbox.memory import _Process, choose_branch, choose_victim, sample

MB = 1024 * 1024
LIMIT = 1000 * MB


def proc(pid, parent, mb, *, adj=0, protected=False, exec_root=None, command="", private=None):
    private_bytes = None if private is None else private * MB
    return _Process(pid, parent, "0", mb * MB, exec_root, protected, adj, command, 0, private_bytes)


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


def test_a_pool_worker_that_holds_the_excess_dies_alone_as_with_the_kernel():
    # A multiprocessing.Pool then waits for its task until the command times out; a harness
    # running code in children it joins learns that one was killed (see below).
    owner = proc(20, 10, 10, command=POOL)
    workers = [proc(100, 20, 900, command=POOL), proc(101, 20, 100, command=POOL)]
    assert killed(sandbox(proc(10, 2, 257), owner, *workers)) == [100]
    # Spread over the pool, the memory takes the owner with every worker.
    spread = pool(20, 100, 3, 400, command=POOL)
    assert killed(sandbox(proc(10, 2, 257), owner, *spread)) == [20, 100, 101, 102]


def test_a_fork_workers_share_of_its_owners_pages_is_not_what_its_death_frees():
    # The owner grows; its fork workers' PSS holds their share of the pages they still
    # share with it, which killing them would not free: the owner goes, with them.
    harness = proc(10, 2, 100, command=HARNESS)
    owner = proc(20, 10, 475, command=TRAIN, private=300)
    workers = [proc(30 + i, 20, 175, command=TRAIN, private=1) for i in range(3)]
    total = 2 + 100 + 475 + 3 * 175
    assert killed(sandbox(harness, owner, *workers), (total - 20) * MB) == [20, 30, 31, 32]
    # A worker whose own memory holds the excess still dies alone.
    workers[1] = proc(31, 20, 575, command=TRAIN, private=400)
    assert killed(sandbox(harness, owner, *workers), (total + 400 - 20) * MB) == [31]


def test_a_forked_child_that_forks_again_dies_with_its_worker_and_the_harness_survives():
    # Code under test, run in a child the harness forked, allocates and forks a worker: the
    # two share what it allocated, and killing them frees it, whatever their private memory.
    harness = proc(10, 2, 19, command=HARNESS, private=0)
    child = proc(20, 10, 219, command=HARNESS, private=0)
    worker = proc(30, 20, 219, command=HARNESS, private=0)
    assert killed(sandbox(harness, child, worker), 200 * MB) == [20, 30]


def test_a_fork_copy_never_frees_more_than_it_holds():
    # Private memory beyond the PSS (hugetlb pages, which the PSS leaves out) frees nothing
    # the limit counts: the tool that holds the excess goes.
    launcher = proc(10, 2, 10, command=HARNESS)
    child = proc(20, 10, 50, command=HARNESS, private=8000)
    tool = proc(30, 10, 2000, command="python3 leak.py", private=2000)
    assert killed(sandbox(launcher, child, tool), 1500 * MB) == [30]


def test_a_harness_running_code_in_children_with_a_manager_survives_a_leak():
    # human-eval's check_correctness, four at once: a Manager's server and a Process each.
    harness = proc(10, 2, 120, command=HARNESS)
    servers = [proc(20 + i, 10, 10, command=HARNESS) for i in range(4)]
    children = [proc(30 + i, 10, 400 if i == 2 else 30, command=HARNESS) for i in range(4)]
    assert killed(sandbox(harness, *servers, *children), 300 * MB) == [32]


def test_a_pool_that_holds_the_excess_together_dies_with_its_owner_or_its_fork_server():
    owner = proc(20, 10, 30, command=POOL)
    spawned = sandbox(proc(10, 2, 257), owner, *pool(20, 100, 2, 750, command=SPAWNED))
    assert killed(spawned) == [20, 100, 101]
    # The fork server forks the workers and goes with them: its owner learns of it
    # (BrokenProcessPool, for a ProcessPoolExecutor).
    server = [proc(30, 20, 10, command=FORKSERVER), *pool(30, 100, 3, 500, command=FORKSERVER)]
    tracker = proc(
        40, 20, 5, command="python3 -c from multiprocessing.resource_tracker import main;main(8)"
    )
    processes = sandbox(proc(10, 2, 257), owner, *server, tracker)
    assert killed(processes) == [30, 100, 101, 102]


def test_a_lone_child_dies_alone_and_its_parent_learns_of_it():
    # A harness runs code in one child (multiprocessing.Process, os.fork) and joins it.
    harness = proc(10, 2, 257, command="python3 harness.py")
    forked = proc(100, 10, 2000, command="python3 harness.py")
    assert killed(sandbox(harness, forked), 900 * MB) == [100]
    spawned = proc(100, 10, 2000, command=SPAWNED)
    assert killed(sandbox(harness, spawned), 900 * MB) == [100]
    server = proc(30, 10, 10, command=FORKSERVER)  # Python 3.14's default
    child = proc(100, 30, 2000, command=FORKSERVER)
    assert killed(sandbox(harness, server, child), 900 * MB) == [100]


def test_a_subshell_is_no_pool_worker():
    # sh -c '(pool); echo $?': the subshell is a copy of its shell, not a worker.
    shell = proc(52, 51, 1, exec_root=50, command="/bin/sh -c (pool); echo $?")
    subshell = proc(53, 52, 1, exec_root=50, command="/bin/sh -c (pool); echo $?")
    workers = pool(53, 100, 4, 40, exec_root=50, command="tail -c 40000000")
    exec_tree = [proc(50, None, 1, exec_root=50), proc(51, 50, 1, exec_root=50)]
    processes = sandbox(*exec_tree, shell, subshell, *workers)
    assert killed(processes, 100 * MB) == [53, *range(100, 104)]


def test_children_running_the_very_same_command_die_one_at_a_time():
    # Unlike a multiprocessing pool's owner, their parent learns that one was killed.
    # A shell's jobs, or xargs -P:
    shell = proc(20, 10, 1, command="sh -c for i in 1 2 3 4; do tail -c 60000000 & done; wait")
    workers = [proc(100, 20, 60, command="tail -c 60000000")]
    workers += pool(20, 101, 3, 50, command="tail -c 60000000")
    processes = sandbox(proc(10, 2, 100, command="harness"), shell, *workers)
    assert killed(processes, 280 * MB) == [100]
    # A grader running one solution on several inputs:
    grader = proc(10, 2, 100, command="python3 grade.py")
    cases = [
        proc(20 + i, 10, mb, command="python3 /tmp/sol.py") for i, mb in enumerate((50, 900, 50))
    ]
    assert killed(sandbox(grader, *cases)) == [21]
    # An agent's shell running echo 1 | python3 sol.py & echo 2 | python3 sol.py &:
    agent = proc(10, 2, 5, command="bash")
    jobs = [proc(20, 10, 700, command="python3 sol.py"), proc(21, 10, 50, command="python3 sol.py")]
    assert killed(sandbox(agent, *jobs), 600 * MB) == [20]


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
    # A test that leaks in the worker itself: pytest reports the worker it lost.
    xdist = "python3 -c import sys;exec(eval(sys.stdin.readline()))"
    leaking = [proc(30, 10, 900, command=xdist), *pool(10, 31, 3, 50, command=xdist)]
    assert killed(sandbox(proc(10, 2, 100, command="pytest -n 4"), *leaking)) == [30]


def test_processes_that_mark_themselves_go_first_only_as_the_kernel_would_take_them():
    # A browser's renderers set their oom_score_adj to 300; a tool runs away elsewhere.
    browser = [proc(50, None, 1, exec_root=50), proc(51, 50, 200, exec_root=50)]
    browser += [proc(52 + i, 51, 150, exec_root=50, adj=300) for i in range(4)]
    tool = [proc(60, None, 1, exec_root=60), proc(61, 60, 1344, exec_root=60)]
    assert killed(sandbox(*browser, *tool), 2048 * MB) == [61]  # 1344 MB beats 150 + 614 MB
    # The same tool in the browser's exec, and a smaller one: then a renderer is the one.
    inside = [*browser, proc(61, 50, 1344, exec_root=50)]
    assert killed(sandbox(*inside), 2048 * MB) == [61]
    small = [proc(60, None, 1, exec_root=60), proc(61, 60, 500, exec_root=60)]
    assert killed(sandbox(*browser, *small), 1300 * MB) in ([52], [53], [54], [55])


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


# Scenarios of the watchdog of the fuse-overlay pull request (its tests 0010 and 0014).
HARNESS = "python3 /opt/agent/harness.py --task 17"
TRAIN = "python3 train.py"
LOKY = (
    "/usr/local/bin/python3 -m joblib.externals.loky.backend.popen_loky_posix "
    "--process-name LokyProcess-1 --pipe 19 --semaphore 7"
)


def tree(*rows, protected=(), commands=None):
    """Processes from (pid, parent, MB) rows, parents first."""
    commands = commands or {}
    return [
        proc(pid, parent, mb, protected=pid in protected, command=commands.get(pid, ""))
        for pid, parent, mb in rows
    ]


def test_192_workers_of_a_training_script_die_with_it_and_the_harness_survives():
    workers = [(100 + i, 3, 240) for i in range(192)]
    for kind in ("", LOKY, TRAIN):
        names = {2: HARNESS, 3: TRAIN, **{100 + i: kind for i in range(192)}} if kind else {}
        processes = tree(
            (1, None, 1), (2, 1, 257), (3, 2, 60), *workers, protected=(1,), commands=names
        )
        kill = choose_victim(processes, 3072 * MB)
        assert kill is not None and kill.pids == (3, *range(100, 292))
        assert kill.pss_bytes == (60 + 192 * 240) * MB
        assert "killed pid 3 and 192 descendant(s)" in kill.message()


def test_a_small_excess_kills_the_process_that_holds_the_memory_on_an_instance():
    rows = [  # pid, parent, KiB, exec root, protected, command
        (1, None, 19348, None, True, "sinit"),
        (2, 1, 289, None, True, "/bin/sh /.singularity.d/startscript"),
        (3, 2, 259, None, False, "sleep 86400"),
        (10, None, 9416, 10, False, "Singularity runtime parent"),
        (11, 10, 19870, 10, False, "/bin/sh -c x=...; sleep 25 | tail -c 100000000 /dev/zero"),
        (12, 11, 252, 10, False, "sleep 25"),
        (13, 11, 97951, 10, False, "tail -c 100000000 /dev/zero"),
    ]
    processes = [
        _Process(pid, parent, "0", kib * 1024, root, protected, 0, command)
        for pid, parent, kib, root, protected, command in rows
    ]
    total = sum(process.pss_bytes for process in processes)
    for excess_kib in (4, 64, 256, 1024, 65536):
        kill = choose_victim(processes, total - excess_kib * 1024)
        assert kill is not None and (kill.pids, kill.exec_root) == ((13,), 10), excess_kib


def test_a_bystander_smaller_than_the_hog_survives_a_small_excess():
    processes = tree((1, None, 1), (2, 1, 2), (3, 2, 2800), (4, 1, 300), protected=(1,))
    assert killed(processes, 3053 * MB) == [3]


def test_of_two_hogs_under_a_harness_one_goes():
    rows = ((1, None, 1), (2, 1, 200), (3, 2, 2000), (4, 2, 1900))
    for tools in (
        ("", ""),
        ("python3 sol.py 1", "python3 sol.py 2"),
        ("python3 sol.py", "python3 sol.py"),
        (HARNESS, HARNESS),  # two forked copies of the harness
    ):
        names = {2: HARNESS, 3: tools[0], 4: tools[1]}
        assert killed(tree(*rows, protected=(1,), commands=names), 4000 * MB) == [3], tools


PYTHON = "/usr/bin/python3"
# A harness that runs code in a child it joins, as HumanEval's does.
HARNESS_JOINS_A_CHILD = """
import multiprocessing, os, sys, time
def touched(mb):
    data = bytearray(mb * 1024 * 1024)
    data[::4096] = b"\\1" * (len(data) // 4096)
    return data
def leak():
    held = touched(400)
    time.sleep(30)
harness = touched(120)
if sys.argv[1] == "os.fork":
    pid = os.fork()
    if pid == 0:
        leak()
        os._exit(0)
    print("harness alive, child exit", os.waitstatus_to_exitcode(os.waitpid(pid, 0)[1]))
else:
    context = multiprocessing.get_context("fork")
    if sys.argv[1] == "manager":  # as human-eval's check_correctness does
        manager = context.Manager()
        results = manager.list()
    child = context.Process(target=leak)
    child.start()
    child.join(30)
    print("harness alive, child exit", child.exitcode)
"""


@pytest.mark.skipif(not Path(PYTHON).exists(), reason=f"needs {PYTHON}")
async def test_a_harness_learns_that_the_child_it_joins_was_killed(tmp_path, fake_runtime, sif):
    provider = make_provider(tmp_path)
    sandbox = await provider.start(SandboxSpec(image=str(sif), memory_mb=300))
    try:
        for how in ("multiprocessing", "os.fork", "manager"):
            command = [PYTHON, "-c", HARNESS_JOINS_A_CHILD, how]
            result = await sandbox.exec(command, timeout_s=60)
            assert result.exit_code == 0, (how, result.stdout, result.stderr)
            assert "harness alive, child exit -9" in result.stdout, how
            assert sandbox.memory_kills[-1].processes == 1
    finally:
        await sandbox.stop()


HARNESS_RUNS_A_COMMAND = """
import subprocess, sys
command = subprocess.run([sys.executable, "-c", sys.argv[1]])
print("harness alive, command exit", command.returncode, flush=True)
"""
POOL_OWNER_GROWS = """
import multiprocessing, time
def touched(mb):
    data = bytearray(mb * 1024 * 1024)
    data[::4096] = b"\\1" * (len(data) // 4096)
    return data
def idle(_):
    time.sleep(60)
shared = touched(600)  # its fork workers share it with it, copy-on-write
pool = multiprocessing.get_context("fork").Pool(3)
pool.map_async(idle, range(3))
time.sleep(2)
grown = touched(100)
time.sleep(60)
"""


@pytest.mark.skipif(not Path(PYTHON).exists(), reason=f"needs {PYTHON}")
async def test_a_growing_pool_owner_dies_with_its_fork_workers(tmp_path, fake_runtime, sif):
    provider = make_provider(tmp_path, memory_poll_s=0.5)
    sandbox = await provider.start(SandboxSpec(image=str(sif), memory_mb=700))
    try:
        command = [PYTHON, "-c", HARNESS_RUNS_A_COMMAND, POOL_OWNER_GROWS]
        result = await sandbox.exec(command, timeout_s=60)
        assert "harness alive, command exit -9" in result.stdout, (result.stdout, result.stderr)
        assert len(sandbox.memory_kills) == 1 and sandbox.memory_kills[0].processes >= 4
    finally:
        await sandbox.stop()


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


def test_mount_tables_are_read_as_the_kernel_writes_them():
    # The kernel escapes space, tab, newline and backslash, and nothing else: no other
    # character (a no-break space, U+2028) separates fields or lines.
    raw = (
        b"36 35 0:20 / /a\\040b\\011c\\134d rw - tmpfs tmpfs rw\n"
        b"37 35 0:21 / /x\xc2\xa0y\xe2\x80\xa8z rw shared:1 - tmpfs none rw,size=1k\n"
        b"39 35 0:23 / /x\xe2\x80\xa8-\xe2\x80\xa8 rw - tmpfs tmpfs rw\n"
        b"garbage - x\n"
        b"38 35 0:22 /sub /mnt rw - ext4 /dev/sda rw\n"
        b"40 35 0:24 / /src rw - tmpfs a\\040source\\134 rw\n"
    )
    found = [(m.point, m.fstype, m.root, m.source) for m in procfs.mounts(raw)]
    assert found == [
        ("/a b\tc\\d", "tmpfs", "/", "tmpfs"),
        ("/x\xa0y\u2028z", "tmpfs", "/", "none"),
        ("/x\u2028-\u2028", "tmpfs", "/", "tmpfs"),
        ("/mnt", "ext4", "/sub", "/dev/sda"),
        ("/src", "tmpfs", "/", "a source\\"),
    ]
    assert procfs.sandbox_tmpfs(raw) == []  # it read these as lines of their own and failed


async def test_a_failing_tmpfs_measure_never_keeps_a_kill_from_happening(monkeypatch):
    def broken(pid: int) -> int:
        raise IndexError("a mount table nobody expected")

    monkeypatch.setattr(procfs, "tmpfs_bytes", broken)
    hog = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "data = bytearray(200 * 2**20); data[::4096] = b'1' * (len(data) // 4096); "
        "print(1, flush=True); import time; time.sleep(60)",
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        assert hog.stdout is not None and await hog.stdout.readline() == b"1\n"
        root = (hog.pid, procfs.start_time(hog.pid))
        kills: list = []
        await memory._check(
            50 * MB, lambda: None, lambda: [root], kills.append, lambda: hog.pid, None
        )
        assert len(kills) == 1 and kills[0].tmpfs_bytes is None
        assert await asyncio.wait_for(hog.wait(), 10) == -signal.SIGKILL
    finally:
        if hog.returncode is None:
            hog.kill()
            await hog.wait()


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


def test_memory_falls_back_to_the_rss_without_smaps_rollup(tmp_path, monkeypatch):
    # Kernels before 4.14 have no smaps_rollup: a process must not weigh nothing there.
    pid = tmp_path / "proc" / "100"
    (pid / "task" / "100").mkdir(parents=True)
    (pid / "status").write_text("Name:\tpython3\nVmRSS:\t  102400 kB\n")
    monkeypatch.setattr(procfs, "Path", lambda path: tmp_path / str(path).lstrip("/"))
    assert procfs.memory_bytes(100) == (100 * MB, 100 * MB)
    # Hugetlb pages are not in the PSS, and not in what a death frees either.
    (pid / "smaps_rollup").write_text(
        "00400000-7ffe00000000 ---p 00000000 00:00 0  [rollup]\n"
        "Rss:              204800 kB\n"
        "Pss:              153600 kB\n"
        "Private_Clean:      1024 kB\n"
        "Private_Dirty:     50176 kB\n"
        "Private_Hugetlb:  819200 kB\n"
    )
    assert procfs.memory_bytes(100) == (150 * MB, 50 * MB)
