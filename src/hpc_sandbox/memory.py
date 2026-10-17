"""A sampled PSS memory watchdog that kills the branch of processes holding the memory.

setuid HPC sites give users no cgroups, so a sandbox's memory is guarded by
sampling the proportional set size (PSS) of its processes: the instance's
process tree plus the tree of every active ``exec`` (exec'd commands join the
instance's namespaces but stay host-side children of their ``exec`` call).
This is a best-effort guard, not a hard limit; usage can overshoot between
samples.

On a breach the watchdog kills one branch of the process forest, never the
sandbox's own processes (recorded when it starts). It starts from the
heaviest of the sandbox's top-level branches (each ``exec``'s tree, and each
tree below the sandbox's own processes: commands left running in the
background by earlier ones) and descends into the child branch that holds
the most while that child alone holds the excess over the limit. So the
command that ran away dies and a harness survives a tool command it started,
whatever their sizes; when the memory is spread over several children, their
parent's branch dies whole. As with the kernel's OOM killer, a pool's worker
that holds the excess alone dies alone, so a harness that runs code in
children it joins (with a ``multiprocessing.Manager``, or several at once)
learns that one was killed, while a ``multiprocessing.Pool``, which never
notices a lost worker, waits for its task until the command times out
(``concurrent.futures`` and joblib raise an error instead): the process tree
cannot tell the two apart. It may also kill a larger idle process before a
smaller one that is growing.

``oom_score_adj`` counts the way the kernel's OOM killer counts it, relative to
the parent's value, so that what every process inherited changes nothing:
where a process raised it, that process weighs ``oom_score_adj / 1000`` of the
limit more, for itself alone, as the kernel scores each process alone (the
renderers of a browser, which mark themselves, do not add up). It goes first,
wherever it is, when what its branch holds plus its mark outweighs the branch
the memory leads to, and before its siblings when it outweighs its parent or
holds the excess. A harness that starts its tool commands with
``choom -n 1000 -- cmd`` (or writes ``/proc/self/oom_score_adj``) makes them
preferred victims here and under the kernel alike. A process that lowered it
only weighs less itself. One at ``-1000`` below what the sandbox inherited,
which takes privileges on the host to set, is never killed.

The branch dies whole: its processes are stopped (but not its root, whose
parent sees it killed, never stopped), then everything is SIGKILLed, then any
process forked meanwhile, found among the orphans of the sandbox's own
processes in the branch's sessions, goes too. One that its root moves into a
new session in the instant before the kill survives, and stays watched.

Files on the sandbox's tmpfs mounts hold memory that no kill frees and that no
process's PSS includes unless mapped; kill reports say how much.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from . import procfs

log = logging.getLogger(__name__)

# Passes over the children of a stopped branch: each finds only what was being
# forked while the previous one stopped its parents.
_FREEZE_PASSES = 100
# How long a killed branch root may take to exit before its orphans are swept.
_EXIT_WAIT_S = 1.0
_CLOCK_TICKS = os.sysconf("SC_CLK_TCK")

Identity = tuple[int, str]  # PID and start time


@dataclass(frozen=True)
class MemoryKill:
    pid: int  # root of the killed branch
    pss_bytes: int  # PSS the branch held
    total_bytes: int  # PSS the sandbox held
    limit_bytes: int
    exec_root: int | None  # the exec whose process tree held the branch, if any
    processes: int = 1  # how many processes the branch held
    tmpfs_bytes: int | None = None  # held by files on the sandbox's tmpfs mounts, if known
    pids: tuple[int, ...] = ()  # the branch's processes as sampled, root first

    def message(self) -> str:
        killed = f"pid {self.pid}"
        if self.processes > 1:
            killed += f" and {self.processes - 1} descendant(s)"
        tmpfs = "" if self.tmpfs_bytes is None else f"; tmpfs files hold {self.tmpfs_bytes} bytes"
        return (
            f"hpc-sandbox: memory limit exceeded (PSS {self.total_bytes} > "
            f"{self.limit_bytes} bytes); killed {killed} using {self.pss_bytes} bytes{tmpfs}"
        )


@dataclass(frozen=True)
class _Process:
    pid: int
    parent: int | None  # None for the root of a sampled tree
    start_time: str
    pss_bytes: int
    exec_root: int | None
    protected: bool
    adj: int  # oom_score_adj
    command: str = ""  # the command line; "" if unknown
    session: int = 0  # the session ID, which orphans keep when another process adopts them

    @property
    def identity(self) -> Identity:
        return (self.pid, self.start_time)


def sample(
    instance_pid: int | None,
    exec_roots: Iterable[int | Identity],
    protected: Mapping[int, str] | None = None,
) -> list[_Process]:
    """PSS of every process in the instance tree and in each exec tree, parents first.

    ``protected`` maps the sandbox's own processes (killing them would end the
    sandbox) to their start times; the instance process always is one, and its
    tree is skipped once its PID names another process than the one recorded
    there. So is an exec root given with its start time.
    """
    processes: list[_Process] = []
    seen: set[int] = set()
    own = dict(protected or {})
    trees: list[tuple[int, int | None]] = []
    if instance_pid and own.get(instance_pid, procfs.start_time(instance_pid)) == procfs.start_time(
        instance_pid
    ):
        trees.append((instance_pid, None))
    for root in exec_roots:
        pid, started = root if isinstance(root, tuple) else (root, None)
        if started is None or procfs.start_time(pid) == started:
            trees.append((pid, pid))
    for root, exec_root in trees:
        pending: list[tuple[int, _Process | None]] = [(root, None)]
        while pending:
            pid, parent = pending.pop()
            identity = procfs.identity(pid)
            if pid in seen or identity is None:
                continue
            started, session = identity
            seen.add(pid)
            adj = procfs.oom_score_adj(pid)
            if adj is None:  # gone meanwhile: count it as what it inherited
                adj = parent.adj if parent else 0
            process = _Process(
                pid,
                parent.pid if parent else None,
                started,
                procfs.resident_bytes(pid),
                exec_root,
                (exec_root is None and pid == root) or own.get(pid) == started,
                adj,
                procfs.cmdline(pid) or "",
                session,
            )
            processes.append(process)
            pending.extend((child, process) for child in procfs.children(pid))
    return processes


def choose_branch(processes: list[_Process], limit_bytes: int) -> list[_Process]:
    """The branch to kill, root first, or nothing while the sandbox is within its limit.

    ``processes`` lists parents before their children, as :func:`sample` does.
    """
    total = sum(process.pss_bytes for process in processes)
    if total <= limit_bytes:
        return []
    excess = total - limit_bytes
    by_pid = {process.pid: process for process in processes}
    children: dict[int | None, list[_Process]] = defaultdict(list)
    baseline: dict[int, int] = {}  # the oom_score_adj of each process's tree root
    for process in processes:
        parent = by_pid.get(process.parent) if process.parent is not None else None
        children[parent.pid if parent else None].append(process)
        baseline[process.pid] = baseline[parent.pid] if parent else process.adj

    def shielded(process: _Process) -> bool:
        return process.adj <= -1000 < baseline[process.pid]

    def killable(process: _Process) -> bool:
        return not process.protected and not shielded(process)

    def bonus(process: _Process) -> int:
        """What an oom_score_adj above or below its parent's adds to a process, as the
        kernel's OOM killer counts it (a thousandth of the limit per point)."""
        if shielded(process):
            return 0
        parent = by_pid.get(process.parent) if process.parent is not None else None
        return (process.adj - parent.adj) * limit_bytes // 1000 if parent else 0

    def mark(process: _Process) -> int:
        """How much more a process marked as a preferred victim weighs."""
        return max(bonus(process), 0)

    def weight(process: _Process) -> int:
        """A process's own weight with its mark: what the kernel would score it."""
        if shielded(process):
            return 0
        return max(0, process.pss_bytes + bonus(process))

    branch: dict[int, int] = {}  # memory of each process's subtree; marks lower it, never raise it
    freed: dict[int, int] = {}  # PSS that killing each subtree frees
    for process in reversed(processes):  # children before parents
        below = children[process.pid]
        branch[process.pid] = (
            weight(process) - mark(process) + sum(branch[kid.pid] for kid in below)
        )
        own = process.pss_bytes if killable(process) else 0
        freed[process.pid] = own + sum(freed[kid.pid] for kid in below)

    def candidates(pid: int | None) -> list[_Process]:
        """The killable branches right below ``pid``, looking through processes that are not."""
        found, pending = [], list(children[pid])
        while pending:
            process = pending.pop()
            if killable(process):
                found.append(process)
            else:
                pending.extend(children[process.pid])
        return [process for process in found if freed[process.pid]]

    top = candidates(None)
    if not top:
        return []  # nothing that may be killed holds memory
    node = max(top, key=lambda kid: branch[kid.pid])
    # A process marked as a preferred victim goes first wherever it is when what it
    # holds and its mark outweigh the branch the memory leads to. Marks never add up
    # (a browser's renderers mark themselves): the kernel scores each process alone.
    preferred = [p for p in processes if mark(p) and killable(p) and freed[p.pid]]
    best = max(preferred, key=lambda p: branch[p.pid] + mark(p), default=None)
    if best is not None and branch[best.pid] + mark(best) > branch[node.pid]:
        node = best
    while kids := candidates(node.pid):
        marked = [  # more exposed than the node, and outweighing it or enough on their own
            kid
            for kid in kids
            if node.adj < kid.adj
            and (weight(node) < branch[kid.pid] + mark(kid) or freed[kid.pid] >= excess)
        ]
        heaviest = max(kids, key=lambda kid: freed[kid.pid])
        if marked:
            node = max(marked, key=lambda kid: branch[kid.pid] + mark(kid))  # a preferred victim
        elif freed[heaviest.pid] >= excess:
            node = heaviest  # it holds the breach on its own
        else:
            break
    found = [node]
    for process in found:  # grows while it is walked
        found.extend(children[process.pid])
    return [process for process in found if killable(process)]


def choose_victim(processes: list[_Process], limit_bytes: int) -> MemoryKill | None:
    """What :func:`watch` would kill now, as a report."""
    branch = choose_branch(processes, limit_bytes)
    return _report(processes, branch, limit_bytes, None) if branch else None


def _report(
    processes: list[_Process],
    branch: list[_Process],
    limit_bytes: int,
    tmpfs_bytes: int | None,
) -> MemoryKill:
    root = branch[0]
    return MemoryKill(
        root.pid,
        sum(process.pss_bytes for process in branch),
        sum(process.pss_bytes for process in processes),
        limit_bytes,
        root.exec_root,
        len(branch),
        tmpfs_bytes,
        tuple(process.pid for process in branch),
    )


async def watch(
    limit_bytes: int,
    *,
    instance_pid: Callable[[], int | None],
    exec_roots: Callable[[], Iterable[int | Identity]],
    on_kill: Callable[[MemoryKill], None],
    interval_s: float = 1.0,
    container_pid: Callable[[], int | None] | None = None,
    protected: Callable[[], Mapping[int, str]] | None = None,
) -> None:
    """Sample forever, killing the branch that holds the memory on each breach.

    ``container_pid`` names a process in the container's mount namespace, for
    measuring its tmpfs mounts (default: the instance process). ``protected``
    gives the sandbox's own processes; see :func:`sample`.
    """
    while True:
        try:
            await _check(
                limit_bytes,
                instance_pid,
                exec_roots,
                on_kill,
                container_pid,
                protected,
            )
        except Exception:  # the next sample may well work
            log.exception("memory watchdog")
        await asyncio.sleep(interval_s)


async def _check(
    limit_bytes: int,
    instance_pid: Callable[[], int | None],
    exec_roots: Callable[[], Iterable[int | Identity]],
    on_kill: Callable[[MemoryKill], None],
    container_pid: Callable[[], int | None] | None,
    protected: Callable[[], Mapping[int, str]] | None,
) -> None:
    """One sample, and a kill on a breach."""
    root = instance_pid()
    own = dict(protected()) if protected is not None else {}
    since = _now_ticks()
    processes = await asyncio.to_thread(sample, root, list(exec_roots()), own)
    branch = choose_branch(processes, limit_bytes)
    if not branch or procfs.start_time(branch[0].pid) != branch[0].start_time:
        return
    viewer = container_pid() if container_pid is not None else root
    tmpfs = await asyncio.to_thread(procfs.tmpfs_bytes, viewer) if viewer else None
    if procfs.start_time(branch[0].pid) != branch[0].start_time:
        return  # it ended meanwhile
    # Charge the kill before the first signal: an exec whose command dies must
    # find it when its caller sees the exit.
    on_kill(_report(processes, branch, limit_bytes, tmpfs))
    owners = {p.pid: p.start_time for p in processes if p.protected and p.exec_root is None}
    sessions = {process.session for process in branch}
    sampled = {process.identity for process in processes}
    await asyncio.to_thread(_kill_branch, branch, sampled, owners, since, sessions)


def _now_ticks() -> int:
    """Now, in the clock ticks since boot that process start times count."""
    return int(time.clock_gettime(time.CLOCK_BOOTTIME) * _CLOCK_TICKS)


def _kill_branch(
    branch: list[_Process],
    sampled: set[Identity],
    owners: Mapping[int, str],
    since: int,
    sessions: set[int],
) -> int:
    """SIGKILL the branch and whatever it forked since it was sampled; return how many died.

    The root's descendants are stopped (stopped processes cannot fork), the
    root is killed at once (never stopped: its parent, ``nsenter`` or a
    job-control shell, would see that), and then whatever the stopped ones
    forked before they stopped is found and stopped in turn. All of it dies.
    What the root forked meanwhile has been adopted by the sandbox's own
    processes (``owners``, PID to start time) once the root is gone: their
    children started since ``since`` in the branch's ``sessions`` (orphans
    keep their session, so other commands' are left alone) that were not
    sampled are stopped and killed too, with their descendants. Processes left
    out of the branch (the shielded ones) stay out.
    """
    root, rest = branch[0], branch[1:]
    stopped: dict[int, str] = {}
    for process in rest:  # parents first, so they cannot replace children
        if _signal(process.pid, process.start_time, signal.SIGSTOP):
            stopped[process.pid] = process.start_time
    killed = int(_signal(root.pid, root.start_time, signal.SIGKILL))
    deadline = time.monotonic() + _EXIT_WAIT_S
    while procfs.owned_live_start_time(root.pid) == root.start_time:
        if time.monotonic() > deadline:
            break
        time.sleep(0.002)
    adopted = []
    for owner, owner_start in owners.items():
        if procfs.start_time(owner) != owner_start:
            continue  # its PID names another process now
        for child in procfs.children(owner):
            identity = procfs.identity(child)
            if identity is None or (child, identity[0]) in sampled:
                continue
            started, session = identity
            if int(started) >= since and session in sessions:
                adopted.append((child, started))
    for pid, started in adopted:
        if _signal(pid, started, signal.SIGSTOP):
            stopped[pid] = started
    _stop_descendants(stopped, sampled, list(stopped))
    return killed + sum(_signal(pid, started, signal.SIGKILL) for pid, started in stopped.items())


def _stop_descendants(stopped: dict[int, str], sampled: set[Identity], frontier: list[int]) -> None:
    """Stop the children of the ``frontier`` that are not stopped yet, and theirs, and so on.

    A process forks no more once stopped, so each one's children need one look.
    """
    for _ in range(_FREEZE_PASSES):
        found = []
        for pid in frontier:
            for child in procfs.children(pid):
                started = procfs.start_time(child)
                if started is None or child in stopped or (child, started) in sampled:
                    continue
                if _signal(child, started, signal.SIGSTOP):
                    stopped[child] = started
                    found.append(child)
        if not found:
            return
        frontier = found


def _signal(pid: int, start_time: str, signum: int) -> bool:
    if procfs.start_time(pid) != start_time:
        return False  # gone, or its PID reused
    try:
        os.kill(pid, signum)
    except (ProcessLookupError, PermissionError):  # gone, or a setuid helper
        return False
    return True
