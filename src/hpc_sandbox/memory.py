"""A sampled PSS memory watchdog that kills the subtree that holds the excess.

setuid HPC sites give users no cgroups, so a sandbox's memory is guarded by
sampling the proportional set size (PSS) of its processes: the instance's
process tree plus the tree of every active ``exec`` (exec'd commands join the
instance's namespaces but stay host-side children of their ``exec`` call).

On a breach the watchdog kills the smallest process subtree whose memory alone
brings the sandbox back under its limit: a process and all of its descendants.
That is the command that ran away, however its memory is spread: one process
that grew, or a pool of workers that are each smaller than their grandparent
(``make -j``, ``pytest -n``, joblib's ``n_jobs=-1``, sized by the node's CPU
count). A process that runs commands (an agent's harness in the sandbox)
survives the command that ran away, unless it holds the excess itself. When no
single subtree holds the excess (the memory is spread over several independent
commands), the largest subtree is killed, and the next sample decides again.
The instance's process and its direct children (the container's init and the
keep-alive loop) are never killed. This is a best-effort guard, not a hard
limit; usage can overshoot between samples.
"""

from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from . import procfs


@dataclass(frozen=True)
class MemoryKill:
    pid: int  # the root of the killed subtree
    pss_bytes: int  # what the subtree held
    total_bytes: int
    limit_bytes: int
    exec_root: int | None  # the exec whose process tree held the victim, if any
    pids: tuple[int, ...] = field(default=())  # the subtree: pid first, then its descendants

    def message(self) -> str:
        descendants = len(self.pids) - 1
        killed = f"pid {self.pid}" + (
            f" and its {descendants} descendant{'s' if descendants != 1 else ''}"
            if descendants > 0
            else ""
        )
        return (
            f"hpc-sandbox: memory limit exceeded (PSS {self.total_bytes} > "
            f"{self.limit_bytes} bytes); killed {killed} using {self.pss_bytes} bytes"
        )


@dataclass(frozen=True)
class _Process:
    pid: int
    start_time: str
    pss_bytes: int
    exec_root: int | None
    protected: bool
    parent: int | None = None  # its parent in the sample; None for a tree's root


def sample(instance_pid: int | None, exec_roots: Iterable[int]) -> list[_Process]:
    """PSS of every process in the instance tree and in each exec tree, with its parent, parents first.

    The instance process and its direct children (the container's init and the
    keep-alive loop) are protected; killing them would end the sandbox.
    """
    processes: list[_Process] = []
    seen: set[int] = set()
    trees: list[tuple[int, int | None]] = []
    if instance_pid:
        trees.append((instance_pid, None))
    trees.extend((root, root) for root in exec_roots)
    for root, exec_root in trees:
        protected = {root, *procfs.children(root)} if exec_root is None else set()
        pending: list[tuple[int, int | None]] = [(root, None)]
        while pending:
            pid, parent = pending.pop(0)
            started = procfs.start_time(pid)
            if pid in seen or started is None:
                continue
            seen.add(pid)
            processes.append(
                _Process(
                    pid, started, procfs.resident_bytes(pid), exec_root, pid in protected, parent
                )
            )
            pending.extend((child, pid) for child in procfs.children(pid))
    return processes


def choose_victim(processes: list[_Process], limit_bytes: int) -> MemoryKill | None:
    """The subtree to kill for a sample over ``limit_bytes`` (see the module docstring), or None."""
    total = sum(process.pss_bytes for process in processes)
    candidates = [process for process in processes if not process.protected]
    if total <= limit_bytes or not candidates:
        return None
    children: dict[int, list[_Process]] = {}
    for process in processes:
        if process.parent is not None:
            children.setdefault(process.parent, []).append(process)
    held: dict[int, int] = {}
    depth: dict[int, int] = {}
    for process in processes:  # parents come first
        depth[process.pid] = depth.get(process.parent, -1) + 1 if process.parent is not None else 0
    for process in reversed(processes):  # children are summed before their parents
        held[process.pid] = process.pss_bytes + sum(
            held[child.pid] for child in children.get(process.pid, ())
        )
    excess = total - limit_bytes
    covering = [process for process in candidates if held[process.pid] >= excess]
    if covering:  # the deepest culprit: the smallest subtree whose end alone ends the breach
        victim = min(covering, key=lambda process: (held[process.pid], -depth[process.pid]))
    else:  # spread over independent subtrees: the largest, then the next sample decides again
        victim = max(candidates, key=lambda process: held[process.pid])
    pids: list[int] = []
    pending = [victim]
    while pending:
        process = pending.pop(0)
        pids.append(process.pid)
        pending.extend(children.get(process.pid, ()))
    return MemoryKill(
        victim.pid, held[victim.pid], total, limit_bytes, victim.exec_root, tuple(pids)
    )


async def watch(
    limit_bytes: int,
    *,
    instance_pid: Callable[[], int | None],
    exec_roots: Callable[[], Iterable[int]],
    on_kill: Callable[[MemoryKill], None],
    interval_s: float = 1.0,
) -> None:
    """Sample forever, killing the subtree that holds the excess on each breach."""
    while True:
        processes = await asyncio.to_thread(sample, instance_pid(), list(exec_roots()))
        kill = choose_victim(processes, limit_bytes)
        if kill is not None:
            started = {process.pid: process.start_time for process in processes}
            killed = False
            for pid in kill.pids:  # its root first, so it starts nothing new meanwhile
                if procfs.start_time(pid) != started[pid]:
                    continue
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    continue
                killed = True
            if killed:
                on_kill(kill)
        await asyncio.sleep(interval_s)
