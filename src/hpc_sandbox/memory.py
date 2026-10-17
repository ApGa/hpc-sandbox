"""A sampled PSS memory watchdog that kills the subtree that holds the excess.

setuid HPC sites give users no cgroups, so a sandbox's memory is guarded by
sampling the proportional set size (PSS) of its processes: the instance's
process tree plus the tree of every active ``exec`` (exec'd commands join the
instance's namespaces but stay host-side children of their ``exec`` call).

On a breach the watchdog follows the memory down and kills a process subtree
(a process and all of its descendants): it starts from the largest subtree that
no other killable process contains (an exec's whole tree, or one that the
container's init or keep-alive loop runs) and moves into the child subtree that
holds the most, as long as that child alone holds the excess. That is the
command that ran away, however its memory is spread: one process that grew, or
a pool of workers that are each smaller than their grandparent (``make -j``,
``pytest -n``, joblib's ``n_jobs=-1``, sized by the node's CPU count), where it
stops at the pool's parent once no single worker holds the excess. A process
that runs commands (an agent's harness in the sandbox) survives the command
that ran away, unless it holds the excess itself, and a small process beside
the memory (the keep-alive loop's ``sleep``, a helper in a pipeline) is never
taken for it because a small excess, as at the first sample over the limit,
would fit in it. When no subtree holds the excess (it is spread over several
independent commands), the largest subtree is killed, and the next sample
decides again. The instance's process and its direct children (the container's
init and the keep-alive loop) are never killed. This is a best-effort guard,
not a hard limit; usage can overshoot between samples.
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
    for process in reversed(processes):  # children are summed before their parents
        held[process.pid] = process.pss_bytes + sum(
            held[child.pid] for child in children.get(process.pid, ())
        )
    excess = total - limit_bytes
    # From the largest subtree that no other candidate contains (an exec's tree, or one that the
    # container's init or keep-alive loop runs) down into the child that holds the most, while that
    # child alone holds the excess. A small excess fits in any small process; it is the memory that
    # leads to the victim. Spread over subtrees that each hold less: the largest, and sample again.
    killable = {process.pid for process in processes if not process.protected}
    roots = [process for process in candidates if process.parent not in killable]
    victim = max(roots, key=lambda process: held[process.pid])
    while True:
        below = [child for child in children.get(victim.pid, ()) if not child.protected]
        heaviest = max(below, key=lambda process: held[process.pid], default=None)
        if heaviest is None or held[heaviest.pid] < excess:
            break
        victim = heaviest
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
