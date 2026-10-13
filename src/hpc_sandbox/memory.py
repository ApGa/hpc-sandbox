"""A sampled PSS memory watchdog with OOM-killer semantics.

setuid HPC sites give users no cgroups, so a sandbox's memory is guarded by
sampling the proportional set size (PSS) of its processes: the instance's
process tree plus the tree of every active ``exec`` (exec'd commands join the
instance's namespaces but stay host-side children of their ``exec`` call). On
a breach the largest process is SIGKILLed, like the kernel OOM killer, rather
than the whole instance: a harness running in the same sandbox survives a tool
command that ran away. This is a best-effort guard, not a hard limit; usage can
overshoot between samples.
"""

from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from . import procfs


@dataclass(frozen=True)
class MemoryKill:
    pid: int
    pss_bytes: int
    total_bytes: int
    limit_bytes: int
    exec_root: int | None  # the exec whose process tree held the victim, if any

    def message(self) -> str:
        return (
            f"hpc-sandbox: memory limit exceeded (PSS {self.total_bytes} > "
            f"{self.limit_bytes} bytes); killed pid {self.pid} using {self.pss_bytes} bytes"
        )


@dataclass(frozen=True)
class _Process:
    pid: int
    start_time: str
    pss_bytes: int
    exec_root: int | None
    protected: bool


def sample(instance_pid: int | None, exec_roots: Iterable[int]) -> list[_Process]:
    """PSS of every process in the instance tree and in each exec tree.

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
        for pid in (root, *procfs.descendants([root])):
            started = procfs.start_time(pid)
            if pid in seen or started is None:
                continue
            seen.add(pid)
            processes.append(
                _Process(pid, started, procfs.resident_bytes(pid), exec_root, pid in protected)
            )
    return processes


def choose_victim(processes: list[_Process], limit_bytes: int) -> MemoryKill | None:
    total = sum(process.pss_bytes for process in processes)
    candidates = [process for process in processes if not process.protected]
    if total <= limit_bytes or not candidates:
        return None
    victim = max(candidates, key=lambda process: process.pss_bytes)
    return MemoryKill(victim.pid, victim.pss_bytes, total, limit_bytes, victim.exec_root)


async def watch(
    limit_bytes: int,
    *,
    instance_pid: Callable[[], int | None],
    exec_roots: Callable[[], Iterable[int]],
    on_kill: Callable[[MemoryKill], None],
    interval_s: float = 1.0,
) -> None:
    """Sample forever, killing the largest unprotected process on each breach."""
    while True:
        processes = await asyncio.to_thread(sample, instance_pid(), list(exec_roots()))
        kill = choose_victim(processes, limit_bytes)
        if kill is not None:
            victim = next(process for process in processes if process.pid == kill.pid)
            if procfs.start_time(kill.pid) == victim.start_time:
                try:
                    os.kill(kill.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                else:
                    on_kill(kill)
        await asyncio.sleep(interval_s)
