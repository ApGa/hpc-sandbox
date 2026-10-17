"""Bounded, killable host subprocesses: the transport behind every runtime call."""

from __future__ import annotations

import asyncio
import os
import signal
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import procfs

DEFAULT_OUTPUT_LIMIT = 16 * 1024 * 1024
TIMEOUT_EXIT_CODE = 124
_REAP_TIMEOUT_S = 5.0
_EXIT_DRAIN_S = 1.0
# Readers left behind when a background process still holds stdout or stderr
# after the command itself exited. They keep draining (and discarding) until
# EOF so that the background process never hits EPIPE.
_lingering: set[asyncio.Task[None]] = set()


class BoundedOutput:
    """Keep the head and tail of a stream while draining arbitrarily noisy commands."""

    def __init__(self, limit: int) -> None:
        if limit < 2:
            raise ValueError("output limit must be at least 2 bytes")
        self.head_limit = limit // 2
        self.tail_limit = limit - self.head_limit
        self.head = bytearray()
        self.tail: deque[bytes] = deque()
        self.tail_size = 0
        self.total = 0

    def append(self, data: bytes) -> None:
        self.total += len(data)
        head_count = min(len(data), self.head_limit - len(self.head))
        self.head.extend(data[:head_count])
        data = data[head_count:]
        if data:
            self.tail.append(data)
            self.tail_size += len(data)
        while self.tail_size > self.tail_limit:
            first = self.tail.popleft()
            excess = self.tail_size - self.tail_limit
            if len(first) > excess:
                self.tail.appendleft(first[excess:])
                self.tail_size -= excess
            else:
                self.tail_size -= len(first)

    def getvalue(self) -> bytes:
        omitted = self.total - len(self.head) - self.tail_size
        marker = (
            f"\n[hpc-sandbox: output truncated, {omitted} bytes omitted]\n".encode()
            if omitted
            else b""
        )
        return bytes(self.head) + marker + b"".join(self.tail)


@dataclass
class ProcessResult:
    returncode: int
    stdout: bytes
    stderr: bytes
    timed_out: bool = False

    def detail(self) -> str:
        """Both streams as text, for error messages."""
        return (self.stderr + self.stdout).decode(errors="replace").strip()


def exit_status(returncode: int) -> int:
    """Map asyncio's negative signal codes to the shell convention (128 + signal)."""
    return 128 - returncode if returncode < 0 else returncode


def _send(kill: Callable[[int, int], None], target: int) -> None:
    with suppress(ProcessLookupError, PermissionError):
        kill(target, signal.SIGKILL)


def kill_tree(pid: int) -> None:
    """SIGKILL the process group led by ``pid`` and every descendant of ``pid``.

    Descendants that left the group (``setsid``, job control) are found through
    the process tree. Only daemons that were already re-parented escape; those
    die with their instance.
    """
    victims = procfs.descendants([pid])
    _send(os.killpg, pid)
    for victim in victims:
        _send(os.kill, victim)


async def run_process(
    argv: Sequence[str],
    *,
    timeout_s: float | None,
    stdin: bytes | None = None,
    env: Mapping[str, str] | None = None,
    cwd: str | Path | None = None,
    output_limit: int = DEFAULT_OUTPUT_LIMIT,
    on_start: Callable[[int], None] | None = None,
) -> ProcessResult:
    """Run ``argv`` in its own session, bounding its run time and captured output.

    On timeout the process tree is killed and the result has ``timed_out`` set
    and exit status 124. On cancellation the tree is killed before
    ``CancelledError`` propagates. The call returns once the command exits,
    even if a background process it started still holds an output pipe open.
    """
    proc = await _create(
        argv,
        stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=None if env is None else dict(env),
        cwd=cwd,
        start_new_session=True,
    )
    assert proc.stdout is not None and proc.stderr is not None
    stdout, stderr = BoundedOutput(output_limit), BoundedOutput(output_limit)
    readers = [
        asyncio.create_task(_read(proc.stdout, stdout)),
        asyncio.create_task(_read(proc.stderr, stderr)),
    ]
    feeder = asyncio.create_task(_feed(proc, stdin))
    try:
        if on_start is not None:
            on_start(proc.pid)
        # Not wait_for: on Python 3.11 it swallows a cancellation that arrives
        # just as the command exits, and the caller would never learn of it.
        async with asyncio.timeout(timeout_s):
            await _exited(proc)
    except TimeoutError:
        kill_tree(proc.pid)
        if await _settle(proc, readers, feeder):
            raise asyncio.CancelledError from None
        return ProcessResult(
            TIMEOUT_EXIT_CODE, stdout.getvalue(), stderr.getvalue(), timed_out=True
        )
    except BaseException:
        kill_tree(proc.pid)
        await _settle(proc, readers, feeder)
        raise
    await _drain(readers, feeder)
    assert proc.returncode is not None
    return ProcessResult(exit_status(proc.returncode), stdout.getvalue(), stderr.getvalue())


async def _create(argv: Sequence[str], **options: Any) -> asyncio.subprocess.Process:
    """``asyncio.create_subprocess_exec``, safe to cancel.

    On Python 3.11, cancelling it while it connects the pipes leaves it
    waiting forever for the process to finish. So creation always completes;
    when the caller was cancelled meanwhile, the new process is killed.
    """
    creation = asyncio.ensure_future(asyncio.create_subprocess_exec(*argv, **options))
    try:
        return await asyncio.shield(creation)
    except asyncio.CancelledError:
        while not creation.done():
            with suppress(asyncio.CancelledError):
                await asyncio.wait({creation})
        if not creation.cancelled() and creation.exception() is None:
            proc = creation.result()
            kill_tree(proc.pid)
            exited = asyncio.ensure_future(_exited(proc))
            with suppress(asyncio.CancelledError):
                await asyncio.wait({exited}, timeout=_REAP_TIMEOUT_S)
            exited.cancel()
        raise


async def _read(stream: asyncio.StreamReader, sink: BoundedOutput) -> None:
    while chunk := await stream.read(65536):
        sink.append(chunk)


async def _feed(proc: asyncio.subprocess.Process, data: bytes | None) -> None:
    if data is None or proc.stdin is None:
        return
    try:
        proc.stdin.write(data)
        await proc.stdin.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass
    finally:
        proc.stdin.close()


async def _exited(proc: asyncio.subprocess.Process) -> None:
    """Wait for the process to exit.

    ``Process.wait()`` resolves only once every pipe has closed, which a
    background child holding stdout can delay indefinitely, so the exit
    status is also polled.
    """
    waiter = asyncio.ensure_future(proc.wait())
    delay = 0.01
    try:
        while proc.returncode is None and not waiter.done():
            await asyncio.wait({waiter}, timeout=delay)
            delay = min(delay * 2, 0.1)
    finally:
        waiter.cancel()


async def _drain(readers: list[asyncio.Task[None]], feeder: asyncio.Task[None]) -> None:
    await asyncio.wait(readers, timeout=_EXIT_DRAIN_S)
    _linger(readers, feeder)


def _linger(readers: list[asyncio.Task[None]], feeder: asyncio.Task[None]) -> None:
    for reader in readers:
        if not reader.done():
            _lingering.add(reader)
            reader.add_done_callback(_lingering.discard)
    if not feeder.done():
        feeder.cancel()


async def _settle(
    proc: asyncio.subprocess.Process,
    readers: list[asyncio.Task[None]],
    feeder: asyncio.Task[None],
) -> bool:
    """Wait, bounded, for a killed process; return whether we were cancelled meanwhile."""

    async def settle() -> None:
        await _exited(proc)
        await asyncio.wait(readers, timeout=_EXIT_DRAIN_S)

    task = asyncio.ensure_future(settle())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _REAP_TIMEOUT_S
    cancelled = False
    while not task.done() and (remaining := deadline - loop.time()) > 0:
        try:
            await asyncio.wait({task}, timeout=remaining)
        except asyncio.CancelledError:
            cancelled = True
    task.cancel()
    _linger(readers, feeder)
    return cancelled
