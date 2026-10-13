"""Exclusive ``flock`` helpers that never block the event loop."""

from __future__ import annotations

import asyncio
import fcntl
import os
import stat
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

from .types import SandboxError


def open_lock_file(path: Path, *, private: bool = True) -> int:
    """Open (creating) a lock file without following links.

    A private lock must be a regular file owned by this uid; shared caches
    (``private=False``) may hold lock files created by group members.
    """
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600 if private else 0o664)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or (private and info.st_uid != os.getuid()):
            raise SandboxError(f"refusing foreign lock file: {path}")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


@asynccontextmanager
async def locked(path: Path, *, private: bool = True, poll_s: float = 0.05) -> AsyncIterator[None]:
    """Hold an exclusive lock on ``path``; waiting stays cancellable."""
    descriptor = open_lock_file(path, private=private)
    try:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                await asyncio.sleep(poll_s)
        yield
    finally:
        os.close(descriptor)


@contextmanager
def locked_sync(path: Path, *, private: bool = True) -> Iterator[None]:
    """Blocking variant of :func:`locked` for code running in worker threads."""
    descriptor = open_lock_file(path, private=private)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)
