"""Linux procfs helpers: stable process identities, process trees and PSS.

A PID alone is not an identity: it can be reused as soon as its process is
reaped. Everything that signals a process it did not just spawn first proves
the identity ``(pid, start time)`` from ``/proc/<pid>/stat``.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path

_INSTANCE_TITLES = ("Singularity instance:", "Apptainer instance:")


def _stat_fields(pid: int) -> list[str] | None:
    """Return the ``stat`` fields after the command name (field 3 onwards)."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    end = stat.rfind(")")
    if end < 0:
        return None
    fields = stat[end + 2 :].split()
    return fields if len(fields) > 19 else None


def start_time(pid: int) -> str | None:
    """Return the process start time (clock ticks since boot), zombies included."""
    fields = _stat_fields(pid)
    return fields[19] if fields else None


def identity_matches(pid: int, expected_start_time: str) -> bool:
    """Whether ``pid`` still names the same process; a zombie still matches."""
    return pid > 0 and start_time(pid) == expected_start_time


def owned_live_start_time(pid: int) -> str | None:
    """Return the start time of a live (non-zombie) process owned by this uid."""
    if pid <= 0:
        return None
    try:
        owner = Path(f"/proc/{pid}").stat().st_uid
    except OSError:
        return None
    fields = _stat_fields(pid)
    if owner != os.getuid() or fields is None or fields[0] == "Z":
        return None
    return fields[19]


def instance_start_time(pid: int, name: str) -> str | None:
    """Return the start time only if ``pid`` is our live instance process ``name``."""
    try:
        command = (
            Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
        )
    except OSError:
        return None
    if name not in command or not any(title in command for title in _INSTANCE_TITLES):
        return None
    return owned_live_start_time(pid)


def cmdline(pid: int) -> str | None:
    """The command line of ``pid`` with arguments joined by spaces."""
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return None
    return raw.replace(b"\0", b" ").decode(errors="replace")


def environ(pid: int) -> dict[str, str] | None:
    """The environment ``pid`` was started with (it must belong to this uid)."""
    try:
        raw = Path(f"/proc/{pid}/environ").read_bytes()
    except OSError:
        return None
    entries = (item.decode(errors="replace").partition("=") for item in raw.split(b"\0") if item)
    return {key: value for key, separator, value in entries if separator and key}


def namespace_pid(pid: int) -> int | None:
    """The PID of ``pid`` inside its innermost PID namespace (1 for an init)."""
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("NSpid:"):
                return int(line.split()[-1])
    except (OSError, ValueError, IndexError):
        return None
    return None


def children(pid: int) -> list[int]:
    """Direct children of every thread of ``pid``."""
    result: list[int] = []
    try:
        threads = list(Path(f"/proc/{pid}/task").iterdir())
    except OSError:
        return result
    for thread in threads:
        try:
            result.extend(int(child) for child in (thread / "children").read_text().split())
        except (OSError, ValueError):
            continue
    return result


def descendants(roots: Iterable[int]) -> list[int]:
    """Every live descendant of ``roots`` (excluding the roots), parents first."""
    seen = set(roots)
    pending = list(seen)
    found: list[int] = []
    while pending:
        for child in children(pending.pop()):
            if child not in seen:
                seen.add(child)
                found.append(child)
                pending.append(child)
    return found


def resident_bytes(pid: int) -> int:
    """Proportional set size of ``pid``, falling back to RSS when PSS is unreadable.

    The setuid Singularity starter is not dumpable, so its ``smaps_rollup``
    raises ``PermissionError``; charging its RSS is conservative. A vanished or
    zombie process counts as zero.
    """
    proc = Path(f"/proc/{pid}")
    try:
        for line in (proc / "smaps_rollup").read_text().splitlines():
            if line.startswith("Pss:"):
                return int(line.split()[1]) * 1024
        return 0
    except PermissionError:
        pass
    except OSError:
        return 0
    try:
        for line in (proc / "status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0
