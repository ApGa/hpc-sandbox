"""Linux procfs helpers: stable process identities, process trees and PSS.

A PID alone is not an identity: it can be reused as soon as its process is
reaped. Everything that signals a process it did not just spawn first proves
the identity ``(pid, start time)`` from ``/proc/<pid>/stat``.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
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


def parent(pid: int) -> int | None:
    """The PID of ``pid``'s parent, or None if it is gone."""
    fields = _stat_fields(pid)
    return int(fields[1]) if fields else None


def identity(pid: int) -> tuple[str, int] | None:
    """The start time and session ID of ``pid``, or None if it is gone."""
    fields = _stat_fields(pid)
    return (fields[19], int(fields[3])) if fields else None


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


def argv(pid: int) -> list[str] | None:
    """The arguments of ``pid``, None if unreadable (or a zombie's)."""
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return None
    if not raw:
        return None
    return [os.fsdecode(part) for part in raw.rstrip(b"\0").split(b"\0")]


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
    """Proportional set size of ``pid``; see :func:`memory_bytes`."""
    return memory_bytes(pid)[0]


# Where a process's PSS and private memory are read: smaps_rollup sums smaps, which kernels
# before 4.14 have alone.
_SMAPS = ("smaps_rollup", "smaps")


def memory_bytes(pid: int) -> tuple[int, int]:
    """Proportional set size and private memory (what its death alone frees) of ``pid``.

    They come from ``smaps_rollup``, or from ``smaps`` where the kernel has no rollup; a
    process whose main thread exited answers through another thread. Both fall back to the
    RSS where these cannot be read: the setuid Singularity starter is not dumpable, and
    charging its RSS is conservative. A vanished or zombie process counts as zero.
    """
    proc = Path(f"/proc/{pid}")
    name = next((name for name in _SMAPS if (proc / name).exists()), _SMAPS[0])
    try:
        rollup = _rollup(proc / name)
        if rollup is None:  # its main thread exited: ask one of the others
            for thread in (proc / "task").iterdir():
                if thread.name != str(pid) and (rollup := _rollup(thread / name)):
                    break
        return rollup or (0, 0)
    except PermissionError:
        pass
    except OSError:
        return 0, 0
    try:
        for line in (proc / "status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1]) * 1024
                return rss, rss
    except OSError:
        pass
    return 0, 0


def _rollup(path: Path) -> tuple[int, int] | None:
    """The Pss and private memory ``path`` (a smaps_rollup or smaps file) sums up, or None if
    it holds no Pss. Not Private_Hugetlb: the Pss leaves hugetlb pages out, and the two are
    compared."""
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None
    except PermissionError:
        raise
    except OSError:
        return None
    kib = dict.fromkeys(("Pss", "Private_Clean", "Private_Dirty"), 0)
    found = False
    for line in text.splitlines():
        name, colon, value = line.partition(":")
        if colon and name in kib and (fields := value.split()) and fields[0].isdigit():
            kib[name] += int(fields[0])
            found = found or name == "Pss"
    if not found:
        return None
    return kib["Pss"] * 1024, (kib["Private_Clean"] + kib["Private_Dirty"]) * 1024


def oom_score_adj(pid: int) -> int | None:
    """``oom_score_adj`` of ``pid`` (-1000 to 1000), None if unreadable."""
    try:
        return int(Path(f"/proc/{pid}/oom_score_adj").read_text())
    except (OSError, ValueError):
        return None


# Where a sandbox's own tmpfs mounts sit; others it sees may be the host's.
SANDBOX_TMPFS = (
    "/dev",
    "/dev/shm",
    "/tmp",
    "/var/tmp",
    "/run",
    "/run/lock",
    "/var/run",
    "/var/lock",
)


def tmpfs_bytes(pid: int) -> int | None:
    """Bytes held by files on the tmpfs mounted at :data:`SANDBOX_TMPFS` in ``pid``'s view.

    Tried through ``pid`` and then its descendants, until one's mount table is
    readable (a setuid runtime's own processes are not); None if none is.
    """
    for candidate in (pid, *descendants([pid])):
        try:
            mountinfo = Path(f"/proc/{candidate}/mountinfo").read_bytes()
        except OSError:
            continue
        used = 0
        for mount_point in sandbox_tmpfs(mountinfo):
            try:
                info = os.statvfs(f"/proc/{candidate}/root{mount_point}")
            except OSError:
                continue
            used += (info.f_blocks - info.f_bfree) * info.f_frsize
        return used
    return None


def sandbox_tmpfs(mountinfo: str | bytes) -> list[str]:
    """Mount points of the tmpfs file systems at :data:`SANDBOX_TMPFS`, one per file system.

    Only whole file systems count: a directory of a host tmpfs bound there
    (``/tmp`` from a scratch directory in RAM) would report every file of that
    tmpfs.
    """
    found: list[str] = []
    devices = set()
    for mount in mounts(mountinfo):
        if mount.fstype != "tmpfs" or mount.root != "/" or mount.point not in SANDBOX_TMPFS:
            continue
        if mount.device not in devices and mount.point not in found:  # statvfs sees the top mount
            devices.add(mount.device)
            found.append(mount.point)
    return found


@dataclass(frozen=True)
class Mount:
    """One mount of a mount table (``/proc/<pid>/mountinfo``)."""

    device: str  # major:minor
    root: str  # what of its file system is mounted
    point: str
    fstype: str
    source: str
    options: str  # the file system's


def mounts(mountinfo: str | bytes) -> list[Mount]:
    """The mounts of a mount table, read as the kernel writes it: a line per mount, fields
    separated by single spaces, and paths with spaces, tabs, newlines and backslashes octal-
    escaped (and nothing else). Paths are decoded as file names; malformed lines are skipped."""
    raw = os.fsencode(mountinfo) if isinstance(mountinfo, str) else mountinfo
    found = []
    for line in raw.split(b"\n"):
        fields = line.split(b" ")
        try:  # optional fields from the seventh, then "-", the type, the source, the options
            separator = fields.index(b"-", 6)
            fstype, source, options = fields[separator + 1 : separator + 4]
        except ValueError:
            continue
        found.append(
            Mount(
                os.fsdecode(fields[2]),
                _path(fields[3]),
                _path(fields[4]),
                os.fsdecode(fstype),
                _path(source),
                os.fsdecode(options),
            )
        )
    return found


def mount_table(pid: int | str = "self") -> list[Mount]:
    """The mounts ``pid`` sees."""
    return mounts(Path(f"/proc/{pid}/mountinfo").read_bytes())


def _path(field: bytes) -> str:
    return os.fsdecode(re.sub(rb"\\([0-7]{3})", lambda match: bytes([int(match[1], 8)]), field))
