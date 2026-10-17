"""Session directories and the janitor that recovers abandoned ones.

Every sandbox owns one private directory below the node-local session root::

    <session root>/hpcsb-<32 hex>/   mode 0700, named after the sandbox
        .lease                       flock held by the owning process
        owner.json                   owner identity, container identity, labels
        rootfs/                      the writable root filesystem
        tmp/  var-tmp/               bound to /tmp and /var/tmp

plus whatever the provider keeps next to them. A session is abandoned when
nobody holds its lease and its recorded owner process is gone. The janitor
asks the provider to stop what still runs for such a session, which proves
process identities and fails closed on any ambiguity, and only then deletes it.

A stopped session leaves the session root by one rename into ``.trash/``, under
the janitor lock, and its files are deleted after the lock is released: no
janitor can lease a session that is being deleted, and starts and other stops
never wait for a deletion. A trash entry is named after the session and the
process deleting it; janitors finish the deletions of processes that died
(:data:`TRASH_GRACE_S` after the entry was moved, so that an ``rm`` the process
left running is not raced).
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import re
import shutil
import signal
import socket
import stat
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import procfs
from .locks import locked, locked_sync, open_lock_file, try_locked
from .types import SandboxError

SCHEMA = "hpc-sandbox/session-v1"
NAME_PREFIX = "hpcsb-"
_NAME = re.compile(r"hpcsb-[0-9a-f]{32}")
LEASE_FILE = ".lease"
OWNER_FILE = "owner.json"
JANITOR_LOCK = ".janitor.lock"
_KILL_WAIT_S = 5.0
TRASH_DIR = ".trash"
# How long after a trash entry was moved a janitor may take over its deletion.
TRASH_GRACE_S = 600.0
# sh -c DELETE_SCRIPT hpc-sandbox-delete TREE [LOG]: delete a trash entry as remove_tree does (refusing
# directories this uid does not own, restoring owner access to directories first), without following links or
# crossing into other mounts; LOG, where the caller sends its errors, is removed when it succeeds.
DELETE_SCRIPT = r"""
tree=$1; log=${2:-}
foreign=$(find "$tree" -xdev -type d ! -uid "$(id -u)" -print 2>/dev/null | head -n 1)
if [ -n "$foreign" ]; then
    echo "hpc-sandbox: refusing to delete $tree: $foreign is not owned by this uid" >&2
    exit 3
fi
find "$tree" -xdev -type d ! -perm -0700 -exec chmod u+rwx -- {} \; 2>/dev/null
rm -rf --one-file-system -- "$tree" || exit
if [ -n "$log" ]; then rm -f -- "$log"; fi
"""

# Stops what still runs for an abandoned session; see :func:`sweep`.
Stopper = Callable[[Path, "dict[str, Any] | None"], Awaitable[bool]]


def new_instance_name() -> str:
    return NAME_PREFIX + uuid.uuid4().hex


@dataclass
class Session:
    """A session directory whose lease this process holds until :meth:`release`."""

    name: str
    path: Path
    lease: int | None

    @property
    def rootfs(self) -> Path:
        return self.path / "rootfs"

    @property
    def tmp(self) -> Path:
        return self.path / "tmp"

    @property
    def var_tmp(self) -> Path:
        return self.path / "var-tmp"

    def write_owner_record(self, **fields: Any) -> None:
        """Atomically publish who owns this session and which container it runs."""
        identity = procfs.start_time(os.getpid())
        record = {
            "schema": SCHEMA,
            "instance_name": self.name,
            "session_directory": str(self.path),
            "hostname": socket.gethostname(),
            "owner_pid": os.getpid(),
            "owner_start_time": identity,
            **fields,
        }
        temporary = self.path / f".{OWNER_FILE}.{os.getpid()}.tmp"
        temporary.write_text(json.dumps(record, sort_keys=True) + "\n")
        temporary.chmod(0o600)
        os.replace(temporary, self.path / OWNER_FILE)

    def release(self) -> None:
        if self.lease is not None:
            os.close(self.lease)
            self.lease = None


def open_session(root: Path, name: str) -> Session:
    """Create and lease ``root/name``.

    The janitor lock makes publication and leasing atomic for janitors: a crash
    before the lease is taken leaves a directory the next janitor recovers.
    """
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with locked_sync(root / JANITOR_LOCK):
        return _lease(root, name)


def _lease(root: Path, name: str) -> Session:
    path = root / name
    path.mkdir(mode=0o700)
    path.chmod(0o700)
    try:
        lease = open_lock_file(path / LEASE_FILE)
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        shutil.rmtree(path, ignore_errors=True)
        raise
    return Session(name, path, lease)


def remove_session(session: Session) -> None:
    """Delete a stopped session's directory and drop its lease.

    rmtree unlinks the lease before the directory disappears, and a janitor
    could then lease a fresh inode and take the half-deleted session for an
    abandoned one. So the directory first leaves the session root by a rename
    into the trash, under the janitor lock (one metadata operation), and is
    deleted after the lock is released.
    """
    doomed = trash_session(session)
    if doomed is not None:
        remove_tree(doomed)


def trash_session(session: Session) -> Path | None:
    """Move a stopped session into the trash under the janitor lock and drop its lease; the caller deletes the
    returned trash entry (None: the session directory is gone already)."""
    doomed = None
    try:
        with locked_sync(session.path.parent / JANITOR_LOCK):
            doomed = move_to_trash(session.path)
    finally:
        session.release()
    return doomed


def _owner_tag() -> str:
    return f"{os.getpid()}-{procfs.start_time(os.getpid())}"


def move_to_trash(path: Path, trash: Path | None = None) -> Path | None:
    """Rename the session directory ``path`` (or a trash entry) into ``trash`` (default: the trash of the
    session root ``path`` is in) under this process's tag; call with the janitor lock held.

    Returns the new path, or None if ``path`` is gone.
    """
    trash = path.parent / TRASH_DIR if trash is None else trash
    trash.mkdir(mode=0o700, exist_ok=True)
    info = trash.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise SandboxError(f"refusing foreign trash directory {trash}")
    target = trash / f"{path.name.split('.', 1)[0]}.{_owner_tag()}.{uuid.uuid4().hex[:8]}"
    try:
        os.rename(path, target)
    except FileNotFoundError:
        return None
    return target


def _claim_dead_trash(root: Path) -> list[Path]:
    """Trash entries whose deleting process is gone, renamed under this process's tag; janitor lock held."""
    trash = root / TRASH_DIR
    claimed: list[Path] = []
    try:
        entries = list(trash.iterdir())
    except OSError:
        return claimed
    for entry in entries:
        if entry.name.startswith(".") and entry.name.endswith(".log"):
            if not (trash / entry.name[1:-4]).exists():
                entry.unlink(missing_ok=True)  # the error log of a deletion that ended
            continue
        parts = entry.name.split(".")
        if len(parts) != 3 or not _NAME.fullmatch(parts[0]):
            continue
        pid, _, started = parts[1].partition("-")
        if pid.isdigit() and procfs.identity_matches(int(pid), started):
            continue  # still being deleted by its live owner
        try:
            info = entry.lstat()
        except OSError:
            continue
        if entry.is_symlink() or info.st_uid != os.getuid():
            continue
        if time.time() - info.st_ctime < TRASH_GRACE_S:
            continue  # an rm its owner left running may still be deleting it
        moved = move_to_trash(entry, trash)
        if moved is not None:
            claimed.append(moved)
    return claimed


async def create_session(root: Path, name: str) -> Session:
    """:func:`open_session` for the event loop.

    The janitor lock is awaited, never waited for in a worker thread: a sweep
    holding it needs worker threads to finish. A session created after the
    caller was cancelled is removed again.
    """
    await asyncio.to_thread(root.mkdir, mode=0o700, parents=True, exist_ok=True)
    async with locked(root / JANITOR_LOCK):
        task = asyncio.ensure_future(asyncio.to_thread(_lease, root, name))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            while not task.done():
                with suppress(asyncio.CancelledError):
                    await asyncio.wait({task})
            if not task.cancelled() and task.exception() is None:
                session = task.result()
                try:
                    await asyncio.to_thread(remove_tree, session.path)
                finally:
                    session.release()
            raise


async def trash_session_awaiting(session: Session) -> Path | None:
    """:func:`trash_session` for the event loop: the janitor lock is awaited,
    never waited for in a worker thread (see :func:`create_session`)."""
    doomed = None
    try:
        async with locked(session.path.parent / JANITOR_LOCK):
            doomed = await asyncio.to_thread(move_to_trash, session.path)
    finally:
        session.release()
    return doomed


def remove_tree(path: Path) -> None:
    """Remove a private tree, restoring owner access to directories first.

    Tasks and package managers create directories with mode 000 or 0555.
    Owner rwx is restored top-down, without following links, so rmtree can
    traverse them. Directories owned by anyone else, and mount points (a
    rootfs still mounted, alive or not), stop the removal.
    """
    try:
        device = path.lstat().st_dev
    except FileNotFoundError:
        return

    def make_traversable(directory: Path) -> None:
        info = directory.lstat()
        if stat.S_ISLNK(info.st_mode):
            return
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
            raise SandboxError(f"refusing to remove a directory this uid does not own: {directory}")
        if info.st_dev != device:
            raise SandboxError(f"refusing to remove a tree through the mount point {directory}")
        directory.chmod(stat.S_IMODE(info.st_mode) | 0o700)

    if path.is_symlink():
        raise SandboxError(f"refusing to remove a symlinked tree: {path}")
    make_traversable(path)

    def fail(error: OSError) -> None:
        raise error

    for directory, directories, _files in os.walk(path, onerror=fail):
        for name in directories:
            make_traversable(Path(directory) / name)
    shutil.rmtree(path)


@dataclass
class JanitorReport:
    stopped: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    live: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _read_owner_record(path: Path) -> dict[str, Any] | None:
    try:
        record = json.loads((path / OWNER_FILE).read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return record if isinstance(record, dict) else None


def record_provider(record: dict[str, Any] | None) -> str | None:
    """The provider an owner record belongs to, or None if it cannot tell.

    Records name their provider; older ones are recognized by their fields.
    """
    if not record:
        return None
    if isinstance(record.get("provider"), str):
        return record["provider"]
    if "instance_pid_file" in record:
        return "singularity"
    if "anchor_pid" in record or "init_pid" in record:
        return "enroot"
    return None


def labels_match(labels: Any, wanted: Mapping[str, str]) -> bool:
    return isinstance(labels, dict) and all(labels.get(k) == v for k, v in wanted.items())


async def sweep(
    root: Path,
    stop: Stopper,
    *,
    labels: Mapping[str, str] | None = None,
    provider: str | None = None,
    claim_unknown: bool = True,
    wait: bool = True,
) -> JanitorReport | None:
    """Stop and remove abandoned sessions below ``root``.

    A session is abandoned when nobody holds its lease and its recorded owner
    process is gone. With ``labels``, only sessions whose labels include them
    are considered. With ``provider``, sessions of another provider (see
    :func:`record_provider`) are skipped, since only their provider knows how
    to stop them, and so are sessions of no recognizable provider unless
    ``claim_unknown``. For each remaining one, ``stop(path, record)`` stops
    whatever still runs; ``record`` is the owner record if it is bound to
    this session on this host, else None. Live sessions and unrecognized
    directories are never touched; foreign directories and failed stops are
    reported and kept. With ``wait=False`` it sweeps only if no other process
    holds the root's janitor lock right now, and returns None when one does.
    """
    report = JanitorReport()
    if not root.is_dir():
        return report
    doomed: list[Path] = []
    janitor = locked(root / JANITOR_LOCK) if wait else try_locked(root / JANITOR_LOCK)
    async with janitor as held:
        if held is False:
            return None
        for path in sorted(root.iterdir()):
            if not _NAME.fullmatch(path.name) or path.is_symlink() or not path.is_dir():
                continue
            info = path.lstat()
            if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
                report.errors.append(f"refusing foreign or non-private session {path}")
                continue
            if provider is not None:
                owner = record_provider(_read_owner_record(path))
                if owner != provider and (owner is not None or not claim_unknown):
                    continue
            await _sweep_session(path, stop, labels, report, doomed)
        try:
            doomed += _claim_dead_trash(root)
        except (OSError, SandboxError) as error:
            report.errors.append(f"could not claim the trash of {root}: {error}")
    for path in doomed:
        try:
            await asyncio.to_thread(remove_tree, path)
        except (OSError, SandboxError) as error:
            report.errors.append(f"could not delete {path.name}: {error}")
    return report


async def _sweep_session(
    path: Path,
    stop: Stopper,
    labels: Mapping[str, str] | None,
    report: JanitorReport,
    doomed: list[Path],
) -> None:
    try:
        lease = open_lock_file(path / LEASE_FILE)
    except (OSError, SandboxError) as error:
        report.errors.append(f"could not open the lease of {path.name}: {error}")
        return
    try:
        try:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            report.live.append(path.name)
            return
        record = _read_owner_record(path)
        if labels is not None and not (record and labels_match(record.get("labels"), labels)):
            return
        owner_pid, owner_start = (
            (record or {}).get("owner_pid"),
            (record or {}).get("owner_start_time"),
        )
        if isinstance(owner_pid, int) and procfs.identity_matches(owner_pid, str(owner_start)):
            report.live.append(path.name)
            return
        bound = bool(
            record
            and record.get("schema") == SCHEMA
            and record.get("instance_name") == path.name
            and record.get("session_directory") == str(path)
            and record.get("hostname") == socket.gethostname()
        )
        if await stop(path, record if bound else None):
            report.stopped.append(path.name)
        moved = move_to_trash(path)
        if moved is not None:
            doomed.append(moved)
        report.removed.append(path.name)
    except (OSError, SandboxError) as error:
        report.errors.append(f"could not clean {path.name}: {error}")
    finally:
        os.close(lease)


def read_bound_record(path: Path) -> dict[str, Any] | None:
    """The owner record of the session at ``path`` if it is bound to it on this host."""
    record = _read_owner_record(path)
    bound = bool(
        record
        and record.get("schema") == SCHEMA
        and record.get("instance_name") == path.name
        and record.get("session_directory") == str(path)
        and record.get("hostname") == socket.gethostname()
    )
    return record if bound else None


def kill_exact(processes: list[tuple[int, str]], name: str) -> bool:
    """SIGKILL the processes ``(pid, start time)`` that still run sandbox ``name``.

    A process is signalled only if its start time matches and its command
    line names the sandbox; return whether any was running.
    """
    killed = []
    for pid, started in processes:
        if procfs.owned_live_start_time(pid) != started:
            continue
        command = procfs.cmdline(pid)
        if command and name not in command:  # empty while it exits
            raise SandboxError(f"refusing to stop {name!r}: PID {pid} identity changed")
        signal_kill(pid)
        killed.append((pid, started))
    deadline = time.monotonic() + _KILL_WAIT_S
    for pid, started in killed:
        while procfs.owned_live_start_time(pid) == started:
            if time.monotonic() > deadline:
                raise SandboxError(f"process {pid} of {name!r} survived SIGKILL")
            time.sleep(0.02)
    return bool(killed)


def signal_kill(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
