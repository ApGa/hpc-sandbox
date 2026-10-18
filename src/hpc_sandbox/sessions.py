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
process that moved it, and whatever deletes it holds an flock on it; janitors
take over the entries nobody holds whose process died, or that have waited for
a deletion longer than :data:`TRASH_GRACE_S`.
"""

from __future__ import annotations

import asyncio
import errno
import fcntl
import json
import os
import re
import shutil
import signal
import socket
import stat
import subprocess
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import procfs
from .locks import locked, locked_sync, open_lock_file, try_locked
from .process import run_process
from .types import SandboxError

SCHEMA = "hpc-sandbox/session-v1"
NAME_PREFIX = "hpcsb-"
_NAME = re.compile(r"hpcsb-[0-9a-f]{32}")
LEASE_FILE = ".lease"
OWNER_FILE = "owner.json"
JANITOR_LOCK = ".janitor.lock"
_KILL_WAIT_S = 5.0
TRASH_DIR = ".trash"
# Deletions of one session root wait for this lock, to run one at a time.
DELETIONS_LOCK = ".deletions.lock"
# A full scratch (blocks, inodes or quota): no room even to move a session into the trash.
_NO_ROOM = (errno.ENOSPC, errno.EDQUOT)
_DELETE_TIMEOUT_S = 3600.0
# A sweep holds at most this many trash entries at once, each by a descriptor until it is
# deleted: past that, it deletes them, takes the janitor lock again and goes on.
_HELD_AT_ONCE = 64
# How long after a trash entry was moved a janitor may take over its deletion while the process that moved it
# lives, if no deletion holds it: that process may be about to start one.
TRASH_GRACE_S = 60.0
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
    prepare_session_root(root)
    with locked_sync(root / JANITOR_LOCK):
        return _lease(root, name)


def prepare_session_root(root: Path) -> None:
    """Create a session root with its trash and deletion lock, so that a stop
    on a scratch filled meanwhile needs no new inode."""
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    (root / TRASH_DIR).mkdir(mode=0o700, exist_ok=True)
    os.close(open_lock_file(root / DELETIONS_LOCK))


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


def leased(path: Path) -> bool:
    """Whether a live process holds the lease of the session directory ``path``."""
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK  # a FIFO there never blocks
    try:
        descriptor = os.open(path / LEASE_FILE, flags)
    except OSError:
        return False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            return False
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    except OSError:
        return False
    finally:
        os.close(descriptor)
    return False


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


def _claim_dead_trash(root: Path, claimed: list[tuple[Path, int | None]], taken: set[str]) -> bool:
    """Add to ``claimed`` the trash entries no deletion holds whose mover is gone (or late), renamed
    under this process's tag and held, until it holds :data:`_HELD_AT_ONCE`; janitor lock held.
    ``taken`` names the entries the sweep claimed before. Returns whether it stopped there."""
    trash = root / TRASH_DIR
    try:
        entries = list(trash.iterdir())
    except OSError:
        return False
    for entry in entries:
        if entry.name in taken:
            continue  # the sweep could not delete it
        if len(claimed) >= _HELD_AT_ONCE:
            return True
        if entry.name.startswith(".") and entry.name.endswith(".log"):
            if not (trash / entry.name[1:-4]).exists():
                entry.unlink(missing_ok=True)  # the error log of a deletion that ended
            continue
        parts = entry.name.split(".")
        if len(parts) != 3 or not _NAME.fullmatch(parts[0]):
            continue
        try:
            info = entry.lstat()
        except OSError:
            continue
        if info.st_uid != os.getuid() or stat.S_ISLNK(info.st_mode):
            continue
        if not stat.S_ISDIR(info.st_mode):
            entry.unlink(missing_ok=True)  # no session's: left where one was by older versions
            continue
        held = hold_trash_entry(entry)
        if held is None:
            continue  # a deletion holds it
        pid, _, started = parts[1].partition("-")
        owner_alive = pid.isdigit() and procfs.identity_matches(int(pid), started)
        moved = None
        try:
            if not owner_alive or time.time() - info.st_ctime >= TRASH_GRACE_S:
                moved = move_to_trash(entry, trash)  # else its owner may be about to delete it
        finally:
            if moved is None:
                os.close(held)
        if moved is not None:
            claimed.append((moved, held))
            taken.add(moved.name)
    return False


async def create_session(root: Path, name: str) -> Session:
    """:func:`open_session` for the event loop.

    The janitor lock is awaited, never waited for in a worker thread: a sweep
    holding it needs worker threads to finish. The lease is taken inline, a
    few metadata operations on local scratch, so that the node-wide lock is
    never held while a busy executor gets to a worker thread.
    """
    await asyncio.to_thread(prepare_session_root, root)
    async with locked(root / JANITOR_LOCK):
        return _lease(root, name)


async def trash_session_awaiting(session: Session) -> Path | None:
    """Move a stopped session into the trash under the janitor lock and drop its
    lease; the caller deletes the returned trash entry (None: the session is gone
    already). The lock is awaited, never waited for in a worker thread, and the
    session is moved inline (see :func:`create_session`).

    rmtree unlinks the lease before the directory disappears, and a janitor
    could then lease a fresh inode and take the half-deleted session for an
    abandoned one: so a session leaves the session root before it is deleted.
    """
    doomed = None
    try:
        async with locked(session.path.parent / JANITOR_LOCK):
            try:
                doomed = move_to_trash(session.path)
            except OSError as error:
                if error.errno not in _NO_ROOM:
                    raise
                # No room even for that: delete it where it is, as the lease and the lock
                # keep janitors away.
                await delete_tree(session.path)
    finally:
        session.release()
    return doomed


def hold_trash_entry(entry: Path) -> int | None:
    """An open descriptor of the trash entry ``entry`` holding its lock, which tells janitors that a
    deletion has it; None if it is gone or held already."""
    try:
        descriptor = os.open(entry, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        return None
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(descriptor)
        return None
    return descriptor


def _being_deleted(entry: Path) -> bool:
    """Whether a deletion holds the trash entry ``entry``."""
    held = hold_trash_entry(entry)
    if held is not None:
        os.close(held)
    return held is None and entry.is_dir()


async def delete_trash_entry(path: Path, held: int | None) -> None:
    """Delete a trash entry that ``held`` holds (see :func:`hold_trash_entry`), then let it go."""
    try:
        await delete_tree(path)
    finally:
        if held is not None:
            os.close(held)


async def delete_tree(path: Path) -> None:
    """Delete ``path`` as :data:`DELETE_SCRIPT` does, in a child process: no worker thread, and no
    limit on the tree's depth."""
    argv = ["/bin/sh", "-c", DELETE_SCRIPT, "hpc-sandbox-delete", str(path)]
    result = await run_process(argv, timeout_s=_DELETE_TIMEOUT_S)
    if result.returncode != 0 or result.timed_out:
        raise SandboxError(f"could not delete {path}: {result.detail()}")


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
        try:
            info = directory.lstat()
        except FileNotFoundError:  # another deletion of the tree got there first
            return
        if stat.S_ISLNK(info.st_mode):
            return
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
            raise SandboxError(f"refusing to remove a directory this uid does not own: {directory}")
        if info.st_dev != device:
            raise SandboxError(f"refusing to remove a tree through the mount point {directory}")
        with suppress(FileNotFoundError):
            directory.chmod(stat.S_IMODE(info.st_mode) | 0o700)

    if path.is_symlink():
        raise SandboxError(f"refusing to remove a symlinked tree: {path}")
    make_traversable(path)

    def fail(error: OSError) -> None:
        if not isinstance(error, FileNotFoundError):
            raise error

    try:
        for directory, directories, _files in os.walk(path, onerror=fail):
            for name in directories:
                make_traversable(Path(directory) / name)
        while os.path.lexists(path):
            with suppress(FileNotFoundError):  # another deletion removed an entry first: go on
                shutil.rmtree(path)
    except RecursionError:  # deeper than Python recurses: rm walks it without limit
        argv = ["/bin/sh", "-c", DELETE_SCRIPT, "hpc-sandbox-delete", str(path)]
        result = subprocess.run(
            argv, capture_output=True, text=True, timeout=_DELETE_TIMEOUT_S, check=False
        )
        if result.returncode != 0:
            raise SandboxError(f"could not delete {path}: {result.stderr.strip()}") from None


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
    delete: bool = True,
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
    With ``delete=False`` it only stops: the caller deletes ``root`` whole.

    It holds at most :data:`_HELD_AT_ONCE` trash entries at once: past that, it
    deletes them after the janitor lock and takes the lock again for the rest.
    """
    report = JanitorReport()
    if not root.is_dir():
        return report
    seen: set[str] = set()  # the sessions it looked at
    taken: set[str] = set()  # the trash entries it claimed
    doomed: list[tuple[Path, int | None]] = []
    first = True
    try:
        while True:
            janitor = locked(root / JANITOR_LOCK) if wait else try_locked(root / JANITOR_LOCK)
            async with janitor as acquired:
                if acquired is False:  # another process holds it, and takes what is left
                    return None if first else report
                first = full = False
                for path in sorted(root.iterdir()):
                    if not _NAME.fullmatch(path.name) or path.name in seen:
                        continue
                    if path.is_symlink() or not path.is_dir():
                        continue
                    if len(doomed) >= _HELD_AT_ONCE:
                        full = True
                        break
                    seen.add(path.name)
                    info = path.lstat()
                    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
                        report.errors.append(f"refusing foreign or non-private session {path}")
                        continue
                    if provider is not None:
                        owner = record_provider(_read_owner_record(path))
                        if owner != provider and (owner is not None or not claim_unknown):
                            continue
                    await _sweep_session(path, stop, labels, report, doomed if delete else None)
                try:
                    if delete and not full:
                        full = _claim_dead_trash(root, doomed, taken)
                except (OSError, SandboxError) as error:
                    report.errors.append(f"could not claim the trash of {root}: {error}")
            while doomed:  # each one handed over before it is deleted
                path, held = doomed.pop(0)
                try:
                    await delete_trash_entry(path, held)
                except (OSError, SandboxError) as error:
                    report.errors.append(f"could not delete {path.name}: {error}")
            if not full:
                return report
    finally:  # cancelled or failed: let go of what is left, for other janitors to take
        for _, held in doomed:
            if held is not None:
                os.close(held)


async def _sweep_session(
    path: Path,
    stop: Stopper,
    labels: Mapping[str, str] | None,
    report: JanitorReport,
    doomed: list[tuple[Path, int | None]] | None,
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
        if doomed is None:  # stop only
            return
        try:
            moved = move_to_trash(path)
        except OSError as error:
            if error.errno not in _NO_ROOM:
                raise
            await delete_tree(path)  # no room to move it: here, under its lease and the lock
            moved = None
        if moved is not None:
            doomed.append((moved, hold_trash_entry(moved)))
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
