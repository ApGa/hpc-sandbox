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
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import socket
import stat
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import procfs
from .locks import locked, locked_sync, open_lock_file
from .types import SandboxError

SCHEMA = "hpc-sandbox/session-v1"
NAME_PREFIX = "hpcsb-"
_NAME = re.compile(r"hpcsb-[0-9a-f]{32}")
LEASE_FILE = ".lease"
OWNER_FILE = "owner.json"
JANITOR_LOCK = ".janitor.lock"

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
    """Delete a stopped session's directory, then drop its lease."""
    try:
        # rmtree unlinks the lease before the directory disappears; holding the
        # janitor lock stops a janitor from leasing a fresh inode meanwhile.
        with locked_sync(session.path.parent / JANITOR_LOCK):
            remove_tree(session.path)
    finally:
        session.release()


def remove_tree(path: Path) -> None:
    """Remove a private tree, restoring owner access to directories first.

    Tasks and package managers create directories with mode 000 or 0555.
    Owner rwx is restored top-down, without following links, so rmtree can
    traverse them. Directories owned by anyone else stop the removal.
    """

    def make_traversable(directory: Path) -> None:
        info = directory.lstat()
        if stat.S_ISLNK(info.st_mode):
            return
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
            raise SandboxError(f"refusing to remove a directory this uid does not own: {directory}")
        directory.chmod(stat.S_IMODE(info.st_mode) | 0o700)

    if path.is_symlink():
        raise SandboxError(f"refusing to remove a symlinked tree: {path}")
    try:
        make_traversable(path)
    except FileNotFoundError:
        return

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


def labels_match(labels: Any, wanted: Mapping[str, str]) -> bool:
    return isinstance(labels, dict) and all(labels.get(k) == v for k, v in wanted.items())


async def sweep(
    root: Path,
    stop: Stopper,
    *,
    labels: Mapping[str, str] | None = None,
) -> JanitorReport:
    """Stop and remove abandoned sessions below ``root``.

    A session is abandoned when nobody holds its lease and its recorded owner
    process is gone. With ``labels``, only sessions whose labels include them
    are considered. For each one, ``stop(path, record)`` stops whatever still
    runs; ``record`` is the owner record if it is bound to this session on
    this host, else None. Live sessions and unrecognized directories are never
    touched; foreign directories and failed stops are reported and kept.
    """
    report = JanitorReport()
    if not root.is_dir():
        return report
    async with locked(root / JANITOR_LOCK):
        for path in sorted(root.iterdir()):
            if not _NAME.fullmatch(path.name) or path.is_symlink() or not path.is_dir():
                continue
            info = path.lstat()
            if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
                report.errors.append(f"refusing foreign or non-private session {path}")
                continue
            await _sweep_session(path, stop, labels, report)
    return report


async def _sweep_session(
    path: Path,
    stop: Stopper,
    labels: Mapping[str, str] | None,
    report: JanitorReport,
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
        remove_tree(path)
        report.removed.append(path.name)
    except (OSError, SandboxError) as error:
        report.errors.append(f"could not clean {path.name}: {error}")
    finally:
        os.close(lease)
