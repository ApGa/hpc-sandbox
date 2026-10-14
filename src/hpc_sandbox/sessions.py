"""Session directories, exact instance teardown and the janitor.

Every sandbox owns one private directory below the node-local session root::

    <session root>/hpcsb-<32 hex>/   mode 0700, named after the instance
        .lease                       flock held by the owning process
        owner.json                   owner identity, instance identity, labels
        instance.pid                 written by ``instance start --pid-file``
        rootfs/                      the writable root filesystem
        tmp/  var-tmp/               bound to /tmp and /var/tmp

Singularity CE and Apptainer keep unprivileged instance metadata below the
passwd home and ignore ``SINGULARITY_CONFIGDIR``, so instance listings cannot
be scoped to a session root. Teardown therefore acts on one exact UUID name, and
only when the runtime's PID, the PID file, the recorded PID and the Linux start
time agree; it fails closed on any ambiguity. Nothing here uses wildcards or
``instance stop --all``.
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
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import procfs
from .locks import locked, locked_sync, open_lock_file
from .runtime import ContainerRuntime, instance_pid
from .types import SandboxError

SCHEMA = "hpc-sandbox/session-v1"
NAME_PREFIX = "hpcsb-"
_NAME = re.compile(r"hpcsb-[0-9a-f]{32}")
LEASE_FILE = ".lease"
OWNER_FILE = "owner.json"
PID_FILE = "instance.pid"
JANITOR_LOCK = ".janitor.lock"
CONTROL_LOCK = ".instance-control.lock"


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

    @property
    def pid_file(self) -> Path:
        return self.path / PID_FILE

    def write_owner_record(self, **fields: Any) -> None:
        """Atomically publish who owns this session and which instance it runs."""
        identity = procfs.start_time(os.getpid())
        record = {
            "schema": SCHEMA,
            "instance_name": self.name,
            "session_directory": str(self.path),
            "hostname": socket.gethostname(),
            "owner_pid": os.getpid(),
            "owner_start_time": identity,
            "instance_pid_file": str(self.pid_file),
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


def read_pid_file(path: Path) -> int | None:
    """The PID written by ``--pid-file``; None if it does not exist yet."""
    try:
        info = os.stat(path, follow_symlinks=False)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o022
        ):
            raise SandboxError(f"refusing unsafe instance PID file: {path}")
        pid = int(path.read_text().strip())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as error:
        raise SandboxError(f"invalid instance PID file {path}: {error}") from error
    if pid <= 0:
        raise SandboxError(f"invalid instance PID {pid} in {path}")
    return pid


def terminate_exact(pid: int, name: str, start_time: str, *, grace_s: float) -> None:
    """SIGTERM, then SIGKILL, the instance process ``pid`` after re-proving its identity."""
    for sig, wait_s in ((signal.SIGTERM, grace_s), (signal.SIGKILL, 5.0)):
        if procfs.owned_live_start_time(pid) != start_time:
            return  # gone, or a zombie waiting for its reaper
        if procfs.instance_start_time(pid, name) != start_time:
            raise SandboxError(f"refusing to signal instance {name!r}: PID identity changed")
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return
        deadline = time.monotonic() + wait_s
        while time.monotonic() < deadline and procfs.owned_live_start_time(pid) == start_time:
            time.sleep(0.05)
    if procfs.owned_live_start_time(pid) == start_time:
        raise SandboxError(f"instance process {name!r} survived SIGKILL")


async def stop_instance(
    runtime: ContainerRuntime,
    name: str,
    *,
    pid_file: Path,
    expected_pid: int | None,
    expected_start_time: str | None,
    grace_s: float,
    timeout_s: float,
) -> bool:
    """Stop exactly the instance ``name``; return whether anything was running.

    Call with the instance-control lock held.
    """
    record = await runtime.instance_record(name)
    pid_from_file = read_pid_file(pid_file)
    if record is not None:
        runtime_pid = instance_pid(record)
        current = procfs.owned_live_start_time(runtime_pid)
        if pid_from_file != runtime_pid:
            raise SandboxError(f"refusing to stop {name!r}: PID file and runtime disagree")
        if current is None:
            raise SandboxError(f"refusing to stop {name!r}: process identity cannot be proved")
        if expected_pid is not None and expected_pid != runtime_pid:
            raise SandboxError(f"refusing to stop {name!r}: recorded PID changed")
        if expected_start_time is not None and expected_start_time != current:
            raise SandboxError(f"refusing to stop {name!r}: PID was reused")
        result = await runtime.run(
            "instance", "stop", "--timeout", f"{grace_s:g}", name, timeout_s=timeout_s
        )
        if await runtime.instance_record(name) is not None:
            raise SandboxError(f"instance {name!r} survived stop: {result.detail()}")
        await asyncio.to_thread(terminate_exact, runtime_pid, name, current, grace_s=2.0)
        return True
    if (
        expected_pid is not None
        and expected_start_time is not None
        and procfs.owned_live_start_time(expected_pid) == expected_start_time
    ):
        if pid_from_file not in (None, expected_pid):
            raise SandboxError(f"refusing direct cleanup of {name!r}: PID file changed")
        await asyncio.to_thread(
            terminate_exact, expected_pid, name, expected_start_time, grace_s=2.0
        )
        return True
    if pid_from_file is not None and procfs.instance_start_time(pid_from_file, name) is not None:
        raise SandboxError(f"unowned live instance process remains for {name!r}")
    return False


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
    runtime: ContainerRuntime,
    *,
    labels: Mapping[str, str] | None = None,
    grace_s: float = 10.0,
    stop_timeout_s: float = 60.0,
) -> JanitorReport:
    """Stop and remove abandoned sessions below ``root``.

    A session is abandoned when nobody holds its lease and its recorded owner
    process is gone. With ``labels``, only sessions whose labels include them
    are considered. Live sessions and unrecognized directories are never
    touched; foreign directories and ambiguous identities are reported.
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
            await _sweep_session(path, runtime, labels, grace_s, stop_timeout_s, report)
    return report


async def _sweep_session(
    path: Path,
    runtime: ContainerRuntime,
    labels: Mapping[str, str] | None,
    grace_s: float,
    stop_timeout_s: float,
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
            and record.get("instance_pid_file") == str(path / PID_FILE)
        )
        async with locked(path.parent / CONTROL_LOCK):
            if not bound and await runtime.instance_record(path.name) is not None:
                raise SandboxError("instance is running but its owner record is not bound to it")
            expected_pid = record.get("instance_pid") if bound and record else None
            expected_start = record.get("instance_start_time") if bound and record else None
            if await stop_instance(
                runtime,
                path.name,
                pid_file=path / PID_FILE,
                expected_pid=expected_pid if isinstance(expected_pid, int) else None,
                expected_start_time=str(expected_start) if expected_start else None,
                grace_s=grace_s,
                timeout_s=stop_timeout_s,
            ):
                report.stopped.append(path.name)
        remove_tree(path)
        report.removed.append(path.name)
    except (OSError, SandboxError) as error:
        report.errors.append(f"could not clean {path.name}: {error}")
    finally:
        os.close(lease)
