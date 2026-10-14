"""What every provider shares: sandboxes backed by a session directory.

Each sandbox owns one private session directory on node-local scratch (see
:mod:`hpc_sandbox.sessions`) holding its writable root filesystem and the
directories bound to its ``/tmp`` and ``/var/tmp``. This module implements
everything that does not depend on the container runtime: spec validation,
command bookkeeping, file transfers through the private ``/tmp``, snapshots,
the memory watchdog and cancellation-safe teardown. A provider supplies how to
fill a root filesystem from an image, and its sandbox class supplies how to
start the container, run one command in it and stop it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import stat
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any, Generic, TypeVar

from . import memory
from .images import ImageStore, ResolvedImage
from .paths import default_scratch_root
from .process import TIMEOUT_EXIT_CODE, ProcessResult, kill_tree, run_process
from .sessions import (
    JanitorReport,
    Session,
    labels_match,
    new_instance_name,
    open_session,
    remove_session,
    remove_tree,
    sweep,
)
from .types import (
    ExecResult,
    SandboxError,
    SandboxLostError,
    SandboxSnapshot,
    SandboxSpec,
    SandboxStartError,
)

log = logging.getLogger(__name__)

SNAPSHOT_FORMAT = "hpc-sandbox/rootfs-v1"
RESERVED_TARGETS = ("/tmp", "/var/tmp")
TRANSFER_DIR = ".hpc-sandbox-transfer"
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PROBE_SCRIPT = """\
set -e
mkdir -p -- "$1"
mkdir -- "$2"
rmdir -- "$2"
probe="$1/.hpc-sandbox-probe-$$"
: > "$probe"
rm -f -- "$probe"
"""
_UPLOAD_FILE = """\
if [ -d "$2" ]; then echo "hpc-sandbox: $2 is a directory" >&2; exit 1; fi
mkdir -p -- "$(dirname -- "$2")" && cp -p -- "$1" "$2"
"""
_UPLOAD_DIR = 'mkdir -p -- "$2" && cp -pR -- "$1/." "$2/"\n'
_MISSING_SOURCE = 66
_DOWNLOAD = f"""\
if [ ! -e "$1" ]; then
    echo "hpc-sandbox: no such file or directory: $1" >&2
    exit {_MISSING_SOURCE}
fi
cp -pRH -- "$1" "$2"
"""
_PROBE_TIMEOUT_S = 120.0

T = TypeVar("T")
S = TypeVar("S", bound="SessionSandbox")


class SessionProvider(Generic[S]):
    """Start, restore and reap sandboxes whose state lives in session directories.

    Subclasses set ``name``, ``capabilities``, ``images`` and ``sandbox_type``
    and implement :meth:`_extract` and :meth:`_stop_abandoned`.
    """

    name: str
    capabilities: frozenset[str]
    images: ImageStore
    sandbox_type: type[S]
    network_none_hint = "network 'none' is not supported by this provider"

    def __init__(
        self,
        *,
        scratch_root: str | Path | None,
        default_timeout_s: float,
        start_timeout_s: float,
        stop_timeout_s: float,
        output_limit_bytes: int,
        memory_poll_s: float,
    ) -> None:
        self.scratch_root = Path(scratch_root) if scratch_root else default_scratch_root()
        self.default_timeout_s = default_timeout_s
        self.start_timeout_s = start_timeout_s
        self.stop_timeout_s = stop_timeout_s
        self.output_limit_bytes = output_limit_bytes
        self.memory_poll_s = memory_poll_s
        self._sandboxes: set[S] = set()
        self._initial_sweep: asyncio.Future[None] | None = None

    @property
    def session_root(self) -> Path:
        return self.scratch_root / "sessions"

    @property
    def snapshot_root(self) -> Path:
        return self.scratch_root / "snapshots"

    @property
    def sandboxes(self) -> frozenset[S]:
        """The sandboxes this provider started that have not been stopped."""
        return frozenset(self._sandboxes)

    async def start(self, spec: SandboxSpec) -> S:
        self._check(spec)
        await self._prepare()
        image = await self.images.resolve(spec.image)
        return await self._launch(spec, partial(self._extract, image))

    async def restore(self, snapshot: SandboxSnapshot, spec: SandboxSpec) -> S:
        """Start a sandbox for ``spec`` whose files come from ``snapshot``."""
        self._check(spec)
        source = self._snapshot_dir(snapshot)
        if snapshot.image != spec.image:
            raise ValueError(f"snapshot {snapshot.id} is of {snapshot.image!r}, not {spec.image!r}")
        await self._prepare()
        return await self._launch(spec, partial(self._copy_snapshot, source))

    async def delete_snapshot(self, snapshot: SandboxSnapshot) -> None:
        await asyncio.to_thread(remove_tree, self._snapshot_dir(snapshot))

    async def reap(self, labels: Mapping[str, str] | None = None) -> JanitorReport:
        """Stop sandboxes and remove sessions that match ``labels``.

        With ``labels``, this provider's own live sandboxes whose labels include
        them are stopped. Then abandoned sessions (no lease holder, owner process
        gone) of any process on this node are swept, filtered by ``labels`` when
        given. Sessions owned by other live processes are never touched.
        """
        report = JanitorReport()
        if labels is not None:
            mine = [
                sandbox for sandbox in self._sandboxes if labels_match(sandbox.spec.labels, labels)
            ]
            results = await asyncio.gather(
                *(sandbox.stop() for sandbox in mine), return_exceptions=True
            )
            for sandbox, result in zip(mine, results, strict=True):
                if isinstance(result, BaseException):
                    report.errors.append(f"could not stop {sandbox.id}: {result}")
                else:
                    report.stopped.append(sandbox.id)
                    report.removed.append(sandbox.id)
        swept = await sweep(self.session_root, self._stop_abandoned, labels=labels)
        report.stopped += swept.stopped
        report.removed += swept.removed
        report.live += swept.live
        report.errors += swept.errors
        return report

    async def copy_tree(self, source: Path, target: Path) -> None:
        """``cp -a --reflink=auto``: near-free on reflink filesystems, a full copy otherwise."""
        result = await run_process(
            ["cp", "-a", "--reflink=auto", "--", str(source), str(target)],
            timeout_s=self.start_timeout_s,
        )
        if result.returncode != 0 or result.timed_out:
            raise SandboxError(f"could not copy {source} to {target}: {result.detail()}")

    async def _extract(self, image: ResolvedImage, session: Session) -> None:
        """Fill ``session.rootfs`` from ``image``."""
        raise NotImplementedError

    async def _stop_abandoned(self, path: Path, record: dict[str, Any] | None) -> bool:
        """Stop whatever still runs for the abandoned session at ``path``.

        ``record`` is the session's owner record when it is bound to the
        session, else None. Return whether anything was running; raise
        :class:`SandboxError` to leave the session alone.
        """
        raise NotImplementedError

    def _scratch_directories(self) -> list[Path]:
        """Private directories created below the scratch root before the first start."""
        return [self.scratch_root, self.session_root, self.snapshot_root]

    async def _launch(self, spec: SandboxSpec, populate: Callable[[Session], Awaitable[None]]) -> S:
        sandbox = self.sandbox_type(self, spec)
        await sandbox._boot(populate)
        return sandbox

    def _check(self, spec: SandboxSpec) -> None:
        if spec.network not in ("host", "none"):
            raise ValueError(f"unknown network mode {spec.network!r}")
        if spec.network == "none" and "network_none" not in self.capabilities:
            raise ValueError(self.network_none_hint)
        if not PurePosixPath(spec.workdir).is_absolute():
            raise ValueError(f"workdir must be absolute: {spec.workdir!r}")
        if spec.memory_mb is not None and spec.memory_mb <= 0:
            raise ValueError("memory_mb must be positive")
        if spec.cpus is not None and spec.cpus <= 0:
            raise ValueError("cpus must be positive")
        check_env(spec.env)
        for mount in spec.mounts:
            target = PurePosixPath(mount.target)
            if not target.is_absolute() or ".." in target.parts or target == PurePosixPath("/"):
                raise ValueError(f"invalid mount target {mount.target!r}")
            if any(
                target == PurePosixPath(r) or PurePosixPath(r) in target.parents
                for r in RESERVED_TARGETS
            ):
                raise ValueError(f"mount target {mount.target!r} is reserved for private tmp")
            if any(c in text for text in (mount.source, mount.target) for c in ":,"):
                raise ValueError(f"mount paths cannot contain ':' or ',': {mount}")
            if not Path(mount.source).exists():
                raise ValueError(f"mount source does not exist: {mount.source!r}")

    async def _prepare(self) -> None:
        for directory in self._scratch_directories():
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.scratch_root.stat().st_uid != os.getuid():
            raise SandboxError(f"scratch root {self.scratch_root} is owned by another user")
        if self._initial_sweep is None:
            self._initial_sweep = asyncio.ensure_future(self._sweep_abandoned())
        await asyncio.shield(self._initial_sweep)

    async def _sweep_abandoned(self) -> None:
        """Recover sessions left by crashed processes, once per provider."""
        report = await sweep(self.session_root, self._stop_abandoned)
        for error in report.errors:
            log.warning("hpc-sandbox janitor: %s", error)

    async def _copy_snapshot(self, source: Path, session: Session) -> None:
        for name, target in (
            ("rootfs", session.rootfs),
            ("tmp", session.tmp),
            ("var-tmp", session.var_tmp),
        ):
            await self.copy_tree(source / name, target)

    def _snapshot_dir(self, snapshot: SandboxSnapshot) -> Path:
        path = Path(snapshot.path)
        if snapshot.provider != self.name or snapshot.metadata.get("format") != SNAPSHOT_FORMAT:
            raise ValueError(f"snapshot {snapshot.id} was not taken by this provider")
        if path.parent != self.snapshot_root or path.name != snapshot.id or not path.is_dir():
            raise ValueError(f"snapshot {snapshot.id} is not in {self.snapshot_root}")
        return path


class SessionSandbox:
    """One running container whose files live in a session directory.

    Subclasses implement :meth:`_prepare_rootfs`, :meth:`_start_container`,
    :meth:`_spawn`, :meth:`_container_alive`, :meth:`_stop_container` and
    :meth:`_identity`, and may override :meth:`_watch_root`.
    """

    probe_hint = ""

    def __init__(self, provider: SessionProvider[Any], spec: SandboxSpec) -> None:
        self.id = new_instance_name()
        self.spec = spec
        self.memory_kills: list[memory.MemoryKill] = []
        self._provider = provider
        self._session: Session | None = None
        self._running = False
        self._lost: str | None = None
        # Active exec calls: host PID of the spawned process -> memory kills charged to it.
        self._execs: dict[int, list[memory.MemoryKill]] = {}
        self._watchdog: asyncio.Task[None] | None = None
        self._stop_task: asyncio.Future[None] | None = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id!r}, image={self.spec.image!r})"

    @property
    def session_dir(self) -> Path | None:
        return self._session.path if self._session else None

    async def exec(
        self,
        cmd: str | list[str],
        *,
        timeout_s: float | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        stdin: bytes | None = None,
    ) -> ExecResult:
        """Run ``cmd`` (a shell string or an argv list) in the sandbox.

        Every call has a timeout (the provider default when ``timeout_s`` is
        None); on expiry the command's process tree is killed and the result has
        ``timed_out`` set and exit code 124. ``env`` extends the spec's env.
        """
        self._require_running()
        argv = ["/bin/sh", "-c", cmd] if isinstance(cmd, str) else list(cmd)
        if not argv:
            raise ValueError("empty command")
        timeout = self._provider.default_timeout_s if timeout_s is None else timeout_s
        if timeout <= 0:
            raise ValueError("timeout_s must be positive")
        environment = {"HOME": "/root", **self.spec.env, **(env or {})}
        check_env(environment)
        return await self._run(
            argv, cwd=cwd or self.spec.workdir, env=environment, stdin=stdin, timeout_s=timeout
        )

    async def upload(self, src: str | Path, dst: str) -> None:
        """Copy a host file to path ``dst``, or a host directory's contents into ``dst``."""
        self._require_running()
        source = Path(src)
        if not source.exists():
            raise FileNotFoundError(f"no such file or directory: {source}")
        async with self._transfer() as (host_dir, container_dir):
            payload = host_dir / "payload"
            if source.is_dir():
                await asyncio.to_thread(shutil.copytree, source, payload, symlinks=True)
            else:
                await asyncio.to_thread(shutil.copy2, source, payload)
            script = _UPLOAD_DIR if source.is_dir() else _UPLOAD_FILE
            result = await self._transfer_exec(script, f"{container_dir}/payload", dst)
            if result.exit_code != 0:
                raise SandboxError(f"upload to {dst} failed: {result.stderr.strip()}")

    async def download(self, src: str, dst: str | Path) -> None:
        """Copy sandbox file ``src`` to host path ``dst``, or a directory to ``dst``."""
        self._require_running()
        target = Path(dst)
        async with self._transfer() as (host_dir, container_dir):
            result = await self._transfer_exec(_DOWNLOAD, src, f"{container_dir}/payload")
            if result.exit_code == _MISSING_SOURCE:
                raise FileNotFoundError(f"no such file or directory in the sandbox: {src}")
            if result.exit_code != 0:
                raise SandboxError(f"download of {src} failed: {result.stderr.strip()}")
            payload = host_dir / "payload"
            if payload.is_dir() and not payload.is_symlink():
                await asyncio.to_thread(
                    shutil.copytree, payload, target, symlinks=True, dirs_exist_ok=True
                )
            elif target.is_dir():
                raise IsADirectoryError(f"download target is a directory: {target}")
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                await asyncio.to_thread(shutil.copy2, payload, target, follow_symlinks=False)

    async def snapshot(self) -> SandboxSnapshot:
        """Copy the writable rootfs and private tmp dirs; processes are not captured.

        Take snapshots while the sandbox is quiescent: files written during the
        copy may be captured partially.
        """
        self._require_running()
        session = self._session
        assert session is not None
        await self._run(["/bin/sh", "-c", "sync"], cwd="/", env={}, stdin=None, timeout_s=60.0)
        snapshot_id = f"snap-{uuid.uuid4().hex}"
        target = self._provider.snapshot_root / snapshot_id
        target.mkdir(mode=0o700)
        try:
            for source, name in (
                (session.rootfs, "rootfs"),
                (session.tmp, "tmp"),
                (session.var_tmp, "var-tmp"),
            ):
                await self._provider.copy_tree(source, target / name)
            await asyncio.to_thread(remove_tree, target / "tmp" / TRANSFER_DIR)
            snapshot = SandboxSnapshot(
                id=snapshot_id,
                provider=self._provider.name,
                path=str(target),
                image=self.spec.image,
                metadata={
                    "format": SNAPSHOT_FORMAT,
                    "created_at": time.time(),
                    "source_sandbox": self.id,
                    "workdir": self.spec.workdir,
                    "labels": dict(self.spec.labels),
                },
            )
            (target / "snapshot.json").write_text(json.dumps(asdict(snapshot), indent=2) + "\n")
        except BaseException:
            await asyncio.to_thread(remove_tree, target)
            raise
        return snapshot

    async def stop(self) -> None:
        """Stop the container and delete its session; idempotent and cancellation-safe.

        Teardown always runs to completion; a cancelled caller sees
        ``CancelledError`` only afterwards. If teardown fails (a process whose
        identity cannot be proved), the session is kept for the janitor and a
        later ``stop()`` retries.
        """
        task = self._stop_task
        if task is None or (task.done() and not task.cancelled() and task.exception()):
            task = self._stop_task = asyncio.ensure_future(self._teardown())
        await _finish(task)

    # Runtime-specific steps.

    def _prepare_rootfs(self) -> None:
        """Make the populated rootfs startable; runs in a worker thread."""
        raise NotImplementedError

    async def _start_container(self) -> None:
        """Start the container; call :meth:`_record` once its identity is known."""
        raise NotImplementedError

    async def _spawn(
        self,
        argv: list[str],
        *,
        cwd: str,
        env: Mapping[str, str],
        stdin: bytes | None,
        timeout_s: float,
        on_start: Callable[[int], None],
    ) -> ProcessResult:
        """Run ``argv`` in the container with exactly ``env`` and working directory ``cwd``."""
        raise NotImplementedError

    def _container_alive(self) -> bool:
        raise NotImplementedError

    async def _stop_container(self, session: Session) -> None:
        """Stop the container and everything in it, or raise :class:`SandboxError`."""
        raise NotImplementedError

    def _identity(self) -> dict[str, Any]:
        """Fields of the owner record that identify the container's processes."""
        raise NotImplementedError

    def _watch_root(self) -> int | None:
        """Host PID whose process tree the memory watchdog charges to the sandbox.

        The root and its direct children are never killed.
        """
        return None

    # Shared machinery.

    async def _boot(self, populate: Callable[[Session], Awaitable[None]]) -> None:
        provider = self._provider
        try:
            self._session = await asyncio.to_thread(open_session, provider.session_root, self.id)
            self._record()
            await populate(self._session)
            await asyncio.to_thread(self._prepare_rootfs)
            await self._start_container()
            await self._probe()
        except BaseException as error:
            try:
                await self.stop()
            except Exception as cleanup_error:  # noqa: BLE001 - reported with the cause
                raise SandboxStartError(
                    f"sandbox {self.id} failed to start and its cleanup failed: {cleanup_error}"
                ) from error
            if isinstance(error, SandboxStartError) or not isinstance(error, Exception):
                raise
            raise SandboxStartError(f"sandbox {self.id} failed to start: {error}") from error
        self._running = True
        provider._sandboxes.add(self)
        if self.spec.memory_mb:
            self._watchdog = asyncio.create_task(
                memory.watch(
                    self.spec.memory_mb * 1024 * 1024,
                    instance_pid=self._watch_root,
                    exec_roots=lambda: list(self._execs),
                    on_kill=self._on_memory_kill,
                    interval_s=provider.memory_poll_s,
                )
            )

    def _record(self) -> None:
        assert self._session is not None
        self._session.write_owner_record(
            labels=dict(self.spec.labels), image=self.spec.image, **self._identity()
        )

    async def _probe(self) -> None:
        """Fail fast unless the rootfs and the workdir are writable."""
        marker = f"/.hpc-sandbox-probe-{uuid.uuid4().hex}"
        argv = ["/bin/sh", "-c", _PROBE_SCRIPT, "hpc-sandbox", self.spec.workdir, marker]
        result = await self._run(argv, cwd="/", env={}, stdin=None, timeout_s=_PROBE_TIMEOUT_S)
        if result.exit_code != 0:
            raise SandboxStartError(
                f"the sandbox rootfs or workdir {self.spec.workdir} is not writable "
                f"(exit {result.exit_code}): {result.stderr.strip()}{self.probe_hint}"
            )

    async def _run(
        self,
        argv: list[str],
        *,
        cwd: str,
        env: Mapping[str, str],
        stdin: bytes | None,
        timeout_s: float,
    ) -> ExecResult:
        kills: list[memory.MemoryKill] = []
        roots: list[int] = []

        def started(pid: int) -> None:
            roots.append(pid)
            self._execs[pid] = kills

        try:
            result = await self._spawn(
                argv, cwd=cwd, env=env, stdin=stdin, timeout_s=timeout_s, on_start=started
            )
        finally:
            for pid in roots:
                self._execs.pop(pid, None)
        if self._stop_task is not None:
            raise SandboxError(f"sandbox {self.id} was stopped while the command ran")
        stdout = result.stdout.decode(errors="replace")
        stderr = result.stderr.decode(errors="replace")
        if result.timed_out:
            note = f"hpc-sandbox: command timed out after {timeout_s:g} s"
            return ExecResult(TIMEOUT_EXIT_CODE, stdout, _append(stderr, note), timed_out=True)
        if kills:
            return ExecResult(137, stdout, _append(stderr, kills[-1].message()))
        if result.returncode != 0 and not self._container_alive():
            self._running = False
            self._lost = stderr.strip() or "the container process is gone"
            raise SandboxLostError(f"sandbox {self.id} lost its container: {self._lost}")
        return ExecResult(result.returncode, stdout, stderr)

    @asynccontextmanager
    async def _transfer(self) -> AsyncIterator[tuple[Path, str]]:
        """A fresh directory visible on the host and, through /tmp, in the sandbox."""
        assert self._session is not None
        token = uuid.uuid4().hex
        host_dir = self._session.tmp / TRANSFER_DIR / token
        host_dir.mkdir(mode=0o700, parents=True)
        try:
            yield host_dir, f"/tmp/{TRANSFER_DIR}/{token}"
        finally:
            await asyncio.to_thread(remove_tree, host_dir)

    async def _transfer_exec(self, script: str, *args: str) -> ExecResult:
        return await self._run(
            ["/bin/sh", "-c", script, "hpc-sandbox", *args],
            cwd=self.spec.workdir,
            env={},
            stdin=None,
            timeout_s=self._provider.default_timeout_s,
        )

    def _require_running(self) -> None:
        if self._lost is not None:
            raise SandboxLostError(f"sandbox {self.id} lost its container: {self._lost}")
        if not self._running:
            raise SandboxError(f"sandbox {self.id} is not running")

    def _on_memory_kill(self, kill: memory.MemoryKill) -> None:
        log.warning("sandbox %s: %s", self.id, kill.message())
        self.memory_kills.append(kill)
        if kill.exec_root is not None and (charged := self._execs.get(kill.exec_root)) is not None:
            charged.append(kill)

    async def _teardown(self) -> None:
        self._running = False
        self._provider._sandboxes.discard(self)
        if self._watchdog is not None:
            self._watchdog.cancel()
            await asyncio.gather(self._watchdog, return_exceptions=True)
            self._watchdog = None
        for pid in list(self._execs):
            kill_tree(pid)
        session = self._session
        if session is None:
            return
        await self._stop_container(session)
        await asyncio.to_thread(remove_session, session)
        self._session = None


def check_env(env: Mapping[str, str]) -> None:
    for key, value in env.items():
        if not _ENV_NAME.fullmatch(key):
            raise ValueError(f"invalid environment variable name {key!r}")
        if "\0" in value:
            raise ValueError(f"environment variable {key} contains a NUL byte")


def make_owner_writable(rootfs: Path) -> None:
    """Give the owner rwx on directories and rw on files of an extracted rootfs.

    Images contain root-private build caches; extraction as this uid keeps
    their modes, which the owner could then neither modify nor delete.
    """
    for directory, directories, files in os.walk(rootfs):
        for path in (Path(directory), *(Path(directory) / name for name in directories + files)):
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                continue
            if info.st_uid != os.getuid():
                raise SandboxStartError(f"extracted rootfs contains a foreign-owned path: {path}")
            wanted = 0o700 if stat.S_ISDIR(info.st_mode) else 0o600
            if stat.S_IMODE(info.st_mode) & wanted != wanted:
                path.chmod(stat.S_IMODE(info.st_mode) | wanted)


def _append(text: str, line: str) -> str:
    separator = "\n" if text and not text.endswith("\n") else ""
    return f"{text}{separator}{line}\n"


async def poll(probe: Callable[[], T | None], timeout_s: float) -> T | None:
    """Call ``probe`` every 50 ms until it returns a value or ``timeout_s`` passes."""
    deadline = time.monotonic() + timeout_s
    while (value := probe()) is None and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    return value


async def _finish(task: asyncio.Future[None]) -> None:
    """Await shielded cleanup to completion, re-raising a caller cancellation after it."""
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()
    if cancelled:
        raise asyncio.CancelledError
