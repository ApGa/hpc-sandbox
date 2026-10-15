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
import shlex
import stat
import threading
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any, Generic, NoReturn, Self, TypeVar

from . import memory, safefs
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
# Printed on stderr by each provider's prologue right before it executes the
# command, and stripped from the result: without it, the command never ran.
START_MARKER = "hpc-sandbox: command started"
_START_LINE = f"{START_MARKER}\n".encode()
# Names a shell may refuse to assign, which makes a POSIX shell exit: bash
# makes these read-only, and dash takes only a number for OPTIND. The prologue
# sets them only where the shell allows it.
_SHELL_RESERVED = frozenset(
    {"BASHOPTS", "BASHPID", "BASH_VERSINFO", "EUID", "OPTIND", "PPID", "SHELLOPTS", "UID"}
)
MIN_OUTPUT_LIMIT = 4096
RESERVED_TARGETS = ("/tmp", "/var/tmp")
TRANSFER_DIR = ".hpc-sandbox-transfer"
# How long removing a transfer's files may take: code in the sandbox can keep
# refilling them, and the session's removal takes care of what is left.
_TRANSFER_CLEANUP_S = 60.0
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
# Ownership is not copied out: root in a user namespace cannot give the copy an
# owner the namespace does not map (files on host mounts), and GNU cp -p then
# fails where busybox cp -p only warns. Busybox has no --preserve=LIST.
_DOWNLOAD = f"""\
if [ ! -e "$1" ]; then
    echo "hpc-sandbox: no such file or directory: $1" >&2
    exit {_MISSING_SOURCE}
fi
case "$(cp --help 2>&1)" in
    *--preserve*) exec cp -RH --preserve=mode,timestamps -- "$1" "$2" ;;
    *) exec cp -pRH -- "$1" "$2" ;;
esac
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
    # Each provider keeps its sessions apart: janitors only know their own runtime.
    sessions_directory = "sessions"

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
        if output_limit_bytes < MIN_OUTPUT_LIMIT:
            raise ValueError(f"output_limit_bytes must be at least {MIN_OUTPUT_LIMIT}")
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
        return self.scratch_root / self.sessions_directory

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
        for root, claim_unknown in self._session_roots():
            swept = await sweep(
                root,
                self._stop_abandoned,
                labels=labels,
                provider=self.name,
                claim_unknown=claim_unknown,
            )
            report.stopped += swept.stopped
            report.removed += swept.removed
            report.live += swept.live
            report.errors += swept.errors
        return report

    def _session_roots(self) -> list[tuple[Path, bool]]:
        """Directories janitors sweep, and whether they claim sessions of no known provider."""
        return [(self.session_root, True)]

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
        self.images.format.check(spec.image)
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
        for root, claim_unknown in self._session_roots():
            report = await sweep(
                root, self._stop_abandoned, provider=self.name, claim_unknown=claim_unknown
            )
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

    ``async with await provider.start(spec) as sandbox:`` stops the sandbox
    when the block exits, however it exits.

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

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

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
        async with self._transfer() as (staging, container_dir):
            await _in_thread(partial(safefs.copy_in, source, "payload", staging))
            script = _UPLOAD_DIR if source.is_dir() else _UPLOAD_FILE
            result = await self._transfer_exec(script, f"{container_dir}/payload", dst)
            if result.exit_code != 0:
                raise SandboxError(f"upload to {dst} failed: {result.stderr.strip()}")

    async def download(self, src: str, dst: str | Path) -> None:
        """Copy sandbox file ``src`` to host path ``dst``, or a directory to ``dst``."""
        self._require_running()
        target = Path(dst)
        async with self._transfer() as (staging, container_dir):
            result = await self._transfer_exec(_DOWNLOAD, src, f"{container_dir}/payload")
            if result.exit_code == _MISSING_SOURCE:
                raise FileNotFoundError(f"no such file or directory in the sandbox: {src}")
            if result.exit_code != 0:
                raise SandboxError(f"download of {src} failed: {result.stderr.strip()}")
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                await _in_thread(partial(safefs.copy_out, "payload", staging, target))
            except SandboxError as error:  # what the sandbox left there cannot be copied
                raise SandboxError(f"download of {src} to {target} failed: {error}") from error

    async def snapshot(self) -> SandboxSnapshot:
        """Copy the writable rootfs and private tmp dirs; processes are not captured.

        Take snapshots while the sandbox is quiescent: files written during the
        copy may be captured partially.
        """
        self._require_running()
        session = self._session
        assert session is not None
        await self._run(["/bin/sh", "-c", "sync"], cwd="/", env={}, stdin=None, timeout_s=60.0)
        try:
            await asyncio.to_thread(self._prepare_snapshot)
        except OSError as error:
            raise SandboxError(
                f"could not prepare sandbox {self.id} for a snapshot: {error}"
            ) from error
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
                    **self._user(),
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
        command: list[str],
        *,
        stdin: bytes | None,
        timeout_s: float,
        on_start: Callable[[int], None],
    ) -> ProcessResult:
        """Run ``command`` (a :func:`prologue`) in the container with the container's
        own environment, and report the host PID of the process it spawns."""
        raise NotImplementedError

    def _full_environment(self, env: Mapping[str, str]) -> dict[str, str]:
        """The variables a command gets on top of what the runtime sets up."""
        return dict(env)

    def _container_alive(self) -> bool:
        raise NotImplementedError

    async def _stop_container(self, session: Session) -> None:
        """Stop the container and everything in it, or raise :class:`SandboxError`."""
        raise NotImplementedError

    def _identity(self) -> dict[str, Any]:
        """Fields of the owner record that identify the container's processes."""
        raise NotImplementedError

    def _user(self) -> dict[str, Any]:
        """Who commands run as, recorded in the owner record and snapshot metadata.

        ``container_uid`` is the uid inside the sandbox; files it writes belong
        to the calling user on the host either way.
        """
        return {"remap_root": False, "container_uid": os.getuid()}

    def _prepare_snapshot(self) -> None:
        """Make the rootfs copyable before a snapshot; runs in a worker thread."""

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
            provider=self._provider.name,
            labels=dict(self.spec.labels),
            image=self.spec.image,
            **self._user(),
            **self._identity(),
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
                prologue(argv, cwd, stdin=stdin is not None),
                stdin=environment_script(self._full_environment(env)) + (stdin or b""),
                timeout_s=timeout_s,
                on_start=started,
            )
        finally:
            for pid in roots:
                self._execs.pop(pid, None)
        if self._stop_task is not None:
            raise SandboxError(f"sandbox {self.id} was stopped while the command ran")
        started_command, raw_stderr = split_start_marker(result.stderr)
        stdout = result.stdout.decode(errors="replace")
        stderr = raw_stderr.decode(errors="replace")
        if not started_command:
            if not self._container_alive():
                self._mark_lost(stderr)
            state = "timed out" if result.timed_out else f"exit {result.returncode}"
            detail = (stderr + stdout).strip() or "no output"
            raise SandboxError(f"sandbox {self.id}: the command did not start ({state}): {detail}")
        if result.timed_out:
            note = f"hpc-sandbox: command timed out after {timeout_s:g} s"
            return ExecResult(TIMEOUT_EXIT_CODE, stdout, _append(stderr, note), timed_out=True)
        if kills:
            return ExecResult(137, stdout, _append(stderr, kills[-1].message()))
        if result.returncode != 0 and not self._container_alive():
            self._mark_lost(stderr)
        return ExecResult(result.returncode, stdout, stderr)

    def _mark_lost(self, detail: str) -> NoReturn:
        self._running = False
        self._lost = detail.strip() or "the container process is gone"
        raise SandboxLostError(f"sandbox {self.id} lost its container: {self._lost}")

    @property
    def _output_limit(self) -> int:
        """Bytes captured per stream: the provider's limit plus the start marker."""
        return self._provider.output_limit_bytes + len(_START_LINE)

    @asynccontextmanager
    async def _transfer(self) -> AsyncIterator[tuple[int, str]]:
        """A fresh directory in the sandbox's /tmp: a host descriptor and its path inside.

        Code in the sandbox can replace anything below its /tmp at any time,
        so the host side only works through descriptors (see :mod:`safefs`).
        """
        assert self._session is not None
        tmp, token = self._session.tmp, uuid.uuid4().hex
        try:
            root, staging = await _in_thread(
                lambda _: _open_staging(tmp, token),
                abandon=lambda fds: [os.close(fd) for fd in fds],
            )
        except OSError as error:
            raise SandboxError(
                f"sandbox {self.id}: could not prepare a transfer: {error}"
            ) from error
        try:
            yield staging, f"/tmp/{TRANSFER_DIR}/{token}"
        finally:
            os.close(staging)
            try:
                deadline = time.monotonic() + _TRANSFER_CLEANUP_S
                await _in_thread(partial(safefs.remove, token, root, deadline=deadline))
            except Exception as error:  # noqa: BLE001 - the transfer itself is what matters
                log.warning("sandbox %s: could not remove transfer files: %s", self.id, error)
            finally:
                os.close(root)

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


async def _in_thread(
    func: Callable[[threading.Event], T], *, abandon: Callable[[T], object] | None = None
) -> T:
    """``func(stop)`` in a worker thread that never outlives the call.

    Transfers hand the thread descriptors that the caller closes right after,
    so cancelling the caller sets ``stop`` and still waits for the thread. A
    result that arrives anyway is passed to ``abandon``.
    """
    stop = threading.Event()
    task = asyncio.ensure_future(asyncio.to_thread(func, stop))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        stop.set()
        while not task.done():
            with suppress(asyncio.CancelledError):
                await asyncio.wait({task})
        if not task.cancelled() and task.exception() is None and abandon is not None:
            abandon(task.result())
        raise


def _open_staging(tmp: Path, token: str) -> tuple[int, int]:
    """Descriptors of the transfer root below ``tmp`` and of a new ``token`` directory in it.

    Code in the sandbox may have taken the owner's access to either directory
    away; it is given back.
    """
    with safefs.opened_dir(tmp) as tmp_fd:
        safefs.open_up(tmp_fd)
        root = safefs.make_dir(TRANSFER_DIR, tmp_fd)
    try:
        safefs.open_up(root)
        os.mkdir(token, 0o700, dir_fd=root)
        return root, safefs.open_dir(token, root)
    except BaseException:
        os.close(root)
        raise


def check_env(env: Mapping[str, str]) -> None:
    for key, value in env.items():
        if not _ENV_NAME.fullmatch(key):
            raise ValueError(f"invalid environment variable name {key!r}")
        if "\0" in value:
            raise ValueError(f"environment variable {key} contains a NUL byte")
        try:
            os.fsencode(value)
        except UnicodeEncodeError:
            raise ValueError(f"environment variable {key} is not encodable text") from None


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


# Run by /bin/sh -c with the working directory, the start marker, "stdin" or
# "none", and the command as positional parameters. The first line of stdin is
# the environment (see environment_script); `read` takes it a byte at a time,
# so the command gets exactly the rest, or /dev/null if the caller sent none.
_PROLOGUE = """\
hpc_sandbox_nl='
'
IFS= read -r hpc_sandbox_env || exit
eval "$hpc_sandbox_env" || exit
cd -- "$1" || exit
[ "$3" = stdin ] || exec </dev/null || exit
printf '%s\\n' "$2" >&2
shift 3
exec "$@"
"""
_NEWLINE = '"$hpc_sandbox_nl"'  # how environment_script writes a newline


def prologue(argv: Sequence[str], cwd: str, *, stdin: bool) -> list[str]:
    """``argv`` run by ``/bin/sh`` in ``cwd``, with the environment read from stdin.

    Send :func:`environment_script` ahead of the command's own stdin; without
    ``stdin``, the command reads /dev/null as if none were given. The prologue
    announces the start on stderr once everything else has succeeded. The
    working directory is entered here because some runtimes silently fall
    back to another one; a missing one, or a missing environment line, ends
    the prologue before the announcement.
    """
    mode = "stdin" if stdin else "none"
    return ["/bin/sh", "-c", _PROLOGUE, "hpc-sandbox", cwd, START_MARKER, mode, *argv]


def environment_script(env: Mapping[str, str]) -> bytes:
    """One line of shell that exports ``env`` verbatim (POSIX quoting).

    It travels through stdin rather than on a command line, where every user
    of the node could read the values, or through a file, which would make
    commands depend on the sandbox's own filesystem. Newlines in values are
    written as a variable the prologue sets, so the line never breaks. Names
    a shell may refuse are set only where it allows them, since a refused
    assignment makes a POSIX shell exit, and bash's warnings about locales
    the image lacks are not the command's output. Values that are not UTF-8
    come back as the bytes they were decoded from (see :func:`os.fsdecode`).
    """
    commands = []
    # A variable of the prologue's own name goes last, after every use of it.
    for key, value in sorted(env.items(), key=lambda item: item[0] == "hpc_sandbox_nl"):
        quoted = _NEWLINE.join(shlex.quote(part) for part in value.split("\n"))
        assignment = f"{key}={quoted}"
        if key in _SHELL_RESERVED:
            commands.append(f"if ({assignment}) 2>/dev/null; then export {assignment}; fi")
        elif key == "LANG" or key.startswith("LC_"):
            commands.append(f"export {assignment} 2>/dev/null")
        else:
            commands.append(f"export {assignment}")
    return os.fsencode("; ".join(commands) + "\n")


def split_start_marker(stderr: bytes) -> tuple[bool, bytes]:
    """Whether the prologue's start marker is in ``stderr``, and ``stderr`` without it.

    The runtime may print warnings before the prologue runs, so only the first
    occurrence counts (the command's own output comes after it).
    """
    index = stderr.find(_START_LINE)
    if index < 0:
        return False, stderr
    return True, stderr[:index] + stderr[index + len(_START_LINE) :]


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
