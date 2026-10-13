"""Rootless sandboxes as Singularity CE / Apptainer instances.

What made the production SWE-rebench and Frontier-CS sandboxes work on ABCI,
and is kept here:

- setuid mode without ``--fakeroot`` (ABCI forces squashfuse for fakeroot with
  overlays and silently falls back to a read-only rootfs);
- a private writable rootfs *directory* per sandbox, extracted with
  ``build --sandbox --fix-perms`` onto node-local scratch: no loop devices and
  never ``--writable-tmpfs`` (ABCI caps the session tmpfs at 64 MiB);
- ``--cleanenv --containall --no-home --no-mount home,cwd,bind-paths`` with
  session-private ``/tmp`` and ``/var/tmp`` binds;
- exact UUID instance names, a ``--pid-file``, PID start-time identities and
  leased session directories, so janitors stop only what they can prove is
  theirs, never ``instance stop --all``;
- a timeout on every runtime call, host networking by default, and a PSS
  memory watchdog instead of cgroups.

OCI-derived images ship an empty ``%startscript``, and an instance whose
startscript exits is removed at once. The production backend built an adapter
SIF with ``build --fakeroot``; with a writable rootfs directory the keep-alive
startscript is simply written into the extracted rootfs instead.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shlex
import shutil
import stat
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Literal, TypeVar

from . import memory, procfs
from .images import ImageStore, ResolvedImage
from .locks import locked
from .paths import default_image_cache, default_scratch_root, should_stage
from .process import DEFAULT_OUTPUT_LIMIT, TIMEOUT_EXIT_CODE, kill_tree, run_process
from .runtime import ContainerRuntime, instance_pid
from .sessions import (
    CONTROL_LOCK,
    JanitorReport,
    Session,
    labels_match,
    new_instance_name,
    open_session,
    read_pid_file,
    remove_session,
    remove_tree,
    stop_instance,
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

KEEPALIVE_STARTSCRIPT = """\
#!/bin/sh
# Written by hpc-sandbox: keep the instance alive; commands run through exec.
command -v sleep >/dev/null 2>&1 || exit 127
trap 'kill $! 2>/dev/null; exit 0' HUP INT TERM
while :; do
    sleep 86400 &
    wait $!
done
"""
SNAPSHOT_FORMAT = "hpc-sandbox/rootfs-v1"
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_RESERVED_TARGETS = ("/tmp", "/var/tmp")
_TRANSFER_DIR = ".hpc-sandbox-transfer"
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
_PID_WAIT_S = 5.0

T = TypeVar("T")


class SingularityProvider:
    """Start, restore and reap sandboxes; implements platoon's ``SandboxProvider``.

    ``scratch_root`` must be node-local (it holds every writable rootfs);
    ``image_cache`` may be shared storage that supports ``flock``. Set
    ``allow_network_none`` only where the site lets users create network
    namespaces, ``gpu`` to ``"nv"`` or ``"rocm"`` to pass GPUs through, and
    ``cgroup_limits`` where rootless cgroups work (Apptainer with systemd), to
    turn ``cpus``/``memory_mb`` into hard limits.
    """

    name = "singularity"

    def __init__(
        self,
        *,
        binary: str | None = None,
        scratch_root: str | Path | None = None,
        image_cache: str | Path | None = None,
        stage_images: bool | None = None,
        default_timeout_s: float = 600.0,
        start_timeout_s: float = 1800.0,
        stop_grace_s: float = 10.0,
        stop_timeout_s: float = 60.0,
        allow_network_none: bool = False,
        gpu: Literal["nv", "rocm"] | None = None,
        cgroup_limits: bool = False,
        bind_resolv_conf: bool = True,
        extra_start_args: Sequence[str] = (),
        output_limit_bytes: int = DEFAULT_OUTPUT_LIMIT,
        memory_poll_s: float = 1.0,
    ) -> None:
        self.scratch_root = Path(scratch_root) if scratch_root else default_scratch_root()
        self.runtime = ContainerRuntime(
            binary,
            tmp_dir=self.scratch_root / "runtime-tmp",
            cache_dir=self.scratch_root / "runtime-cache",
        )
        cache = Path(image_cache) if image_cache else default_image_cache()
        if stage_images is None:
            stage_images = should_stage(cache, self.scratch_root)
        self.images = ImageStore(
            self.runtime,
            cache,
            stage_dir=self.scratch_root / "images" if stage_images else None,
            pull_timeout_s=start_timeout_s,
        )
        self.default_timeout_s = default_timeout_s
        self.start_timeout_s = start_timeout_s
        self.stop_grace_s = stop_grace_s
        self.stop_timeout_s = stop_timeout_s
        self.allow_network_none = allow_network_none
        self.gpu = gpu
        self.cgroup_limits = cgroup_limits
        self.bind_resolv_conf = bind_resolv_conf
        self.extra_start_args = tuple(extra_start_args)
        self.output_limit_bytes = output_limit_bytes
        self.memory_poll_s = memory_poll_s
        self.capabilities = frozenset(
            {"snapshot", "isolation"}
            | ({"network_none"} if allow_network_none else set())
            | ({"gpu"} if gpu else set())
        )
        self._sandboxes: set[SingularitySandbox] = set()
        self._initial_sweep: asyncio.Future[None] | None = None

    @property
    def session_root(self) -> Path:
        return self.scratch_root / "sessions"

    @property
    def snapshot_root(self) -> Path:
        return self.scratch_root / "snapshots"

    @property
    def sandboxes(self) -> frozenset[SingularitySandbox]:
        """The sandboxes this provider started that have not been stopped."""
        return frozenset(self._sandboxes)

    async def start(self, spec: SandboxSpec) -> SingularitySandbox:
        self._check(spec)
        await self._prepare()
        image = await self.images.resolve(spec.image)
        sandbox = SingularitySandbox(self, spec)
        await sandbox._boot(partial(self._extract, image))
        return sandbox

    async def restore(self, snapshot: SandboxSnapshot, spec: SandboxSpec) -> SingularitySandbox:
        """Start a sandbox for ``spec`` whose files come from ``snapshot``."""
        self._check(spec)
        source = self._snapshot_dir(snapshot)
        if snapshot.image != spec.image:
            raise ValueError(f"snapshot {snapshot.id} is of {snapshot.image!r}, not {spec.image!r}")
        await self._prepare()
        sandbox = SingularitySandbox(self, spec)
        await sandbox._boot(partial(self._copy_snapshot, source))
        return sandbox

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
        swept = await sweep(
            self.session_root,
            self.runtime,
            labels=labels,
            grace_s=self.stop_grace_s,
            stop_timeout_s=self.stop_timeout_s,
        )
        report.stopped += swept.stopped
        report.removed += swept.removed
        report.live += swept.live
        report.errors += swept.errors
        return report

    def _check(self, spec: SandboxSpec) -> None:
        if spec.network not in ("host", "none"):
            raise ValueError(f"unknown network mode {spec.network!r}")
        if spec.network == "none" and not self.allow_network_none:
            raise ValueError(
                "network 'none' needs allow_network_none=True; setuid Singularity reserves "
                "network namespaces for root unless the site permits them"
            )
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
                for r in _RESERVED_TARGETS
            ):
                raise ValueError(f"mount target {mount.target!r} is reserved for private tmp")
            if any(c in text for text in (mount.source, mount.target) for c in ":,"):
                raise ValueError(f"mount paths cannot contain ':' or ',': {mount}")
            if not Path(mount.source).exists():
                raise ValueError(f"mount source does not exist: {mount.source!r}")

    async def _prepare(self) -> None:
        for directory in (
            self.scratch_root,
            self.session_root,
            self.snapshot_root,
            self.runtime.tmp_dir,
            self.runtime.cache_dir,
        ):
            assert directory is not None
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.scratch_root.stat().st_uid != os.getuid():
            raise SandboxError(f"scratch root {self.scratch_root} is owned by another user")
        if self._initial_sweep is None:
            self._initial_sweep = asyncio.ensure_future(self._sweep_abandoned())
        await asyncio.shield(self._initial_sweep)

    async def _sweep_abandoned(self) -> None:
        """Recover sessions left by crashed processes, once per provider."""
        report = await sweep(
            self.session_root,
            self.runtime,
            grace_s=self.stop_grace_s,
            stop_timeout_s=self.stop_timeout_s,
        )
        for error in report.errors:
            log.warning("hpc-sandbox janitor: %s", error)

    async def _extract(self, image: ResolvedImage, session: Session) -> None:
        result = await self.runtime.run(
            "build",
            "--sandbox",
            "--fix-perms",
            str(session.rootfs),
            str(image.path),
            timeout_s=self.start_timeout_s,
        )
        if result.returncode != 0 or result.timed_out:
            raise SandboxStartError(f"could not extract {image.path}: {result.detail()}")
        await asyncio.to_thread(make_owner_writable, session.rootfs)

    async def _copy_snapshot(self, source: Path, session: Session) -> None:
        for name, target in (
            ("rootfs", session.rootfs),
            ("tmp", session.tmp),
            ("var-tmp", session.var_tmp),
        ):
            await self.copy_tree(source / name, target)

    async def copy_tree(self, source: Path, target: Path) -> None:
        """``cp -a --reflink=auto``: near-free on reflink filesystems, a full copy otherwise."""
        result = await run_process(
            ["cp", "-a", "--reflink=auto", "--", str(source), str(target)],
            timeout_s=self.start_timeout_s,
        )
        if result.returncode != 0 or result.timed_out:
            raise SandboxError(f"could not copy {source} to {target}: {result.detail()}")

    def _snapshot_dir(self, snapshot: SandboxSnapshot) -> Path:
        path = Path(snapshot.path)
        if snapshot.provider != self.name or snapshot.metadata.get("format") != SNAPSHOT_FORMAT:
            raise ValueError(f"snapshot {snapshot.id} was not taken by this provider")
        if path.parent != self.snapshot_root or path.name != snapshot.id or not path.is_dir():
            raise ValueError(f"snapshot {snapshot.id} is not in {self.snapshot_root}")
        return path


class SingularitySandbox:
    """One running instance; create it with :meth:`SingularityProvider.start`."""

    def __init__(self, provider: SingularityProvider, spec: SandboxSpec) -> None:
        self.id = new_instance_name()
        self.spec = spec
        self.memory_kills: list[memory.MemoryKill] = []
        self._provider = provider
        self._runtime = provider.runtime
        self._session: Session | None = None
        self._instance_pid: int | None = None
        self._instance_start_time: str | None = None
        self._instance_may_exist = False
        self._running = False
        self._lost: str | None = None
        self._resolv_conf = False
        # Active exec calls: host PID of the runtime process -> memory kills charged to it.
        self._execs: dict[int, list[memory.MemoryKill]] = {}
        self._watchdog: asyncio.Task[None] | None = None
        self._stop_task: asyncio.Future[None] | None = None

    def __repr__(self) -> str:
        return f"SingularitySandbox({self.id!r}, image={self.spec.image!r})"

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
            await asyncio.to_thread(remove_tree, target / "tmp" / _TRANSFER_DIR)
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
        """Stop the instance and delete its session; idempotent and cancellation-safe.

        Teardown always runs to completion; a cancelled caller sees
        ``CancelledError`` only afterwards. If teardown fails (an instance whose
        identity cannot be proved), the session is kept for the janitor and a
        later ``stop()`` retries.
        """
        task = self._stop_task
        if task is None or (task.done() and not task.cancelled() and task.exception()):
            task = self._stop_task = asyncio.ensure_future(self._teardown())
        await _finish(task)

    async def _boot(self, populate: Callable[[Session], Awaitable[None]]) -> None:
        provider = self._provider
        try:
            self._session = await asyncio.to_thread(open_session, provider.session_root, self.id)
            self._record()
            await populate(self._session)
            await asyncio.to_thread(self._prepare_rootfs)
            await self._start_instance()
            await self._probe()
        except BaseException as error:
            try:
                await self.stop()
            except Exception as cleanup_error:
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
                    instance_pid=lambda: self._instance_pid,
                    exec_roots=lambda: list(self._execs),
                    on_kill=self._on_memory_kill,
                    interval_s=provider.memory_poll_s,
                )
            )

    def _record(self) -> None:
        assert self._session is not None
        self._session.write_owner_record(
            labels=dict(self.spec.labels),
            image=self.spec.image,
            instance_pid=self._instance_pid,
            instance_start_time=self._instance_start_time,
        )

    def _prepare_rootfs(self) -> None:
        """Install the keep-alive startscript, mount points and private tmp dirs."""
        assert self._session is not None
        session = self._session
        metadata = session.rootfs / ".singularity.d"
        if metadata.is_symlink() or not metadata.is_dir():
            raise SandboxStartError(f"{metadata} is not a directory; is the image a SIF?")
        startscript = metadata / "startscript"
        startscript.unlink(missing_ok=True)
        startscript.write_text(KEEPALIVE_STARTSCRIPT)
        startscript.chmod(0o755)
        for target in _RESERVED_TARGETS:
            make_mount_point(session.rootfs, target, directory=True)
        home = session.rootfs / "root"  # HOME defaults to /root, which this uid owns here
        if not home.exists() and not home.is_symlink():
            home.mkdir()
        for mount in self.spec.mounts:
            make_mount_point(session.rootfs, mount.target, directory=Path(mount.source).is_dir())
        self._resolv_conf = (
            self.spec.network == "host"
            and self._provider.bind_resolv_conf
            and Path("/etc/resolv.conf").is_file()
        )
        if self._resolv_conf:
            try:
                make_mount_point(session.rootfs, "/etc/resolv.conf", directory=False)
            except SandboxStartError:
                self._resolv_conf = False  # the image links it elsewhere; keep its own
        for directory in (session.tmp, session.var_tmp):
            directory.mkdir(exist_ok=True)
            directory.chmod(0o1777)

    def _start_args(self) -> list[str]:
        assert self._session is not None
        session, provider = self._session, self._provider
        args = [
            "instance",
            "start",
            "--cleanenv",
            "--containall",
            "--no-home",
            "--no-eval",
            "--no-mount",
            "home,cwd,bind-paths",
            "--writable",
            "--pid-file",
            str(session.pid_file),
            "--bind",
            f"{session.tmp}:/tmp:rw",
            "--bind",
            f"{session.var_tmp}:/var/tmp:rw",
        ]
        if self._resolv_conf:
            args += ["--bind", f"{Path('/etc/resolv.conf').resolve()}:/etc/resolv.conf:ro"]
        for mount in self.spec.mounts:
            mode = "ro" if mount.read_only else "rw"
            args += ["--bind", f"{Path(mount.source).resolve()}:{mount.target}:{mode}"]
        if self.spec.network == "none":
            args += ["--net", "--network", "none"]
        if provider.gpu:
            args.append(f"--{provider.gpu}")
        if provider.cgroup_limits:
            if self.spec.cpus:
                args += ["--cpus", f"{self.spec.cpus:g}"]
            if self.spec.memory_mb:
                args += ["--memory", str(self.spec.memory_mb * 1024 * 1024)]
        return [*args, *provider.extra_start_args, str(session.rootfs), self.id]

    async def _start_instance(self) -> None:
        assert self._session is not None
        session, provider = self._session, self._provider
        async with locked(provider.session_root / CONTROL_LOCK):
            # Cancellation can arrive after the runtime created the instance but
            # before its result reaches us, so cleanup must assume it exists.
            self._instance_may_exist = True
            result = await self._runtime.run(
                *self._start_args(), timeout_s=provider.start_timeout_s
            )
            if result.returncode != 0 or result.timed_out:
                raise SandboxStartError(f"instance start failed: {result.detail()}")
            pid = await _poll(lambda: read_pid_file(session.pid_file), _PID_WAIT_S)
            if pid is None:
                raise SandboxStartError("the instance did not write its PID file")
            record = await self._runtime.instance_record(self.id)
            if record is None:
                raise SandboxStartError(
                    "the instance exited right after starting; the image needs /bin/sh and "
                    "sleep for the keep-alive loop"
                )
            if instance_pid(record) != pid:
                raise SandboxStartError("the instance PID file and runtime metadata disagree")
            started = await _poll(lambda: procfs.owned_live_start_time(pid), _PID_WAIT_S)
            if started is None:
                raise SandboxStartError("could not prove the instance process identity")
            self._instance_pid, self._instance_start_time = pid, started
            self._record()

    async def _probe(self) -> None:
        """Fail fast unless the rootfs and the workdir are writable."""
        marker = f"/.hpc-sandbox-probe-{uuid.uuid4().hex}"
        argv = ["/bin/sh", "-c", _PROBE_SCRIPT, "hpc-sandbox", self.spec.workdir, marker]
        result = await self._run(argv, cwd="/", env={}, stdin=None, timeout_s=_PROBE_TIMEOUT_S)
        if result.exit_code != 0:
            raise SandboxStartError(
                f"the sandbox rootfs or workdir {self.spec.workdir} is not writable "
                f"(exit {result.exit_code}): {result.stderr.strip()}. Never use --fakeroot "
                "on ABCI: it falls back to a read-only rootfs."
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
            result = await self._runtime.run(
                "exec",
                "--cleanenv",
                "--cwd",
                cwd,
                f"instance://{self.id}",
                *with_env(argv, env),
                timeout_s=timeout_s,
                stdin=stdin,
                output_limit=self._provider.output_limit_bytes,
                on_start=started,
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
        if result.returncode == 255 and not self._instance_alive():
            self._running = False
            self._lost = stderr.strip() or "the instance process is gone"
            raise SandboxLostError(f"sandbox {self.id} lost its instance: {self._lost}")
        return ExecResult(result.returncode, stdout, stderr)

    @asynccontextmanager
    async def _transfer(self) -> AsyncIterator[tuple[Path, str]]:
        """A fresh directory visible on the host and, through /tmp, in the sandbox."""
        assert self._session is not None
        token = uuid.uuid4().hex
        host_dir = self._session.tmp / _TRANSFER_DIR / token
        host_dir.mkdir(mode=0o700, parents=True)
        try:
            yield host_dir, f"/tmp/{_TRANSFER_DIR}/{token}"
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

    def _instance_alive(self) -> bool:
        return (
            self._instance_pid is not None
            and procfs.owned_live_start_time(self._instance_pid) == self._instance_start_time
        )

    def _require_running(self) -> None:
        if self._lost is not None:
            raise SandboxLostError(f"sandbox {self.id} lost its instance: {self._lost}")
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
        if self._instance_may_exist:
            async with locked(self._provider.session_root / CONTROL_LOCK):
                await stop_instance(
                    self._runtime,
                    self.id,
                    pid_file=session.pid_file,
                    expected_pid=self._instance_pid,
                    expected_start_time=self._instance_start_time,
                    grace_s=self._provider.stop_grace_s,
                    timeout_s=self._provider.stop_timeout_s,
                )
            self._instance_may_exist = False
        await asyncio.to_thread(remove_session, session)
        self._session = None


def check_env(env: Mapping[str, str]) -> None:
    for key, value in env.items():
        if not _ENV_NAME.fullmatch(key):
            raise ValueError(f"invalid environment variable name {key!r}")
        if "\0" in value:
            raise ValueError(f"environment variable {key} contains a NUL byte")


def with_env(argv: list[str], env: Mapping[str, str]) -> list[str]:
    """Wrap ``argv`` so it runs with ``env`` exported, values passed verbatim.

    ``--env`` splits on commas and ``--env-file`` is shell-evaluated by the
    runtime, so values are POSIX-quoted into a tiny ``sh`` prologue instead.
    """
    if not env:
        return argv
    exports = "".join(f"export {key}={shlex.quote(value)}\n" for key, value in env.items())
    return ["/bin/sh", "-c", f'{exports}exec "$@"', "hpc-sandbox", *argv]


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


def make_mount_point(rootfs: Path, target: str, *, directory: bool) -> None:
    """Create a bind target inside a writable rootfs without leaving it.

    ``--writable`` binds need existing targets. Components that are symlinks
    could point anywhere on the host, so they are refused.
    """
    path = rootfs
    parts = PurePosixPath(target).relative_to("/").parts
    for index, part in enumerate(parts):
        path = path / part
        if path.is_symlink():
            raise SandboxStartError(f"mount target {target} crosses a symlink in the image")
        try:
            if index < len(parts) - 1 or directory:
                path.mkdir(exist_ok=True)
            elif not path.exists():
                path.touch()
            elif not path.is_file():
                raise SandboxStartError(f"mount target {target} is not a file in the image")
        except FileExistsError as error:
            raise SandboxStartError(f"mount target {target} is not a directory") from error


def _append(text: str, line: str) -> str:
    separator = "\n" if text and not text.endswith("\n") else ""
    return f"{text}{separator}{line}\n"


async def _poll(probe: Callable[[], T | None], timeout_s: float) -> T | None:
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
