"""Rootless sandboxes as Singularity CE / Apptainer instances.

Each sandbox is one persistent instance started from a private, writable root
filesystem directory:

- the image is extracted with ``build --sandbox --fix-perms`` once per node
  into a template on node-local scratch, and each sandbox's session directory
  gets a copy of it (``cp -a --reflink=auto``: a reflink clone on XFS or Btrfs,
  a plain copy elsewhere), so no loop devices, overlays or ``--fakeroot`` are
  needed and the size of the writable layer is bounded only by the scratch
  filesystem (unlike ``--writable-tmpfs``, which many sites cap at a few tens
  of MiB);
- the instance runs with ``--cleanenv --containall --no-home --no-eval
  --no-mount home,cwd,bind-paths`` and session-private ``/tmp`` and
  ``/var/tmp`` binds, as the calling user;
- instances have exact UUID names, a ``--pid-file`` and PID start-time
  identities, and session directories are leased, so teardown and janitors
  stop only what they can prove is theirs and never use
  ``instance stop --all``. Unprivileged instance metadata is kept per user,
  not per job, so this matters on shared nodes. Starting and stopping one
  instance hold that instance's own control lock, so instances start and
  stop in parallel;
- every runtime call has a timeout, networking is the host's by default, and
  memory is guarded by a PSS watchdog because users rarely get cgroups.

OCI-derived images ship an empty ``%startscript``, and an instance whose
startscript exits is removed at once, so a keep-alive startscript is written
into the extracted rootfs.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import stat
import time
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from . import procfs
from .images import ImageStore, ResolvedImage, SifImages
from .locks import locked
from .paths import default_image_cache, should_stage
from .pods import PodService
from .process import DEFAULT_OUTPUT_LIMIT, ProcessResult
from .runtime import PASSED_DESPITE_CLEANENV, ContainerRuntime, instance_pid
from .sandbox import (
    RESERVED_TARGETS,
    SessionProvider,
    SessionSandbox,
    make_owner_writable,
    poll,
)
from .sessions import Session, remove_tree
from .types import SandboxError, SandboxStartError

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
PID_FILE = "instance.pid"
CONTROL_LOCK = ".instance-control.lock"
TEMPLATE_DIR = "templates"
TEMPLATE_MARKER = ".complete"
_PID_WAIT_S = 5.0


class SingularityProvider(SessionProvider["SingularitySandbox"]):
    """Start, restore and reap sandboxes backed by Singularity CE / Apptainer instances.

    ``scratch_root`` must be node-local (it holds every writable rootfs);
    ``image_cache`` may be shared storage that supports ``flock``. Set
    ``allow_network_none`` only where the site lets users create network
    namespaces, ``gpu`` to ``"nv"`` or ``"rocm"`` to pass GPUs through, and
    ``cgroup_limits`` where rootless cgroups work (Apptainer with systemd), to
    turn ``cpus``/``memory_mb`` into hard limits.

    Pods (:meth:`start_pod`) find slirp4netns for egress at ``slirp4netns``,
    else on PATH, else as fetched by ``hpc-sandbox fetch-tools``; their
    services get ``service_stop_grace_s`` between SIGTERM and SIGKILL when
    their pod stops.
    """

    name = "singularity"
    network_none_hint = (
        "network 'none' needs allow_network_none=True; setuid Singularity reserves "
        "network namespaces for root unless the site permits them"
    )

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
        slirp4netns: str | None = None,
        service_stop_grace_s: float = 0.0,
        rootfs_templates: bool = True,
    ) -> None:
        super().__init__(
            scratch_root=scratch_root,
            default_timeout_s=default_timeout_s,
            start_timeout_s=start_timeout_s,
            stop_timeout_s=stop_timeout_s,
            output_limit_bytes=output_limit_bytes,
            memory_poll_s=memory_poll_s,
        )
        self.sandbox_type = SingularitySandbox
        self.service_type = SingularityService
        self.slirp4netns_path = slirp4netns
        self.service_stop_grace_s = service_stop_grace_s
        self.runtime = ContainerRuntime(
            binary,
            tmp_dir=self.scratch_root / "runtime-tmp",
            cache_dir=self.scratch_root / "runtime-cache",
        )
        cache = Path(image_cache) if image_cache else default_image_cache()
        if stage_images is None:
            stage_images = should_stage(cache, self.scratch_root)
        self.images = ImageStore(
            SifImages(self.runtime),
            cache,
            stage_dir=self.scratch_root / "images" if stage_images else None,
            pull_timeout_s=start_timeout_s,
            prepare=self._make_scratch_directories,
        )
        self.stop_grace_s = stop_grace_s
        self.rootfs_templates = rootfs_templates
        self.allow_network_none = allow_network_none
        self.gpu = gpu
        self.cgroup_limits = cgroup_limits
        self.bind_resolv_conf = bind_resolv_conf
        self.extra_start_args = tuple(extra_start_args)
        self.capabilities = frozenset(
            {"snapshot", "isolation", "pods"}
            | ({"network_none"} if allow_network_none else set())
            | ({"gpu"} if gpu else set())
        )

    def _scratch_directories(self) -> list[Path]:
        assert self.runtime.tmp_dir is not None and self.runtime.cache_dir is not None
        return [*super()._scratch_directories(), self.runtime.tmp_dir, self.runtime.cache_dir]

    async def _extract(self, image: ResolvedImage, session: Session) -> None:
        if not self.rootfs_templates:
            await self._extract_into(image, session.rootfs)
            return
        template = await self._rootfs_template(image)
        await self.copy_tree(template, session.rootfs)

    async def _extract_into(self, image: ResolvedImage, rootfs: Path) -> None:
        result = await self.runtime.run(
            "build",
            "--sandbox",
            "--fix-perms",
            str(rootfs),
            str(image.path),
            timeout_s=self.start_timeout_s,
        )
        if result.returncode != 0 or result.timed_out:
            raise SandboxStartError(f"could not extract {image.path}: {result.detail()}")
        await asyncio.to_thread(make_owner_writable, rootfs)

    async def _rootfs_template(self, image: ResolvedImage) -> Path:
        """This node's extracted root filesystem of ``image``, which every sandbox of it copies.

        Extracted once per image file, under a lock per image, and published by
        a rename once complete. Like a staged image, a template is trusted later
        when it is a private directory of this uid holding its marker. No
        sandbox ever runs on it or mounts it: each gets a copy (reflinked where
        the filesystem can), so nothing a sandbox writes reaches the template.
        """
        key = f"sha256-{image.sha256}" if image.sha256 else _stat_name(image.path)
        root = self.scratch_root / TEMPLATE_DIR
        final = root / key
        if _template_ready(final):
            return final / "rootfs"
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        async with locked(root / f"{key}.lock"):
            if _template_ready(final):
                return final / "rootfs"
            work = root / f".{key}.{os.getpid()}.{uuid.uuid4().hex[:8]}"
            work.mkdir(mode=0o700)
            try:
                await self._extract_into(image, work / "rootfs")
                marker = {
                    "image": str(image.path),
                    "reference": image.reference,
                    "sha256": image.sha256,
                }
                (work / TEMPLATE_MARKER).write_text(json.dumps(marker, sort_keys=True) + "\n")
                os.rename(work, final)
            except BaseException:
                await asyncio.shield(asyncio.to_thread(remove_tree, work))
                raise
        return final / "rootfs"

    async def _stop_abandoned(self, path: Path, record: dict[str, Any] | None) -> bool:
        if record is not None and record.get("instance_pid_file") != str(path / PID_FILE):
            record = None
        async with locked(path / CONTROL_LOCK):
            if record is None and await self.runtime.instance_record(path.name) is not None:
                raise SandboxError("instance is running but its owner record is not bound to it")
            expected_pid = record.get("instance_pid") if record else None
            expected_start = record.get("instance_start_time") if record else None
            return await stop_instance(
                self.runtime,
                path.name,
                pid_file=path / PID_FILE,
                expected_pid=expected_pid if isinstance(expected_pid, int) else None,
                expected_start_time=str(expected_start) if expected_start else None,
                grace_s=self.stop_grace_s,
                timeout_s=self.stop_timeout_s,
            )


class SingularitySandbox(SessionSandbox):
    """One running instance; create it with :meth:`SingularityProvider.start`."""

    probe_hint = (
        ". A runtime that falls back to a read-only rootfs (for example --fakeroot "
        "where the site forces squashfuse) causes this."
    )

    def __init__(self, provider: SingularityProvider, spec: Any) -> None:
        super().__init__(provider, spec)
        self._provider: SingularityProvider = provider
        self._runtime = provider.runtime
        self._instance_pid: int | None = None
        self._instance_start_time: str | None = None
        self._instance_may_exist = False
        self._resolv_conf = False

    def _identity(self) -> dict[str, Any]:
        assert self._session is not None
        return {
            "instance_pid_file": str(self._session.path / PID_FILE),
            "instance_pid": self._instance_pid,
            "instance_start_time": self._instance_start_time,
        }

    def _user(self) -> dict[str, Any]:
        fakeroot = any(arg in ("--fakeroot", "-f") for arg in self._provider.extra_start_args)
        return {"remap_root": fakeroot, "container_uid": 0 if fakeroot else os.getuid()}

    def _watch_root(self) -> int | None:
        return self._instance_pid

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
        for target in RESERVED_TARGETS:
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
            str(session.path / PID_FILE),
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

    def _start_environment(self) -> dict[str, str]:
        """Variables ``instance start`` needs beyond the runtime's environment.

        Where the site sets ``use nvidia-container-cli = yes``, ``--nv`` with
        ``--containall`` exposes only the GPUs ``NVIDIA_VISIBLE_DEVICES`` names
        and none when it is unset, so default it to every GPU this process
        may use (inside a Slurm allocation, the allocated ones).
        """
        if self._provider.gpu == "nv" and "NVIDIA_VISIBLE_DEVICES" not in os.environ:
            return {"NVIDIA_VISIBLE_DEVICES": "all"}
        return {}

    async def _start_container(self) -> None:
        assert self._session is not None
        session, provider = self._session, self._provider
        pid_file = session.path / PID_FILE
        async with locked(session.path / CONTROL_LOCK):
            # Cancellation can arrive after the runtime created the instance but
            # before its result reaches us, so cleanup must assume it exists.
            self._instance_may_exist = True
            result = await self._runtime.run(
                *self._start_args(),
                timeout_s=provider.start_timeout_s,
                extra_env=self._start_environment(),
                container=True,
            )
            if result.returncode != 0 or result.timed_out:
                raise SandboxStartError(f"instance start failed: {result.detail()}")
            pid = await poll(lambda: read_pid_file(pid_file), _PID_WAIT_S)
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
            started = await poll(lambda: procfs.owned_live_start_time(pid), _PID_WAIT_S)
            if started is None:
                raise SandboxStartError("could not prove the instance process identity")
            self._instance_pid, self._instance_start_time = pid, started
            self._record()

    async def _spawn(
        self,
        command: list[str],
        *,
        stdin: bytes | None,
        timeout_s: float,
        on_start: Callable[[int], None],
    ) -> ProcessResult:
        # The prologue enters the working directory; "/" always exists.
        return await self._runtime.run(
            "exec",
            "--cleanenv",
            "--cwd",
            "/",
            f"instance://{self.id}",
            *command,
            timeout_s=timeout_s,
            stdin=stdin,
            output_limit=self._output_limit,
            on_start=on_start,
            container=True,
        )

    def _container_alive(self) -> bool:
        return (
            self._instance_pid is not None
            and procfs.owned_live_start_time(self._instance_pid) == self._instance_start_time
        )

    async def _stop_container(self, session: Session) -> None:
        if not self._instance_may_exist:
            return
        async with locked(session.path / CONTROL_LOCK):
            await stop_instance(
                self._runtime,
                self.id,
                pid_file=session.path / PID_FILE,
                expected_pid=self._instance_pid,
                expected_start_time=self._instance_start_time,
                grace_s=self._provider.stop_grace_s,
                timeout_s=self._provider.stop_timeout_s,
            )
        self._instance_may_exist = False


class SingularityService(PodService, SingularitySandbox):
    """A pod service as a long-lived ``singularity exec``; start it with ``pod.start_service``.

    The launcher's init puts the runtime in a nested user namespace of its
    own, so Singularity runs unprivileged (its non-setuid starter) as root of
    that namespace, which holds no capability over the pod's network, and its
    starter is never the service's PID 1. ``--containall`` gives the container
    its own PID and IPC namespaces too; commands enter all of them.
    """

    anchor_options = ("--userns",)

    def _user(self) -> dict[str, Any]:
        return {"remap_root": True, "container_uid": 0}

    def _prepare_service_rootfs(self) -> None:
        assert self._session is not None
        rootfs = self._session.rootfs
        make_mount_point(rootfs, "/etc", directory=True)  # never through a link
        for target in ("/etc/hosts", "/etc/resolv.conf"):
            path = rootfs / target.lstrip("/")
            if path.is_symlink():  # an image's link, followed by nothing yet
                path.unlink()
            make_mount_point(rootfs, target, directory=False)
        make_mount_point(rootfs, "/dev/shm", directory=True)

    def _runtime_command(self, argv: list[str]) -> list[str]:
        assert self._session is not None
        session = self._session
        args = [
            "exec",
            "--cleanenv",
            "--no-eval",
            "--writable",
            "--containall",
            "--no-home",
            "--no-mount",
            "home,cwd,bind-paths",
            "--bind",
            f"{session.tmp}:/tmp:rw",
            "--bind",
            f"{session.var_tmp}:/var/tmp:rw",
            "--bind",
            f"{self.shm_path}:/dev/shm:rw",
            "--bind",
            f"{self.hosts_path}:/etc/hosts:ro",
            "--bind",
            f"{self.resolv_path}:/etc/resolv.conf:ro",
        ]
        for mount in self.spec.mounts:
            mode = "ro" if mount.read_only else "rw"
            args += ["--bind", f"{Path(mount.source).resolve()}:{mount.target}:{mode}"]
        return [self._runtime.path, *args, str(session.rootfs), *argv]

    def _runtime_environment(self) -> dict[str, str]:
        environment = self._runtime.environment()
        return {k: v for k, v in environment.items() if k not in PASSED_DESPITE_CLEANENV}


def _template_ready(path: Path) -> bool:
    """Whether ``path`` is a complete template: a private directory of this uid with its marker."""
    try:
        info = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISDIR(info.st_mode)
        and info.st_uid == os.getuid()
        and not stat.S_IMODE(info.st_mode) & 0o077
        and (path / TEMPLATE_MARKER).is_file()
        and (path / "rootfs").is_dir()
    )


def _stat_name(path: Path) -> str:
    """A template name for an image file whose digest is unknown: its stat identity."""
    info = path.stat()
    return f"stat-{info.st_dev}-{info.st_ino}-{info.st_size}-{info.st_mtime_ns}"


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

    Acts only when the runtime's PID, the PID file, the recorded PID and the
    process start time agree, and fails closed otherwise. Call with the
    instance-control lock held.
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
