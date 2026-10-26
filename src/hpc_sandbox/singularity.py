"""Rootless sandboxes as Singularity CE / Apptainer instances.

Each sandbox is one persistent instance started from a private, writable root
filesystem directory:

- the image is extracted with ``build --sandbox --fix-perms`` into the
  sandbox's session directory on node-local scratch, or, with
  ``rootfs_templates``, once per node into a template that each sandbox's
  session gets a copy of (``cp -a --reflink=auto``: a reflink clone on XFS or
  Btrfs, a plain copy elsewhere), so no loop devices, overlays or
  ``--fakeroot`` are needed and the size of the writable layer is bounded
  only by the scratch filesystem (unlike ``--writable-tmpfs``, which many
  sites cap at a few tens of MiB);
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

With ``rootfs_layout="fuse-overlay"`` a sandbox's root filesystem is not a copy
but a ``fuse-overlayfs`` mount, made by this user: the node's template of the
image is the read-only lower layer and the session's ``overlay/upper`` holds
what the sandbox writes, with Docker's semantics (whiteouts, opaque and renamed
directories). The instance runs it unprivileged (``--userns --writable``), so
a start and a stop cost the same whatever the image's size, and deleting a
session deletes only what its sandbox wrote. A snapshot is a copy of the upper
layer and names the template it lies over; a restore mounts a copy of it over
that template, so neither copies the image. File operations in the sandbox go
through FUSE: metadata-heavy ones (``find``, many small files) are slower than
on a copy. GPUs, cgroup limits and ``--fakeroot`` need the copy layout, and pod
services always get a copy.

With ``fakeroot=True`` (copy layout only) an instance starts with ``--fakeroot``:
commands run as root of a user namespace that maps root to the calling user
and the user's subordinate ids (``/etc/subuid``, ``/etc/subgid``) to the users
after it, so they can switch users as in Docker, and what they write as those
users belongs to subordinate ids on the host, which the calling user cannot
delete. So once such an instance has stopped, a short ``exec --fakeroot`` over
the session's root filesystem and private ``/tmp`` and ``/var/tmp`` gives those
files back to root of the namespace, the calling user, before the session is
deleted (:func:`restore_owner`); janitors do the same for an abandoned session
whose owner record says it ran so. Extraction gives every file of the image to
the calling user, so root inside sees them all as its own: a spec's file owners
manifest gives the paths it lists their owners in the image, as root of the
instance's user namespace (``nsenter`` into it runs the launcher's
``file-owners`` form, :mod:`hpc_sandbox.anchor`), once the instance runs and
before any command does. A snapshot copies the files as root of that namespace
too, which reads those of every user it maps, and gives each copy to the
calling user.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import shutil
import signal
import stat
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from . import procfs
from .images import ImageStore, ResolvedImage, SifImages
from .locks import locked
from .paths import default_image_cache, should_stage
from .pods import ANCHOR_SCRIPT, PodService
from .process import DEFAULT_OUTPUT_LIMIT, ProcessResult, run_process
from .runtime import PASSED_DESPITE_CLEANENV, ContainerRuntime, instance_pid
from .sandbox import (
    RESERVED_TARGETS,
    SNAPSHOT_FORMAT,
    SessionProvider,
    SessionSandbox,
    make_owner_writable,
    poll,
)
from .sessions import Session, leased, remove_tree
from .types import SandboxError, SandboxSnapshot, SandboxStartError

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
PID_FILE = "instance.pid"
CONTROL_LOCK = ".instance-control.lock"
TEMPLATE_DIR = "templates"
TEMPLATE_MARKER = ".complete"
ROOTFS_LAYOUTS = ("copy", "fuse-overlay")
# How extra_start_args may ask for --fakeroot.
_FAKEROOT_FLAGS = ("--fakeroot", "-f")
OVERLAY_DIR = (
    "overlay"  # <session>/overlay/{upper,work}: the writable layer of a fuse-overlay rootfs
)
OVERLAY_RECORD = (
    "fuse-overlay.json"  # <session>/fuse-overlay.json: the mount, for teardown and janitors
)
#: A snapshot of a fuse-overlay sandbox: its upper layer (``upper/``), and the template it lies over
#: (``lower``).
OVERLAY_SNAPSHOT_FORMAT = "hpc-sandbox/fuse-overlay-v1"
# The type a fuse-overlayfs mount has in the mount table, the name of its daemon, and where a
# session's (or its trash entry's) rootfs lies.
OVERLAY_FSTYPE = "fuse.fuse-overlayfs"
OVERLAY_DAEMON = "fuse-overlayfs"
_SESSION_ROOTFS = re.compile(r".*/hpcsb-[0-9a-f]{32}(\.[0-9]+-[0-9]+\.[0-9a-f]{8})?/rootfs")
# An extraction of a template, or a damaged template moved aside, by process PID started at START.
_EXTRACTION = re.compile(
    r"\.(?P<key>.+)\.(?P<pid>[0-9]+)-(?P<start>[0-9]+)\.[0-9a-f]{8}(?P<damaged>\.damaged)?"
)
_PID_WAIT_S = 5.0
_UNMOUNT_TIMEOUT_S = 30.0
# How long giving back the files of a stopped --fakeroot sandbox may take: it walks the whole
# root filesystem.
_RESTORE_TIMEOUT_S = 600.0
# sh -c RESTORE_OWNER_SCRIPT, run as root of a --fakeroot container over a stopped session's
# root filesystem with its private /tmp and /var/tmp bound: give root, the calling user outside,
# everything another id of the namespace owns there, never following a link or crossing into
# another filesystem. What it cannot change is not the session's (the mount points of /proc, /sys
# and /dev, and files the runtime binds in, show an unmapped owner): chown's failures on them are
# ignored, and the session's deletion still refuses any directory of another user left in it. It
# exits 127 when the image lacks the tools, and with find's status when find fails (one without
# -user, -group or -exec {} +).
RESTORE_OWNER_SCRIPT = r"""
for tool in find chown; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "hpc-sandbox: the image has no $tool" >&2
    exit 127
  fi
done
set -- /
for directory in /tmp /var/tmp; do
  if [ -d "$directory" ]; then set -- "$@" "$directory"; fi
done
find "$@" -xdev \( ! -user 0 -o ! -group 0 \) \
  -exec /bin/sh -c 'chown -h 0:0 -- "$@" 2>/dev/null; exit 0' hpc-sandbox-chown {} +
"""
_PROBE_TIMEOUT_S = 5.0  # a live daemon answers statfs at once; a stopped one never does


class SingularityProvider(SessionProvider["SingularitySandbox"]):
    """Start, restore and reap sandboxes backed by Singularity CE / Apptainer instances.

    ``scratch_root`` must be node-local (it holds every writable rootfs);
    ``image_cache`` may be shared storage that supports ``flock``. Set
    ``allow_network_none`` only where the site lets users create network
    namespaces, ``gpu`` to ``"nv"`` or ``"rocm"`` to pass GPUs through, and
    ``cgroup_limits`` where rootless cgroups work (Apptainer with systemd), to
    turn ``cpus``/``memory_mb`` into hard limits. ``fakeroot`` starts instances
    with ``--fakeroot`` (copy layout only, where the site gives users
    subordinate ids): commands run as root and can switch to other users, and
    what those users write is given back to your uid before a session is
    deleted. ``extra_start_args`` holding ``--fakeroot`` means the same.

    Pods (:meth:`start_pod`) find slirp4netns for egress at ``slirp4netns``,
    else on PATH, else as fetched by ``hpc-sandbox fetch-tools``; their
    services get ``service_stop_grace_s`` between SIGTERM and SIGKILL when
    their pod stops.
    """

    name = "singularity"
    snapshot_formats = (SNAPSHOT_FORMAT, OVERLAY_SNAPSHOT_FORMAT)
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
        rootfs_templates: bool | None = None,
        rootfs_layout: Literal["copy", "fuse-overlay"] | None = None,
        fakeroot: bool = False,
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
        self.allow_network_none = allow_network_none
        self.gpu = gpu
        self.cgroup_limits = cgroup_limits
        self.bind_resolv_conf = bind_resolv_conf
        self.extra_start_args = tuple(extra_start_args)
        self.fakeroot = fakeroot or any(arg in _FAKEROOT_FLAGS for arg in self.extra_start_args)
        self.capabilities = frozenset(
            {"snapshot", "isolation", "pods"}
            | ({"network_none"} if allow_network_none else set())
            | ({"gpu"} if gpu else set())
        )
        layout = rootfs_layout or os.environ.get("HPC_SANDBOX_ROOTFS_LAYOUT") or "copy"
        if layout not in ROOTFS_LAYOUTS:
            raise ValueError(f"rootfs_layout must be one of {ROOTFS_LAYOUTS}, not {layout!r}")
        if layout == "fuse-overlay":
            conflict = (
                "GPUs, cgroup limits and --fakeroot need rootfs_layout='copy'"
                if gpu or cgroup_limits or self.fakeroot
                else "it needs rootfs_templates"
                if rootfs_templates is False
                else None
            )
            if conflict and rootfs_layout is not None:
                raise ValueError(f"the fuse-overlay layout runs unprivileged instances: {conflict}")
            if conflict:  # a site-wide default this provider cannot take
                layout = "copy"
        self.rootfs_layout = layout
        # A copy of a template creates as many inodes as an extraction: on XFS, 64-128 copies at
        # once took longer than 64-128 extractions (reflinks update refcounts too), so the copy
        # layout extracts per sandbox unless asked; the fuse-overlay layout needs the template as
        # its lower layer.
        self.rootfs_templates = (
            layout == "fuse-overlay" if rootfs_templates is None else rootfs_templates
        )
        self._copy_templates = bool(rootfs_templates)  # in the copy layout, and pod services

    def _scratch_directories(self) -> list[Path]:
        assert self.runtime.tmp_dir is not None and self.runtime.cache_dir is not None
        return [*super()._scratch_directories(), self.runtime.tmp_dir, self.runtime.cache_dir]

    def _file_owners_refusal(self) -> str | None:
        if self.fakeroot:
            return None
        return (
            "file_owners need fakeroot=True: without --fakeroot, a sandbox's user namespace maps "
            "only your uid, so its files cannot belong to other users"
        )

    async def _extract(self, image: ResolvedImage, session: Session) -> None:
        if self.rootfs_layout == "fuse-overlay":
            template = await self._rootfs_template(image)
            await mount_overlay(template, session, timeout_s=self.start_timeout_s)
            return
        await self._extract_service(image, session)

    async def _extract_service(self, image: ResolvedImage, session: Session) -> None:
        # The copy layout whatever the provider's: nothing unmounts a service's rootfs.
        if not self._copy_templates:
            await self._extract_into(image, session.rootfs)
            return
        await self.copy_tree(await self._rootfs_template(image), session.rootfs)

    async def restore(self, snapshot: SandboxSnapshot, spec: Any) -> SingularitySandbox:
        """Start a sandbox for ``spec`` whose files come from ``snapshot``; a fuse-overlay sandbox's
        snapshot (its upper layer) is mounted over the template it was taken over, with a copy of
        that upper layer."""
        if snapshot.metadata.get("format") != OVERLAY_SNAPSHOT_FORMAT:
            return await super().restore(snapshot, spec)
        self._check(spec)
        source = self._snapshot_dir(snapshot)
        if snapshot.image != spec.image:
            raise ValueError(f"snapshot {snapshot.id} is of {snapshot.image!r}, not {spec.image!r}")
        if self.rootfs_layout != "fuse-overlay":
            raise ValueError(
                f"snapshot {snapshot.id} is of a fuse-overlay sandbox; "
                "restore it with rootfs_layout='fuse-overlay'"
            )
        lower = self._snapshot_lower(snapshot)
        await self._check_file_owners(spec)
        await self._prepare()
        return await self._launch(
            spec, partial(self._restore_overlay, source, lower), restored=True
        )

    def _snapshot_lower(self, snapshot: SandboxSnapshot) -> Path:
        """The template a fuse-overlay snapshot lies over: one of this node's complete templates, or
        ValueError."""
        lower = Path(str(snapshot.metadata.get("lower", "")))
        templates = self.scratch_root / TEMPLATE_DIR
        if (
            lower.name != "rootfs"
            or lower.parent.parent != templates
            or not _template_ready(lower.parent)
        ):
            raise ValueError(
                f"snapshot {snapshot.id} lies over {str(lower)!r}, which is not a template of this "
                f"node (in {templates})"
            )
        return lower

    async def _restore_overlay(self, source: Path, lower: Path, session: Session) -> None:
        (session.path / OVERLAY_DIR).mkdir(mode=0o700)
        await self.copy_tree(source / "upper", session.path / OVERLAY_DIR / "upper")
        await mount_overlay(lower, session, timeout_s=self.start_timeout_s, upper_exists=True)
        for name, target in (("tmp", session.tmp), ("var-tmp", session.var_tmp)):
            await self.copy_tree(source / name, target)

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
        """This node's extracted root filesystem of ``image``, which copy layout
        sandboxes copy and fuse-overlay sandboxes mount as their lower layer.

        Extracted once per image file, under a lock per image, and published by
        a rename once complete. Like a staged image, a template is trusted later
        when it is a private directory of this uid holding its marker; one that
        lost it (to an age-based cleaner, or a removal cut short) is moved aside
        and extracted again. No sandbox writes to a template: a copy (reflinked
        where the filesystem can) is the sandbox's own, and an overlay writes
        to its upper layer.
        """
        key = f"sha256-{image.sha256}" if image.sha256 else _stat_name(image.path)
        root = self.scratch_root / TEMPLATE_DIR
        final = root / key
        if _template_ready(final):
            return final / "rootfs"
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        damaged = None
        async with locked(root / f"{key}.lock"):
            if _template_ready(final):
                return final / "rootfs"
            if os.path.lexists(final):
                damaged = root / f"{_extraction_name(key)}.damaged"
                os.rename(final, damaged)
            work = root / _extraction_name(key)
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
        if damaged is not None:  # unless an overlay still uses it: then a janitor deletes it later
            for error in await asyncio.to_thread(_remove_extractions, [damaged]):
                log.warning("hpc-sandbox: %s", error)
        return final / "rootfs"

    async def _sweep_node(self) -> list[str]:
        errors = await unmount_dead_overlays()
        templates = self.scratch_root / TEMPLATE_DIR
        return errors + await asyncio.to_thread(_remove_abandoned_extractions, templates)

    async def _stop_abandoned(self, path: Path, record: dict[str, Any] | None) -> bool:
        if record is not None and record.get("instance_pid_file") != str(path / PID_FILE):
            record = None
        # Starts take the control lock before an instance; without one (nothing started, and a
        # full scratch has no room to create it), nothing can be starting one now either.
        control = (path / CONTROL_LOCK).exists()
        async with locked(path / CONTROL_LOCK) if control else contextlib.nullcontext():
            if record is None and await self.runtime.instance_record(path.name) is not None:
                raise SandboxError("instance is running but its owner record is not bound to it")
            expected_pid = record.get("instance_pid") if record else None
            expected_start = record.get("instance_start_time") if record else None
            stopped = await stop_instance(
                self.runtime,
                path.name,
                pid_file=path / PID_FILE,
                expected_pid=expected_pid if isinstance(expected_pid, int) else None,
                expected_start_time=str(expected_start) if expected_start else None,
                grace_s=self.stop_grace_s,
                timeout_s=self.stop_timeout_s,
            ) | await unmount_overlay(path)
        if record is not None and record.get("remap_root") is True:  # it ran with --fakeroot
            await _restore_owner_or_warn(self.runtime, path)
        return stopped


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
        fakeroot = self._provider.fakeroot
        return {"remap_root": fakeroot, "container_uid": 0 if fakeroot else os.getuid()}

    def _workdir_umask(self) -> str:
        # The other users of a --fakeroot sandbox must be able to enter a working directory that
        # the start makes: as Docker's -w makes one, mode 755 whatever the caller's umask.
        return "022" if self._provider.fakeroot else ""

    def _watch_root(self) -> int | None:
        return self._instance_pid

    async def _apply_file_owners(self) -> None:
        """Give the image's paths that ``spec.file_owners`` lists their owners, as root of the
        instance's user namespace, which maps them with ``--fakeroot``: the launcher's
        ``file-owners`` form, run there, changes them through the host's view of the session's
        root filesystem, which is the instance's root, before any command runs."""
        if self.spec.file_owners is None:
            return
        assert self._session is not None
        command = [
            sys.executable,
            "-I",
            "-S",
            str(ANCHOR_SCRIPT),
            str(os.getpid()),
            "file-owners",
            os.path.abspath(self.spec.file_owners),
            str(self._session.rootfs),
        ]
        result = await self._as_instance_root(
            command, "apply its file owners", self._provider.start_timeout_s, SandboxStartError
        )
        if result.returncode != 0 or result.timed_out:
            raise SandboxStartError(
                f"could not apply the file owners of {self.spec.file_owners}: {result.detail()}"
            )
        if report := result.stdout.decode(errors="replace").strip():
            log.warning("sandbox %s: %s", self.id, report)

    async def _snapshot_copy(self, source: Path, target: Path) -> None:
        """A ``--fakeroot`` sandbox's snapshot copies its files as root of the instance's user
        namespace, which reads those of every user it maps whatever their modes, and gives each
        copy to the calling user (``--no-preserve=ownership``): as that user, the copy failed on
        a file or directory of another user that kept it out."""
        if not self._provider.fakeroot:
            await super()._snapshot_copy(source, target)
            return
        command = ["cp", "-a", "--no-preserve=ownership", "--reflink=auto", "--"]
        result = await self._as_instance_root(
            [*command, str(source), str(target)],
            "copy its files",
            self._provider.start_timeout_s,
            SandboxError,
        )
        if result.returncode != 0 or result.timed_out:
            raise SandboxError(f"could not copy {source} to {target}: {result.detail()}")

    async def _as_instance_root(
        self, command: list[str], doing: str, timeout_s: float, error: type[SandboxError]
    ) -> ProcessResult:
        """Run ``command`` as root of the instance's user namespace, in this mount namespace:
        ``nsenter`` enters the namespace through a descriptor opened before the instance's
        identity is proven again, so it is this instance's. Raises ``error``, saying what could
        not be done (``doing``), when the instance is gone."""
        assert self._instance_pid is not None
        pid = self._instance_pid
        try:
            namespace = os.open(f"/proc/{pid}/ns/user", os.O_RDONLY | os.O_CLOEXEC)
        except OSError as failure:
            raise error(
                f"could not {doing}: cannot enter the instance's user namespace: {failure}"
            ) from failure
        try:
            if procfs.owned_live_start_time(pid) != self._instance_start_time:
                raise error(f"could not {doing}: the instance exited")
            argv = [
                self._provider.nsenter,
                f"--user=/proc/{os.getpid()}/fd/{namespace}",
                "--preserve-credentials",
                "--",
                *command,
            ]
            environment = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
            return await run_process(argv, timeout_s=timeout_s, env=environment)
        finally:
            os.close(namespace)

    async def _snapshot_rootfs(self, target: Path) -> tuple[str, dict[str, Any]]:
        """A fuse-overlay sandbox's snapshot is its upper layer, what it wrote over the node's
        template (which never changes); a copy-layout sandbox's is its whole rootfs."""
        assert self._session is not None
        record_path = self._session.path / OVERLAY_RECORD
        if not record_path.exists():
            return await super()._snapshot_rootfs(target)
        record = json.loads(record_path.read_text())
        await self._provider.copy_tree(Path(record["upper"]), target / "upper")
        return OVERLAY_SNAPSHOT_FORMAT, {"lower": record["lower"]}

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
            *(["--userns"] if (session.path / OVERLAY_RECORD).exists() else []),
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
        if provider.fakeroot and not set(_FAKEROOT_FLAGS) & set(provider.extra_start_args):
            args.append("--fakeroot")
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
        if self._instance_may_exist:
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
        # Only once nothing runs on it: a session whose instance could not be stopped stays mounted.
        await unmount_overlay(session.path)
        if self._provider.fakeroot:  # and only once nothing runs that could write more
            await _restore_owner_or_warn(self._runtime, session.path)


async def restore_owner(runtime: ContainerRuntime, session_path: Path) -> None:
    """Give the calling user back what other users of a stopped ``--fakeroot`` sandbox wrote.

    Run as root of a new ``--fakeroot`` container over the session's root filesystem, with its
    private ``/tmp`` and ``/var/tmp`` bound, :data:`RESTORE_OWNER_SCRIPT` makes root of the
    namespace, the calling user outside, the owner of every file another id owns there. Those
    files belong to the user's subordinate ids on the host, so without this nothing could delete
    the session. Raises :class:`SandboxError` when the container could not run it.
    """
    rootfs = session_path / "rootfs"
    if rootfs.is_symlink() or not rootfs.is_dir():
        return  # never filled: nothing ran in it
    binds = []
    for name, target in (("tmp", "/tmp"), ("var-tmp", "/var/tmp")):
        directory = session_path / name
        if directory.is_dir() and not directory.is_symlink():
            binds += ["--bind", f"{directory}:{target}:rw"]
    result = await runtime.run(
        "exec",
        "--fakeroot",
        "--cleanenv",
        "--containall",
        "--no-home",
        "--no-eval",
        "--no-mount",
        "home,cwd,bind-paths",
        "--writable",
        *binds,
        str(rootfs),
        "/bin/sh",
        "-c",
        RESTORE_OWNER_SCRIPT,
        timeout_s=_RESTORE_TIMEOUT_S,
        container=True,
    )
    if result.returncode != 0 or result.timed_out:
        raise SandboxError(
            "could not give back the files other users of the --fakeroot sandbox left in "
            f"{session_path}: {result.detail()}"
        )


async def _restore_owner_or_warn(runtime: ContainerRuntime, session_path: Path) -> None:
    """:func:`restore_owner`, logging a failure: the session's deletion then names what it left."""
    try:
        await restore_owner(runtime, session_path)
    except SandboxError as error:
        log.warning("hpc-sandbox: %s", error)


def _fuse_tools() -> tuple[str, str]:
    mount = shutil.which("fuse-overlayfs")
    if mount is None:
        raise SandboxStartError(
            "the fuse-overlay layout needs fuse-overlayfs and fusermount3 (or fusermount) on PATH"
        )
    return mount, _fusermount()


def _fusermount() -> str:
    """What unmounts an overlay: a janitor needs only this, not fuse-overlayfs."""
    unmount = shutil.which("fusermount3") or shutil.which("fusermount")
    if unmount is None:
        raise SandboxError("unmounting a fuse-overlay needs fusermount3 (or fusermount) on PATH")
    return unmount


async def mount_overlay(
    template: Path, session: Session, *, timeout_s: float, upper_exists: bool = False
) -> None:
    """Mount ``session.rootfs`` as a fuse-overlayfs of ``template`` (read-only lower layer) and the
    session's own ``overlay/upper``, new or (``upper_exists``, a restore) already filled.  The
    record is written first, so a teardown or a janitor unmounts whatever may be mounted."""
    mount, _ = _fuse_tools()
    overlay = session.path / OVERLAY_DIR
    upper, work = overlay / "upper", overlay / "work"
    for directory in (work, session.rootfs) if upper_exists else (upper, work, session.rootfs):
        directory.mkdir(mode=0o700, parents=True)
    for path in (template, upper, work):
        if any(c in str(path) for c in ",:"):
            raise SandboxStartError(f"fuse-overlayfs cannot take a path with ',' or ':': {path}")
    record = {"mountpoint": str(session.rootfs), "lower": str(template), "upper": str(upper)}
    temporary = session.path / f".{OVERLAY_RECORD}.tmp"
    temporary.write_text(json.dumps(record, sort_keys=True) + "\n")
    os.replace(temporary, session.path / OVERLAY_RECORD)
    result = await run_process(
        [mount, "-o", f"lowerdir={template},upperdir={upper},workdir={work}", str(session.rootfs)],
        timeout_s=timeout_s,
    )
    if result.returncode != 0 or result.timed_out:
        raise SandboxStartError(f"could not mount the overlay of {template}: {result.detail()}")


def _mounted(path: Path) -> bool:
    """Whether something is mounted at ``path`` in this mount namespace.

    The mount table holds real paths, octal-escaped; ``path`` itself is never
    looked up, since a mount whose daemon died answers nothing.
    """
    target = os.path.join(os.path.realpath(path.parent), path.name)
    return any(mount.point == target for mount in procfs.mount_table())


async def unmount_overlay(session_path: Path) -> bool:
    """Unmount a session's fuse-overlay rootfs, if it has one that is mounted; whether one was.

    Raises :class:`SandboxError` if it stays mounted, so the session is kept rather than deleted
    through it."""
    if not (session_path / OVERLAY_RECORD).exists():
        return False
    mountpoint = session_path / "rootfs"  # where mount_overlay mounts, whatever the record says
    if not _mounted(mountpoint):
        return False
    unmount = _fusermount()
    for args in (["-u"], ["-u", "-z"]):
        result = await run_process([unmount, *args, str(mountpoint)], timeout_s=_UNMOUNT_TIMEOUT_S)
        if not _mounted(mountpoint):
            return True
    raise SandboxError(f"could not unmount the overlay at {mountpoint}: {result.detail()}")


async def unmount_dead_overlays() -> list[str]:
    """Unmount this user's session overlays whose fuse-overlayfs died, wherever they lie; returns
    errors.

    A fuse-overlayfs killed with SIGKILL (a job or step whose processes Slurm
    kills, a crash) leaves its mount in the node's mount table, where it
    counts towards fusermount's ``mount_max`` (FUSE mounts of every user on the
    node); with a per-job scratch root, no janitor sweeps its session again.
    An overlay is dead when nobody holds its session's lease, no fuse-overlayfs
    of this user serves its device, and ``statfs`` of it fails with
    ``ENOTCONN``: never ``stat``, which the kernel answers from fuse-overlayfs's
    attribute cache, valid for decades. Probes run in a child process with a
    time limit, since one of a stopped daemon's mount would wait for it.
    """
    mountinfo = await asyncio.to_thread(Path("/proc/self/mountinfo").read_bytes)
    candidates = await asyncio.to_thread(_unserved_overlays, mountinfo)
    dead = [mountpoint for mountpoint in candidates if await _disconnected(mountpoint)]
    if not dead:
        return []
    try:
        unmount = _fusermount()
    except SandboxError as error:
        return [f"cannot unmount {len(dead)} dead overlays: {error}"]
    errors = []
    for mountpoint in dead:
        result = await run_process([unmount, "-u", "-z", mountpoint], timeout_s=_UNMOUNT_TIMEOUT_S)
        if (result.returncode != 0 or result.timed_out) and _mounted(Path(mountpoint)):
            errors.append(f"could not unmount the dead overlay at {mountpoint}: {result.detail()}")
    return errors


def _unserved_overlays(mountinfo: str | bytes) -> list[str]:
    """This user's session overlays that nobody's lease holds and no fuse-overlayfs of this user
    serves."""
    mounts = procfs.mounts(mountinfo)
    owner = f"user_id={os.getuid()}"
    served_paths = _served_overlays()
    served = {mount.device for mount in mounts if mount.point in served_paths}
    return [
        mount.point
        for mount in mounts
        if mount.fstype == OVERLAY_FSTYPE
        and owner in mount.options.split(",")
        and _SESSION_ROOTFS.fullmatch(mount.point)
        and mount.device not in served  # a mount shows up at every place it propagated to
        and _private_directory(Path(mount.point).parent)
        and not leased(Path(mount.point).parent)
    ]


def _private_directory(path: Path) -> bool:
    """Whether ``path`` is a directory of this uid's that only it may enter: a session's."""
    try:
        info = path.lstat()
    except OSError:
        return False
    return stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077


async def _disconnected(mountpoint: str) -> bool:
    """Whether ``mountpoint``'s FUSE daemon is gone: its ``statfs`` fails with ``ENOTCONN``."""
    probe = (
        "import errno, os, sys\n"
        "try:\n    os.statvfs(sys.argv[1])\n"
        "except OSError as error:\n    sys.exit(3 if error.errno == errno.ENOTCONN else 1)"
    )
    result = await run_process(
        [sys.executable, "-I", "-S", "-c", probe, mountpoint], timeout_s=_PROBE_TIMEOUT_S
    )
    return result.returncode == 3 and not result.timed_out


def _served_overlays() -> set[str]:
    """The real paths of the mount points this user's fuse-overlayfs processes serve."""
    served = set()
    for argv in _overlay_daemons().values():
        target = argv[-1]
        served.add(
            os.path.join(os.path.realpath(os.path.dirname(target)), os.path.basename(target))
        )
    return served


def _overlay_daemons() -> dict[int, list[str]]:
    """This user's fuse-overlayfs processes, and their arguments."""
    daemons = {}
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        try:
            if entry.stat().st_uid != os.getuid():
                continue
        except OSError:
            continue
        argv = procfs.argv(int(entry.name))
        if argv and len(argv) > 1 and os.path.basename(argv[0]) == OVERLAY_DAEMON:
            daemons[int(entry.name)] = argv
    return daemons


def _overlay_layers() -> list[Path]:
    """The directories this user's fuse-overlayfs processes hold open, their layers among
    them, under their current names."""
    layers = []
    for pid in _overlay_daemons():
        with contextlib.suppress(OSError):
            for descriptor in os.scandir(f"/proc/{pid}/fd"):
                with contextlib.suppress(OSError):
                    target = os.readlink(descriptor.path)
                    if target.startswith("/"):
                        layers.append(Path(target))
    return layers


def _extraction_name(key: str) -> str:
    """A name for an extraction of template ``key`` by this process, or a damaged template it moved
    aside."""
    return f".{key}.{os.getpid()}-{procfs.start_time(os.getpid())}.{uuid.uuid4().hex[:8]}"


def _remove_abandoned_extractions(root: Path) -> list[str]:
    """Delete the extractions in ``root`` whose process died, and the damaged templates; returns
    errors."""
    try:
        names = [entry.name for entry in os.scandir(root)]
    except OSError:
        return []
    abandoned = []
    for name in names:  # damaged templates once unused, whoever moved them aside
        match = _EXTRACTION.fullmatch(name)
        if match and (
            match["damaged"] or not procfs.identity_matches(int(match["pid"]), match["start"])
        ):
            abandoned.append(root / name)
    return _remove_extractions(abandoned) if abandoned else []


def _remove_extractions(paths: list[Path]) -> list[str]:
    """Delete these extractions, but none that a fuse-overlayfs still uses; returns errors."""
    layers = _overlay_layers()
    errors = []
    for path in paths:
        real = Path(os.path.realpath(path))  # as /proc names open directories
        if any(layer == real or real in layer.parents for layer in layers):
            continue
        try:
            remove_tree(path)
        except (OSError, SandboxError) as error:
            errors.append(f"could not delete {path}: {error}")
    return errors


class SingularityService(PodService, SingularitySandbox):
    """A pod service as a long-lived ``singularity exec``; start it with ``pod.start_service``.

    The launcher's init puts the runtime in a nested user namespace of its
    own, so Singularity runs unprivileged (its non-setuid starter) in that
    namespace, which holds no capability over the pod's network, and its
    starter is never the service's PID 1. The namespace maps root alone with
    ``run_as="root"``, and root and every subordinate id of the pod with
    ``"subids"``; with ``"caller"`` it maps your uid and gid, and Singularity
    runs with ``--userns`` as them. ``--containall`` gives the container its
    own PID and IPC namespaces too; commands enter all of them.
    """

    def _userns_options(self) -> list[str]:
        if self.service.run_as == "caller":
            return ["--userns", f"caller={os.getuid()}:{os.getgid()}"]
        return ["--userns", self.service.run_as]  # root, or subids

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
            *(["--userns"] if self.service.run_as == "caller" else []),
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
    info = path.stat()  # a rewrite in place can keep the size and mtime; the ctime changes
    return f"stat-{info.st_dev}-{info.st_ino}-{info.st_size}-{info.st_mtime_ns}-{info.st_ctime_ns}"


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
