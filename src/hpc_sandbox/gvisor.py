"""Rootless sandboxes in gVisor, with ``runsc``.

gVisor runs a sandbox's processes on a kernel of its own, the Sentry, which is a
process of yours on the host. Root in a gVisor sandbox holds every capability,
but only towards that kernel: it can mount file systems, create namespaces,
switch users, trace processes and change its firewall, and none of it reaches
the host. A sandbox is built like this:

- **runsc** comes from ``binary`` or ``PATH``; hpc-sandbox never downloads it.
  Every call runs it with ``--rootless``, which has it make a user namespace
  that maps your uid alone, with ``--root`` set to a directory of the session,
  so each sandbox keeps runsc's state to itself, and with
  ``--signal-unkillable-policy=linux``: a PID namespace's init ignores the
  SIGKILL it sends itself, as Linux does.
- **Images** are SIF files, pulled and extracted by Singularity CE or Apptainer
  into the node's gVisor template of the image (``templates-gvisor/``). The
  template is the sandbox's read-only lower layer: gVisor's overlay
  (``--overlay2=root:dir=``) keeps what the sandbox writes in a file of its
  session, so a start copies nothing, and root in it can give files to any
  user. ``/tmp`` and ``/var/tmp`` are part of that overlay: gVisor would mount
  a tmpfs of its own over an empty ``/tmp``, so a template's ``/tmp`` is never
  empty.
- **Mount points.** runsc's gofer creates a bind's missing mount point in the
  root filesystem it serves, which is the template, before it makes that
  read-only. So runsc is only ever given mount points that the template has:
  a bind whose target the image has goes there; any other goes to a mount
  point of the provider's own directory, ``/.hpc-sandbox``, which each template
  gets when it is extracted, and is bound at its target from inside the
  sandbox once it runs. The host reaches the sandbox's files through one
  directory of the session, bound at ``/.hpc-sandbox/transfer``, which
  transfers go through.
- **Lifecycle.** The launcher (:mod:`hpc_sandbox.anchor`'s ``exec`` form),
  forked from a thread that lives as long as the process, arranges to be
  SIGKILLed when this process dies, then runs ``runsc run`` in the foreground,
  whose container command is a keep-alive loop. runsc's own processes, the
  gofer and the Sentry (``runsc-sandbox``), die with it, so a crashed owner
  leaves only files behind. Commands run with ``runsc exec``, which keeps no
  state of its own on the host: a timeout kills the command's process groups
  inside the sandbox too.
- **Snapshots** are the overlay's upper layer, as ``runsc tar rootfs-upper``
  writes it, with the template it lies over; a restore lays a copy of that
  archive over the same template (the ``dev.gvisor.tar.rootfs.upper``
  annotation), and keeps the owners of its files.
- **Limits.** ``memory_mb`` arms a watchdog that samples what the Sentry holds
  for the sandbox (:class:`SentryMemory`), and stops the whole sandbox when it
  exceeds the limit: the sandbox's processes are not host processes, so no
  single one can be picked. ``cpus`` becomes the CPUs the launcher may run on,
  chosen among those your other gVisor sandboxes on the node use least, which
  gVisor reports to the sandbox as its CPUs. ``overlay_size_mb`` caps what a
  sandbox writes to its root filesystem, not to its transfer directory or its
  read-write mounts. Rootless runsc applies no cgroup limits.

Host requirements: runsc (with its ``gvisor-bin/`` beside it, for releases that
ship one), unprivileged user namespaces, and Singularity CE or Apptainer for
images.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import math
import os
import queue
import random
import re
import resource
import shutil
import socket
import stat
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Coroutine, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any, Literal, NoReturn

from . import memory, procfs
from .anchor import cpu_list
from .images import ImageStore, ResolvedImage, SifImages
from .locks import locked
from .paths import default_image_cache, should_stage
from .process import DEFAULT_OUTPUT_LIMIT, ProcessResult, run_process
from .runtime import ContainerRuntime
from .sandbox import TRANSFER_DIR, SessionProvider, SessionSandbox, poll
from .sessions import Session, kill_exact, remove_tree
from .templates import (
    extract_image,
    hold_template,
    remove_abandoned_extractions,
    rootfs_template,
    template_ready,
)
from .types import SandboxError, SandboxSnapshot, SandboxSpec, SandboxStartError

log = logging.getLogger(__name__)

ANCHOR_SCRIPT = Path(__file__).with_name("anchor.py")
#: A snapshot of a gVisor sandbox: the overlay's upper layer (``rootfs-upper.tar``), and the
#: template it lies over (``lower``).
SNAPSHOT_FORMAT = "hpc-sandbox/gvisor-overlay-v1"
PLATFORMS = ("systrap", "ptrace", "kvm")
HOST_UDS = ("open", "create", "all")
GUEST_CGROUPS = ("v1", "v2")
#: The gVisor provider's templates, on the scratch root: its own, since each holds its mount
#: points (:data:`PROVIDER_DIR`).
TEMPLATE_DIR = "templates-gvisor"
#: The provider's directory in each template's root filesystem, which the image's own is replaced
#: with: the transfer directory's mount point (``transfer/``), and mount points for the binds whose
#: targets the image lacks (``mounts/d<N>`` for directories, ``mounts/f<N>`` for files).
PROVIDER_DIR = ".hpc-sandbox"
TRANSFER_TARGET = f"/{PROVIDER_DIR}/transfer"
MOUNT_POINTS = f"/{PROVIDER_DIR}/mounts"
#: An empty file in each template's ``/tmp`` that the image left empty, so that gVisor keeps
#: ``/tmp`` in its overlay.
KEEP_TMP = ".hpc-sandbox"
#: On the scratch root: held while a start chooses its CPUs and starts its launcher.
CPU_LOCK = "cpus.lock"
# The session's files, besides the shared ones.
BUNDLE = "config.json"  # the OCI bundle's spec; the session directory is the bundle
STATE_DIR = "runsc"  # runsc's --root
OVERLAY_DIR = "overlay"  # where gVisor keeps the file that backs the overlay's upper layer
EXEC_DIR = "exec"  # the sandbox-side PIDs of running commands
ROOTFS_RECORD = "rootfs.json"  # the lower layer, and the upper layer's archive of a restore
UPPER_ARCHIVE = "rootfs-upper.tar"
RUNSC_LOG = "runsc.log"
UPPER_ANNOTATION = "dev.gvisor.tar.rootfs.upper"
READY = "hpc-sandbox-ready"
# The container's command, PID 1 of the sandbox: tasks cannot kill it (SIGKILL from inside a PID
# namespace never reaches its init, with --signal-unkillable-policy=linux), it reaps orphans, and
# its end ends the sandbox.
KEEPALIVE = f"""\
trap '' HUP INT QUIT TERM USR1 USR2
command -v sleep >/dev/null 2>&1 || {{ echo "hpc-sandbox: the image has no sleep" >&2; exit 127; }}
echo {READY}
exec </dev/null >/dev/null 2>&1
while :; do sleep 86400 & wait $!; done
"""
# Run as root in a new sandbox, with KIND POINT TARGET MODE for each bind that runsc made at a
# mount point of the provider's: bind it at its target, made in the sandbox's overlay as Docker
# makes a missing mount point. The mount point stays read-only on the host whatever the sandbox
# does; the remount only makes the target say so too.
BIND_SCRIPT = """\
set -e
while [ "$#" -ge 4 ]; do
    if [ "$1" = d ]; then
        mkdir -p "$3"
    else
        parent=${3%/*}
        mkdir -p "${parent:-/}"
        [ -e "$3" ] || : > "$3"
    fi
    mount -o bind "$2" "$3"
    if [ "$4" = ro ]; then mount -o remount,bind,ro "$3" 2>/dev/null || :; fi
    shift 4
done
"""
# Every capability gVisor knows: root in a sandbox holds them all, towards gVisor's kernel.
CAPABILITIES = tuple(
    f"CAP_{name}"
    for name in (
        "CHOWN DAC_OVERRIDE DAC_READ_SEARCH FOWNER FSETID KILL SETGID SETUID SETPCAP "
        "LINUX_IMMUTABLE NET_BIND_SERVICE NET_BROADCAST NET_ADMIN NET_RAW IPC_LOCK IPC_OWNER "
        "SYS_MODULE SYS_RAWIO SYS_CHROOT SYS_PTRACE SYS_PACCT SYS_ADMIN SYS_BOOT SYS_NICE "
        "SYS_RESOURCE SYS_TIME SYS_TTY_CONFIG MKNOD LEASE AUDIT_WRITE AUDIT_CONTROL SETFCAP "
        "MAC_OVERRIDE MAC_ADMIN SYSLOG WAKE_ALARM BLOCK_SUSPEND AUDIT_READ PERFMON BPF "
        "CHECKPOINT_RESTORE"
    ).split()
)
# The environment of runsc on the host: enough to run it, nothing a sandbox could have chosen.
_HOST_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
# Run in the sandbox once it is up: the image's environment, as Singularity builds it, read back
# from the process that sourced it. A script that fails or exits leaves the default PATH.
_IMAGE_ENVIRONMENT = (
    'for hpc_sandbox_script in /.singularity.d/env/*.sh; do if [ -f "$hpc_sandbox_script" ]; '
    'then . "$hpc_sandbox_script"; fi; done >/dev/null 2>&1 </dev/null; '
    "exec cat /proc/self/environ"
)
_SHELL_STATE = ("PWD", "OLDPWD", "SHLVL", "_", "hpc_sandbox_script")
_SANDBOX_ID = re.compile(r"hpcsb-[0-9a-f]{32}")
# runsc ps's table: UID PID PPID PGID C TTY STIME TIME CMD. A command's name holds at most 15
# bytes, so a line break in one cannot make a row of that many fields.
_PS_FIELDS = 9
_QUERY_TIMEOUT_S = 30.0
_KILL_WAIT_S = 5.0
_KILL_CONCURRENCY = 16  # runsc kill processes at once, on a timeout
_NOFILE_CAP = 1 << 20  # what an unlimited RLIMIT_NOFILE becomes in the sandbox (Docker's default)


class RunscRuntime:
    """The ``runsc`` command line, run with a fixed environment.

    ``binary`` is a name on ``PATH`` or a path, ``runsc`` by default. It is looked up when first
    needed, never downloaded; releases that ship ``gvisor.tar`` keep their helpers in a
    ``gvisor-bin/`` directory, which must stay beside it.
    """

    def __init__(self, binary: str | None = None) -> None:
        self.binary = binary
        self._path: str | None = None

    @property
    def path(self) -> str:
        if self._path is None:
            binary = self.binary or "runsc"
            resolved = shutil.which(binary)
            if resolved is None:
                where = "an executable file" if os.sep in binary else "an executable on PATH"
                raise SandboxError(f"runsc {binary!r} is not {where}")
            self._path = resolved
        return self._path

    @property
    def helpers(self) -> Path:
        """Where the release's helper binaries (``gvisor_sentry`` and others) must be."""
        return Path(self.path).resolve().parent / "gvisor-bin"

    @staticmethod
    def environment() -> dict[str, str]:
        return {"PATH": _HOST_PATH}

    async def run(
        self,
        *args: str,
        timeout_s: float,
        stdin: bytes | None = None,
        output_limit: int = DEFAULT_OUTPUT_LIMIT,
        on_start: Callable[[int], None] | None = None,
    ) -> ProcessResult:
        try:
            return await run_process(
                [self.path, *args],
                timeout_s=timeout_s,
                stdin=stdin,
                env=self.environment(),
                output_limit=output_limit,
                on_start=on_start,
            )
        except OSError as error:
            raise SandboxError(f"could not run {self.path}: {error}") from error

    async def version(self) -> str:
        result = await self.run("--version", timeout_s=_QUERY_TIMEOUT_S)
        if result.returncode != 0:
            raise SandboxError(f"{self.path} --version failed: {result.detail()}")
        return result.stdout.decode(errors="replace").strip().splitlines()[0]


class GvisorProvider(SessionProvider["GvisorSandbox"]):
    """Start, restore and reap sandboxes in gVisor, with rootless ``runsc``.

    ``binary`` is runsc, a name on ``PATH`` or a path; ``singularity_binary`` is the Singularity
    CE or Apptainer that pulls SIF images and extracts them into the node's templates, found as
    :class:`~hpc_sandbox.runtime.ContainerRuntime` finds it. ``platform`` is gVisor's way of
    running the sandbox's code: ``"systrap"`` (the default), ``"ptrace"``, or ``"kvm"`` where
    ``/dev/kvm`` can be opened. ``host_uds`` lets the sandbox use the Unix sockets of the host
    that its mounts hold: ``"open"`` (connect to them), ``"create"`` (make ones the host reaches)
    or ``"all"``. ``net_raw`` allows raw and packet sockets (``ping``, packet capture).
    ``guest_cgroups`` (``"v1"`` or ``"v2"``) mounts gVisor's own cgroups at ``/sys/fs/cgroup``,
    for software that manages cgroups itself; they limit nothing on the host.
    ``overlay_size_mb`` caps what each sandbox writes to its root filesystem, ``/tmp`` included,
    but not to its transfer directory or read-write mounts, which are the host's directories;
    ``None`` leaves it to the scratch filesystem. ``extra_start_args`` are runsc options added to
    each ``runsc run``.

    Sandboxes die with the process that started them. Pods are not supported yet.
    """

    name = "gvisor"
    sessions_directory = "sessions-gvisor"
    snapshot_formats = (SNAPSHOT_FORMAT,)

    def __init__(
        self,
        *,
        binary: str | None = None,
        singularity_binary: str | None = None,
        scratch_root: str | Path | None = None,
        image_cache: str | Path | None = None,
        stage_images: bool | None = None,
        default_timeout_s: float = 600.0,
        start_timeout_s: float = 1800.0,
        stop_timeout_s: float = 60.0,
        platform: Literal["systrap", "ptrace", "kvm"] = "systrap",
        host_uds: Literal["open", "create", "all"] | None = None,
        net_raw: bool = False,
        guest_cgroups: Literal["v1", "v2"] | None = None,
        overlay_size_mb: int | None = None,
        bind_resolv_conf: bool = True,
        extra_start_args: Sequence[str] = (),
        output_limit_bytes: int = DEFAULT_OUTPUT_LIMIT,
        memory_poll_s: float = 1.0,
    ) -> None:
        if platform not in PLATFORMS:
            raise ValueError(f"platform must be one of {PLATFORMS}, not {platform!r}")
        if host_uds is not None and host_uds not in HOST_UDS:
            raise ValueError(f"host_uds must be None or one of {HOST_UDS}, not {host_uds!r}")
        if guest_cgroups is not None and guest_cgroups not in GUEST_CGROUPS:
            raise ValueError(
                f"guest_cgroups must be None or one of {GUEST_CGROUPS}, not {guest_cgroups!r}"
            )
        if overlay_size_mb is not None and (
            isinstance(overlay_size_mb, bool)
            or not isinstance(overlay_size_mb, int)
            or overlay_size_mb <= 0
        ):
            raise ValueError(f"overlay_size_mb must be a positive integer, not {overlay_size_mb!r}")
        super().__init__(
            scratch_root=scratch_root,
            default_timeout_s=default_timeout_s,
            start_timeout_s=start_timeout_s,
            stop_timeout_s=stop_timeout_s,
            output_limit_bytes=output_limit_bytes,
            memory_poll_s=memory_poll_s,
        )
        self.sandbox_type = GvisorSandbox
        self.runtime = RunscRuntime(binary)
        self.image_runtime = ContainerRuntime(
            singularity_binary,
            tmp_dir=self.scratch_root / "runtime-tmp",
            cache_dir=self.scratch_root / "runtime-cache",
        )
        cache = Path(image_cache) if image_cache else default_image_cache()
        if stage_images is None:
            stage_images = should_stage(cache, self.scratch_root)
        self.images = ImageStore(
            SifImages(self.image_runtime, provider="gvisor"),
            cache,
            stage_dir=self.scratch_root / "images" if stage_images else None,
            pull_timeout_s=start_timeout_s,
            prepare=self._make_scratch_directories,
        )
        self.platform = platform
        self.host_uds = host_uds
        self.net_raw = net_raw
        self.guest_cgroups = guest_cgroups
        self.overlay_size_mb = overlay_size_mb
        self.bind_resolv_conf = bind_resolv_conf
        self.extra_start_args = tuple(extra_start_args)
        self.capabilities = frozenset({"snapshot", "isolation", "network_none"})

    @property
    def template_root(self) -> Path:
        """Where this provider's templates are: ``templates-gvisor/`` on the scratch root."""
        return self.scratch_root / TEMPLATE_DIR

    async def start(self, spec: SandboxSpec) -> GvisorSandbox:
        self._check(spec)
        self._find_runsc()  # before an image is pulled or extracted
        return await super().start(spec)

    def _check(self, spec: SandboxSpec, *, service: bool = False) -> None:
        super()._check(spec, service=service)
        reserved = PurePosixPath("/", PROVIDER_DIR)
        for mount in spec.mounts:
            target = PurePosixPath(mount.target)
            if target == reserved or reserved in target.parents:
                raise ValueError(f"mount target {mount.target!r} is reserved for hpc-sandbox")
        if any(c in str(self.scratch_root) for c in ",:"):  # --overlay2 and --root misread them
            raise ValueError(
                f"the gvisor provider's scratch root cannot contain ',' or ':': {self.scratch_root}"
            )

    def _find_runsc(self) -> None:
        try:
            self.runtime.path  # noqa: B018 - looks runsc up
        except SandboxError as error:
            raise SandboxStartError(str(error)) from None

    def _scratch_directories(self) -> list[Path]:
        runtime = self.image_runtime
        assert runtime.tmp_dir is not None and runtime.cache_dir is not None
        return [*super()._scratch_directories(), runtime.tmp_dir, runtime.cache_dir]

    def _file_owners_refusal(self) -> str | None:
        return (
            "the gvisor provider does not apply file owners manifests yet: root in a gVisor "
            "sandbox can give its files to other users itself"
        )

    async def _extract(self, image: ResolvedImage, session: Session) -> None:
        """The node's gVisor template of ``image`` becomes the sandbox's lower layer: nothing is
        copied."""
        lower = await rootfs_template(image, self.template_root, self._extract_into)
        _write_record(session, lower, None)

    async def _extract_into(self, image: ResolvedImage, rootfs: Path) -> None:
        await extract_image(self.image_runtime, image, rootfs, timeout_s=self.start_timeout_s)
        await asyncio.to_thread(prepare_template, rootfs)

    async def restore(self, snapshot: SandboxSnapshot, spec: SandboxSpec) -> GvisorSandbox:
        """Start a sandbox for ``spec`` whose overlay starts from the snapshot's upper layer, over
        the template that the snapshot's sandbox ran on."""
        self._check(spec)
        source = self._snapshot_dir(snapshot)
        if snapshot.image != spec.image:
            raise ValueError(f"snapshot {snapshot.id} is of {snapshot.image!r}, not {spec.image!r}")
        lower = self._snapshot_lower(snapshot)
        await self._check_file_owners(spec)
        self._find_runsc()
        await self._prepare()
        return await self._launch(spec, partial(self._restore_upper, source, lower), restored=True)

    def _snapshot_lower(self, snapshot: SandboxSnapshot) -> Path:
        """The template a snapshot's upper layer lies over: one of this node's complete
        templates, or ValueError."""
        lower = Path(str(snapshot.metadata.get("lower", "")))
        templates = self.template_root
        if (
            lower.name != "rootfs"
            or lower.parent.parent != templates
            or not template_ready(lower.parent)
        ):
            raise ValueError(
                f"snapshot {snapshot.id} lies over {str(lower)!r}, which is not a template of this "
                f"node (in {templates})"
            )
        return lower

    async def _restore_upper(self, source: Path, lower: Path, session: Session) -> None:
        upper = session.path / UPPER_ARCHIVE
        await self.copy_tree(source / UPPER_ARCHIVE, upper)
        _write_record(session, lower, upper)

    async def _sweep_node(self) -> list[str]:
        """Delete the template extractions that dead processes left."""
        return await asyncio.to_thread(remove_abandoned_extractions, self.template_root)

    async def _stop_abandoned(self, path: Path, record: dict[str, Any] | None) -> bool:
        """Kill what still runs of an abandoned sandbox, which usually died with its owner: the
        launcher and the Sentry that its owner record names, then any process of yours whose
        arguments name the session's own state directory."""
        processes = []
        if record is not None:
            for role in ("sentry", "launcher"):  # the Sentry first: its end stops the sandbox
                pid, started = record.get(f"{role}_pid"), record.get(f"{role}_start_time")
                if isinstance(pid, int) and isinstance(started, str):
                    processes.append((pid, started))
        stopped = await asyncio.to_thread(kill_exact, processes, path.name)
        survivors = await asyncio.to_thread(runsc_processes, path / STATE_DIR)
        if survivors:
            stopped |= await asyncio.to_thread(kill_exact, survivors, path.name)
        return stopped

    def _choose_cpus(self, cpus: float) -> list[int] | None:
        """The CPUs a sandbox with ``cpus`` runs on: as many as ``cpus`` rounded up, of those this
        process may use, picking the ones your gVisor sandboxes on this node use least
        (:func:`cpus_in_use`) and, among those, from a random one on; None for all of them."""
        allowed = sorted(os.sched_getaffinity(0))
        count = math.ceil(cpus)
        if count >= len(allowed):
            return None
        load = cpus_in_use()
        offset = random.randrange(len(allowed))  # noqa: S311 - spreads ties, no secret
        order = allowed[offset:] + allowed[:offset]
        return sorted(sorted(order, key=lambda cpu: load[cpu])[:count])


@dataclass(frozen=True)
class Bind:
    """A host path bound into a sandbox: at ``target``, or, when the template lacks ``target``,
    first at the provider's mount point ``point`` and from there at ``target`` inside the sandbox
    (:func:`plan_binds`)."""

    source: Path
    target: str
    read_only: bool
    point: str | None = None

    @property
    def directory(self) -> bool:
        return self.source.is_dir()


class GvisorSandbox(SessionSandbox):
    """One running gVisor sandbox; create it with :meth:`GvisorProvider.start`."""

    _transfer_target = TRANSFER_TARGET

    def __init__(self, provider: GvisorProvider, spec: SandboxSpec) -> None:
        super().__init__(provider, spec)
        self._provider: GvisorProvider = provider
        self._launcher: subprocess.Popen[bytes] | None = None
        self._launcher_start_time: str | None = None
        self._sentry_pid: int | None = None
        self._sentry_start_time: str | None = None
        self._environment: dict[str, str] = {}
        self._cpus: list[int] | None = None
        self._lower: Path | None = None
        self._upper: Path | None = None
        self._binds: list[Bind] = []
        self._template_lock: int | None = None
        self._memory_kill: memory.MemoryKill | None = None

    # Paths of the session.

    @property
    def _state(self) -> Path:
        assert self._session is not None
        return self._session.path / STATE_DIR

    @property
    def _transfer_source(self) -> Path:
        assert self._session is not None
        return self._session.tmp / TRANSFER_DIR

    # What the session's owner record and snapshots say.

    def _identity(self) -> dict[str, Any]:
        return {
            "launcher_pid": self._launcher.pid if self._launcher else None,
            "launcher_start_time": self._launcher_start_time,
            "sentry_pid": self._sentry_pid,
            "sentry_start_time": self._sentry_start_time,
        }

    def _user(self) -> dict[str, Any]:
        # Root of gVisor's kernel; every file the sandbox writes on the host is yours.
        return {"remap_root": True, "container_uid": 0}

    def _workdir_umask(self) -> str:
        # The other users of the sandbox must be able to enter a working directory that the start
        # makes, as Docker's -w makes one: mode 755 whatever the caller's umask.
        return "022"

    def _full_environment(self, env: Mapping[str, str]) -> dict[str, str]:
        return {**self._environment, **env}

    # The runsc command line.

    def _global_flags(self) -> list[str]:
        """runsc's options before each subcommand for this sandbox."""
        assert self._session is not None
        provider = self._provider
        overlay = f"root:dir={self._session.path / OVERLAY_DIR}"
        if provider.overlay_size_mb is not None:
            overlay += f",size={provider.overlay_size_mb}m"
        flags = [
            "--rootless",
            f"--root={self._state}",
            f"--platform={provider.platform}",
            f"--network={'none' if self.spec.network == 'none' else 'host'}",
            "--signal-unkillable-policy=linux",
            f"--overlay2={overlay}",
            "--allow-suid",
        ]
        if provider.host_uds:
            flags.append(f"--host-uds={provider.host_uds}")
        if provider.net_raw:
            flags += ["--net-raw", "--allow-packet-socket-write"]
        if provider.guest_cgroups == "v2":
            flags.append("--in-sandbox-cgroup=v2")
        if self._upper is not None:
            flags.append("--allow-rootfs-tar-annotation")
        return flags

    def _bundle(self) -> dict[str, Any]:
        """The OCI spec of the sandbox's container (``config.json``)."""
        assert self._session is not None and self._lower is not None
        provider = self._provider
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limits = [
            _NOFILE_CAP if value == resource.RLIM_INFINITY else value for value in (soft, hard)
        ]
        mounts: list[dict[str, Any]] = [
            {"destination": "/proc", "type": "proc", "source": "proc"},
            {"destination": "/dev", "type": "tmpfs", "source": "tmpfs"},
            {
                "destination": "/sys",
                "type": "sysfs",
                "source": "sysfs",
                "options": ["nosuid", "noexec", "nodev"],
            },
            _bind(self._transfer_source, TRANSFER_TARGET, read_only=False),
        ]
        for bind in self._binds:
            mounts.append(_bind(bind.source, bind.point or bind.target, bind.read_only))
        if provider.guest_cgroups:
            mounts.append(
                {
                    "destination": "/sys/fs/cgroup",
                    "type": "cgroup",
                    "source": "cgroup",
                    "options": ["nosuid", "noexec", "nodev"],
                }
            )
        namespaces = [{"type": kind} for kind in ("pid", "ipc", "uts", "mount")]
        if self.spec.network == "none":
            namespaces.append({"type": "network"})
        spec: dict[str, Any] = {
            "ociVersion": "1.0.2",
            "process": {
                "terminal": False,
                "user": {"uid": 0, "gid": 0},
                "args": ["/bin/sh", "-c", KEEPALIVE, self.id],
                "env": [f"PATH={_HOST_PATH}"],
                "cwd": "/",
                "capabilities": {
                    kind: list(CAPABILITIES)
                    for kind in ("bounding", "effective", "inheritable", "permitted", "ambient")
                },
                "rlimits": [{"type": "RLIMIT_NOFILE", "soft": limits[0], "hard": limits[1]}],
            },
            "root": {"path": str(self._lower), "readonly": False},
            "hostname": socket.gethostname(),
            "mounts": mounts,
            "linux": {"namespaces": namespaces},
        }
        if self._upper is not None:
            spec["annotations"] = {UPPER_ANNOTATION: str(self._upper)}
        return spec

    # The lifecycle.

    def _prepare_rootfs(self) -> None:
        """Write the bundle and make the session's directories, and the template's mount points
        that the sandbox's binds need."""
        assert self._session is not None
        session = self._session
        record = json.loads((session.path / ROOTFS_RECORD).read_text())
        self._lower = Path(record["lower"])
        self._upper = Path(record["upper"]) if record.get("upper") else None
        if any(c in str(session.path) for c in ",:"):  # --overlay2 and --root would misread it
            raise SandboxStartError(f"runsc cannot take a session path with ',' or ':': {session}")
        for directory in (STATE_DIR, OVERLAY_DIR, EXEC_DIR):
            (session.path / directory).mkdir(mode=0o700)
        session.tmp.mkdir(mode=0o700)
        session.var_tmp.mkdir(mode=0o700)
        self._transfer_source.mkdir(mode=0o700)
        binds: list[tuple[Path, str, bool]] = []
        resolv_conf = Path("/etc/resolv.conf")
        if (
            self.spec.network == "host"
            and self._provider.bind_resolv_conf
            and resolv_conf.is_file()
        ):
            binds.append((resolv_conf.resolve(), "/etc/resolv.conf", True))
        for mount in self.spec.mounts:
            binds.append((Path(mount.source).resolve(), mount.target, mount.read_only))
        self._binds = plan_binds(self._lower, binds)
        pooled = [bind for bind in self._binds if bind.point is not None]
        make_mount_points(
            self._lower,
            directories=sum(bind.directory for bind in pooled),
            files=sum(not bind.directory for bind in pooled),
        )
        bundle = session.path / BUNDLE
        bundle.write_text(json.dumps(self._bundle(), indent=1) + "\n")
        bundle.chmod(0o600)

    async def _start_container(self) -> None:
        assert self._session is not None and self._lower is not None
        provider = self._provider
        self._template_lock = await hold_template(self._lower)
        if self.spec.cpus is None:
            await self._start_launcher()
        else:
            # One start at a time chooses CPUs, until its launcher, which names them, runs.
            async with locked(provider.scratch_root / CPU_LOCK):
                self._cpus = await asyncio.to_thread(provider._choose_cpus, self.spec.cpus)
                await self._start_launcher()
        launcher = self._launcher
        assert launcher is not None
        self._launcher_start_time = procfs.start_time(launcher.pid)
        self._record()
        log_path = self._session.path / RUNSC_LOG
        deadline = time.monotonic() + provider.start_timeout_s
        while READY not in _read_lines(log_path):
            if launcher.poll() is not None:
                raise SandboxStartError(
                    f"runsc exited during startup (status {launcher.returncode}): {_tail(log_path)}"
                )
            if time.monotonic() > deadline:
                raise SandboxStartError(
                    f"the sandbox did not start within {provider.start_timeout_s:g} s: "
                    f"{_tail(log_path)}"
                )
            await asyncio.sleep(0.02)
        sentry = await poll(self._find_sentry, _KILL_WAIT_S)
        if sentry is None:
            raise SandboxStartError("could not find the sandbox's Sentry (runsc-sandbox)")
        self._sentry_pid, self._sentry_start_time = sentry
        self._record()
        await self._bind_at_targets()
        self._environment = await self._image_environment()

    async def _start_launcher(self) -> None:
        """Start the launcher, from the thread that starts every launcher (:class:`_Spawner`)."""
        assert self._session is not None
        provider = self._provider
        cpus = ["--cpus", ",".join(map(str, self._cpus))] if self._cpus else []
        command = [
            sys.executable,
            "-I",
            "-S",
            str(ANCHOR_SCRIPT),
            str(os.getpid()),
            "exec",
            *cpus,
            "--",
            provider.runtime.path,
            *self._global_flags(),
            *provider.extra_start_args,
            "run",
            "--bundle",
            str(self._session.path),
            self.id,
        ]
        with (self._session.path / RUNSC_LOG).open("wb") as output:
            started = _SPAWNER.submit(
                partial(
                    subprocess.Popen,
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    env=provider.runtime.environment(),
                    start_new_session=True,
                )
            )
            try:
                self._launcher = await asyncio.shield(asyncio.wrap_future(started))
            except asyncio.CancelledError:
                with suppress(Exception):  # it is forked by now, or in an instant: stop() kills it
                    self._launcher = started.result()
                raise

    def _find_sentry(self) -> tuple[int, str] | None:
        """The Sentry of this sandbox, among the launcher's descendants, with its start time."""
        assert self._launcher is not None
        for pid in procfs.descendants([self._launcher.pid]):
            arguments = procfs.argv(pid)
            if (
                arguments
                and os.path.basename(arguments[0]) == "runsc-sandbox"
                and self.id in arguments[-1:]
                and (started := procfs.owned_live_start_time(pid)) is not None
            ):
                return pid, started
        return None

    async def _bind_at_targets(self) -> None:
        """Bind, inside the sandbox, what runsc bound at the provider's mount points at its
        targets."""
        arguments = []
        for bind in self._binds:
            if bind.point is not None:
                kind = "d" if bind.directory else "f"
                arguments += [kind, bind.point, bind.target, "ro" if bind.read_only else "rw"]
        if not arguments:
            return
        result = await self._spawn(
            ["/bin/sh", "-c", BIND_SCRIPT, "hpc-sandbox", *arguments],
            stdin=None,
            timeout_s=_QUERY_TIMEOUT_S,
            on_start=lambda _pid: None,
        )
        if result.returncode != 0 or result.timed_out:
            targets = ", ".join(arguments[2::4])
            detail = (result.stderr + result.stdout).decode(errors="replace").strip()
            raise SandboxStartError(
                f"could not bind {targets} in the sandbox, which the image lacks: binding them "
                f"needs mkdir and mount in the image: {detail or f'exit {result.returncode}'}"
            )

    async def _image_environment(self) -> dict[str, str]:
        """The variables the image's environment scripts set, which every command gets."""
        result = await self._spawn(
            ["/bin/sh", "-c", _IMAGE_ENVIRONMENT],
            stdin=None,
            timeout_s=_QUERY_TIMEOUT_S,
            on_start=lambda _pid: None,
        )
        environment = {}
        for item in result.stdout.split(b"\0"):
            key, separator, value = item.decode(errors="surrogateescape").partition("=")
            if separator and key and key not in _SHELL_STATE:
                environment[key] = value
        if result.returncode != 0 or "PATH" not in environment:
            log.warning(
                "sandbox %s: could not read the image's environment (exit %s): %s",
                self.id,
                result.returncode,
                result.stderr.decode(errors="replace").strip() or "no output",
            )
            environment.setdefault("PATH", _HOST_PATH)
        return environment

    async def _spawn(
        self,
        command: list[str],
        *,
        stdin: bytes | None,
        timeout_s: float,
        on_start: Callable[[int], None],
    ) -> ProcessResult:
        """``runsc exec`` of ``command``; on a timeout or a cancellation, the command's process
        groups in the sandbox are killed too, since killing ``runsc exec`` leaves them running."""
        assert self._session is not None
        if not self._container_alive():
            self._mark_lost("the sandbox's Sentry is gone")
        pid_file = self._session.path / EXEC_DIR / uuid.uuid4().hex
        argv = [
            self._provider.runtime.path,
            *self._global_flags(),
            "exec",
            "--cwd",
            "/",
            "--internal-pid-file",
            str(pid_file),
            self.id,
            *command,
        ]
        try:
            try:
                result = await run_process(
                    argv,
                    timeout_s=timeout_s,
                    stdin=stdin,
                    env=self._provider.runtime.environment(),
                    output_limit=self._output_limit,
                    on_start=on_start,
                )
            except BaseException:
                await asyncio.shield(self._kill_command(pid_file))
                raise
            if result.timed_out:
                await self._kill_command(pid_file)
        finally:
            pid_file.unlink(missing_ok=True)
        if not self._container_alive() and self._stop_task is None and not self._stopping:
            # runsc exec may report success for a command whose sandbox died under it.
            self._mark_lost(result.stderr.decode(errors="replace"))
        return result

    async def _kill_command(self, pid_file: Path) -> None:
        """SIGKILL, inside the sandbox, the process groups of the command whose sandbox-side PID
        ``pid_file`` holds and of everything below it, several at once."""
        try:
            pid = int(pid_file.read_text().strip())
        except (OSError, ValueError):
            return  # it never started
        if not self._container_alive():
            return
        runtime = self._provider.runtime
        try:
            listing = await runtime.run(
                *self._global_flags(), "ps", self.id, timeout_s=_QUERY_TIMEOUT_S
            )
        except SandboxError as error:  # the sandbox's own end, a stop, takes care of the rest
            log.warning("sandbox %s: could not list its processes: %s", self.id, error)
            return
        groups = command_groups(listing.stdout.decode(errors="replace"), pid) or [pid]
        slots = asyncio.Semaphore(_KILL_CONCURRENCY)

        async def kill(group: int) -> None:
            async with slots:
                await runtime.run(
                    *self._global_flags(),
                    "kill",
                    f"--pgid={group}",
                    self.id,
                    "KILL",
                    timeout_s=_QUERY_TIMEOUT_S,
                )

        outcomes = await asyncio.gather(*map(kill, groups), return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                log.warning("sandbox %s: could not kill a command in it: %s", self.id, outcome)

    def _container_alive(self) -> bool:
        """Whether the Sentry runs, and is not on its way out: a command whose Sentry was killed
        can end while the kernel still tears the Sentry down."""
        return (
            self._sentry_pid is not None
            and self._memory_kill is None
            and procfs.owned_live_start_time(self._sentry_pid) == self._sentry_start_time
            and not procfs.exiting(self._sentry_pid)
        )

    def _mark_lost(self, detail: str) -> NoReturn:
        if self._memory_kill is not None:
            detail = f"{self._memory_kill.message()}; the whole sandbox was stopped"
        super()._mark_lost(detail)

    async def _stop_container(self, session: Session) -> None:
        """SIGKILL the launcher, which runsc's own processes die with; wait for them, the
        recorded Sentry whatever its arguments say, and kill any process that still names the
        session's state directory. Nothing is left for ``runsc delete``, whose state is in the
        session."""
        launcher = self._launcher
        if launcher is not None:
            provider = self._provider
            if launcher.poll() is None:
                launcher.kill()
            if await poll(launcher.poll, provider.stop_timeout_s) is None:
                raise SandboxError(f"the launcher of sandbox {self.id} survived SIGKILL")
            sentry, started = self._sentry_pid, self._sentry_start_time
            recorded = [(sentry, started)] if sentry is not None and started is not None else []
            survivors = await asyncio.to_thread(
                _outlived, session.path / STATE_DIR, recorded, _KILL_WAIT_S, provider.stop_timeout_s
            )
            if running := [process for process in survivors if not procfs.exiting(process[0])]:
                await asyncio.to_thread(kill_exact, running, self.id)
            if sentry is not None and procfs.owned_live_start_time(sentry) == started:
                raise SandboxError(f"the Sentry of sandbox {self.id} survived its launcher")
            self._launcher = None
        self._cpus = None
        if self._template_lock is not None:
            os.close(self._template_lock)
            self._template_lock = None

    async def _snapshot_rootfs(self, target: Path) -> tuple[str, dict[str, Any]]:
        """The overlay's upper layer, as ``runsc tar rootfs-upper`` writes it, and the template
        it lies over."""
        assert self._lower is not None
        archive = target / UPPER_ARCHIVE
        result = await self._provider.runtime.run(
            *self._global_flags(),
            "tar",
            "rootfs-upper",
            f"--file={archive}",
            self.id,
            timeout_s=self._provider.start_timeout_s,
        )
        if result.returncode != 0 or result.timed_out or not archive.is_file():
            raise SandboxError(f"could not archive the sandbox's files: {result.detail()}")
        return SNAPSHOT_FORMAT, {"lower": str(self._lower)}

    # The memory limit.

    def _memory_watchdog(self, limit_bytes: int) -> Coroutine[Any, Any, None]:
        return self._watch_memory(limit_bytes)

    async def _watch_memory(self, limit_bytes: int) -> None:
        """Sample what the Sentry holds for the sandbox, and stop the whole sandbox when it
        exceeds ``limit_bytes``."""
        sentry, started = self._sentry_pid, self._sentry_start_time
        if sentry is None or started is None:
            return
        held = SentryMemory(sentry, started)
        while self._memory_kill is None:
            try:
                await self._check_memory(held, limit_bytes)
            except Exception:  # the next sample may well work
                log.exception("memory watchdog")
            await asyncio.sleep(self._provider.memory_poll_s)

    async def _check_memory(self, held: SentryMemory, limit_bytes: int) -> None:
        total = await asyncio.to_thread(held.read)
        if total is None or total <= limit_bytes or not self._container_alive():
            return
        kill = memory.MemoryKill(
            pid=held.pid,
            pss_bytes=total,
            total_bytes=total,
            limit_bytes=limit_bytes,
            exec_root=None,
            pids=(held.pid,),
            measure="gVisor's memory",
        )
        self._memory_kill = kill  # before the signal: commands that fail find it
        self._on_memory_kill(kill)
        await asyncio.to_thread(kill_exact, [(held.pid, held.started)], self.id)


class SentryMemory:
    """What a Sentry holds of the host's memory for its sandbox: the allocated size of the memory
    files it holds open (memfds such as ``runsc-memory``, where gVisor keeps the memory of the
    sandbox's processes and its tmpfs files), each counted once, and its own anonymous memory.
    The host's files that the sandbox reads or maps are not in them.

    Unlike the PSS of the Sentry's processes, it counts what no host process maps (gVisor unmaps
    an idle process's memory from the stub that runs it) and never counts a page twice (a stub
    and its threads map the same memory). The descriptors are listed again every so often, and
    whenever one changed.
    """

    _RESCAN = 30  # samples between listings of the Sentry's descriptors

    def __init__(self, pid: int, started: str) -> None:
        self.pid = pid
        self.started = started
        self._files: dict[str, tuple[int, int]] = {}  # descriptor -> the file's (device, inode)
        self._samples = 0

    def read(self) -> int | None:
        """The bytes it holds, or None once the Sentry is gone."""
        if self._samples % self._RESCAN == 0:
            self._files = self._memory_files()
        self._samples += 1
        total = self._allocated()
        if total is None:
            self._files = self._memory_files()
            total = self._allocated() or 0
        anonymous = _rss_anon(self.pid)
        if anonymous is None or procfs.owned_live_start_time(self.pid) != self.started:
            return None
        return total + anonymous

    def _allocated(self) -> int | None:
        """The allocated bytes of the files listed last, None if one is no longer there."""
        total = 0
        for descriptor, identity in self._files.items():
            try:
                info = os.stat(f"/proc/{self.pid}/fd/{descriptor}")
            except OSError:
                return None
            if (info.st_dev, info.st_ino) != identity:
                return None
            total += info.st_blocks * 512
        return total

    def _memory_files(self) -> dict[str, tuple[int, int]]:
        found: dict[str, tuple[int, int]] = {}
        try:
            entries = list(os.scandir(f"/proc/{self.pid}/fd"))
        except OSError:
            return found
        for entry in entries:
            try:
                if not os.readlink(entry.path).startswith("/memfd:"):
                    continue
                info = os.stat(entry.path)
            except OSError:
                continue
            identity = (info.st_dev, info.st_ino)
            if identity not in found.values():
                found[entry.name] = identity
        return found


def _rss_anon(pid: int) -> int | None:
    """The anonymous memory resident in ``pid`` (``RssAnon``), in bytes."""
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        return None
    for line in status.splitlines():
        if line.startswith("RssAnon:"):
            return int(line.split()[1]) * 1024
    return 0


class _Spawner:
    """Forks every launcher from one thread that lives as long as the process.

    The kernel sends a launcher's death signal (``PR_SET_PDEATHSIG``) when the thread that forked
    it ends, not the process; a caller's thread may end long before the process does, as one that
    runs ``asyncio.run`` in a pool does.
    """

    def __init__(self) -> None:
        self._reset()
        os.register_at_fork(after_in_child=self._reset)

    def _reset(self) -> None:
        self._lock = threading.Lock()
        self._requests: queue.SimpleQueue[
            tuple[Callable[[], Any], concurrent.futures.Future[Any]]
        ] = queue.SimpleQueue()
        self._thread: threading.Thread | None = None

    def submit(self, func: Callable[[], Any]) -> concurrent.futures.Future[Any]:
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._requests = queue.SimpleQueue()
                self._thread = threading.Thread(
                    target=self._serve,
                    args=(self._requests,),
                    name="hpc-sandbox-launcher",
                    daemon=True,
                )
                self._thread.start()
            self._requests.put((func, future))
        return future

    @staticmethod
    def _serve(
        requests: queue.SimpleQueue[tuple[Callable[[], Any], concurrent.futures.Future[Any]]],
    ) -> None:
        while True:
            func, future = requests.get()
            if future.set_running_or_notify_cancel():
                try:
                    future.set_result(func())
                except BaseException as error:  # noqa: BLE001 - the caller gets it
                    future.set_exception(error)


_SPAWNER = _Spawner()


def prepare_template(rootfs: Path) -> None:
    """Give a new template the provider's directory (:data:`PROVIDER_DIR`), in place of any the
    image has, and a ``/tmp`` that is not empty: gVisor mounts a tmpfs of its own over an empty
    one, which would keep ``/tmp`` out of the overlay, and so out of snapshots."""
    tmp = rootfs / "tmp"
    if not tmp.is_symlink():
        if not tmp.exists():
            tmp.mkdir()
            tmp.chmod(0o1777)
        if tmp.is_dir() and not any(tmp.iterdir()):
            (tmp / KEEP_TMP).touch(0o644)
    path = rootfs / PROVIDER_DIR
    if path.is_symlink() or (path.exists() and not path.is_dir()):
        path.unlink()
    elif path.exists():
        remove_tree(path)
    for directory in (path, path / "transfer", path / "mounts"):
        directory.mkdir()
        directory.chmod(0o755)


def make_mount_points(rootfs: Path, *, directories: int, files: int) -> None:
    """Have the template's mount points ``d0``... and ``f0``... in its provider directory, for so
    many binds of directories and of files, creating those it lacks. Each creation is atomic, so
    starts that need the same ones at once agree."""
    if not directories and not files:
        return
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor = os.open(rootfs / PROVIDER_DIR / "mounts", flags)
    try:
        for index in range(directories):
            with suppress(FileExistsError):
                os.mkdir(f"d{index}", 0o755, dir_fd=descriptor)
        for index in range(files):
            create = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
            with suppress(FileExistsError):
                os.close(os.open(f"f{index}", create, 0o644, dir_fd=descriptor))
    finally:
        os.close(descriptor)


def plan_binds(rootfs: Path, binds: Sequence[tuple[Path, str, bool]]) -> list[Bind]:
    """Where runsc binds each ``(source, target, read_only)`` without writing to the template
    ``rootfs``: at ``target`` when the template has it, as runsc's gofer resolves it
    (:func:`resolve_in_root`), with the source's type (a directory, or a regular file); else at a
    mount point of the provider's directory, numbered among the binds of its type. A target that
    resolves into the provider's directory, or into an earlier bind at its target, where the gofer
    would resolve it in that bind's source, goes to a mount point too."""
    root = str(rootfs)
    provider_dir = os.path.join(root, PROVIDER_DIR)
    taken = [provider_dir]
    planned = []
    counts = {True: 0, False: 0}
    for source, target, read_only in binds:
        directory = source.is_dir()
        resolved = _existing_target(root, target, directory=directory)
        if (
            resolved is not None
            and not _within(provider_dir, resolved)  # it would hide the transfer directory
            and not any(_within(resolved, tree) for tree in taken)
        ):
            taken.append(resolved)
            planned.append(Bind(source, target, read_only))
            continue
        kind = "d" if directory else "f"
        point = f"{MOUNT_POINTS}/{kind}{counts[directory]}"
        counts[directory] += 1
        planned.append(Bind(source, target, read_only, point))
    return planned


def _existing_target(root: str, target: str, *, directory: bool) -> str | None:
    """Where ``target`` resolves in ``root``, if a directory (``directory``) or a regular file is
    there."""
    try:
        resolved = resolve_in_root(root, target)
        mode = os.lstat(resolved).st_mode
    except (OSError, ValueError):
        return None
    return resolved if (stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode)) else None


def resolve_in_root(root: str, path: str) -> str:
    """``path`` resolved in the directory ``root`` as runsc's gofer resolves a mount's destination
    (its ``ResolveSymlinks``): symlinks are followed inside ``root``, an absolute one from
    ``root``, and ``..`` stops at ``root``; missing components are kept. ``ValueError`` after 255
    symlinks."""
    return _resolve(root, root, path, 255)


def _resolve(root: str, base: str, path: str, follow: int) -> str:
    if follow == 0:
        raise ValueError(f"too many symlinks in {path!r}")
    for name in os.path.normpath(path).split("/"):
        if not name:
            continue
        candidate = os.path.normpath(os.path.join(base, name))
        if not candidate.startswith(root):  # no way out of root with ..
            base = root
            continue
        try:
            info = os.lstat(candidate)
        except FileNotFoundError:
            base = candidate
            continue
        if stat.S_ISLNK(info.st_mode):
            link = os.readlink(candidate)
            base = _resolve(root, root if link.startswith("/") else base, link, follow - 1)
            continue
        base = candidate
    return base


def _within(path: str, tree: str) -> bool:
    return path == tree or path.startswith(tree.rstrip("/") + "/")


def command_groups(listing: str, pid: int) -> list[int]:
    """The process groups of ``pid`` and of every process below it, from ``runsc ps``'s table
    (UID, PID, PPID, PGID...), ``pid``'s first.

    A row needs every column: a command can put a line break in its name, but not a whole row.
    A PID listed twice is ignored, and so is the group of PID 1, the keep-alive loop's.
    """
    parents: dict[int, int] = {}
    groups: dict[int, int] = {}
    repeated = set()
    for line in listing.splitlines()[1:]:
        fields = line.split()
        if len(fields) < _PS_FIELDS:
            continue
        try:
            process, parent, group = int(fields[1]), int(fields[2]), int(fields[3])
        except ValueError:
            continue
        if process in groups:
            repeated.add(process)
        parents[process], groups[process] = parent, group
    for process in repeated:
        del parents[process], groups[process]
    if pid not in groups:
        return []
    tree = {pid}
    grew = True
    while grew:
        below = {process for process, parent in parents.items() if parent in tree}
        grew = not below <= tree
        tree |= below
    found = [groups[pid]]
    for process in sorted(tree):
        if groups[process] not in found:
            found.append(groups[process])
    return [group for group in found if group != 1]


def cpus_in_use() -> Counter[int]:
    """How many of your gVisor sandboxes on this node may run on each CPU.

    A sandbox is known by the processes of yours that run ``runsc --rootless`` and end their
    arguments with its ID: its launcher, whose ``--cpus`` says its CPUs until it runs runsc, and
    then runsc's own processes, whose CPU affinity does.
    """
    sandboxes: dict[str, set[int]] = {}
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        arguments = procfs.argv(pid)
        if (
            not arguments
            or not _SANDBOX_ID.fullmatch(arguments[-1])
            or arguments[-1] in sandboxes
            or "--rootless" not in arguments
            or procfs.owned_live_start_time(pid) is None
        ):
            continue
        try:
            if str(ANCHOR_SCRIPT) in arguments and "--cpus" in arguments:
                cpus = cpu_list(arguments[arguments.index("--cpus") + 1])
            else:
                cpus = os.sched_getaffinity(pid)
        except (OSError, ValueError, IndexError):
            continue
        sandboxes[arguments[-1]] = set(cpus)
    load: Counter[int] = Counter()
    for cpus in sandboxes.values():
        load.update(cpus)
    return load


def runsc_processes(state: Path) -> list[tuple[int, str]]:
    """Your live processes whose arguments name ``state`` as runsc's ``--root``, with their start
    times: what runs for the sandbox whose state directory it is, which no other sandbox uses."""
    wanted = {f"--root={state}", f"-root={state}"}
    found = []
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        arguments = procfs.argv(pid)
        if not arguments or not (
            wanted & set(arguments)
            or any(
                flag in ("--root", "-root") and value == str(state)
                for flag, value in zip(arguments, arguments[1:], strict=False)
            )
        ):
            continue
        if (started := procfs.owned_live_start_time(pid)) is not None:
            found.append((pid, started))
    return found


def _outlived(
    state: Path, recorded: Sequence[tuple[int, str]], settle_s: float, timeout_s: float
) -> list[tuple[int, str]]:
    """What still runs of a sandbox whose launcher is dead: the processes that name ``state``
    (:func:`runsc_processes`), and the ``recorded`` ones, whatever their arguments say.

    runsc's processes die with their parents at once, but the kernel can take a while to tear
    down one that held much memory, whose arguments are gone by then: such processes, on their
    way out, get up to ``timeout_s``; the others ``settle_s``.
    """
    started = time.monotonic()
    while True:
        found = dict(runsc_processes(state))
        for pid, start_time in recorded:
            if procfs.owned_live_start_time(pid) == start_time:
                found[pid] = start_time
        elapsed = time.monotonic() - started
        if (
            not found
            or elapsed >= timeout_s
            or (elapsed >= settle_s and not any(map(procfs.exiting, found)))
        ):
            return list(found.items())
        time.sleep(0.02)


def _bind(source: Path, target: str, read_only: bool) -> dict[str, Any]:
    return {
        "destination": target,
        "type": "bind",
        "source": str(source),
        "options": ["rbind", "ro" if read_only else "rw"],
    }


def _write_record(session: Session, lower: Path, upper: Path | None) -> None:
    record = {"lower": str(lower), "upper": str(upper) if upper else None}
    (session.path / ROOTFS_RECORD).write_text(json.dumps(record, sort_keys=True) + "\n")


def _read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(errors="replace").splitlines()
    except OSError:
        return []


def _tail(path: Path, lines: int = 20) -> str:
    return "\n".join(_read_lines(path)[-lines:]).strip() or "no output"
