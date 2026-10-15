"""Rootless sandboxes as enroot containers.

Enroot has no daemon and no notion of a running instance: ``enroot start``
runs one command in a container and the container ends with it. A sandbox is
therefore built around one long-lived ``enroot start``:

- **Rootfs.** The image (a squashfs file from ``enroot import``) is extracted
  with ``enroot create`` into the sandbox's session directory on node-local
  scratch, as a private writable directory.
- **Namespaces.** :mod:`hpc_sandbox.anchor` creates fresh user, mount, PID
  and IPC namespaces (and a network namespace with only loopback for
  ``network="none"``) and runs ``enroot start --rw`` in them as PID 1. The
  container's command is a keep-alive loop, so the container's init is that
  loop: tasks cannot signal it, orphans are reaped by it, and killing it from
  the host kills every process in the sandbox. Both the launcher and the init
  are SIGKILLed when the process that started them dies, so a crashed owner
  leaves only files behind.
- **Commands** enter the init's namespaces and root directory with
  ``nsenter``, in two steps (the PID and IPC namespaces belong to the
  launcher's user namespace, the mounts to enroot's nested one), and run with
  the container's environment plus the requested variables. Each command
  costs a couple of milliseconds instead of a full ``enroot start``.
- **Isolation from the site.** ``enroot start`` runs with a minimal
  environment: the home directory is not mounted (``ENROOT_MOUNT_HOME=no``;
  an empty value would fall back to the site default), the login shell and
  user-level enroot configuration are skipped, ``/dev`` is restricted, the
  NVIDIA and Mellanox hooks stay off unless GPUs are requested, and batch-job
  variables do not reach site hooks. ``/tmp`` and ``/var/tmp`` are
  session-private directories and ``/dev/shm`` is a private tmpfs.

Host requirements: enroot 3.x with its ``+caps`` helpers for imports,
util-linux ``nsenter`` and unprivileged user namespaces.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from . import procfs, safefs
from .images import (
    DIGEST,
    LOCAL_PREFIXES,
    ImageStore,
    ResolvedImage,
)
from .paths import default_image_cache, should_stage
from .process import DEFAULT_OUTPUT_LIMIT, ProcessResult, run_process
from .runtime import RUNTIME_CANDIDATES
from .sandbox import SessionProvider, SessionSandbox, make_owner_writable, poll
from .sessions import Session
from .types import SandboxError, SandboxLostError, SandboxSpec, SandboxStartError

ANCHOR_SCRIPT = Path(__file__).with_name("anchor.py")
READY = "hpc-sandbox-ready"
KEEPALIVE = f"""\
trap '' HUP INT QUIT TERM USR1 USR2
command -v sleep >/dev/null 2>&1 || {{ echo "hpc-sandbox: the image has no sleep" >&2; exit 127; }}
echo {READY}
exec </dev/null >/dev/null 2>&1
while :; do sleep 86400 & wait $!; done
"""
RC_FILE = "rc"
ANCHOR_LOG = "anchor.log"
# Runs a command in a working directory: sh -c ENTER hpc-sandbox CWD ARGV...
_ENTER = 'cd -- "$1" && shift && exec "$@"'
_SHELL_STATE = ("PWD", "OLDPWD", "SHLVL", "_")
_DOCKER_HUB = ("docker.io", "index.docker.io", "registry-1.docker.io")
_KILL_WAIT_S = 5.0

# Enroot 3.5's whiteout converter aborts with EEXIST when a layer contains both
# a whiteout and an entry of the same name, and cannot unlink whiteouts below
# depth five in read-only directories. Installed first on PATH during imports,
# this wrapper resolves those cases the way OCI defines them, then runs the
# real converter, whose file capabilities stay in effect.
AUFS2OVLFS_WRAPPER = """\
#!/bin/bash
set -euo pipefail

layer=$1
declare -a repair_dirs=()
declare -a repair_modes=()

repair_dir() {
    local directory=$1 existing
    for existing in "${repair_dirs[@]}"; do
        if [ "${existing}" = "${directory}" ]; then
            return
        fi
    done
    repair_dirs+=("${directory}")
    repair_modes+=("$(stat -c '%a' -- "${directory}")")
    chmod u+rwx -- "${directory}"
}

restore_modes() {
    local restore_status=0 index
    for ((index=${#repair_dirs[@]} - 1; index >= 0; index--)); do
        if ! chmod -- "${repair_modes[index]}" "${repair_dirs[index]}"; then
            restore_status=1
        fi
    done
    return "${restore_status}"
}

finish() {
    local status=$?
    trap - EXIT
    if ! restore_modes && [ "${status}" -eq 0 ]; then
        status=1
    fi
    exit "${status}"
}
trap finish EXIT

# Make whiteout parents writable, remembering their modes for restoration.
while IFS= read -r -d '' marker; do
    repair_dir "${marker%/*}"
done < <(find "${layer}" -type f -name '.wh.*' -print0)

# A whiteout only hides entries of lower layers. A same-layer non-directory
# wins outright; a same-layer directory becomes opaque, keeping its own
# children while still hiding the lower ones.
while IFS= read -r -d '' marker; do
    name=${marker##*/}
    case "${name}" in
        .wh..wh.*|.wh.|.wh..|.wh...) continue ;;
    esac
    target=${marker%/*}/${name#.wh.}
    if [ -L "${target}" ] || { [ -e "${target}" ] && [ ! -d "${target}" ]; }; then
        rm -- "${marker}"
    elif [ -d "${target}" ]; then
        opaque=${target}/.wh..wh..opq
        repair_dir "${target}"
        if [ ! -e "${opaque}" ] && [ ! -L "${opaque}" ]; then
            mv -- "${marker}" "${opaque}"
        elif [ -f "${opaque}" ] && [ ! -s "${opaque}" ] && [ ! -L "${opaque}" ]; then
            rm -- "${marker}"
        fi
    fi
done < <(find "${layer}" -type f -empty -name '.wh.*' -print0)

"${HPC_SANDBOX_REAL_AUFS2OVLFS:?}" "$@"
"""


def import_uri(reference: str) -> str:
    """The ``enroot import`` URI of an image reference.

    Accepts ``docker://`` references in either Docker's ``registry/path`` or
    enroot's ``registry#path`` spelling, bare references (``ubuntu:24.04``,
    ``ghcr.io/org/image:tag``) and enroot's ``dockerd://`` and ``podman://``.
    Docker Hub is spelled the way enroot expects (``docker://library/...``
    without a registry). Raises ValueError for references enroot cannot
    import, including digest pins, which enroot 3.x misparses.
    """
    if reference.startswith(("dockerd://", "podman://")):
        return reference
    if "://" in reference and not reference.startswith(("docker://", "https://", "http://")):
        raise ValueError(f"enroot cannot import {reference!r}; use a docker:// reference")
    target = reference.split("://", 1)[1] if "://" in reference else reference
    if "@sha256:" in target:
        raise ValueError(
            f"enroot cannot import by digest ({reference!r}); import the image once with "
            "`enroot import` and pin the .sqsh file with an @sha256:<hex> suffix instead"
        )
    if target.endswith(".sif"):
        raise ValueError(f"{reference!r} is a SIF image; the enroot provider runs squashfs images")
    registry, separator, path = target.partition("#")
    if not separator:
        registry, separator, path = target.partition("/")
        if not separator or not ("." in registry or ":" in registry or registry == "localhost"):
            return f"docker://{target}"
    if registry in _DOCKER_HUB:
        return f"docker://{path}"
    return f"docker://{registry}#{path}"


class EnrootRuntime:
    """The enroot command line, run with environments this package controls.

    ``cache_dir`` and ``temp_dir`` hold layers and extraction space for
    imports unless the caller's environment already sets ``ENROOT_CACHE_PATH``
    or ``ENROOT_TEMP_PATH``; ``config_dir`` is an empty directory used as
    ``ENROOT_CONFIG_PATH`` when containers start, so user-level hooks, mounts
    and environment files never apply to sandboxes. ``tools_dir`` receives the
    whiteout converter wrapper.
    """

    def __init__(
        self,
        binary: str | None = None,
        *,
        cache_dir: Path,
        temp_dir: Path,
        config_dir: Path,
        tools_dir: Path,
    ) -> None:
        self.binary = binary
        self.cache_dir = cache_dir
        self.temp_dir = temp_dir
        self.config_dir = config_dir
        self.tools_dir = tools_dir
        self._path: str | None = None

    @property
    def path(self) -> str:
        if self._path is None:
            resolved = shutil.which(self.binary or "enroot")
            if resolved is None:
                raise SandboxError(f"enroot executable {self.binary or 'enroot'!r} is not on PATH")
            self._path = resolved
        return self._path

    async def run(
        self, *args: str, env: Mapping[str, str], timeout_s: float, output_limit: int = 1 << 20
    ) -> ProcessResult:
        try:
            return await run_process(
                [self.path, *args], timeout_s=timeout_s, env=env, output_limit=output_limit
            )
        except OSError as error:
            raise SandboxError(f"could not run {self.path}: {error}") from error

    async def version(self) -> str:
        result = await self.run("version", env=os.environ, timeout_s=30.0)
        if result.returncode != 0:
            raise SandboxError(f"{self.path} version failed: {result.detail()}")
        return result.stdout.decode(errors="replace").strip()

    def import_environment(self) -> dict[str, str]:
        """The caller's environment (proxies, registry credentials) for ``enroot import``.

        Layers are extracted with GNU tar; ``--delay-directory-restore`` lets
        images that revisit a read-only directory extract instead of failing
        with ``Permission denied``.
        """
        env = dict(os.environ)
        env.setdefault("ENROOT_CACHE_PATH", str(self.cache_dir))
        env.setdefault("ENROOT_TEMP_PATH", str(self.temp_dir))
        tar_options = env.get("TAR_OPTIONS", "").split()
        if "--delay-directory-restore" not in tar_options:
            env["TAR_OPTIONS"] = " ".join([*tar_options, "--delay-directory-restore"])
        wrapper = self.tools_dir / "enroot-aufs2ovlfs"
        real = shutil.which("enroot-aufs2ovlfs", path=env.get("PATH"))
        if real is not None and Path(real).resolve() != wrapper.resolve():
            self.tools_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            if not wrapper.is_file() or wrapper.read_text() != AUFS2OVLFS_WRAPPER:
                temporary = wrapper.with_name(f".{wrapper.name}.{os.getpid()}")
                temporary.write_text(AUFS2OVLFS_WRAPPER)
                temporary.chmod(0o700)
                os.replace(temporary, wrapper)
            env["HPC_SANDBOX_REAL_AUFS2OVLFS"] = str(Path(real).resolve())
            env["PATH"] = os.pathsep.join([str(self.tools_dir), env.get("PATH", "")])
        return env

    def container_environment(
        self, session: Session, *, remap_root: bool, gpus: str | None
    ) -> dict[str, str]:
        """The environment of ``enroot create`` and ``enroot start`` for ``session``.

        Only what enroot needs is passed, so batch-job and user variables never
        reach site hooks. Boolean settings use explicit ``yes``/``no``: enroot
        treats an empty value as unset and falls back to the site default.
        """
        env = {
            key: os.environ[key]
            for key in (
                "PATH",
                "HOME",
                "USER",
                "LOGNAME",
                "ENROOT_LIBRARY_PATH",
                "ENROOT_SYSCONF_PATH",
            )
            if key in os.environ
        }
        env.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
        env.update(
            ENROOT_DATA_PATH=str(session.path),
            ENROOT_RUNTIME_PATH=str(session.path / "runtime"),
            ENROOT_CACHE_PATH=str(self.cache_dir),
            ENROOT_TEMP_PATH=str(self.temp_dir),
            ENROOT_CONFIG_PATH=str(self.config_dir),
            ENROOT_MOUNT_HOME="no",
            ENROOT_LOGIN_SHELL="no",
            ENROOT_RESTRICT_DEV="yes",
            ENROOT_ROOTFS_WRITABLE="yes",
            ENROOT_REMAP_ROOT="yes" if remap_root else "no",
            NVIDIA_VISIBLE_DEVICES=gpus or "void",
            MELLANOX_VISIBLE_DEVICES="void",
        )
        if gpus:
            env["NVIDIA_DRIVER_CAPABILITIES"] = os.environ.get(
                "NVIDIA_DRIVER_CAPABILITIES", "compute,utility"
            )
        return env


class SquashfsImages:
    """Squashfs images, imported by enroot and recognized by their magic bytes."""

    name = "squashfs"
    suffix = ".sqsh"

    def __init__(self, runtime: EnrootRuntime) -> None:
        self.runtime = runtime

    @property
    def tool(self) -> str:
        return self.runtime.path

    def is_local(self, reference: str) -> bool:
        return reference.startswith(LOCAL_PREFIXES) or DIGEST.sub("", reference).endswith(
            (".sqsh", ".squashfs")
        )

    def pull_uri(self, reference: str) -> str:
        return import_uri(reference)

    async def validate(self, path: Path) -> bool:
        try:
            with path.open("rb") as stream:
                return stream.read(4) == b"hsqs"
        except OSError:
            return False

    async def pull(self, uri: str, target: Path, *, timeout_s: float) -> ProcessResult:
        return await self.runtime.run(
            "import",
            "--output",
            str(target),
            uri,
            env=self.runtime.import_environment(),
            timeout_s=timeout_s,
        )


class EnrootProvider(SessionProvider["EnrootSandbox"]):
    """Start, restore and reap sandboxes backed by enroot containers.

    ``scratch_root`` must be node-local (it holds every writable rootfs);
    ``image_cache`` may be shared storage that supports ``flock``. Sandboxes
    run as root inside (``remap_root``, enroot's ``--root``), which is your
    uid outside; set ``remap_root=False`` to run as your own uid. ``gpus``
    enables enroot's NVIDIA hook: ``"all"`` passes every GPU this process may
    use (inside a Slurm allocation, the allocated ones), a list such as
    ``"0,1"`` selects GPUs numbered as this process sees them. Sandboxes are
    killed when the process that started them exits.
    """

    name = "enroot"
    sessions_directory = "sessions-enroot"

    def __init__(
        self,
        *,
        binary: str | None = None,
        scratch_root: str | Path | None = None,
        image_cache: str | Path | None = None,
        stage_images: bool | None = None,
        default_timeout_s: float = 600.0,
        start_timeout_s: float = 1800.0,
        stop_timeout_s: float = 60.0,
        remap_root: bool = True,
        gpus: str | None = None,
        extra_start_args: Sequence[str] = (),
        output_limit_bytes: int = DEFAULT_OUTPUT_LIMIT,
        memory_poll_s: float = 1.0,
    ) -> None:
        super().__init__(
            scratch_root=scratch_root,
            default_timeout_s=default_timeout_s,
            start_timeout_s=start_timeout_s,
            stop_timeout_s=stop_timeout_s,
            output_limit_bytes=output_limit_bytes,
            memory_poll_s=memory_poll_s,
        )
        self.sandbox_type = EnrootSandbox
        self.runtime = EnrootRuntime(
            binary,
            cache_dir=self.scratch_root / "runtime-cache",
            temp_dir=self.scratch_root / "runtime-tmp",
            config_dir=self.scratch_root / "enroot-config",
            tools_dir=self.scratch_root / "enroot-tools",
        )
        cache = Path(image_cache) if image_cache else default_image_cache()
        if stage_images is None:
            stage_images = should_stage(cache, self.scratch_root)
        self.images = ImageStore(
            SquashfsImages(self.runtime),
            cache,
            stage_dir=self.scratch_root / "images" if stage_images else None,
            pull_timeout_s=start_timeout_s,
        )
        self.remap_root = remap_root
        self.gpus = gpus
        self.extra_start_args = tuple(extra_start_args)
        self.capabilities = frozenset(
            {"snapshot", "isolation", "network_none"} | ({"gpu"} if gpus else set())
        )
        self._nsenter: str | None = None

    @property
    def nsenter(self) -> str:
        if self._nsenter is None:
            resolved = shutil.which("nsenter")
            if resolved is None:
                raise SandboxError("nsenter (util-linux) is not on PATH")
            self._nsenter = resolved
        return self._nsenter

    def _session_roots(self) -> list[tuple[Path, bool]]:
        # Earlier versions kept enroot sessions in the Singularity provider's
        # directory: sweep the ones recognizably ours there too, and those
        # nobody can tell apart when no Singularity janitor could take them.
        singularity = any(shutil.which(binary) for binary in RUNTIME_CANDIDATES)
        return [*super()._session_roots(), (self.scratch_root / "sessions", not singularity)]

    def _check(self, spec: SandboxSpec) -> None:
        super()._check(spec)
        for mount in spec.mounts:
            if any(c.isspace() for text in (mount.source, mount.target) for c in text):
                raise ValueError(f"mount paths cannot contain whitespace: {mount}")

    def _scratch_directories(self) -> list[Path]:
        runtime = self.runtime
        return [
            *super()._scratch_directories(),
            runtime.cache_dir,
            runtime.temp_dir,
            runtime.config_dir,
        ]

    async def _extract(self, image: ResolvedImage, session: Session) -> None:
        result = await self.runtime.run(
            "create",
            "--name",
            session.rootfs.name,
            str(image.path),
            env=self.runtime.container_environment(
                session, remap_root=self.remap_root, gpus=self.gpus
            ),
            timeout_s=self.start_timeout_s,
        )
        if result.returncode != 0 or result.timed_out:
            raise SandboxStartError(
                f"could not create a rootfs from {image.path}: {result.detail()}"
            )
        await asyncio.to_thread(make_owner_writable, session.rootfs)

    async def _stop_abandoned(self, path: Path, record: dict[str, Any] | None) -> bool:
        if record is None:
            return False
        processes = []
        for role in ("init", "anchor"):  # the init first, so its parent can reap it
            pid, started = record.get(f"{role}_pid"), record.get(f"{role}_start_time")
            if isinstance(pid, int) and isinstance(started, str):
                processes.append((pid, started))
        return await asyncio.to_thread(kill_exact, processes, path.name)


class EnrootSandbox(SessionSandbox):
    """One running enroot container; create it with :meth:`EnrootProvider.start`."""

    def __init__(self, provider: EnrootProvider, spec: SandboxSpec) -> None:
        super().__init__(provider, spec)
        self._provider: EnrootProvider = provider
        self._anchor: subprocess.Popen[bytes] | None = None
        self._anchor_start_time: str | None = None
        self._init_pid: int | None = None
        self._init_start_time: str | None = None
        self._environment: dict[str, str] = {}

    def _identity(self) -> dict[str, Any]:
        return {
            "anchor_pid": self._anchor.pid if self._anchor else None,
            "anchor_start_time": self._anchor_start_time,
            "init_pid": self._init_pid,
            "init_start_time": self._init_start_time,
        }

    def _user(self) -> dict[str, Any]:
        provider = self._provider
        remap_root = provider.remap_root or any(
            arg in ("--root", "-r") for arg in provider.extra_start_args
        )
        return {"remap_root": remap_root, "container_uid": 0 if remap_root else os.getuid()}

    def _watch_root(self) -> int | None:
        # The launcher and the container's init (its only child) are protected.
        return self._anchor.pid if self._anchor and self._init_pid else None

    def _prepare_rootfs(self) -> None:
        assert self._session is not None
        session = self._session
        for directory in (session.tmp, session.var_tmp):
            directory.mkdir(exist_ok=True)
            directory.chmod(0o1777)
        home = session.rootfs / "root"  # HOME defaults to /root
        if not home.exists() and not home.is_symlink():
            home.mkdir()
        # Run the command as given: skip the image's entrypoint and rc.local.
        (session.path / RC_FILE).write_text('exec "$@"\n')

    def _prepare_snapshot(self) -> None:
        assert self._session is not None
        make_placeholders_readable(self._session.rootfs)

    def _start_args(self) -> list[str]:
        assert self._session is not None
        session, provider = self._session, self._provider
        args = ["start", "--rw", *(["--root"] if provider.remap_root else [])]
        args += ["--rc", str(session.path / RC_FILE)]
        mounts = [
            f"{session.tmp}:/tmp:none:x-create=dir,bind,rw,nosuid",
            f"{session.var_tmp}:/var/tmp:none:x-create=dir,bind,rw,nosuid",
            "tmpfs:/dev/shm:tmpfs:x-create=dir,rw,nosuid,nodev,mode=1777",
        ]
        for mount in self.spec.mounts:
            source = Path(mount.source).resolve()
            kind = "dir" if source.is_dir() else "file"
            mode = "ro" if mount.read_only else "rw"
            mounts.append(f"{source}:{mount.target}:none:x-create={kind},bind,{mode},nosuid")
        for mount in mounts:
            args += ["--mount", mount]
        return [
            *args,
            *provider.extra_start_args,
            session.rootfs.name,
            "/bin/sh",
            "-c",
            KEEPALIVE,
            self.id,
        ]

    async def _start_container(self) -> None:
        assert self._session is not None
        session, provider = self._session, self._provider
        command = [
            sys.executable,
            "-I",
            "-S",
            str(ANCHOR_SCRIPT),
            str(os.getpid()),
            self.spec.network,
            "--",
            provider.runtime.path,
            *self._start_args(),
        ]
        environment = provider.runtime.container_environment(
            session, remap_root=provider.remap_root, gpus=provider.gpus
        )
        log_path = session.path / ANCHOR_LOG
        with log_path.open("wb") as log:
            # Popen, not asyncio: nothing can interrupt the spawn before the
            # handle is recorded, and no watcher thread waits on this child.
            self._anchor = subprocess.Popen(  # noqa: ASYNC220 - a fork and exec
                command,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                env=environment,
                start_new_session=True,
            )
        anchor = self._anchor
        self._anchor_start_time = procfs.start_time(anchor.pid)
        self._record()
        deadline = time.monotonic() + provider.start_timeout_s
        while READY not in _read_lines(log_path):
            if anchor.poll() is not None:
                raise SandboxStartError(
                    f"the container exited during startup (status {anchor.returncode}): "
                    f"{_tail(log_path)}"
                )
            if time.monotonic() > deadline:
                raise SandboxStartError(
                    f"the container did not start within {provider.start_timeout_s:g} s: "
                    f"{_tail(log_path)}"
                )
            await asyncio.sleep(0.02)
        children = procfs.children(anchor.pid)
        if len(children) != 1:
            raise SandboxStartError(f"unexpected process tree below the launcher: {children}")
        (init,) = children
        started = procfs.owned_live_start_time(init)
        if started is None or procfs.namespace_pid(init) != 1:
            raise SandboxStartError("could not prove the identity of the container's init")
        environment = procfs.environ(init)
        if environment is None:
            raise SandboxStartError("could not read the container's environment")
        self._init_pid, self._init_start_time = init, started
        self._environment = {k: v for k, v in environment.items() if k not in _SHELL_STATE}
        self._record()

    def _enter_args(self) -> list[str]:
        """``nsenter`` twice: the launcher's user namespace owns the PID, IPC and
        network namespaces, enroot's nested user namespace owns the mounts."""
        assert self._anchor is not None and self._init_pid is not None
        nsenter, anchor, init = self._provider.nsenter, self._anchor.pid, self._init_pid
        outer = [
            nsenter,
            f"--user=/proc/{anchor}/ns/user",
            f"--pid=/proc/{init}/ns/pid",
            f"--ipc=/proc/{init}/ns/ipc",
        ]
        if self.spec.network == "none":
            outer.append(f"--net=/proc/{init}/ns/net")
        inner = [
            nsenter,
            f"--user=/proc/{init}/ns/user",
            f"--mount=/proc/{init}/ns/mnt",
            f"--root=/proc/{init}/root",
            f"--wd=/proc/{init}/root",
        ]
        return [*outer, "--preserve-credentials", "--", *inner, "--preserve-credentials", "--"]

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
        # Re-prove the init's identity: nsenter must never enter a recycled PID.
        if not self._container_alive():
            self._running = False
            self._lost = "the container's init process is gone"
            raise SandboxLostError(f"sandbox {self.id} lost its container: {self._lost}")
        return await run_process(
            [*self._enter_args(), "/bin/sh", "-c", _ENTER, "hpc-sandbox", cwd, *argv],
            timeout_s=timeout_s,
            stdin=stdin,
            env={**self._environment, **env},
            output_limit=self._provider.output_limit_bytes,
            on_start=on_start,
        )

    def _container_alive(self) -> bool:
        return (
            self._init_pid is not None
            and procfs.owned_live_start_time(self._init_pid) == self._init_start_time
        )

    async def _stop_container(self, session: Session) -> None:
        anchor = self._anchor
        if anchor is None:
            return
        if anchor.poll() is None:
            init = self._init_pid
            if init is None:  # startup failed before the init was identified
                init = next(iter(procfs.children(anchor.pid)), None)
            started = procfs.owned_live_start_time(init) if init is not None else None
            if init is not None and started is not None and init in procfs.children(anchor.pid):
                _kill(init)  # its parent, the launcher, reaps it and exits
            if await poll(anchor.poll, _KILL_WAIT_S) is None:
                anchor.kill()
            if await poll(anchor.poll, self._provider.stop_timeout_s) is None:
                raise SandboxError(f"the launcher of sandbox {self.id} survived SIGKILL")
        if self._init_pid is not None and self._container_alive():
            raise SandboxError(f"the container init of sandbox {self.id} survived teardown")
        self._anchor = None


def make_placeholders_readable(rootfs: Path) -> None:
    """Let the owner read the empty mode-0 mount points enroot creates.

    Enroot creates missing bind targets (``/etc/resolv.conf``, mount targets,
    ...) as empty files and directories with mode 0, which a copy as this uid
    cannot open. Inside the container they stay hidden below their mounts.
    The rootfs is live, so entries may vanish or turn into symlinks meanwhile:
    those are skipped, and modes are only ever changed on what was checked.
    Other unreadable directories are left as they are (the copy reports
    them), and so is anything more than :data:`safefs.MAX_DEPTH` levels down.
    """
    # Depth-first with one open descriptor per level.
    stack: list[tuple[int, Iterator[str]]] = []
    try:
        fd = os.open(rootfs, safefs.DIRECTORY)
        stack.append((fd, iter(safefs.listing(fd))))
        while stack:
            fd, names = stack[-1]
            name = next(names, None)
            if name is None:
                os.close(stack.pop()[0])
                continue
            if not _open_placeholder(name, fd) or len(stack) >= safefs.MAX_DEPTH:
                continue
            try:
                child = os.open(name, safefs.DIRECTORY, dir_fd=fd)
            except OSError:
                continue  # gone, replaced, or unreadable
            try:
                stack.append((child, iter(safefs.listing(child))))
            except OSError:
                os.close(child)
    finally:
        for fd, _ in stack:
            os.close(fd)


def _open_placeholder(name: str, dir_fd: int) -> bool:
    """Make ``name`` readable if it is a placeholder; return whether it is a directory."""
    try:
        item = os.open(name, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=dir_fd)
    except FileNotFoundError:
        return False
    try:
        info = os.fstat(item)
        directory = stat.S_ISDIR(info.st_mode)
        placeholder = (
            stat.S_IMODE(info.st_mode) == 0
            and info.st_uid == os.getuid()
            and (directory or (stat.S_ISREG(info.st_mode) and info.st_size == 0))
        )
        if placeholder:
            os.chmod(f"/proc/self/fd/{item}", 0o500 if directory else 0o400)
        return directory
    finally:
        os.close(item)


def kill_exact(processes: list[tuple[int, str]], name: str) -> bool:
    """SIGKILL the processes ``(pid, start time)`` that still run sandbox ``name``.

    A process is signalled only if its start time matches and its command
    line names the sandbox; return whether any was running.
    """
    killed = []
    for pid, started in processes:
        if procfs.owned_live_start_time(pid) != started:
            continue
        if name not in (procfs.cmdline(pid) or ""):
            raise SandboxError(f"refusing to stop {name!r}: PID {pid} identity changed")
        _kill(pid)
        killed.append((pid, started))
    deadline = time.monotonic() + _KILL_WAIT_S
    for pid, started in killed:
        while procfs.owned_live_start_time(pid) == started:
            if time.monotonic() > deadline:
                raise SandboxError(f"process {pid} of {name!r} survived SIGKILL")
            time.sleep(0.02)
    return bool(killed)


def _kill(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(errors="replace").splitlines()
    except OSError:
        return []


def _tail(path: Path, lines: int = 20) -> str:
    return "\n".join(_read_lines(path)[-lines:]).strip() or "no output"
