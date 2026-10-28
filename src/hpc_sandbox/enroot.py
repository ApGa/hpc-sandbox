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
  launcher's user namespace, the mounts to enroot's nested one). The
  container's environment plus the requested variables is exported by a
  prologue inside the container; the host-side ``nsenter`` processes run with
  a minimal environment. Each command costs a couple of milliseconds instead
  of a full ``enroot start``.
- **Isolation from the site.** ``enroot start`` runs with a minimal
  environment and a system configuration the provider writes instead of the
  site's ``/etc/enroot``: enroot's standard system mounts, a minimal ``/dev``,
  and only the standard hooks sandboxes need. The home directory is not
  mounted (``ENROOT_MOUNT_HOME=no``; an empty value would fall back to the
  site default), the login shell and user-level enroot configuration are
  skipped, the NVIDIA hook acts only when ``gpus`` is set, the Mellanox hook
  never runs, and batch-job variables do not reach hooks. ``/tmp`` and
  ``/var/tmp`` are session-private directories and ``/dev/shm`` is a private
  tmpfs.
- **Restores** start from files that code in the sandbox wrote, so enroot
  must not act on them from the host: its ``/etc/fstab`` and
  ``/etc/environment`` are set aside while the container starts, hooks that
  write into the rootfs do not run (their results are in the snapshot), and
  commands get the environment recorded with the snapshot.

Host requirements: enroot 3.x with its ``+caps`` helpers for imports,
util-linux ``nsenter`` and unprivileged user namespaces.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from . import procfs, safefs
from .images import (
    DIGEST,
    LOCAL_PREFIXES,
    SQUASHFS_MAGIC,
    SQUASHFS_SUFFIXES,
    ImageStore,
    ResolvedImage,
    file_magic,
    is_sif,
    local_path,
)
from .paths import default_image_cache, should_stage
from .pods import PodService
from .process import DEFAULT_OUTPUT_LIMIT, ProcessResult, run_process
from .runtime import RUNTIME_CANDIDATES
from .sandbox import SessionProvider, SessionSandbox, make_owner_writable, poll
from .sessions import Session, kill_exact, remove_tree, signal_kill
from .types import SandboxError, SandboxSpec, SandboxStartError

if TYPE_CHECKING:
    from .gvisor import GvisorProvider

log = logging.getLogger(__name__)
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
# The environment of host-side helpers (nsenter): enough to run them, nothing
# the container could have chosen.
_HOST_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
_SHELL_STATE = ("PWD", "OLDPWD", "SHLVL", "_")
ENVIRONMENT_FILE = "environment.json"
# Variables `enroot import` may need from the caller, besides ENROOT_* settings.
_IMPORT_ENV = frozenset(
    {
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "LANG",
        "LC_ALL",
        "TMPDIR",
        "TAR_OPTIONS",
        "SOURCE_DATE_EPOCH",
        "REGISTRY_AUTH_FILE",
        "CONTAINER_HOST",
        "XDG_CONFIG_HOME",
        "XDG_CACHE_HOME",
        "XDG_DATA_HOME",
        "XDG_RUNTIME_DIR",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "CURL_CA_BUNDLE",
        *(f"{name}_proxy" for name in ("http", "https", "all", "no")),
        *(f"{name}_PROXY" for name in ("HTTP", "HTTPS", "ALL", "NO")),
    }
)
# Prefixes of variables enroot settings and dockerd:// and podman:// imports need.
_IMPORT_PREFIXES = ("ENROOT_", "DOCKER_", "CONTAINERS_")
# The configuration section of the enroot script, which loads enroot.conf and
# its drop-ins and sets the defaults.
_CONFIG_SECTION = re.compile(
    r"^# -+ BEGIN CONFIG -+\n(.*?)^# -+ END CONFIG -+$", re.MULTILINE | re.DOTALL
)
_SETTING_TIMEOUT_S = 30.0
_VARIABLE_REFERENCE = re.compile(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)")
_DOCKER_HUB = ("docker.io", "index.docker.io", "registry-1.docker.io")
_KILL_WAIT_S = 5.0
# The mounts every sandbox starts with: enroot's standard system mounts, with
# a minimal /dev built here rather than by a hook (a /dev that depends on a
# hook falls back to the host's whole /dev when the hook is missing), and the
# host's /etc/hosts. /tmp, /var/tmp and /dev/shm are mounted per sandbox.
SYSTEM_FSTAB = """\
/proc        /proc        none    x-create=dir,rbind,rw,nosuid,nodev,noexec,rslave  0 -1
/sys         /sys         none    x-create=dir,rbind,ro,nosuid,nodev,noexec,rslave  0 -1
tmpfs        /dev         tmpfs   x-create=dir,rw,nosuid,noexec,mode=755,slave      0 -1
/dev/null    /dev/null    none    x-create=file,bind,rw,nosuid,noexec,private       0 -1
/dev/zero    /dev/zero    none    x-create=file,bind,rw,nosuid,noexec,private       0 -1
/dev/full    /dev/full    none    x-create=file,bind,rw,nosuid,noexec,private       0 -1
/dev/random  /dev/random  none    x-create=file,bind,rw,nosuid,noexec,private       0 -1
/dev/urandom /dev/urandom none    x-create=file,bind,rw,nosuid,noexec,private       0 -1
/dev/tty     /dev/tty     none    x-create=file,bind,rw,nosuid,noexec,private       0 -1
devpts       /dev/pts     devpts  x-create=dir,rw,nosuid,noexec,newinstance,ptmxmode=0666,mode=620,slave 0 -1
tmpfs        /var/run     tmpfs   x-create=dir,rw,nosuid,nodev,mode=755,slave       0 -1
tmpfs        /var/lock    tmpfs   x-create=dir,rw,nosuid,nodev,mode=755,slave       0 -1
/etc/hosts   /etc/hosts   none    x-create=file,bind,ro,nosuid,nodev,noexec,private 0 -1
"""  # noqa: E501 - an fstab line cannot be wrapped
# The symlinks of the minimal /dev, created after the mounts above.
DEV_LINKS_HOOK = """\
#!/bin/sh
# Written by hpc-sandbox: the symlinks of a minimal /dev.
set -eu
ln -s /proc/self/fd "${ENROOT_ROOTFS}/dev/fd"
ln -s /proc/self/fd/0 "${ENROOT_ROOTFS}/dev/stdin"
ln -s /proc/self/fd/1 "${ENROOT_ROOTFS}/dev/stdout"
ln -s /proc/self/fd/2 "${ENROOT_ROOTFS}/dev/stderr"
ln -s /dev/pts/ptmx "${ENROOT_ROOTFS}/dev/ptmx"
"""
DEV_LINKS_NAME = "10-hpc-sandbox-dev.sh"
# Hooks shipped with enroot that sandboxes use when the installation has them
# enabled: passwd and group entries for the mapped user, apt working as
# remapped root, and NVIDIA GPUs (it acts only with gpus set).
STANDARD_HOOKS = ("10-aptfix.sh", "10-shadow.sh", "98-nvidia.sh")
# Of those, the ones a restore runs. A restored rootfs was written by code in
# the sandbox, and the other hooks write into it with host-side shell code that
# follows its symlinks; their results are in the snapshot anyway. The NVIDIA
# hook defers to nvidia-container-cli, which is built to configure untrusted
# root filesystems: it resolves paths inside the rootfs and runs the rootfs's
# ldconfig inside the container.
RESTORE_HOOKS = ("98-nvidia.sh",)

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
    without a registry). A digest pin (``image[:tag]@sha256:<hex>``) becomes
    ``image:sha256:<hex>``: enroot 3.x reads ``@`` as a user name, but passes
    a tag to the registry as given, and a registry serves a manifest by digest
    where a tag would go. Raises ValueError for references enroot cannot import.
    """
    if reference.startswith(("dockerd://", "podman://")):
        return reference
    if "://" in reference and not reference.startswith(("docker://", "https://", "http://")):
        raise ValueError(f"enroot cannot import {reference!r}; use a docker:// reference")
    target = reference.split("://", 1)[1] if "://" in reference else reference
    digest = ""
    if match := DIGEST.search(target):
        digest, target = f":sha256:{match.group(1)}", target[: match.start()]
        name = max(target.rfind("/"), target.rfind("#")) + 1
        target = target[:name] + target[name:].split(":", 1)[0]  # the digest decides, not a tag
    if "@sha256:" in target:
        raise ValueError(f"invalid digest in {reference!r}: expected @sha256: and 64 hex digits")
    if target.endswith(".sif"):
        raise ValueError(f"{reference!r} is a SIF image; the enroot provider runs squashfs images")
    registry, separator, path = target.partition("#")
    if not separator:
        registry, separator, path = target.partition("/")
        if not separator or not ("." in registry or ":" in registry or registry == "localhost"):
            return f"docker://{target}{digest}"
    if registry in _DOCKER_HUB:
        return f"docker://{path}{digest}"
    return f"docker://{registry}#{path}{digest}"


class EnrootRuntime:
    """The enroot command line, run with environments and configuration this package controls.

    ``cache_dir`` and ``temp_dir`` hold layers and extraction space for
    imports unless the caller's environment already sets ``ENROOT_CACHE_PATH``
    or ``ENROOT_TEMP_PATH``. When containers start, ``config_dir`` is an empty
    ``ENROOT_CONFIG_PATH``, so user-level hooks, mounts and environment files
    never apply, and a system configuration written below ``sysconf_root``
    replaces the installation's; see :meth:`prepare_sysconf`. ``tools_dir``
    receives the whiteout converter wrapper.
    """

    def __init__(
        self,
        binary: str | None = None,
        *,
        cache_dir: Path,
        temp_dir: Path,
        config_dir: Path,
        sysconf_root: Path,
        tools_dir: Path,
        import_env: Sequence[str] = (),
    ) -> None:
        self.binary = binary
        self.cache_dir = cache_dir
        self.temp_dir = temp_dir
        self.config_dir = config_dir
        self.sysconf_root = sysconf_root
        self.sysconf_dir: Path | None = None
        self.restore_sysconf_dir: Path | None = None
        self.tools_dir = tools_dir
        self.import_env = tuple(import_env)
        self._path: str | None = None

    @property
    def path(self) -> str:
        if self._path is None:
            resolved = shutil.which(self.binary or "enroot")
            if resolved is None:
                raise SandboxError(f"enroot executable {self.binary or 'enroot'!r} is not on PATH")
            self._path = resolved
        return self._path

    def enroot_conf(self) -> Path:
        """The configuration file the enroot installation reads."""
        match = re.search(r'config::init\s+"([^"$]+)"', self._script())
        return Path(match.group(1)) if match else Path("/etc/enroot/enroot.conf")

    def setting(self, key: str) -> str | None:
        """The value enroot itself would use for ``key``, or None if it is unset or false.

        bash runs the configuration section of the installed ``enroot``
        script, without the directories it creates, so the environment,
        ``enroot.conf``, its drop-ins and enroot's defaults combine exactly as
        they do for enroot. Without that section, only the environment counts.
        """
        match = _CONFIG_SECTION.search(self._script())
        if match is None:
            return os.environ.get(key) or None
        section = "".join(
            line for line in match.group(1).splitlines(keepends=True) if "mkdir" not in line
        )
        script = (
            "hpc_sandbox_key=$1\nshift\nset -euo pipefail\n"
            f'{section}\nprintf "%s" "${{!hpc_sandbox_key-}}"\n'
        )
        try:
            process = subprocess.Popen(
                ["bash", "-c", script, "enroot-config", key],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError:
            return None
        try:
            output, _ = process.communicate(timeout=_SETTING_TIMEOUT_S)
        except subprocess.TimeoutExpired:  # a $(command) in enroot.conf hangs
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            assert process.stdout is not None
            process.stdout.close()
            return None
        value = output.decode(errors="replace")
        return value if process.returncode == 0 and value else None

    def site_sysconf(self) -> Path:
        """The installation's system configuration directory, found as enroot finds it.

        :meth:`setting`, else the default written in the enroot script (which
        depends on how enroot was installed).
        """
        if configured := self.setting("ENROOT_SYSCONF_PATH"):
            return Path(configured)
        match = re.search(r'config::export\s+ENROOT_SYSCONF_PATH\s+"([^"$]+)"', self._script())
        return Path(match.group(1)) if match else self.enroot_conf().parent

    def _script(self) -> str:
        try:
            return Path(self.path).read_text(errors="replace")
        except (OSError, SandboxError):
            return ""

    def prepare_sysconf(self, *, gpus: bool) -> list[str]:
        """Write the system configurations sandboxes start with; return the hooks skipped.

        Sites extend their enroot configuration for interactive jobs (home
        directories, cgroups, Slurm and PMIx variables, entrypoints, terminal
        settings) with things that do not belong in sandboxes. Sandboxes get
        :data:`SYSTEM_FSTAB`, no environment files, the ``/dev`` symlink hook
        and those :data:`STANDARD_HOOKS` the installation has enabled, copied
        from it; restored sandboxes only get :data:`RESTORE_HOOKS`. Each
        configuration is written once into a directory named after its
        content and never changed, so providers with other installations or
        versions sharing the scratch root cannot alter it. Raises
        SandboxError when ``gpus`` is set and the NVIDIA hook is missing.
        """
        source = self.site_sysconf() / "hooks.d"
        hooks = {}
        missing = []
        for name in STANDARD_HOOKS:
            path = source / name
            try:
                if not os.access(path, os.X_OK):
                    raise OSError("not executable")  # enroot skips such hooks too
                hooks[name] = path.read_bytes()
            except OSError:
                missing.append(name)
        if gpus and "98-nvidia.sh" in missing:
            raise SandboxError(
                f"gpus needs enroot's NVIDIA hook, but {source}/98-nvidia.sh is not enabled"
            )
        self.sysconf_dir = self._publish_sysconf(hooks)
        restore_hooks = {name: content for name, content in hooks.items() if name in RESTORE_HOOKS}
        self.restore_sysconf_dir = self._publish_sysconf(restore_hooks)
        return missing

    def _publish_sysconf(self, hooks: dict[str, bytes]) -> Path:
        files = {
            "mounts.d/10-system.fstab": (SYSTEM_FSTAB.encode(), 0o600),
            f"hooks.d/{DEV_LINKS_NAME}": (DEV_LINKS_HOOK.encode(), 0o700),
            **{f"hooks.d/{name}": (content, 0o700) for name, content in hooks.items()},
        }
        digest = hashlib.sha256()
        for name, (content, mode) in sorted(files.items()):
            digest.update(f"{name}\0{mode:o}\0{len(content)}\0".encode() + content)
        target = self.sysconf_root / digest.hexdigest()[:24]
        if target.is_dir():
            return target
        self.sysconf_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".partial-", dir=self.sysconf_root))
        try:
            for child in ("hooks.d", "mounts.d", "environ.d"):
                (staging / child).mkdir(mode=0o700)
            for name, (content, mode) in files.items():
                (staging / name).write_bytes(content)
                (staging / name).chmod(mode)
            staging.chmod(0o700)
            try:
                os.rename(staging, target)
            except OSError:
                if not target.is_dir():  # not just another process publishing the same
                    raise
        finally:
            if staging.exists():
                remove_tree(staging)
        return target

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
        """The environment of ``enroot import``: what pulling needs.

        Kept from the caller: ``PATH``, home and locale, XDG directories,
        proxies, CA bundles, ``SOURCE_DATE_EPOCH``, ``ENROOT_*`` settings, the
        Docker and Podman variables ``dockerd://`` and ``podman://`` imports
        use, every variable the registry credentials file names (enroot
        evaluates ``$VAR`` and ``$(command)`` password fields), and the names
        or ``PREFIX*`` patterns in ``import_env``. Batch-job, GPU and other
        variables are dropped. Layers are extracted with GNU tar;
        ``--delay-directory-restore`` lets images that revisit a read-only
        directory extract instead of failing with ``Permission denied``.
        """
        names = set(_IMPORT_ENV) | self.credential_variables()
        prefixes = _IMPORT_PREFIXES + tuple(p[:-1] for p in self.import_env if p.endswith("*"))
        names |= {name for name in self.import_env if not name.endswith("*")}
        env = {
            key: value
            for key, value in os.environ.items()
            if key in names or key.startswith(prefixes)
        }
        env.setdefault("ENROOT_CACHE_PATH", str(self.cache_dir))
        env.setdefault("ENROOT_TEMP_PATH", str(self.temp_dir))
        tar_options = env.get("TAR_OPTIONS", "").split()
        if "--delay-directory-restore" not in tar_options:
            env["TAR_OPTIONS"] = " ".join([*tar_options, "--delay-directory-restore"])
        wrapper = self.tools_dir / "enroot-aufs2ovlfs"
        real = shutil.which("enroot-aufs2ovlfs", path=env.get("PATH"))
        if real is not None and Path(real).resolve() != wrapper.resolve():
            self.tools_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            _publish(wrapper, AUFS2OVLFS_WRAPPER.encode(), 0o700)
            env["HPC_SANDBOX_REAL_AUFS2OVLFS"] = str(Path(real).resolve())
            env["PATH"] = os.pathsep.join([str(self.tools_dir), env.get("PATH", "")])
        return env

    def _sysconf(self, restored: bool) -> Path:
        directory = self.restore_sysconf_dir if restored else self.sysconf_dir
        if directory is None:
            raise SandboxError("the enroot system configuration has not been prepared")
        return directory

    def credential_variables(self) -> set[str]:
        """Names of the variables enroot's registry credentials file refers to."""
        configured = self.setting("ENROOT_CONFIG_PATH")
        config_home = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
        directory = Path(configured) if configured else Path(config_home, "enroot")
        try:
            text = (directory / ".credentials").read_text(errors="replace")
        except OSError:
            return set()
        return set(_VARIABLE_REFERENCE.findall(text))

    def container_environment(
        self, session: Session, *, remap_root: bool, gpus: str | None, restored: bool = False
    ) -> dict[str, str]:
        """The environment of ``enroot create`` and ``enroot start`` for ``session``.

        Only what enroot needs is passed, so batch-job and user variables never
        reach hooks. Boolean settings use explicit ``yes``/``no``: enroot
        treats an empty value as unset and falls back to the site default.
        Restored sandboxes start with the restore system configuration.
        """
        env = {
            key: os.environ[key]
            for key in ("PATH", "HOME", "USER", "LOGNAME", "ENROOT_LIBRARY_PATH")
            if key in os.environ
        }
        env.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
        env.update(
            ENROOT_DATA_PATH=str(session.path),
            ENROOT_RUNTIME_PATH=str(session.path / "runtime"),
            ENROOT_CACHE_PATH=str(self.cache_dir),
            ENROOT_TEMP_PATH=str(self.temp_dir),
            ENROOT_CONFIG_PATH=str(self.config_dir),
            ENROOT_SYSCONF_PATH=str(self._sysconf(restored)),
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
            SQUASHFS_SUFFIXES
        )

    def check(self, reference: str) -> None:
        if not self.is_local(reference):
            import_uri(reference)  # raises ValueError for what enroot cannot import
            return
        path = local_path(reference)
        if path.suffix == ".sif" or is_sif(path):
            raise ValueError(
                f"{reference!r} is a SIF image; the enroot provider runs squashfs images"
            )

    def pull_uri(self, reference: str) -> str:
        return import_uri(reference)

    async def validate(self, path: Path) -> bool:
        return file_magic(path, 0, len(SQUASHFS_MAGIC)) == SQUASHFS_MAGIC

    async def pull(self, uri: str, target: Path, *, timeout_s: float) -> ProcessResult:
        # Reading enroot's settings runs bash, which can take a while.
        env = await asyncio.to_thread(self.runtime.import_environment)
        return await self.runtime.run(
            "import", "--output", str(target), uri, env=env, timeout_s=timeout_s
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

    The :class:`~hpc_sandbox.singularity.SingularityProvider` option names are
    accepted too, so one set of options works for both: ``gpu="nv"`` means
    ``gpus="all"``; ``allow_network_none=False`` refuses ``network="none"``,
    which is otherwise always available because every sandbox gets its own
    network namespace; ``stop_grace_s`` is ignored because sandboxes are
    stopped with SIGKILL, and ``rootfs_templates`` because ``enroot create``
    extracts each sandbox's root filesystem from its image, which is the copy
    layout (``rootfs_layout="copy"``). ``gpu="rocm"``, ``cgroup_limits=True``,
    ``rootfs_layout="fuse-overlay"`` and ``fakeroot=True`` (enroot maps only
    your uid, so root cannot switch users) cannot be honored and raise
    ValueError, as does a setting given under both names.

    Pods (:meth:`start_pod`) find slirp4netns for egress at ``slirp4netns``,
    else on PATH, else as fetched by ``hpc-sandbox fetch-tools``; their
    services get ``service_stop_grace_s`` between SIGTERM and SIGKILL when
    their pod stops. ``gvisor``, a :class:`~hpc_sandbox.gvisor.GvisorProvider`,
    runs the services whose spec says ``runtime="gvisor"`` in gVisor, with its
    SIF images, and adds the ``gvisor_services`` capability.
    """

    network_none_hint = "network 'none' was turned off with allow_network_none=False"
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
        bind_resolv_conf: bool = True,
        import_env: Sequence[str] = (),
        extra_start_args: Sequence[str] = (),
        output_limit_bytes: int = DEFAULT_OUTPUT_LIMIT,
        memory_poll_s: float = 1.0,
        gpu: Literal["nv", "rocm"] | None = None,
        allow_network_none: bool | None = None,
        stop_grace_s: float | None = None,
        cgroup_limits: bool = False,
        slirp4netns: str | None = None,
        service_stop_grace_s: float = 0.0,
        rootfs_templates: bool | None = None,
        rootfs_layout: Literal["copy", "fuse-overlay"] | None = None,
        fakeroot: bool = False,
        gvisor: GvisorProvider | None = None,
    ) -> None:
        if gpu is not None:
            if gpus is not None:
                raise ValueError("give gpus or gpu, not both")
            if gpu != "nv":
                raise ValueError(f"the enroot provider passes NVIDIA GPUs only, not gpu={gpu!r}")
            gpus = "all"
        if cgroup_limits:
            raise ValueError(
                "the enroot provider cannot apply cgroup limits; memory_mb arms the memory watchdog"
            )
        if rootfs_layout not in (None, "copy"):
            raise ValueError(
                "the enroot provider extracts each sandbox's root filesystem with enroot create "
                f"(rootfs_layout='copy'), not rootfs_layout={rootfs_layout!r}"
            )
        if fakeroot:
            raise ValueError(
                "the enroot provider maps only your uid into a sandbox, so root there cannot "
                "switch users as with Singularity's fakeroot; remap_root gives root without them"
            )
        del stop_grace_s  # SIGKILL needs no grace period
        del rootfs_templates  # enroot create extracts every sandbox's root filesystem
        super().__init__(
            scratch_root=scratch_root,
            default_timeout_s=default_timeout_s,
            start_timeout_s=start_timeout_s,
            stop_timeout_s=stop_timeout_s,
            output_limit_bytes=output_limit_bytes,
            memory_poll_s=memory_poll_s,
        )
        self.sandbox_type = EnrootSandbox
        self.service_type = EnrootService
        self.slirp4netns_path = slirp4netns
        self.service_stop_grace_s = service_stop_grace_s
        self.runtime = EnrootRuntime(
            binary,
            cache_dir=self.scratch_root / "runtime-cache",
            temp_dir=self.scratch_root / "runtime-tmp",
            config_dir=self.scratch_root / "enroot-config",
            sysconf_root=self.scratch_root / "enroot-sysconf",
            tools_dir=self.scratch_root / "enroot-tools",
            import_env=import_env,
        )
        cache = Path(image_cache) if image_cache else default_image_cache()
        if stage_images is None:
            stage_images = should_stage(cache, self.scratch_root)
        self.images = ImageStore(
            SquashfsImages(self.runtime),
            cache,
            stage_dir=self.scratch_root / "images" if stage_images else None,
            pull_timeout_s=start_timeout_s,
            prepare=self._make_scratch_directories,
        )
        self.remap_root = remap_root
        self.gpus = gpus
        self.bind_resolv_conf = bind_resolv_conf
        self.extra_start_args = tuple(extra_start_args)
        self.capabilities = frozenset(
            {"snapshot", "isolation", "pods"}
            | ({"network_none"} if allow_network_none is not False else set())
            | ({"gpu"} if gpus else set())
        )
        self._take_gvisor(gvisor)
        self._sysconf: asyncio.Future[None] | None = None

    def _session_roots(self) -> list[tuple[Path, bool]]:
        # hpc-sandbox 0.1.0 kept enroot sessions in the Singularity provider's
        # directory: sweep the ones recognizably ours there too, and those
        # nobody can tell apart when no Singularity janitor could take them.
        singularity = any(shutil.which(binary) for binary in RUNTIME_CANDIDATES)
        return [*super()._session_roots(), (self.scratch_root / "sessions", not singularity)]

    def _check(self, spec: SandboxSpec, *, service: bool = False) -> None:
        super()._check(spec, service=service)
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

    async def _prepare(self) -> None:
        await super()._prepare()
        task = self._sysconf
        if task is None or (task.done() and (task.cancelled() or task.exception())):
            task = self._sysconf = asyncio.ensure_future(self._prepare_sysconf())
        await asyncio.shield(task)

    async def _prepare_sysconf(self) -> None:
        """Write the system configurations once per provider."""
        missing = await asyncio.to_thread(self.runtime.prepare_sysconf, gpus=bool(self.gpus))
        for name in missing:
            log.info("enroot hook %s is not enabled; sandboxes start without it", name)

    async def _copy_snapshot(self, source: Path, session: Session) -> None:
        await super()._copy_snapshot(source, session)
        if (source / ENVIRONMENT_FILE).is_file():
            await asyncio.to_thread(
                shutil.copyfile, source / ENVIRONMENT_FILE, session.path / ENVIRONMENT_FILE
            )

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

    def _container_pid(self) -> int | None:
        return self._init_pid  # the launcher still sees the host's mounts

    def _prepare_rootfs(self) -> None:
        assert self._session is not None
        session = self._session
        for directory in (session.tmp, session.var_tmp):
            directory.mkdir(exist_ok=True)
            directory.chmod(0o1777)
        rootfs = session.rootfs
        # enroot and its hooks write below these from the host side.
        for name in ("etc", "dev"):
            path = rootfs / name
            if path.is_symlink() or (path.exists() and not path.is_dir()):
                raise SandboxStartError(f"/{name} in the sandbox's rootfs is not a directory")
        lock = rootfs / ".lock"  # enroot opens it for writing before it enters the rootfs
        if lock.is_symlink() or (lock.exists() and not lock.is_file()):
            _remove(lock)
        home = rootfs / "root"  # HOME defaults to /root
        if not home.exists() and not home.is_symlink():
            home.mkdir()
        # A restore keeps its snapshot's time zone, unless the snapshot predates
        # recorded environments: sandboxes then got the zone at run time.
        if not self._restored or not (session.path / ENVIRONMENT_FILE).is_file():
            install_time_zone(rootfs)
        # Run the command as given: skip the image's entrypoint and rc.local.
        (session.path / RC_FILE).write_text('exec "$@"\n')

    def _prepare_snapshot(self) -> None:
        assert self._session is not None
        make_placeholders_readable(self._session.rootfs)

    def _snapshot_files(self, target: Path) -> None:
        # The environment a restore gives its commands: not the rootfs's
        # /etc/environment, which code in the sandbox may have rewritten.
        (target / ENVIRONMENT_FILE).write_text(json.dumps(self._environment, indent=2) + "\n")

    def _hide_rootfs_config(self) -> list[tuple[Path, Path]]:
        """Set aside the files of a restored rootfs that enroot reads from the host side.

        enroot mounts what ``/etc/fstab`` lists and loads ``/etc/environment``
        before it enters the rootfs; in a restored rootfs, code from the
        sandbox may have written both.
        """
        assert self._session is not None
        etc = self._session.rootfs / "etc"
        token = uuid.uuid4().hex
        hidden = []
        for name in ("fstab", "environment"):
            path = etc / name
            if os.path.lexists(path):
                aside = etc / f".hpc-sandbox-{name}-{token}"
                os.rename(path, aside)
                hidden.append((aside, path))
        return hidden

    @staticmethod
    def _put_back(hidden: list[tuple[Path, Path]]) -> None:
        for aside, path in hidden:
            if os.path.lexists(path):
                _remove(path)
            os.rename(aside, path)

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
        resolv_conf = Path("/etc/resolv.conf")
        if self.spec.network == "host" and provider.bind_resolv_conf and resolv_conf.is_file():
            mounts.append(
                f"{resolv_conf.resolve()}:/etc/resolv.conf:none:"
                "x-create=file,bind,ro,nosuid,nodev,noexec"
            )
        mounts += self._spec_mounts()
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

    def _spec_mounts(self) -> list[str]:
        mounts = []
        for mount in self.spec.mounts:
            source = Path(mount.source).resolve()
            kind = "dir" if source.is_dir() else "file"
            mode = "ro" if mount.read_only else "rw"
            mounts.append(f"{source}:{mount.target}:none:x-create={kind},bind,{mode},nosuid")
        return mounts

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
            session, remap_root=provider.remap_root, gpus=provider.gpus, restored=self._restored
        )
        hidden = await asyncio.to_thread(self._hide_rootfs_config) if self._restored else []
        await self._launch_anchor(command, environment)
        # Only now that enroot is done with them; a failed start deletes the session.
        if hidden:
            await asyncio.to_thread(self._put_back, hidden)
        anchor = self._anchor
        assert anchor is not None
        children = procfs.children(anchor.pid)
        if len(children) != 1:
            raise SandboxStartError(f"unexpected process tree below the launcher: {children}")
        (init,) = children
        started = procfs.owned_live_start_time(init)
        if started is None or procfs.namespace_pid(init) != 1:
            raise SandboxStartError("could not prove the identity of the container's init")
        init_environment = procfs.environ(init)
        if init_environment is None:
            raise SandboxStartError("could not read the container's environment")
        self._init_pid, self._init_start_time = init, started
        self._environment = {
            key: value for key, value in init_environment.items() if key not in _SHELL_STATE
        }
        recorded = session.path / ENVIRONMENT_FILE
        if self._restored and recorded.is_file():
            data = json.loads(recorded.read_text())
            self._environment = {str(key): str(value) for key, value in data.items()}
        elif self._restored:  # a snapshot from before environments were recorded
            self._environment.setdefault("PATH", _HOST_PATH)
        self._record()

    async def _launch_anchor(self, command: list[str], environment: dict[str, str]) -> None:
        """Start the launcher and wait until the container's keep-alive loop runs."""
        assert self._session is not None
        provider = self._provider
        log_path = self._session.path / ANCHOR_LOG
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
        command: list[str],
        *,
        stdin: bytes | None,
        timeout_s: float,
        on_start: Callable[[int], None],
    ) -> ProcessResult:
        # Re-prove the init's identity: nsenter must never enter a recycled PID.
        if not self._container_alive():
            self._mark_lost("the container's init process is gone")
        # The container's environment reaches the command through the prologue's
        # stdin, never the host-side nsenter processes (think LD_PRELOAD).
        return await run_process(
            [*self._enter_args(), *command],
            timeout_s=timeout_s,
            stdin=stdin,
            env={"PATH": _HOST_PATH},
            output_limit=self._output_limit,
            on_start=on_start,
        )

    def _full_environment(self, env: Mapping[str, str]) -> dict[str, str]:
        return {**self._environment, **env}

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
                signal_kill(init)  # its parent, the launcher, reaps it and exits
            if await poll(anchor.poll, _KILL_WAIT_S) is None:
                anchor.kill()
            if await poll(anchor.poll, self._provider.stop_timeout_s) is None:
                raise SandboxError(f"the launcher of sandbox {self.id} survived SIGKILL")
        if self._init_pid is not None and self._container_alive():
            raise SandboxError(f"the container init of sandbox {self.id} survived teardown")
        self._anchor = None


class EnrootService(PodService, EnrootSandbox):
    """A pod service as an enroot container; start it with ``pod.start_service``.

    Enroot's own user namespace, nested in the pod's, is where root lives, so
    commands in the service hold no capability over the pod's network. With
    ``run_as="caller"``, the launcher nests a user namespace that maps your uid
    and gid to the pod's root, and enroot runs without ``--root`` in it, so
    commands run as your uid. Enroot maps one id into a container, so it cannot
    run ``"subids"`` services.
    """

    run_modes = ("root", "caller")

    def _userns_options(self) -> list[str]:
        if self.service.run_as == "caller":
            return ["--userns", f"caller={os.getuid()}:{os.getgid()}"]
        return []  # enroot's own user namespace holds root

    @property
    def _remap_root(self) -> bool:
        return self._provider.remap_root and self.service.run_as == "root"

    def _runtime_command(self, argv: list[str]) -> list[str]:
        assert self._session is not None
        session, provider = self._session, self._provider
        args = ["start", "--rw", *(["--root"] if self._remap_root else [])]
        args += ["--rc", str(session.path / RC_FILE)]
        mounts = [
            f"{session.tmp}:/tmp:none:x-create=dir,bind,rw,nosuid",
            f"{session.var_tmp}:/var/tmp:none:x-create=dir,bind,rw,nosuid",
            f"{self.shm_path}:/dev/shm:none:x-create=dir,bind,rw,nosuid,nodev",
            f"{self.hosts_path}:/etc/hosts:none:x-create=file,bind,ro,nosuid,nodev,noexec",
            f"{self.resolv_path}:/etc/resolv.conf:none:x-create=file,bind,ro,nosuid,nodev,noexec",
            *self._spec_mounts(),
        ]
        for mount in mounts:
            args += ["--mount", mount]
        return [
            provider.runtime.path,
            *args,
            *provider.extra_start_args,
            session.rootfs.name,
            *argv,
        ]

    def _runtime_environment(self) -> dict[str, str]:
        assert self._session is not None
        return self._provider.runtime.container_environment(
            self._session, remap_root=self._remap_root, gpus=None
        )


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


def install_time_zone(rootfs: Path, source: Path = Path("/etc/localtime")) -> None:
    """Give ``rootfs`` the host's time zone as a regular ``/etc/localtime``.

    Images usually have a symlink there into their own zoneinfo database; it
    is replaced, never written through. Nothing happens without an ``/etc``
    directory in the rootfs or a time zone on the host.
    """
    try:
        zone = source.read_bytes()
    except OSError:
        return
    try:
        etc = os.open(rootfs / "etc", safefs.DIRECTORY)
    except OSError:
        return
    try:
        try:
            info = os.stat("localtime", dir_fd=etc, follow_symlinks=False)
        except FileNotFoundError:
            info = None
        if info is not None and stat.S_ISDIR(info.st_mode):
            return
        if info is not None:
            os.unlink("localtime", dir_fd=etc)
        safefs.write_file("localtime", etc, zone, mode=0o644)
    finally:
        os.close(etc)


def _publish(path: Path, content: bytes, mode: int) -> None:
    """Atomically write ``content`` to ``path`` unless it already holds it."""
    try:
        if path.read_bytes() == content and stat.S_IMODE(path.stat().st_mode) == mode:
            return
    except OSError:
        pass
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}")
    temporary.write_bytes(content)
    temporary.chmod(mode)
    os.replace(temporary, path)


def _remove(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        remove_tree(path)
    else:
        path.unlink()


def _read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(errors="replace").splitlines()
    except OSError:
        return []


def _tail(path: Path, lines: int = 20) -> str:
    return "\n".join(_read_lines(path)[-lines:]).strip() or "no output"
