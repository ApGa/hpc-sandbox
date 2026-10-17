"""Helper programs pods use: the host's, or a pinned release fetched once.

Pods reach the internet through slirp4netns. It is taken, in this order, from
an explicit path, from ``PATH`` when it supports what pods need, or from a
pinned static release that ``hpc-sandbox fetch-tools`` downloads once (compute
nodes may have no internet) and checks against its SHA-256 digest. The site's
package gets its distribution's fixes; the pinned one only when the pin moves.
"""

from __future__ import annotations

import hashlib
import logging
import os
import platform
import shutil
import subprocess
import tempfile
import urllib.request
from pathlib import Path

from .types import SandboxError

log = logging.getLogger(__name__)

SLIRP4NETNS_VERSION = "1.3.6"
SLIRP4NETNS_SHA256 = {
    "x86_64": "411167abceff1fc8a7c562f53ed9f26d3ec8a17df4e2fe6eaa3bd53f1c1e1d48",
    "aarch64": "045f85dd0bc51a61b99cdfe6bd22282cbc308b73caa64fc8859c11d3775c5817",
}
SLIRP4NETNS_URL = (
    "https://github.com/rootless-containers/slirp4netns/releases/download/"
    "v{version}/slirp4netns-{arch}"
)
# What pods ask of slirp4netns (all there since 0.4).
SLIRP4NETNS_FLAGS = (
    "--disable-host-loopback",
    "--enable-sandbox",
    "--enable-seccomp",
    "--exit-fd",
    "--ready-fd",
)
_HELP_TIMEOUT_S = 10.0
_FETCH_TIMEOUT_S = 120.0
# Checks already made, by path, modification time and size.
_checked: dict[tuple[str, int, int], bool] = {}


def tools_dir() -> Path:
    """Where fetched tools live: ``HPC_SANDBOX_TOOLS``, else the XDG cache."""
    if configured := os.environ.get("HPC_SANDBOX_TOOLS"):
        return Path(configured)
    cache = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
    return cache / "hpc-sandbox" / "tools"


def pinned_slirp4netns(arch: str | None = None) -> Path:
    arch = arch or platform.machine()
    return tools_dir() / f"slirp4netns-{SLIRP4NETNS_VERSION}-{arch}"


def find_slirp4netns(explicit: str | None = None) -> str:
    """The slirp4netns to run: ``explicit``, the one on PATH if it has what pods
    need, or the pinned release fetched by ``hpc-sandbox fetch-tools``."""
    if explicit is not None:
        path = shutil.which(explicit)
        if path is None:
            raise SandboxError(f"slirp4netns {explicit!r} is not an executable")
        found = path
    elif (on_path := shutil.which("slirp4netns")) and supports_pods(on_path):
        found = on_path
    else:
        pinned = pinned_slirp4netns()
        if not pinned.is_file():
            raise SandboxError(
                "pods with egress need slirp4netns: install it, pass its path as "
                "slirp4netns=, or run `hpc-sandbox fetch-tools` once (it downloads "
                f"slirp4netns {SLIRP4NETNS_VERSION} to {pinned.parent})"
            )
        check_digest(pinned)
        found = str(pinned)
    return found


def supports_pods(binary: str) -> bool:
    """Whether ``binary --help`` lists every flag pods use."""
    key = _key(binary)
    if key in _checked:
        return _checked[key]
    try:
        result = subprocess.run(
            [binary, "--help"],
            capture_output=True,
            timeout=_HELP_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    text = (result.stdout + result.stderr).decode(errors="replace")
    _checked[key] = all(flag in text for flag in SLIRP4NETNS_FLAGS)
    return _checked[key]


def _key(path: str | Path) -> tuple[str, int, int]:
    try:
        info = os.stat(path)
    except OSError:
        return (str(path), -1, -1)
    return (str(path), info.st_mtime_ns, info.st_size)


def check_digest(path: Path, arch: str | None = None) -> None:
    arch = arch or platform.machine()
    expected = SLIRP4NETNS_SHA256.get(arch)
    if expected is None:
        raise SandboxError(f"no pinned slirp4netns for {arch}")
    key = _key(path)
    if _checked.get(key):
        return
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != expected:
        raise SandboxError(f"{path} has SHA-256 {digest}, not the pinned {expected}")
    _checked[key] = True


def fetch_slirp4netns(arch: str | None = None) -> Path:
    """Download the pinned slirp4netns for ``arch`` into :func:`tools_dir`, once."""
    arch = arch or platform.machine()
    if arch not in SLIRP4NETNS_SHA256:
        raise SandboxError(f"no pinned slirp4netns for {arch}")
    target = pinned_slirp4netns(arch)
    if target.is_file():
        check_digest(target, arch)
        return target
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    url = SLIRP4NETNS_URL.format(version=SLIRP4NETNS_VERSION, arch=arch)
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".partial-", delete=False) as out:
        partial = Path(out.name)
        try:
            with urllib.request.urlopen(url, timeout=_FETCH_TIMEOUT_S) as response:
                shutil.copyfileobj(response, out)
        except BaseException:
            partial.unlink(missing_ok=True)
            raise
    try:
        check_digest(partial, arch)
        partial.chmod(0o755)
        os.replace(partial, target)
    finally:
        partial.unlink(missing_ok=True)
    return target
