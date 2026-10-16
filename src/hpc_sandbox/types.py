"""Public data types: the sandbox contract every provider implements.

:class:`SandboxProvider` and :class:`Sandbox` are structural protocols, so
callers can type against them without depending on a particular runtime, and
adapters to other sandbox interfaces only need these field and method names.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable

Network = Literal["host", "none"]


@dataclass
class Mount:
    """A host path bound into the sandbox at ``target``."""

    source: str
    target: str
    read_only: bool = True


@dataclass
class SandboxSpec:
    """What to start: an image plus the per-sandbox runtime settings.

    ``image`` is a ``docker://`` (or other OCI/ORAS) reference, which is pulled
    once into the image cache, or a local ``.sif`` path. ``cpus`` is advisory
    unless the provider enables cgroup limits; ``memory_mb`` arms the PSS
    watchdog. ``labels`` (rollout id, run, ...) select sandboxes for janitors.
    """

    image: str
    workdir: str = "/app"
    env: dict[str, str] = field(default_factory=dict)
    mounts: list[Mount] = field(default_factory=list)
    network: Network = "host"
    cpus: float | None = None
    memory_mb: int | None = None
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class ExecResult:
    """The outcome of one command; ``exit_code`` is 124 when it timed out."""

    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool = False


# How a command that could not enter its working directory reports it: exit
# status CWD_ERROR_EXIT_CODE (126, as with `docker exec`), empty stdout, and on
# stderr only the line CWD_ERROR_PREFIX + "<directory>: <reason>", the reason
# being one of CWD_ERROR_REASONS.
CWD_ERROR_EXIT_CODE = 126
CWD_ERROR_PREFIX = "hpc-sandbox: cannot enter the working directory "
CWD_ERROR_REASONS = (
    "it does not exist or cannot be reached",
    "not a directory",
    "permission denied",
)


@dataclass(frozen=True)
class DroppedLink:
    """A symlink a download left out, so that host tools never follow it.

    ``path`` is relative to the download's destination, ``target`` the link's
    text, and ``reason`` one of ``"absolute"``, ``"absolute into the tree"``
    (absolute, but naming something inside the downloaded tree as the
    sandbox sees it), ``"escapes with .."`` (its leading ``..`` climb above
    the destination) or ``"climbs after a name"`` (``a/..`` leads wherever
    ``a`` does).
    """

    path: str
    target: str
    reason: str


@dataclass
class DownloadReport:
    """What a download left out: at most ``DROPPED_LINKS_SHOWN`` links, and how many in all."""

    dropped_links: list[DroppedLink] = field(default_factory=list)
    dropped_total: int = 0


DROPPED_LINKS_SHOWN = 100


@dataclass
class SandboxSnapshot:
    """A point-in-time copy of a sandbox's filesystem state.

    Snapshots capture files (the writable rootfs and the private ``/tmp`` and
    ``/var/tmp``), never processes. ``path`` is a host directory on the node
    that took the snapshot, so it can only be restored there.
    """

    id: str
    provider: str
    path: str
    image: str
    metadata: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class Sandbox(Protocol):
    id: str

    async def exec(
        self,
        cmd: str | list[str],
        *,
        timeout_s: float | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        stdin: bytes | None = None,
    ) -> ExecResult: ...

    async def upload(self, src: str | Path, dst: str) -> None: ...

    async def download(self, src: str, dst: str | Path) -> DownloadReport: ...

    async def snapshot(self) -> SandboxSnapshot: ...

    async def stop(self) -> None: ...


@runtime_checkable
class SandboxProvider(Protocol):
    name: str
    capabilities: frozenset[str]

    async def start(self, spec: SandboxSpec) -> Sandbox: ...

    async def restore(self, snapshot: SandboxSnapshot, spec: SandboxSpec) -> Sandbox: ...


class SandboxError(RuntimeError):
    """The sandbox infrastructure failed (as opposed to the command in it)."""


class SandboxStartError(SandboxError):
    """A sandbox could not be started; nothing it created was left behind."""


class SandboxLostError(SandboxError):
    """The instance behind a running sandbox died."""
