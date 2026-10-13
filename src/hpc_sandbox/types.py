"""Public data types, mirroring platoon's sandbox contract (PLAN-v1 section 4.2).

hpc-sandbox never imports platoon. platoon's ``singularity`` provider adapts
these classes to its own identically named ones, so the field names and method
signatures here must stay in step with ``platoon/sandbox/base.py``.
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

    async def download(self, src: str, dst: str | Path) -> None: ...

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
