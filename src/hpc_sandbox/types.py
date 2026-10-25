"""Public data types: the sandbox contract every provider implements.

:class:`SandboxProvider` and :class:`Sandbox` are structural protocols, so
callers can type against them without depending on a particular runtime, and
adapters to other sandbox interfaces only need these field and method names.
"""

from __future__ import annotations

from collections.abc import Mapping
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

    ``image`` is a remote reference (``docker://repo:tag``, or another one the
    runtime can pull), which is pulled once into the image cache, or a local
    image file: ``.sif`` for Singularity/Apptainer, ``.sqsh`` for enroot. An
    ``@sha256:<hex>`` suffix pins it: to a remote image's digest, or to a local
    file's SHA-256. ``cpus`` is advisory unless the provider enables cgroup
    limits; ``memory_mb`` arms the PSS watchdog. ``labels`` (a run, a task,
    ...) select sandboxes for janitors.
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


PodNetwork = Literal["none", "egress"]
ServiceHealth = Literal["starting", "healthy", "unhealthy", "exited"]


@dataclass
class HealthCheck:
    """A docker-compose healthcheck, run in the service like ``exec``.

    ``test`` is ``["CMD", program, arg...]``, ``["CMD-SHELL", command]`` or a
    shell command. A check that exits 0 within ``timeout_s`` passes. The
    service is healthy after a pass, and unhealthy after ``retries`` failures in
    a row, not counting failures within ``start_period_s`` of the start, during
    which checks run every ``start_interval_s`` instead of ``interval_s``.
    """

    test: list[str] | str
    interval_s: float = 30.0
    timeout_s: float = 30.0
    retries: int = 3
    start_period_s: float = 0.0
    start_interval_s: float = 5.0


@dataclass
class ServiceSpec:
    """One service of a pod.

    ``sandbox`` gives its image, workdir, environment, mounts and limits (its
    ``network`` is ignored: services share the pod's). ``command`` is the full
    argv to run; without one the service keeps running idle, like a sandbox,
    for ``exec`` (the agent's ``main`` service). ``name`` resolves to 127.0.0.1
    in every service and is the service's hostname. ``ulimits`` maps resource
    names (``nofile``, ``stack``...) to ``(soft, hard)``, ``-1`` for unlimited,
    within the hard limits of the host process; ``nproc`` and ``locks`` are
    accepted but not applied (``nproc`` would count every process of the pod,
    and under Docker it does not bind root either).
    """

    name: str
    sandbox: SandboxSpec
    command: list[str] | None = None
    healthcheck: HealthCheck | None = None
    shm_mb: int | None = None
    ulimits: dict[str, tuple[int, int]] = field(default_factory=dict)


@dataclass
class PodSpec:
    """A group of services sharing one network, like a docker-compose project.

    ``network`` is ``"none"`` (loopback only) or ``"egress"`` (outbound access
    through slirp4netns, never to the host's loopback). Each upstream
    (``{"gateway": "unix:///path/gw.sock"}``) is reachable in every service at
    ``127.0.0.1:<port>``, by default 18080, 18081... in order
    (``upstream_ports`` overrides), relayed to the host's Unix socket.
    ``labels`` select pods for :meth:`reap` and janitors. Each port in
    ``exposes`` is reachable from the host through a Unix socket in the pod's
    directory (:attr:`SessionPod.endpoints`), and every service resolves each
    name in ``hosts`` to 127.0.0.1, as it resolves the services' names
    (``"judge"`` for an upstream that a pod port relays).
    """

    network: PodNetwork = "none"
    upstreams: dict[str, str] = field(default_factory=dict)
    upstream_ports: dict[str, int] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)
    exposes: list[int] = field(default_factory=list)
    hosts: list[str] = field(default_factory=list)


@runtime_checkable
class Pod(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def routes(self) -> Mapping[str, str]: ...  # {"gateway": "http://127.0.0.1:18080"}

    @property
    def services(self) -> Mapping[str, Sandbox]: ...

    async def start_service(
        self, spec: ServiceSpec, wait: Literal["started", "healthy"] = "started"
    ) -> Sandbox: ...

    async def health(self, name: str) -> ServiceHealth: ...

    async def logs(self, name: str) -> str: ...

    async def set_network(self, mode: PodNetwork) -> None: ...

    async def stop(self) -> None: ...


@runtime_checkable
class PodProvider(Protocol):
    """A provider with the ``pods`` capability."""

    name: str
    capabilities: frozenset[str]

    async def start_pod(self, spec: PodSpec) -> Pod: ...


class SandboxError(RuntimeError):
    """The sandbox infrastructure failed (as opposed to the command in it)."""


class SandboxStartError(SandboxError):
    """A sandbox could not be started; nothing it created was left behind."""


class SandboxLostError(SandboxError):
    """The container behind a running sandbox died."""
