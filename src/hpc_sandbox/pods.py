"""Pods: services sharing one network, like a docker-compose project.

A pod is a holder process (:mod:`hpc_sandbox.holder`) that owns a user
namespace mapping the caller to root (and, with ``users="subids"``, the
caller's subordinate ids to the users after it) and a network namespace with
only an up loopback, plus one container per service:

- **Services.** Each service is a sandbox whose container runs under the
  launcher's ``join`` form (:mod:`hpc_sandbox.anchor`): it joins the pod's user
  and network namespaces, gets mount, PID, IPC and UTS namespaces of its own
  (compose's defaults), its hostname, resource limits and a sized
  ``/dev/shm``, and an init process of ours as PID 1 that runs the runtime as
  its child, passes SIGTERM on and exits with the command's status. The
  runtime puts the container in a nested user namespace, where root holds no
  capability over the pod's network: it can neither reconfigure it nor sniff
  it, as under Docker.
- **Users.** A service's ``run_as`` says what that nested namespace maps:
  root alone (``"root"``), the caller's own uid and gid, not root
  (``"caller"``), or root and every subordinate id the pod maps
  (``"subids"``). What a ``subids`` service writes as another user belongs to
  a subordinate id on the host, which the caller cannot delete, so it is given
  back to the caller (:func:`give_back`) before the files are deleted. The
  launcher of a ``subids`` service whose spec has a file owners manifest gives
  the image's paths their owners before the container and its command start.
- **Names.** Every service resolves the names of all services, its own
  hostname and the pod's host aliases to 127.0.0.1 through an ``/etc/hosts``
  of its own, extended as services join; ``/etc/resolv.conf`` names
  slirp4netns's resolver.
- **Commands** enter a service in two steps: the pod's user and network
  namespaces, then the container's own namespaces, so that they end as root
  of the container's nested user namespace.
- **Upstreams.** The holder relays a pod port per upstream to a Unix socket
  on the host. It binds before any service starts, without ``SO_REUSEPORT``,
  so no service can take the port over.
- **Exposed ports** go the other way: the holder listens on a Unix socket per
  exposed port in the pod's directory, ``exposed/<port>.sock``, which only the
  caller can reach, and forwards each connection to the port on the pod's
  loopback, at 127.0.0.1 or else ::1.
- **Egress** goes through slirp4netns with ``--disable-host-loopback``, whose
  exit pipe only the holder holds: it dies with the holder.
- **Lifetime.** The holder, the launchers and their inits die with the
  process that started them; the pod's session directory holds the services'
  session directories, so the janitor removes pods whole, giving back what a
  ``subids`` pod's other users left first.
- **gVisor services.** A service with ``runtime="gvisor"`` runs in gVisor,
  through the ``gvisor`` option of the pod's provider, in the pod's network
  (:class:`hpc_sandbox.gvisor.GvisorService`); the other services run as
  above.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import resource
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import weakref
from collections.abc import AsyncIterator, Callable, Container, Coroutine, Mapping
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self, get_args

from . import procfs, subids
from .anchor import FILE_OWNERS_REPORT
from .process import kill_tree, run_process
from .sandbox import SessionSandbox, check_env, check_file_owners, environment_script, poll
from .sessions import Session, create_session, new_instance_name, trash_session_awaiting
from .types import (
    GUEST_CGROUPS,
    HealthCheck,
    PodNetwork,
    PodSpec,
    SandboxError,
    SandboxLostError,
    SandboxSnapshot,
    SandboxStartError,
    ServiceHealth,
    ServiceRuntime,
    ServiceSecurebits,
    ServiceSpec,
)

if TYPE_CHECKING:
    from .sandbox import SessionProvider

log = logging.getLogger(__name__)

HOLDER_SCRIPT = Path(__file__).with_name("holder.py")
ANCHOR_SCRIPT = Path(__file__).with_name("anchor.py")
SERVICES_DIRECTORY = "services"
EXPOSED_DIRECTORY = "exposed"
HOLDER_LOG = "holder.log"
SLIRP_LOG = "slirp4netns.log"
SERVICE_LOG = "service.log"
FIRST_UPSTREAM_PORT = 18080
DEFAULT_SHM_MB = 64  # Docker's default /dev/shm
NAMESERVER = "10.0.2.3"  # slirp4netns's DNS forwarder
LOG_TAIL_BYTES = 1 << 20
READY = "hpc-sandbox-ready"
# Run by /bin/sh -c as the service's container command, with its name, the
# umask that a missing working directory is made with (empty: the service's
# own), the working directory and the command as arguments: announce that the
# container is up, take the environment from the first line of stdin (see
# environment_script), enter the working directory, creating it as Docker
# does, and run the command with stdin from /dev/null.
SERVICE_START = """\
echo hpc-sandbox-ready
hpc_sandbox_nl='
'
IFS= read -r hpc_sandbox_env || exit 125
eval "$hpc_sandbox_env" || exit 125
exec </dev/null
( if [ -n "$1" ]; then umask "$1"; fi; mkdir -p -- "$2" ) 2>/dev/null
cd -- "$2" || exit 125
shift 2
exec "$@"
"""
# The command of a service without one: it stays up, idle, for exec. It is the PID 1 of its PID
# namespace where the runtime starts no init for it (Singularity's --no-init), so that a kill of
# every process from a command spares it, and it forks nothing that such a kill would leave it to
# start again. As PID 1 it reaps no orphan, as Docker's "sleep infinity" does not.
SERVICE_KEEPALIVE = """\
trap '' HUP INT QUIT TERM USR1 USR2
command -v sleep >/dev/null 2>&1 || { echo "hpc-sandbox: the image has no sleep" >&2; exit 127; }
exec >/dev/null 2>&1
exec sleep 2147483647
"""
# sh -c GIVE_BACK_SCRIPT hpc-sandbox-give-back TREE, run as root of a user namespace that maps the
# caller's subordinate ids as a pod's with users="subids" does: give root, the caller outside, every
# file below TREE that another id of the namespace owns, never following a link or crossing into
# another filesystem. chown's failures, on owners the namespace does not map, are ignored: the
# deletion refuses what is left, and names it. What disappears during the walk is no error: a
# dropped service's trash entry, which its own deletion empties meanwhile, takes files and
# directories that find has listed, a directory among them before find could enter it, which
# -ignore_readdir_race alone does not excuse (GNU findutils 4.8 reports it). So find's errors that
# say "No such file or directory" (C's messages: give_back runs it with LC_ALL=C) are dropped, and
# the script fails, with find's status, only on another error, which it prints.
GIVE_BACK_SCRIPT = r"""
errors=$(find "$1" -xdev -ignore_readdir_race \( ! -user 0 -o ! -group 0 \) \
  -exec /bin/sh -c 'chown -h 0:0 -- "$@" 2>/dev/null; exit 0' hpc-sandbox-chown {} + \
  2>&1 >/dev/null)
status=$?
[ "$status" -eq 0 ] && exit 0
failed=0
while IFS= read -r line; do
  case $line in
    *": No such file or directory") ;;
    *) printf '%s\n' "$line" >&2; failed=1 ;;
  esac
done <<EOF
$errors
EOF
[ "$failed" -eq 0 ] || exit "$status"
"""
# How long giving back a pod's files may take: it walks every service's root filesystem.
_GIVE_BACK_TIMEOUT_S = 600.0
POD_USERS = ("single", "subids")
RUN_AS = ("root", "caller", "subids")
SERVICE_RUNTIMES: tuple[str, ...] = get_args(ServiceRuntime)
SECUREBITS: tuple[str, ...] = get_args(ServiceSecurebits)
APT_DROP_IN = 'APT::Sandbox::User "root";\n'
APT_DROP_IN_NAME = "99hpc-sandbox-aptfix"
_SERVICE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,62}")
_UPSTREAM_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*")
_HOST_ALIAS = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,252}")
_CONTROL_TIMEOUT_S = 30.0
_UNIX_PATH_MAX = 108  # sun_path, with its terminating NUL
_CONNECT_RETRY_S = 0.01
_EXIT_STATUS_WAIT_S = 1.0
_KILL_WAIT_S = 5.0
_SHELL_STATE = ("PWD", "OLDPWD", "SHLVL", "_")
# Resource limits services may set, by their docker-compose names.
ULIMITS = (
    "core",
    "cpu",
    "data",
    "fsize",
    "locks",
    "memlock",
    "msgqueue",
    "nice",
    "nofile",
    "nproc",
    "rss",
    "rtprio",
    "rttime",
    "sigpending",
    "stack",
)
# Accepted but not applied: rlimit counts of processes are kept per user
# namespace, so a service's nproc would count every process of the pod (under
# Docker it does not bind root either); locks has had no effect since Linux 2.4.
UNAPPLIED_ULIMITS = ("locks", "nproc")


def check_pod(spec: PodSpec) -> dict[str, int]:
    """Validate ``spec``; return the pod port of each upstream."""
    if spec.network not in ("none", "egress"):
        raise ValueError(f"unknown pod network {spec.network!r}")
    if spec.users not in POD_USERS:
        raise ValueError(f"pod users must be one of {POD_USERS}, not {spec.users!r}")
    unknown = set(spec.upstream_ports) - set(spec.upstreams)
    if unknown:
        raise ValueError(f"upstream_ports names unknown upstreams: {sorted(unknown)}")
    ports: dict[str, int] = {}
    next_port = FIRST_UPSTREAM_PORT
    for name, url in spec.upstreams.items():
        if not _UPSTREAM_NAME.fullmatch(name):
            raise ValueError(f"invalid upstream name {name!r}")
        upstream_path(url)
        if name in spec.upstream_ports:
            port = spec.upstream_ports[name]
        else:
            while next_port in ports.values() or next_port in spec.upstream_ports.values():
                next_port += 1
            port = next_port
        if not isinstance(port, int) or not 0 < port < 65536:
            raise ValueError(f"invalid port {port!r} for upstream {name!r}")
        if port in ports.values():
            raise ValueError(f"upstreams share port {port}")
        ports[name] = port
    for port in spec.exposes:
        check_exposed_port(port, ports)
    if len(set(spec.exposes)) != len(spec.exposes):
        raise ValueError(f"exposes names a port twice: {spec.exposes!r}")
    if isinstance(spec.hosts, str):
        raise ValueError(f"hosts is a list of names, not the string {spec.hosts!r}")
    for alias in spec.hosts:
        if not isinstance(alias, str) or not _HOST_ALIAS.fullmatch(alias):
            raise ValueError(f"invalid host alias {alias!r}")
    return ports


def check_exposed_port(port: int, upstream_ports: Mapping[str, int]) -> None:
    """Validate a port to expose: a pod port that no upstream relays."""
    if type(port) is not int or not 0 < port < 65536:  # not a bool either
        raise ValueError(f"invalid port {port!r} to expose")
    for name, upstream_port in upstream_ports.items():
        if port == upstream_port:
            raise ValueError(f"port {port} relays upstream {name!r}: it cannot be exposed")


def upstream_path(url: str) -> str:
    """The host path of an upstream, given as ``unix:///absolute/path``."""
    if not url.startswith("unix://"):
        raise ValueError(f"upstreams are Unix sockets (unix:///path), not {url!r}")
    path = url[len("unix://") :]
    if not path.startswith("/") or any(c in path for c in "\n\0"):
        raise ValueError(f"invalid upstream socket path in {url!r}")
    if len(os.fsencode(path)) >= _UNIX_PATH_MAX:
        raise ValueError(f"upstream socket path longer than {_UNIX_PATH_MAX - 1} bytes: {path!r}")
    return path


def check_service(spec: ServiceSpec) -> None:
    if not _SERVICE_NAME.fullmatch(spec.name):
        raise ValueError(f"invalid service name {spec.name!r}")
    if spec.command is not None and (
        not spec.command or not all(isinstance(arg, str) for arg in spec.command)
    ):
        raise ValueError("a service command is a non-empty list of strings")
    if spec.shm_mb is not None and spec.shm_mb <= 0:
        raise ValueError("shm_mb must be positive")
    if spec.run_as not in RUN_AS:
        raise ValueError(f"run_as must be one of {RUN_AS}, not {spec.run_as!r}")
    if spec.runtime is not None and spec.runtime not in SERVICE_RUNTIMES:
        raise ValueError(f"runtime must be None or one of {SERVICE_RUNTIMES}, not {spec.runtime!r}")
    if spec.securebits is not None and spec.securebits not in SECUREBITS:
        raise ValueError(f"securebits must be None or one of {SECUREBITS}, not {spec.securebits!r}")
    if spec.guest_cgroups is not None and spec.guest_cgroups not in GUEST_CGROUPS:
        raise ValueError(
            f"guest_cgroups must be None or one of {GUEST_CGROUPS}, not {spec.guest_cgroups!r}"
        )
    if spec.runtime is None:
        for name in ("securebits", "guest_cgroups"):
            if getattr(spec, name) is not None:
                raise ValueError(
                    f"{name} is a setting of gVisor services: it needs runtime='gvisor'"
                )
    if spec.sandbox.file_owners is not None and spec.run_as != "subids":
        raise ValueError(
            "file_owners need run_as='subids': a service that runs as root alone or as your uid "
            "maps one id, so its files cannot belong to other users"
        )
    for name, limits in spec.ulimits.items():
        if name not in ULIMITS or (
            name not in UNAPPLIED_ULIMITS
            and getattr(resource, f"RLIMIT_{name.upper()}", None) is None
        ):
            raise ValueError(f"unknown ulimit {name!r}")
        soft, hard = limits
        if not all(isinstance(value, int) and value >= -1 for value in (soft, hard)):
            raise ValueError(f"ulimit {name} needs integers (-1 for unlimited): {limits!r}")
        if hard != -1 and (soft == -1 or soft > hard):
            raise ValueError(f"ulimit {name}: the soft limit exceeds the hard one: {limits!r}")
    if spec.healthcheck is not None:
        check_healthcheck(spec.healthcheck)
    check_env(spec.sandbox.env)


def check_healthcheck(check: HealthCheck) -> None:
    health_argv(check)
    for name in ("interval_s", "timeout_s", "start_interval_s"):
        if getattr(check, name) <= 0:
            raise ValueError(f"healthcheck {name} must be positive")
    if check.start_period_s < 0 or check.retries < 1:
        raise ValueError("healthcheck start_period_s must be >= 0 and retries >= 1")


def health_argv(check: HealthCheck) -> list[str] | None:
    """The command a healthcheck runs, as docker-compose reads ``test``; None for NONE."""
    test = check.test
    if isinstance(test, str):
        return ["/bin/sh", "-c", test]
    if not test or not all(isinstance(part, str) for part in test):
        raise ValueError(f"invalid healthcheck test {test!r}")
    kind, args = test[0], list(test[1:])
    if kind == "NONE":
        return None
    if kind == "CMD" and args:
        return args
    if kind == "CMD-SHELL" and len(args) == 1:
        return ["/bin/sh", "-c", args[0]]
    raise ValueError(f"healthcheck test must start with CMD, CMD-SHELL or NONE: {test!r}")


def effective_ulimits(ulimits: Mapping[str, tuple[int, int]]) -> dict[str, tuple[int, int]]:
    """``ulimits`` within the hard limits of this process (-1: unlimited)."""
    result = {}
    for name, (soft, hard) in ulimits.items():
        if name in UNAPPLIED_ULIMITS:
            continue
        current = resource.getrlimit(getattr(resource, f"RLIMIT_{name.upper()}"))[1]
        if current != resource.RLIM_INFINITY and (hard == -1 or hard > current):
            hard = current
        if hard != -1 and (soft == -1 or soft > hard):
            soft = hard
        result[name] = (soft, hard)
    return result


def hosts_file(names: list[str]) -> str:
    lines = ["127.0.0.1\tlocalhost", "::1\tlocalhost ip6-localhost ip6-loopback"]
    return "".join(f"{line}\n" for line in lines + [f"127.0.0.1\t{n}" for n in names])


@dataclass
class _Health:
    status: ServiceHealth = "starting"
    stopped: bool = False
    failures: int = 0
    output: str = ""  # of the last check that failed
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None

    def set(self, status: ServiceHealth) -> None:
        if status != self.status:
            self.status = status
            self.changed.set()
            self.changed = asyncio.Event()


class SessionPod:
    """A running pod; create it with ``provider.start_pod(spec)``.

    ``async with await provider.start_pod(spec) as pod:`` stops the pod when
    the block exits, however it exits.
    """

    def __init__(
        self,
        provider: SessionProvider[Any],
        spec: PodSpec,
        *,
        ids: subids.SubordinateIds | None = None,
    ) -> None:
        self._id = new_instance_name()
        self.spec = spec
        self._ports = check_pod(spec)
        # The checked values, kept: a later change to the spec cannot reach the pod.
        self._exposes = tuple(spec.exposes)
        self._aliases = tuple(spec.hosts)
        # The subordinate ids that a pod with users="subids" maps, and the user namespace of this
        # process that maps them, which the pod's is made in (see id_namespace).
        self._ids = ids
        self._id_namespace: int | None = None
        self._routes = {name: f"http://127.0.0.1:{port}" for name, port in self._ports.items()}
        self._endpoints: dict[int, str] = {}
        self._expose_lock = asyncio.Lock()
        self._provider = provider
        self._session: Session | None = None
        self._services: dict[str, PodService] = {}
        self._names: list[str] = []
        # Services whose /etc/hosts exists, by name; written and extended under the lock.
        self._hosts: dict[str, PodService] = {}
        self._hosts_lock = threading.Lock()
        self._health: dict[str, _Health] = {}
        self._starting: set[asyncio.Task[Any]] = set()
        self._holder: subprocess.Popen[bytes] | None = None
        self._holder_start_time: str | None = None
        self._control: socket.socket | None = None
        self._control_lock = asyncio.Lock()
        self._network_lock = asyncio.Lock()
        self._slirp: subprocess.Popen[bytes] | None = None
        self._slirp_start_time: str | None = None
        self._running = False
        self._stop_task: asyncio.Future[None] | None = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id!r}, services={list(self._services)})"

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    @property
    def id(self) -> str:
        return self._id

    @property
    def routes(self) -> Mapping[str, str]:
        """Where each upstream is, inside the pod: ``{"gateway": "http://127.0.0.1:18080"}``."""
        return dict(self._routes)

    @property
    def endpoints(self) -> Mapping[int, str]:
        """Where each exposed port is, on the host: ``{8080: "/.../exposed/8080.sock"}``."""
        return dict(self._endpoints)

    @property
    def services(self) -> Mapping[str, PodService]:
        """The services that started, by name (stopped with the pod)."""
        return dict(self._services)

    @property
    def network(self) -> PodNetwork:
        """``egress`` while slirp4netns runs, else ``none``."""
        slirp = self._slirp
        return "egress" if slirp is not None and slirp.poll() is None else "none"

    @property
    def path(self) -> Path | None:
        return self._session.path if self._session else None

    @property
    def services_root(self) -> Path:
        assert self._session is not None
        return self._session.path / SERVICES_DIRECTORY

    @property
    def exposed_root(self) -> Path:
        assert self._session is not None
        return self._session.path / EXPOSED_DIRECTORY

    @property
    def holder_pid(self) -> int:
        assert self._holder is not None
        return self._holder.pid

    async def start_service(
        self, spec: ServiceSpec, wait: Literal["started", "healthy"] = "started"
    ) -> PodService:
        """Start a service; with ``wait="healthy"``, also wait until its healthcheck passes.

        A service without a healthcheck is healthy once it runs. A service that
        becomes unhealthy or exits first is stopped and raises
        :class:`SandboxStartError`. Failed and cancelled starts leave nothing.
        """
        self._require_running()
        check_service(spec)
        if wait not in ("started", "healthy"):
            raise ValueError(f"wait must be 'started' or 'healthy', not {wait!r}")
        if spec.name in self._names:
            raise ValueError(f"pod {self.id} already has a service named {spec.name!r}")
        service_type = self._provider._service_type_for(spec)
        service_type._check_start(self, spec)
        if spec.sandbox.file_owners is not None:
            await asyncio.to_thread(check_file_owners, spec.sandbox.file_owners)
            self._require_running()
            if spec.name in self._names:  # taken while the manifest was read
                raise ValueError(f"pod {self.id} already has a service named {spec.name!r}")
        self._names.append(spec.name)
        task = asyncio.ensure_future(self._start_service(service_type, spec, wait))
        self._starting.add(task)
        try:
            return await task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise
            raise SandboxStartError(
                f"pod {self.id} was stopped while service {spec.name!r} started"
            ) from None
        finally:
            self._starting.discard(task)
            if spec.name not in self._services:
                self._names.remove(spec.name)
                await asyncio.shield(asyncio.to_thread(self._forget, spec.name))

    async def health(self, name: str) -> ServiceHealth:
        """``starting``, ``healthy`` or ``unhealthy`` (docker-compose's rules), or
        ``exited`` once the service's command has exited (its ``exit_status`` is
        then known)."""
        service = self._service(name)
        if not service._command_running():
            await poll(lambda: service.exit_status, _EXIT_STATUS_WAIT_S)
            return "exited"
        return self._health[name].status

    async def logs(self, name: str) -> str:
        """The tail of what the service's command printed (stdout and stderr)."""
        service = self._service(name)
        return await asyncio.to_thread(service.log_tail, LOG_TAIL_BYTES)

    async def set_network(self, mode: PodNetwork) -> None:
        """Switch between ``none`` (loopback only) and ``egress`` (slirp4netns)."""
        if mode not in ("none", "egress"):
            raise ValueError(f"unknown pod network {mode!r}")
        self._require_running()
        async with self._network_lock:
            if mode == self.network:
                return
            # Whatever ran before goes first; a stop runs to its end even when cancelled.
            await _finish(asyncio.ensure_future(self._stop_egress()))
            if mode == "egress":
                await self._start_egress()

    async def expose(self, port: int) -> str:
        """Make ``port`` of the pod reachable from the host; return the path of its socket.

        The holder listens on the Unix socket ``exposed/<port>.sock`` in the pod's
        directory, which only the caller's uid can reach, and forwards each
        connection to ``port`` on the pod's loopback (127.0.0.1, else ::1), whether
        or not a service listens there yet. A port that is exposed already keeps
        its socket.
        """
        self._require_running()
        check_exposed_port(port, self._ports)
        async with self._expose_lock:
            if port not in self._endpoints:
                # Runs to its end even when cancelled, so the holder and the endpoints agree.
                await _finish(asyncio.ensure_future(self._expose(port)))
        return self._endpoints[port]

    async def open_connection(self, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Connect to an exposed port from the host, like
        :func:`asyncio.open_unix_connection` on its socket, whatever the length of
        the socket's path."""
        self._require_running()
        if port not in self._endpoints:
            raise KeyError(f"pod {self.id} does not expose port {port}")
        try:
            connection = await connect_unix(self._endpoints[port], _CONTROL_TIMEOUT_S)
        except (ConnectionRefusedError, FileNotFoundError):
            # The holder died, or the pod stopped, since the check: say which.
            await poll(lambda: None if self.holder_alive() else True, _EXIT_STATUS_WAIT_S)
            self._require_running()
            raise
        try:
            return await asyncio.open_unix_connection(sock=connection)
        except BaseException:
            connection.close()
            raise

    async def stop(self) -> None:
        """Stop every service and the holder and delete the pod; idempotent and
        cancellation-safe, like :meth:`SessionSandbox.stop`."""
        task = self._stop_task
        if task is None or (task.done() and not task.cancelled() and task.exception()):
            task = self._stop_task = asyncio.ensure_future(self._teardown())
        await _finish(task)

    # Startup.

    async def _boot(self) -> None:
        provider = self._provider
        try:
            if self._ids is not None:
                self._id_namespace = await id_namespace(self._ids, provider.start_timeout_s)
            self._session = await create_session(provider.pod_root, self.id)
            self.services_root.mkdir(mode=0o700)
            self.exposed_root.mkdir(mode=0o700)
            self.exposed_root.chmod(0o700)  # whatever the umask: the holder binds in it
            self._record()
            await self._start_holder()
            for port in self._exposes:
                await self._expose(port)
            if self.spec.network == "egress":
                await self._start_egress()
        except BaseException as error:
            try:
                await self.stop()
            except Exception as cleanup_error:  # noqa: BLE001 - reported with the cause
                raise SandboxStartError(
                    f"pod {self.id} failed to start and its cleanup failed: {cleanup_error}"
                ) from error
            if isinstance(error, SandboxStartError) or not isinstance(error, Exception):
                raise
            raise SandboxStartError(f"pod {self.id} failed to start: {error}") from error
        self._running = True
        provider._pods.add(self)

    def _record(self) -> None:
        assert self._session is not None
        self._session.write_owner_record(
            provider=self._provider.name,
            kind="pod",
            labels=dict(self.spec.labels),
            users=self.spec.users,
            holder_pid=self._holder.pid if self._holder else None,
            holder_start_time=self._holder_start_time,
            slirp_pid=self._slirp.pid if self._slirp else None,
            slirp_start_time=self._slirp_start_time,
        )

    async def _start_holder(self) -> None:
        assert self._session is not None
        namespace = self._id_namespace  # with users="subids", the holder makes the pod's inside
        ours, theirs = socket.socketpair()
        try:
            with (self._session.path / HOLDER_LOG).open("wb") as output:
                # Popen, not asyncio: nothing interrupts the spawn before the handle is kept.
                self._holder = subprocess.Popen(  # noqa: ASYNC220 - a fork and exec
                    [
                        sys.executable,
                        "-I",
                        "-S",
                        str(HOLDER_SCRIPT),
                        str(os.getpid()),
                        str(theirs.fileno()),
                        self.id,
                        "single" if namespace is None else f"nested:{namespace}",
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    pass_fds=[theirs.fileno(), *([] if namespace is None else [namespace])],
                    env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
                    start_new_session=True,
                )
        except BaseException:
            ours.close()
            raise
        finally:
            theirs.close()
        self._control = ours
        ours.setblocking(False)
        self._holder_start_time = procfs.start_time(self._holder.pid)
        self._record()
        lines = [
            f"relay {port} {upstream_path(self.spec.upstreams[name])}"
            for name, port in self._ports.items()
        ]
        reply = await self._ask("\n".join([*lines, "start"]), "ready")
        if problems := reply.removeprefix("ready").strip():
            log.warning("pod %s: %s", self.id, problems)

    async def _ask(self, request: str, expected: str, fds: list[int] | None = None) -> str:
        """Send control lines to the holder; return its answer, which must start with ``expected``.

        An exchange runs to its end even when the caller is cancelled, so the
        next one reads its own answer.
        """
        if self._control is None or self._holder is None:
            raise SandboxLostError(f"pod {self.id} has no holder")
        async with self._control_lock:
            exchange = asyncio.ensure_future(self._exchange(request, expected, fds))
            try:
                return await asyncio.shield(exchange)
            except asyncio.CancelledError:
                while not exchange.done():
                    with contextlib.suppress(asyncio.CancelledError):
                        await asyncio.wait({exchange})
                if not exchange.cancelled():
                    exchange.exception()  # retrieved: the caller is gone
                raise

    async def _exchange(self, request: str, expected: str, fds: list[int] | None) -> str:
        control = self._control
        if control is None:
            raise SandboxLostError(f"pod {self.id} has no holder")
        loop = asyncio.get_running_loop()
        data = (request + "\n").encode()
        if fds:
            await asyncio.to_thread(socket.send_fds, control, [data], fds)
        else:
            await loop.sock_sendall(control, data)
        try:
            answer = await _read_answer(control, _CONTROL_TIMEOUT_S)
        except TimeoutError:  # its answer may come later, out of step: give up on it
            self._control = None
            control.close()
            raise SandboxLostError(
                f"the holder of pod {self.id} did not answer {request!r}"
            ) from None
        if answer is None:
            raise SandboxLostError(f"the holder of pod {self.id} exited: {self._holder_log()}")
        if answer.startswith("error") or not answer.startswith(expected):
            raise SandboxError(f"the holder of pod {self.id} answered {answer!r}")
        return answer

    async def _expose(self, port: int) -> None:
        """Have the holder listen on ``port``'s socket, in the directory it gets as a descriptor."""
        directory = self.exposed_root
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            await self._ask(f"expose {port}", "exposed", fds=[fd])
        finally:
            os.close(fd)
        self._endpoints[port] = str(directory / f"{port}.sock")

    def _holder_log(self) -> str:
        if self._session is None:
            return "no output"
        try:
            text = (self._session.path / HOLDER_LOG).read_text(errors="replace").strip()
        except OSError:
            return "no output"
        return text[-2000:] or "no output"

    # Egress.

    async def _start_egress(self) -> None:
        assert self._session is not None and self._holder is not None
        binary = self._provider.slirp4netns()
        ready_read, ready_write = os.pipe()
        exit_read, exit_write = os.pipe()
        try:
            with (self._session.path / SLIRP_LOG).open("ab") as output:
                self._slirp = subprocess.Popen(  # noqa: ASYNC220 - a fork and exec
                    [
                        binary,
                        "--configure",
                        "--mtu=65520",
                        "--disable-host-loopback",
                        "--enable-sandbox",
                        "--enable-seccomp",
                        f"--exit-fd={exit_read}",
                        f"--ready-fd={ready_write}",
                        str(self._holder.pid),
                        "tap0",
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    pass_fds=[exit_read, ready_write],
                    start_new_session=True,
                )
            os.close(exit_read)
            os.close(ready_write)
            exit_read = ready_write = -1
            self._slirp_start_time = procfs.start_time(self._slirp.pid)
            self._record()
            # Only the holder keeps the exit pipe open: slirp4netns dies with it.
            await self._ask("hold", "held", fds=[exit_write])
            os.close(exit_write)
            exit_write = -1
            async with asyncio.timeout(self._provider.start_timeout_s):
                ready = await asyncio.to_thread(os.read, ready_read, 1)
            if ready != b"1":
                raise SandboxError(f"slirp4netns failed: {self._slirp_log()}")
        except BaseException:
            for fd in (exit_read, ready_write, exit_write):
                if fd >= 0:
                    os.close(fd)
            await self._stop_egress()
            raise
        finally:
            os.close(ready_read)
        log.info("pod %s: egress through %s (pid %d)", self.id, binary, self._slirp.pid)

    async def _stop_egress(self) -> None:
        slirp = self._slirp
        if self._control is not None and self._holder is not None and self._holder.poll() is None:
            with contextlib.suppress(SandboxError):
                await self._ask("release", "released")
        if slirp is not None and await poll(slirp.poll, _KILL_WAIT_S) is None:
            slirp.kill()
            if await poll(slirp.poll, _KILL_WAIT_S) is None:
                raise SandboxError(f"slirp4netns of pod {self.id} survived SIGKILL")
        self._slirp = None
        self._slirp_start_time = None
        if self._session is not None:
            try:  # a full scratch must not stop the pod's teardown: janitors prove identities
                self._record()
            except OSError as error:
                log.warning("pod %s: could not update its owner record: %s", self.id, error)

    def _slirp_log(self) -> str:
        assert self._session is not None
        try:
            return (self._session.path / SLIRP_LOG).read_text(errors="replace")[-2000:].strip()
        except OSError:
            return "no output"

    # Services.

    async def _start_service(
        self, service_type: type[PodService], spec: ServiceSpec, wait: str
    ) -> PodService:
        provider = self._provider
        service = service_type(provider, self, spec)
        await asyncio.to_thread(self._announce, spec.name)
        await service._boot(await service._populate())
        health = self._health[spec.name] = _Health()
        self._services[spec.name] = service
        try:
            if spec.healthcheck is not None and (argv := health_argv(spec.healthcheck)):
                health.task = asyncio.create_task(
                    self._check_health(service, spec.healthcheck, argv, health)
                )
            else:
                health.set("healthy")
            if wait == "healthy":
                timeout = provider.start_timeout_s
                try:
                    async with asyncio.timeout(timeout):
                        status = await self._settled(spec.name)
                except TimeoutError:
                    raise SandboxStartError(
                        f"service {spec.name!r} of pod {self.id} was not healthy within "
                        f"{timeout:g} s: {health.output.strip() or 'no check finished'}"
                    ) from None
                if status != "healthy":
                    detail = health.output.strip() or service.log_tail(4096).strip()
                    raise SandboxStartError(
                        f"service {spec.name!r} of pod {self.id} is {status}: "
                        f"{detail or 'no output'}"
                    )
        except BaseException:
            await self._drop(spec.name)
            raise
        return service

    def _announce(self, name: str) -> None:
        """Make ``name`` resolve in every service whose /etc/hosts exists."""
        with self._hosts_lock:
            for other, service in self._hosts.items():
                if other != name:
                    service.add_host(name)

    def _host_names(self, own: str) -> list[str]:
        """The names a service's /etc/hosts maps to 127.0.0.1: the other services',
        the pod's host aliases, and its own."""
        others = [name for name in list(self._names) if name != own]
        return list(dict.fromkeys([*others, *self._aliases, own]))

    def _write_hosts(self, service: PodService) -> None:
        """Write a new service's /etc/hosts; from now on it hears of names that join."""
        with self._hosts_lock:
            service.hosts_path.write_text(hosts_file(self._host_names(service.name)))
            self._hosts[service.name] = service

    def _forget(self, name: str) -> None:
        """Take a service that failed to start out of the other services' /etc/hosts."""
        with self._hosts_lock:
            self._hosts.pop(name, None)
            for other, service in self._hosts.items():
                service.rewrite_hosts(hosts_file(self._host_names(other)))

    async def _settled(self, name: str) -> ServiceHealth:
        """Wait until the service is no longer starting."""
        health = self._health[name]
        service = self._services[name]
        while True:
            if not service._command_running():
                return "exited"
            if health.status != "starting":
                return health.status
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(0.2):
                    await health.changed.wait()

    async def _check_health(
        self, service: PodService, check: HealthCheck, argv: list[str], health: _Health
    ) -> None:
        """Run the healthcheck forever, following Docker's rules."""
        started = time.monotonic()
        while not health.stopped:
            in_start_period = time.monotonic() - started < check.start_period_s
            starting = health.status == "starting"
            await asyncio.sleep(
                check.start_interval_s if in_start_period and starting else check.interval_s
            )
            if health.stopped or not service._command_running():
                return
            in_start_period = time.monotonic() - started < check.start_period_s
            try:
                result = await service.exec(argv, timeout_s=check.timeout_s)
                passed, output = result.exit_code == 0, result.stdout + result.stderr
            except Exception as error:  # noqa: BLE001 - a check that cannot run fails
                log.warning("pod %s: healthcheck of %s: %s", self.id, service.name, error)
                passed, output = False, f"hpc-sandbox: the healthcheck did not run: {error}"
            if passed:
                health.failures = 0
                health.set("healthy")
            else:
                health.output = output[-4096:]
                if not (in_start_period and health.status == "starting"):
                    health.failures += 1
                    if health.failures >= check.retries:
                        health.set("unhealthy")

    async def _drop(self, name: str) -> None:
        """Stop a service and forget it, whatever happens to the caller meanwhile."""
        health = self._health.pop(name, None)
        service = self._services.pop(name, None)

        async def drop() -> None:
            if health is not None and health.task is not None:
                health.stopped = True
                health.task.cancel()
                await asyncio.gather(health.task, return_exceptions=True)
            if service is not None:
                await service.stop()

        await _finish(asyncio.ensure_future(drop()))

    def _service(self, name: str) -> PodService:
        self._require_running()
        if name not in self._services:
            raise KeyError(f"pod {self.id} has no service {name!r}")
        return self._services[name]

    def holder_alive(self) -> bool:
        holder = self._holder
        return (
            holder is not None
            and holder.poll() is None
            and procfs.owned_live_start_time(holder.pid) == self._holder_start_time
        )

    def _require_running(self) -> None:
        if self._stop_task is not None or not self._running:
            raise SandboxError(f"pod {self.id} is not running")
        if not self.holder_alive():
            raise SandboxLostError(f"pod {self.id} lost its holder: {self._holder_log()}")

    # Teardown.

    async def _teardown(self) -> None:
        self._running = False
        self._provider._pods.discard(self)
        for task in list(self._starting):
            task.cancel()
        if self._starting:
            await asyncio.wait(self._starting)
        for health in self._health.values():
            health.stopped = True
            if health.task is not None:
                health.task.cancel()
        await asyncio.gather(
            *(h.task for h in self._health.values() if h.task), return_exceptions=True
        )
        for service in self._services.values():
            service._stopping = True  # no new commands; what ends from now on was stopped, not lost
        grace = self._provider.service_stop_grace_s
        commands = [s for s in self._services.values() if s.service.command is not None]
        if grace > 0 and commands:  # an idle loop has nothing to finish
            asked = await asyncio.gather(
                *(service._terminate() for service in commands), return_exceptions=True
            )
            for service, outcome in zip(commands, asked, strict=True):
                if isinstance(outcome, Exception):
                    log.warning("pod %s: could not stop %s: %s", self.id, service.name, outcome)
            deadline = time.monotonic() + grace
            # Until the launchers report the statuses: the command exits before its status gets
            # up through the runtime's processes and the init, which a stop would kill.
            while time.monotonic() < deadline and any(s.exit_status is None for s in commands):
                await asyncio.sleep(0.05)
        results = await asyncio.gather(
            *(service.stop() for service in self._services.values()), return_exceptions=True
        )
        failures = [str(result) for result in results if isinstance(result, BaseException)]
        if not failures and self.spec.users == "subids" and self._session is not None:
            await self._give_back(self._session.path)  # nothing runs any more that could write
        await self._stop_egress()
        await self._stop_holder()
        if failures:
            raise SandboxError(f"pod {self.id}: could not stop services: {'; '.join(failures)}")
        self._services.clear()
        session = self._session
        if session is not None:
            doomed = await trash_session_awaiting(session)
            self._session = None
            if doomed is not None:
                await self._provider._delete_in_background(doomed)

    async def _give_back(self, path: Path) -> None:
        """Give your uid back what the pod's other users own below ``path``, in the holder's user
        namespace while it lives. A failure is logged: the deletion then names what is left."""
        holder = self._holder.pid if self._holder is not None and self.holder_alive() else None
        try:
            await give_back(path, self._provider.nsenter, holder=holder, ids=self._ids)
        except (SandboxError, OSError) as error:
            log.warning("pod %s: %s", self.id, error)

    async def _stop_holder(self) -> None:
        holder, control = self._holder, self._control
        self._control = None
        if control is not None:
            control.close()  # the holder exits when its control socket closes
        if holder is None:
            return
        if await poll(holder.poll, _KILL_WAIT_S) is None:
            holder.kill()
            if await poll(holder.poll, self._provider.stop_timeout_s) is None:
                raise SandboxError(f"the holder of pod {self.id} survived SIGKILL")
        self._holder = None


class PodService(SessionSandbox):
    """One service of a pod: a container under a launcher joined to the pod.

    Providers combine this with their sandbox class and implement
    :meth:`_runtime_command`, :meth:`_runtime_environment`,
    :meth:`_prepare_service_rootfs` and :meth:`_userns_options`.
    """

    _tracked = False  # the pod tracks its services
    #: The ``run_as`` modes that the runtime can give a service.
    run_modes: tuple[str, ...] = RUN_AS

    @classmethod
    def _check_start(cls, pod: SessionPod, spec: ServiceSpec) -> None:
        """Refuse, with ``ValueError``, a service that this runtime cannot start in ``pod``."""
        provider = pod._provider
        if spec.run_as not in cls.run_modes:
            raise ValueError(
                f"the {provider.name} provider runs services with run_as in {cls.run_modes}, not "
                f"{spec.run_as!r}: its containers map one id"
            )
        if spec.run_as == "subids" and pod.spec.users != "subids":
            raise ValueError("run_as='subids' needs a pod started with PodSpec(users='subids')")
        provider._check(spec.sandbox, service=True)

    def __init__(self, provider: SessionProvider[Any], pod: SessionPod, service: ServiceSpec):
        super().__init__(provider, service.sandbox)  # type: ignore[arg-type]
        self.pod = pod
        self.service = service
        self.name = service.name
        # Who may own the service's command on the host: you, or once it switched users, one of
        # the subordinate ids its namespace maps.
        ids = pod._ids
        self._owners: Container[int] = (
            ids.uids.ids if ids is not None and service.run_as == "subids" else ()
        )
        self._exit_status: int | None = None
        self._ulimits = effective_ulimits(service.ulimits)
        self._service_anchor: subprocess.Popen[bytes] | None = None
        self._service_anchor_start: str | None = None
        self._service_init: int | None = None
        self._service_init_start: str | None = None
        self._container: int | None = None
        self._container_start: str | None = None
        self._service_environment: dict[str, str] = {}

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r}, pod={self.pod.id!r})"

    # What the runtime supplies.

    def _runtime_command(self, argv: list[str]) -> list[str]:
        """The runtime command line that runs ``argv`` in the service's container."""
        raise NotImplementedError

    def _runtime_environment(self) -> dict[str, str]:
        raise NotImplementedError

    def _prepare_service_rootfs(self) -> None:
        """Runtime-specific preparation of the rootfs; runs in a worker thread."""

    def _userns_options(self) -> list[str]:
        """The launcher's ``--userns`` option for the service's ``run_as``, which makes the user
        namespace the runtime runs in; none when the runtime makes one of its own."""
        return []

    def _user(self) -> dict[str, Any]:
        caller = self.service.run_as == "caller"
        return {"remap_root": not caller, "container_uid": os.getuid() if caller else 0}

    def _workdir_umask(self) -> str:
        # Like a --fakeroot sandbox's: the users a subids service switches to must be able to
        # enter a working directory that the start makes, as Docker's -w makes it.
        return "022" if self.service.run_as == "subids" else ""

    async def _apply_file_owners(self) -> None:
        """Nothing: the launcher gave the image's paths their owners before the container started
        (``--file-owners``)."""

    # Files.

    @property
    def _files(self) -> Path:
        if self._session is None:
            raise SandboxError(f"service {self.name!r} of pod {self.pod.id} is stopped")
        return self._session.path

    @property
    def hosts_path(self) -> Path:
        return self._files / "hosts"

    @property
    def resolv_path(self) -> Path:
        return self._files / "resolv.conf"

    @property
    def shm_path(self) -> Path:
        return self._files / "shm"

    def add_host(self, name: str) -> None:
        """Make ``name`` resolve to 127.0.0.1 here; appended to the file the service sees."""
        line = f"127.0.0.1\t{name}\n"
        flags = os.O_RDWR | os.O_APPEND | os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            with os.fdopen(os.open(self.hosts_path, flags), "r+") as stream:
                if line not in stream.read():
                    stream.write(line)
        except (OSError, SandboxError) as error:
            log.warning("pod %s: could not add %s to %s: %s", self.pod.id, name, self.name, error)

    def rewrite_hosts(self, content: str) -> None:
        """Replace what the service's /etc/hosts says, in place: it is bound into the service."""
        try:
            fd = os.open(self.hosts_path, os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        except (OSError, SandboxError) as error:
            log.warning(
                "pod %s: could not rewrite the hosts of %s: %s", self.pod.id, self.name, error
            )
            return
        with os.fdopen(fd, "w") as stream:
            stream.write(content)
            stream.truncate()

    def _prepare_rootfs(self) -> None:
        super()._prepare_rootfs()
        self.pod._write_hosts(self)
        self.resolv_path.write_text(f"nameserver {NAMESERVER}\n")
        self.shm_path.mkdir(mode=0o700)
        add_apt_drop_in(self._session.rootfs if self._session else Path("/nonexistent"))
        self._prepare_service_rootfs()

    def _session_root(self) -> Path:
        return self.pod.services_root

    async def _populate(self) -> Callable[[Session], Coroutine[Any, Any, None]]:
        """Resolve the service's image; return what fills its session from it."""
        provider = self._provider
        image = await provider.images.resolve(self.spec.image)
        return partial(provider._extract_service, image)

    @property
    def _writes_as_other_users(self) -> bool:
        """Whether the service may leave files that your subordinate ids own on the host."""
        return self.service.run_as == "subids"

    async def _discard(self, session: Session) -> None:
        if self.pod._stop_task is not None:  # the pod's deletion takes these files too
            session.release()
            return
        if self._writes_as_other_users:
            await self.pod._give_back(session.path)
        await super()._discard(session)

    async def snapshot(self) -> SandboxSnapshot:
        raise SandboxError("snapshots of pod services are not supported yet")

    def log_tail(self, limit: int) -> str:
        """The last ``limit`` bytes the command printed (stdout and stderr)."""
        try:
            with (self._files / SERVICE_LOG).open("rb") as stream:
                head = stream.read(64 * 1024)
                marker = head.find(f"{READY}\n".encode())
                start = marker + len(READY) + 1 if marker >= 0 else 0
                size = stream.seek(0, os.SEEK_END)
                stream.seek(max(start, size - limit))
                return stream.read().decode(errors="replace")
        except (OSError, SandboxError):
            return ""

    # Starting.

    def _service_argv(self) -> list[str]:
        command = self.service.command or ["/bin/sh", "-c", SERVICE_KEEPALIVE]
        umask = self._workdir_umask()
        return ["/bin/sh", "-c", SERVICE_START, self.id, umask, self.spec.workdir, *command]

    def _anchor_command(self) -> list[str]:
        shm_mb = self.service.shm_mb or DEFAULT_SHM_MB
        options = ["--hostname", self.name, "--shm", f"{self.shm_path}:{shm_mb * 1024 * 1024}"]
        for name, (soft, hard) in self._ulimits.items():
            options += ["--rlimit", f"{name}={soft}:{hard}"]
        if self.spec.file_owners is not None:  # a subids service: the pod's namespace maps them
            rootfs = str(self._files / "rootfs")
            options += ["--file-owners", os.path.abspath(self.spec.file_owners), rootfs]
        return [
            sys.executable,
            "-I",
            "-S",
            str(ANCHOR_SCRIPT),
            str(os.getpid()),
            "join",
            str(self.pod.holder_pid),
            *options,
            *self._userns_options(),
            "--",
            *self._runtime_command(self._service_argv()),
        ]

    async def _start_container(self) -> None:
        provider = self._provider
        log_path = self._files / SERVICE_LOG
        if not self.pod.holder_alive():
            raise SandboxLostError(f"pod {self.pod.id} lost its holder")
        command = self._anchor_command()
        environment = self._runtime_environment()
        with log_path.open("wb") as output:
            self._service_anchor = subprocess.Popen(  # noqa: ASYNC220 - a fork and exec
                command,
                stdin=subprocess.PIPE,
                stdout=output,
                stderr=subprocess.STDOUT,
                env=environment,
                cwd="/",  # never the caller's directory, which nothing in the service needs
                start_new_session=True,
            )
        anchor = self._service_anchor
        self._service_anchor_start = procfs.start_time(anchor.pid)
        self._record()
        try:
            deadline = time.monotonic() + provider.start_timeout_s
            while not _contains(log_path, f"{READY}\n"):
                if anchor.poll() is not None:
                    raise SandboxStartError(
                        f"service {self.name!r} exited during startup "
                        f"(status {anchor.returncode}): {_tail(log_path)}"
                    )
                if time.monotonic() > deadline:
                    raise SandboxStartError(
                        f"service {self.name!r} did not start within "
                        f"{provider.start_timeout_s:g} s: {_tail(log_path)}"
                    )
                await asyncio.sleep(0.02)
            self._identify()
            self._report_file_owners(log_path)
            # The container's environment, read while its shell waits for the
            # service's own variables on stdin.
            environ = procfs.environ(self._container) if self._container else None
            if environ is None:
                raise SandboxStartError("could not read the service container's environment")
            self._service_environment = {
                key: value for key, value in environ.items() if key not in _SHELL_STATE
            }
            assert anchor.stdin is not None
            line = environment_script({"HOME": "/root", **self.spec.env})
            await asyncio.to_thread(write_and_close, anchor.stdin, line)
        finally:
            if anchor.stdin is not None and not anchor.stdin.closed:
                anchor.stdin.close()
        self._record()

    def _report_file_owners(self, log_path: Path) -> None:
        """Log what the launcher said of the entries of the file owners manifest it skipped."""
        if self.spec.file_owners is None:
            return
        with contextlib.suppress(OSError), log_path.open("rb") as stream:
            head = stream.read(64 * 1024).partition(f"{READY}\n".encode())[0]
            for line in head.decode(errors="replace").splitlines():
                if line.startswith(FILE_OWNERS_REPORT):
                    log.warning("pod %s: service %s: %s", self.pod.id, self.name, line)

    def _identify(self) -> None:
        """Find the init (the launcher's child) and the container's shell (by its name)."""
        anchor = self._service_anchor
        assert anchor is not None
        children = procfs.children(anchor.pid)
        if len(children) != 1:
            raise SandboxStartError(f"unexpected process tree below the launcher: {children}")
        (init,) = children
        started = procfs.owned_live_start_time(init)
        if started is None or procfs.namespace_pid(init) != 1:
            raise SandboxStartError("could not prove the identity of the service's init")
        self._service_init, self._service_init_start = init, started
        wanted = ["/bin/sh", "-c", SERVICE_START, self.id]
        for pid in procfs.descendants([init]):
            argv = procfs.argv(pid)
            if argv is not None and argv[:4] == wanted:
                container_start = procfs.owned_live_start_time(pid, self._owners)
                if container_start is not None:
                    self._container, self._container_start = pid, container_start
                    return
        raise SandboxStartError("could not find the service's container process")

    async def _probe(self) -> None:
        if self.service.command is None:  # a command may well exit at once, as it should
            await super()._probe()

    # Commands.

    def _enter_args(self) -> list[str]:
        """``nsenter`` twice: the pod's user and network namespaces, then the container's own."""
        anchor, container = self._service_anchor, self._container
        assert anchor is not None and container is not None
        nsenter = self._provider.nsenter
        outer = [
            nsenter,
            f"--user=/proc/{anchor.pid}/ns/user",
            f"--net=/proc/{anchor.pid}/ns/net",
            "--preserve-credentials",
            "--",
        ]
        inner = [
            nsenter,
            f"--user=/proc/{container}/ns/user",
            f"--mount=/proc/{container}/ns/mnt",
            f"--pid=/proc/{container}/ns/pid",
            f"--ipc=/proc/{container}/ns/ipc",
            f"--uts=/proc/{container}/ns/uts",
            f"--root=/proc/{container}/root",
            f"--wd=/proc/{container}/root",
            "--preserve-credentials",
            "--",
        ]
        limits = []
        if self._ulimits:
            prlimit = shutil.which("prlimit")
            if prlimit is None:
                raise SandboxError("ulimits need prlimit (util-linux) on PATH")
            limits = [prlimit]
            for name, (soft, hard) in self._ulimits.items():
                limits.append(f"--{name}={_limit(soft)}:{_limit(hard)}")
            limits.append("--")
        return [*limits, *outer, *inner]

    async def _spawn(
        self,
        command: list[str],
        *,
        stdin: bytes | None,
        timeout_s: float,
        on_start: Callable[[int], None],
    ) -> Any:
        from .process import run_process

        if not self._container_alive():
            self._mark_lost(
                "the service's command exited"
                if self.pod.holder_alive()
                else "the pod's holder is gone"
            )
        return await run_process(
            [*self._enter_args(), *command],
            timeout_s=timeout_s,
            stdin=stdin,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
            output_limit=self._output_limit,
            on_start=on_start,
        )

    def _full_environment(self, env: Mapping[str, str]) -> dict[str, str]:
        return {**self._service_environment, **env}

    # State.

    @property
    def exit_status(self) -> int | None:
        """The exit status of the service's command, once it exited (128 + signal if killed)."""
        anchor = self._service_anchor
        if anchor is not None and anchor.poll() is not None:
            code = anchor.returncode  # negative when the launcher itself was killed
            self._exit_status = 128 - code if code < 0 else code
        return self._exit_status

    def _command_running(self) -> bool:
        container = self._container
        return (
            container is not None
            and procfs.owned_live_start_time(container, self._owners) == self._container_start
        )

    def _container_alive(self) -> bool:
        return self._command_running() and self.pod.holder_alive()

    def _watch_root(self) -> int | None:
        anchor = self._service_anchor
        return anchor.pid if anchor is not None and self._container else None

    def _own_processes(self) -> list[int]:
        """The launcher, the init, the runtime's processes between the init and the
        container (Singularity's starter and its shim init, which the container
        dies with), and the container itself when it is the idle loop."""
        own = [pid for pid in (self._watch_root(), self._service_init) if pid]
        runtime, pid = [], self._container
        while pid and pid not in own and (parent := procfs.parent(pid)) and parent not in own:
            runtime.append(parent)
            pid = parent
        if self._container and pid and procfs.parent(pid) == self._service_init:
            own += runtime
        if self.service.command is None and self._container:
            own.append(self._container)  # the idle loop is the service
        return own

    def _container_pid(self) -> int | None:
        return self._container

    def _identity(self) -> dict[str, Any]:
        anchor = self._service_anchor
        return {
            "pod": self.pod.id,
            "service": self.name,
            "anchor_pid": anchor.pid if anchor else None,
            "anchor_start_time": self._service_anchor_start,
            "init_pid": self._service_init,
            "init_start_time": self._service_init_start,
            "container_pid": self._container,
            "container_start_time": self._container_start,
            "run_as": self.service.run_as,
        }

    async def _terminate(self) -> None:
        """Ask the command to stop: SIGTERM to the init, which passes it on."""
        init = self._service_init
        if init is not None and procfs.owned_live_start_time(init) == self._service_init_start:
            with contextlib.suppress(ProcessLookupError):
                os.kill(init, signal.SIGTERM)

    async def _stop_container(self, session: Session) -> None:
        anchor = self._service_anchor
        if anchor is None:
            return
        if anchor.stdin is not None and not anchor.stdin.closed:
            anchor.stdin.close()
        if anchor.poll() is None:
            init = self._service_init
            if init is None:  # startup failed before the init was identified
                init = next(iter(procfs.children(anchor.pid)), None)
            started = procfs.owned_live_start_time(init) if init is not None else None
            if init is not None and started is not None and init in procfs.children(anchor.pid):
                with contextlib.suppress(ProcessLookupError):
                    os.kill(init, signal.SIGKILL)  # PID 1: the whole namespace goes with it
            if await poll(anchor.poll, _KILL_WAIT_S) is None:
                kill_tree(anchor.pid)
            if await poll(anchor.poll, self._provider.stop_timeout_s) is None:
                raise SandboxError(f"the launcher of service {self.name!r} survived SIGKILL")
        if self._command_running():
            raise SandboxError(f"the container of service {self.name!r} survived teardown")
        code = anchor.returncode  # negative when the launcher itself was killed
        self._exit_status = 128 - code if code < 0 else code
        self._service_anchor = None


async def connect_unix(path: str, timeout_s: float) -> socket.socket:
    """A Unix stream socket connected to ``path``, however long the path.

    A socket address holds at most 107 bytes of path, so the connection goes
    through ``/proc/self/fd`` and a descriptor of the socket itself. While the
    listener's backlog is full, it tries again until ``timeout_s`` passes.
    """
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        connection.setblocking(False)
        target = os.open(path, os.O_PATH | os.O_CLOEXEC)
        try:
            deadline = time.monotonic() + timeout_s
            while True:
                try:
                    connection.connect(f"/proc/self/fd/{target}")
                    return connection
                except BlockingIOError:  # the backlog is full; nothing was started
                    if time.monotonic() > deadline:
                        raise TimeoutError(
                            f"{path} accepted no connection within {timeout_s:g} s"
                        ) from None
                    await asyncio.sleep(_CONNECT_RETRY_S)
        finally:
            os.close(target)
    except BaseException:
        connection.close()
        raise


# The user namespaces that map this process's subordinate ids, by those ids: open descriptors that
# keep them, with the device and inode of the namespace they name, which the holders of pods with
# users="subids" make theirs in. newuidmap and newgidmap map one once (they can take seconds where
# the ranges of many users overlap: shadow-utils looks up the owner of each range that holds an id
# it checks), and every later pod of the process, and of the processes it forks, reuses it. In an
# event loop, its lock lets one start map while the others wait; event loops of other threads may
# map at the same time, and then one namespace is kept.
_ID_NAMESPACES: dict[subids.SubordinateIds, tuple[int, tuple[int, int]]] = {}
_ID_NAMESPACES_LOCK = threading.Lock()
_ID_NAMESPACE_LOCKS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
    weakref.WeakKeyDictionary()
)


async def id_namespace(ids: subids.SubordinateIds, timeout_s: float) -> int:
    """A descriptor of a user namespace that maps root to your uid and gid and 1 to N to ``ids``:
    this process's, made the first time with ``newuidmap`` and ``newgidmap``, each allowed
    ``timeout_s``, or the one it inherited. Raises :class:`SandboxStartError` when they refuse."""
    if (fd := _kept_id_namespace(ids)) is not None:
        return fd
    lock = _ID_NAMESPACE_LOCKS.setdefault(asyncio.get_running_loop(), asyncio.Lock())
    async with lock:
        if (fd := _kept_id_namespace(ids)) is None:
            async with _mapped_holder(ids, timeout_s) as pid:
                fd = os.open(f"/proc/{pid}/ns/user", os.O_RDONLY | os.O_CLOEXEC)
                # Kept at once: the holder's exit, which follows, may be cancelled.
                with _ID_NAMESPACES_LOCK:
                    kept, _ = _ID_NAMESPACES.setdefault(ids, (fd, _identity(fd)))
                if kept != fd:  # an event loop of another thread mapped them too
                    os.close(fd)
                    fd = kept
    return fd


def _kept_id_namespace(ids: subids.SubordinateIds) -> int | None:
    """The descriptor of the namespace of ``ids`` that this process keeps, while it still names
    that namespace: a process forked after the mapping inherits it, but may have closed it since,
    and its number may be another file's."""
    with _ID_NAMESPACES_LOCK:
        if (kept := _ID_NAMESPACES.get(ids)) is None:
            return None
        fd, identity = kept
        with contextlib.suppress(OSError):
            if _identity(fd) == identity:
                return fd
        del _ID_NAMESPACES[ids]  # not closed: the number is no longer ours
    return None


def _identity(fd: int) -> tuple[int, int]:
    status = os.fstat(fd)
    return status.st_dev, status.st_ino


async def give_back(
    path: Path,
    nsenter: str,
    *,
    holder: int | None = None,
    ids: subids.SubordinateIds | None = None,
    timeout_s: float = _GIVE_BACK_TIMEOUT_S,
) -> None:
    """Make your uid the owner again of everything below ``path`` that a subordinate id owns.

    What a service with ``run_as="subids"`` writes as another user belongs to one of your
    subordinate ids on the host, which you can neither change nor delete, nor delete a directory
    that holds it. :data:`GIVE_BACK_SCRIPT` runs as root of a user namespace that maps those ids
    as the pod's does, with the host's ``find`` and ``chown``: ``holder``'s (a live pod's holder),
    else this process's (:func:`id_namespace`, of ``ids``, by default :func:`subids.find`'s).
    Run it only once nothing in the pod runs any more. Raises :class:`SandboxError` when it could
    not run.
    """
    if holder is not None:
        namespace = f"/proc/{holder}/ns/user"
    else:
        fd = await id_namespace(ids or subids.find(), timeout_s)
        namespace = f"/proc/{os.getpid()}/fd/{fd}"
    argv = [
        nsenter,
        f"--user={namespace}",
        "--preserve-credentials",
        "--",
        "/bin/sh",
        "-c",
        GIVE_BACK_SCRIPT,
        "hpc-sandbox-give-back",
        str(path),
    ]
    environment = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LC_ALL": "C"}
    result = await run_process(argv, timeout_s=timeout_s, env=environment)
    if result.returncode != 0 or result.timed_out:
        raise SandboxError(
            f"could not give back what the pod's other users left in {path}: {result.detail()}"
        )


@contextlib.asynccontextmanager
async def _mapped_holder(ids: subids.SubordinateIds, timeout_s: float) -> AsyncIterator[int]:
    """A holder whose user namespace ``newuidmap`` and ``newgidmap`` map with ``ids``, for as long
    as the block runs; it yields the holder's PID."""
    ours, theirs = socket.socketpair()
    output = tempfile.TemporaryFile()
    try:
        holder = subprocess.Popen(  # noqa: ASYNC220 - a fork and exec
            [
                sys.executable,
                "-I",
                "-S",
                str(HOLDER_SCRIPT),
                str(os.getpid()),
                str(theirs.fileno()),
                "subordinate-ids",
                "subids",
            ],
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            pass_fds=[theirs.fileno()],
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
            start_new_session=True,
        )
    except BaseException:
        ours.close()
        output.close()
        raise
    finally:
        theirs.close()
    loop = asyncio.get_running_loop()

    async def expect(expected: str) -> None:
        try:
            answer = await _read_answer(ours, _CONTROL_TIMEOUT_S)
        except TimeoutError:
            answer = "nothing in time"
        if answer is None or not answer.startswith(expected):
            output.seek(0)
            detail = output.read().decode(errors="replace").strip() or answer or "no output"
            raise SandboxStartError(f"could not map your subordinate ids: {detail}")

    try:
        ours.setblocking(False)
        await expect("unshared")
        await subids.map_ids(holder.pid, ids, timeout_s)
        await loop.sock_sendall(ours, b"mapped\nstart\n")
        await expect("ready")
        yield holder.pid
    finally:
        ours.close()  # the holder exits when its control socket closes
        if await poll(holder.poll, _KILL_WAIT_S) is None:
            holder.kill()
            await poll(holder.poll, _KILL_WAIT_S)
        output.close()


async def _read_answer(control: socket.socket, timeout_s: float) -> str | None:
    """A holder's next answer on its control socket, or None if it closed it; raises
    :class:`TimeoutError` after ``timeout_s``."""
    loop = asyncio.get_running_loop()
    buffer = b""
    async with asyncio.timeout(timeout_s):
        while b"\n" not in buffer:
            chunk = await loop.sock_recv(control, 4096)
            if not chunk:
                return None
            buffer += chunk
    return buffer.decode(errors="replace").strip()


def add_apt_drop_in(rootfs: Path) -> None:
    """Let apt run as root of a user namespace with one id: no privilege drop to _apt.

    Only for Debian-based rootfs; written without following links, as the
    image may hold any.
    """
    from . import safefs

    if not (rootfs / "etc" / "debian_version").is_file():
        return
    try:
        with safefs.opened_dir(rootfs / "etc") as etc:
            apt = safefs.make_dir("apt", etc, mode=0o755)
            try:
                conf = safefs.make_dir("apt.conf.d", apt, mode=0o755)
                try:
                    with contextlib.suppress(FileNotFoundError):
                        os.unlink(APT_DROP_IN_NAME, dir_fd=conf)
                    safefs.write_file(APT_DROP_IN_NAME, conf, APT_DROP_IN.encode(), mode=0o644)
                finally:
                    os.close(conf)
            finally:
                os.close(apt)
    except OSError as error:
        log.warning("could not let apt run as root in %s: %s", rootfs, error)


def _limit(value: int) -> str:
    return "unlimited" if value < 0 else str(value)


def write_and_close(stream: Any, data: bytes) -> None:
    try:
        stream.write(data)
        stream.flush()
    finally:
        stream.close()


def _contains(path: Path, text: str) -> bool:
    try:
        with path.open("rb") as stream:
            return text.encode() in stream.read(64 * 1024)
    except OSError:
        return False


def _tail(path: Path, limit: int = 2000) -> str:
    try:
        return path.read_text(errors="replace")[-limit:].strip() or "no output"
    except OSError:
        return "no output"


async def _finish(task: asyncio.Future[None]) -> None:
    from .sandbox import _finish as finish

    await finish(task)
