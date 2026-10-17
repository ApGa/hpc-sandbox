"""Pods: services sharing one network, like a docker-compose project.

A pod is a holder process (:mod:`hpc_sandbox.holder`) that owns a user
namespace mapping the caller to root and a network namespace with only an up
loopback, plus one container per service:

- **Services.** Each service is a sandbox whose container runs under the
  launcher's ``join`` form (:mod:`hpc_sandbox.anchor`): it joins the pod's user
  and network namespaces, gets mount, PID, IPC and UTS namespaces of its own
  (compose's defaults), its hostname, resource limits and a sized
  ``/dev/shm``, and an init process of ours as PID 1 that runs the runtime as
  its child, passes SIGTERM on and exits with the command's status. The
  runtime puts the container in a nested user namespace, where root holds no
  capability over the pod's network: it can neither reconfigure it nor sniff
  it, as under Docker.
- **Names.** Every service resolves the names of all services, and its own
  hostname, to 127.0.0.1 through an ``/etc/hosts`` of its own, extended as
  services join; ``/etc/resolv.conf`` names slirp4netns's resolver.
- **Commands** enter a service in two steps: the pod's user and network
  namespaces, then the container's own namespaces, so that they end as root
  of the container's nested user namespace.
- **Upstreams.** The holder relays a pod port per upstream to a Unix socket
  on the host. It binds before any service starts, without ``SO_REUSEPORT``,
  so no service can take the port over.
- **Egress** goes through slirp4netns with ``--disable-host-loopback``, whose
  exit pipe only the holder holds: it dies with the holder.
- **Lifetime.** The holder, the launchers and their inits die with the
  process that started them; the pod's session directory holds the services'
  session directories, so the janitor removes pods whole.
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
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self

from . import procfs
from .process import kill_tree
from .sandbox import SessionSandbox, check_env, environment_script, poll
from .sessions import Session, create_session, new_instance_name, trash_session_awaiting
from .types import (
    HealthCheck,
    PodNetwork,
    PodSpec,
    SandboxError,
    SandboxLostError,
    SandboxSnapshot,
    SandboxStartError,
    ServiceHealth,
    ServiceSpec,
)

if TYPE_CHECKING:
    from .sandbox import SessionProvider

log = logging.getLogger(__name__)

HOLDER_SCRIPT = Path(__file__).with_name("holder.py")
ANCHOR_SCRIPT = Path(__file__).with_name("anchor.py")
SERVICES_DIRECTORY = "services"
HOLDER_LOG = "holder.log"
SLIRP_LOG = "slirp4netns.log"
SERVICE_LOG = "service.log"
FIRST_UPSTREAM_PORT = 18080
DEFAULT_SHM_MB = 64  # Docker's default /dev/shm
NAMESERVER = "10.0.2.3"  # slirp4netns's DNS forwarder
LOG_TAIL_BYTES = 1 << 20
READY = "hpc-sandbox-ready"
# Run by /bin/sh -c as the service's container command, with its name, the
# working directory and the command as arguments: announce that the container
# is up, take the environment from the first line of stdin (see
# environment_script), enter the working directory, creating it as Docker
# does, and run the command with stdin from /dev/null.
SERVICE_START = """\
echo hpc-sandbox-ready
hpc_sandbox_nl='
'
IFS= read -r hpc_sandbox_env || exit 125
eval "$hpc_sandbox_env" || exit 125
exec </dev/null
mkdir -p -- "$1" 2>/dev/null
cd -- "$1" || exit 125
shift
exec "$@"
"""
# The command of a service without one: it stays up, idle, for exec.
SERVICE_KEEPALIVE = """\
trap '' HUP INT QUIT TERM USR1 USR2
command -v sleep >/dev/null 2>&1 || { echo "hpc-sandbox: the image has no sleep" >&2; exit 127; }
exec >/dev/null 2>&1
while :; do sleep 86400 & wait $!; done
"""
APT_DROP_IN = 'APT::Sandbox::User "root";\n'
APT_DROP_IN_NAME = "99hpc-sandbox-aptfix"
_SERVICE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,62}")
_UPSTREAM_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*")
_CONTROL_TIMEOUT_S = 30.0
_UNIX_PATH_MAX = 108  # sun_path, with its terminating NUL
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
    return ports


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

    def __init__(self, provider: SessionProvider[Any], spec: PodSpec) -> None:
        self._id = new_instance_name()
        self.spec = spec
        self._ports = check_pod(spec)
        self._routes = {name: f"http://127.0.0.1:{port}" for name, port in self._ports.items()}
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
        self._provider._check(spec.sandbox, service=True)
        self._names.append(spec.name)
        task = asyncio.ensure_future(self._start_service(spec, wait))
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
            self._session = await create_session(provider.pod_root, self.id)
            self.services_root.mkdir(mode=0o700)
            self._record()
            await self._start_holder()
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
            holder_pid=self._holder.pid if self._holder else None,
            holder_start_time=self._holder_start_time,
            slirp_pid=self._slirp.pid if self._slirp else None,
            slirp_start_time=self._slirp_start_time,
        )

    async def _start_holder(self) -> None:
        assert self._session is not None
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
        buffer = b""
        deadline = time.monotonic() + _CONTROL_TIMEOUT_S
        while b"\n" not in buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:  # its answer may come later, out of step: give up on it
                self._control = None
                control.close()
                raise SandboxLostError(f"the holder of pod {self.id} did not answer {request!r}")
            try:
                async with asyncio.timeout(remaining):
                    chunk = await loop.sock_recv(control, 4096)
            except TimeoutError:
                continue
            if not chunk:
                raise SandboxLostError(f"the holder of pod {self.id} exited: {self._holder_log()}")
            buffer += chunk
        answer = buffer.decode(errors="replace").strip()
        if answer.startswith("error") or not answer.startswith(expected):
            raise SandboxError(f"the holder of pod {self.id} answered {answer!r}")
        return answer

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
            self._record()

    def _slirp_log(self) -> str:
        assert self._session is not None
        try:
            return (self._session.path / SLIRP_LOG).read_text(errors="replace")[-2000:].strip()
        except OSError:
            return "no output"

    # Services.

    async def _start_service(self, spec: ServiceSpec, wait: str) -> PodService:
        provider = self._provider
        service = provider.service_type(provider, self, spec)
        await asyncio.to_thread(self._announce, spec.name)
        image = await provider.images.resolve(spec.sandbox.image)
        await service._boot(lambda session: provider._extract_service(image, session))
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
                        f"service {spec.name!r} of pod {self.id} is {status}: {detail or 'no output'}"
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

    def _write_hosts(self, service: PodService) -> None:
        """Write a new service's /etc/hosts; from now on it hears of names that join."""
        with self._hosts_lock:
            names = [name for name in list(self._names) if name != service.name]
            service.hosts_path.write_text(hosts_file([*names, service.name]))
            self._hosts[service.name] = service

    def _forget(self, name: str) -> None:
        """Take a service that failed to start out of the other services' /etc/hosts."""
        with self._hosts_lock:
            self._hosts.pop(name, None)
            for other, service in self._hosts.items():
                names = [n for n in list(self._names) if n != other]
                service.rewrite_hosts(hosts_file([*names, other]))

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
        grace = self._provider.service_stop_grace_s
        commands = [s for s in self._services.values() if s.service.command is not None]
        if grace > 0 and commands:  # an idle loop has nothing to finish
            for service in commands:
                service.terminate()
            deadline = time.monotonic() + grace
            # Until the launchers report the statuses: the command exits before its status gets
            # up through the runtime's processes and the init, which a stop would kill.
            while time.monotonic() < deadline and any(s.exit_status is None for s in commands):
                await asyncio.sleep(0.05)
        results = await asyncio.gather(
            *(service.stop() for service in self._services.values()), return_exceptions=True
        )
        failures = [str(result) for result in results if isinstance(result, BaseException)]
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
    :meth:`_runtime_command`, :meth:`_runtime_environment` and
    :meth:`_prepare_service_rootfs`.
    """

    _tracked = False  # the pod tracks its services
    anchor_options: tuple[str, ...] = ()

    def __init__(self, provider: SessionProvider[Any], pod: SessionPod, service: ServiceSpec):
        super().__init__(provider, service.sandbox)  # type: ignore[arg-type]
        self.pod = pod
        self.service = service
        self.name = service.name
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

    # Files.

    @property
    def _files(self) -> Path:
        assert self._session is not None
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
        except (OSError, AssertionError) as error:
            log.warning("pod %s: could not add %s to %s: %s", self.pod.id, name, self.name, error)

    def rewrite_hosts(self, content: str) -> None:
        """Replace what the service's /etc/hosts says, in place: it is bound into the service."""
        try:
            fd = os.open(self.hosts_path, os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        except (OSError, AssertionError) as error:
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

    async def _discard(self, session: Session) -> None:
        if self.pod._stop_task is not None:  # the pod's deletion takes these files too
            session.release()
            return
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
        except (OSError, AssertionError):
            return ""

    # Starting.

    def _service_argv(self) -> list[str]:
        command = self.service.command or ["/bin/sh", "-c", SERVICE_KEEPALIVE]
        return ["/bin/sh", "-c", SERVICE_START, self.id, self.spec.workdir, *command]

    def _anchor_command(self) -> list[str]:
        shm_mb = self.service.shm_mb or DEFAULT_SHM_MB
        options = ["--hostname", self.name, "--shm", f"{self.shm_path}:{shm_mb * 1024 * 1024}"]
        for name, (soft, hard) in self._ulimits.items():
            options += ["--rlimit", f"{name}={soft}:{hard}"]
        return [
            sys.executable,
            "-I",
            "-S",
            str(ANCHOR_SCRIPT),
            str(os.getpid()),
            "join",
            str(self.pod.holder_pid),
            *options,
            *self.anchor_options,
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
            await asyncio.to_thread(_write_and_close, anchor.stdin, line)
        finally:
            if anchor.stdin is not None and not anchor.stdin.closed:
                anchor.stdin.close()
        self._record()

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
                container_start = procfs.owned_live_start_time(pid)
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
            and procfs.owned_live_start_time(container) == self._container_start
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
        }

    def terminate(self) -> None:
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
        container = self._container
        if (
            container is not None
            and procfs.owned_live_start_time(container) == self._container_start
        ):
            raise SandboxError(f"the container of service {self.name!r} survived teardown")
        self._exit_status = anchor.returncode
        self._service_anchor = None


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


def _write_and_close(stream: Any, data: bytes) -> None:
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
