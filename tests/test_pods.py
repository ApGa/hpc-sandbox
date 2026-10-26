"""Pods on the fake runtimes: real namespaces, holders, launchers and relays."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import dataclasses
import errno
import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from conftest import (
    FakeSubids,
    hide_from_path,
    make_enroot_provider,
    make_provider,
    namespaces_unavailable,
)
from hpc_sandbox import EnrootService, SandboxSpec, SingularityService, pods, sessions, subids
from hpc_sandbox.pods import check_pod, health_argv, hosts_file
from hpc_sandbox.procfs import owned_live_start_time
from hpc_sandbox.types import (
    HealthCheck,
    PodSpec,
    SandboxError,
    SandboxLostError,
    SandboxStartError,
    ServiceSpec,
)
from test_trash import janitor_lock_held

PYTHON = "/usr/bin/python3"
SERVER = textwrap.dedent(
    """\
    import http.server, os, sys
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = f"{os.environ.get('GREETING', 'ok')}\\n".encode()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *args):
            pass
    print("serving", flush=True)
    http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
    """
)
FETCH = (
    "import sys, urllib.request; "
    "print(urllib.request.urlopen(sys.argv[1], timeout=5).read().decode().strip())"
)
pytestmark = pytest.mark.skipif(not Path(PYTHON).exists(), reason=f"needs {PYTHON}")


@pytest.fixture(params=["enroot", "singularity"])
async def runtime(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[tuple[Any, str]]:
    """A provider on a fake runtime, and an image for it."""
    if reason := namespaces_unavailable():
        pytest.skip(reason)
    if request.param == "enroot":
        fake: Any = request.getfixturevalue("fake_enroot")
        image = fake.make_sqsh(tmp_path / "images" / "task.sqsh", {"app/hello.txt": "hello\n"})
        provider: Any = make_enroot_provider(tmp_path)
    else:
        fake = request.getfixturevalue("fake_runtime")
        image = fake.make_sif(tmp_path / "images" / "task.sif", {"app/hello.txt": "hello\n"})
        provider = make_provider(tmp_path)
    yield provider, str(image)
    for pod in provider.pods:
        await pod.stop()


@pytest.fixture
def gateway(tmp_path: Path) -> Iterator[Path]:
    """A Unix socket answering HTTP, standing in for an upstream service on the host, such as a
    model gateway."""
    path = tmp_path / "gw.sock"
    server = socket.socket(socket.AF_UNIX)
    server.bind(str(path))
    server.listen(16)

    def serve() -> None:
        while True:
            try:
                connection, _ = server.accept()
            except OSError:
                return
            with connection:
                connection.recv(4096)
                connection.sendall(b"HTTP/1.0 200 OK\r\n\r\ngateway-ok\n")

    threading.Thread(target=serve, daemon=True).start()
    yield path
    server.close()


def server(port: int) -> list[str]:
    return [PYTHON, "-c", SERVER, str(port)]


def fetch(url: str) -> list[str]:
    return [PYTHON, "-c", FETCH, url]


def ready(port: int) -> HealthCheck:
    return HealthCheck(
        ["CMD", *fetch(f"http://127.0.0.1:{port}/")],
        interval_s=0.2,
        start_period_s=30,
        start_interval_s=0.2,
    )


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


async def test_services_reach_each_other_by_name_and_the_upstream(runtime, gateway):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec(upstreams={"gateway": f"unix://{gateway}"}))
    assert pod.routes == {"gateway": "http://127.0.0.1:18080"}
    judge = ServiceSpec(
        "judge",
        SandboxSpec(image=image, env={"GREETING": "judge-ok"}),
        command=server(8082),
        healthcheck=ready(8082),
    )
    await pod.start_service(judge, wait="healthy")
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image, env={"A": "1"})))
    assert (await main.exec(fetch("http://judge:8082/"))).stdout == "judge-ok\n"
    assert (await main.exec(fetch("http://127.0.0.1:18080/"))).stdout == "gateway-ok\n"
    assert (await main.exec("hostname")).stdout == "main\n"
    assert (await main.exec("id -u")).stdout == "0\n"
    hosts = (await main.exec("cat /etc/hosts")).stdout
    assert "127.0.0.1\tjudge" in hosts and "127.0.0.1\tmain" in hosts
    assert (await main.exec("cat /etc/resolv.conf")).stdout == "nameserver 10.0.2.3\n"
    # Each service has its own environment, and nothing of the host's.
    env = (await main.exec("echo A=$A G=${GREETING-unset} H=${FAKE_ENROOT_STATE-unset}")).stdout
    assert env == "A=1 G=unset H=unset\n"
    assert (await pod.services["judge"].exec("echo $GREETING")).stdout == "judge-ok\n"
    # Commands see only their service's processes.
    processes = (await main.exec("ls /proc | grep -c '^[0-9]'")).stdout
    assert int(processes) < 10, processes
    assert await pod.health("judge") == "healthy" and await pod.health("main") == "healthy"
    assert "serving" in await pod.logs("judge")
    await pod.stop()


async def test_a_service_started_later_resolves_and_is_resolved(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    late = await pod.start_service(ServiceSpec("late", SandboxSpec(image=image)))
    assert (await main.exec("getent hosts late")).stdout.split()[0] == "127.0.0.1"
    assert (await late.exec("getent hosts main")).stdout.split()[0] == "127.0.0.1"
    await pod.stop()


# What `ifconfig lo down` asks of the kernel. In Python, since `ip` need not be on the PATH that the
# fake runtimes give commands: RHEL keeps it in /usr/sbin.
LOOPBACK_DOWN = (
    "import fcntl, socket, struct; "
    "fcntl.ioctl(socket.socket(), 0x8914, struct.pack('16sh22x', b'lo', 0))"  # SIOCSIFFLAGS
)


async def assert_no_hold_on_the_network(service: Any) -> None:
    """Commands in ``service`` can neither reconfigure the pod's network nor sniff it."""
    down = await service.exec([PYTHON, "-c", LOOPBACK_DOWN])
    assert down.exit_code != 0 and "not permitted" in down.stderr.lower(), down
    raw = await service.exec(
        [PYTHON, "-c", "import socket; socket.socket(socket.AF_PACKET, socket.SOCK_RAW)"]
    )
    assert raw.exit_code != 0 and "PermissionError" in raw.stderr, raw


# Each runtime with each run_as that it gives.
RUN_MODES = [
    (kind, run_as)
    for kind, service in (("enroot", EnrootService), ("singularity", SingularityService))
    for run_as in service.run_modes
]


@pytest.mark.parametrize(("runtime", "run_as"), RUN_MODES, indirect=["runtime"])
async def test_no_service_can_touch_the_pods_network(runtime, run_as, request):
    provider, image = runtime
    if run_as == "subids":
        request.getfixturevalue("fake_subids")  # stand-ins for newuidmap and newgidmap
    pod = await provider.start_pod(PodSpec(users="subids" if run_as == "subids" else "single"))
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image), run_as=run_as))
    await assert_no_hold_on_the_network(main)
    # What compose services expect of root anyway, and get as any user: privileged ports and ping.
    bind = "import socket; s = socket.socket(); s.bind(('127.0.0.1', 80)); print('bound')"
    assert (await main.exec([PYTHON, "-c", bind])).stdout == "bound\n"
    ping = "import socket; socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_ICMP)"
    assert (await main.exec([PYTHON, "-c", ping])).exit_code == 0
    await pod.stop()


async def test_upstream_ports_are_the_pods_own(runtime, gateway):
    provider, image = runtime
    port = free_port()
    spec = PodSpec(upstreams={"gateway": f"unix://{gateway}"}, upstream_ports={"gateway": port})
    first, second = await provider.start_pod(spec), await provider.start_pod(spec)
    a = await first.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    b = await second.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    for service in (a, b):  # the same port in two pods
        assert (await service.exec(fetch(f"http://127.0.0.1:{port}/"))).stdout == "gateway-ok\n"
    taken = f"import socket; socket.socket().bind(('127.0.0.1', {port}))"
    assert "Address already in use" in (await a.exec([PYTHON, "-c", taken])).stderr
    with socket.socket() as probe, pytest.raises(ConnectionRefusedError):
        probe.connect(("127.0.0.1", port))  # not on the host's loopback
    # A judge in one pod is not reachable from another.
    judge = ServiceSpec(
        "judge", SandboxSpec(image=image), command=server(8082), healthcheck=ready(8082)
    )
    await first.start_service(judge, wait="healthy")
    assert (await a.exec(fetch("http://127.0.0.1:8082/"))).stdout == "ok\n"
    assert (await b.exec(fetch("http://127.0.0.1:8082/"))).exit_code != 0
    await first.stop()
    await second.stop()


async def test_services_get_their_ulimits_and_a_sized_shm(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    spec = ServiceSpec(
        "main",
        SandboxSpec(image=image),
        shm_mb=16,
        ulimits={"nofile": (1000, 2000)},
    )
    main = await pod.start_service(spec)
    assert (await main.exec("echo $(ulimit -Sn) $(ulimit -Hn)")).stdout == "1000 2000\n"
    shm = await main.exec("df -k /dev/shm | tail -1")
    assert shm.stdout.split()[1] == str(16 * 1024), shm
    await pod.stop()


async def test_health_follows_compose_rules(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    slow = ServiceSpec(
        "slow",
        SandboxSpec(image=image),
        command=["sh", "-c", "sleep 1; touch /tmp/ready; exec sleep 600"],
        healthcheck=HealthCheck(
            ["CMD-SHELL", "test -e /tmp/ready"],
            interval_s=0.2,
            retries=1,
            start_period_s=30,
            start_interval_s=0.2,
        ),
    )
    await pod.start_service(slow, wait="healthy")  # failures in the start period do not count
    assert await pod.health("slow") == "healthy"
    broken = ServiceSpec(
        "broken",
        SandboxSpec(image=image),
        command=["sleep", "600"],
        healthcheck=HealthCheck("echo nope >&2; exit 1", interval_s=0.2, retries=2),
    )
    with pytest.raises(SandboxStartError, match="unhealthy: nope"):
        await pod.start_service(broken, wait="healthy")
    assert set(pod.services) == {"slow"}
    assert len(await leftovers(pod)) == 1  # slow's directory
    # The name is free again.
    broken.healthcheck = None
    await pod.start_service(broken, wait="healthy")
    await pod.stop()


async def test_health_says_exited_once_the_command_exits(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    job = ServiceSpec("job", SandboxSpec(image=image), command=["sh", "-c", "echo done; exit 3"])
    service = await pod.start_service(job)
    for _ in range(100):
        if await pod.health("job") == "exited" and service.exit_status is not None:
            break
        await asyncio.sleep(0.05)
    assert await pod.health("job") == "exited"
    assert service.exit_status == 3
    assert await pod.logs("job") == "done\n"
    with pytest.raises(SandboxLostError):
        await service.exec("true")
    failing = ServiceSpec(
        "failing",
        SandboxSpec(image=image),
        command=["sh", "-c", "exit 1"],
        healthcheck=HealthCheck("true", interval_s=5),
    )
    with pytest.raises(SandboxStartError, match="exited"):
        await pod.start_service(failing, wait="healthy")
    await pod.stop()


async def test_stop_leaves_nothing(runtime, gateway):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec(upstreams={"gateway": f"unix://{gateway}"}))
    await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    await pod.start_service(ServiceSpec("job", SandboxSpec(image=image), command=["sleep", "600"]))
    processes = [pod._holder.pid] + [
        pid
        for service in pod.services.values()
        for pid in (service._service_anchor.pid, service._service_init, service._container)
    ]
    path = pod.path
    await pod.stop()
    await pod.stop()  # idempotent
    assert not path.exists() and provider.pods == frozenset()
    assert [pid for pid in processes if owned_live_start_time(pid)] == []
    with pytest.raises(SandboxError, match="not running"):
        await pod.start_service(ServiceSpec("x", SandboxSpec(image=image)))


def hold_extraction(provider: Any, monkeypatch: pytest.MonkeyPatch) -> asyncio.Event:
    """Make service starts wait after extracting their rootfs; the event says one got there."""
    extract, reached = provider._extract_service, asyncio.Event()

    async def held(image: Any, session: Any) -> None:
        await extract(image, session)
        reached.set()
        await asyncio.sleep(3600)

    monkeypatch.setattr(provider, "_extract_service", held)
    return reached


async def leftovers(pod: Any) -> list[str]:
    """Service sessions in the pod's directory, and what its trash still holds once
    the deletions this process started are done."""
    assert await pod._provider.wait_for_deletions(30)
    root = pod.services_root
    sessions = [p.name for p in root.iterdir() if p.name.startswith("hpcsb-")]
    trash = [p.name for p in (root / ".trash").iterdir()] if (root / ".trash").is_dir() else []
    return sorted(sessions + trash)


async def test_a_cancelled_start_leaves_nothing(runtime, monkeypatch):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    with monkeypatch.context() as patch:
        reached = hold_extraction(provider, patch)
        task = asyncio.ensure_future(
            pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
        )
        await reached.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert pod.services == {} and await leftovers(pod) == []
    # Cancelled while waiting for health: the running service goes too.
    never = HealthCheck("exit 1", interval_s=0.1, start_period_s=3600, start_interval_s=0.1)
    spec = ServiceSpec(
        "main", SandboxSpec(image=image), command=["sleep", "600"], healthcheck=never
    )
    task = asyncio.ensure_future(pod.start_service(spec, wait="healthy"))
    await asyncio.sleep(1)
    assert "main" in pod.services
    anchor = pod.services["main"]._service_anchor
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert pod.services == {} and await leftovers(pod) == [] and anchor.poll() is not None
    await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))  # the name is free
    await pod.stop()


async def test_stopping_a_pod_during_a_start_stops_the_start(runtime, monkeypatch):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    reached = hold_extraction(provider, monkeypatch)
    task = asyncio.ensure_future(pod.start_service(ServiceSpec("main", SandboxSpec(image=image))))
    await reached.wait()
    path = pod.path
    await pod.stop()
    with pytest.raises(SandboxStartError, match="stopped"):
        await task
    assert not path.exists()


async def test_calls_after_the_holder_died_raise_lost(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    pod._holder.kill()
    pod._holder.wait()
    with pytest.raises(SandboxLostError):
        await main.exec("true")
    with pytest.raises(SandboxLostError):
        await pod.start_service(ServiceSpec("other", SandboxSpec(image=image)))
    await pod.stop()


async def test_reap_stops_pods_by_label(runtime):
    provider, image = runtime
    first = await provider.start_pod(PodSpec(labels={"rollout": "a"}))
    second = await provider.start_pod(PodSpec(labels={"rollout": "b"}))
    await first.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    report = await provider.reap({"rollout": "a"})
    assert report.stopped == [first.id] and not report.errors
    assert provider.pods == {second}
    await second.stop()


CRASH = """\
import asyncio, os, signal, sys
sys.path.insert(0, {src!r})
from hpc_sandbox import EnrootProvider, SingularityProvider, SandboxSpec
from hpc_sandbox.types import PodSpec, ServiceSpec

async def main():
    kind, scratch, cache, image = sys.argv[1:]
    providers = {{"enroot": EnrootProvider, "singularity": SingularityProvider}}
    provider = providers[kind](scratch_root=scratch, image_cache=cache, stage_images=False)
    pod = await provider.start_pod(PodSpec(labels={{"t": "crash"}}))
    service = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    print(pod.id, pod._holder.pid, service._service_anchor.pid, service._container, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

asyncio.run(main())
"""


async def test_the_janitor_removes_the_pods_of_a_dead_owner(runtime, tmp_path):
    provider, image = runtime
    kind = "enroot" if type(provider).__name__.startswith("Enroot") else "singularity"
    src = str(Path(__file__).resolve().parents[1] / "src")
    script = CRASH.format(src=src)
    command = [sys.executable, "-c", script, kind, str(provider.scratch_root)]
    result = await asyncio.to_thread(
        subprocess.run,
        [*command, str(tmp_path / "cache"), image],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    pod_id, *pids = result.stdout.split()
    await asyncio.sleep(0.5)  # the parent-death signals
    assert [pid for pid in map(int, pids) if owned_live_start_time(pid)] == []
    assert (provider.pod_root / pod_id).is_dir()  # files only
    report = await provider.reap()
    assert pod_id in report.removed and not report.errors
    assert not (provider.pod_root / pod_id).exists()


async def test_the_pods_janitor_deletes_nothing_under_the_pod_roots_lock(
    runtime, tmp_path, monkeypatch
):
    provider, image = runtime
    kind = "enroot" if type(provider).__name__.startswith("Enroot") else "singularity"
    src = str(Path(__file__).resolve().parents[1] / "src")
    command = [sys.executable, "-c", CRASH.format(src=src), kind, str(provider.scratch_root)]
    result = await asyncio.to_thread(
        subprocess.run,
        [*command, str(tmp_path / "cache"), image],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    pod_id = result.stdout.split()[0]
    await asyncio.sleep(0.5)  # the parent-death signals
    deleted = []
    original = sessions.delete_tree

    async def recording(path: Path) -> None:
        deleted.append((path.name.split(".")[0], janitor_lock_held(provider.pod_root)))
        await original(path)

    monkeypatch.setattr(sessions, "delete_tree", recording)
    report = await provider.reap()
    assert pod_id in report.removed and not report.errors
    # The pod goes whole, services and all, once its root's janitor lock is released.
    assert deleted == [(pod_id, False)]


def test_pod_spec_validation():
    assert check_pod(PodSpec(upstreams={"a": "unix:///a", "b": "unix:///b"})) == {
        "a": 18080,
        "b": 18081,
    }
    ports = check_pod(
        PodSpec(upstreams={"a": "unix:///a", "b": "unix:///b"}, upstream_ports={"a": 18081})
    )
    assert ports == {"a": 18081, "b": 18080}
    for bad in (
        PodSpec(network="host"),  # type: ignore[arg-type]
        PodSpec(upstreams={"a": "http://x"}),
        PodSpec(upstreams={"a": "unix://relative"}),
        PodSpec(upstreams={"a": "unix:///a"}, upstream_ports={"b": 1}),
        PodSpec(upstreams={"a": "unix:///a", "b": "unix:///b"}, upstream_ports={"a": 9, "b": 9}),
        PodSpec(upstreams={"a": "unix:///a"}, upstream_ports={"a": 70000}),
    ):
        with pytest.raises(ValueError):
            check_pod(bad)


def test_healthcheck_tests_read_like_compose():
    assert health_argv(HealthCheck(["CMD", "curl", "-f", "x"])) == ["curl", "-f", "x"]
    assert health_argv(HealthCheck(["CMD-SHELL", "a && b"])) == ["/bin/sh", "-c", "a && b"]
    assert health_argv(HealthCheck("a && b")) == ["/bin/sh", "-c", "a && b"]
    assert health_argv(HealthCheck(["NONE"])) is None
    for bad in ([], ["CMD"], ["CMD-SHELL", "a", "b"], ["RUN", "x"]):
        with pytest.raises(ValueError):
            health_argv(HealthCheck(bad))


def test_hosts_files_name_every_service():
    assert hosts_file(["judge", "main"]) == (
        "127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n"
        "127.0.0.1\tjudge\n127.0.0.1\tmain\n"
    )


async def test_service_spec_validation(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    for bad in (
        ServiceSpec("bad name", SandboxSpec(image=image)),
        ServiceSpec("main", SandboxSpec(image=image), command=[]),
        ServiceSpec("main", SandboxSpec(image=image), shm_mb=0),
        ServiceSpec("main", SandboxSpec(image=image), ulimits={"files": (1, 1)}),
        ServiceSpec("main", SandboxSpec(image=image), ulimits={"nofile": (10, 5)}),
        ServiceSpec("main", SandboxSpec(image=image), healthcheck=HealthCheck("x", retries=0)),
        ServiceSpec("main", SandboxSpec(image=image, workdir="relative")),
    ):
        with pytest.raises(ValueError):
            await pod.start_service(bad)
    await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    with pytest.raises(ValueError, match="already has"):
        await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    await pod.stop()


FAKE_SLIRP = """\
#!{python}
import json, os, sys
args = sys.argv[1:]
if args == ["--help"]:
    print("--disable-host-loopback --enable-sandbox --enable-seccomp --exit-fd --ready-fd")
    sys.exit(0)
with open({record!r}, "a") as out:
    out.write(json.dumps(args) + "\\n")
flags = dict(a.split("=", 1) for a in args if a.startswith("--") and "=" in a)
os.write(int(flags["--ready-fd"]), b"1")
while os.read(int(flags["--exit-fd"]), 1):
    pass
"""


@pytest.fixture
def fake_slirp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    record = tmp_path / "slirp-calls.jsonl"
    path = tmp_path / "slirp-bin" / "slirp4netns"
    path.parent.mkdir()
    path.write_text(FAKE_SLIRP.format(python=sys.executable, record=str(record)))
    path.chmod(0o755)
    monkeypatch.setenv("PATH", f"{path.parent}{os.pathsep}{os.environ['PATH']}")
    return record


async def test_egress_runs_slirp4netns_held_by_the_holder(runtime, fake_slirp):
    provider, _ = runtime
    pod = await provider.start_pod(PodSpec(network="egress"))
    (call,) = [json.loads(line) for line in fake_slirp.read_text().splitlines()]
    assert {"--disable-host-loopback", "--enable-sandbox", "--enable-seccomp"} <= set(call)
    assert call[-2:] == [str(pod._holder.pid), "tap0"]
    slirp = pod._slirp
    await pod.set_network("none")  # the holder lets go of the exit pipe
    assert slirp.poll() == 0 and pod.network == "none"
    await pod.set_network("egress")
    slirp = pod._slirp
    pod._holder.kill()  # slirp4netns dies with the holder
    assert await asyncio.to_thread(slirp.wait, 10) == 0
    await pod.stop()


async def test_egress_without_slirp4netns_fails_before_starting(runtime, monkeypatch, tmp_path):
    provider, _ = runtime
    hide_from_path(monkeypatch, "slirp4netns")  # the host's
    monkeypatch.setenv("HPC_SANDBOX_TOOLS", str(tmp_path / "no-tools"))  # and the pinned one
    with pytest.raises(SandboxError, match="fetch-tools"):
        await provider.start_pod(PodSpec(network="egress"))
    assert not provider.pod_root.exists() or list(provider.pod_root.iterdir()) in (
        [],
        [provider.pod_root / ".janitor.lock"],
    )


# Regression tests for pod edge cases: the upstream relays, service names, output and exit
# statuses, limits, cancelled starts, sweeps, and stops.

HALF_CLOSE = (
    "import socket, sys; s = socket.create_connection(('127.0.0.1', int(sys.argv[1])));"
    " s.sendall(b'GET / HTTP/1.0\\r\\n\\r\\n'); s.shutdown(socket.SHUT_WR);"
    " print(s.recv(4096).decode().strip().splitlines()[-1])"
)
TAKE_IPV6 = (
    "import socket; s = socket.socket(socket.AF_INET6); s.bind(('::1', 18080)); print('taken')"
)


async def test_the_relay_keeps_the_reply_of_a_half_closed_request(runtime, gateway):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec(upstreams={"gateway": f"unix://{gateway}"}))
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    assert (await main.exec([PYTHON, "-c", HALF_CLOSE, "18080"])).stdout == "gateway-ok\n"
    await pod.stop()


async def test_no_service_can_take_a_relay_port_on_ipv6(runtime, gateway):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec(upstreams={"gateway": f"unix://{gateway}"}))
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    taken = await main.exec([PYTHON, "-c", TAKE_IPV6])
    assert taken.exit_code != 0 and "Address already in use" in taken.stderr, taken
    assert (await main.exec(fetch("http://localhost:18080/"))).stdout == "gateway-ok\n"
    await pod.stop()


async def test_services_starting_together_resolve_each_other(runtime, monkeypatch):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    written, release = asyncio.Event(), asyncio.Event()
    start_container = provider.service_type._start_container

    async def held(self):
        if self.name == "first":  # its /etc/hosts exists; it is not registered yet
            written.set()
            await release.wait()
        await start_container(self)

    monkeypatch.setattr(provider.service_type, "_start_container", held)
    first = asyncio.ensure_future(pod.start_service(ServiceSpec("first", SandboxSpec(image=image))))
    await written.wait()
    second = await pod.start_service(ServiceSpec("second", SandboxSpec(image=image)))
    release.set()
    first = await first
    assert (await first.exec("getent hosts second")).exit_code == 0
    assert (await second.exec("getent hosts first")).exit_code == 0
    await pod.stop()


async def test_a_failed_start_leaves_no_name_behind(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    ghost = ServiceSpec(
        "ghost",
        SandboxSpec(image=image),
        command=["sleep", "600"],
        healthcheck=HealthCheck("exit 1", interval_s=0.1, retries=1),
    )
    with pytest.raises(SandboxStartError):
        await pod.start_service(ghost, wait="healthy")
    assert "ghost" not in (await main.exec("cat /etc/hosts")).stdout
    await pod.stop()


async def test_output_goes_through_a_pipe(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    talker = ServiceSpec(
        "talker",
        SandboxSpec(image=image),
        command=[
            "sh",
            "-c",
            "echo before; echo err > /dev/stderr; head -c 300000 /dev/zero | tr '\\0' x; "
            "echo; echo after; exec sleep 600",
        ],
        ulimits={"fsize": (100000, 100000)},  # files only; output is no file
    )
    await pod.start_service(talker)
    for _ in range(100):
        if "after" in (logs := await pod.logs("talker")):
            break
        await asyncio.sleep(0.05)
    assert logs.startswith("before\nerr\n") and logs.endswith("after\n"), logs[:100]
    assert await pod.health("talker") == "healthy"
    await pod.stop()


async def test_exit_status_is_known_once_health_says_exited(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    for index in range(5):
        job = await pod.start_service(
            ServiceSpec(f"job{index}", SandboxSpec(image=image), command=["sh", "-c", "exit 7"])
        )
        while await pod.health(job.name) != "exited":
            await asyncio.sleep(0.001)
        assert job.exit_status == 7
    await pod.stop()


async def test_stop_does_not_wait_for_idle_services(runtime):
    provider, image = runtime
    provider.service_stop_grace_s = 5.0
    pod = await provider.start_pod(PodSpec())
    await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    await pod.start_service(ServiceSpec("job", SandboxSpec(image=image), command=["sleep", "600"]))
    started = time.monotonic()
    await pod.stop()
    assert time.monotonic() - started < 3


async def test_nproc_is_accepted_but_not_applied_and_snapshots_are_refused(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    spec = ServiceSpec("main", SandboxSpec(image=image), ulimits={"nproc": (5, 5), "locks": (1, 1)})
    main = await pod.start_service(spec)
    many = await main.exec("for i in 1 2 3 4 5 6 7 8 9 10; do sleep 1 & done; wait; echo done")
    assert many.stdout == "done\n", many
    with pytest.raises(SandboxError, match="not supported"):
        await main.snapshot()
    await pod.stop()


def test_upstream_socket_paths_must_fit():
    with pytest.raises(ValueError, match="longer than"):
        check_pod(PodSpec(upstreams={"a": "unix:///" + "x" * 120}))


async def test_a_cancelled_start_while_the_janitor_lock_is_held_leaves_nothing(runtime):
    provider, _ = runtime
    first = await provider.start_pod(PodSpec())  # the scratch exists
    await first.stop()
    lock = os.open(provider.pod_root / ".janitor.lock", os.O_RDWR)
    fcntl.flock(lock, fcntl.LOCK_EX)  # as a sweep in another process would
    fds = len(os.listdir("/proc/self/fd"))
    try:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.5):
                await provider.start_pod(PodSpec())
    finally:
        os.close(lock)
    await asyncio.sleep(0.2)
    assert [p.name for p in provider.pod_root.iterdir() if p.name.startswith("hpcsb-")] == []
    assert len(os.listdir("/proc/self/fd")) <= fds


async def test_the_janitor_removes_pod_directories_without_records(runtime):
    provider, _ = runtime
    pod = await provider.start_pod(PodSpec())
    await pod.stop()
    stray = provider.pod_root / "hpcsb-0123456789abcdef0123456789abcdef"
    stray.mkdir(mode=0o700)
    report = await provider.reap()
    assert stray.name in report.removed and not stray.exists()


async def test_the_janitor_clears_a_pod_without_services_on_a_full_scratch(runtime, monkeypatch):
    provider, _ = runtime
    session = sessions.open_session(provider.pod_root, sessions.new_instance_name())
    session.write_owner_record(provider=provider.name, kind="pod", labels={})
    record = json.loads((session.path / "owner.json").read_text())
    record["owner_start_time"] = "0"  # its owner died before it started a service
    (session.path / "owner.json").write_text(json.dumps(record))
    (session.path / "services").mkdir(mode=0o700)
    session.release()
    original = sessions.locked

    def full(path: Path, **options):  # no room for a new file, such as the services' lock
        if path.parent.name == "services" and not path.exists():
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC), str(path))
        return original(path, **options)

    monkeypatch.setattr(sessions, "locked", full)
    report = await provider.reap()
    assert (report.removed, report.errors) == ([session.name], [])
    assert await provider.wait_for_deletions(30) and not session.path.exists()


async def test_a_sweep_and_many_stops_do_not_wait_for_each_other(runtime, tmp_path):
    # A sweep holds the janitor lock while it needs worker threads; stops must
    # not hold all of those threads waiting for that lock.
    provider, _ = runtime
    loop = asyncio.get_running_loop()
    loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=2))
    pods = [await provider.start_pod(PodSpec()) for _ in range(4)]
    stray = provider.pod_root / "hpcsb-fedcba9876543210fedcba9876543210"
    stray.mkdir(mode=0o700)
    async with asyncio.timeout(60):
        await asyncio.gather(provider.reap(), *(pod.stop() for pod in pods))


HOG = (
    "import time; data = bytearray(150 << 20); data[::4096] = b'x' * len(data[::4096]); "
    "time.sleep(600)"
)


async def test_the_watchdog_spares_the_runtime_between_the_init_and_the_service(runtime):
    # Background hogs an exec leaves behind are adopted by the container's
    # init (Singularity's shim): they may die, the service may not.
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image, memory_mb=250)))
    await main.exec(f'for i in 1 2; do nohup {PYTHON} -c "{HOG}" >/dev/null 2>&1 & done')
    for _ in range(100):
        if main.memory_kills:
            break
        await asyncio.sleep(0.1)
    assert main.memory_kills and main.memory_kills[0].processes == 1
    assert await pod.health("main") == "healthy"
    assert (await main.exec("echo alive")).stdout == "alive\n"
    await pod.stop()


async def test_a_graceful_stop_reaches_the_command_and_statuses_are_shell_like(
    runtime, monkeypatch
):
    provider, image = runtime
    provider.service_stop_grace_s = 5.0
    # A status takes a while up through Singularity's processes: the stop must wait for it.
    monkeypatch.setenv("FAKE_SINGULARITY_EXIT_DELAY", "1")
    pod = await provider.start_pod(PodSpec())
    polite = ServiceSpec(
        "polite",
        SandboxSpec(image=image),
        command=["sh", "-c", "trap 'exit 3' TERM; while :; do sleep 0.1; done"],
    )
    service = await pod.start_service(polite)
    victim = await pod.start_service(
        ServiceSpec("victim", SandboxSpec(image=image), command=["sleep", "600"])
    )
    os.kill(victim._service_anchor.pid, signal.SIGKILL)  # the launcher itself
    while victim.exit_status is None:
        await asyncio.sleep(0.01)
    assert victim.exit_status == 137
    started = time.monotonic()
    await pod.stop()
    assert service.exit_status == 3 and time.monotonic() - started < 4
    assert victim.exit_status == 137  # still, once stopped


async def test_a_pod_stopping_gracefully_refuses_new_commands_meanwhile(runtime):
    provider, image = runtime
    provider.service_stop_grace_s = 10.0
    pod = await provider.start_pod(PodSpec())
    slow = ["sh", "-c", "trap 'sleep 2; exit 3' TERM; while :; do sleep 0.1; done"]
    await pod.start_service(ServiceSpec("slow", SandboxSpec(image=image), command=slow))
    idle = await pod.start_service(ServiceSpec("idle", SandboxSpec(image=image)))
    stopping = asyncio.ensure_future(pod.stop())
    await asyncio.sleep(0.5)
    with pytest.raises(SandboxError, match="being stopped"):
        await idle.exec("true")
    await stopping


async def test_an_exec_running_while_its_pod_stops_gracefully_was_stopped_not_lost(runtime):
    provider, image = runtime
    provider.service_stop_grace_s = 5.0
    pod = await provider.start_pod(PodSpec())
    polite = ServiceSpec(
        "polite",
        SandboxSpec(image=image),
        command=["sh", "-c", "trap 'exit 3' TERM; while :; do sleep 0.1; done"],
    )
    service = await pod.start_service(polite)
    running = asyncio.ensure_future(service.exec("sleep 30"))
    await asyncio.sleep(0.5)
    await pod.stop()
    with pytest.raises(SandboxError, match="stopped while the command ran") as caught:
        await running
    assert not isinstance(caught.value, SandboxLostError)


async def test_a_pod_whose_owner_record_cannot_be_written_still_stops_and_leaves_nothing(
    runtime, monkeypatch
):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    holder, path = pod._holder, pod.path

    def full(self: Any, **fields: Any) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(sessions.Session, "write_owner_record", full)
    await pod.stop()
    assert await provider.wait_for_deletions(30)
    assert holder.poll() is not None and not path.exists()


async def test_a_cancelled_network_switch_leaves_a_state_that_tells_the_truth(runtime, fake_slirp):
    provider, _ = runtime
    pod = await provider.start_pod(PodSpec(network="egress"))
    task = asyncio.ensure_future(pod.set_network("none"))
    await asyncio.sleep(0)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert pod.network == ("egress" if pod._slirp and pod._slirp.poll() is None else "none")
    await pod.set_network("egress")
    assert pod.network == "egress" and pod._slirp.poll() is None
    await pod.stop()


def test_a_service_rootfs_never_writes_through_a_linked_etc(tmp_path):
    from hpc_sandbox.singularity import SingularityService

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "hosts").symlink_to("/nowhere")
    rootfs = tmp_path / "rootfs"
    rootfs.mkdir()
    (rootfs / "etc").symlink_to(outside)
    service = SingularityService.__new__(SingularityService)
    service._session = type("S", (), {"rootfs": rootfs})()
    with pytest.raises(SandboxError, match="symlink"):
        service._prepare_service_rootfs()
    assert (outside / "hosts").is_symlink()  # untouched


# Users: services that run as the caller, and pods that map the caller's subordinate ids.


def is_enroot(provider: Any) -> bool:
    return type(provider).__name__.startswith("Enroot")


async def until(probe: Any, timeout_s: float = 30.0) -> None:
    """Wait until ``probe()`` (or the awaitable it returns) is true."""
    deadline = time.monotonic() + timeout_s
    while True:
        value = probe()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return
        assert time.monotonic() < deadline, "timed out"
        await asyncio.sleep(0.05)


# Pods with users="subids" need services that can switch users, which only Singularity's give.
on_singularity = pytest.mark.parametrize("runtime", ["singularity"], indirect=True)


async def test_a_caller_service_runs_as_your_uid_without_capabilities(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    job = await pod.start_service(
        ServiceSpec(
            "job",
            SandboxSpec(image=image),
            command=["sh", "-c", "id -u > /tmp/uid; exec sleep 600"],
            run_as="caller",
        )
    )
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image), run_as="caller"))
    root = await pod.start_service(ServiceSpec("root", SandboxSpec(image=image)))
    me = [str(os.getuid()), str(os.getgid())]
    assert (await main.exec("id -u; id -g")).stdout.split() == me
    capabilities = (await main.exec("grep CapEff /proc/self/status")).stdout.split()[-1]
    assert int(capabilities, 16) == 0
    assert (await root.exec("id -u")).stdout == "0\n"
    root_capabilities = (await root.exec("grep CapEff /proc/self/status")).stdout.split()[-1]
    assert int(root_capabilities, 16) != 0
    uid_file = job.session_dir / "tmp" / "uid"
    await until(uid_file.exists)
    await until(lambda: uid_file.read_text().strip())
    assert uid_file.read_text() == f"{os.getuid()}\n"  # the command itself, not only exec
    assert (await main.exec("echo x > /tmp/mine && echo ok")).stdout == "ok\n"
    assert (main.session_dir / "tmp" / "mine").stat().st_uid == os.getuid()
    assert await pod.health("job") == "healthy" and await pod.health("main") == "healthy"
    record = json.loads((main.session_dir / "owner.json").read_text())
    assert (record["run_as"], record["remap_root"], record["container_uid"]) == (
        "caller",
        False,
        os.getuid(),
    )
    await pod.stop()


async def test_caller_services_run_the_runtime_as_your_uid(runtime, request):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    await pod.start_service(ServiceSpec("main", SandboxSpec(image=image), run_as="caller"))
    await pod.start_service(ServiceSpec("root", SandboxSpec(image=image)))
    if is_enroot(provider):  # without --root, enroot maps your uid to itself
        caller, root = request.getfixturevalue("fake_enroot").calls("start")[-2:]
        assert "--root" not in caller["argv"] and caller["env"]["ENROOT_REMAP_ROOT"] == "no"
        assert "--root" in root["argv"] and root["env"]["ENROOT_REMAP_ROOT"] == "yes"
    else:  # with --userns, Singularity does
        services = request.getfixturevalue("fake_runtime").calls("exec")
        caller, root = [call for call in services if "--containall" in call][-2:]
        assert caller[:2] == ["exec", "--userns"] and "--userns" not in root
    await pod.stop()


@on_singularity
async def test_a_subids_pod_maps_your_subordinate_ids_with_newuidmap(runtime, fake_subids):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec(users="subids"))
    start, count = str(FakeSubids.START), str(FakeSubids.COUNT)
    (uid_call, gid_call) = fake_subids.calls()
    # Once per process: the pod's user namespace is made in one that the tools mapped.
    mapped = uid_call[1]
    assert mapped != str(pod._holder.pid) and not owned_live_start_time(int(mapped))
    assert uid_call == ["newuidmap", mapped, "0", str(os.getuid()), "1", "1", start, count]
    assert gid_call == ["newgidmap", mapped, "0", str(os.getgid()), "1", "1", start, count]
    other = await provider.start_pod(PodSpec(users="subids"))
    assert len(fake_subids.calls()) == 2
    holders = [Path(f"/proc/{each._holder.pid}/ns/user").stat().st_ino for each in (pod, other)]
    assert holders[0] != holders[1]  # each pod has a namespace of its own
    await other.stop()
    assert json.loads((pod.path / "owner.json").read_text())["users"] == "subids"
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    caller = await pod.start_service(
        ServiceSpec("caller", SandboxSpec(image=image), run_as="caller")
    )
    assert (await main.exec("id -u")).stdout == "0\n"
    assert (await caller.exec("id -u")).stdout == f"{os.getuid()}\n"
    sub = await pod.start_service(ServiceSpec("sub", SandboxSpec(image=image), run_as="subids"))
    assert (await sub.exec("id -u")).stdout == "0\n"
    # Its namespace maps what the pod's maps, each id to itself: here only root, since no
    # unprivileged process may map more than its own id, as the fake tools do.
    assert (await sub.exec("cat /proc/self/uid_map")).stdout.split() == ["0", "0", "1"]
    assert json.loads((sub.session_dir / "owner.json").read_text())["run_as"] == "subids"
    await pod.stop()


async def test_pod_users_are_refused_without_what_they_need(runtime, fake_subids, monkeypatch):
    provider, image = runtime
    with pytest.raises(ValueError, match="users"):
        await provider.start_pod(PodSpec(users="all"))  # type: ignore[arg-type]
    pod = await provider.start_pod(PodSpec())
    subids_here = "run_as in" if is_enroot(provider) else "users='subids'"
    for bad, match in (
        (ServiceSpec("x", SandboxSpec(image=image), run_as="subids"), subids_here),
        (ServiceSpec("x", SandboxSpec(image=image), run_as="nobody"), "run_as must"),  # type: ignore[arg-type]
    ):
        with pytest.raises(ValueError, match=match):
            await pod.start_service(bad)
    assert pod.services == {}
    await pod.stop()

    async def subids_pod() -> None:
        await (await provider.start_pod(PodSpec(users="subids"))).stop()

    for refused in (subids_pod, provider.map_subordinate_ids):
        if is_enroot(provider):  # its containers map one id, so its pods map no more
            with pytest.raises(ValueError, match="no services with run_as='subids'"):
                await refused()
            continue
        with monkeypatch.context() as patch:
            patch.setattr(subids, "SUBGID", fake_subids.directory / "missing")
            with pytest.raises(SandboxError, match="no range of subordinate ids .*missing"):
                await refused()
        with monkeypatch.context() as patch:
            hide_from_path(patch, "newuidmap")
            with pytest.raises(SandboxError, match="newuidmap"):
                await refused()
    # Refused before anything started.
    assert fake_subids.calls() == []
    assert [p.name for p in provider.pod_root.iterdir() if p.name.startswith("hpcsb-")] == []


@on_singularity
async def test_a_pod_whose_ids_cannot_be_mapped_leaves_nothing(runtime, fake_subids):
    provider, _ = runtime
    fake_subids.fails.touch()
    with pytest.raises(SandboxStartError, match="uid_map failed: Operation not permitted"):
        await provider.start_pod(PodSpec(users="subids"))
    assert provider.pods == frozenset()
    assert await provider.wait_for_deletions(30)
    assert (
        not provider.pod_root.exists()
        or [p.name for p in provider.pod_root.iterdir() if p.name.startswith("hpcsb-")] == []
    )
    (holder,) = {int(call[1]) for call in fake_subids.calls()}
    assert not owned_live_start_time(holder)
    fake_subids.fails.unlink()  # a later pod tries again
    await (await provider.start_pod(PodSpec(users="subids"))).stop()


@pytest.fixture
def give_backs(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, bool]]:
    """Each give-back's directory and whether it ran in a live pod's holder."""
    calls: list[tuple[str, bool]] = []
    original = pods.give_back

    async def recording(path: Path, nsenter: str, **options: Any) -> None:
        calls.append((str(path), options.get("holder") is not None))
        await original(path, nsenter, **options)

    monkeypatch.setattr(pods, "give_back", recording)
    return calls


@on_singularity
async def test_a_subids_pod_gives_back_what_its_other_users_wrote(runtime, fake_subids, give_backs):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec(users="subids"))
    path = pod.path
    await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    # A subids service that stops before its pod: its own files.
    failing = ServiceSpec(
        "failing",
        SandboxSpec(image=image),
        command=["sleep", "600"],
        healthcheck=HealthCheck("exit 1", interval_s=0.1, retries=1),
        run_as="subids",
    )
    with pytest.raises(SandboxStartError):
        await pod.start_service(failing, wait="healthy")
    (dropped, in_holder) = give_backs.pop()
    assert Path(dropped).parent == pod.services_root and in_holder
    await pod.stop()
    assert give_backs == [(str(path), True)]  # the whole pod, in its holder's namespace
    assert await provider.wait_for_deletions(30) and not path.exists()


CRASH_SUBIDS = """\
import asyncio, os, signal, sys
from pathlib import Path
sys.path.insert(0, {src!r})
import hpc_sandbox.subids
from hpc_sandbox import SingularityProvider, SandboxSpec
from hpc_sandbox.types import PodSpec, ServiceSpec

async def main():
    scratch, cache, image, listings = sys.argv[1:]
    if listings:  # the fake's
        hpc_sandbox.subids.SUBUID = Path(listings, "subuid")
        hpc_sandbox.subids.SUBGID = Path(listings, "subgid")
    provider = SingularityProvider(scratch_root=scratch, image_cache=cache, stage_images=False)
    pod = await provider.start_pod(PodSpec(users="subids", labels={{"t": "crash"}}))
    command = {command!r}
    service = await pod.start_service(
        ServiceSpec("main", SandboxSpec(image=image), command=command, run_as="subids")
    )
    for _ in range(600):
        if command is None or (service.session_dir / "tmp" / "ready").exists():
            break
        if service.exit_status is not None:
            print(await pod.logs("main"), file=sys.stderr, flush=True)
            break
        await asyncio.sleep(0.05)
    print(pod.id, pod._holder.pid, service._service_anchor.pid, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

asyncio.run(main())
"""


async def crash_with_a_subids_pod(
    provider: Any, image: str, tmp_path: Path, listings: str, command: list[str] | None = None
) -> tuple[str, list[int], str]:
    """Start a subids pod with a service in a process that then dies by SIGKILL; the pod's id,
    the PIDs of its holder and its service's launcher, and what the process printed on stderr."""
    src = str(Path(__file__).resolve().parents[1] / "src")
    script = CRASH_SUBIDS.format(src=src, command=command)
    arguments = [str(provider.scratch_root), str(tmp_path / "cache"), image, listings]
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", script, *arguments],
        capture_output=True,
        text=True,
        timeout=600,  # it maps its own subordinate ids, which can take long
        check=False,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    pod_id, *pids = result.stdout.split()
    await until(lambda: not any(owned_live_start_time(int(pid)) for pid in pids), 10)
    return pod_id, [int(pid) for pid in pids], result.stderr


@on_singularity
async def test_the_janitor_gives_back_what_an_abandoned_subids_pod_left(
    runtime, fake_subids, give_backs, tmp_path
):
    provider, image = runtime
    pod_id, _, _ = await crash_with_a_subids_pod(
        provider, image, tmp_path, str(fake_subids.directory)
    )
    path = provider.pod_root / pod_id
    assert path.is_dir()
    mapped = len(fake_subids.calls())
    report = await provider.reap()
    assert pod_id in report.removed and not report.errors
    # In this process's namespace of the subordinate ids: the pod's holder died with its owner.
    assert give_backs == [(str(path), False)] and len(fake_subids.calls()) == mapped + 2
    assert await provider.wait_for_deletions(30) and not path.exists()


MAP_THEN_FORK = """\
import asyncio, os, sys, traceback
from pathlib import Path
sys.path.insert(0, {src!r})
import hpc_sandbox.subids
from hpc_sandbox import SingularityProvider
from hpc_sandbox.types import PodSpec

scratch, cache, listings = sys.argv[1:]
hpc_sandbox.subids.SUBUID = Path(listings, "subuid")
hpc_sandbox.subids.SUBGID = Path(listings, "subgid")
provider = SingularityProvider(scratch_root=scratch, image_cache=cache, stage_images=False)

async def pod():
    async with await provider.start_pod(PodSpec(users="subids")):
        pass
    assert await provider.wait_for_deletions(30)

asyncio.run(provider.map_subordinate_ids())  # as a fork server would, before it forks
child = os.fork()
if child == 0:
    try:
        asyncio.run(pod())
    except BaseException:
        traceback.print_exc()
        os._exit(1)
    os._exit(0)
sys.exit(os.waitstatus_to_exitcode(os.waitpid(child, 0)[1]))
"""


@on_singularity
async def test_subordinate_ids_mapped_ahead_serve_the_pods_of_forked_processes(
    runtime, fake_subids, tmp_path
):
    provider, _ = runtime
    await provider.map_subordinate_ids()
    await provider.map_subordinate_ids()
    await (await provider.start_pod(PodSpec(users="subids"))).stop()
    assert len(fake_subids.calls()) == 2  # newuidmap and newgidmap, once
    # A process that maps them, then forks: the child's pods map nothing.
    src = str(Path(__file__).resolve().parents[1] / "src")
    arguments = [str(provider.scratch_root), str(tmp_path / "cache"), str(fake_subids.directory)]
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", MAP_THEN_FORK.format(src=src), *arguments],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert len(fake_subids.calls()) == 4


async def test_an_id_namespace_whose_descriptor_was_closed_is_mapped_again(fake_subids):
    if reason := namespaces_unavailable():
        pytest.skip(reason)
    ids = subids.find()
    os.close(await pods.id_namespace(ids, 30))  # as a forked process may close what it inherited
    other = os.open(os.devnull, os.O_RDONLY)  # which may take its number
    try:
        fd = await pods.id_namespace(ids, 30)
        assert os.readlink(f"/proc/self/fd/{fd}").startswith("user:[")
        assert len(fake_subids.calls()) == 4
        assert await pods.id_namespace(ids, 30) == fd
    finally:
        os.close(other)


def user_namespace_descriptors() -> set[str]:
    """This process's descriptors of user namespaces."""
    found = set()
    for fd in os.listdir("/proc/self/fd"):
        with contextlib.suppress(OSError):
            if os.readlink(f"/proc/self/fd/{fd}").startswith("user:["):
                found.add(fd)
    return found


def test_event_loops_of_two_threads_keep_one_id_namespace(fake_subids, monkeypatch):
    if reason := namespaces_unavailable():
        pytest.skip(reason)
    ids = subids.find()
    both = threading.Barrier(2, timeout=60)
    mapped_holder = pods._mapped_holder

    @contextlib.asynccontextmanager
    async def once_both_looked(*args: Any) -> AsyncIterator[int]:
        await asyncio.to_thread(both.wait)  # neither found a namespace kept
        async with mapped_holder(*args) as pid:
            yield pid

    monkeypatch.setattr(pods, "_mapped_holder", once_both_looked)
    before = user_namespace_descriptors()
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        kept = set(pool.map(lambda _: asyncio.run(pods.id_namespace(ids, 30)), range(2)))
    assert len(fake_subids.calls()) == 4  # each mapped one, and both use the same
    assert len(kept) == 1 and len(user_namespace_descriptors() - before) == 1


def test_subordinate_ranges_are_read_as_newuidmap_reads_them(tmp_path):
    listing = tmp_path / "subuid"
    listing.write_text(
        "# a comment\n\nalice:100000:0\nbob:x:65536\nalice:165536:65536\n1234:300000:10\n"
    )
    assert subids.first_range(listing, {"alice"}) == subids.IdRange(165536, 65536)
    assert subids.first_range(listing, {"1234", "carol"}) == subids.IdRange(300000, 10)
    assert subids.first_range(listing, {"bob"}) is None
    assert subids.first_range(tmp_path / "missing", {"alice"}) is None
    assert subids.IdRange(300000, 10).ids == range(300000, 300010)


def test_a_process_of_a_subordinate_id_counts_as_the_services():
    try:
        owner = Path("/proc/1").stat().st_uid  # usually root's
    except OSError:
        pytest.skip("PID 1 is hidden (hidepid)")
    if owner == os.getuid():
        pytest.skip("PID 1 is this uid's")
    assert owned_live_start_time(1) is None
    assert owned_live_start_time(1, range(owner, owner + 1)) is not None
    assert owned_live_start_time(os.getpid()) is not None


@on_singularity
async def test_a_subids_service_makes_a_missing_workdir_that_its_other_users_can_enter(
    runtime, fake_subids
):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec(users="subids"))
    found = {}
    previous = os.umask(0o027)
    try:
        for run_as in ("subids", "root"):
            spec = ServiceSpec(
                run_as,
                SandboxSpec(image=image, workdir="/made/deep"),
                command=["sh", "-c", "exec sleep 600"],
                run_as=run_as,
            )
            service = await pod.start_service(spec)
            found[run_as] = (await service.exec("stat -c %a /made /made/deep")).stdout.split()
    finally:
        os.umask(previous)
    assert found == {"subids": ["755", "755"], "root": ["750", "750"]}
    await pod.stop()


async def subids_unavailable() -> str | None:
    """Why this machine cannot map subordinate ids into a pod, or None. It maps them once for the
    tests of this process, which can take seconds."""
    if reason := namespaces_unavailable():
        return reason
    try:
        await pods.id_namespace(subids.find(), 600)
    except SandboxError as error:
        return f"no subordinate ids to map here: {error}"
    return None


# As root of a subids service: switch to uid and gid 999, then write in a directory of its own,
# which you cannot enter on the host.
SWITCH = (
    "import os, time; os.setresgid(999, 999, 999); os.setgroups([]); os.setresuid(999, 999, 999); "
    "os.mkdir('/tmp/own', 0o700); open('/tmp/own/file', 'w').write('x'); "
    "open('/tmp/ready', 'w').close(); time.sleep(600)"
)


@on_singularity
async def test_subids_services_switch_users_and_their_files_are_given_back(runtime):
    provider, image = runtime
    if reason := await subids_unavailable():
        pytest.skip(reason)
    pod = await provider.start_pod(PodSpec(users="subids"))
    ids = pod._ids
    assert ids is not None
    spec = ServiceSpec(
        "svc", SandboxSpec(image=image), command=[PYTHON, "-c", SWITCH], run_as="subids"
    )
    service = await pod.start_service(spec)
    other = await pod.start_service(dataclasses.replace(spec, name="other"))
    for each in (service, other):
        ready = each.session_dir / "tmp" / "ready"
        await until(lambda: ready.exists() or each.exit_status is not None)
        assert ready.exists(), (each.exit_status, await pod.logs(each.name))
    own = service.session_dir / "tmp" / "own"
    assert own.lstat().st_uid == ids.uids.start + 998 and own.lstat().st_gid == ids.gids.start + 998
    assert (await service.exec("stat -c '%u %g %a' /tmp/own")).stdout == "999 999 700\n"
    assert await pod.health("svc") == "healthy"  # its command switched users: it runs on
    mapped = (await service.exec("cat /proc/self/uid_map")).stdout.split()
    assert mapped == ["0", "0", "1", "1", "1", str(ids.uids.count)]
    await assert_no_hold_on_the_network(service)  # its root, with every subordinate id
    # Root of one id cannot switch users.
    root = await pod.start_service(
        ServiceSpec("root", SandboxSpec(image=image), command=[PYTHON, "-c", SWITCH])
    )
    await until(lambda: root.exit_status is not None)
    assert root.exit_status == 1 and "Invalid argument" in await pod.logs("root")
    # A service that stops before its pod, then the pod: nothing is left.
    other_path = other.session_dir
    await other.stop()
    assert await provider.wait_for_deletions(30) and not other_path.exists()
    path = pod.path
    await pod.stop()
    assert await provider.wait_for_deletions(30) and not path.exists()
    assert list((provider.pod_root / ".trash").iterdir()) == []


@on_singularity
async def test_the_janitor_gives_back_what_a_dead_owners_subids_service_wrote(runtime, tmp_path):
    provider, image = runtime
    if reason := await subids_unavailable():
        pytest.skip(reason)
    pod_id, _, stderr = await crash_with_a_subids_pod(
        provider, image, tmp_path, "", command=[PYTHON, "-c", SWITCH]
    )
    path = provider.pod_root / pod_id
    owned = list(path.glob("services/hpcsb-*/tmp/own"))
    assert len(owned) == 1, stderr
    assert owned[0].lstat().st_uid != os.getuid()  # a subordinate id's, which you cannot delete
    report = await provider.reap()
    assert pod_id in report.removed and not report.errors
    assert await provider.wait_for_deletions(30) and not path.exists()
    assert list((provider.pod_root / ".trash").iterdir()) == []
