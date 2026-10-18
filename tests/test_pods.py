"""Pods on the fake runtimes: real namespaces, holders, launchers and relays."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
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
from conftest import make_enroot_provider, make_provider, namespaces_unavailable
from test_trash import janitor_lock_held

from hpc_sandbox import SandboxSpec, sessions
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
FETCH = "import sys, urllib.request; print(urllib.request.urlopen(sys.argv[1], timeout=5).read().decode().strip())"
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
    """A Unix socket answering HTTP, standing in for platoon's gateway."""
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


async def test_root_in_a_service_cannot_touch_the_pods_network(runtime):
    provider, image = runtime
    pod = await provider.start_pod(PodSpec())
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    down = await main.exec("ip link set lo down")
    assert down.exit_code != 0 and "not permitted" in down.stderr.lower(), down
    raw = await main.exec(
        [PYTHON, "-c", "import socket; socket.socket(socket.AF_PACKET, socket.SOCK_RAW)"]
    )
    assert raw.exit_code != 0 and "PermissionError" in raw.stderr, raw
    # What compose services expect of root anyway: privileged ports and ping.
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
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setenv("HPC_SANDBOX_TOOLS", str(tmp_path / "no-tools"))
    with pytest.raises(SandboxError, match="fetch-tools"):
        await provider.start_pod(PodSpec(network="egress"))
    assert not provider.pod_root.exists() or list(provider.pod_root.iterdir()) in (
        [],
        [provider.pod_root / ".janitor.lock"],
    )


# What the reviews of pods found, kept fixed.

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
            "echo before; echo err > /dev/stderr; head -c 300000 /dev/zero | tr '\\0' x; echo; echo after; exec sleep 600",
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


HOG = "import time; data = bytearray(150 << 20); data[::4096] = b'x' * len(data[::4096]); time.sleep(600)"


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
