"""Pods end to end against a real runtime on a compute node.

Run on a compute node (never a login node) with::

    HPC_SANDBOX_INTEGRATION=1 python -m pytest tests/test_pods_integration.py -v

``HPC_SANDBOX_TEST_RUNTIME`` picks the runtime as for ``test_integration.py``.
Services run ``HPC_SANDBOX_TEST_POD_IMAGE`` (default ``docker://alpine:3.20``,
whose busybox serves and fetches HTTP); the apt check runs
``HPC_SANDBOX_TEST_APT_IMAGE`` (default ``docker://python:3.12-slim``). Egress
checks run where slirp4netns is found (see ``hpc-sandbox fetch-tools``) and
reach PyPI and Debian's mirrors.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
from test_integration import RUNTIME, make_provider

from hpc_sandbox import SandboxSpec
from hpc_sandbox.procfs import owned_live_start_time
from hpc_sandbox.types import HealthCheck, PodSpec, SandboxError, ServiceSpec

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("HPC_SANDBOX_INTEGRATION") != "1",
        reason="set HPC_SANDBOX_INTEGRATION=1 on a compute node with a container runtime",
    ),
]
IMAGE = os.environ.get("HPC_SANDBOX_TEST_POD_IMAGE", "docker://alpine:3.20")
APT_IMAGE = os.environ.get("HPC_SANDBOX_TEST_APT_IMAGE", "docker://python:3.12-slim")
SRC = str(Path(__file__).resolve().parents[1] / "src")
JUDGE = (
    'echo judge up; while true; do printf "HTTP/1.0 200 OK\\r\\n\\r\\njudge-ok $SECRET\\n" '
    "| nc -l -p 8082 >/dev/null; done"
)
READY = HealthCheck(
    ["CMD-SHELL", "wget -qO- -T 2 http://127.0.0.1:8082/"],
    interval_s=1,
    start_interval_s=0.2,
    start_period_s=60,
)


def egress_available(scratch: Path) -> str | None:
    try:
        make_provider(scratch).slirp4netns()
    except SandboxError as error:
        return str(error)
    return None


@pytest.fixture
def scratch() -> Iterator[Path]:
    from hpc_sandbox.paths import default_scratch_root
    from hpc_sandbox.sessions import remove_tree

    path = default_scratch_root() / f"pods-integration-{uuid.uuid4().hex[:8]}"
    yield path
    remove_tree(path)


@pytest.fixture
async def provider(scratch: Path) -> AsyncIterator[Any]:
    provider = make_provider(scratch)
    yield provider
    for pod in provider.pods:
        await pod.stop()
    report = await provider.reap()
    assert not report.errors, report.errors
    assert await provider.wait_for_deletions(120)  # before the scratch root goes


@pytest.fixture
def gateway(tmp_path: Path) -> Iterator[Path]:
    path = tmp_path / "gw.sock"
    server = socket.socket(socket.AF_UNIX)
    server.bind(str(path))
    server.listen(64)

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


def judge(secret: str = "s3cret") -> ServiceSpec:
    return ServiceSpec(
        "judge",
        SandboxSpec(image=IMAGE, env={"SECRET": secret}),
        command=["/bin/sh", "-c", JUDGE],
        healthcheck=READY,
    )


async def test_services_reach_each_other_and_the_upstream(provider, gateway):
    pod = await provider.start_pod(PodSpec(upstreams={"gateway": f"unix://{gateway}"}))
    await pod.start_service(judge(), wait="healthy")
    main = await pod.start_service(
        ServiceSpec(
            "main", SandboxSpec(image=IMAGE, env={"A": "1"}), ulimits={"nofile": (1000, 2000)}
        )
    )
    checks = {
        "hostname": ("hostname", "main"),
        "judge by name": ("wget -qO- -T 5 http://judge:8082/", "judge-ok s3cret"),
        "relay": ("wget -qO- -T 5 http://127.0.0.1:18080/", "gateway-ok"),
        "uid": ("id -u", "0"),
        "links": ("ip -o link | cut -d: -f2 | xargs", "lo"),
        "ulimits": ("echo $(ulimit -Sn) $(ulimit -Hn)", "1000 2000"),
        "shm": ("df -k /dev/shm | tail -1 | awk '{print $2}'", str(64 * 1024)),
        "env": ("echo A=$A SECRET=${SECRET-unset}", "A=1 SECRET=unset"),
        "resolv.conf": ("cat /etc/resolv.conf", "nameserver 10.0.2.3"),
        "bind :80": ("nc -l -p 80 & sleep 0.3; kill $! && echo bound", "bound"),
        "ping": ("ping -c1 -W2 127.0.0.1 >/dev/null && echo pong", "pong"),
    }
    for label, (command, expected) in checks.items():
        result = await main.exec(command)
        assert result.stdout.strip() == expected, (label, result)
    down = await main.exec("ip link set lo down")
    assert down.exit_code != 0 and "not permitted" in down.stderr.lower(), down
    taken = await main.exec("nc -l -p 18080")
    assert taken.exit_code != 0 and "in use" in taken.stderr, taken
    assert await pod.health("judge") == "healthy"
    assert "judge up" in await pod.logs("judge")
    await pod.stop()


async def test_pods_are_closed_to_the_host_and_to_each_other(provider, gateway):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    spec = PodSpec(upstreams={"gateway": f"unix://{gateway}"}, upstream_ports={"gateway": port})
    first, second = await provider.start_pod(spec), await provider.start_pod(spec)
    await first.start_service(judge("first"), wait="healthy")
    a = await first.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE)))
    b = await second.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE)))
    assert (await a.exec("wget -qO- -T 5 http://127.0.0.1:8082/")).stdout == "judge-ok first\n"
    assert (await b.exec("wget -qO- -T 5 http://127.0.0.1:8082/")).exit_code != 0
    for service in (a, b):
        relayed = await service.exec(f"wget -qO- -T 5 http://127.0.0.1:{port}/")
        assert relayed.stdout == "gateway-ok\n", relayed
    for host_port in (port, 8082):
        with socket.socket() as probe, pytest.raises(OSError):
            probe.settimeout(2)
            probe.connect(("127.0.0.1", host_port))
    await first.stop()
    await second.stop()


async def test_network_none_has_no_egress(provider):
    pod = await provider.start_pod(PodSpec())
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE)))
    result = await main.exec("wget -qO- -T 3 http://deb.debian.org/ 2>&1; echo rc=$?")
    assert "rc=0" not in result.stdout, result
    await pod.stop()


async def test_egress_reaches_the_internet_but_not_the_hosts_loopback(provider, scratch):
    if reason := egress_available(scratch):
        pytest.skip(reason)
    with socket.socket() as listener:  # a service on the host's loopback
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        pod = await provider.start_pod(PodSpec(network="egress"))
        main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE)))
        resolved = await main.exec("nslookup pypi.org 10.0.2.3")
        assert resolved.exit_code == 0, resolved
        fetched = await main.exec("wget -qO- -T 10 http://deb.debian.org/debian/ | head -c 200")
        assert fetched.exit_code == 0 and fetched.stdout, fetched
        host = await main.exec(f"wget -qO- -T 3 http://10.0.2.2:{port}/ 2>&1; echo rc=$?")
        assert "rc=0" not in host.stdout, host
        await pod.set_network("none")
        cut = await main.exec("wget -qO- -T 3 http://deb.debian.org/ 2>&1; echo rc=$?")
        assert "rc=0" not in cut.stdout, cut
        await pod.set_network("egress")
        again = await main.exec("wget -qO- -T 10 http://deb.debian.org/debian/ | head -c 200")
        assert again.exit_code == 0 and again.stdout, again
        await pod.stop()


async def test_apt_works_as_root_in_a_service(provider, scratch):
    if reason := egress_available(scratch):
        pytest.skip(reason)
    pod = await provider.start_pod(PodSpec(network="egress"))
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=APT_IMAGE)))
    result = await main.exec(
        "apt-get update -qq && apt-get install -y -qq iproute2 >/dev/null && ip -V", timeout_s=600
    )
    assert result.exit_code == 0, result
    await pod.stop()


CRASH = """\
import asyncio, os, signal, sys
sys.path.insert(0, {src!r})
from hpc_sandbox import EnrootProvider, SingularityProvider, SandboxSpec
from hpc_sandbox.types import PodSpec, ServiceSpec

async def main():
    kind, scratch, image = sys.argv[1:]
    providers = {{"enroot": EnrootProvider, "singularity": SingularityProvider}}
    provider = providers[kind](scratch_root=scratch)
    pod = await provider.start_pod(PodSpec(labels={{"t": "crash"}}))
    for name in ("main", "judge"):
        service = await pod.start_service(ServiceSpec(name, SandboxSpec(image=image)))
        print(service._service_anchor.pid, service._service_init, service._container, flush=True)
    print(pod.id, pod._holder.pid, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

asyncio.run(main())
"""


async def test_a_dead_owners_pods_die_and_the_janitor_removes_them(provider, scratch):
    kind = "enroot" if RUNTIME == "enroot" else "singularity"
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", CRASH.format(src=SRC), kind, str(scratch), IMAGE],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    *services, last = result.stdout.splitlines()
    pod_id, holder = last.split()
    pids = [int(holder), *(int(pid) for line in services for pid in line.split())]
    deadline = time.monotonic() + 10
    while any(owned_live_start_time(pid) for pid in pids) and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    assert [pid for pid in pids if owned_live_start_time(pid)] == []
    report = await provider.reap()
    assert pod_id in report.removed and not report.errors
    assert not (provider.pod_root / pod_id).exists()


async def test_many_pods_start_and_stop_cleanly(provider, gateway):
    """Pods with two services each, some stopped mid-start; nothing may remain."""
    count = int(os.environ.get("HPC_SANDBOX_TEST_PODS", "16"))
    timings: list[float] = []

    async def one(index: int) -> None:
        started = time.monotonic()
        pod = await provider.start_pod(PodSpec(upstreams={"gateway": f"unix://{gateway}"}))
        try:
            await pod.start_service(judge(str(index)), wait="healthy")
            if index % 5 == 4:  # stopped while main starts
                task = asyncio.ensure_future(
                    pod.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE)))
                )
                await asyncio.sleep(0.05)
                await pod.stop()
                with pytest.raises(SandboxError):
                    await task
                return
            main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE)))
            timings.append(time.monotonic() - started)
            result = await main.exec("wget -qO- -T 10 http://judge:8082/")
            assert result.stdout == f"judge-ok {index}\n", result
        finally:
            await pod.stop()

    await asyncio.gather(*(one(index) for index in range(count)))
    assert provider.pods == frozenset()
    assert await provider.wait_for_deletions(300)
    assert [p.name for p in provider.pod_root.iterdir() if p.name.startswith("hpcsb-")] == []
    trash = provider.pod_root / ".trash"
    assert not trash.exists() or list(trash.iterdir()) == []
    timings.sort()
    p99 = timings[min(len(timings) - 1, round(0.99 * (len(timings) - 1)))]
    print(f"\n{count} pods: setup p50 {statistics.median(timings):.2f} s, p99 {p99:.2f} s")
