"""Pods end to end against a real runtime on a compute node.

Run on a compute node (never a login node) with::

    HPC_SANDBOX_INTEGRATION=1 python -m pytest tests/test_pods_integration.py -v

``HPC_SANDBOX_TEST_RUNTIME`` picks the runtime as for ``test_integration.py``.
Services run ``HPC_SANDBOX_TEST_POD_IMAGE`` (default ``docker://alpine:3.20``,
whose busybox serves and fetches HTTP); the apt check runs
``HPC_SANDBOX_TEST_APT_IMAGE`` (default ``docker://python:3.12-slim``). Egress
checks run where slirp4netns is found (see ``hpc-sandbox fetch-tools``) and
reach PyPI and Debian's mirrors. The check that other users cannot reach
exposed ports runs where the user has subordinate ids (``/etc/subuid`` and
``/etc/subgid``) and ``newuidmap`` and ``newgidmap`` are installed.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import pwd
import shutil
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

from hpc_sandbox import SandboxSpec
from hpc_sandbox.procfs import owned_live_start_time
from hpc_sandbox.types import HealthCheck, PodSpec, SandboxError, ServiceSpec
from test_integration import RUNTIME, make_provider

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
    '| nc -l -p "$PORT" >/dev/null; done'
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


def judge(secret: str = "s3cret", name: str = "judge", port: int = 8082) -> ServiceSpec:
    ready = HealthCheck(
        ["CMD-SHELL", f"wget -qO- -T 2 http://127.0.0.1:{port}/"],
        interval_s=1,
        start_interval_s=0.2,
        start_period_s=60,
    )
    return ServiceSpec(
        name,
        SandboxSpec(image=IMAGE, env={"SECRET": secret, "PORT": str(port)}),
        command=["/bin/sh", "-c", JUDGE],
        healthcheck=ready,
    )


async def ask(pod: Any, port: int) -> str:
    """The body of the reply to an HTTP request sent from the host through an exposed port. The
    judge's nc serves one connection at a time, and the holder closes a connection that finds
    nothing listening, so a request between two connections is sent again."""
    for _ in range(50):
        reader, writer = await pod.open_connection(port)
        try:
            writer.write(b"GET / HTTP/1.0\r\n\r\n")
            await writer.drain()
            async with asyncio.timeout(10):
                reply = await reader.read()
        except ConnectionError:  # closed before the request went through
            reply = b""
        finally:
            writer.close()
        if reply:
            return reply.decode().split("\r\n\r\n", 1)[-1]
        await asyncio.sleep(0.1)
    raise AssertionError(f"port {port} of pod {pod.id} never answered")


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
    pod = await provider.start_pod(PodSpec(labels={{"t": "crash"}}, exposes=[8082]))
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


async def test_the_host_reaches_exposed_ports_through_their_sockets(provider):
    pod = await provider.start_pod(PodSpec(exposes=[8082]))
    await pod.start_service(judge("first"), wait="healthy")
    assert await ask(pod, 8082) == "judge-ok first\n"
    await pod.start_service(judge("second", name="other", port=8083), wait="healthy")
    path = Path(await pod.expose(8083))
    assert await ask(pod, 8083) == "judge-ok second\n"
    assert pod.endpoints == {8082: str(path.parent / "8082.sock"), 8083: str(path)}
    for each in (path, path.parent):
        info = each.lstat()
        assert info.st_uid == os.getuid() and info.st_mode & 0o077 == 0, (each, oct(info.st_mode))
    await pod.stop()
    assert not path.exists()


def subordinate_ids(path: str) -> tuple[int, int] | None:
    """The first id and the count of the caller's subordinate range in ``path``."""
    names = {str(os.getuid())}
    with contextlib.suppress(KeyError):
        names.add(pwd.getpwuid(os.getuid()).pw_name)
    try:
        lines = Path(path).read_text().splitlines()
    except OSError:
        return None
    for line in lines:
        fields = line.strip().split(":")
        if len(fields) == 3 and fields[0] in names and fields[1].isdigit() and fields[2].isdigit():
            return int(fields[1]), int(fields[2])
    return None


# Run as another user through a user namespace that maps the caller to root and uid 1 to one of the
# caller's subordinate ids: connect to an exposed port's socket through descriptors the test opened
# (of the socket itself, of its directory) and through its path, as the namespace's root (the
# caller), then as its uid 1, another user on the host.
OTHER_USER = """\
import ctypes, errno, json, os, socket, sys

path, socket_fd, directory_fd = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
if ctypes.CDLL(None, use_errno=True).unshare(0x10000000) != 0:  # CLONE_NEWUSER
    sys.exit("unshare: " + os.strerror(ctypes.get_errno()))
print("unshared", flush=True)
if sys.stdin.readline() != "mapped\\n":
    sys.exit("the namespace was not mapped")


def attempt(address):
    try:
        with socket.socket(socket.AF_UNIX) as connection:
            connection.connect(address)
        return "connected"
    except OSError as error:
        return errno.errorcode.get(error.errno, str(error))


def by_path():
    try:
        target = os.open(path, os.O_PATH)  # through every directory above the socket
    except OSError as error:
        return errno.errorcode.get(error.errno, str(error))
    try:
        return attempt(f"/proc/self/fd/{target}")
    finally:
        os.close(target)


def attempts():
    return {
        "socket": attempt(f"/proc/self/fd/{socket_fd}"),
        "directory": attempt(f"/proc/self/fd/{directory_fd}/{os.path.basename(path)}"),
        "path": by_path(),
    }


report = {"caller": attempts()}
os.setgroups([])
os.setresgid(1, 1, 1)
os.setresuid(1, 1, 1)
report["other"] = attempts()
print(json.dumps(report), flush=True)
"""


def connect_as_another_user(path: str, ids: list[tuple[str, int, int]]) -> dict:
    socket_fd = os.open(path, os.O_PATH | os.O_CLOEXEC)
    directory_fd = os.open(os.path.dirname(path), os.O_PATH | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        helper = subprocess.Popen(
            [sys.executable, "-I", "-c", OTHER_USER, path, str(socket_fd), str(directory_fd)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            pass_fds=(socket_fd, directory_fd),
        )
        try:
            assert helper.stdout is not None and helper.stdout.readline() == "unshared\n"
            for tool, own, other in ids:
                command = [tool, str(helper.pid), "0", str(own), "1", "1", str(other), "1"]
                subprocess.run(command, check=True, capture_output=True, timeout=300)
            out, err = helper.communicate("mapped\n", timeout=60)
        finally:
            if helper.poll() is None:
                helper.kill()
                helper.wait()
        assert helper.returncode == 0, err
        return json.loads(out)
    finally:
        os.close(socket_fd)
        os.close(directory_fd)


async def test_other_users_cannot_reach_exposed_ports(provider):
    uids, gids = subordinate_ids("/etc/subuid"), subordinate_ids("/etc/subgid")
    tools = [shutil.which("newuidmap"), shutil.which("newgidmap")]
    if not (uids and gids and all(tools)):
        pytest.skip("needs subordinate ids in /etc/subuid and /etc/subgid, newuidmap and newgidmap")
    pod = await provider.start_pod(PodSpec(exposes=[8082]))
    await pod.start_service(judge(), wait="healthy")
    # The last id of each range: newuidmap and newgidmap look up the owner of every range in the
    # file that holds an id they check, which takes long where the ranges of many users overlap.
    ids = [(tools[0], os.getuid(), sum(uids) - 1), (tools[1], os.getgid(), sum(gids) - 1)]
    report = await asyncio.to_thread(connect_as_another_user, pod.endpoints[8082], ids)
    ways = ("socket", "directory", "path")
    assert report["caller"] == dict.fromkeys(ways, "connected"), report
    # The socket's mode, its directory's mode, and the modes of the directories above both
    # keep other users out.
    assert report["other"] == dict.fromkeys(ways, "EACCES"), report
    await pod.stop()


async def test_aliases_name_upstreams_and_another_pods_exposed_port(provider, gateway, tmp_path):
    judging = await provider.start_pod(PodSpec(exposes=[8082]))
    await judging.start_service(judge("shared"), wait="healthy")
    link = tmp_path / "judge.sock"  # a short path: an upstream's must fit in a socket address
    link.symlink_to(judging.endpoints[8082])
    spec = PodSpec(
        upstreams={"gateway": f"unix://{gateway}", "judge": f"unix://{link}"},
        upstream_ports={"gateway": 8090, "judge": 8082},
        hosts=["gateway", "judge"],
    )
    pod = await provider.start_pod(spec)
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE)))
    checks = {
        "aliases": ("grep -cE '^127.0.0.1\t(gateway|judge)$' /etc/hosts", "2"),
        "resolved": ("ping -c1 -W2 judge | head -1 | cut -d' ' -f3", "(127.0.0.1):"),
        "an upstream": ("wget -qO- -T 5 http://gateway:8090/", "gateway-ok"),
        "another pod": ("wget -qO- -T 5 http://judge:8082/", "judge-ok shared"),
    }
    for label, (command, expected) in checks.items():
        result = await main.exec(command)
        assert result.stdout.strip() == expected, (label, result)
    await pod.stop()
    await judging.stop()


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
