"""Pods end to end against a real runtime on a compute node.

Run on a compute node (never a login node) with::

    HPC_SANDBOX_INTEGRATION=1 python -m pytest tests/test_pods_integration.py -v

``HPC_SANDBOX_TEST_RUNTIME`` picks the runtime as for ``test_integration.py``.
Services run ``HPC_SANDBOX_TEST_POD_IMAGE`` (default ``docker://alpine:3.20``,
whose busybox serves and fetches HTTP); the apt check runs
``HPC_SANDBOX_TEST_APT_IMAGE`` (default ``docker://python:3.12-slim``), and the
checks of who services run as run ``HPC_SANDBOX_TEST_REDIS_IMAGE`` (default
``docker://redis:7-alpine``) and ``HPC_SANDBOX_TEST_POSTGRES_IMAGE`` (default
``docker://postgres:16-alpine``), with their entrypoints. Egress checks run where
slirp4netns is found (see ``hpc-sandbox fetch-tools``) and reach PyPI and
Debian's mirrors. ``HPC_SANDBOX_TEST_SUBIDS=1`` also tests pods with
``users="subids"``, with Singularity, where your user has subordinate ids that
``newuidmap`` and ``newgidmap`` map.
"""

from __future__ import annotations

import asyncio
import dataclasses
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

from hpc_sandbox import SandboxSpec
from hpc_sandbox.procfs import owned_live_start_time
from hpc_sandbox.types import HealthCheck, PodSpec, SandboxError, ServiceRunAs, ServiceSpec
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
REDIS_IMAGE = os.environ.get("HPC_SANDBOX_TEST_REDIS_IMAGE", "docker://redis:7-alpine")
POSTGRES_IMAGE = os.environ.get("HPC_SANDBOX_TEST_POSTGRES_IMAGE", "docker://postgres:16-alpine")
SUBIDS = RUNTIME != "enroot" and os.environ.get("HPC_SANDBOX_TEST_SUBIDS") == "1"
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


# Who services run as. The official database images start as root, give their data directories to
# their own user and switch to it (gosu, su-exec); as another user they skip both.
DATABASES = {
    "redis": ServiceSpec(
        "redis",
        SandboxSpec(image=REDIS_IMAGE, workdir="/data"),
        command=["docker-entrypoint.sh", "redis-server"],
        healthcheck=HealthCheck(
            ["CMD", "redis-cli", "ping"], interval_s=1, start_interval_s=0.2, start_period_s=120
        ),
    ),
    "postgres": ServiceSpec(
        "postgres",
        SandboxSpec(image=POSTGRES_IMAGE, workdir="/", env={"POSTGRES_PASSWORD": "hpc-sandbox"}),
        command=["docker-entrypoint.sh", "postgres"],
        healthcheck=HealthCheck(
            ["CMD", "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
            interval_s=1,
            start_interval_s=0.2,
            start_period_s=120,
        ),
    ),
}
# Where each keeps its data, and its port.
DATA = {"redis": "/data", "postgres": "/var/lib/postgresql/data"}
PORTS = {"redis": 6379, "postgres": 5432}
# As root, add a user of uid 999 (in group users: busybox takes gid 999 for a new user's group,
# which alpine gives ping) and switch to it, write in a directory of its own, then stay up.
SWITCH_USERS = (
    "adduser -D -H -u 999 -G users svc && exec su -s /bin/sh svc -c '"
    "id -u > /tmp/uid && mkdir -m 700 /tmp/own && echo x > /tmp/own/file && "
    "touch /tmp/ready && exec sleep 600'"
)
RUN_AS: list[ServiceRunAs] = ["root", "caller", "subids"]


def skip_unless_possible(run_as: ServiceRunAs) -> None:
    if run_as == "subids" and RUNTIME == "enroot":
        pytest.skip("enroot maps one id into a container: no subids services")
    if run_as == "subids" and not SUBIDS:
        pytest.skip("set HPC_SANDBOX_TEST_SUBIDS=1 where your user has subordinate ids")


async def until(probe: Any, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while not probe():
        assert time.monotonic() < deadline, "timed out"
        await asyncio.sleep(0.1)


def owner(path: Path) -> int:
    return path.lstat().st_uid


@pytest.mark.parametrize("run_as", RUN_AS)
async def test_a_service_keeps_your_supplementary_groups_in_every_mode(provider, run_as):
    """A service's command has your supplementary groups, unmapped, whatever its mode, as a
    sandbox does: the runtime enters your directory and binds the service's mounts through them,
    such as project storage that only a group opens."""
    skip_unless_possible(run_as)
    if len(os.getgroups()) < 2:
        pytest.skip("your user has no supplementary group besides its own")
    pod = await provider.start_pod(PodSpec(users="subids" if run_as == "subids" else "single"))
    record = "grep '^Groups:' /proc/self/status > /tmp/g && mv /tmp/g /tmp/groups && exec sleep 600"
    spec = ServiceSpec(
        "svc", SandboxSpec(image=IMAGE), command=["/bin/sh", "-c", record], run_as=run_as
    )
    service = await pod.start_service(spec)
    groups = service.session_dir / "tmp" / "groups"
    await until(lambda: groups.exists() or service.exit_status is not None, 60)
    assert groups.exists(), await pod.logs("svc")
    assert len(groups.read_text().split()[1:]) == len(os.getgroups()), groups.read_text()
    await pod.stop()


@pytest.mark.parametrize("run_as", RUN_AS)
@pytest.mark.parametrize("database", sorted(DATABASES))
async def test_database_images_run_as_the_caller_or_with_subordinate_ids(
    provider, database, run_as
):
    skip_unless_possible(run_as)
    pod = await provider.start_pod(PodSpec(users="subids" if run_as == "subids" else "single"))
    spec = dataclasses.replace(DATABASES[database], run_as=run_as)
    started = time.monotonic()
    if run_as == "root":
        service = await pod.start_service(spec)
        if RUNTIME == "enroot":
            # enroot's seccomp filter makes chown and setuid succeed and do nothing: still root
            # after the switch, the entrypoint starts over, again and again, and is never ready.
            await asyncio.sleep(10)
            assert service.exit_status is None, await pod.logs(database)
            assert await pod.health(database) == "starting"
            data = service.session_dir / "rootfs" / DATA[database].lstrip("/")
            assert owner(data) == os.getuid()
        else:  # with one id, chown to their user fails, and they exit at once
            await until(lambda: service.exit_status is not None, 120)
            assert service.exit_status != 0, await pod.logs(database)
            assert "Invalid argument" in await pod.logs(database)
        await pod.stop()
        return
    service = await pod.start_service(spec, wait="healthy")
    healthy = time.monotonic() - started
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE)))
    reached = await main.exec(f"nc -z -w 5 {database} {PORTS[database]} && echo open")
    assert reached.stdout == "open\n", reached
    data = service.session_dir / "rootfs" / DATA[database].lstrip("/")
    if run_as == "caller":  # it runs as your uid: what it writes is yours
        assert owner(data) == os.getuid()
        assert (await service.exec("id -u")).stdout == f"{os.getuid()}\n"
    else:  # its own user's: one of your subordinate ids on the host
        assert owner(data) in pod._ids.uids.ids, owner(data)
    assert await pod.health(database) == "healthy"
    started = time.monotonic()
    path = pod.path
    await pod.stop()
    stopped = time.monotonic() - started
    assert await provider.wait_for_deletions(120) and not path.exists()
    assert list((provider.pod_root / ".trash").iterdir()) == []
    print(f"\n{database} as {run_as}: healthy in {healthy:.2f} s, pod stopped in {stopped:.2f} s")


async def test_a_download_from_a_subids_service_gives_you_what_its_users_own(provider, tmp_path):
    """What a service's users other than root wrote, only for themselves, reaches the host's
    copy, which your uid owns: busybox's cp -p keeps owners, which the copy is taken from."""
    skip_unless_possible("subids")
    pod = await provider.start_pod(PodSpec(users="subids"))
    spec = ServiceSpec("svc", SandboxSpec(image=IMAGE), run_as="subids")
    service = await pod.start_service(spec)
    made = await service.exec(
        "mkdir -p /tmp/out/deep && echo kept > /tmp/out/deep/file && chmod 700 /tmp/out/deep"
        " && chmod 600 /tmp/out/deep/file && chown -R 999:999 /tmp/out"
    )
    assert made.exit_code == 0, made
    await service.download("/tmp/out", tmp_path / "out")
    assert (tmp_path / "out" / "deep" / "file").read_text() == "kept\n"
    await pod.stop()


@pytest.mark.parametrize("run_as", RUN_AS)
async def test_only_a_service_with_subordinate_ids_switches_users(provider, run_as):
    skip_unless_possible(run_as)
    pod = await provider.start_pod(PodSpec(users="subids" if run_as == "subids" else "single"))
    spec = ServiceSpec(
        "svc", SandboxSpec(image=IMAGE), command=["/bin/sh", "-c", SWITCH_USERS], run_as=run_as
    )
    service = await pod.start_service(spec)
    tmp = service.session_dir / "tmp"
    if run_as == "root" and RUNTIME == "enroot":  # enroot's seccomp filter: su switches nothing
        await until(lambda: (tmp / "ready").exists() or service.exit_status is not None, 60)
        assert (tmp / "ready").exists(), await pod.logs("svc")
        assert (tmp / "uid").read_text() == "0\n" and owner(tmp / "own") == os.getuid()
        await pod.stop()
        return
    if run_as != "subids":  # the caller cannot add a user; root of one id cannot switch to it
        await until(lambda: service.exit_status is not None, 60)
        assert service.exit_status != 0, await pod.logs("svc")
        await pod.stop()
        return
    await until(lambda: (tmp / "ready").exists() or service.exit_status is not None, 60)
    assert (tmp / "ready").exists(), await pod.logs("svc")
    assert (await service.exec("cat /tmp/uid; stat -c '%u %a' /tmp/own")).stdout == "999\n999 700\n"
    assert owner(tmp / "own") == pod._ids.uids.start + 998
    assert await pod.health("svc") == "healthy"  # its command switched users and runs on
    down = await service.exec("ip link set lo down")  # its root, with every subordinate id
    assert down.exit_code != 0 and "not permitted" in down.stderr.lower(), down
    path = pod.path
    await pod.stop()
    assert await provider.wait_for_deletions(120) and not path.exists()
    assert list((provider.pod_root / ".trash").iterdir()) == []


@pytest.mark.parametrize("run_as", RUN_AS)
async def test_an_idle_service_outlives_a_kill_of_every_process(provider, run_as):
    """A command's kill -9 -1 kills what the service's commands run, never the service, whose
    keepalive is PID 1 there, as a sandbox's and a Docker container's are; nothing of the
    service's own starts again behind it, so its processes are the ones it had before (what was
    killed waits there as a zombie, which a PID 1 that forks nothing never reaps)."""
    skip_unless_possible(run_as)
    if RUNTIME == "enroot":
        pytest.skip("with enroot, the service's init is the PID 1 that such a kill spares")
    listing = (
        'for p in /proc/[0-9]*; do p=${p#/proc/}; [ "$p" = "$$" ] && continue; '
        'read -r s < "/proc/$p/stat" 2>/dev/null || continue; '
        'case $s in *") Z "*) continue ;; esac; echo "$p"; done'
    )
    pod = await provider.start_pod(PodSpec(users="subids" if run_as == "subids" else "single"))
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE), run_as=run_as))
    before = set((await main.exec(listing)).stdout.split())
    await main.exec("sleep 300 & kill -9 -1")
    assert (await main.exec("echo alive")).stdout == "alive\n"
    assert set((await main.exec(listing)).stdout.split()) <= before
    assert await pod.health("main") == "healthy"
    await pod.stop()


CRASH_SUBIDS = """\
import asyncio, os, signal, sys
sys.path.insert(0, {src!r})
from hpc_sandbox import SandboxSpec, SingularityProvider
from hpc_sandbox.types import PodSpec, ServiceSpec

async def main():
    scratch, image, script = sys.argv[1:]
    provider = SingularityProvider(scratch_root=scratch)
    pod = await provider.start_pod(PodSpec(users="subids", labels={{"t": "crash"}}))
    command = ["/bin/sh", "-c", script]
    spec = ServiceSpec("svc", SandboxSpec(image=image), command=command, run_as="subids")
    service = await pod.start_service(spec)
    for _ in range(600):
        if (service.session_dir / "tmp" / "ready").exists() or service.exit_status is not None:
            break
        await asyncio.sleep(0.1)
    if not (service.session_dir / "tmp" / "ready").exists():
        print(await pod.logs("svc"), file=sys.stderr, flush=True)
    print(pod.id, pod._holder.pid, service._service_anchor.pid, service._service_init, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

asyncio.run(main())
"""


async def test_the_janitor_gives_back_what_a_dead_owners_subids_pod_left(provider, scratch):
    skip_unless_possible("subids")
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", CRASH_SUBIDS.format(src=SRC), str(scratch), IMAGE, SWITCH_USERS],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    pod_id, *pids = result.stdout.split()
    await until(lambda: not any(owned_live_start_time(int(pid)) for pid in pids), 10)
    path = provider.pod_root / pod_id
    owned = list(path.glob("services/hpcsb-*/tmp/own"))
    assert len(owned) == 1, result.stderr
    assert owner(owned[0]) != os.getuid()  # a subordinate id's, which you cannot delete
    report = await provider.reap()
    assert pod_id in report.removed and not report.errors
    assert await provider.wait_for_deletions(120) and not path.exists()
    assert list((provider.pod_root / ".trash").iterdir()) == []
