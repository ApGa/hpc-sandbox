"""Pod services in gVisor end to end: a real ``runsc`` running services of Singularity CE or
Apptainer pods, on a compute node.

Run on a compute node (never a login node), with runsc on ``PATH`` (with its release's
``gvisor-bin/`` beside it) and Singularity CE or Apptainer, which runs the pods' other services and
pulls the images of both::

    HPC_SANDBOX_INTEGRATION=1 python -m pytest tests/test_gvisor_pods_integration.py -v

Without ``HPC_SANDBOX_INTEGRATION=1``, runsc or Singularity, every test skips. Services run
``HPC_SANDBOX_TEST_POD_IMAGE`` (default ``docker://alpine:3.20``), and the checks that need Python
run ``HPC_SANDBOX_TEST_APT_IMAGE`` (default ``docker://python:3.12-slim``).
``HPC_SANDBOX_TEST_GVISOR_PLATFORM`` picks gVisor's platform (default ``systrap``). Egress checks
run where slirp4netns is found. ``HPC_SANDBOX_TEST_SUBIDS=1`` also runs a gVisor service in a pod
with ``users="subids"``. ``HPC_SANDBOX_TEST_GOJUDGE``, a directory that holds go-judge's
``go-judge`` binary, as its release archive does, runs it as a gVisor service of the pods' image,
with and without ``securebits="ignore"``. Where the pod's provider cannot run a service of its own
(Singularity CE or Apptainer in the pod), the tests that pair one with a gVisor service skip, naming
the error; ``tests/test_pods_integration.py`` tests those services.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from hpc_sandbox import (
    GvisorProvider,
    GvisorService,
    HealthCheck,
    Mount,
    PodSpec,
    SandboxError,
    SandboxLostError,
    SandboxSpec,
    ServiceSpec,
    SingularityProvider,
)
from hpc_sandbox.gvisor import STATE_DIR, runsc_processes
from hpc_sandbox.paths import default_scratch_root
from hpc_sandbox.procfs import owned_live_start_time
from hpc_sandbox.runtime import RUNTIME_CANDIDATES
from hpc_sandbox.sessions import remove_tree

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("HPC_SANDBOX_INTEGRATION") != "1",
        reason="set HPC_SANDBOX_INTEGRATION=1 on a compute node with runsc and Singularity",
    ),
    pytest.mark.skipif(shutil.which("runsc") is None, reason="needs runsc on PATH"),
    pytest.mark.skipif(
        not any(shutil.which(binary) for binary in RUNTIME_CANDIDATES),
        reason="needs singularity or apptainer on PATH, for the pods and the images",
    ),
]
IMAGE = os.environ.get("HPC_SANDBOX_TEST_POD_IMAGE", "docker://alpine:3.20")
APT_IMAGE = os.environ.get("HPC_SANDBOX_TEST_APT_IMAGE", "docker://python:3.12-slim")
PLATFORM = os.environ.get("HPC_SANDBOX_TEST_GVISOR_PLATFORM", "systrap")
SUBIDS = os.environ.get("HPC_SANDBOX_TEST_SUBIDS") == "1"
GOJUDGE = os.environ.get("HPC_SANDBOX_TEST_GOJUDGE")
SRC = str(Path(__file__).resolve().parents[1] / "src")
JUDGE = (
    'echo judge up; while true; do printf "HTTP/1.0 200 OK\\r\\n\\r\\njudge-ok $SECRET\\n" '
    '| nc -l -p "$PORT" >/dev/null; done'
)
# Run with Python: what prctl(PR_SET_SECUREBITS) does with the bits go-judge sets before each
# program (NOROOT, NO_SETUID_FIXUP and their locks, and KEEP_CAPS_LOCKED), and the bits after.
SECUREBITS = (
    "import ctypes; libc = ctypes.CDLL(None, use_errno=True); "
    "result = libc.prctl(28, 0x2F, 0, 0, 0); "
    "print('securebits', result, ctypes.get_errno(), libc.prctl(27, 0, 0, 0, 0), flush=True)"
)


def make_provider(scratch: Path, **options: Any) -> SingularityProvider:
    gvisor = GvisorProvider(scratch_root=scratch, platform=PLATFORM, **options.pop("gvisor", {}))
    return SingularityProvider(scratch_root=scratch, gvisor=gvisor, **options)


@pytest.fixture
def scratch() -> Iterator[Path]:
    path = default_scratch_root() / f"pods-gvisor-integration-{uuid.uuid4().hex[:8]}"
    yield path
    try:
        remove_tree(path)
    except OSError:
        shutil.rmtree(path, ignore_errors=True)


async def finish(provider: SingularityProvider) -> None:
    for pod in provider.pods:
        await pod.stop()
    report = await provider.reap()
    assert not report.errors, report.errors
    assert await provider.wait_for_deletions(120)  # before the scratch root goes


@pytest.fixture
async def provider(scratch: Path) -> AsyncIterator[SingularityProvider]:
    provider = make_provider(scratch, service_stop_grace_s=10.0)
    yield provider
    await finish(provider)


@pytest.fixture
def gateway(tmp_path: Path) -> Iterator[Path]:
    path = tmp_path / "gw.sock"
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(str(path))
    listener.listen(64)

    def serve() -> None:
        while True:
            try:
                connection, _ = listener.accept()
            except OSError:
                return
            with connection:
                connection.recv(4096)
                connection.sendall(b"HTTP/1.0 200 OK\r\n\r\ngateway-ok\n")

    threading.Thread(target=serve, daemon=True).start()
    yield path
    listener.close()


# Whether the pod's provider runs a service of its own here: None, or why not.
_PLAIN_SERVICES: dict[str, str | None] = {}


async def needs_plain_services(provider: SingularityProvider) -> None:
    """Skip, naming the error, where the pod's provider cannot run a service of its own: the
    tests that pair one with a gVisor service. Checked once per run."""
    if "error" not in _PLAIN_SERVICES:
        pod = await provider.start_pod(PodSpec())
        try:
            service = await pod.start_service(ServiceSpec("plain", SandboxSpec(image=IMAGE)))
            result = await service.exec("true")
            error = None if result.exit_code == 0 else f"exit {result.exit_code}: {result.stderr}"
        except SandboxError as failure:
            error = str(failure)
        finally:
            await pod.stop()
        _PLAIN_SERVICES["error"] = error
    if (error := _PLAIN_SERVICES["error"]) is not None:
        pytest.skip(f"the {provider.name} provider runs no service of its own here: {error}")


def judge(name: str = "judge", port: int = 8082, image: str = IMAGE, **options: Any) -> ServiceSpec:
    """A gVisor service that answers HTTP on ``port`` with busybox's nc."""
    ready = HealthCheck(
        ["CMD-SHELL", f"wget -qO- -T 2 http://127.0.0.1:{port}/"],
        interval_s=1,
        start_interval_s=0.2,
        start_period_s=60,
    )
    sandbox = options.pop("sandbox", None) or SandboxSpec(
        image=image, env={"SECRET": "s3cret", "PORT": str(port)}
    )
    return ServiceSpec(
        name,
        sandbox,
        command=["/bin/sh", "-c", JUDGE],
        healthcheck=ready,
        runtime="gvisor",
        **options,
    )


async def request(pod: Any, port: int, text: bytes) -> bytes:
    """The reply to ``text``, sent from the host through an exposed port. nc serves one connection
    at a time, and the holder closes a connection that finds nothing listening, so a request
    between two connections is sent again."""
    for _ in range(100):
        reader, writer = await pod.open_connection(port)
        try:
            writer.write(text)
            await writer.drain()
            async with asyncio.timeout(60):
                reply = await reader.read()
        except ConnectionError:
            reply = b""
        finally:
            writer.close()
        if reply:
            return reply
        await asyncio.sleep(0.1)
    raise AssertionError(f"port {port} of pod {pod.id} never answered")


async def ask(pod: Any, port: int) -> str:
    reply = await request(pod, port, b"GET / HTTP/1.0\r\n\r\n")
    return reply.decode().split("\r\n\r\n", 1)[-1]


async def wait_for(probe: Any, timeout_s: float = 30.0) -> Any:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if value := await probe():
            return value
        await asyncio.sleep(0.2)
    raise AssertionError("timed out")


async def test_a_gvisor_service_is_one_of_the_pods_services(provider, gateway):
    await needs_plain_services(provider)
    spec = PodSpec(upstreams={"gateway": f"unix://{gateway}"}, exposes=[8082], hosts=["alias"])
    pod = await provider.start_pod(spec)
    service = await pod.start_service(judge(), wait="healthy")
    assert isinstance(service, GvisorService)
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE)))
    # Its sockets are on the pod's loopback: the other services reach it by name, the host
    # through an exposed port, and it reaches the upstream.
    assert (await main.exec("wget -qO- -T 5 http://judge:8082/")).stdout == "judge-ok s3cret\n"
    assert await ask(pod, 8082) == "judge-ok s3cret\n"
    checks = {
        "relay": ("wget -qO- -T 5 http://127.0.0.1:18080/", "gateway-ok"),
        "hostname": ("hostname", "judge"),
        "kernel": ("uname -r | grep -c gvisor", "1"),
        "uid": ("id -u", "0"),
        "names": ("grep -cE '^127[.]0[.]0[.]1[[:space:]]+(main|alias|judge)$' /etc/hosts", "3"),
        "resolv.conf": ("cat /etc/resolv.conf", "nameserver 10.0.2.3"),
        "root's privileges": ("mount -t tmpfs none /mnt && echo mounted", "mounted"),
        "bind :80": ("nc -l -p 80 & sleep 0.5; kill $! && echo bound", "bound"),
    }
    for label, (command, expected) in checks.items():
        result = await service.exec(command)
        assert result.stdout.strip() == expected, (label, result)
    taken = await service.exec("nc -l -p 18080")
    assert taken.exit_code != 0 and "in use" in taken.stderr, taken
    # Root of gVisor's kernel holds no capability over the pod's network.
    changed = await service.exec("ip addr add 10.9.9.9/32 dev lo")
    assert changed.exit_code != 0, changed
    assert "10.9.9.9" not in (await main.exec("ip -o addr show lo")).stdout
    assert await pod.health("judge") == "healthy" and "judge up" in await pod.logs("judge")
    await pod.stop()


async def test_a_pod_whose_every_service_runs_in_gvisor(provider, tmp_path):
    pod = await provider.start_pod(PodSpec(exposes=[8082]))
    await pod.start_service(judge(), wait="healthy")
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE), runtime="gvisor"))
    assert all(isinstance(service, GvisorService) for service in pod.services.values())
    assert (await main.exec("wget -qO- -T 5 http://judge:8082/")).stdout == "judge-ok s3cret\n"
    assert await ask(pod, 8082) == "judge-ok s3cret\n"
    assert (await main.exec("uname -r | grep -c gvisor")).stdout == "1\n"
    source = tmp_path / "in.txt"
    source.write_text("hello\n")
    await main.upload(source, "/tmp/in.txt")
    assert (await main.exec("cat /tmp/in.txt && echo back > /tmp/out.txt")).stdout == "hello\n"
    await main.download("/tmp/out.txt", tmp_path / "out.txt")
    assert (tmp_path / "out.txt").read_text() == "back\n"
    await pod.stop()


async def test_a_gvisor_service_cannot_watch_or_change_the_pods_network(scratch):
    provider = make_provider(scratch, gvisor={"net_raw": True})  # raw sockets, if gVisor could
    try:
        pod = await provider.start_pod(PodSpec())
        service = await pod.start_service(
            ServiceSpec("judge", SandboxSpec(image=APT_IMAGE), runtime="gvisor")
        )
        for family, kind in (("AF_PACKET", "SOCK_RAW"), ("AF_INET", "SOCK_RAW")):
            code = f"import socket; socket.socket(socket.{family}, socket.{kind}, 1)"
            result = await service.exec(["python3", "-c", code])
            assert result.exit_code != 0 and "PermissionError" in result.stderr, result
    finally:
        await finish(provider)


async def test_a_gvisor_services_command_its_exit_and_timeouts(provider):
    pod = await provider.start_pod(PodSpec())
    spec = SandboxSpec(image=IMAGE)
    done = await pod.start_service(
        ServiceSpec("done", spec, command=["/bin/sh", "-c", "sleep 1; exit 3"], runtime="gvisor")
    )

    async def exited() -> bool:
        return await pod.health("done") == "exited"

    await wait_for(exited)
    assert done.exit_status == 3
    with pytest.raises(SandboxLostError):
        await done.exec("true")
    idle = await pod.start_service(ServiceSpec("idle", spec, runtime="gvisor"))
    result = await idle.exec("sleep 300 & sleep 300", timeout_s=2)
    assert result.timed_out, result
    left = await idle.exec("ps -o args | grep -c '^sleep 300' || true")
    assert left.stdout.strip() == "0", left
    await pod.stop()


async def test_a_graceful_stop_reaches_a_pid_1_that_handles_sigterm(provider):
    pod = await provider.start_pod(PodSpec())
    spec = SandboxSpec(image=IMAGE)
    loop = "trap 'echo got-term; exit 7' TERM; echo up; while :; do sleep 0.2; done"
    handled = await pod.start_service(
        ServiceSpec("handled", spec, command=["/bin/sh", "-c", loop], runtime="gvisor")
    )
    unhandled = await pod.start_service(
        ServiceSpec("unhandled", spec, command=["sleep", "600"], runtime="gvisor")
    )
    started = time.monotonic()
    await pod.stop()
    # As in a Docker container: PID 1 gets SIGTERM only when it handles it, and the other one is
    # killed once the grace period is over.
    assert (handled.exit_status, unhandled.exit_status) == (7, 128 + signal.SIGKILL)
    assert time.monotonic() - started < 30


async def test_volumes_are_shared_with_the_other_services(provider, tmp_path):
    await needs_plain_services(provider)
    volume, readonly = tmp_path / "volume", tmp_path / "readonly"
    for directory in (volume, readonly):
        directory.mkdir()
    (readonly / "f").write_text("ro\n")
    mounts = [Mount(str(volume), "/vol", read_only=False), Mount(str(readonly), "/srv/ro")]
    pod = await provider.start_pod(PodSpec())
    service = await pod.start_service(
        ServiceSpec("judge", SandboxSpec(image=IMAGE, mounts=mounts), runtime="gvisor")
    )
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=IMAGE, mounts=mounts)))
    write = "echo one > /vol/a && mkdir /vol/d && echo two > /vol/d/b"
    assert (await main.exec(write)).exit_code == 0
    seen = await service.exec("cat /vol/a /vol/d/b && ls /vol")
    assert seen.stdout == "one\ntwo\na\nd\n", seen
    assert (await service.exec("echo three > /vol/c && chmod 600 /vol/c")).exit_code == 0
    assert (await main.exec("cat /vol/c")).stdout == "three\n"
    assert (volume / "c").stat().st_uid == os.getuid()
    assert (await service.exec("cat /srv/ro/f")).stdout == "ro\n"
    assert (await service.exec("echo x > /srv/ro/f")).exit_code != 0
    assert (readonly / "f").read_text() == "ro\n"
    await pod.stop()


async def test_limits_of_a_gvisor_service(provider):
    if len(os.sched_getaffinity(0)) < 4:
        pytest.skip("needs four CPUs")
    pod = await provider.start_pod(PodSpec())
    command = ["/bin/sh", "-c", 'echo "stack $(ulimit -s) nproc $(nproc)"; exec sleep 600']
    service = await pod.start_service(
        ServiceSpec(
            "judge",
            SandboxSpec(image=IMAGE, cpus=2),
            command=command,
            ulimits={"stack": (-1, -1), "nofile": (1000, 2000)},
            shm_mb=128,
            runtime="gvisor",
        )
    )
    checks = {
        "cpus": ("nproc", "2"),
        "stack": ("ulimit -s", "unlimited"),
        "nofile": ("echo $(ulimit -Sn) $(ulimit -Hn)", "1000 2000"),
        "shm": ("df -k /dev/shm | tail -1 | awk '{print $2}'", str(128 * 1024)),
    }
    for label, (check, expected) in checks.items():
        result = await service.exec(check)
        assert result.stdout.strip() == expected, (label, result)

    async def logged() -> str | None:
        text = await pod.logs("judge")
        return text if "stack" in text else None

    assert "stack unlimited nproc 2" in await wait_for(logged)
    await pod.stop()


async def test_the_memory_limit_stops_the_whole_service(provider):
    pod = await provider.start_pod(PodSpec())
    service = await pod.start_service(
        ServiceSpec("hog", SandboxSpec(image=IMAGE, memory_mb=200), shm_mb=1024, runtime="gvisor")
    )
    with pytest.raises(SandboxError):  # lost while it runs, or after
        await service.exec("head -c 400m /dev/zero > /dev/shm/fill; sleep 30", timeout_s=60)
        await service.exec("true")
    assert service.memory_kills and service.memory_kills[0].measure == "gVisor's memory"
    assert await pod.health("hog") == "exited"
    await pod.stop()


async def test_a_gvisor_service_whose_holder_died_is_lost_and_stops(provider):
    pod = await provider.start_pod(PodSpec())
    idle = await pod.start_service(ServiceSpec("idle", SandboxSpec(image=IMAGE), runtime="gvisor"))
    # It fills /dev/shm past its memory limit once the holder is gone.
    fill = "sleep 15; head -c 400m /dev/zero > /dev/shm/fill; exec sleep 600"
    hog = await pod.start_service(
        ServiceSpec(
            "hog",
            SandboxSpec(image=IMAGE, memory_mb=200),
            command=["/bin/sh", "-c", fill],
            shm_mb=1024,
            runtime="gvisor",
        )
    )
    services = (idle, hog)
    sentries = [(service._sentry_pid, service._sentry_start_time) for service in services]
    states = [service.session_dir / STATE_DIR for service in services]
    # One process: what a timed-out command leaves as a zombie of the idle loop, which reaps
    # nothing, would show in runsc ps as well.
    command = asyncio.ensure_future(idle.exec("sleep 300", timeout_s=5))
    await asyncio.sleep(1)
    os.kill(pod.holder_pid, signal.SIGKILL)

    async def holder_gone() -> bool:
        return not pod.holder_alive()

    await wait_for(holder_gone)
    assert [owned_live_start_time(pid) == started for pid, started in sentries] == [True, True]
    # A command that times out is still killed inside gVisor, as in a Singularity service, and
    # reports its timeout; the next one finds the service lost.
    result = await command
    assert result.timed_out, result

    async def idle_loop_alone() -> bool:
        listing = await idle._gvisor.runtime.run(*idle._global_flags(), "ps", idle.id, timeout_s=30)
        return [row.split()[1] for row in listing.stdout.decode().splitlines()[1:]] == ["1"]

    await wait_for(idle_loop_alone, timeout_s=10)
    with pytest.raises(SandboxLostError, match="holder"):
        await idle.exec("true")
    # The memory limit still stops the whole service.
    assert hog.memory_kills == []

    async def killed() -> bool:
        return bool(hog.memory_kills)

    await wait_for(killed, timeout_s=90)
    assert len(hog.memory_kills) == 1
    await pod.stop()
    assert [owned_live_start_time(pid) == started for pid, started in sentries] == [False, False]
    assert [runsc_processes(state) for state in states] == [[], []]


CRASH = """\
import asyncio, os, signal, sys
sys.path.insert(0, {src!r})
from hpc_sandbox import GvisorProvider, SingularityProvider, PodSpec, SandboxSpec, ServiceSpec

async def main():
    scratch, image, platform = sys.argv[1:]
    gvisor = GvisorProvider(scratch_root=scratch, platform=platform)
    provider = SingularityProvider(scratch_root=scratch, gvisor=gvisor)
    pod = await provider.start_pod(PodSpec(labels={{"t": "crash"}}))
    spec = ServiceSpec("judge", SandboxSpec(image=image), runtime="gvisor")
    service = await pod.start_service(spec)
    await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    print(service._launcher.pid, service._sentry_pid, service.session_dir, flush=True)
    print(pod.id, pod._holder.pid, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

asyncio.run(main())
"""


async def test_a_dead_owners_gvisor_services_die_and_the_janitor_removes_them(provider, scratch):
    await needs_plain_services(provider)
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", CRASH.format(src=SRC), str(scratch), IMAGE, PLATFORM],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    first, last = result.stdout.splitlines()
    launcher, sentry, session = first.split()
    pod_id, holder = last.split()
    pids = [int(launcher), int(sentry), int(holder)]
    deadline = time.monotonic() + 10
    while any(owned_live_start_time(pid) for pid in pids) and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    assert [pid for pid in pids if owned_live_start_time(pid)] == []
    assert runsc_processes(Path(session) / STATE_DIR) == []
    report = await provider.reap()
    assert pod_id in report.removed and not report.errors
    assert not (provider.pod_root / pod_id).exists()


async def test_egress_reaches_a_gvisor_service_and_is_cut(provider):
    try:
        provider.slirp4netns()
    except SandboxError as error:
        pytest.skip(str(error))
    pod = await provider.start_pod(PodSpec(network="egress"))
    service = await pod.start_service(
        ServiceSpec("judge", SandboxSpec(image=IMAGE), runtime="gvisor")
    )
    fetch = "wget -qO- -T 10 http://deb.debian.org/debian/ | head -c 100"
    assert (await service.exec(fetch)).stdout, "no egress"
    await pod.set_network("none")
    cut = await service.exec("wget -qO- -T 3 http://deb.debian.org/ 2>&1; echo rc=$?")
    assert "rc=0" not in cut.stdout, cut
    await pod.set_network("egress")
    assert (await service.exec(fetch)).stdout, "no egress after it was restored"
    await pod.stop()


async def test_securebits_ignore_lets_prctl_succeed_without_setting_them(provider):
    pod = await provider.start_pod(PodSpec())
    command = ["/bin/sh", "-c", f'python3 -c "{SECUREBITS}"; exec sleep 600']
    spec = SandboxSpec(image=APT_IMAGE)
    ignoring = await pod.start_service(
        ServiceSpec("ignoring", spec, command=command, runtime="gvisor", securebits="ignore")
    )
    refusing = await pod.start_service(
        ServiceSpec("refusing", spec, command=command, runtime="gvisor")
    )

    async def logged(name: str) -> str:
        async def found() -> str | None:
            text = await pod.logs(name)
            return text if "securebits" in text else None

        return await wait_for(found)

    # The command, and what it starts, get the filter: prctl returns 0, and no bit is set.
    assert "securebits 0 0 0" in await logged("ignoring")
    assert "securebits -1 1 0" in await logged("refusing")  # EPERM
    # So do the commands entered with exec.
    entered = await ignoring.exec(["python3", "-c", SECUREBITS])
    assert entered.stdout.strip() == "securebits 0 0 0", entered
    assert (await refusing.exec(["python3", "-c", SECUREBITS])).stdout.strip() == (
        "securebits -1 1 0"
    )
    await pod.stop()


@pytest.mark.skipif(not SUBIDS, reason="set HPC_SANDBOX_TEST_SUBIDS=1 where you have subids")
async def test_a_subids_pod_runs_gvisor_services(provider, tmp_path):
    await needs_plain_services(provider)
    volume = tmp_path / "volume"
    volume.mkdir()
    mounts = [Mount(str(volume), "/vol", read_only=False)]
    pod = await provider.start_pod(PodSpec(users="subids"))
    sandbox = SandboxSpec(image=IMAGE, env={"SECRET": "s3cret", "PORT": "8082"}, mounts=mounts)
    service = await pod.start_service(judge(run_as="subids", sandbox=sandbox), wait="healthy")
    main = await pod.start_service(
        ServiceSpec("main", SandboxSpec(image=IMAGE, mounts=mounts), run_as="subids")
    )
    assert (await main.exec("wget -qO- -T 5 http://judge:8082/")).stdout == "judge-ok s3cret\n"
    assert (await service.exec("id -u")).stdout == "0\n"
    try:
        # What a service wrote as another user is nobody's in gVisor, which reads it only as
        # others may.
        write = "echo a > /vol/open && echo b > /vol/closed && chmod 644 /vol/open && "
        write += "chmod 600 /vol/closed && chown 1000:1000 /vol/open /vol/closed"
        assert (await main.exec(write)).exit_code == 0
        for name, mode in (("open", "-rw-r--r--"), ("closed", "-rw-------")):
            owner = await service.exec(f"ls -ln /vol/{name} | awk '{{print $1, $3}}'")
            assert owner.stdout == f"{mode} 65534\n", (name, owner)
        assert (await service.exec("cat /vol/open")).stdout == "a\n"
        assert (await service.exec("cat /vol/closed")).exit_code != 0
        # A chown in a mount gives the host's file no other owner.
        assert (await service.exec("echo g > /vol/g; chown 1000:1000 /vol/g")).exit_code == 0
        assert (volume / "g").stat().st_uid == os.getuid()
    finally:
        await main.exec("chown -R 0:0 /vol")  # back to your uid, for the test's cleanup
    await pod.stop()


async def run_in_go_judge(pod: Any, port: int, command: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps({"cmd": [command]}).encode()
    head = f"POST /run HTTP/1.0\r\nContent-Type: application/json\r\nContent-Length: {len(body)}"
    reply = await request(pod, port, head.encode() + b"\r\n\r\n" + body)
    (result,) = json.loads(reply.split(b"\r\n\r\n", 1)[1])
    return result


@pytest.mark.skipif(not GOJUDGE, reason="set HPC_SANDBOX_TEST_GOJUDGE to go-judge's directory")
async def test_go_judge_runs_as_a_gvisor_service_with_securebits_ignored(provider):
    assert GOJUDGE is not None
    binaries = [Mount(str(Path(GOJUDGE).resolve()), "/opt/go-judge")]
    pod = await provider.start_pod(PodSpec(exposes=[5050, 5051]))
    for port, securebits in ((5050, "ignore"), (5051, None)):
        start = (
            f"mkdir -p /tmp/gojudge && exec /opt/go-judge/go-judge -http-addr 127.0.0.1:{port} "
            "-parallelism 2 -dir /tmp/gojudge"
        )
        await pod.start_service(
            ServiceSpec(
                f"judge{port}",
                SandboxSpec(image=IMAGE, mounts=binaries, memory_mb=4096, cpus=2),
                command=["/bin/sh", "-c", start],
                runtime="gvisor",
                securebits=securebits,  # type: ignore[arg-type]
                guest_cgroups="v2",  # go-judge as root needs cgroups
            )
        )
    echo = {
        "args": ["/bin/echo", "hi"],
        "env": ["PATH=/usr/bin:/bin"],
        "files": [
            {"content": ""},
            {"name": "stdout", "max": 10240},
            {"name": "stderr", "max": 10240},
        ],
        "cpuLimit": 1_000_000_000,
        "memoryLimit": 104_857_600,
        "procLimit": 50,
    }
    accepted = await run_in_go_judge(pod, 5050, echo)
    assert (accepted["status"], accepted["files"]["stdout"]) == ("Accepted", "hi\n"), accepted
    spin = {**echo, "args": ["/bin/sh", "-c", "while :; do :; done"], "cpuLimit": 500_000_000}
    assert (await run_in_go_judge(pod, 5050, spin))["status"] == "Time Limit Exceeded"
    refused = await run_in_go_judge(pod, 5051, echo)
    assert refused["status"] == "Internal Error" and "keep_capability" in json.dumps(refused)
    await pod.stop()
