"""Pod services in gVisor, on the fake runtimes: a fake ``runsc`` under the real launcher, in the
real namespaces of a pod whose other services run on the fake Singularity or enroot."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import threading
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from conftest import (
    FakeRunsc,
    FakeRuntime,
    hide_from_path,
    make_enroot_provider,
    make_gvisor_provider,
    make_provider,
)
from hpc_sandbox import (
    GvisorProvider,
    GvisorSandbox,
    GvisorService,
    HealthCheck,
    Mount,
    PodService,
    PodSpec,
    SandboxLostError,
    SandboxSpec,
    SandboxStartError,
    ServiceSpec,
    SingularityProvider,
    procfs,
)
from hpc_sandbox.gvisor import PR_SET_SECUREBITS, securebits_filter
from hpc_sandbox.procfs import owned_live_start_time

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


def judge(image: str, port: int, **options: Any) -> ServiceSpec:
    """A gVisor service that answers HTTP on ``port``."""
    sandbox = options.pop("sandbox", SandboxSpec(image=image, env={"GREETING": "judge-ok"}))
    return ServiceSpec(
        "judge",
        sandbox,
        command=server(port),
        healthcheck=ready(port),
        runtime="gvisor",
        **options,
    )


@pytest.fixture(params=["singularity", "enroot"])
async def pods(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    fake_runsc: FakeRunsc,
    fake_runtime: FakeRuntime,
) -> AsyncIterator[tuple[Any, str, str]]:
    """A provider with pods on a fake runtime, whose gVisor is the fake runsc; an image for its own
    services, and a SIF image for gVisor's."""
    sif = str(fake_runtime.make_sif(tmp_path / "images" / "judge.sif", {"app/x": "x\n"}))
    gvisor = make_gvisor_provider(tmp_path)
    if request.param == "enroot":
        fake = request.getfixturevalue("fake_enroot")
        image = str(fake.make_sqsh(tmp_path / "images" / "main.sqsh", {"app/x": "x\n"}))
        provider: Any = make_enroot_provider(tmp_path, gvisor=gvisor, service_stop_grace_s=3.0)
    else:
        image = sif
        provider = make_provider(tmp_path, gvisor=gvisor, service_stop_grace_s=3.0)
    yield provider, image, sif
    for pod in provider.pods:
        await pod.stop()


@pytest.fixture
def gateway(tmp_path: Path) -> Iterator[Path]:
    """A Unix socket answering HTTP, standing in for an upstream service on the host."""
    path = tmp_path / "gw.sock"
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(str(path))
    listener.listen(16)

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


def namespace(pid: int, kind: str) -> str:
    return os.readlink(f"/proc/{pid}/ns/{kind}")


async def http_get(pod: Any, port: int) -> str:
    reader, writer = await pod.open_connection(port)
    writer.write(b"GET / HTTP/1.0\r\n\r\n")
    await writer.drain()
    data = await asyncio.wait_for(reader.read(), 10)
    writer.close()
    return data.split(b"\r\n\r\n", 1)[-1].decode().strip()


async def test_a_gvisor_service_is_one_of_the_pods_services(pods, gateway, fake_runsc):
    provider, image, sif = pods
    port = free_port()
    spec = PodSpec(
        upstreams={"api": f"unix://{gateway}"}, exposes=[port], hosts=["alias"], labels={"t": "g"}
    )
    pod = await provider.start_pod(spec)
    service = await pod.start_service(judge(sif, port), wait="healthy")
    assert isinstance(service, GvisorService) and "gvisor_services" in provider.capabilities
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image)))
    # By name from the other services, from the host through an exposed port, and out to the
    # upstream: the service's sockets are on the pod's loopback.
    assert (await main.exec(fetch(f"http://judge:{port}/"))).stdout == "judge-ok\n"
    assert await http_get(pod, port) == "judge-ok"
    assert (await service.exec(fetch("http://127.0.0.1:18080/"))).stdout == "gateway-ok\n"
    assert (await service.exec("hostname")).stdout == "judge\n"
    hosts = (await service.exec("cat /etc/hosts")).stdout  # main joined after judge
    assert {"127.0.0.1\tjudge", "127.0.0.1\tmain", "127.0.0.1\talias"} <= set(hosts.splitlines())
    assert (await service.exec("cat /etc/resolv.conf")).stdout == "nameserver 10.0.2.3\n"
    assert (await service.exec("echo $GREETING $HOME")).stdout == "judge-ok /root\n"
    assert await pod.health("judge") == "healthy" and "serving" in await pod.logs("judge")
    # The launcher joined the pod's network namespace, and nested a user namespace in the pod's.
    launcher = service._launcher
    assert launcher is not None
    assert launcher.args[5:7] == ["gvisor-service", str(pod.holder_pid)]
    (runsc,) = procfs.children(launcher.pid)
    assert namespace(runsc, "net") == namespace(pod.holder_pid, "net")
    assert namespace(runsc, "user") not in (
        namespace(pod.holder_pid, "user"),
        namespace(os.getpid(), "user"),
    )
    owner = json.loads((service.session_dir / "owner.json").read_text())
    assert (owner["provider"], owner["runtime"], owner["pod"]) == (provider.name, "gvisor", pod.id)
    assert owned_live_start_time(owner["sentry_pid"]) == owner["sentry_start_time"]
    (run,) = fake_runsc.calls("run")
    assert "--network=host" in run["argv"] and "--oci-seccomp" not in run["argv"]
    await pod.stop()


async def test_a_pod_whose_every_service_runs_in_gvisor(pods, tmp_path):
    provider, _, sif = pods
    port = free_port()
    pod = await provider.start_pod(PodSpec(exposes=[port]))
    await pod.start_service(judge(sif, port), wait="healthy")
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=sif), runtime="gvisor"))
    assert all(isinstance(service, GvisorService) for service in pod.services.values())
    assert (await main.exec(fetch(f"http://judge:{port}/"))).stdout == "judge-ok\n"
    assert await http_get(pod, port) == "judge-ok"
    source = tmp_path / "in.txt"
    source.write_text("hello\n")
    await main.upload(source, "/tmp/in.txt")
    assert (await main.exec("cat /tmp/in.txt && echo back > /tmp/out.txt")).stdout == "hello\n"
    await main.download("/tmp/out.txt", tmp_path / "out.txt")
    assert (tmp_path / "out.txt").read_text() == "back\n"
    await pod.stop()


async def test_its_command_is_pid_1_and_its_exit_ends_the_service(pods):
    provider, _, sif = pods
    pod = await provider.start_pod(PodSpec())
    spec = SandboxSpec(image=sif)
    done = await pod.start_service(
        ServiceSpec("done", spec, command=["/bin/sh", "-c", "exit 3"], runtime="gvisor")
    )
    for _ in range(100):  # the command exits once it has its environment
        if await pod.health("done") == "exited":
            break
        await asyncio.sleep(0.05)
    assert await pod.health("done") == "exited" and done.exit_status == 3
    with pytest.raises(SandboxLostError, match="command exited"):
        await done.exec("true")
    # Its working directory is there as soon as the start returns.
    working = await pod.start_service(
        ServiceSpec(
            "working",
            SandboxSpec(image=sif, workdir="/srv/work"),
            command=["sleep", "600"],
            runtime="gvisor",
        )
    )
    assert (await working.exec("pwd")).stdout == "/srv/work\n"
    idle = await pod.start_service(ServiceSpec("idle", spec, runtime="gvisor", run_as="subids"))
    assert (await idle.exec("id -u; cat /proc/1/cmdline | tr '\\0' ' '")).stdout.startswith(
        "0\nsleep 2147483647"
    )
    await pod.stop()


async def test_a_graceful_stop_reaches_a_pid_1_that_handles_sigterm(pods):
    provider, _, sif = pods
    pod = await provider.start_pod(PodSpec())
    spec = SandboxSpec(image=sif)
    handled = await pod.start_service(
        ServiceSpec(
            "handled",
            spec,
            command=["/bin/sh", "-c", "trap 'exit 7' TERM; while :; do sleep 0.1; done"],
            runtime="gvisor",
        )
    )
    unhandled = await pod.start_service(
        ServiceSpec("unhandled", spec, command=["sleep", "600"], runtime="gvisor")
    )
    processes = [(service._launcher.pid, service._sentry_pid) for service in (handled, unhandled)]
    path = pod.path
    await pod.stop()
    # As in a Docker container, PID 1 gets SIGTERM only when it handles it; the other is killed.
    assert (handled.exit_status, unhandled.exit_status) == (7, 128 + signal.SIGKILL)
    assert [pid for pair in processes for pid in pair if owned_live_start_time(pid)] == []
    assert await provider.wait_for_deletions(30) and not path.exists()


async def test_the_memory_limit_stops_the_whole_service(pods, fake_runsc):
    provider, _, sif = pods
    pod = await provider.start_pod(PodSpec())
    spec = SandboxSpec(image=sif, memory_mb=40)
    service = await pod.start_service(ServiceSpec("hog", spec, runtime="gvisor"))
    fake_runsc.set_value("memory-file-mb", "64")
    for _ in range(100):
        if service.memory_kills:
            break
        await asyncio.sleep(0.1)
    assert service.memory_kills and service.memory_kills[0].measure == "gVisor's memory"
    with pytest.raises(SandboxLostError, match="whole sandbox was stopped"):
        await service.exec("true")
    assert await pod.health("hog") == "exited"
    await pod.stop()


async def test_limits_mounts_and_cpus(pods, tmp_path):
    provider, image, sif = pods
    if len(os.sched_getaffinity(0)) < 2:
        pytest.skip("needs two CPUs")
    volume = tmp_path / "volume"
    volume.mkdir()
    pod = await provider.start_pod(PodSpec())
    mounts = [Mount(str(volume), "/vol", read_only=False)]
    service = await pod.start_service(
        ServiceSpec(
            "limited",
            SandboxSpec(image=sif, mounts=mounts, cpus=1),
            ulimits={"stack": (-1, -1), "nofile": (1024, 2048), "nproc": (5, 5)},
            shm_mb=32,
            runtime="gvisor",
        )
    )
    main = await pod.start_service(ServiceSpec("main", SandboxSpec(image=image, mounts=mounts)))
    bundle = json.loads((service.session_dir / "config.json").read_text())
    limits = {
        limit["type"]: (limit["soft"], limit["hard"]) for limit in bundle["process"]["rlimits"]
    }
    assert limits["RLIMIT_STACK"] == ((1 << 64) - 1, (1 << 64) - 1)
    assert limits["RLIMIT_NOFILE"] == (1024, 2048) and "RLIMIT_NPROC" not in limits
    assert (await service.exec("ulimit -n")).stdout == "1024\n"
    statvfs = "import os; s = os.statvfs('/dev/shm'); print(s.f_blocks * s.f_frsize >> 20)"
    assert (await service.exec([PYTHON, "-c", statvfs])).stdout == "32\n"
    # One volume, written from either side.
    await main.exec("echo from-main > /vol/a")
    assert (await service.exec("cat /vol/a && echo from-gvisor > /vol/b")).stdout == "from-main\n"
    assert (await main.exec("cat /vol/b")).stdout == "from-gvisor\n"
    assert (volume / "b").stat().st_uid == os.getuid()
    # Kept to one CPU, chosen by the gVisor provider, from the launcher on.
    launcher = service._launcher
    (cpu,) = service._cpus
    assert launcher.args[7:9] == ["--cpus", str(cpu)]
    assert os.sched_getaffinity(service._sentry_pid) == {cpu}
    await pod.stop()


async def test_securebits_and_guest_cgroups_reach_runsc(tmp_path, fake_runsc, fake_runtime):
    sif = str(fake_runtime.make_sif(tmp_path / "images" / "judge.sif"))
    gvisor = make_gvisor_provider(tmp_path, guest_cgroups="v1")
    provider = make_provider(tmp_path, gvisor=gvisor)
    pod = await provider.start_pod(PodSpec())
    spec = SandboxSpec(image=sif)
    plain = await pod.start_service(ServiceSpec("plain", spec, runtime="gvisor"))
    judging = await pod.start_service(
        ServiceSpec("judging", spec, runtime="gvisor", securebits="ignore", guest_cgroups="v2")
    )
    for service, seccomp, version in ((plain, False, "v1"), (judging, True, "v2")):
        bundle = json.loads((service.session_dir / "config.json").read_text())
        assert ("seccomp" in bundle["linux"]) is seccomp
        assert any(m["destination"] == "/sys/fs/cgroup" for m in bundle["mounts"])
        (run,) = [call for call in fake_runsc.calls("run") if call["argv"][-1] == service.id]
        assert ("--oci-seccomp" in run["argv"]) is seccomp
        assert ("--in-sandbox-cgroup=v2" in run["argv"]) is (version == "v2")
    bundle = json.loads((judging.session_dir / "config.json").read_text())
    assert bundle["linux"]["seccomp"] == securebits_filter()
    (rule,) = securebits_filter()["syscalls"]
    assert rule["names"] == ["prctl"] and rule["errnoRet"] == 0
    assert rule["args"] == [{"index": 0, "value": PR_SET_SECUREBITS, "op": "SCMP_CMP_EQ"}]
    await pod.stop()


async def test_the_janitor_stops_what_runs_of_a_gvisor_service(pods):
    provider, _, sif = pods
    pod = await provider.start_pod(PodSpec())
    service = await pod.start_service(
        ServiceSpec("judge", SandboxSpec(image=sif), runtime="gvisor")
    )
    owner = json.loads((service.session_dir / "owner.json").read_text())
    running = [(owner[f"{r}_pid"], owner[f"{r}_start_time"]) for r in ("sentry", "launcher")]
    assert await provider._stop_abandoned_service(service.session_dir, owner)
    assert [pid for pid, started in running if owned_live_start_time(pid) == started] == []
    await pod.stop()


CRASH = """\
import asyncio, os, signal, sys
sys.path.insert(0, {src!r})
from hpc_sandbox import GvisorProvider, SingularityProvider, PodSpec, SandboxSpec, ServiceSpec

async def main():
    scratch, cache, image = sys.argv[1:]
    common = dict(scratch_root=scratch, image_cache=cache, stage_images=False)
    provider = SingularityProvider(**common, gvisor=GvisorProvider(**common))
    pod = await provider.start_pod(PodSpec(labels={{"t": "crash"}}))
    spec = ServiceSpec("judge", SandboxSpec(image=image), runtime="gvisor")
    service = await pod.start_service(spec)
    print(pod.id, pod._holder.pid, service._launcher.pid, service._sentry_pid, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

asyncio.run(main())
"""


async def test_a_gvisor_service_dies_with_its_owner_and_the_janitor_removes_its_pod(
    tmp_path, fake_runsc, fake_runtime
):
    sif = str(fake_runtime.make_sif(tmp_path / "images" / "judge.sif"))
    provider = make_provider(tmp_path, gvisor=make_gvisor_provider(tmp_path))
    src = str(Path(__file__).resolve().parents[1] / "src")
    command = [sys.executable, "-c", CRASH.format(src=src), str(provider.scratch_root)]
    result = await asyncio.to_thread(
        subprocess.run,
        [*command, str(tmp_path / "cache"), sif],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    pod_id, *pids = result.stdout.split()
    for _ in range(50):  # the parent-death signals
        if not [pid for pid in map(int, pids) if owned_live_start_time(pid)]:
            break
        await asyncio.sleep(0.1)
    assert [pid for pid in map(int, pids) if owned_live_start_time(pid)] == []
    report = await provider.reap()
    assert pod_id in report.removed and not report.errors
    assert not (provider.pod_root / pod_id).exists()


async def test_a_gvisor_service_whose_holder_died_is_lost(pods):
    provider, _, sif = pods
    pod = await provider.start_pod(PodSpec())
    service = await pod.start_service(
        ServiceSpec("judge", SandboxSpec(image=sif), runtime="gvisor")
    )
    sentry = (service._sentry_pid, service._sentry_start_time)
    os.kill(pod.holder_pid, signal.SIGKILL)
    for _ in range(50):
        if not pod.holder_alive():
            break
        await asyncio.sleep(0.1)
    with pytest.raises(SandboxLostError, match="holder is gone"):
        await service.exec("true")
    await pod.stop()
    assert owned_live_start_time(sentry[0]) != sentry[1]


def running(argv: list[str]) -> list[int]:
    """Your processes whose command line is ``argv``."""
    wanted = "\0".join(argv) + "\0"
    found = []
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                if entry.stat().st_uid == os.getuid() and (entry / "cmdline").read_text() == wanted:
                    found.append(int(entry.name))
            except OSError:
                continue
    return found


async def test_after_its_holder_died_timeouts_and_the_memory_limit_still_apply(pods, fake_runsc):
    provider, _, sif = pods
    pod = await provider.start_pod(PodSpec())
    service = await pod.start_service(
        ServiceSpec("judge", SandboxSpec(image=sif, memory_mb=40), runtime="gvisor")
    )
    command = service.exec("sleep 3017 & sleep 3017", timeout_s=3)
    started = asyncio.ensure_future(command)
    for _ in range(100):
        if len(running(["sleep", "3017"])) == 2:
            break
        await asyncio.sleep(0.05)
    os.kill(pod.holder_pid, signal.SIGKILL)
    for _ in range(50):
        if not pod.holder_alive():
            break
        await asyncio.sleep(0.1)
    # The Sentry runs until the pod stops. A command that times out meanwhile is still killed, and
    # reports its timeout, as in a Singularity service; the next one finds the service lost.
    result = await started
    assert result.timed_out and result.exit_code == 124, result
    assert any("--pgid=" in " ".join(call["argv"]) for call in fake_runsc.calls("kill"))
    assert running(["sleep", "3017"]) == []
    with pytest.raises(SandboxLostError, match="holder is gone"):
        await service.exec("true")
    # And the memory limit still stops the whole service.
    assert service.memory_kills == []
    sentry = (service._sentry_pid, service._sentry_start_time)
    fake_runsc.set_value("memory-file-mb", "64")
    for _ in range(100):
        if service.memory_kills:
            break
        await asyncio.sleep(0.1)
    assert len(service.memory_kills) == 1
    for _ in range(50):
        if owned_live_start_time(sentry[0]) != sentry[1]:
            break
        await asyncio.sleep(0.1)
    assert owned_live_start_time(sentry[0]) != sentry[1]
    await pod.stop()


async def test_gvisor_services_are_refused_without_what_they_need(
    tmp_path, fake_runsc, fake_runtime, monkeypatch
):
    sif = str(fake_runtime.make_sif(tmp_path / "images" / "judge.sif"))
    plain = make_provider(tmp_path)
    assert "gvisor_services" not in plain.capabilities
    pod = await plain.start_pod(PodSpec())
    spec = SandboxSpec(image=sif)
    with pytest.raises(ValueError, match=r"gvisor=GvisorProvider\(\.\.\.\)"):
        await pod.start_service(ServiceSpec("judge", spec, runtime="gvisor"))
    for options, message in (
        ({"runtime": "kata"}, "runtime must be"),
        ({"securebits": "ignore"}, "securebits is a setting of gVisor services"),
        ({"guest_cgroups": "v2"}, "guest_cgroups is a setting of gVisor services"),
        ({"runtime": "gvisor", "securebits": "keep"}, "securebits must be"),
        ({"runtime": "gvisor", "guest_cgroups": "v3"}, "guest_cgroups must be"),
    ):
        with pytest.raises(ValueError, match=message):
            await pod.start_service(ServiceSpec("judge", spec, **options))
    await pod.stop()
    with pytest.raises(TypeError, match="GvisorProvider"):
        SingularityProvider(scratch_root=tmp_path / "s", gvisor="runsc")  # type: ignore[arg-type]

    provider = make_provider(tmp_path, gvisor=make_gvisor_provider(tmp_path))
    pod = await provider.start_pod(PodSpec())
    owners = tmp_path / "owners"
    owners.write_text("0 0 /app\n")
    for service, message in (
        (ServiceSpec("j", spec, runtime="gvisor", run_as="caller"), "run_as 'root' or 'subids'"),
        (
            ServiceSpec(
                "j",
                SandboxSpec(image=sif, file_owners=str(owners)),
                runtime="gvisor",
                run_as="subids",
            ),
            "file owners manifests",
        ),
        (ServiceSpec("j", SandboxSpec(image="x.sqsh"), runtime="gvisor"), "squashfs"),
        (
            ServiceSpec(
                "j",
                SandboxSpec(image=sif, mounts=[Mount(str(tmp_path), "/.hpc-sandbox/x")]),
                runtime="gvisor",
            ),
            "reserved",
        ),
    ):
        with pytest.raises(ValueError, match=message):
            await pod.start_service(service)
    # No runsc: refused before an image is pulled or extracted, leaving nothing.
    provider.gvisor.runtime._path = None
    hide_from_path(monkeypatch, "runsc")
    with pytest.raises(SandboxStartError, match="runsc"):
        await pod.start_service(ServiceSpec("judge", spec, runtime="gvisor"))
    assert not [path for path in pod.services_root.iterdir() if path.name.startswith("hpcsb-")]
    await pod.stop()


def test_a_gvisor_service_defines_every_name_both_its_parents_define():
    """PodService comes first in GvisorService's MRO: a name that it and GvisorSandbox both define
    is defined on GvisorService too, so that one PodService gains later cannot silently replace
    what runs the service in gVisor."""

    def defined(cls: type) -> set[str]:
        return {name for name in vars(cls) if name == "__init__" or not name.startswith("__")}

    shared = defined(PodService) & defined(GvisorSandbox)
    assert {"_spawn", "_start_container", "_stop_container"} <= shared
    assert sorted(shared - defined(GvisorService)) == []
    assert "_memory_watchdog" not in defined(GvisorService)  # PodService has none


def test_the_gvisor_option_takes_a_gvisor_provider(tmp_path):
    gvisor = GvisorProvider(scratch_root=tmp_path)
    provider = SingularityProvider(scratch_root=tmp_path, gvisor=gvisor)
    assert provider.gvisor is gvisor and "gvisor_services" in provider.capabilities
    with pytest.raises(TypeError):
        SingularityProvider(scratch_root=tmp_path, gvisor=object())  # type: ignore[arg-type]
