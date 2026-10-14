"""End-to-end checks against a real runtime on a compute node.

Run on a compute node (never a login node) with::

    HPC_SANDBOX_INTEGRATION=1 python -m pytest tests/test_integration.py -v

``HPC_SANDBOX_TEST_RUNTIME`` picks the runtime (``singularity``, ``enroot``,
or ``auto``: the first of singularity, apptainer and enroot on PATH).
``HPC_SANDBOX_TEST_IMAGE`` picks the image (default ``docker://alpine:3.20``;
any image with /bin/sh, sleep, tail and cp works, including a local .sif or
.sqsh). Scratch defaults to the job-local directory, as in real runs; with
Singularity, set ``HPC_SANDBOX_TEST_NETWORK_NONE=1`` where the site allows
network namespaces.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from hpc_sandbox import EnrootProvider, SandboxSpec, SingularityProvider
from hpc_sandbox.cli import resolve_runtime
from hpc_sandbox.paths import default_scratch_root
from hpc_sandbox.process import run_process
from hpc_sandbox.procfs import owned_live_start_time
from hpc_sandbox.sandbox import SessionProvider
from hpc_sandbox.sessions import remove_tree

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("HPC_SANDBOX_INTEGRATION") != "1",
        reason="set HPC_SANDBOX_INTEGRATION=1 on a compute node with a container runtime",
    ),
]
IMAGE = os.environ.get("HPC_SANDBOX_TEST_IMAGE", "docker://alpine:3.20")
RUNTIME = resolve_runtime(os.environ.get("HPC_SANDBOX_TEST_RUNTIME", "auto"))
NETWORK_NONE = RUNTIME == "enroot" or os.environ.get("HPC_SANDBOX_TEST_NETWORK_NONE") == "1"
SRC = str(Path(__file__).resolve().parents[1] / "src")
enroot_only = pytest.mark.skipif(RUNTIME != "enroot", reason="enroot provider behavior")


def make_provider(scratch: Path) -> SessionProvider[Any]:
    if RUNTIME == "enroot":
        return EnrootProvider(scratch_root=scratch)
    return SingularityProvider(scratch_root=scratch, allow_network_none=NETWORK_NONE)


@pytest.fixture
def scratch() -> Iterator[Path]:
    path = default_scratch_root() / f"integration-{uuid.uuid4().hex[:8]}"
    yield path
    try:
        remove_tree(path)
    except OSError:
        shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
async def provider(scratch: Path) -> AsyncIterator[SessionProvider[Any]]:
    provider = make_provider(scratch)
    yield provider
    for sandbox in provider.sandboxes:
        await sandbox.stop()
    report = await provider.reap()
    assert not report.errors, report.errors


async def test_lifecycle_keeps_the_uid_and_isolates_tmp(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, env={"WORDS": "a b; $(c)"}))
    session = sandbox.session_dir
    assert session is not None
    result = await sandbox.exec('id -u; printf "%s\\n" "$WORDS"; pwd')
    assert result.exit_code == 0, result.stderr
    uid = "0" if RUNTIME == "enroot" else str(os.getuid())  # enroot maps root to this uid
    assert result.stdout.splitlines() == [uid, "a b; $(c)", "/app"]
    marker = f"hpc-sandbox-{uuid.uuid4().hex}"
    assert (await sandbox.exec(f"touch /etc/{marker} /tmp/{marker} && echo ok")).stdout == "ok\n"
    assert (session / "tmp" / marker).exists() and not Path("/tmp", marker).exists()
    assert (session / "rootfs" / "etc" / marker).stat().st_uid == os.getuid()
    assert (await sandbox.exec(["cat"], stdin=b"piped")).stdout == "piped"
    await sandbox.stop()
    assert not session.exists()


async def test_timeouts_kill_the_command_inside_the_container(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    result = await sandbox.exec("sleep 300", timeout_s=2)
    assert result.timed_out and result.exit_code == 124
    await asyncio.sleep(1)
    processes = await sandbox.exec("ps -o args 2>/dev/null || ps")
    assert "sleep 300" not in processes.stdout


async def test_transfers_round_trip(provider, tmp_path):
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    tree = tmp_path / "tree"
    (tree / "nested").mkdir(parents=True)
    (tree / "nested" / "data.bin").write_bytes(bytes(range(256)) * 1000)
    await sandbox.upload(tree, "/app/tree")
    await sandbox.download("/app/tree", tmp_path / "back")
    assert (tmp_path / "back" / "nested" / "data.bin").read_bytes() == bytes(range(256)) * 1000


async def test_snapshot_and_restore(provider):
    spec = SandboxSpec(image=IMAGE)
    sandbox = await provider.start(spec)
    await sandbox.exec("echo v1 > /app/state && echo note > /tmp/note")
    snapshot = await sandbox.snapshot()
    await sandbox.exec("echo v2 > /app/state")
    restored = await provider.restore(snapshot, spec)
    assert (await restored.exec("cat /app/state /tmp/note")).stdout == "v1\nnote\n"
    await provider.delete_snapshot(snapshot)


async def test_memory_watchdog_kills_the_hog_and_keeps_the_sandbox(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, memory_mb=128))
    hog = "tail -c 600000000 /dev/zero"  # never sees EOF: holds 600 MB until killed
    result = await sandbox.exec(hog, timeout_s=120)
    assert result.exit_code == 137, (result.stdout, result.stderr)
    assert "memory limit exceeded" in result.stderr
    assert (await sandbox.exec("echo alive")).stdout == "alive\n"


CRASH = """\
import asyncio, os, signal, sys
from hpc_sandbox import EnrootProvider, SandboxSpec, SingularityProvider

async def main():
    runtime, scratch, image = sys.argv[1:]
    providers = {"enroot": EnrootProvider, "singularity": SingularityProvider}
    provider = providers[runtime](scratch_root=scratch)
    sandbox = await provider.start(SandboxSpec(image=image, labels={'t': 'j'}))
    pid = sandbox._init_pid if runtime == "enroot" else sandbox._instance_pid
    print(pid, sandbox.id, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

asyncio.run(main())
"""


async def test_janitor_recovers_an_abandoned_sandbox(provider, scratch):
    result = await run_process(
        [sys.executable, "-c", CRASH, RUNTIME, str(scratch), IMAGE],
        timeout_s=600,
        env={**os.environ, "PYTHONPATH": SRC},
    )
    assert result.returncode == 137, result.detail()
    pid, name = result.stdout.decode().split()
    if RUNTIME == "enroot":
        # The container dies with its owner; only its files are left.
        for _ in range(100):
            if owned_live_start_time(int(pid)) is None:
                break
            await asyncio.sleep(0.05)
        assert owned_live_start_time(int(pid)) is None
    else:
        assert owned_live_start_time(int(pid)) is not None
    report = await provider.reap({"t": "j"})
    assert report.removed == [name] and not report.errors
    assert owned_live_start_time(int(pid)) is None


@pytest.mark.skipif(
    not NETWORK_NONE,
    reason="set HPC_SANDBOX_TEST_NETWORK_NONE=1 where users may create network namespaces",
)
async def test_network_none_has_only_loopback(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, network="none"))
    # /proc/net/dev lists exactly the namespace's interfaces. /sys/class/net also holds plain
    # files, e.g. bonding_masters wherever the bonding driver is loaded.
    result = await sandbox.exec("cat /proc/net/dev")
    assert result.exit_code == 0, result.stderr
    interfaces = [
        line.split(":", 1)[0].strip() for line in result.stdout.splitlines()[2:] if ":" in line
    ]
    assert interfaces == ["lo"]


@enroot_only
async def test_enroot_containers_are_kept_apart_from_the_host(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    mounts = (await sandbox.exec("cut -d' ' -f5 /proc/self/mountinfo")).stdout.split()
    assert "/root" not in mounts  # the site's home-directory hook stays off
    assert "/dev/shm" in mounts and "/tmp" in mounts
    environment = (await sandbox.exec("env")).stdout
    assert "SLURM_" not in environment and "PMIX_" not in environment
    background = (await sandbox.exec("sleep 300 >/dev/null 2>&1 & echo $!")).stdout.strip()
    assert (await sandbox.exec("kill -9 -1; echo survived")).stdout == "survived\n"
    assert (await sandbox.exec(f"kill -0 {background} 2>/dev/null || echo gone")).stdout == "gone\n"
    assert (await sandbox.exec("tr '\\0' ' ' < /proc/1/cmdline")).stdout.endswith(f"{sandbox.id} ")


@enroot_only
async def test_enroot_loopback_works_without_a_network(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, network="none"))
    if (await sandbox.exec("command -v nc && command -v wget")).exit_code != 0:
        pytest.skip("the image has no nc and wget")
    listen = "(nc -l -p 8080 -s 127.0.0.1 > /tmp/received &); sleep 0.5"
    assert (await sandbox.exec(listen)).exit_code == 0
    assert (await sandbox.exec("echo hello | nc -w 2 127.0.0.1 8080")).exit_code == 0
    await asyncio.sleep(0.3)
    assert (await sandbox.exec("cat /tmp/received")).stdout == "hello\n"
    assert (await sandbox.exec("wget -qO- -T 3 http://1.1.1.1/")).exit_code != 0


def test_doctor_smoke_test_passes():
    result = asyncio.run(
        run_process(
            [sys.executable, "-m", "hpc_sandbox", "doctor", "--runtime", RUNTIME, "--image", IMAGE],
            timeout_s=1800,
            env={**os.environ, "PYTHONPATH": SRC},
        )
    )
    assert result.returncode == 0, result.detail()
