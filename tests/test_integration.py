"""End-to-end checks against a real Singularity CE / Apptainer on a compute node.

Run on a compute node (never a login node) with::

    HPC_SANDBOX_INTEGRATION=1 python -m pytest tests/test_integration.py -v

``HPC_SANDBOX_TEST_IMAGE`` picks the image (default ``docker://alpine:3.20``;
any image with /bin/sh, sleep, tail and cp works, including a local .sif).
Scratch defaults to the job-local directory, as in production; set
``HPC_SANDBOX_TEST_NETWORK_NONE=1`` where the site allows network namespaces.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest

from hpc_sandbox import SandboxSpec, SingularityProvider
from hpc_sandbox.paths import default_scratch_root
from hpc_sandbox.process import run_process
from hpc_sandbox.procfs import owned_live_start_time

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("HPC_SANDBOX_INTEGRATION") != "1",
        reason="set HPC_SANDBOX_INTEGRATION=1 on a compute node with singularity or apptainer",
    ),
]
IMAGE = os.environ.get("HPC_SANDBOX_TEST_IMAGE", "docker://alpine:3.20")
SRC = str(Path(__file__).resolve().parents[1] / "src")


@pytest.fixture
def scratch() -> Iterator[Path]:
    path = default_scratch_root() / f"integration-{uuid.uuid4().hex[:8]}"
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
async def provider(scratch: Path) -> AsyncIterator[SingularityProvider]:
    allow_none = os.environ.get("HPC_SANDBOX_TEST_NETWORK_NONE") == "1"
    provider = SingularityProvider(scratch_root=scratch, allow_network_none=allow_none)
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
    assert result.stdout.splitlines() == [str(os.getuid()), "a b; $(c)", "/app"]
    marker = f"hpc-sandbox-{uuid.uuid4().hex}"
    assert (await sandbox.exec(f"touch /etc/{marker} /tmp/{marker} && echo ok")).stdout == "ok\n"
    assert (session / "tmp" / marker).exists() and not Path("/tmp", marker).exists()
    assert (await sandbox.exec(["cat"], stdin=b"piped")).stdout == "piped"
    await sandbox.stop()
    assert not session.exists()


async def test_timeouts_kill_the_command_inside_the_instance(provider):
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


async def test_janitor_recovers_an_abandoned_instance(provider, scratch):
    script = (
        "import asyncio, os, signal, sys\n"
        "from hpc_sandbox import SandboxSpec, SingularityProvider\n"
        "async def main():\n"
        "    provider = SingularityProvider(scratch_root=sys.argv[1])\n"
        "    sandbox = await provider.start(SandboxSpec(image=sys.argv[2], labels={'t': 'j'}))\n"
        "    print(sandbox._instance_pid, sandbox.id, flush=True)\n"
        "    os.kill(os.getpid(), signal.SIGKILL)\n"
        "asyncio.run(main())\n"
    )
    result = await run_process(
        [sys.executable, "-c", script, str(scratch), IMAGE],
        timeout_s=600,
        env={**os.environ, "PYTHONPATH": SRC},
    )
    assert result.returncode == 137, result.detail()
    pid, name = result.stdout.decode().split()
    assert owned_live_start_time(int(pid)) is not None
    report = await provider.reap({"t": "j"})
    assert report.removed == [name] and not report.errors
    assert owned_live_start_time(int(pid)) is None


@pytest.mark.skipif(
    os.environ.get("HPC_SANDBOX_TEST_NETWORK_NONE") != "1",
    reason="set HPC_SANDBOX_TEST_NETWORK_NONE=1 where users may create network namespaces",
)
async def test_network_none_has_only_loopback(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, network="none"))
    # /proc/net/dev lists exactly the namespace's interfaces. /sys/class/net also holds plain
    # files, e.g. bonding_masters wherever the bonding driver is loaded (ABCI compute nodes).
    result = await sandbox.exec("cat /proc/net/dev")
    assert result.exit_code == 0, result.stderr
    interfaces = [line.split(":", 1)[0].strip() for line in result.stdout.splitlines()[2:] if ":" in line]
    assert interfaces == ["lo"]


def test_doctor_smoke_test_passes():
    result = asyncio.run(
        run_process(
            [sys.executable, "-m", "hpc_sandbox", "doctor", "--image", IMAGE],
            timeout_s=1800,
            env={**os.environ, "PYTHONPATH": SRC},
        )
    )
    assert result.returncode == 0, result.detail()
