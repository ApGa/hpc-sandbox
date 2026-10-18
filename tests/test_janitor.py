from __future__ import annotations

import asyncio
import fcntl
import json
import os
import sys
from pathlib import Path

from conftest import FakeRuntime, make_provider
from hpc_sandbox import SandboxSpec
from hpc_sandbox.locks import open_lock_file
from hpc_sandbox.process import run_process
from hpc_sandbox.procfs import owned_live_start_time
from hpc_sandbox.sessions import JANITOR_LOCK

SRC = str(Path(__file__).resolve().parents[1] / "src")
CRASH = """\
import asyncio, os, signal, sys
from hpc_sandbox import SandboxSpec, SingularityProvider

async def main():
    scratch, cache, image, *rollouts = sys.argv[1:]
    provider = SingularityProvider(scratch_root=scratch, image_cache=cache, stage_images=False)
    for rollout in rollouts:
        sandbox = await provider.start(SandboxSpec(image=image, labels={"rollout_id": rollout}))
        print(sandbox.id, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

asyncio.run(main())
"""


async def crashed_sandboxes(tmp_path: Path, sif: Path, *rollouts: str) -> list[str]:
    """Start sandboxes in a process that is then SIGKILLed, abandoning them."""
    result = await run_process(
        [
            sys.executable,
            "-c",
            CRASH,
            str(tmp_path / "scratch"),
            str(tmp_path / "cache"),
            str(sif),
            *rollouts,
        ],
        timeout_s=60,
        env={**os.environ, "PYTHONPATH": SRC},
    )
    assert result.returncode == 137, result.detail()
    return result.stdout.decode().split()


def assert_scoped(fake_runtime: FakeRuntime) -> None:
    """Instance queries and stops always name one exact instance."""
    for call in fake_runtime.calls("instance", "list"):
        assert call[-1].startswith("hpcsb-"), call
    for call in fake_runtime.calls("instance", "stop"):
        assert "--all" not in call and call[-1].startswith("hpcsb-"), call


async def test_janitor_recovers_sessions_of_crashed_owners(tmp_path, sif, fake_runtime):
    (name,) = await crashed_sandboxes(tmp_path, sif, "a")
    pid = fake_runtime.instance_pid(name)
    assert owned_live_start_time(pid) is not None
    provider = make_provider(tmp_path)
    report = await provider.reap()
    assert (report.stopped, report.removed, report.errors) == ([name], [name], [])
    assert owned_live_start_time(pid) is None
    assert not (provider.session_root / name).exists()
    assert_scoped(fake_runtime)


async def test_janitor_filters_by_labels(tmp_path, sif, fake_runtime):
    first, second = await crashed_sandboxes(tmp_path, sif, "a", "b")
    provider = make_provider(tmp_path)
    report = await provider.reap({"rollout_id": "a"})
    assert report.removed == [first]
    assert (provider.session_root / second).is_dir()
    assert (await provider.reap()).removed == [second]


async def test_a_new_provider_sweeps_abandoned_sessions_on_first_start(tmp_path, sif, fake_runtime):
    (name,) = await crashed_sandboxes(tmp_path, sif, "a")
    provider = make_provider(tmp_path)
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    try:
        assert not (provider.session_root / name).exists()
    finally:
        await sandbox.stop()


async def test_a_start_never_waits_for_the_janitor_lock_to_sweep(tmp_path, sif, fake_runtime):
    """While another process holds the janitor lock, a first start neither sweeps nor waits; a later
    start of the same provider sweeps."""
    (name,) = await crashed_sandboxes(tmp_path, sif, "a")
    provider = make_provider(tmp_path)
    held = open_lock_file(provider.session_root / JANITOR_LOCK)  # its own open file: another holder
    try:
        fcntl.flock(held, fcntl.LOCK_EX)
        # What start() does before anything else.
        await asyncio.wait_for(provider._prepare(), timeout=10)
        assert (provider.session_root / name).is_dir()
    finally:
        os.close(held)
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    try:
        assert not (provider.session_root / name).exists()
    finally:
        await sandbox.stop()


async def test_janitor_never_touches_live_sessions(tmp_path, sif, fake_runtime):
    owner = make_provider(tmp_path)
    sandbox = await owner.start(SandboxSpec(image=str(sif), labels={"rollout_id": "a"}))
    try:
        report = await make_provider(tmp_path).reap({"rollout_id": "a"})
        assert report.live == [sandbox.id] and not report.removed and not report.errors
        assert (await sandbox.exec("echo alive")).stdout == "alive\n"
    finally:
        await sandbox.stop()


async def test_janitor_refuses_foreign_directories_and_ignores_unknown_ones(tmp_path, fake_runtime):
    provider = make_provider(tmp_path)
    provider.session_root.mkdir(parents=True)
    shared = provider.session_root / ("hpcsb-" + "0" * 32)
    shared.mkdir(mode=0o755)
    shared.chmod(0o755)
    other = provider.session_root / "unrelated"
    other.mkdir()
    report = await provider.reap()
    assert len(report.errors) == 1 and "non-private" in report.errors[0]
    assert shared.is_dir() and other.is_dir()


async def test_janitor_fails_closed_when_identities_disagree(tmp_path, sif, fake_runtime):
    (name,) = await crashed_sandboxes(tmp_path, sif, "a")
    pid = fake_runtime.instance_pid(name)
    provider = make_provider(tmp_path)
    (provider.session_root / name / "instance.pid").write_text(f"{os.getpid()}\n")
    report = await provider.reap()
    assert not report.removed and "disagree" in report.errors[0]
    assert owned_live_start_time(pid) is not None
    assert (provider.session_root / name).is_dir()


async def test_janitor_leaves_sessions_of_other_providers_alone(tmp_path, sif, fake_runtime):
    (name,) = await crashed_sandboxes(tmp_path, sif, "a")
    owner = make_provider(tmp_path).session_root / name / "owner.json"
    record = json.loads(owner.read_text())
    record["provider"] = "enroot"
    owner.write_text(json.dumps(record))
    report = await make_provider(tmp_path).reap()
    assert not report.removed and not report.errors and not report.live
    assert owned_live_start_time(fake_runtime.instance_pid(name)) is not None
    assert owner.exists()
