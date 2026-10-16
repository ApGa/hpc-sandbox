"""Each instance starts and stops under its own control lock, so instances start and stop in parallel."""

from __future__ import annotations

import asyncio
import time

from conftest import make_provider

from hpc_sandbox import SandboxSpec
from hpc_sandbox.singularity import CONTROL_LOCK


async def test_instances_start_and_stop_in_parallel(tmp_path, sif, fake_runtime, monkeypatch):
    provider = make_provider(tmp_path)
    await (await provider.start(SandboxSpec(image=str(sif)))).stop()  # the template is made
    monkeypatch.setenv("FAKE_SINGULARITY_START_DELAY", "1.0")
    began = time.monotonic()
    sandboxes = await asyncio.gather(
        *(provider.start(SandboxSpec(image=str(sif))) for _ in range(6))
    )
    elapsed = time.monotonic() - began
    try:
        assert elapsed < 4.0, f"6 starts of 1 s each took {elapsed:.1f}s: they ran one at a time"
        for sandbox in sandboxes:
            assert (sandbox.session_dir / CONTROL_LOCK).is_file()
        assert not (provider.session_root / CONTROL_LOCK).exists()
    finally:
        await asyncio.gather(*(sandbox.stop() for sandbox in sandboxes))


async def test_a_janitor_stops_an_abandoned_instance_under_its_own_lock(
    tmp_path, sif, fake_runtime
):
    from test_janitor import crashed_sandboxes

    (name,) = await crashed_sandboxes(tmp_path, sif, "r1")
    provider = make_provider(tmp_path)
    report = await provider.reap()
    assert report.stopped == [name] and report.removed == [name] and not report.errors
    assert not (provider.session_root / name).exists()
    assert not (provider.session_root / CONTROL_LOCK).exists()
