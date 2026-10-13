from __future__ import annotations

import json
import os
import signal
import sys
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from pathlib import Path

import pytest

from hpc_sandbox import SingularityProvider

FAKE = Path(__file__).with_name("fake_singularity.py")
MAGIC = b"FAKESIF\n"
_ISOLATED_ENV = (
    "PBS_JOBID",
    "PBS_LOCALDIR",
    "SLURM_JOB_ID",
    "HPC_SANDBOX_SCRATCH",
    "HPC_SANDBOX_IMAGE_CACHE",
    "SINGULARITY_TMPDIR",
    "APPTAINER_TMPDIR",
    "SINGULARITY_CACHEDIR",
    "APPTAINER_CACHEDIR",
    "FAKE_SINGULARITY_PULL_FAILURES",
    "FAKE_SINGULARITY_PULL_DELAY",
    "FAKE_SINGULARITY_INSTANCE_EXITS",
)


class FakeRuntime:
    """Handle on the fake ``singularity`` installed first on PATH."""

    def __init__(self, state: Path) -> None:
        self.state = state

    def calls(self, *prefix: str) -> list[list[str]]:
        path = self.state / "calls.jsonl"
        calls = (
            [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        )
        return [call for call in calls if call[: len(prefix)] == list(prefix)]

    def instance_pid(self, name: str) -> int:
        return json.loads((self.state / "instances" / f"{name}.json").read_text())["pid"]

    @staticmethod
    def make_sif(path: Path, files: dict[str, str] | None = None) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(MAGIC + json.dumps({"files": files or {}}).encode())
        return path

    def kill_instances(self) -> None:
        for record in (self.state / "instances").glob("*.json"):
            with suppress(ProcessLookupError, ValueError):
                os.kill(json.loads(record.read_text())["pid"], signal.SIGKILL)


@pytest.fixture
def fake_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeRuntime]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    wrapper = bin_dir / "singularity"
    wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{FAKE}" "$@"\n')
    wrapper.chmod(0o755)
    state = tmp_path / "fake-state"
    (state / "instances").mkdir(parents=True)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_SINGULARITY_STATE", str(state))
    for name in _ISOLATED_ENV:
        monkeypatch.delenv(name, raising=False)
    runtime = FakeRuntime(state)
    yield runtime
    runtime.kill_instances()


@pytest.fixture
def sif(tmp_path: Path, fake_runtime: FakeRuntime) -> Path:
    return fake_runtime.make_sif(tmp_path / "images" / "task.sif", {"app/hello.txt": "hello\n"})


def make_provider(tmp_path: Path, **options: object) -> SingularityProvider:
    settings: dict[str, object] = {
        "scratch_root": tmp_path / "scratch",
        "image_cache": tmp_path / "cache",
        "stage_images": False,
        "default_timeout_s": 30.0,
        "stop_grace_s": 1.0,
        "memory_poll_s": 0.1,
    }
    settings.update(options)
    return SingularityProvider(**settings)  # type: ignore[arg-type]


@pytest.fixture
async def provider(tmp_path: Path, fake_runtime: FakeRuntime) -> AsyncIterator[SingularityProvider]:
    provider = make_provider(tmp_path)
    yield provider
    for sandbox in provider.sandboxes:
        await sandbox.stop()
