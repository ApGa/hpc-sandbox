from __future__ import annotations

import functools
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from pathlib import Path

import pytest

from hpc_sandbox import EnrootProvider, SingularityProvider, subids
from hpc_sandbox.enroot import ANCHOR_SCRIPT
from hpc_sandbox.sandbox import _DELETIONS

FAKE = Path(__file__).with_name("fake_singularity.py")
FAKE_ENROOT = Path(__file__).with_name("fake_enroot.py")
MAGIC = b"FAKESIF\n"
SQUASHFS_MAGIC = b"hsqs"
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
    "FAKE_SINGULARITY_START_DELAY",
    "FAKE_SINGULARITY_EXIT_DELAY",
    "FAKE_SINGULARITY_FAKEROOT_FAILS",
    "FAKE_FUSE_MOUNT_FAILS",
    "HPC_SANDBOX_ROOTFS_LAYOUT",
    "HPC_SANDBOX_RUNTIME",
    "ENROOT_CACHE_PATH",
    "ENROOT_TEMP_PATH",
    "ENROOT_DATA_PATH",
    "ENROOT_RUNTIME_PATH",
    "ENROOT_CONFIG_PATH",
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

    def environments(self, *prefix: str) -> list[dict[str, str | None]]:
        """The recorded variables of each call whose argv starts with ``prefix``."""
        path = self.state / "environments.jsonl"
        records = [json.loads(line) for line in path.read_text().splitlines()]
        return [
            record["env"] for record in records if record["argv"][: len(prefix)] == list(prefix)
        ]

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


@pytest.fixture(autouse=True)
def _background_deletions_finish() -> Iterator[None]:
    """Stopped sandboxes are deleted in the background: let those deletions end within their
    test."""
    yield
    deadline = time.monotonic() + 60
    while _DELETIONS and time.monotonic() < deadline:
        _DELETIONS.difference_update(
            [path for path in list(_DELETIONS) if not os.path.lexists(path)]
        )
        time.sleep(0.05)


def in_mount_namespace(script: str, *args: str) -> str:
    """Run the Python ``script`` as root of new user and mount namespaces; its output."""
    if shutil.which("unshare") is None:
        pytest.skip("needs unshare")
    if reason := namespaces_unavailable():
        pytest.skip(reason)
    source = str(Path(__file__).parents[1] / "src")
    result = subprocess.run(
        ["unshare", "--user", "--map-root-user", "--mount", sys.executable, "-c", script, *args],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "PYTHONPATH": source},
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


@functools.cache
def namespaces_unavailable() -> str | None:
    """Why sandbox namespaces cannot be created on this host, or None if they can."""
    if shutil.which("nsenter") is None:
        return "nsenter is not installed"
    probe = subprocess.run(
        [sys.executable, "-I", "-S", str(ANCHOR_SCRIPT), str(os.getpid()), "none", "--", "true"],
        capture_output=True,
        timeout=60,
        check=False,
    )
    if probe.returncode != 0:
        return f"unprivileged user namespaces are unavailable: {probe.stderr.decode().strip()}"
    return None


# A stand-in for newuidmap and newgidmap (``newuidmap PID 0 ID 1 1 START COUNT``): it records its
# arguments, then writes the one extent that an unprivileged process may write for another, its own
# id as root, so a pod maps no subordinate ids but takes every step it takes to map them.
FAKE_IDMAP = """\
#!{python}
import json, os, sys
tool = os.path.basename(sys.argv[0])
pid, own = sys.argv[1], sys.argv[3]
with open({calls!r}, "a") as out:
    out.write(json.dumps([tool, *sys.argv[1:]]) + "\\n")
if os.path.exists({fails!r}):
    print(f"{{tool}}: write to {{tool[3:6]}}_map failed: Operation not permitted", file=sys.stderr)
    sys.exit(1)
if tool == "newgidmap":
    with open(f"/proc/{{pid}}/setgroups", "w") as stream:
        stream.write("deny")
with open(f"/proc/{{pid}}/{{tool[3:6]}}_map", "w") as stream:
    stream.write(f"0 {{own}} 1\\n")
"""


class FakeSubids:
    """Handle on the fake ``newuidmap`` and ``newgidmap`` and the subordinate id files."""

    START, COUNT = 200000, 65536

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.fails = directory / "fails"  # exists: the tools refuse

    def calls(self) -> list[list[str]]:
        path = self.directory / "calls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


@pytest.fixture
def fake_subids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeSubids:
    """Fake ``newuidmap`` and ``newgidmap`` first on PATH, and ``/etc/subuid`` and ``/etc/subgid``
    that give this user (by uid, after another user's line) ``FakeSubids.COUNT`` ids from
    ``FakeSubids.START``."""
    directory = tmp_path / "subids"
    directory.mkdir()
    fake = FakeSubids(directory)
    for tool in subids.TOOLS:
        script = directory / tool
        script.write_text(
            FAKE_IDMAP.format(
                python=sys.executable, calls=str(directory / "calls.jsonl"), fails=str(fake.fails)
            )
        )
        script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{directory}{os.pathsep}{os.environ['PATH']}")
    for name in ("SUBUID", "SUBGID"):
        listing = directory / name.lower()
        listing.write_text(f"someone-else:100000:65536\n{os.getuid()}:{fake.START}:{fake.COUNT}\n")
        monkeypatch.setattr(subids, name, listing)
    return fake


def hide_from_path(monkeypatch: pytest.MonkeyPatch, *names: str) -> None:
    """Have ``shutil.which``, which hpc-sandbox finds its tools with, find none of ``names``.

    A test of what happens without a tool must not depend on the host lacking it: hosts keep tools
    in directories that every PATH holds, as GitHub's Ubuntu runners keep slirp4netns in /usr/bin.
    """
    which = shutil.which

    def hiding(cmd: str, mode: int = os.F_OK | os.X_OK, path: str | None = None) -> str | None:
        return None if cmd in names else which(cmd, mode, path)

    monkeypatch.setattr(shutil, "which", hiding)


class FakeEnroot:
    """Handle on the fake ``enroot`` installed first on PATH."""

    def __init__(self, state: Path) -> None:
        self.state = state

    def starts(self) -> list[dict]:
        path = self.state / "starts.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def set(self, knob: str, value: str) -> None:
        (self.state / knob).write_text(value)

    def calls(self, *prefix: str) -> list[dict]:
        path = self.state / "calls.jsonl"
        calls = (
            [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        )
        return [call for call in calls if call["argv"][: len(prefix)] == list(prefix)]

    @staticmethod
    def make_sqsh(path: Path, files: dict[str, str] | None = None) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(SQUASHFS_MAGIC + json.dumps({"files": files or {}}).encode())
        return path


@pytest.fixture
def fake_enroot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeEnroot:
    if reason := namespaces_unavailable():
        pytest.skip(reason)
    bin_dir = tmp_path / "enroot-bin"
    bin_dir.mkdir()
    state = tmp_path / "fake-enroot-state"
    state.mkdir()
    wrapper = bin_dir / "enroot"
    wrapper.write_text(
        f'#!/bin/sh\nFAKE_ENROOT_STATE="{state}" exec "{sys.executable}" "{FAKE_ENROOT}" "$@"\n'
    )
    wrapper.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    for name in _ISOLATED_ENV:
        monkeypatch.delenv(name, raising=False)
    return FakeEnroot(state)


@pytest.fixture
def sqsh(tmp_path: Path, fake_enroot: FakeEnroot) -> Path:
    return fake_enroot.make_sqsh(tmp_path / "images" / "task.sqsh", {"app/hello.txt": "hello\n"})


def make_enroot_provider(tmp_path: Path, **options: object) -> EnrootProvider:
    settings: dict[str, object] = {
        "scratch_root": tmp_path / "scratch",
        "image_cache": tmp_path / "cache",
        "stage_images": False,
        "default_timeout_s": 30.0,
        "start_timeout_s": 60.0,
        "memory_poll_s": 0.1,
    }
    settings.update(options)
    return EnrootProvider(**settings)  # type: ignore[arg-type]


@pytest.fixture
async def enroot_provider(tmp_path: Path, fake_enroot: FakeEnroot) -> AsyncIterator[EnrootProvider]:
    provider = make_enroot_provider(tmp_path)
    yield provider
    for sandbox in provider.sandboxes:
        await sandbox.stop()
