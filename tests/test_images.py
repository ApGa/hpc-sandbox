from __future__ import annotations

import asyncio
import hashlib
import json
import stat
from pathlib import Path

import pytest

from hpc_sandbox import ContainerRuntime, ImageStore, SandboxError, SifImages
from hpc_sandbox.images import is_local, pull_uri

DIGEST = "ab" * 32


def store(tmp_path: Path, **options) -> ImageStore:
    options.setdefault("pull_retry_delay_s", 0.0)
    return ImageStore(SifImages(ContainerRuntime()), tmp_path / "cache", **options)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_reference_kinds_and_pull_uris():
    assert is_local("/images/task.sif") and is_local("./task.sif") and is_local("file:///x.sif")
    assert is_local(f"task.sif@sha256:{DIGEST}")
    assert not is_local("docker://ubuntu:22.04") and not is_local("ubuntu:22.04")
    assert pull_uri("ubuntu:22.04") == "docker://ubuntu:22.04"
    assert pull_uri("docker://ghcr.io#team/img:tag") == "docker://ghcr.io/team/img:tag"
    assert pull_uri("oras://registry/x@sha256:1") == "oras://registry/x@sha256:1"


async def test_local_sif_is_validated_once(tmp_path, sif, fake_runtime):
    images = store(tmp_path)
    first = await images.resolve(str(sif))
    second = await images.resolve(f"file://{sif}")
    assert first.path == second.path == sif.resolve()
    assert len(fake_runtime.calls("sif", "list")) == 1


async def test_local_sif_digest_pins_are_enforced(tmp_path, sif, fake_runtime):
    images = store(tmp_path)
    resolved = await images.resolve(f"{sif}@sha256:{sha256(sif)}")
    assert resolved.sha256 == sha256(sif)
    with pytest.raises(SandboxError, match="expected"):
        await images.resolve(f"{sif}@sha256:{DIGEST}")
    not_a_sif = tmp_path / "plain.sif"
    not_a_sif.write_text("text")
    with pytest.raises(SandboxError, match="not a valid SIF"):
        await images.resolve(str(not_a_sif))
    with pytest.raises(SandboxError, match="not a file"):
        await images.resolve(str(tmp_path / "missing.sif"))


async def test_concurrent_resolves_pull_once_into_a_read_only_cache(
    tmp_path, fake_runtime, monkeypatch
):
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_DELAY", "0.5")
    stores = [store(tmp_path), store(tmp_path)]  # as if two processes shared the cache
    resolved = await asyncio.gather(*(s.resolve("docker://example/task:1") for s in stores))
    assert len(fake_runtime.calls("pull")) == 1
    path = resolved[0].path
    assert resolved[1].path == path and path.parent == tmp_path / "cache"
    assert not stat.S_IMODE(path.stat().st_mode) & 0o222
    sidecar = json.loads(path.with_suffix(".json").read_text())
    assert sidecar["source"] == "docker://example/task:1"
    assert resolved[0].sha256 == resolved[1].sha256 == sidecar["sha256"] == sha256(path)
    assert sorted(p.name for p in path.parent.iterdir() if p.suffix == ".sif") == [path.name]


async def test_digest_pinned_references_are_cached_by_digest(tmp_path, fake_runtime):
    resolved = await store(tmp_path).resolve(f"docker://example/task@sha256:{DIGEST}")
    assert resolved.path.name == f"oci-{DIGEST}.sif"


async def test_pulls_are_retried(tmp_path, fake_runtime, monkeypatch):
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_FAILURES", "2")
    resolved = await store(tmp_path).resolve("docker://example/flaky:1")
    assert resolved.path.exists()
    assert len(fake_runtime.calls("pull")) == 3


async def test_failed_pulls_report_every_attempt_and_leave_no_partials(
    tmp_path, fake_runtime, monkeypatch
):
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_FAILURES", "5")
    with pytest.raises(SandboxError, match="attempt 2"):
        await store(tmp_path, pull_attempts=2).resolve("docker://example/broken:1")
    assert not list((tmp_path / "cache").glob("*.sif"))


async def test_staging_copies_images_to_scratch_under_their_digest(tmp_path, sif, fake_runtime):
    stage = tmp_path / "scratch" / "images"
    images = store(tmp_path, stage_dir=stage)
    resolved = await images.resolve(str(sif))
    assert resolved.path == stage / f"sha256-{sha256(sif)}.sif"
    assert resolved.path.read_bytes() == sif.read_bytes()
    assert not stat.S_IMODE(resolved.path.stat().st_mode) & 0o222
    staged_at = resolved.path.stat().st_mtime_ns
    again = await store(tmp_path, stage_dir=stage).resolve(str(sif))  # a fresh process
    assert again.path == resolved.path and again.path.stat().st_mtime_ns == staged_at
    pulled = await images.resolve("docker://example/task:2")
    assert pulled.path.parent == stage and pulled.sha256 is not None
