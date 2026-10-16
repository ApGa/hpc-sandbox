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
    (pointer,) = path.parent.glob("uri-*.sif.json")
    record = json.loads(pointer.read_text())
    assert record["source"] == "docker://example/task:1"
    assert resolved[0].sha256 == resolved[1].sha256 == record["sha256"] == sha256(path)
    assert path.name == f"sha256-{sha256(path)}.sif"
    assert sorted(p.name for p in path.parent.iterdir() if p.suffix == ".sif") == [path.name]


async def test_digest_pinned_references_are_cached_by_digest(tmp_path, fake_runtime):
    images = store(tmp_path)
    resolved = await images.resolve(f"docker://example/task@sha256:{DIGEST}")
    pointer = json.loads((tmp_path / "cache" / f"oci-{DIGEST}.sif.json").read_text())
    assert resolved.path.name == f"sha256-{pointer['sha256']}.sif"
    await images.pull(f"docker://example/task@sha256:{DIGEST}", refresh=True)
    assert len(fake_runtime.calls("pull")) == 1  # a digest never names another image


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


def uri_key(uri: str) -> str:
    return "uri-" + hashlib.sha256(uri.encode()).hexdigest()[:40]


async def test_refresh_pulls_a_moved_tag_and_keeps_the_previous_image(
    tmp_path, fake_runtime, monkeypatch
):
    images = store(tmp_path)
    first = await images.resolve("docker://example/task:1")
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_REVISION", "2")  # the tag moved
    assert (await images.resolve("docker://example/task:1")).path == first.path
    refreshed = await images.pull("docker://example/task:1", refresh=True)
    assert refreshed.sha256 != first.sha256 and refreshed.path != first.path
    assert (await store(tmp_path).resolve("docker://example/task:1")).path == refreshed.path
    assert first.path.is_file() and len(fake_runtime.calls("pull")) == 2


async def test_a_burst_of_refreshes_pulls_at_most_twice(tmp_path, fake_runtime, monkeypatch):
    await store(tmp_path).resolve("docker://example/task:1")
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_DELAY", "0.5")
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_REVISION", "2")
    stores = [store(tmp_path) for _ in range(4)]  # as if four processes shared the cache
    refreshed = await asyncio.gather(
        *(s.pull("docker://example/task:1", refresh=True) for s in stores)
    )
    # Refreshes asked for while one pull runs share the next one.
    assert len({image.sha256 for image in refreshed}) == 1
    assert len(fake_runtime.calls("pull")) <= 3


@pytest.mark.parametrize("linkable", [True, False])
async def test_caches_of_earlier_versions_are_used_without_pulling(
    tmp_path, fake_runtime, monkeypatch, linkable
):
    uri = "docker://example/task:1"
    old = await store(tmp_path / "old").resolve(uri)
    cache = tmp_path / "cache"
    cache.mkdir()
    legacy = cache / f"{uri_key(uri)}.sif"  # how earlier versions cached an image
    legacy.write_bytes(old.path.read_bytes())
    legacy.chmod(0o444)
    if not linkable:  # another user's file under fs.protected_hardlinks

        def refuse(*_: object) -> None:
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr("hpc_sandbox.images.os.link", refuse)
    pulls = len(fake_runtime.calls("pull"))
    resolved = await store(tmp_path).resolve(uri)
    assert len(fake_runtime.calls("pull")) == pulls
    assert resolved.path == cache / f"sha256-{old.sha256}.sif" and resolved.sha256 == old.sha256
    assert legacy.is_file() and resolved.path.samefile(legacy) is linkable
    assert (await store(tmp_path).resolve(uri)).path == resolved.path


class OtherImages:
    """A second image format pulling the same URIs, as enroot does next to Singularity."""

    name = "other"
    suffix = ".img"
    tool = "other-tool"

    def __init__(self) -> None:
        self.pulls = 0

    def is_local(self, reference: str) -> bool:
        return reference.startswith("/")

    def check(self, reference: str) -> None:
        pass

    def pull_uri(self, reference: str) -> str:
        return pull_uri(reference)

    async def validate(self, path: Path) -> bool:
        return path.is_file() and path.read_bytes().startswith(b"OTHER")

    async def pull(self, uri: str, target: Path, *, timeout_s: float):
        from hpc_sandbox.process import ProcessResult

        self.pulls += 1
        target.write_bytes(b"OTHER " + uri.encode())
        return ProcessResult(0, b"", b"")


async def test_formats_sharing_a_cache_keep_their_own_images(tmp_path, fake_runtime):
    other = OtherImages()
    sif_store, other_store = store(tmp_path), ImageStore(other, tmp_path / "cache")
    for reference in ("docker://example/task:1", f"docker://example/task@sha256:{DIGEST}"):
        for _ in range(2):
            assert (await sif_store.resolve(reference)).path.suffix == ".sif"
            assert (await other_store.resolve(reference)).path.suffix == ".img"
    assert len(fake_runtime.calls("pull")) == 2 and other.pulls == 2


async def test_a_refresh_never_takes_a_pull_that_started_before_it(
    tmp_path, fake_runtime, monkeypatch
):
    await store(tmp_path).resolve("docker://example/task:1")
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_DELAY", "1.0")
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_REVISION", "2")
    earlier = asyncio.ensure_future(store(tmp_path).pull("docker://example/task:1", refresh=True))
    await asyncio.sleep(0.3)  # that pull is under way when the tag moves again
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_REVISION", "3")
    later = await store(tmp_path).pull("docker://example/task:1", refresh=True)
    assert (await earlier).sha256 != later.sha256 and len(fake_runtime.calls("pull")) == 3
    assert b"3" in later.path.read_bytes()
    assert (await store(tmp_path).resolve("docker://example/task:1")).path == later.path


async def test_files_of_earlier_versions_are_adopted_only_once(tmp_path, fake_runtime, monkeypatch):
    uri = "docker://example/task:1"
    old = await store(tmp_path / "old").resolve(uri)
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / f"{uri_key(uri)}.sif").write_bytes(old.path.read_bytes())
    assert (await store(tmp_path).resolve(uri)).sha256 == old.sha256  # adopted, no pull
    pulls = len(fake_runtime.calls("pull"))
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_REVISION", "2")
    refreshed = await store(tmp_path).pull(uri, refresh=True)  # when the old one began is unknown
    assert len(fake_runtime.calls("pull")) == pulls + 1 and refreshed.sha256 != old.sha256
    refreshed.path.unlink()  # cleaned up by hand
    again = await store(tmp_path).resolve(uri)
    assert len(fake_runtime.calls("pull")) == pulls + 2  # pulled, not the superseded file
    assert b"2" in again.path.read_bytes()
