"""The fuse-overlay rootfs layout: a fuse-overlayfs of the node's template and a per-session upper layer, run by
an unprivileged instance, unmounted before its session is deleted (by its owner or a janitor)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from conftest import FakeRuntime, make_provider
from test_janitor import crashed_sandboxes

from hpc_sandbox import SandboxSpec, SandboxStartError
from hpc_sandbox import singularity as singularity_module
from hpc_sandbox.singularity import OVERLAY_RECORD, TEMPLATE_DIR

FAKE_FUSE = Path(__file__).with_name("fake_fuse.py")


@pytest.fixture
def fake_fuse(tmp_path: Path, fake_runtime: FakeRuntime, monkeypatch: pytest.MonkeyPatch) -> Path:
    bin_dir = tmp_path / "bin"
    for tool in ("fuse-overlayfs", "fusermount3"):
        wrapper = bin_dir / tool
        wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{FAKE_FUSE}" "$0" "$@"\n')
        wrapper.chmod(0o755)
    mounts = fake_runtime.state / "mounts"

    def mounted(path: Path) -> bool:
        return any(json.loads(p.read_text())["mountpoint"] == str(path) for p in mounts.glob("*"))

    monkeypatch.setattr(singularity_module, "_mounted", mounted)
    return fake_runtime.state


def fuse_calls(state: Path) -> list[list[str]]:
    path = state / "fuse-calls.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


async def test_a_fuse_overlay_sandbox_runs_unprivileged_on_its_own_upper_layer(
    tmp_path, sif, fake_runtime, fake_fuse
):
    provider = make_provider(tmp_path, rootfs_layout="fuse-overlay")
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    session = sandbox.session_dir
    assert session is not None
    (mount,) = [call for call in fuse_calls(fake_fuse) if call[0] == "fuse-overlayfs"]
    options = dict(item.split("=", 1) for item in mount[2].split(","))
    template = provider.scratch_root / TEMPLATE_DIR
    assert Path(options["lowerdir"]).parent.parent == template
    assert options["upperdir"] == str(session / "overlay" / "upper")
    assert mount[-1] == str(session / "rootfs")
    (start,) = fake_runtime.calls("instance", "start")
    assert "--userns" in start and "--writable" in start and start[-2] == str(session / "rootfs")
    assert (await sandbox.exec("cat hello.txt")).stdout == "hello\n"
    await sandbox.stop()
    unmounts = [call for call in fuse_calls(fake_fuse) if call[0] == "fusermount3"]
    assert unmounts == [["fusermount3", "-u", str(session / "rootfs")]]
    assert not session.exists()


async def test_a_failed_mount_fails_the_start_and_leaves_nothing(
    tmp_path, sif, fake_runtime, fake_fuse, monkeypatch
):
    provider = make_provider(tmp_path, rootfs_layout="fuse-overlay")
    monkeypatch.setenv("FAKE_FUSE_MOUNT_FAILS", "1")
    with pytest.raises(SandboxStartError, match="could not mount the overlay"):
        await provider.start(SandboxSpec(image=str(sif)))
    assert not any(provider.session_root.glob("hpcsb-*"))
    assert not fake_runtime.calls("instance", "start")


async def test_janitors_unmount_the_overlays_of_abandoned_sessions(
    tmp_path, sif, fake_runtime, fake_fuse, monkeypatch
):
    monkeypatch.setenv("HPC_SANDBOX_ROOTFS_LAYOUT", "fuse-overlay")
    (name,) = await crashed_sandboxes(tmp_path, sif, "a")
    session = make_provider(tmp_path).session_root / name
    assert json.loads((session / OVERLAY_RECORD).read_text())["mountpoint"] == str(
        session / "rootfs"
    )
    report = await make_provider(tmp_path).reap()
    assert (report.stopped, report.removed, report.errors) == ([name], [name], [])
    assert ["fusermount3", "-u", str(session / "rootfs")] in fuse_calls(fake_fuse)
    assert not session.exists()


async def test_snapshots_of_fuse_overlay_sandboxes_restore_as_copies(
    tmp_path, sif, fake_runtime, fake_fuse
):
    provider = make_provider(tmp_path, rootfs_layout="fuse-overlay")
    spec = SandboxSpec(image=str(sif))
    sandbox = await provider.start(spec)
    await sandbox.exec("echo v1 > state.txt")
    snapshot = await sandbox.snapshot()
    restored = await provider.restore(snapshot, spec)
    try:
        assert (await restored.exec("cat state.txt")).stdout == "v1\n"
        start = fake_runtime.calls("instance", "start")[-1]
        assert "--userns" not in start and start[-1] == restored.id
        assert not (restored.session_dir / OVERLAY_RECORD).exists()
    finally:
        await sandbox.stop()
        await restored.stop()
        await provider.delete_snapshot(snapshot)


def test_layouts_are_checked(tmp_path, fake_runtime, monkeypatch):
    with pytest.raises(ValueError, match="rootfs_layout must be one of"):
        make_provider(tmp_path, rootfs_layout="overlay")
    with pytest.raises(ValueError, match="need rootfs_layout='copy'"):
        make_provider(tmp_path, rootfs_layout="fuse-overlay", gpu="nv")
    with pytest.raises(ValueError, match="needs rootfs_templates"):
        make_provider(tmp_path, rootfs_layout="fuse-overlay", rootfs_templates=False)
    monkeypatch.setenv("HPC_SANDBOX_ROOTFS_LAYOUT", "fuse-overlay")
    assert make_provider(tmp_path).rootfs_layout == "fuse-overlay"
    assert make_provider(tmp_path, rootfs_layout="copy").rootfs_layout == "copy"
    # A site-wide default a provider cannot take falls back to copies rather than refusing the provider.
    assert make_provider(tmp_path, gpu="nv").rootfs_layout == "copy"
    assert make_provider(tmp_path, extra_start_args=["--fakeroot"]).rootfs_layout == "copy"
    assert make_provider(tmp_path, rootfs_templates=False).rootfs_layout == "copy"
