"""The fuse-overlay rootfs layout: a fuse-overlayfs of the node's template and a per-session upper layer, run by
an unprivileged instance, unmounted before its session is deleted (by its owner or a janitor)."""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

import pytest
from conftest import FakeRuntime, make_provider, namespaces_unavailable
from test_janitor import crashed_sandboxes

from hpc_sandbox import PodSpec, SandboxSnapshot, SandboxSpec, SandboxStartError, ServiceSpec
from hpc_sandbox import singularity as singularity_module
from hpc_sandbox.singularity import OVERLAY_RECORD, OVERLAY_SNAPSHOT_FORMAT, TEMPLATE_DIR

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


async def test_a_fuse_overlay_snapshot_is_its_upper_layer_and_restores_over_the_template(
    tmp_path, sif, fake_runtime, fake_fuse
):
    provider = make_provider(tmp_path, rootfs_layout="fuse-overlay")
    spec = SandboxSpec(image=str(sif))
    sandbox = await provider.start(spec)
    session = sandbox.session_dir
    # What the sandbox wrote, where fuse-overlayfs keeps it (the fake mount does not write through).
    (session / "overlay" / "upper" / "state.txt").write_text("v1\n")
    (mount,) = [call for call in fuse_calls(fake_fuse) if call[0] == "fuse-overlayfs"]
    template = dict(item.split("=", 1) for item in mount[2].split(","))["lowerdir"]
    snapshot = await sandbox.snapshot()
    snapshot_dir = Path(snapshot.path)
    assert (
        snapshot.metadata["format"] == OVERLAY_SNAPSHOT_FORMAT
        and snapshot.metadata["lower"] == template
    )
    assert (snapshot_dir / "upper" / "state.txt").read_text() == "v1\n"
    assert not (snapshot_dir / "rootfs").exists()  # no copy of the image
    restored = await provider.restore(snapshot, spec)
    restored_session = restored.session_dir
    try:
        upper = restored_session / "overlay" / "upper"
        assert (upper / "state.txt").read_text() == "v1\n"  # a copy: the snapshot is left as it was
        remount = [call for call in fuse_calls(fake_fuse) if call[0] == "fuse-overlayfs"][-1]
        options = dict(item.split("=", 1) for item in remount[2].split(","))
        assert options["lowerdir"] == template and options["upperdir"] == str(upper)
        start = fake_runtime.calls("instance", "start")[-1]
        assert "--userns" in start and start[-1] == restored.id
        assert (
            await restored.exec(["cat", "/state.txt"])
        ).stdout == "v1\n"  # the fake maps argv paths
        # A snapshot of the restored sandbox lies over the same template.
        again = await restored.snapshot()
        assert again.metadata["lower"] == template
        await provider.delete_snapshot(again)
    finally:
        await sandbox.stop()
        await restored.stop()
        await provider.delete_snapshot(snapshot)
    assert not snapshot_dir.exists() and not restored_session.exists()


async def test_an_overlay_snapshot_restores_only_over_a_template_of_this_node(
    tmp_path, sif, fake_runtime, fake_fuse
):
    provider = make_provider(tmp_path, rootfs_layout="fuse-overlay")
    spec = SandboxSpec(image=str(sif))
    sandbox = await provider.start(spec)
    snapshot = await sandbox.snapshot()
    try:
        forged = [
            "/etc",
            str(sandbox.session_dir / "rootfs"),  # a sandbox's own root, not a template
            str(
                provider.scratch_root / TEMPLATE_DIR / "sha256-0000" / "rootfs"
            ),  # no such template
        ]
        for lower in forged:
            bad = SandboxSnapshot(
                **{**asdict(snapshot), "metadata": {**snapshot.metadata, "lower": lower}}
            )
            with pytest.raises(ValueError, match="not a template of this node"):
                await provider.restore(bad, spec)
        copies = make_provider(tmp_path, rootfs_layout="copy")
        with pytest.raises(ValueError, match="restore it with rootfs_layout='fuse-overlay'"):
            await copies.restore(snapshot, spec)
        assert len(fake_runtime.calls("instance", "start")) == 1
    finally:
        await sandbox.stop()
        await provider.delete_snapshot(snapshot)


async def test_pod_services_get_the_copy_layout(tmp_path, sif, fake_runtime, fake_fuse):
    if reason := namespaces_unavailable():
        pytest.skip(reason)
    provider = make_provider(tmp_path, rootfs_layout="fuse-overlay")
    pod = await provider.start_pod(PodSpec())
    try:
        service = await pod.start_service(ServiceSpec("main", SandboxSpec(image=str(sif))))
        assert (await service.exec("cat /app/hello.txt")).stdout == "hello\n"
        session = service.session_dir
        assert session is not None and not (session / OVERLAY_RECORD).exists()
        assert fuse_calls(fake_fuse) == []
        # Extracted, as the copy layout does unless templates are asked for.
        assert [call[-2] for call in fake_runtime.calls("build")] == [str(session / "rootfs")]
    finally:
        await pod.stop()

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
