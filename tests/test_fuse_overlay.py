"""The fuse-overlay rootfs layout: a fuse-overlayfs of the node's template and a per-session upper
layer, run by an unprivileged instance, unmounted before its session is deleted (by its owner or a
janitor)."""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import shutil
import signal
import subprocess
import sys
from collections.abc import Callable, Iterator
from dataclasses import asdict
from pathlib import Path

import pytest

from conftest import (
    FakeRuntime,
    hide_from_path,
    in_mount_namespace,
    make_provider,
    namespaces_unavailable,
)
from hpc_sandbox import (
    PodSpec,
    SandboxSnapshot,
    SandboxSpec,
    SandboxStartError,
    ServiceSpec,
    procfs,
)
from hpc_sandbox import singularity as singularity_module
from hpc_sandbox.singularity import OVERLAY_RECORD, OVERLAY_SNAPSHOT_FORMAT
from hpc_sandbox.templates import TEMPLATE_DIR
from test_janitor import crashed_sandboxes

FAKE_FUSE = Path(__file__).with_name("fake_fuse.py")
SFTP_SERVERS = (
    "/usr/lib/openssh/sftp-server",
    "/usr/libexec/openssh/sftp-server",
    "/usr/lib/sftp-server",
)


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


@pytest.fixture
def sshfs(tmp_path: Path) -> Iterator[Callable[[Path], int]]:
    """Mount real FUSE file systems, which sshfs over a local sftp-server makes as fuse-overlayfs
    does."""
    binary = shutil.which("sshfs")
    server = next((path for path in SFTP_SERVERS if os.access(path, os.X_OK)), None)
    if binary is None or server is None or shutil.which("fusermount3") is None:
        pytest.skip("needs sshfs, an sftp-server and fusermount3")
    if not os.access("/dev/fuse", os.R_OK | os.W_OK):
        pytest.skip("needs /dev/fuse")
    wrapper = tmp_path / "sftp.sh"
    wrapper.write_text(f"#!/bin/sh\nexec {server}\n")
    wrapper.chmod(0o755)
    served = tmp_path / "served"
    served.mkdir()
    mounted: list[Path] = []

    def mount(mountpoint: Path) -> int:
        """Mount at ``mountpoint``; the PID of its daemon."""
        mountpoint.mkdir(parents=True)
        source = f"localhost:{served}"
        # fuse-overlayfs's own attribute timeouts: stat answers from cache for decades.
        timeouts = "attr_timeout=1000000000,entry_timeout=1000000000"
        command = [binary, "-o", f"ssh_command={wrapper}", "-o", timeouts, source, str(mountpoint)]
        subprocess.run(command, check=True, timeout=30)
        mounted.append(mountpoint)
        (daemon,) = [
            int(pid)
            for pid in os.listdir("/proc")
            if pid.isdigit() and procfs.argv(int(pid)) == command
        ]
        return daemon

    yield mount
    for mountpoint in mounted:
        subprocess.run(
            ["fusermount3", "-u", "-z", str(mountpoint)], capture_output=True, check=False
        )


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


async def test_janitors_unmount_overlays_whose_record_is_unreadable(
    tmp_path, sif, fake_runtime, fake_fuse, monkeypatch
):
    monkeypatch.setenv("HPC_SANDBOX_ROOTFS_LAYOUT", "fuse-overlay")
    (name,) = await crashed_sandboxes(tmp_path, sif, "a")
    session = make_provider(tmp_path).session_root / name
    (session / OVERLAY_RECORD).write_text("")  # as a crash or a full disk might have left it
    report = await make_provider(tmp_path).reap()
    assert (report.stopped, report.removed, report.errors) == ([name], [name], [])
    assert ["fusermount3", "-u", str(session / "rootfs")] in fuse_calls(fake_fuse)


def test_mounts_are_found_through_links_relative_paths_and_escapes(tmp_path):
    real = tmp_path / "a b\\c\td"
    (real / "rootfs").mkdir(parents=True)
    (tmp_path / "link").symlink_to(real)
    script = """
import subprocess, sys
from pathlib import Path
from hpc_sandbox.singularity import _mounted
paths = [Path(arg) / "rootfs" for arg in sys.argv[1:]]
print([_mounted(path) for path in paths])
subprocess.run(["mount", "-t", "tmpfs", "tmpfs", str(paths[0])], check=True)
print([_mounted(path) for path in paths])
"""
    output = in_mount_namespace(script, str(real), str(tmp_path / "link"), os.path.relpath(real))
    assert output.splitlines() == ["[False, False, False]", "[True, True, True]"]


async def test_janitors_unmount_dead_overlays_wherever_they_lie(tmp_path, sshfs, monkeypatch):
    monkeypatch.setattr(singularity_module, "OVERLAY_FSTYPE", "fuse.sshfs")
    monkeypatch.setattr(singularity_module, "OVERLAY_DAEMON", "sshfs")
    sessions = tmp_path / "a-killed-job" / "sessions"
    dead = sessions / f"hpcsb-{'a' * 32}" / "rootfs"
    trashed = sessions / ".trash" / f"hpcsb-{'b' * 32}.12-345.0123abcd" / "rootfs"
    live = sessions / f"hpcsb-{'c' * 32}" / "rootfs"
    leased = sessions / f"hpcsb-{'d' * 32}" / "rootfs"  # its owner unmounts it
    other = tmp_path / "not-a-session" / "rootfs"
    for session in (dead, trashed, live, leased):
        session.parent.mkdir(mode=0o700, parents=True)  # as sessions are
    daemons = {path: sshfs(path) for path in (dead, trashed, live, leased, other)}
    for path in daemons:
        os.stat(path)  # as every sandbox's start does: the root's attributes are cached
    lease = os.open(leased.parent / ".lease", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lease, fcntl.LOCK_EX)
    try:
        for path in (dead, trashed, leased, other):
            os.kill(daemons[path], signal.SIGKILL)
        for path in (dead, trashed, leased, other):
            while procfs.start_time(daemons[path]) is not None:
                await asyncio.sleep(0.05)
        assert await singularity_module.unmount_dead_overlays() == []
        mounted = {path.parent.name[:7]: singularity_module._mounted(path) for path in daemons}
        assert mounted == {
            "hpcsb-a": False,
            "hpcsb-b": False,
            "hpcsb-c": True,
            "hpcsb-d": True,
            "not-a-s": True,
        }
    finally:
        os.close(lease)


def test_a_served_overlay_is_live_wherever_it_shows_up(tmp_path, monkeypatch):
    # A mount listed again where a bind of its parent propagated it.
    served, other = (tmp_path / f"sessions/hpcsb-{c * 32}/rootfs" for c in "ab")
    for rootfs in (served, other):
        rootfs.parent.mkdir(mode=0o700, parents=True)
    kind = f"- fuse.fuse-overlayfs fuse-overlayfs rw,user_id={os.getuid()},group_id=0"
    mountinfo = "\n".join(
        [
            f"100 50 0:77 / {served} rw {kind}",
            f"101 60 0:77 / /bound{served} rw {kind}",
            f"102 50 0:78 / {other} rw {kind}",
        ]
    )
    monkeypatch.setattr(singularity_module, "_served_overlays", lambda: {str(served)})
    monkeypatch.setattr(singularity_module, "leased", lambda path: False)
    assert singularity_module._unserved_overlays(mountinfo) == [str(other)]
    other.parent.chmod(0o755)  # not a session of this uid's: none of its business
    assert singularity_module._unserved_overlays(mountinfo) == []


def test_mount_points_that_are_not_utf_8_break_nothing(tmp_path):
    script = """
import asyncio, os, subprocess, sys
from pathlib import Path
from hpc_sandbox import procfs, singularity
base = os.fsencode(sys.argv[1])
names = [b"caf\\xe9", "x\\xa0y\\u2028z".encode(), "x\\u2028-\\u2028".encode()]
mounted = []
for name in names:
    odd = os.path.join(base, name)
    os.mkdir(odd)
    subprocess.run([b"mount", b"-t", b"tmpfs", b"tmpfs", odd], check=True)
    mounted.append(singularity._mounted(Path(os.fsdecode(odd))))
print(
    all(mounted),
    asyncio.run(singularity.unmount_dead_overlays()),
    procfs.tmpfs_bytes(os.getpid()) is not None,
)
"""
    assert in_mount_namespace(script, str(tmp_path)).split() == ["True", "[]", "True"]


async def test_janitors_unmount_overlays_without_fuse_overlayfs_on_path(
    tmp_path, sif, fake_runtime, fake_fuse, monkeypatch
):
    monkeypatch.setenv("HPC_SANDBOX_ROOTFS_LAYOUT", "fuse-overlay")
    (name,) = await crashed_sandboxes(tmp_path, sif, "a")
    (tmp_path / "bin" / "fuse-overlayfs").unlink()  # the job had it from a module; not the janitor
    hide_from_path(monkeypatch, "fuse-overlayfs")  # nor the host's
    session = make_provider(tmp_path).session_root / name
    report = await make_provider(tmp_path).reap()
    assert (report.removed, report.errors) == ([name], [])
    assert ["fusermount3", "-u", str(session / "rootfs")] in fuse_calls(fake_fuse)


async def test_janitors_and_first_starts_look_for_dead_overlays(
    tmp_path, sif, fake_runtime, monkeypatch
):
    calls = []

    async def looking() -> list[str]:
        calls.append(len(calls))
        return ["an error"]

    monkeypatch.setattr(singularity_module, "unmount_dead_overlays", looking)
    provider = make_provider(tmp_path)
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    await sandbox.stop()
    report = await provider.reap()
    assert calls == [0, 1] and report.errors == ["an error"]


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
    # A site-wide default a provider cannot take falls back to copies rather than refusing the
    # provider.
    assert make_provider(tmp_path, gpu="nv").rootfs_layout == "copy"
    assert make_provider(tmp_path, extra_start_args=["--fakeroot"]).rootfs_layout == "copy"
    assert make_provider(tmp_path, rootfs_templates=False).rootfs_layout == "copy"
