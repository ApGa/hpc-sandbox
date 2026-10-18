"""Sandboxes of one image start from one extraction per node: a template each sandbox copies."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import make_provider
from hpc_sandbox import SandboxSpec, procfs
from hpc_sandbox.singularity import TEMPLATE_DIR, TEMPLATE_MARKER


async def test_sandboxes_of_one_image_share_one_extraction(tmp_path, sif, fake_runtime):
    provider = make_provider(tmp_path, rootfs_templates=True)
    first = await provider.start(SandboxSpec(image=str(sif)))
    others = await asyncio.gather(*(provider.start(SandboxSpec(image=str(sif))) for _ in range(3)))
    try:
        assert len(fake_runtime.calls("build")) == 1
        templates = [
            path for path in (provider.scratch_root / TEMPLATE_DIR).iterdir() if path.is_dir()
        ]
        assert len(templates) == 1 and (templates[0] / TEMPLATE_MARKER).is_file()
        # Each sandbox writes its own copy: nothing reaches the template or another sandbox.
        await first.exec("echo mine > hello.txt && echo new > new.txt")
        assert (await first.exec("cat hello.txt")).stdout == "mine\n"
        for other in others:
            assert (await other.exec("cat hello.txt")).stdout == "hello\n"
            assert (await other.exec("test -e new.txt")).exit_code != 0
        assert (templates[0] / "rootfs" / "app" / "hello.txt").read_text() == "hello\n"
        assert not (templates[0] / "rootfs" / "app" / "new.txt").exists()
        # The image's root-private build cache became owner-writable once, in the template, and
        # stays so.
        assert os.access(first.session_dir / "rootfs" / "private-cache", os.W_OK)
    finally:
        await asyncio.gather(*(sandbox.stop() for sandbox in [first, *others]))


async def test_the_copy_layout_extracts_every_sandbox_unless_asked(tmp_path, sif, fake_runtime):
    provider = make_provider(tmp_path)
    assert provider.rootfs_layout == "copy" and provider.rootfs_templates is False
    sandboxes = [await provider.start(SandboxSpec(image=str(sif))) for _ in range(2)]
    try:
        assert len(fake_runtime.calls("build")) == 2
        assert not (provider.scratch_root / TEMPLATE_DIR).exists()
    finally:
        await asyncio.gather(*(sandbox.stop() for sandbox in sandboxes))


async def test_a_failed_extraction_publishes_no_template(tmp_path, sif, fake_runtime, monkeypatch):
    provider = make_provider(tmp_path, rootfs_templates=True)
    original = provider._extract_into
    calls = 0

    async def failing(image, rootfs):
        nonlocal calls
        calls += 1
        if calls == 1:
            await original(image, rootfs)
            raise RuntimeError("simulated failure after extracting")
        await original(image, rootfs)

    monkeypatch.setattr(provider, "_extract_into", failing)
    with pytest.raises(Exception, match="simulated failure"):
        await provider.start(SandboxSpec(image=str(sif)))
    root = provider.scratch_root / TEMPLATE_DIR
    assert [path.name for path in root.iterdir() if path.is_dir()] == []
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    try:
        assert (await sandbox.exec("cat hello.txt")).stdout == "hello\n"
        assert len([path for path in root.iterdir() if path.is_dir()]) == 1
    finally:
        await sandbox.stop()


async def test_a_template_that_lost_its_marker_is_extracted_again(tmp_path, sif, fake_runtime):
    provider = make_provider(tmp_path, rootfs_templates=True)
    await (await provider.start(SandboxSpec(image=str(sif)))).stop()
    root = provider.scratch_root / TEMPLATE_DIR
    (template,) = [path for path in root.iterdir() if path.is_dir()]
    (
        template / TEMPLATE_MARKER
    ).unlink()  # as an age-based cleaner, or a removal cut short, leaves it
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    try:
        assert (await sandbox.exec("cat hello.txt")).stdout == "hello\n"
        assert len(fake_runtime.calls("build")) == 2
        assert [path.name for path in root.iterdir() if path.is_dir()] == [template.name]
        assert (template / TEMPLATE_MARKER).is_file()
    finally:
        await sandbox.stop()


def dead_pid() -> int:
    return int(subprocess.run(["sh", "-c", "echo $$"], capture_output=True, check=True).stdout)


def rewrite_in_place(path: Path, old: bytes, new: bytes) -> None:
    """Rewrite ``path`` keeping its inode, size and mtime, as ``cp -p`` or ``rsync -t`` over it
    may."""
    before = path.stat()
    with open(path, "r+b") as stream:
        content = stream.read()
        stream.seek(0)
        stream.write(content.replace(old, new))
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    after = path.stat()
    assert (after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )


async def test_janitors_delete_extractions_of_dead_processes_that_no_overlay_uses(
    tmp_path, fake_runtime
):
    provider = make_provider(tmp_path, rootfs_templates=True)
    root = provider.scratch_root / TEMPLATE_DIR
    gone = dead_pid()
    abandoned = root / f".sha256-{'a' * 64}.{gone}-1.0123abcd"
    used = root / f".sha256-{'b' * 64}.{gone}-1.4567cdef"
    mine = root / f".sha256-{'c' * 64}.{os.getpid()}-{procfs.start_time(os.getpid())}.89abcdef"
    for path in (abandoned, used, mine):
        (path / "rootfs" / "etc").mkdir(parents=True)
    # A fuse-overlayfs holding the directory open, as one keeps its lower layer.
    holder = (
        "import os, sys, time; os.open(sys.argv[1], os.O_RDONLY); print(1, flush=True); "
        "time.sleep(60)"
    )
    overlay = await asyncio.create_subprocess_exec(
        "bash",
        "-c",
        'exec -a fuse-overlayfs "$0" -c "$1" "$2"',
        sys.executable,
        holder,
        str(used / "rootfs"),
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        assert overlay.stdout is not None and await overlay.stdout.readline() == b"1\n"
        report = await provider.reap()
        assert report.errors == []
        assert (abandoned.exists(), used.exists(), mine.exists()) == (False, True, True)
    finally:
        overlay.kill()
        await overlay.wait()
    await provider.reap()
    assert not used.exists() and mine.exists()


async def test_a_template_of_an_image_rewritten_in_place_is_not_reused(tmp_path, sif, fake_runtime):
    provider = make_provider(tmp_path, rootfs_templates=True)
    await (await provider.start(SandboxSpec(image=str(sif)))).stop()
    rewrite_in_place(sif, b': "hello', b': "howdy')
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    try:
        assert (await sandbox.exec("cat hello.txt")).stdout == "howdy\n"
    finally:
        await sandbox.stop()


async def test_a_damaged_template_goes_once_no_overlay_uses_it(tmp_path, sif, fake_runtime):
    provider = make_provider(tmp_path, rootfs_templates=True)
    await (await provider.start(SandboxSpec(image=str(sif)))).stop()
    root = provider.scratch_root / TEMPLATE_DIR
    (template,) = [path for path in root.iterdir() if path.is_dir()]
    (template / TEMPLATE_MARKER).unlink()
    # A fuse-overlayfs still has it as its lower layer when it is moved aside.
    holder = (
        "import os, sys, time; os.open(sys.argv[1], os.O_RDONLY); print(1, flush=True); "
        "time.sleep(60)"
    )
    overlay = await asyncio.create_subprocess_exec(
        "bash",
        "-c",
        'exec -a fuse-overlayfs "$0" -c "$1" "$2"',
        sys.executable,
        holder,
        str(template / "rootfs"),
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        assert overlay.stdout is not None and await overlay.stdout.readline() == b"1\n"
        await (await provider.start(SandboxSpec(image=str(sif)))).stop()
        damaged = [path for path in root.iterdir() if path.name.endswith(".damaged")]
        assert len(damaged) == 1  # kept while in use
    finally:
        overlay.kill()
        await overlay.wait()
    report = await provider.reap()  # this process, which moved it aside, lives on
    assert report.errors == [] and not damaged[0].exists()
