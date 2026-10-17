"""Sandboxes of one image start from one extraction per node: a template each sandbox copies."""

from __future__ import annotations

import asyncio
import os

import pytest
from conftest import make_provider

from hpc_sandbox import SandboxSpec
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
        # The image's root-private build cache became owner-writable once, in the template, and stays so.
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
