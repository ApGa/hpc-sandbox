"""Stopped sessions leave the session root under the janitor lock and are deleted after it, in the background."""

from __future__ import annotations

import asyncio
import fcntl
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest
from conftest import in_mount_namespace, make_provider

from hpc_sandbox import SandboxSpec, sessions
from hpc_sandbox import sandbox as sandbox_module
from hpc_sandbox.sandbox import _DELETIONS
from hpc_sandbox.sessions import JANITOR_LOCK, TRASH_DIR, remove_tree


def janitor_lock_held(root: Path) -> bool:
    descriptor = os.open(root / JANITOR_LOCK, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    else:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


async def test_a_stop_moves_the_session_out_and_deletes_it_in_the_background(
    tmp_path, sif, fake_runtime, monkeypatch
):
    provider = make_provider(tmp_path)
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    # Tasks leave directories they took the owner's permissions from; the deletion restores them first.
    result = await sandbox.exec("mkdir -p locked/inner && touch locked/inner/f && chmod 000 locked")
    assert result.exit_code == 0, result.stderr
    moved: list[tuple[Path, bool]] = []
    original = sessions.move_to_trash

    def recording(path: Path, trash: Path | None = None) -> Path | None:
        moved.append((path, janitor_lock_held(provider.session_root)))
        return original(path, trash)

    monkeypatch.setattr(sessions, "move_to_trash", recording)
    session = sandbox.session_dir
    await sandbox.stop()
    assert session is not None and not session.exists()
    assert moved == [(session, True)]
    await provider.wait_for_deletions()
    assert list((provider.session_root / TRASH_DIR).iterdir()) == []


async def test_a_session_root_with_spaces_and_glob_characters_is_deleted_too(
    tmp_path, sif, fake_runtime
):
    provider = make_provider(tmp_path, scratch_root=tmp_path / "node scratch [*]")
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    await sandbox.stop()
    assert await provider.wait_for_deletions(timeout_s=30)
    assert list((provider.session_root / TRASH_DIR).iterdir()) == []


async def test_janitors_take_over_the_trash_no_deletion_holds(
    tmp_path, sif, fake_runtime, monkeypatch
):
    provider = make_provider(tmp_path)
    trash = provider.session_root / TRASH_DIR
    trash.mkdir(parents=True)
    dead = trash / f"hpcsb-{'a' * 32}.999999999-1.abcdef12"  # its process died: at once
    (dead / "rootfs" / "locked").mkdir(parents=True)
    (dead / "rootfs" / "locked").chmod(0o000)
    held = trash / f"hpcsb-{'c' * 32}.999999999-1.abcdef12"  # a deletion still works on it
    (held / "rootfs").mkdir(parents=True)
    live_tag = f"{os.getpid()}-{sessions.procfs.start_time(os.getpid())}"
    live = trash / f"hpcsb-{'b' * 32}.{live_tag}.abcdef12"  # its process may be about to delete it
    (live / "rootfs").mkdir(parents=True)
    deletion = os.open(held, os.O_RDONLY | os.O_DIRECTORY)
    fcntl.flock(deletion, fcntl.LOCK_EX)
    try:
        report = await provider.reap()
        assert not report.errors
        assert (dead.exists(), held.exists(), live.exists()) == (False, True, True)
        monkeypatch.setattr(sessions, "TRASH_GRACE_S", 0.0)  # its process never started a deletion
        report = await provider.reap()
        assert not report.errors
        assert (held.exists(), live.exists()) == (True, False)
    finally:
        os.close(deletion)


async def test_a_deletion_holds_its_entry_while_it_waits_for_its_turn(
    tmp_path, sif, fake_runtime, monkeypatch
):
    if shutil.which("flock") is None:
        pytest.skip("needs flock")
    provider = make_provider(tmp_path)
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    turn = os.open(provider.session_root / ".deletions.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(turn, fcntl.LOCK_EX)  # another deletion of this session root runs
    try:
        await sandbox.stop()
        (entry,) = (provider.session_root / TRASH_DIR).glob("hpcsb-*")
        deadline = time.monotonic() + 10
        while not sessions._being_deleted(entry) and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        monkeypatch.setattr(sessions, "TRASH_GRACE_S", 0.0)
        report = await provider.reap()
        assert entry.exists() and not report.errors
    finally:
        os.close(turn)
    assert await provider.wait_for_deletions(30)
    assert list((provider.session_root / TRASH_DIR).iterdir()) == []


async def test_a_deletion_runs_even_where_its_log_cannot_be_created(
    tmp_path, sif, fake_runtime, monkeypatch
):
    # As on a full scratch, where the deletion is what would make room again.
    provider = make_provider(tmp_path)
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    monkeypatch.setattr(sessions.uuid, "uuid4", lambda: uuid.UUID(int=0))
    trash = provider.session_root / TRASH_DIR
    tag = f"{os.getpid()}-{sessions.procfs.start_time(os.getpid())}"
    entry = trash / f"{sandbox.id}.{tag}.00000000"
    (trash / f".{entry.name}.log").mkdir(parents=True)  # no file can be created there
    await sandbox.stop()
    deadline = time.monotonic() + 30
    while entry.exists() and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    assert not entry.exists()


async def test_stops_delete_their_own_sessions_while_deletions_fall_behind(
    tmp_path, sif, fake_runtime, monkeypatch
):
    monkeypatch.setattr(sandbox_module, "_DELETION_BACKLOG", 0)
    provider = make_provider(tmp_path)
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    await sandbox.stop()
    assert list((provider.session_root / TRASH_DIR).iterdir()) == []  # before the stop returned


def test_removals_never_go_through_a_mount_point(tmp_path):
    tree = tmp_path / "tree"
    (tree / "rootfs").mkdir(parents=True)
    script = """
import subprocess, sys
from pathlib import Path
from hpc_sandbox.sessions import remove_tree
from hpc_sandbox.types import SandboxError
tree = Path(sys.argv[1])
subprocess.run(["mount", "-t", "tmpfs", "tmpfs", str(tree / "rootfs")], check=True)
(tree / "rootfs" / "kept").write_text("kept")
try:
    remove_tree(tree)
except SandboxError as error:
    print("mount point" in str(error), (tree / "rootfs" / "kept").exists())
"""
    assert in_mount_namespace(script, str(tree)).split() == ["True", "True"]


def test_a_relative_scratch_root_is_made_absolute(tmp_path, fake_runtime, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert make_provider(tmp_path, scratch_root="scratch").scratch_root == tmp_path / "scratch"


async def test_no_worker_thread_is_awaited_under_the_janitor_lock(tmp_path, monkeypatch):
    # The executor may be busy with starts: the node-wide lock would be held all that time.
    root = tmp_path / "sessions"
    root.mkdir()
    held = []
    original = asyncio.to_thread

    async def recording(function, /, *args, **kwargs):
        held.append(janitor_lock_held(root))
        return await original(function, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", recording)
    session = await sessions.create_session(root, sessions.new_instance_name())
    assert await sessions.trash_session_awaiting(session) is not None
    assert held and not any(held)


def removed_during_an_rm(tree: Path) -> bool:
    for top in range(40):
        for sub in range(50):
            (tree / "rootfs" / f"d{top}" / f"s{sub}").mkdir(parents=True)
    rm = subprocess.Popen(["rm", "-rf", "--", str(tree / "rootfs")])
    time.sleep(0.01)
    try:
        remove_tree(tree)
    finally:
        rm.wait()
    return not tree.exists()


def test_a_removal_finishes_what_another_deletion_left(tmp_path):
    # A janitor can claim a pod whose services' entries a detached rm still deletes.
    assert all(removed_during_an_rm(tmp_path / f"tree{trial}") for trial in range(5))


async def test_finished_deletions_are_forgotten(tmp_path, sif, fake_runtime):
    provider = make_provider(tmp_path)
    first = await provider.start(SandboxSpec(image=str(sif)))
    second = await provider.start(SandboxSpec(image=str(sif)))
    await first.stop()
    (deleted,) = _DELETIONS
    while os.path.lexists(deleted):
        await asyncio.sleep(0.05)
    await second.stop()
    assert deleted not in _DELETIONS
