"""Stopped sessions leave the session root under the janitor lock and are deleted after it, in the background."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path

from conftest import in_mount_namespace, make_provider

from hpc_sandbox import SandboxSpec, sessions
from hpc_sandbox.sessions import JANITOR_LOCK, TRASH_DIR


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


async def test_janitors_finish_the_deletions_of_dead_processes_only(
    tmp_path, sif, fake_runtime, monkeypatch
):
    provider = make_provider(tmp_path)
    trash = provider.session_root / TRASH_DIR
    trash.mkdir(parents=True)
    dead = trash / f"hpcsb-{'a' * 32}.999999999-1.abcdef12"
    (dead / "rootfs" / "locked").mkdir(parents=True)
    (dead / "rootfs" / "locked").chmod(0o000)
    live_tag = f"{os.getpid()}-{sessions.procfs.start_time(os.getpid())}"
    live = trash / f"hpcsb-{'b' * 32}.{live_tag}.abcdef12"
    (live / "rootfs").mkdir(parents=True)
    recent = trash / f"hpcsb-{'c' * 32}.999999999-1.abcdef12"
    (recent / "rootfs").mkdir(parents=True)
    report = await provider.reap()
    assert not report.errors
    assert dead.exists() and recent.exists()  # within the grace period: an rm may still be at work
    monkeypatch.setattr(sessions, "TRASH_GRACE_S", 0.0)
    report = await provider.reap()
    assert not report.errors
    assert not dead.exists() and not recent.exists()
    assert live.exists()


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
