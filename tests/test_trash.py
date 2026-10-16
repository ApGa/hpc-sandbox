"""Stopped sessions leave the session root under the janitor lock and are deleted after it."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path

from conftest import make_provider

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


async def test_stopped_sessions_are_deleted_outside_the_janitor_lock(
    tmp_path, sif, fake_runtime, monkeypatch
):
    provider = make_provider(tmp_path)
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    seen: list[tuple[Path, bool]] = []
    original = sessions.remove_tree

    def recording(path: Path) -> None:
        seen.append((path, janitor_lock_held(provider.session_root)))
        original(path)

    monkeypatch.setattr(sessions, "remove_tree", recording)
    session = sandbox.session_dir
    await sandbox.stop()
    assert session is not None and not session.exists()
    assert seen and all(path.parent.name == TRASH_DIR for path, _ in seen)
    assert not any(held for _, held in seen)
    assert list((provider.session_root / TRASH_DIR).iterdir()) == []


async def test_janitors_finish_the_deletions_of_dead_processes_only(tmp_path, sif, fake_runtime):
    provider = make_provider(tmp_path)
    trash = provider.session_root / TRASH_DIR
    trash.mkdir(parents=True)
    dead = trash / f"hpcsb-{'a' * 32}.999999999-1.abcdef12"
    (dead / "rootfs" / "locked").mkdir(parents=True)
    (dead / "rootfs" / "locked").chmod(0o000)
    live_tag = f"{os.getpid()}-{sessions.procfs.start_time(os.getpid())}"
    live = trash / f"hpcsb-{'b' * 32}.{live_tag}.abcdef12"
    (live / "rootfs").mkdir(parents=True)
    report = await provider.reap()
    assert not report.errors
    assert not dead.exists()
    assert live.exists()
