"""Host-side file operations in trees a sandbox controls."""

from __future__ import annotations

import asyncio
import os
import resource
import stat
import sys
import threading
import time
from pathlib import Path
from shutil import SpecialFileError

import pytest

from hpc_sandbox import SandboxError, safefs
from hpc_sandbox.sandbox import (
    TRANSFER_DIR,
    _in_thread,
    _open_staging,
)

TIME_NS = 1_500_000_000_000_000_000


def make_deep_tree(parent: Path, depth: int) -> None:
    """``parent/d/d/...``, ``depth`` levels, built through descriptors (paths would get too long)."""
    current = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for _ in range(depth):
            os.mkdir("d", dir_fd=current)
            child = os.open("d", os.O_RDONLY | os.O_DIRECTORY, dir_fd=current)
            os.close(current)
            current = child
    finally:
        os.close(current)


def test_uploads_keep_modes_times_and_symlinks(tmp_path):
    source = tmp_path / "source"
    (source / "sub").mkdir(parents=True)
    (source / "small").write_text("x")
    (source / "sub" / "script").write_text("#!/bin/sh\n")
    (source / "sub" / "script").chmod(0o750)
    (source / "link").symlink_to("small")
    for path in (source / "small", source / "sub" / "script", source / "sub", source):
        os.utime(path, ns=(TIME_NS, TIME_NS))
    staging = tmp_path / "staging"
    staging.mkdir()
    with safefs.opened_dir(staging) as fd:
        safefs.copy_in(source, "payload", fd)
    copy = staging / "payload"
    for path in (copy / "small", copy / "sub" / "script", copy / "sub", copy):
        assert path.stat().st_mtime_ns == TIME_NS, path
    assert stat.S_IMODE((copy / "sub" / "script").stat().st_mode) == 0o750
    assert os.readlink(copy / "link") == "small"


def test_downloads_never_block_on_a_fifo_swapped_in(tmp_path):
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "file").write_text("data")
    checked = os.stat(tree / "file")  # what the copy saw before the swap
    (tree / "file").unlink()
    os.mkfifo(tree / "file")
    errors: list[Exception] = []

    def copy() -> None:
        with safefs.opened_dir(tree) as fd:
            try:
                safefs._copy_entry("file", fd, tmp_path / "out", checked, None, umask=0, top=True)
            except SandboxError as error:
                errors.append(error)

    thread = threading.Thread(target=copy, daemon=True)
    thread.start()
    thread.join(10)
    assert not thread.is_alive() and "special file" in str(errors[0])
    assert not (tmp_path / "out").exists()


def test_download_targets_are_treated_like_shutil_would(tmp_path):
    tree = tmp_path / "tree"
    (tree / "dir" / "sub").mkdir(parents=True)
    (tree / "dir" / "sub" / "f").write_text("new")
    (tree / "file").write_text("new")
    (tree / "link").symlink_to("file")
    outside = tmp_path / "outside"
    outside.mkdir()
    real_dir = tmp_path / "real-dir"
    real_dir.mkdir()
    (real_dir / "kept").write_text("kept")
    (real_dir / "sub").symlink_to(outside)  # below the target: replaced, not followed
    real_file = tmp_path / "real-file"
    real_file.write_text("old")
    (tmp_path / "dir-link").symlink_to(real_dir)
    (tmp_path / "file-link").symlink_to(real_file)
    with safefs.opened_dir(tree) as fd:
        safefs.copy_out("dir", fd, tmp_path / "dir-link")
        safefs.copy_out("file", fd, tmp_path / "file-link")
        with pytest.raises(FileExistsError):
            safefs.copy_out("dir", fd, real_file)
        with pytest.raises(SandboxError, match="symlink"):  # only a sandbox's race makes one
            safefs.copy_out("link", fd, real_file)
        with pytest.raises(IsADirectoryError):
            safefs.copy_out("file", fd, real_dir)
    assert (tmp_path / "dir-link").is_symlink() and (tmp_path / "file-link").is_symlink()
    assert (real_dir / "kept").exists() and (real_dir / "sub" / "f").read_text() == "new"
    assert not (real_dir / "sub").is_symlink() and not any(outside.iterdir())
    assert real_file.read_text() == "new"


def test_deep_trees_are_refused_or_removed_without_recursion(tmp_path):
    tree = tmp_path / "tree"
    tree.mkdir()
    make_deep_tree(tree, 3 * sys.getrecursionlimit())
    with safefs.opened_dir(tree) as fd:
        with pytest.raises(SandboxError, match="deeper than"):
            safefs.copy_out("d", fd, tmp_path / "copy")
        safefs.remove("d", fd)
    assert not any(tree.iterdir())


def test_removal_opens_up_what_the_sandbox_locked(tmp_path):
    tree = tmp_path / "tree"
    (tree / "a" / "b").mkdir(parents=True)
    (tree / "a" / "b" / "f").write_text("x")
    (tree / "a" / "b").chmod(0)
    (tree / "a").chmod(0o500)
    with safefs.opened_dir(tree) as fd:
        safefs.remove("a", fd)
    assert not any(tree.iterdir())


def test_staging_survives_modes_the_sandbox_restricted(tmp_path):
    for existing in (True, False):
        tmp = tmp_path / f"tmp-{existing}"
        tmp.mkdir()
        if existing:
            (tmp / TRANSFER_DIR).mkdir()
            (tmp / TRANSFER_DIR).chmod(0o500)
        tmp.chmod(0o1555)
        root, staging = _open_staging(tmp, "token")
        os.close(staging)
        os.close(root)
        assert (tmp / TRANSFER_DIR / "token").is_dir()


async def test_cancelled_callers_wait_for_their_worker_thread():
    started, release = threading.Event(), threading.Event()
    seen_stop: list[bool] = []
    abandoned: list[str] = []

    def work(stop: threading.Event) -> str:
        started.set()
        release.wait(10)
        seen_stop.append(stop.is_set())
        return "descriptors"

    task = asyncio.create_task(_in_thread(work, abandon=abandoned.append))
    await asyncio.to_thread(started.wait, 10)
    task.cancel()
    await asyncio.sleep(0.2)
    assert not task.done()  # still waiting for the thread
    task.cancel()  # a second cancellation does not cut the wait short either
    await asyncio.sleep(0.2)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen_stop == [True] and abandoned == ["descriptors"]


def test_removal_gives_up_at_its_deadline(tmp_path):
    tree = tmp_path / "tree"
    (tree / "a" / "b").mkdir(parents=True)
    with safefs.opened_dir(tmp_path) as fd:
        with pytest.raises(SandboxError, match="gave up"):
            safefs.remove("tree", fd, deadline=time.monotonic() - 1)
        safefs.remove("tree", fd, deadline=time.monotonic() + 60)
    assert not tree.exists()


def run_briefly(target, timeout: float = 10.0) -> list[BaseException]:
    """Run ``target`` in a daemon thread; fail rather than hang if it blocks."""
    errors: list[BaseException] = []

    def wrapper() -> None:
        try:
            target()
        except BaseException as error:  # noqa: BLE001 - returned to the test
            errors.append(error)

    thread = threading.Thread(target=wrapper, daemon=True)
    thread.start()
    thread.join(timeout)
    assert not thread.is_alive(), "blocked"
    return errors


def test_copies_stop_within_a_file(tmp_path, monkeypatch):
    monkeypatch.setattr(safefs, "_CHUNK", 64 * 1024)
    tree = tmp_path / "tree"
    tree.mkdir()
    with (tree / "big").open("wb") as big:
        big.truncate(8 * 1024**3)  # sparse
    copy, stop = tmp_path / "copy", threading.Event()

    def stop_once_copying() -> None:
        while not (copy.exists() and copy.stat().st_size):
            time.sleep(0.001)
        stop.set()

    stopper = threading.Thread(target=stop_once_copying, daemon=True)
    stopper.start()
    with safefs.opened_dir(tree) as fd:
        errors = run_briefly(lambda: safefs.copy_out("big", fd, copy, stop))
    assert isinstance(errors[0], safefs.Stopped)
    assert copy.stat().st_size < 1024**3


def test_downloads_refuse_a_fifo_target(tmp_path):
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "file").write_text("data")
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    (tmp_path / "link").symlink_to(fifo)
    with safefs.opened_dir(tree) as fd:
        for target in (fifo, tmp_path / "link"):
            errors = run_briefly(lambda target=target: safefs.copy_out("file", fd, target))
            assert isinstance(errors[0], SpecialFileError), target


def open_descriptors() -> int:
    return len(os.listdir("/proc/self/fd"))


def test_uploads_hold_one_descriptor_per_level(tmp_path):
    source = tmp_path / "source"
    for index in range(300):
        (source / f"d{index}" / "sub").mkdir(parents=True)
        (source / f"d{index}" / "sub" / "f").write_text("x")
    staging = tmp_path / "staging"
    staging.mkdir()
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (max(open_descriptors() + 64, 128), hard))
    try:
        with safefs.opened_dir(staging) as fd:
            safefs.copy_in(source, "payload", fd)
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
    assert (staging / "payload" / "d299" / "sub" / "f").read_text() == "x"


def test_failed_downloads_leak_no_descriptors(tmp_path):
    tree = tmp_path / "tree"
    (tree / "payload" / "sub").mkdir(parents=True)
    locked = tmp_path / "locked"
    (locked / "existing").mkdir(parents=True)
    locked.chmod(0o500)
    (locked / "existing").chmod(0o500)
    try:
        with safefs.opened_dir(tree) as fd:
            before = open_descriptors()
            for target in (locked / "new", locked / "existing"):
                with pytest.raises(PermissionError):
                    safefs.copy_out("payload", fd, target)
                assert open_descriptors() == before, target
    finally:
        (locked / "existing").chmod(0o700)
        locked.chmod(0o700)


def test_downloads_drop_privilege_bits_and_apply_the_umask(tmp_path):
    tree = tmp_path / "tree"
    (tree / "payload" / "shared").mkdir(parents=True)
    (tree / "payload" / "tool").write_text("#!/bin/sh\n")
    (tree / "payload" / "tool").chmod(0o6777)
    (tree / "payload" / "locked").write_text("x")
    (tree / "payload" / "locked").chmod(0o000)
    (tree / "payload" / "shared").chmod(0o3777)
    previous = os.umask(0o027)  # not the fallback current_umask() assumes
    try:
        assert safefs.current_umask() == 0o027
        with safefs.opened_dir(tree) as fd:
            safefs.copy_out("payload", fd, tmp_path / "copy")
    finally:
        os.umask(previous)
    modes = {p.name: stat.S_IMODE(p.stat().st_mode) for p in (tmp_path / "copy").iterdir()}
    assert modes == {"tool": 0o750, "locked": 0o400, "shared": 0o750}


def test_symlinks_stay_inside_by_their_own_target():
    for link, depth, problem in (
        ("file", 0, None),
        ("./sub/./file", 0, None),
        (".", 0, None),
        ("../file", 1, None),
        ("../../x", 2, None),
        ("a/b/c", 0, None),
        ("../../x", 1, "escapes with .."),  # above the root
        ("/etc/passwd", 5, "absolute"),
        ("sub/..", 3, "climbs after a name"),  # wherever sub leads
        ("../sub/../x", 3, "climbs after a name"),
    ):
        assert safefs.link_problem(link, depth) == problem, (link, depth)


def test_downloads_leave_out_symlinks_that_could_lead_outside(tmp_path):
    tree = tmp_path / "tree"
    payload = tree / "payload"
    (payload / "sub" / "deeper").mkdir(parents=True)
    (payload / "sub" / "file").write_text("inside")
    links = {
        "sub/inside": "file",
        "sub/deeper/up": "../file",
        "to-sub": "sub",
        "through-sub": "to-sub/file",  # inside through another link
        "to-root": ".",
        "chain": "absolute",  # on to a link that is left out: dangles
        "absolute": "/etc/passwd",
        "escape": "../outside",
        "deep-escape": "sub/../../outside",
        "root-parent": "to-root/..",
        "dangling": "missing/../../outside",
    }
    for name, target in links.items():
        (payload / name).symlink_to(target)
    with safefs.opened_dir(tree) as fd:
        reported = safefs.copy_out("payload", fd, tmp_path / "copy")
    kept = {"sub/inside", "sub/deeper/up", "to-sub", "through-sub", "to-root", "chain"}
    assert all(target == links[path] for path, target, _ in reported)
    left_out = [path for path, _, _ in reported]
    assert sorted(left_out) == sorted(set(links) - kept)
    assert {path: reason for path, _, reason in reported} == {
        "absolute": "absolute",
        "escape": "escapes with ..",
        "deep-escape": "climbs after a name",
        "root-parent": "climbs after a name",
        "dangling": "climbs after a name",
    }
    copy = tmp_path / "copy"
    assert all(os.readlink(copy / name) == links[name] for name in kept)
    assert not any(os.path.lexists(copy / name) for name in left_out)
    assert (copy / "through-sub").read_text() == "inside" and not (copy / "chain").exists()


def test_downloads_into_one_directory_cannot_combine_into_an_escape(tmp_path):
    secret = tmp_path / "secret.txt"  # next to the download directory
    secret.write_text("host")
    copy = tmp_path / "copy"
    for links in ({"a": "b/..", "c": "d/.."}, {"b": ".", "d": "sub"}, {"b": ".."}):
        tree = tmp_path / f"tree-{len(list(tmp_path.iterdir()))}"
        (tree / "payload" / "sub").mkdir(parents=True)
        for name, target in links.items():
            (tree / "payload" / name).symlink_to(target)
        with safefs.opened_dir(tree) as fd:
            safefs.copy_out("payload", fd, copy)
    top = os.path.realpath(copy)
    for path in copy.rglob("*"):
        resolved = os.path.realpath(path)
        assert os.path.commonpath([top, resolved]) == top, (path, resolved)
    assert not (copy / "a").exists() and not os.path.lexists(copy / "a" / "secret.txt")


def test_failed_downloads_never_leave_links_that_lead_outside(tmp_path):
    tree = tmp_path / "tree"
    (tree / "payload" / "z").mkdir(parents=True)
    (tree / "payload" / "a-absolute").symlink_to("/etc")
    os.mkfifo(tree / "payload" / "z" / "fifo")  # copied after the link, fails the copy
    with safefs.opened_dir(tree) as fd, pytest.raises(SandboxError, match="special file"):
        safefs.copy_out("payload", fd, tmp_path / "copy")
    assert not os.path.lexists(tmp_path / "copy" / "a-absolute")


def test_a_racing_download_cannot_slip_a_symlink_under_a_directory(tmp_path, monkeypatch):
    tree = tmp_path / "tree"
    (tree / "payload" / "a").mkdir(parents=True)
    (tree / "payload" / "a" / "x").symlink_to("../secret.txt")  # one level down: kept
    copy = tmp_path / "copy"
    real_mkdir = os.mkdir

    def racing(path, *args, **kwargs):
        if Path(path) == copy / "a":
            os.symlink(".", path)  # another download got there between the check and mkdir
        return real_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(os, "mkdir", racing)
    with safefs.opened_dir(tree) as fd, pytest.raises(SandboxError, match="changed while"):
        safefs.copy_out("payload", fd, copy)
    assert not os.path.lexists(copy / "x")
