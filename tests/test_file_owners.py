"""File owners manifests: reading them, applying them without following links, and the Singularity
sandboxes that take one (``fakeroot``), whose snapshots copy as root of their namespace. Pod
services that take one are in ``test_pods.py``."""

from __future__ import annotations

import dataclasses
import errno
import json
import logging
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import (
    hide_from_path,
    in_mount_namespace,
    make_provider,
    namespaces_unavailable,
)
from hpc_sandbox import SandboxError, SandboxSpec, SandboxStartError, anchor, pods, subids
from hpc_sandbox.pods import ANCHOR_SCRIPT
from hpc_sandbox.sandbox import RESERVED_TARGETS
from test_pods import subids_unavailable


def manifest(tmp_path: Path, content: str | bytes, name: str = "owners") -> str:
    path = tmp_path / name
    path.write_bytes(content.encode() if isinstance(content, str) else content)
    return str(path)


def test_a_file_owners_manifest_is_read_line_by_line(tmp_path):
    text = (
        b"# from an image's layers\n"
        b"\n"
        b"1000 1000 /srv/app/\n"
        b"0\t42\t/etc/shadow\n"
        b"  503 20 /opt/a b/c d \n"
        b"65534 65534 //srv/./x\n"
        b"7 7 /\n"
        b"1 1 /caf\xe9\n"
    )
    assert anchor.read_file_owners(manifest(tmp_path, text)) == [
        (1000, 1000, (b"srv", b"app")),
        (0, 42, (b"etc", b"shadow")),
        (503, 20, (b"opt", b"a b", b"c d ")),  # a path runs to the end of its line
        (65534, 65534, (b"srv", b"x")),
        (7, 7, ()),  # the root directory itself
        (1, 1, (b"caf\xe9",)),  # any bytes but a newline, a carriage return and NUL
    ]
    assert anchor.read_file_owners(manifest(tmp_path, "")) == []
    for bad in (
        "1000 /srv/app",
        "1000 1000 srv/app",
        "app 1000 /srv/app",
        "1 2 3 /a",
        "1 1 /a/../b",
        "4294967295 0 /a",
        "99999999999 0 /a",
        "1 1 /a\rb",
    ):
        with pytest.raises(ValueError, match="line 2: expected UID GID /PATH"):
            anchor.read_file_owners(manifest(tmp_path, f"# the first line\n{bad}\n"))


def test_a_manifest_with_crlf_line_ends_fails_at_its_first_entry(tmp_path):
    """Its paths would each end with a carriage return, and name nothing."""
    owners = manifest(tmp_path, b"# made on Windows\r\n\r\n0 42 /etc/shadow\r\n1000 1000 /srv\r\n")
    with pytest.raises(ValueError, match=r"line 3: .*'0 42 /etc/shadow\\r' \(CRLF line ends\?\)"):
        anchor.read_file_owners(owners)


@pytest.fixture
def chowns(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int, int]]:
    """What apply_file_owners would change, which this process cannot: each chown's path and
    ids."""
    calls: list[tuple[str, int, int]] = []

    def record(
        path: int | bytes, uid: int, gid: int, *, dir_fd: int | None = None, follow_symlinks=True
    ) -> None:
        if isinstance(path, int):
            calls.append((os.readlink(f"/proc/self/fd/{path}"), uid, gid))
            return
        assert dir_fd is not None and not follow_symlinks  # relative to a directory, never a link
        directory = os.readlink(f"/proc/self/fd/{dir_fd}")
        calls.append((os.path.join(directory, os.fsdecode(path)), uid, gid))

    monkeypatch.setattr(os, "chown", record)
    return calls


def open_descriptors() -> int:
    return len(os.listdir("/proc/self/fd"))


def test_file_owners_never_follow_a_link_and_apply_in_order(tmp_path, chowns):
    root, outside = tmp_path / "rootfs", tmp_path / "outside"
    for directory in (root / "srv" / "app", root / "etc", root / "a" / "b" / "c", outside):
        directory.mkdir(parents=True)
    (root / "etc" / "shadow").write_text("")
    (root / "srv" / "file").write_text("")
    (root / "a" / "b" / "c" / "deep").write_text("")
    (root / "link").symlink_to("srv")  # relative, to a directory inside
    (root / "escape").symlink_to(outside)  # absolute, to a directory outside
    (outside / "secret").write_text("")
    entries = anchor.read_file_owners(
        manifest(
            tmp_path,
            "1 1 /srv/app\n"
            "1000 1000 /srv/app\n"  # a later entry wins
            "0 42 /etc/shadow\n"
            "5 5 /link\n"  # the link itself
            "5 5 /link/app\n"  # below a link: names nothing
            "5 5 /escape/secret\n"
            "5 5 /srv/file/x\n"  # below a file
            "5 5 /missing\n"
            "6 6 /a/b/c/deep\n"
            "6 6 /a/b/c\n"
            "6 6 /a/b\n"
            "7 7 /\n",
        )
    )
    before = open_descriptors()
    skipped = anchor.apply_file_owners(entries, str(root))
    assert open_descriptors() == before
    real = os.path.realpath(root)
    assert chowns == [
        (f"{real}/srv/app", 1, 1),
        (f"{real}/srv/app", 1000, 1000),
        (f"{real}/etc/shadow", 0, 42),
        (f"{real}/link", 5, 5),
        (f"{real}/a/b/c/deep", 6, 6),
        (f"{real}/a/b/c", 6, 6),
        (f"{real}/a/b", 6, 6),
        (real, 7, 7),
    ]
    assert skipped == ["/link/app", "/escape/secret", "/srv/file/x", "/missing"]
    report = anchor.file_owners_report(skipped, len(entries))
    assert report.startswith(anchor.FILE_OWNERS_REPORT) and "skipped 4 of 12 entries" in report
    assert "(missing, below a link or a file, or on another filesystem)" in report
    assert report.endswith(": /link/app, /escape/secret, /srv/file/x, /missing")


def test_file_owners_leave_alone_what_every_sandbox_mounts_over(tmp_path, chowns):
    """A sandbox mounts its own /dev, /proc, /sys, /tmp and /var/tmp, so it never sees what its
    root filesystem has there, and a fakeroot sandbox's give-back, which sees the same mounts,
    could never give it back: such entries are skipped, and paths that only start alike apply."""
    root = tmp_path / "rootfs"
    for name in ("dev", "proc", "sys", "tmp/x", "var/tmp/y", "tmpx", "var/tmpx", "devices"):
        (root / name).mkdir(parents=True)
    entries = anchor.read_file_owners(
        manifest(
            tmp_path,
            "1 1 /tmp\n1 1 /tmp/x\n1 1 //var/./tmp/y\n1 1 /proc\n1 1 /sys/kernel\n1 1 /dev\n"
            "2 2 /tmpx\n2 2 /var/tmpx\n2 2 /var\n2 2 /devices\n",
        )
    )
    skipped = anchor.apply_file_owners(entries, str(root))
    real = os.path.realpath(root)
    assert chowns == [
        (f"{real}/tmpx", 2, 2),
        (f"{real}/var/tmpx", 2, 2),
        (f"{real}/var", 2, 2),
        (f"{real}/devices", 2, 2),
    ]
    assert skipped == ["/tmp", "/tmp/x", "/var/tmp/y", "/proc", "/sys/kernel", "/dev"]
    report = anchor.file_owners_report(skipped, len(entries))
    assert "(at or below /dev, /proc, /sys, /tmp or /var/tmp)" in report
    assert set(RESERVED_TARGETS) <= set(anchor.MOUNTED_OVER)  # the private tmp dirs


def test_file_owners_keep_the_set_id_bits_that_a_chown_clears(tmp_path):
    """The kernel clears a file's set-user-ID and set-group-ID bits at every chown, even one that
    keeps its owner, as here: they are set again, as Docker keeps them. A directory keeps its
    own."""
    root = tmp_path / "rootfs"
    root.mkdir()
    modes = {"setuid": 0o4755, "setgid": 0o2755, "both": 0o6755, "plain": 0o755, "dir": 0o2775}
    for name, mode in modes.items():
        path = root / name
        if name == "dir":
            path.mkdir()
        else:
            path.write_text("#!/bin/sh\n")
        path.chmod(mode)
    me = f"{os.getuid()} {os.getgid()}"
    entries = anchor.read_file_owners(manifest(tmp_path, "".join(f"{me} /{n}\n" for n in modes)))
    assert anchor.apply_file_owners(entries, str(root)) == []
    assert {name: stat.S_IMODE((root / name).lstat().st_mode) for name in modes} == modes


def test_file_owners_the_namespace_does_not_map_change_nothing(tmp_path, chowns, monkeypatch):
    (tmp_path / "rootfs" / "app").mkdir(parents=True)
    maps = {"/proc/self/uid_map": [(0, 1), (1, 65536)], "/proc/self/gid_map": [(0, 1)]}
    monkeypatch.setattr(anchor, "_mapped_ids", maps.__getitem__)
    entries = anchor.read_file_owners(manifest(tmp_path, "0 0 /app\n1000 0 /app\n0 1000 /app\n"))
    with pytest.raises(OSError, match=r"gives /app to gid 1000, .* \(its gids: 0-0\)") as raised:
        anchor.apply_file_owners(entries, str(tmp_path / "rootfs"))
    assert raised.value.errno == errno.EINVAL
    assert chowns == []  # not even the entries before it


# As root of new user and mount namespaces, which map only root: what lies on another filesystem
# names nothing, the mount point itself included.
ANOTHER_FILESYSTEM = """
import ctypes, json, os, sys
from hpc_sandbox import anchor
root = sys.argv[1]
for name in ("mnt", "data"):
    os.mkdir(os.path.join(root, name))
libc = ctypes.CDLL(None, use_errno=True)
assert libc.mount(b"none", os.path.join(root, "mnt").encode(), b"tmpfs", 0, None) == 0
os.mkdir(os.path.join(root, "mnt", "x"))
changed = []
os.chown = lambda path, *ids, **options: changed.append(os.fsdecode(path))
entries = [(0, 0, (b"mnt", b"x")), (0, 0, (b"mnt",)), (0, 0, (b"data",))]
print(json.dumps([anchor.apply_file_owners(entries, root), changed]))
"""


def test_file_owners_never_leave_the_root_filesystems_filesystem(tmp_path):
    root = tmp_path / "rootfs"
    root.mkdir()
    skipped, changed = json.loads(in_mount_namespace(ANOTHER_FILESYSTEM, str(root)))
    assert skipped == ["/mnt/x", "/mnt"] and changed == ["data"]


def launcher(manifest: str, root: Path) -> list[str]:
    """The launcher's file-owners form, as this process's child."""
    script = [sys.executable, "-I", "-S", str(ANCHOR_SCRIPT), str(os.getpid())]
    return [*script, "file-owners", manifest, str(root)]


def test_the_launchers_errors_name_what_failed_without_an_error_number(tmp_path):
    root = tmp_path / "rootfs"
    root.mkdir()
    name = "x" * 300  # longer than a file name may be
    owners = manifest(tmp_path, f"{os.getuid()} {os.getgid()} /{name}\n")
    result = subprocess.run(launcher(owners, root), capture_output=True, text=True, timeout=60)
    assert result.returncode == 1
    ids = f"{os.getuid()}:{os.getgid()}"
    assert result.stderr == f"hpc-sandbox: cannot give /{name} to {ids}: File name too long\n"
    absent = str(tmp_path / "absent")
    result = subprocess.run(launcher(absent, root), capture_output=True, text=True, timeout=60)
    assert result.stderr == f"hpc-sandbox: No such file or directory: {absent!r}\n"


async def test_file_owners_take_effect_as_root_of_a_namespace_that_maps_them(tmp_path):
    """The launcher's file-owners form, in this process's namespace of subordinate ids: the paths
    get the subordinate ids of their owners on the host, a set-group-ID file keeps its bit, and a
    link to a directory outside is changed itself, not followed."""
    if reason := await subids_unavailable():
        pytest.skip(reason)
    ids = subids.find()
    namespace = await pods.id_namespace(ids, 600)
    root, outside = tmp_path / "rootfs", tmp_path / "outside"
    for directory in (root, root / "srv", root / "srv" / "app", outside):
        directory.mkdir()
        directory.chmod(0o755)  # as an image's, whatever the umask: others may look in
    (root / "srv" / "app" / "file").write_text("x")
    (root / "srv" / "chage").write_text("#!/bin/sh\n")
    (root / "srv" / "chage").chmod(0o2755)
    (root / "escape").symlink_to(outside)
    owners = manifest(
        tmp_path,
        "1000 1000 /srv/app\n1000 1000 /srv/app/file\n0 42 /srv\n0 42 /srv/chage\n"
        "1000 1000 /escape\n1000 1000 /escape/x\n",
    )
    nsenter = make_provider(tmp_path).nsenter
    argv = [
        nsenter,
        f"--user=/proc/{os.getpid()}/fd/{namespace}",
        "--preserve-credentials",
        "--",
        *launcher(owners, root),
    ]
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=60, check=False)
        assert result.returncode == 0, result.stderr
        assert "skipped 1 of 6 entries" in result.stdout
        assert result.stdout.rstrip().endswith(": /escape/x")
        user = ids.uids.start + 999, ids.gids.start + 999
        for path in (root / "srv" / "app", root / "srv" / "app" / "file", root / "escape"):
            assert (path.lstat().st_uid, path.lstat().st_gid) == user
        for path in (root / "srv", root / "srv" / "chage"):
            assert path.lstat().st_uid == os.getuid()
            assert path.lstat().st_gid == ids.gids.start + 41
        assert stat.S_IMODE((root / "srv" / "chage").lstat().st_mode) == 0o2755
        assert outside.lstat().st_uid == os.getuid()  # the link was not followed
    finally:
        await pods.give_back(root, nsenter)  # or pytest could not delete tmp_path
    assert {path.lstat().st_uid for path in [root, *root.rglob("*")]} == {os.getuid()}


async def test_a_fakeroot_sandbox_applies_its_file_owners_in_its_instances_namespace(
    tmp_path, sif, fake_runtime, caplog
):
    """The fake's --fakeroot instances map root alone, as Singularity's do where the caller has
    no subordinate ids: entries of root apply, an owner beyond fails the start, which then leaves
    nothing, and the entries skipped are logged."""
    if reason := namespaces_unavailable():
        pytest.skip(reason)
    provider = make_provider(tmp_path, fakeroot=True)
    owners = manifest(tmp_path, "0 0 /app/hello.txt\n0 0 /app\n0 0 /missing/x\n0 0 /tmp/x\n")
    with caplog.at_level(logging.WARNING, logger="hpc_sandbox.singularity"):
        sandbox = await provider.start(SandboxSpec(image=str(sif), file_owners=owners))
    assert "skipped 2 of 4 entries" in caplog.text and ": /missing/x, /tmp/x" in caplog.text
    assert (await sandbox.exec(["cat", "/app/hello.txt"])).stdout == "hello\n"
    await sandbox.stop()
    beyond = manifest(tmp_path, "0 0 /app\n1 1 /app\n", "beyond")
    with pytest.raises(SandboxStartError, match=r"gives /app to uid 1, .* \(its uids: 0-0\)") as e:
        await provider.start(SandboxSpec(image=str(sif), file_owners=beyond))
    assert "[Errno" not in str(e.value)
    assert provider.sandboxes == frozenset()
    assert await provider.wait_for_deletions(30)
    assert [p for p in provider.session_root.iterdir() if p.name.startswith("hpcsb-")] == []
    assert list((fake_runtime.state / "instances").iterdir()) == []  # its instance was stopped


async def test_a_restore_applies_the_file_owners_of_its_spec(tmp_path, sif, fake_runtime):
    """A snapshot keeps no owners: its copies of the files are yours."""
    if reason := namespaces_unavailable():
        pytest.skip(reason)
    provider = make_provider(tmp_path, fakeroot=True)
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    snapshot = await sandbox.snapshot()
    await sandbox.stop()
    spec = SandboxSpec(image=str(sif), file_owners=manifest(tmp_path, "1 1 /app\n"))
    with pytest.raises(SandboxStartError, match=r"gives /app to uid 1"):
        await provider.restore(snapshot, spec)
    owners = manifest(tmp_path, "0 0 /app\n", "root")
    restored = await provider.restore(snapshot, dataclasses.replace(spec, file_owners=owners))
    await restored.stop()
    await provider.delete_snapshot(snapshot)


@pytest.mark.parametrize("fakeroot", [True, False])
async def test_a_fakeroot_sandboxs_snapshot_copies_as_root_of_its_namespace(
    tmp_path, sif, fake_runtime, fakeroot
):
    """A fakeroot sandbox's snapshot copies its files as root of its instance's user namespace,
    which reads what a mode keeps their owner out of, as root inside the sandbox does (the fake's
    namespace maps root alone, so here that owner is you), and gives each copy to you. A sandbox
    without fakeroot copies as you, and fails on such a file."""
    if reason := namespaces_unavailable():
        pytest.skip(reason)
    if not fakeroot and os.geteuid() == 0:
        pytest.skip("root reads every file")
    provider = make_provider(tmp_path, fakeroot=fakeroot)
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    session = sandbox.session_dir
    assert session is not None
    for closed in (session / "rootfs" / "app" / "closed", session / "tmp" / "closed"):
        closed.write_text("kept\n")
        closed.chmod(0)
    try:
        if not fakeroot:
            with pytest.raises(SandboxError, match="Permission denied"):
                await sandbox.snapshot()
            return
        snapshot = await sandbox.snapshot()
    finally:
        await sandbox.stop()
    for name in ("rootfs/app/closed", "tmp/closed"):
        copy = Path(snapshot.path) / name
        assert copy.lstat().st_uid == os.getuid() and stat.S_IMODE(copy.lstat().st_mode) == 0
        copy.chmod(0o600)
        assert copy.read_text() == "kept\n"
    await provider.delete_snapshot(snapshot)
    assert not Path(snapshot.path).exists()


async def test_sandboxes_that_map_one_id_refuse_file_owners_before_anything_starts(
    tmp_path, sif, fake_runtime, monkeypatch
):
    spec = SandboxSpec(image=str(sif), file_owners=manifest(tmp_path, "1000 1000 /app\n"))
    with pytest.raises(ValueError, match="file_owners need fakeroot=True"):
        await make_provider(tmp_path).start(spec)
    fakeroot = make_provider(tmp_path, fakeroot=True)
    for owners, match in (
        (str(tmp_path / "absent"), "cannot read the file owners manifest"),
        (manifest(tmp_path, "1000 /app\n", "bad"), "line 1: expected UID GID /PATH"),
    ):
        with pytest.raises(ValueError, match=match):
            await fakeroot.start(dataclasses.replace(spec, file_owners=owners))
    hide_from_path(monkeypatch, "nsenter")
    with pytest.raises(SandboxError, match="nsenter"):
        await make_provider(tmp_path, fakeroot=True).start(spec)
    assert fake_runtime.calls("instance", "start") == []
    sessions = tmp_path / "scratch" / "sessions"
    assert not sessions.exists() or [p for p in sessions.iterdir() if p.name != ".trash"] == []


async def test_enroot_sandboxes_refuse_file_owners(enroot_provider, sqsh, tmp_path):
    """enroot maps one id into a sandbox, your uid, as root or not."""
    spec = SandboxSpec(image=str(sqsh), file_owners=manifest(tmp_path, "1000 1000 /app\n"))
    with pytest.raises(ValueError, match="map one id, your uid"):
        await enroot_provider.start(spec)
    assert enroot_provider.sandboxes == frozenset()
