"""End-to-end checks of what is the gVisor provider's own, against a real ``runsc``, on a compute
node. What every provider does is in ``test_integration.py``, which runs gVisor with
``HPC_SANDBOX_TEST_RUNTIME=gvisor``.

Run on a compute node (never a login node), with runsc on ``PATH``, and Singularity CE or
Apptainer, or enroot, for images::

    HPC_SANDBOX_INTEGRATION=1 python -m pytest tests/test_gvisor_integration.py -v

Without ``HPC_SANDBOX_INTEGRATION=1``, or without runsc or the runtime of the images on ``PATH``,
every test skips. ``HPC_SANDBOX_TEST_GVISOR_IMAGES`` picks the images (``sif`` or ``enroot``; by
default what the provider finds). ``HPC_SANDBOX_TEST_IMAGE`` picks the main image (default
``docker://alpine:3.20``) and ``HPC_SANDBOX_TEST_APT_IMAGE`` the Debian-based one the tests of
apt and Unix sockets use (default ``docker://python:3.12-slim``).
``HPC_SANDBOX_TEST_GVISOR_PLATFORM`` picks gVisor's platform (default ``systrap``).
"""

from __future__ import annotations

import asyncio
import difflib
import json
import os
import shutil
import socket
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from hpc_sandbox import GvisorProvider, Mount, SandboxLostError, SandboxSpec
from hpc_sandbox.gvisor import SentryMemory, runsc_processes
from hpc_sandbox.paths import default_scratch_root
from hpc_sandbox.procfs import owned_live_start_time
from hpc_sandbox.runtime import RUNTIME_CANDIDATES
from hpc_sandbox.sessions import remove_tree

IMAGES: Any = os.environ.get("HPC_SANDBOX_TEST_GVISOR_IMAGES") or None
IMAGES_RUNTIMES = {
    "sif": "singularity or apptainer",
    "enroot": "enroot",
    "any": "singularity, apptainer or enroot",
}


def images_runtime_found(images: str | None) -> bool:
    """Whether the runtime of ``images`` (``sif``, ``enroot``, or None for either) is on PATH."""
    singularity = any(shutil.which(binary) for binary in RUNTIME_CANDIDATES)
    enroot = shutil.which("enroot") is not None
    return {"sif": singularity, "enroot": enroot}.get(images or "", singularity or enroot)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("HPC_SANDBOX_INTEGRATION") != "1",
        reason="set HPC_SANDBOX_INTEGRATION=1 on a compute node with runsc",
    ),
    pytest.mark.skipif(shutil.which("runsc") is None, reason="needs runsc on PATH"),
    pytest.mark.skipif(
        not images_runtime_found(IMAGES),
        reason=f"needs on PATH {IMAGES_RUNTIMES[IMAGES or 'any']}, for the images",
    ),
]
IMAGE = os.environ.get("HPC_SANDBOX_TEST_IMAGE", "docker://alpine:3.20")
APT_IMAGE = os.environ.get("HPC_SANDBOX_TEST_APT_IMAGE", "docker://python:3.12-slim")
PLATFORM = os.environ.get("HPC_SANDBOX_TEST_GVISOR_PLATFORM", "systrap")


def make_provider(scratch: Path, **options: Any) -> GvisorProvider:
    options.setdefault("images", IMAGES)
    return GvisorProvider(scratch_root=scratch, platform=PLATFORM, **options)


@pytest.fixture
def scratch() -> Iterator[Path]:
    path = default_scratch_root() / f"integration-gvisor-{uuid.uuid4().hex[:8]}"
    yield path
    try:
        remove_tree(path)
    except OSError:
        shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
async def provider(scratch: Path) -> AsyncIterator[GvisorProvider]:
    provider = make_provider(scratch)
    yield provider
    for sandbox in provider.sandboxes:
        await sandbox.stop()
    report = await provider.reap()
    assert not report.errors, report.errors
    assert await provider.wait_for_deletions(120)


async def test_root_in_the_sandbox_with_the_images_environment(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, env={"WORDS": "a b; $(c)"}))
    result = await sandbox.exec('id -u; printf "%s\\n" "$WORDS"; pwd; echo "$PATH"')
    assert result.exit_code == 0, result.stderr
    uid, words, cwd, path = result.stdout.splitlines()
    assert (uid, words, cwd) == ("0", "a b; $(c)", "/app") and "/usr/bin" in path
    assert (await sandbox.exec(["cat"], stdin=b"piped")).stdout == "piped"
    assert (await sandbox.exec("uname -r")).stdout.strip().endswith("gvisor")  # its own kernel
    marker = f"hpc-sandbox-{uuid.uuid4().hex}"
    assert (await sandbox.exec(f"touch /etc/{marker} /tmp/{marker} && echo ok")).stdout == "ok\n"
    assert not Path("/tmp", marker).exists()
    lower = Path(provider.scratch_root, "templates-gvisor")
    assert not list(lower.glob(f"*/rootfs/etc/{marker}"))  # the overlay keeps it


async def test_privileges_stay_in_gvisors_kernel(provider):
    """What workloads ask Docker's privileged mode for works as root here, rootless."""
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    script = """
set -e
mkdir -p /srv/t && mount -t tmpfs none /srv/t && echo x > /srv/t/f && umount /srv/t
unshare -m -u -i -n -p -f sh -c 'hostname inner && hostname'
mkdir -p /srv/jail && cp -a /bin /lib /srv/jail/ && chroot /srv/jail /bin/busybox true
echo data > /srv/owned && chown 1234:5678 /srv/owned && stat -c %u:%g /srv/owned
mknod /tmp/null c 1 3 && echo mknod
adduser -D bob && su bob -s /bin/sh -c 'id -u; echo mine > /tmp/bobs; mkdir /tmp/bobdir'
stat -c %U /tmp/bobs /tmp/bobdir
"""
    result = await sandbox.exec(script, timeout_s=120)
    assert result.exit_code == 0, (result.stdout, result.stderr)
    assert result.stdout.split() == ["inner", "1234:5678", "mknod", "1000", "bob", "bob"]


async def test_apt_switches_to_its_own_user(scratch):
    provider = make_provider(scratch)
    sandbox = await provider.start(SandboxSpec(image=APT_IMAGE))
    try:
        result = await sandbox.exec(
            "apt-get update -qq 2>&1 | grep -c 'unsandboxed as root' || true", timeout_s=600
        )
        assert result.stdout.strip() == "0", result  # apt dropped to _apt for its downloads
    finally:
        await sandbox.stop()
    assert await provider.wait_for_deletions(120)


async def test_timeouts_kill_every_process_group_of_the_command_soon(provider):
    """Killing runsc exec leaves the command running in the sandbox: a timeout kills its process
    groups there, many at once."""
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    groups = "for i in $(seq 100); do setsid sleep 300 & done; sleep 301"
    began = time.monotonic()
    result = await sandbox.exec(groups, timeout_s=3)
    elapsed = time.monotonic() - began
    assert result.timed_out and result.exit_code == 124
    assert elapsed < 3 + 3, elapsed  # 101 kills one after another took 2 s more
    await asyncio.sleep(1)
    processes = await sandbox.exec("ps -o args")
    assert "sleep 30" not in processes.stdout, processes.stdout
    assert (await sandbox.exec("echo alive")).stdout == "alive\n"


async def test_transfers_work_once_root_mounts_over_tmp(provider, tmp_path):
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    mounted = await sandbox.exec("mount -t tmpfs tmpfs /tmp && echo mounted")
    assert mounted.stdout == "mounted\n", mounted.stderr
    source = tmp_path / "in.txt"
    source.write_text("payload\n")
    await sandbox.upload(source, "/tmp/in.txt")
    await sandbox.download("/tmp/in.txt", tmp_path / "back.txt")
    assert (tmp_path / "back.txt").read_text() == "payload\n"


def listing(root: Path) -> list[tuple[str, int, int]]:
    """Every path below a template's root filesystem with its mode and size, but the provider's
    mount points, which starts add as binds need them."""
    points = root / ".hpc-sandbox" / "mounts"
    return sorted(
        (str(path.relative_to(root)), path.lstat().st_mode, path.lstat().st_size)
        for path in root.rglob("*")
        if points not in (path, path.parent)
    )


async def test_binds_leave_the_template_as_it_was(provider, tmp_path):
    """runsc's gofer creates a missing mount point in the root filesystem it serves before it makes
    that read-only: binds whose targets the image lacks go to the provider's mount points, and
    every later sandbox sees the image as it is."""
    first = await provider.start(SandboxSpec(image=IMAGE))
    bundle = json.loads((first.session_dir / "config.json").read_text())
    lower = Path(bundle["root"]["path"])
    before = listing(lower)
    data = tmp_path / "data"
    data.mkdir()
    (data / "in.txt").write_text("data\n")
    config = tmp_path / "config.yaml"
    config.write_text("key: value\n")
    mounts = [
        Mount(str(data), "/srv/new/data"),
        Mount(str(config), "/etc/newapp/config.yaml"),
        Mount(str(data), "/var/run/probe-mnt", read_only=False),  # /var/run is a link to /run
        Mount(str(data), "/media"),  # in the image
    ]
    sandbox = await provider.start(SandboxSpec(image=IMAGE, mounts=mounts))
    seen = await sandbox.exec(
        "cat /srv/new/data/in.txt /etc/newapp/config.yaml /run/probe-mnt/in.txt /media/in.txt"
    )
    assert seen.stdout == "data\nkey: value\ndata\ndata\n", seen.stderr
    for target in ("/srv/new/data", "/etc/newapp/config.yaml", "/media"):
        written = await sandbox.exec(f"echo x > {target}/x 2>&1 || echo x >> {target} 2>&1")
        assert written.exit_code != 0, target  # read-only, whatever the mount point
    remounted = await sandbox.exec(
        "mount -o remount,rw /srv/new/data; mount -o remount,bind,rw /srv/new/data; "
        "touch /srv/new/data/y"
    )
    assert remounted.exit_code != 0 and not (data / "y").exists()
    assert (await sandbox.exec("echo rw > /var/run/probe-mnt/rw")).exit_code == 0
    assert (data / "rw").read_text() == "rw\n"
    (data / "rw").unlink()
    assert listing(lower) == before
    plain = await provider.start(SandboxSpec(image=IMAGE))
    probe = "ls -d /srv/new /etc/newapp /run/probe-mnt 2>/dev/null; echo end"
    assert (await plain.exec(probe)).stdout == "end\n"


async def test_overlay_size_caps_the_root_filesystem_only(scratch, tmp_path):
    """``overlay_size_mb`` caps the overlay; the transfer directory and a read-write mount are the
    host's directories, outside it."""
    provider = make_provider(scratch, overlay_size_mb=64)
    data = tmp_path / "data"
    data.mkdir()
    mount = Mount(str(data), "/srv/data", read_only=False)
    sandbox = await provider.start(SandboxSpec(image=IMAGE, mounts=[mount]))
    try:
        full = await sandbox.exec("dd if=/dev/zero of=/big bs=1M count=100 2>&1")
        assert full.exit_code != 0 and "space" in full.stdout.lower(), full
        assert (await sandbox.exec("rm /big; echo freed > /small && cat /small")).stdout == (
            "freed\n"
        )
        for path in ("/srv/data/big", "/.hpc-sandbox/transfer/big"):
            wrote = await sandbox.exec(f"dd if=/dev/zero of={path} bs=1M count=100 2>&1")
            assert wrote.exit_code == 0, wrote
        assert (data / "big").stat().st_size == 100 << 20
        assert (await sandbox.exec("rm /.hpc-sandbox/transfer/big")).exit_code == 0
    finally:
        await sandbox.stop()
    assert await provider.wait_for_deletions(120)


# Every path of the root filesystem but the kernel's and the transfer directory, with its mode,
# owner, type and size, and the contents of the files the change writes.
FINGERPRINT = r"""
skip='( -path /proc -o -path /sys -o -path /dev -o -path /.hpc-sandbox -o -path /run )'
find / -xdev $skip -prune -o -print | sort | tr '\n' '\0' | xargs -0 stat -c '%n %a %u:%g %F %s'
cat /etc/hpc-probe /app/delta/f1 /tmp/note 2>&1
"""
CHANGE = r"""
set -e
mkdir -p /app/delta
echo one > /app/delta/f1 && chmod 640 /app/delta/f1 && chown 1234:5678 /app/delta/f1
ln -s /app/delta/f1 /app/link && mkfifo /app/fifo && echo note > /tmp/note
echo probe >> /etc/hpc-probe
set -- $(ls -d /etc/*/ /usr/*/ | head -3)
rm -rf "$1"
mv "$2" "${2%/}-moved"
rm -rf "$3" && mkdir "$3" && echo only > "$3/only"
"""


async def test_a_restore_shows_exactly_what_the_snapshot_held(provider):
    """Deletions, renames, replacements, links, a FIFO, modes and owners, in /tmp too."""
    spec = SandboxSpec(image=IMAGE)
    sandbox = await provider.start(spec)
    changed = await sandbox.exec(CHANGE)
    assert changed.exit_code == 0, changed.stderr
    before = await sandbox.exec(FINGERPRINT)
    assert before.exit_code == 0, before.stderr
    snapshot = await sandbox.snapshot()
    await sandbox.exec("echo later > /app/delta/f1")
    restored = await provider.restore(snapshot, spec)
    after = await restored.exec(FINGERPRINT)
    difference = "\n".join(
        difflib.unified_diff(before.stdout.splitlines(), after.stdout.splitlines(), lineterm="")
    )
    assert after.stdout == before.stdout, difference[:3000]
    fresh = await provider.start(spec)
    assert (await fresh.exec("cat /etc/hpc-probe; ls /app")).stdout == ""
    await provider.delete_snapshot(snapshot)


async def test_stop_leaves_no_process_and_no_files(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    session = sandbox.session_dir
    assert session is not None
    running = runsc_processes(session / "runsc")
    assert len(running) >= 3, running  # runsc, the gofer, the Sentry
    await sandbox.stop()
    assert all(owned_live_start_time(pid) != started for pid, started in running)
    assert await provider.wait_for_deletions(120)
    assert not session.exists()


# What a sandbox held when it stopped: 1.5 GB written to its root filesystem, or held in memory.
HEAVY = {
    "rootfs": "dd if=/dev/zero of=/big bs=1M count=1536 2>/dev/null && echo done",
    "memory": (
        "python3 -c 'import time\nb = bytearray(1536 << 20)\n"
        "for i in range(0, len(b), 4096): b[i] = 1\nprint(1, flush=True)\ntime.sleep(1000)'"
        " > /tmp/held 2>&1 &\nuntil [ -s /tmp/held ]; do sleep 0.1; done; echo done"
    ),
}


@pytest.mark.parametrize("held", sorted(HEAVY))
async def test_stop_waits_for_a_sentry_that_held_much(provider, held):
    """The kernel takes a while to tear down a Sentry that held much: stop() waits for it."""
    sandbox = await provider.start(SandboxSpec(image=APT_IMAGE if held == "memory" else IMAGE))
    sentry, started = sandbox._sentry_pid, sandbox._sentry_start_time
    result = await sandbox.exec(HEAVY[held], timeout_s=300)
    assert "done" in result.stdout, result
    await sandbox.stop()
    assert sentry is not None and owned_live_start_time(sentry) != started


async def test_host_sockets_with_host_uds(scratch, tmp_path):
    """A server on a host Unix socket, mounted into a sandbox without a network."""
    path = tmp_path / "gw.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(path))
    server.listen(1)

    def answer() -> None:
        connection, _ = server.accept()
        with connection:
            connection.sendall(b"from the host\n")

    threading.Thread(target=answer, daemon=True).start()
    provider = make_provider(scratch, host_uds="open")
    spec = SandboxSpec(
        image=APT_IMAGE, network="none", mounts=[Mount(str(tmp_path), "/gw", read_only=False)]
    )
    sandbox = await provider.start(spec)
    client = (
        "import socket; s = socket.socket(socket.AF_UNIX); s.connect('/gw/gw.sock'); "
        "print(s.recv(100).decode(), end='')"
    )
    try:
        result = await sandbox.exec(["python3", "-c", client], timeout_s=60)
        assert result.stdout == "from the host\n", result
    finally:
        await sandbox.stop()
        server.close()
    assert await provider.wait_for_deletions(120)


async def test_the_memory_limit_stops_the_whole_sandbox(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, memory_mb=256))
    with pytest.raises(SandboxLostError, match="memory limit exceeded"):
        await sandbox.exec("cat /dev/zero | tail -c 600000000", timeout_s=120)
    assert sandbox.memory_kills and sandbox.memory_kills[0].limit_bytes == 256 << 20


# A process that allocates 1.5 GB with its own instructions, then sleeps or spins: gVisor unmaps
# an idle process's memory from the host process that runs it.
ALLOCATE = {
    "idle": "import time\nb = b'x' * (1536 << 20)\nprint('allocated', flush=True)\ntime.sleep(60)",
    "busy": "import time\nb = b'x' * (1536 << 20)\nprint('allocated', flush=True)\n"
    "t = time.time()\nwhile time.time() - t < 60: pass",
}


@pytest.mark.parametrize("state", sorted(ALLOCATE))
async def test_the_memory_limit_counts_what_no_host_process_maps(provider, state):
    sandbox = await provider.start(SandboxSpec(image=APT_IMAGE, memory_mb=512))
    with pytest.raises(SandboxLostError, match="memory limit exceeded"):
        await sandbox.exec(["python3", "-c", ALLOCATE[state]], timeout_s=120)
    (kill,) = sandbox.memory_kills
    assert kill.total_bytes > 512 << 20


async def test_the_memory_limit_counts_each_page_once(provider):
    """300 MiB touched page by page in a loop, under a 512 MiB limit: the stub that runs the
    process and its threads map the same pages, which PSS summed over them counted twice."""
    sandbox = await provider.start(SandboxSpec(image=APT_IMAGE, memory_mb=512))
    touch = (
        "b = bytearray(300 << 20)\n"
        "for j in range(3):\n"
        "    for i in range(0, len(b), 4096): b[i] = j\n"
        "print('survived')"
    )
    result = await sandbox.exec(["python3", "-c", touch], timeout_s=120)
    assert result.stdout == "survived\n" and not sandbox.memory_kills, result
    sentry, started = sandbox._sentry_pid, sandbox._sentry_start_time
    assert sentry is not None and started is not None
    held = SentryMemory(sentry, started).read()
    assert held is not None and held < 512 << 20, held


async def test_the_memory_limit_leaves_out_the_hosts_files(provider, tmp_path):
    """A 512 MiB file of a mount, read and then mapped whole under a 256 MiB limit: gVisor reads
    and maps the host's files without keeping them in its memory file."""
    data = tmp_path / "data"
    data.mkdir()
    with (data / "big").open("wb") as file:
        for _ in range(512):
            file.write(b"x" * (1 << 20))
    spec = SandboxSpec(image=APT_IMAGE, memory_mb=256, mounts=[Mount(str(data), "/srv/data")])
    sandbox = await provider.start(spec)
    read = (
        "import mmap\n"
        "with open('/srv/data/big', 'rb') as f:\n"
        "    while f.read(1 << 20): pass\n"
        "    m = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)\n"
        "    print(sum(m[i] for i in range(0, len(m), 4096)))"
    )
    result = await sandbox.exec(["python3", "-c", read], timeout_s=120)
    assert result.stdout == f"{ord('x') * (512 << 8)}\n" and not sandbox.memory_kills, result
    sentry, started = sandbox._sentry_pid, sandbox._sentry_start_time
    assert sentry is not None and started is not None
    held = SentryMemory(sentry, started).read()
    assert held is not None and held < 128 << 20, held


async def test_cpus_limit_what_the_sandbox_sees(provider, scratch):
    if len(os.sched_getaffinity(0)) < 3:
        pytest.skip("needs 3 CPUs")
    other = make_provider(scratch / "other")  # as another process's would
    sandboxes = [
        await provider.start(SandboxSpec(image=IMAGE, cpus=1)),
        await other.start(SandboxSpec(image=IMAGE, cpus=1)),
    ]
    try:
        cpus = []
        for sandbox in sandboxes:
            assert (await sandbox.exec("nproc")).stdout == "1\n"
            assert sandbox._sentry_pid is not None
            cpus.append(os.sched_getaffinity(sandbox._sentry_pid))
        assert not cpus[0] & cpus[1], cpus
    finally:
        await sandboxes[1].stop()
    assert await other.wait_for_deletions(120)


async def test_concurrent_starts(provider):
    sandboxes = await asyncio.gather(*(provider.start(SandboxSpec(image=IMAGE)) for _ in range(8)))
    results = await asyncio.gather(*(sandbox.exec("echo $$") for sandbox in sandboxes))
    assert all(result.exit_code == 0 for result in results)
    await asyncio.gather(*(sandbox.stop() for sandbox in sandboxes))
