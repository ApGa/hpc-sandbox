"""End-to-end checks against a real runtime on a compute node.

Run on a compute node (never a login node) with::

    HPC_SANDBOX_INTEGRATION=1 python -m pytest tests/test_integration.py -v

``HPC_SANDBOX_TEST_RUNTIME`` picks the runtime (``singularity``, ``enroot``,
or ``auto``: the first of singularity, apptainer and enroot on PATH).
``HPC_SANDBOX_TEST_IMAGE`` picks the image (default ``docker://alpine:3.20``;
any image with /bin/sh, sleep, tail and cp works, including a local .sif or
.sqsh). Scratch defaults to the job-local directory, as in real runs; with
Singularity, set ``HPC_SANDBOX_TEST_NETWORK_NONE=1`` where the site allows
network namespaces. On a GPU node, ``HPC_SANDBOX_TEST_GPUS`` (``all`` with
enroot, ``nv`` or ``rocm`` with Singularity) checks GPU passthrough with an
image that has ``nvidia-smi`` or ``rocm-smi`` available through the runtime.
"""

from __future__ import annotations

import asyncio
import difflib
import hashlib
import os
import shutil
import sys
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from hpc_sandbox import EnrootProvider, SandboxSpec, SingularityProvider
from hpc_sandbox.cli import resolve_runtime
from hpc_sandbox.paths import default_scratch_root
from hpc_sandbox.process import run_process
from hpc_sandbox.procfs import owned_live_start_time
from hpc_sandbox.sandbox import SessionProvider
from hpc_sandbox.sessions import remove_tree

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("HPC_SANDBOX_INTEGRATION") != "1",
        reason="set HPC_SANDBOX_INTEGRATION=1 on a compute node with a container runtime",
    ),
]
IMAGE = os.environ.get("HPC_SANDBOX_TEST_IMAGE", "docker://alpine:3.20")
RUNTIME = resolve_runtime(os.environ.get("HPC_SANDBOX_TEST_RUNTIME", "auto"))
NETWORK_NONE = RUNTIME == "enroot" or os.environ.get("HPC_SANDBOX_TEST_NETWORK_NONE") == "1"
SRC = str(Path(__file__).resolve().parents[1] / "src")
enroot_only = pytest.mark.skipif(RUNTIME != "enroot", reason="enroot provider behavior")


def make_provider(scratch: Path) -> SessionProvider[Any]:
    if RUNTIME == "enroot":
        return EnrootProvider(scratch_root=scratch)
    return SingularityProvider(scratch_root=scratch, allow_network_none=NETWORK_NONE)


@pytest.fixture
def scratch() -> Iterator[Path]:
    path = default_scratch_root() / f"integration-{uuid.uuid4().hex[:8]}"
    yield path
    try:
        remove_tree(path)
    except OSError:
        shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
async def provider(scratch: Path) -> AsyncIterator[SessionProvider[Any]]:
    provider = make_provider(scratch)
    yield provider
    for sandbox in provider.sandboxes:
        await sandbox.stop()
    report = await provider.reap()
    assert not report.errors, report.errors


async def test_lifecycle_keeps_the_uid_and_isolates_tmp(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, env={"WORDS": "a b; $(c)"}))
    session = sandbox.session_dir
    assert session is not None
    result = await sandbox.exec('id -u; printf "%s\\n" "$WORDS"; pwd')
    assert result.exit_code == 0, result.stderr
    uid = "0" if RUNTIME == "enroot" else str(os.getuid())  # enroot maps root to this uid
    assert result.stdout.splitlines() == [uid, "a b; $(c)", "/app"]
    marker = f"hpc-sandbox-{uuid.uuid4().hex}"
    assert (await sandbox.exec(f"touch /etc/{marker} /tmp/{marker} && echo ok")).stdout == "ok\n"
    assert (session / "tmp" / marker).exists() and not Path("/tmp", marker).exists()
    assert (session / "rootfs" / "etc" / marker).stat().st_uid == os.getuid()
    assert (await sandbox.exec(["cat"], stdin=b"piped")).stdout == "piped"
    await sandbox.stop()
    assert not session.exists()


async def test_timeouts_kill_the_command_inside_the_container(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    result = await sandbox.exec("sleep 300", timeout_s=2)
    assert result.timed_out and result.exit_code == 124
    await asyncio.sleep(1)
    processes = await sandbox.exec("ps -o args 2>/dev/null || ps")
    assert "sleep 300" not in processes.stdout


async def test_transfers_round_trip(provider, tmp_path):
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    tree = tmp_path / "tree"
    (tree / "nested").mkdir(parents=True)
    (tree / "nested" / "data.bin").write_bytes(bytes(range(256)) * 1000)
    await sandbox.upload(tree, "/app/tree")
    await sandbox.download("/app/tree", tmp_path / "back")
    assert (tmp_path / "back" / "nested" / "data.bin").read_bytes() == bytes(range(256)) * 1000


async def test_snapshot_and_restore(provider):
    spec = SandboxSpec(image=IMAGE)
    sandbox = await provider.start(spec)
    await sandbox.exec("echo v1 > /app/state && echo note > /tmp/note")
    snapshot = await sandbox.snapshot()
    await sandbox.exec("echo v2 > /app/state")
    restored = await provider.restore(snapshot, spec)
    assert (await restored.exec("cat /app/state /tmp/note")).stdout == "v1\nnote\n"
    await provider.delete_snapshot(snapshot)


# Every path of the root filesystem (but the kernel's and the private tmp dirs) with its mode and type, the size
# and link target of what is not a directory (a directory's size depends on how it was built), and the contents of
# the files the change below writes: busybox's find, stat and xargs suffice.
FINGERPRINT = r"""
skip='( -path /proc -o -path /sys -o -path /dev -o -path /tmp -o -path /var/tmp -o -path /run )'
find / -xdev $skip -prune -o -type d -print | sort | tr '\n' '\0' | xargs -0 stat -c '%n %a %F'
find / -xdev $skip -prune -o ! -type d -print | sort | tr '\n' '\0' | xargs -0 stat -c '%n %a %F %s %N'
cat /etc/hpc-probe /app/delta/f1 /app/delta/f2 2>&1
"""
CHANGE = r"""
set -e
mkdir -p /app/delta
echo one > /app/delta/f1 && echo two > /app/delta/f2 && chmod 640 /app/delta/f1
ln -s /app/delta/f1 /app/link && mkfifo /app/fifo
echo probe >> /etc/hpc-probe
set -- $(ls -d /etc/*/ /usr/*/ | head -3)
rm -rf "$1"                                   # an image directory, deleted
mv "$2" "${2%/}-moved"                        # one renamed
rm -rf "$3" && mkdir "$3" && echo only > "$3/only"   # one replaced by a new one
"""


async def test_a_restore_shows_what_the_sandbox_deleted_renamed_and_replaced(provider):
    """Whatever the layout keeps of a sandbox (a copy of its rootfs, or what it wrote over the image), a restore
    shows exactly its files: image directories deleted, renamed and replaced, links, a FIFO and modes."""
    spec = SandboxSpec(image=IMAGE)
    sandbox = await provider.start(spec)
    changed = await sandbox.exec(CHANGE)
    assert changed.exit_code == 0, changed.stderr
    before = await sandbox.exec(FINGERPRINT)
    assert before.exit_code == 0, before.stderr
    snapshot = await sandbox.snapshot()
    restored = await provider.restore(snapshot, spec)
    after = await restored.exec(FINGERPRINT)
    assert after.exit_code == 0, after.stderr
    difference = "\n".join(
        difflib.unified_diff(
            before.stdout.splitlines(), after.stdout.splitlines(), lineterm="", n=0
        )
    )
    assert after.stdout == before.stdout, difference[:3000]
    fresh = await provider.start(spec)  # and the image itself is untouched
    assert (await fresh.exec("cat /etc/hpc-probe; ls /app")).stdout == ""
    await provider.delete_snapshot(snapshot)


async def test_memory_watchdog_kills_the_hog_and_keeps_the_sandbox(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, memory_mb=128))
    # tail buffers its last 600 MB and never sees EOF; reading a pipe keeps GNU tail from
    # seeking /dev/zero. The hog is killed first, then cat by SIGPIPE.
    hog = "cat /dev/zero | tail -c 600000000"
    result = await sandbox.exec(hog, timeout_s=120)
    assert result.exit_code == 137, (result.stdout, result.stderr)
    assert "memory limit exceeded" in result.stderr
    assert (await sandbox.exec("echo alive")).stdout == "alive\n"


CRASH = """\
import asyncio, os, signal, sys
from hpc_sandbox import EnrootProvider, SandboxSpec, SingularityProvider

async def main():
    runtime, scratch, image = sys.argv[1:]
    providers = {"enroot": EnrootProvider, "singularity": SingularityProvider}
    provider = providers[runtime](scratch_root=scratch)
    sandbox = await provider.start(SandboxSpec(image=image, labels={'t': 'j'}))
    pid = sandbox._init_pid if runtime == "enroot" else sandbox._instance_pid
    print(pid, sandbox.id, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

asyncio.run(main())
"""


async def test_janitor_recovers_an_abandoned_sandbox(provider, scratch):
    result = await run_process(
        [sys.executable, "-c", CRASH, RUNTIME, str(scratch), IMAGE],
        timeout_s=600,
        env={**os.environ, "PYTHONPATH": SRC},
    )
    assert result.returncode == 137, result.detail()
    pid, name = result.stdout.decode().split()
    if RUNTIME == "enroot":
        # The container dies with its owner; only its files are left.
        for _ in range(100):
            if owned_live_start_time(int(pid)) is None:
                break
            await asyncio.sleep(0.05)
        assert owned_live_start_time(int(pid)) is None
    else:
        assert owned_live_start_time(int(pid)) is not None
    report = await provider.reap({"t": "j"})
    assert report.removed == [name] and not report.errors
    assert owned_live_start_time(int(pid)) is None


@pytest.mark.skipif(
    not NETWORK_NONE,
    reason="set HPC_SANDBOX_TEST_NETWORK_NONE=1 where users may create network namespaces",
)
async def test_network_none_has_only_loopback(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, network="none"))
    # /proc/net/dev lists exactly the namespace's interfaces. /sys/class/net also holds plain
    # files, e.g. bonding_masters wherever the bonding driver is loaded.
    result = await sandbox.exec("cat /proc/net/dev")
    assert result.exit_code == 0, result.stderr
    interfaces = [
        line.split(":", 1)[0].strip() for line in result.stdout.splitlines()[2:] if ":" in line
    ]
    assert interfaces == ["lo"]


@enroot_only
async def test_enroot_containers_are_kept_apart_from_the_host(provider, monkeypatch):
    monkeypatch.setenv("HPCS_CANARY", "1")
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    mounts = (await sandbox.exec("cut -d' ' -f5 /proc/self/mountinfo")).stdout.split()
    assert "/root" not in mounts  # the site's home-directory hook stays off
    assert "/dev/shm" in mounts and "/tmp" in mounts and "/etc/resolv.conf" in mounts
    assert not {"/etc/hostname", "/etc/machine-id", "/etc/rc.local"} & set(mounts)  # site config
    environment = (await sandbox.exec("env")).stdout
    for host_only in ("HPCS_CANARY=", "SLURM_", "PMIX_", "ENROOT_", "USER=", "TERM="):
        assert host_only not in environment
    assert (await sandbox.exec("test -L /dev/fd && id -un")).stdout == "root\n"
    background = (await sandbox.exec("sleep 300 >/dev/null 2>&1 & echo $!")).stdout.strip()
    assert (await sandbox.exec("kill -9 -1; echo survived")).stdout == "survived\n"
    assert (await sandbox.exec(f"kill -0 {background} 2>/dev/null || echo gone")).stdout == "gone\n"
    assert (await sandbox.exec("tr '\\0' ' ' < /proc/1/cmdline")).stdout.endswith(f"{sandbox.id} ")


@enroot_only
async def test_enroot_loopback_works_without_a_network(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, network="none"))
    if (await sandbox.exec("command -v nc && command -v wget")).exit_code != 0:
        pytest.skip("the image has no nc and wget")
    listen = "(nc -l -p 8080 -s 127.0.0.1 > /tmp/received &); sleep 0.5"
    assert (await sandbox.exec(listen)).exit_code == 0
    assert (await sandbox.exec("echo hello | nc -w 2 127.0.0.1 8080")).exit_code == 0
    await asyncio.sleep(0.3)
    assert (await sandbox.exec("cat /tmp/received")).stdout == "hello\n"
    assert (await sandbox.exec("wget -qO- -T 3 http://1.1.1.1/")).exit_code != 0


@pytest.mark.skipif(not os.environ.get("HPC_SANDBOX_TEST_GPUS"), reason="set HPC_SANDBOX_TEST_GPUS")
async def test_gpus_are_passed_through(scratch):
    gpus = os.environ["HPC_SANDBOX_TEST_GPUS"]
    if RUNTIME == "enroot":
        provider = EnrootProvider(scratch_root=scratch, gpus=gpus)
    else:
        provider = SingularityProvider(scratch_root=scratch, gpu=gpus)  # type: ignore[arg-type]
    assert "gpu" in provider.capabilities
    async with await provider.start(SandboxSpec(image=IMAGE)) as sandbox:
        result = await sandbox.exec("nvidia-smi -L 2>/dev/null || rocm-smi --showid")
    assert result.exit_code == 0 and "GPU" in result.stdout, (result.stdout, result.stderr)


def test_doctor_smoke_test_passes():
    result = asyncio.run(
        run_process(
            [sys.executable, "-m", "hpc_sandbox", "doctor", "--runtime", RUNTIME, "--image", IMAGE],
            timeout_s=1800,
            env={**os.environ, "PYTHONPATH": SRC},
        )
    )
    assert result.returncode == 0, result.detail()


@enroot_only
async def test_enroot_dev_is_minimal_and_concurrent_starts_work(provider):
    sandboxes = await asyncio.gather(*(provider.start(SandboxSpec(image=IMAGE)) for _ in range(4)))
    devices = set((await sandboxes[0].exec("ls /dev")).stdout.split())
    assert {"null", "zero", "urandom", "fd", "stdin", "pts", "ptmx", "shm"} <= devices
    assert not {"kmsg", "fuse", "mem"} & devices and not any(d.startswith("nvme") for d in devices)
    assert (await sandboxes[0].exec("test -c /dev/pts/ptmx && echo pty")).stdout == "pty\n"


@enroot_only
async def test_enroot_restores_ignore_what_the_sandbox_planted(provider, tmp_path):
    host_dir = tmp_path / "host"
    host_dir.mkdir()
    (host_dir / "key").write_text("HOST-KEY\n")
    secret = tmp_path / "secret.env"
    secret.write_text("API_TOKEN=host-token\n")
    spec = SandboxSpec(image=IMAGE)
    sandbox = await provider.start(spec)
    planted = await sandbox.exec(
        f"echo '{host_dir} /mnt/host none x-create=dir,bind,rw 0 -1' >> /etc/fstab"
        f" && ln -sf {secret} /etc/environment && echo planted"
    )
    assert planted.stdout == "planted\n", planted.stderr
    snapshot = await sandbox.snapshot()
    restored = await provider.restore(snapshot, spec)
    probe = await restored.exec("cat /mnt/host/key 2>/dev/null; touch /mnt/host/x 2>/dev/null; env")
    assert "HOST-KEY" not in probe.stdout and "API_TOKEN" not in probe.stdout
    assert sorted(path.name for path in host_dir.iterdir()) == ["key"]
    assert (await restored.exec("grep -c /mnt/host /etc/fstab")).stdout == "1\n"  # its own file
    await provider.delete_snapshot(snapshot)


async def test_transfers_ignore_symlinks_the_sandbox_plants(provider, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    planted = await sandbox.exec(
        f"rm -rf /tmp/.hpc-sandbox-transfer && ln -s {outside} /tmp/.hpc-sandbox-transfer"
    )
    assert planted.exit_code == 0, planted.stderr
    source = tmp_path / "in.txt"
    source.write_text("payload\n")
    await sandbox.upload(source, "/app/in.txt")
    await sandbox.download("/app/in.txt", tmp_path / "back.txt")
    assert (tmp_path / "back.txt").read_text() == "payload\n"
    assert not any(outside.iterdir())


@enroot_only
async def test_enroot_sandboxes_keep_the_hosts_time_zone(provider):
    host = Path("/etc/localtime")
    if not host.exists():
        pytest.skip("the host has no /etc/localtime")
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    digest = hashlib.sha256(host.read_bytes()).hexdigest()
    result = await sandbox.exec("sha256sum /etc/localtime && env -u TZ date +%Z")
    assert result.exit_code == 0, result.stderr
    checksum, zone = result.stdout.split("\n")[:2]
    host_zone = (await run_process(["env", "-u", "TZ", "date", "+%Z"], timeout_s=10)).stdout
    assert checksum.split()[0] == digest and zone == host_zone.decode().strip()
    # The image's own zoneinfo is untouched: UTC is still UTC.
    utc = await sandbox.exec(
        "for zone in Etc/UTC UTC; do"
        ' [ -f "/usr/share/zoneinfo/$zone" ] && TZ="$zone" date +%Z && exit; done; echo none'
    )
    assert utc.stdout.strip() in ("UTC", "none"), utc.stdout


@enroot_only
async def test_commands_survive_a_sandbox_mounting_over_its_tmp(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, env={"FROM_SPEC": "1"}))
    mounted = await sandbox.exec("mount -t tmpfs tmpfs /tmp && echo mounted")
    assert mounted.stdout == "mounted\n", mounted.stderr
    result = await sandbox.exec('echo "$FROM_SPEC"; cat', stdin=b"in\n")
    assert result.stdout == "1\nin\n", result.stderr


async def test_commands_without_stdin_read_dev_null(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE))
    result = await sandbox.exec("[ -c /proc/self/fd/0 ] && echo null; cat")
    assert result.stdout == "null\n", result.stderr


async def test_names_a_shell_reserves_do_not_stop_commands(provider):
    reserved = {"OPTIND": "x", "BASHPID": "1", "UID": "5", "LC_ALL": "no-such-locale"}
    sandbox = await provider.start(SandboxSpec(image=IMAGE, env={"FROM_SPEC": "1"}))
    result = await sandbox.exec('echo "$FROM_SPEC"', env=reserved)
    assert (result.exit_code, result.stdout) == (0, "1\n"), result.stderr


async def test_commands_get_the_images_path_and_none_of_the_hosts_variables(provider, monkeypatch):
    await provider.images.resolve(IMAGE)  # pulled with the real proxy settings
    monkeypatch.setenv("PATH", f"/host-only/bin:{os.environ['PATH']}")
    monkeypatch.setenv("HPCS_CANARY", "1")
    monkeypatch.setenv("https_proxy", "http://user:secret@proxy:3128")
    spec = SandboxSpec(image=IMAGE, env={"FROM_SPEC": "spec", "BOTH": "spec"})
    sandbox = await provider.start(spec)
    output = (await sandbox.exec("env", env={"BOTH": "call"})).stdout
    env = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
    assert (env.get("FROM_SPEC"), env.get("BOTH")) == ("spec", "call")
    for host_only in ("/host-only", "HPCS_CANARY", "secret"):
        assert host_only not in output, host_only
    if RUNTIME == "enroot":  # the init holds the image's environment: every command gets it
        init = (await sandbox.exec("tr '\\0' '\\n' < /proc/1/environ")).stdout
        image_env = dict(line.split("=", 1) for line in init.splitlines() if "=" in line)
        shell_state = {"PWD", "OLDPWD", "SHLVL", "_", "HOME"}
        assert image_env.keys() - shell_state, init
        for key, value in image_env.items():
            assert key in shell_state or env.get(key) == value, (key, value, env.get(key))


POOL_UNDER_A_HARNESS = """# The "harness": the largest single process. The "tool": a pool of smaller workers.
cat /dev/zero | tail -c 100000000 > /dev/null &
harness=$!
sleep 2
sh -c 'for i in 1 2 3 4; do cat /dev/zero | tail -c 60000000 > /dev/null & done; wait'
echo "pool exit $?"
sleep 1
for p in /proc/[0-9]*/cmdline; do tr '\\0' ' ' < "$p" 2>/dev/null; echo; done |
    grep -c "tail -c 100000000 $" | sed 's/^/harness tails: /'
kill "$harness"
"""


# A pool that is the command itself: it dies whole, with the command.
POOL_AS_THE_COMMAND = (
    "for i in 1 2 3 4; do cat /dev/zero | tail -c 40000000 > /dev/null & done; wait"
)


async def test_memory_watchdog_explains_a_command_killed_with_its_children(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, memory_mb=100))
    for _ in range(3):
        result = await sandbox.exec(POOL_AS_THE_COMMAND, timeout_s=120)
        assert result.exit_code == 137, (result.stdout, result.stderr)
        assert "memory limit exceeded" in result.stderr and "descendant" in result.stderr
    # The command outlives its children by an instant: its own status, and the note.
    result = await sandbox.exec(f"({POOL_AS_THE_COMMAND}); echo sub=$?", timeout_s=120)
    assert (result.exit_code, result.stdout) == (0, "sub=137\n"), result.stderr
    assert "memory limit exceeded" in result.stderr


# Forks a child holding 2 MB every 10 ms, without end.
FORKER = """while :; do
    (cat /dev/zero | tail -c 2000000 > /dev/null) &
    sleep 0.01
done"""


async def test_memory_watchdog_leaves_nothing_a_forking_command_started(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, memory_mb=100))
    for _ in range(3):
        result = await sandbox.exec(FORKER, timeout_s=60)
        assert result.exit_code == 137 and "memory limit exceeded" in result.stderr, result
    await asyncio.sleep(1)
    left = await sandbox.exec(SURVIVORS)
    assert left.stdout.strip() == "0", left


# The forker's children still running, however coreutils names itself (not this grep).
SURVIVORS = """for p in /proc/[0-9]*/cmdline; do tr '\\0' ' ' < "$p" 2>/dev/null; echo; done |
    grep -c "tail -c 2000000 $"
"""


async def test_memory_watchdog_spares_what_other_commands_leave_running(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, memory_mb=100))
    detach = "for i in $(seq 100); do (sleep 1000 &); sleep 0.02; done; echo made"
    maker = asyncio.ensure_future(sandbox.exec(detach, timeout_s=120))
    await asyncio.sleep(0.3)
    for _ in range(2):  # killed while the other command detaches its processes
        assert (await sandbox.exec(POOL_AS_THE_COMMAND, timeout_s=120)).exit_code == 137
    assert (await maker).stdout == "made\n"
    count = SURVIVORS.replace("tail -c 2000000 $", "sleep 1000 $")
    assert (await sandbox.exec(count)).stdout.strip() == "100"


async def test_memory_watchdog_kills_a_runaway_pool_and_spares_the_harness(provider):
    sandbox = await provider.start(SandboxSpec(image=IMAGE, memory_mb=200))
    result = await sandbox.exec(["/bin/sh", "-c", POOL_UNDER_A_HARNESS], timeout_s=120)
    assert result.exit_code == 0, (result.stdout, result.stderr)  # the command itself survived
    assert "pool exit 137" in result.stdout and "harness tails: 1" in result.stdout, result.stdout
    assert "memory limit exceeded" in result.stderr and "descendant" in result.stderr
    (kill,) = sandbox.memory_kills
    assert kill.processes >= 5 and kill.pss_bytes > kill.total_bytes // 2
