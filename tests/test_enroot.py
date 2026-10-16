from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import signal
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import make_enroot_provider
from test_safefs import make_deep_tree

from hpc_sandbox import (
    EnrootProvider,
    Mount,
    Sandbox,
    SandboxError,
    SandboxLostError,
    SandboxProvider,
    SandboxSpec,
    SandboxStartError,
)
from hpc_sandbox.enroot import AUFS2OVLFS_WRAPPER, KEEPALIVE, import_uri
from hpc_sandbox.process import run_process
from hpc_sandbox.procfs import cmdline, namespace_pid, owned_live_start_time
from hpc_sandbox.sessions import Session, open_session

PYTHON = "/usr/bin/python3"
SRC = str(Path(__file__).resolve().parents[1] / "src")


def option_values(argv: list[str], flag: str) -> list[str]:
    return [argv[index + 1] for index, arg in enumerate(argv[:-1]) if arg == flag]


def interfaces(proc_net_dev: str) -> list[str]:
    return [line.split(":", 1)[0].strip() for line in proc_net_dev.splitlines()[2:] if ":" in line]


async def gone(pid: int, start_time: str | None = None) -> bool:
    for _ in range(100):
        current = owned_live_start_time(pid)
        if current is None or (start_time is not None and current != start_time):
            return True
        await asyncio.sleep(0.05)
    return False


async def test_provider_and_sandbox_satisfy_the_protocols(enroot_provider, sqsh):
    assert isinstance(enroot_provider, SandboxProvider)
    assert enroot_provider.name == "enroot"
    assert enroot_provider.capabilities == {"snapshot", "isolation", "network_none"}
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    assert isinstance(sandbox, Sandbox)


async def test_start_runs_enroot_with_a_controlled_environment(
    enroot_provider, sqsh, fake_enroot, monkeypatch
):
    monkeypatch.setenv("SLURM_JOB_ID", "123")
    monkeypatch.setenv("ENROOT_MOUNT_HOME", "")  # empty means "site default" to enroot
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh), labels={"rollout_id": "r1"}))
    session = sandbox.session_dir
    assert session is not None and session.parent == enroot_provider.session_root
    assert stat.S_IMODE(session.stat().st_mode) == 0o700
    (create,) = fake_enroot.calls("create")
    assert create["argv"] == ["create", "--name", "rootfs", str(sqsh)]
    (start,) = fake_enroot.calls("start")
    argv, env = start["argv"], start["env"]
    assert "--rw" in argv and "--root" in argv
    assert option_values(argv, "--rc") == [str(session / "rc")]
    mounts = option_values(argv, "--mount")
    assert f"{session}/tmp:/tmp:none:x-create=dir,bind,rw,nosuid" in mounts
    assert f"{session}/var-tmp:/var/tmp:none:x-create=dir,bind,rw,nosuid" in mounts
    assert any(mount.startswith("tmpfs:/dev/shm:tmpfs:") for mount in mounts)
    assert argv[-5:] == ["rootfs", "/bin/sh", "-c", KEEPALIVE, sandbox.id]
    assert env["ENROOT_DATA_PATH"] == str(session)
    assert env["ENROOT_MOUNT_HOME"] == "no" and env["ENROOT_LOGIN_SHELL"] == "no"
    assert env["ENROOT_RESTRICT_DEV"] == "yes" and env["NVIDIA_VISIBLE_DEVICES"] == "void"
    config = Path(env["ENROOT_CONFIG_PATH"])
    assert config.parent == enroot_provider.scratch_root and not any(config.iterdir())
    assert "SLURM_JOB_ID" not in env
    owner = json.loads((session / "owner.json").read_text())
    assert owner["labels"] == {"rollout_id": "r1"} and owner["owner_pid"] == os.getpid()
    init, anchor = owner["init_pid"], owner["anchor_pid"]
    assert namespace_pid(init) == 1 and sandbox.id in (cmdline(init) or "")
    assert Path(f"/proc/{anchor}/task/{anchor}/children").read_text().split() == [str(init)]
    cache = session / "rootfs" / "private-cache"
    assert stat.S_IMODE(cache.stat().st_mode) & 0o700 == 0o700


async def test_exec_runs_as_root_with_verbatim_env_cwd_and_stdin(
    enroot_provider, sqsh, monkeypatch
):
    monkeypatch.setenv("HOST_ONLY", "leak")
    value = "two words; $(echo injected) 'quoted' \"double\""
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh), env={"GREETING": value}))
    result = await sandbox.exec(
        'id -u; printf "%s\\n" "$GREETING"; cat hello.txt; pwd; echo "$HOME"; echo "${HOST_ONLY-unset}"'
    )
    assert result.exit_code == 0, result.stderr
    assert result.stdout.splitlines() == ["0", value, "hello", "/app", "/root", "unset"]
    result = await sandbox.exec(["cat"], stdin=b"from stdin")
    assert (result.exit_code, result.stdout) == (0, "from stdin")
    result = await sandbox.exec("echo $EXTRA; exit 3", env={"EXTRA": "x"}, cwd="/tmp")
    assert (result.exit_code, result.stdout) == (3, "x\n")


async def test_remap_root_can_be_turned_off(tmp_path, sqsh):
    provider = make_enroot_provider(tmp_path, remap_root=False)
    sandbox = await provider.start(SandboxSpec(image=str(sqsh)))
    try:
        assert (await sandbox.exec("id -u")).stdout == f"{os.getuid()}\n"
    finally:
        await sandbox.stop()


async def test_commands_share_a_private_pid_namespace_whose_init_survives_tasks(
    enroot_provider, sqsh
):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    init = await sandbox.exec("tr '\\0' ' ' < /proc/1/cmdline")
    assert sandbox.id in init.stdout
    assert int((await sandbox.exec("echo $$")).stdout) < 1000
    background = (await sandbox.exec("sleep 300 >/dev/null 2>&1 & echo $!")).stdout.strip()
    assert (await sandbox.exec(f"kill -0 {background} && echo running")).stdout == "running\n"
    result = await sandbox.exec("kill -9 -1; kill -9 1; echo survived")
    assert result.stdout == "survived\n"
    assert (await sandbox.exec(f"kill -0 {background} || echo killed")).stdout == "killed\n"
    assert (await sandbox.exec("echo alive")).stdout == "alive\n"


async def test_exec_timeout_kills_the_command_and_keeps_the_sandbox(enroot_provider, sqsh):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    result = await sandbox.exec("echo started; sleep 30", timeout_s=0.5)
    assert result.timed_out and result.exit_code == 124
    assert result.stdout == "started\n"
    assert "timed out after 0.5 s" in result.stderr
    assert (await sandbox.exec("echo alive")).stdout == "alive\n"


async def test_cancelled_exec_kills_its_process_tree(enroot_provider, sqsh):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    task = asyncio.create_task(sandbox.exec("sleep 30 & echo $! > /tmp/sleep.pid; wait"))
    pid_file = sandbox.session_dir / "tmp" / "sleep.pid"
    while not pid_file.exists() or not pid_file.read_text().strip():
        await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.2)
    survivors = await sandbox.exec(f"kill -0 {pid_file.read_text().strip()} || echo gone")
    assert survivors.stdout == "gone\n"


async def test_upload_and_download_files_and_directories(enroot_provider, sqsh, tmp_path):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    source = tmp_path / "in.txt"
    source.write_text("payload\n")
    await sandbox.upload(source, "/app/data/in.txt")
    assert (await sandbox.exec("cat data/in.txt")).stdout == "payload\n"
    tree = tmp_path / "tree"
    (tree / "nested").mkdir(parents=True)
    (tree / "nested" / "a.bin").write_bytes(bytes(range(256)) * 100)
    (tree / "link").symlink_to("nested/a.bin")
    await sandbox.upload(tree, "/app/tree")
    back = tmp_path / "back"
    await sandbox.download("/app/tree", back)
    assert (back / "nested" / "a.bin").read_bytes() == bytes(range(256)) * 100
    assert os.readlink(back / "link") == "nested/a.bin"
    with pytest.raises(FileNotFoundError):
        await sandbox.download("/app/missing.txt", tmp_path / "missing.txt")
    assert not any((sandbox.session_dir / "tmp" / ".hpc-sandbox-transfer").iterdir())


async def test_snapshot_and_restore_copy_files_including_mount_points(
    enroot_provider, sqsh, tmp_path
):
    data = tmp_path / "data"
    data.mkdir()
    spec = SandboxSpec(image=str(sqsh), mounts=[Mount(str(data), "/mnt/data")])
    sandbox = await enroot_provider.start(spec)
    placeholder = sandbox.session_dir / "rootfs" / "mnt" / "data"
    assert stat.S_IMODE(placeholder.stat().st_mode) == 0  # created by enroot for the bind
    await sandbox.exec("echo v1 > state.txt && echo scratch > /tmp/note")
    snapshot = await sandbox.snapshot()
    await sandbox.exec("echo v2 > state.txt")
    assert snapshot.provider == "enroot" and snapshot.image == str(sqsh)
    restored = await enroot_provider.restore(snapshot, spec)
    assert (await restored.exec("cat state.txt /tmp/note")).stdout == "v1\nscratch\n"
    assert (await sandbox.exec("cat state.txt")).stdout == "v2\n"
    await enroot_provider.delete_snapshot(snapshot)
    assert not Path(snapshot.path).exists()


async def test_stop_is_idempotent_and_kills_everything(enroot_provider, sqsh):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    owner = json.loads((sandbox.session_dir / "owner.json").read_text())
    background = int((await sandbox.exec("sleep 300 & echo $!")).stdout)
    assert background > 0
    session = sandbox.session_dir
    await asyncio.gather(sandbox.stop(), sandbox.stop())
    await sandbox.stop()
    assert owned_live_start_time(owner["init_pid"]) != owner["init_start_time"]
    assert owned_live_start_time(owner["anchor_pid"]) != owner["anchor_start_time"]
    assert session is not None and not session.exists()
    assert sandbox not in enroot_provider.sandboxes
    with pytest.raises(SandboxError, match="not running"):
        await sandbox.exec("true")


async def test_a_dead_container_is_reported_as_lost(enroot_provider, sqsh):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    owner = json.loads((sandbox.session_dir / "owner.json").read_text())
    os.kill(owner["init_pid"], signal.SIGKILL)
    assert await gone(owner["init_pid"], owner["init_start_time"])
    with pytest.raises(SandboxLostError):
        await sandbox.exec("true")
    with pytest.raises(SandboxLostError):
        await sandbox.exec("true")
    await sandbox.stop()
    assert not enroot_provider.session_root.joinpath(sandbox.id).exists()


async def test_failed_start_leaves_nothing_behind(enroot_provider, sqsh, fake_enroot):
    fake_enroot.set("start-fails", "1")
    with pytest.raises(SandboxStartError, match="simulated start failure"):
        await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    assert not any(enroot_provider.session_root.glob("hpcsb-*"))
    assert not enroot_provider.sandboxes


async def test_spec_validation(enroot_provider, sqsh, tmp_path):
    spaced = tmp_path / "with space"
    spaced.mkdir()
    with pytest.raises(ValueError, match="whitespace"):
        await enroot_provider.start(SandboxSpec(image=str(sqsh), mounts=[Mount(str(spaced), "/x")]))
    with pytest.raises(ValueError, match="reserved"):
        await enroot_provider.start(
            SandboxSpec(image=str(sqsh), mounts=[Mount(str(tmp_path), "/var/tmp/x")])
        )
    with pytest.raises(ValueError, match="digest"):
        await enroot_provider.start(SandboxSpec(image="docker://alpine@sha256:abc"))


async def test_network_none_has_only_an_up_loopback(enroot_provider, sqsh):
    isolated = await enroot_provider.start(SandboxSpec(image=str(sqsh), network="none"))
    result = await isolated.exec("cat /proc/net/dev")
    assert interfaces(result.stdout) == ["lo"]
    server = "import socket; s = socket.create_server(('127.0.0.1', 0)); print(s.getsockname()[1])"
    assert (await isolated.exec([PYTHON, "-c", server])).exit_code == 0
    shared = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    host = interfaces(Path("/proc/net/dev").read_text())
    assert interfaces((await shared.exec("cat /proc/net/dev")).stdout) == host


async def test_mounts_are_bound_read_only_or_writable(enroot_provider, sqsh, tmp_path):
    data, out = tmp_path / "data", tmp_path / "out"
    data.mkdir()
    out.mkdir()
    (data / "input.txt").write_text("mounted\n")
    single = tmp_path / "single.txt"
    single.write_text("one file\n")
    sandbox = await enroot_provider.start(
        SandboxSpec(
            image=str(sqsh),
            mounts=[
                Mount(str(data), "/mnt/data"),
                Mount(str(out), "/mnt/out", read_only=False),
                Mount(str(single), "/opt/single.txt"),
            ],
        )
    )
    assert (await sandbox.exec("cat /mnt/data/input.txt /opt/single.txt")).stdout == (
        "mounted\none file\n"
    )
    assert (await sandbox.exec("touch /mnt/data/new")).exit_code != 0
    assert (await sandbox.exec("echo result > /mnt/out/result")).exit_code == 0
    assert (out / "result").read_text() == "result\n"


@pytest.mark.skipif(not Path(PYTHON).exists(), reason=f"needs {PYTHON}")
async def test_memory_watchdog_kills_the_largest_process(enroot_provider, sqsh):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh), memory_mb=100))
    hog = "import time; data = bytearray(300 * 1024 * 1024); time.sleep(30)"
    result = await sandbox.exec([PYTHON, "-c", hog], timeout_s=20)
    assert result.exit_code == 137
    assert "memory limit exceeded" in result.stderr
    assert (await sandbox.exec("echo survived")).stdout == "survived\n"


async def test_reap_stops_only_matching_sandboxes(enroot_provider, sqsh):
    first = await enroot_provider.start(SandboxSpec(image=str(sqsh), labels={"rollout_id": "a"}))
    second = await enroot_provider.start(SandboxSpec(image=str(sqsh), labels={"rollout_id": "b"}))
    report = await enroot_provider.reap({"rollout_id": "a"})
    assert report.stopped == [first.id] and not report.errors
    assert enroot_provider.sandboxes == {second}
    assert (await second.exec("echo b")).stdout == "b\n"


CRASH = """\
import asyncio, os, signal, sys
from hpc_sandbox import EnrootProvider, SandboxSpec

async def main():
    scratch, cache, image = sys.argv[1:]
    provider = EnrootProvider(scratch_root=scratch, image_cache=cache, stage_images=False)
    sandbox = await provider.start(SandboxSpec(image=image, labels={"rollout_id": "a"}))
    print(sandbox.id, sandbox._anchor.pid, sandbox._init_pid, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

asyncio.run(main())
"""


async def test_sandboxes_die_with_their_owner_and_the_janitor_removes_them(tmp_path, sqsh):
    result = await run_process(
        [
            sys.executable,
            "-c",
            CRASH,
            str(tmp_path / "scratch"),
            str(tmp_path / "cache"),
            str(sqsh),
        ],
        timeout_s=120,
        env={**os.environ, "PYTHONPATH": SRC},
    )
    assert result.returncode == 128 + signal.SIGKILL, result.detail()
    name, anchor, init = result.stdout.decode().split()
    assert await gone(int(anchor)) and await gone(int(init))
    provider = make_enroot_provider(tmp_path)
    assert (provider.session_root / name).is_dir()
    report = await provider.reap({"rollout_id": "a"})
    assert (report.stopped, report.removed, report.errors) == ([], [name], [])
    assert not (provider.session_root / name).exists()


def abandoned_session(root: Path, name: str, **identity: object) -> Path:
    """A session whose owner is gone, recording ``identity`` as its processes."""
    session = open_session(root, name)
    session.write_owner_record(labels={}, image="x", **identity)
    record = json.loads((session.path / "owner.json").read_text())
    record["owner_start_time"] = "0"  # no live process started at boot time zero
    (session.path / "owner.json").write_text(json.dumps(record))
    session.release()
    return session.path


async def test_janitor_kills_survivors_only_after_proving_their_identity(tmp_path, fake_enroot):
    provider = make_enroot_provider(tmp_path)
    provider.session_root.mkdir(parents=True)
    name, other = "hpcsb-" + "1" * 32, "hpcsb-" + "2" * 32
    # "; :" keeps the shell from exec'ing sleep, so its command line names the session.
    survivor = await asyncio.create_subprocess_exec(
        "/bin/sh", "-c", "sleep 300; :", name, start_new_session=True
    )
    stranger = await asyncio.create_subprocess_exec("sleep", "300", start_new_session=True)
    try:
        for process, session in ((survivor, name), (stranger, other)):
            started = None
            while started is None:
                started = owned_live_start_time(process.pid)
            abandoned_session(
                provider.session_root, session, init_pid=process.pid, init_start_time=started
            )
        report = await provider.reap()
        assert report.stopped == [name] and report.removed == [name]
        assert len(report.errors) == 1 and "identity changed" in report.errors[0]
        assert await asyncio.wait_for(survivor.wait(), 10) == -signal.SIGKILL
        assert stranger.returncode is None and (provider.session_root / other).is_dir()
    finally:
        for process in (survivor, stranger):
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            await process.wait()


async def test_images_are_imported_once_with_safe_settings(
    enroot_provider, fake_enroot, tmp_path, monkeypatch
):
    converter_dir = tmp_path / "converter"
    converter_dir.mkdir()
    converter = converter_dir / "enroot-aufs2ovlfs"
    converter.write_text("#!/bin/sh\nexit 0\n")
    converter.chmod(0o755)
    monkeypatch.setenv("PATH", f"{os.environ['PATH']}{os.pathsep}{converter_dir}")
    monkeypatch.setenv("TAR_OPTIONS", "--no-same-owner")
    spec = SandboxSpec(image="docker://docker.io/library/alpine:3.20")
    first = await enroot_provider.start(spec)
    second = await enroot_provider.start(spec)
    assert first.id != second.id
    (call,) = fake_enroot.calls("import")
    assert call["argv"][-1] == "docker://library/alpine:3.20"
    env = call["env"]
    assert env["TAR_OPTIONS"] == "--no-same-owner --delay-directory-restore"
    wrapper_dir = enroot_provider.scratch_root / "enroot-tools"
    assert env["PATH"].split(os.pathsep)[0] == str(wrapper_dir)
    real = env["HPC_SANDBOX_REAL_AUFS2OVLFS"]
    assert Path(real).name == "enroot-aufs2ovlfs" and Path(real).parent != wrapper_dir
    cached = list(enroot_provider.images.cache_dir.glob("*.sqsh"))
    assert len(cached) == 1 and not stat.S_IMODE(cached[0].stat().st_mode) & 0o222


async def test_failed_imports_are_retried_and_reported(tmp_path, fake_enroot):
    provider = make_enroot_provider(tmp_path)
    provider.images.pull_retry_delay_s = 0.0
    fake_enroot.set("import-failures", "2")
    resolved = await provider.images.resolve("docker://example/flaky:1")
    assert resolved.path.exists() and len(fake_enroot.calls("import")) == 3
    fake_enroot.set("import-failures", "10")
    with pytest.raises(SandboxError, match="attempt 3"):
        await provider.images.resolve("docker://example/broken:1")
    assert not list(provider.images.cache_dir.glob("*broken*"))


def test_import_uris():
    assert import_uri("ubuntu:24.04") == "docker://ubuntu:24.04"
    assert import_uri("docker://docker.io/library/ubuntu") == "docker://library/ubuntu"
    assert import_uri("index.docker.io/library/ubuntu") == "docker://library/ubuntu"
    assert import_uri("ghcr.io/org/image:1") == "docker://ghcr.io#org/image:1"
    assert (
        import_uri("docker://nvcr.io#nvidia/pytorch:24.01")
        == "docker://nvcr.io#nvidia/pytorch:24.01"
    )
    assert import_uri("localhost:5000/image") == "docker://localhost:5000#image"
    assert import_uri("dockerd://image:1") == "dockerd://image:1"
    digest = "sha256:" + "a" * 64
    for reference, uri in (
        (f"docker://alpine@{digest}", f"docker://alpine:{digest}"),
        (f"alpine:3.20@{digest}", f"docker://alpine:{digest}"),
        (f"docker://docker.io/library/ubuntu@{digest}", f"docker://library/ubuntu:{digest}"),
        (f"nvcr.io/nvidia/pytorch:24.01-py3@{digest}", f"docker://nvcr.io#nvidia/pytorch:{digest}"),
        (f"docker://nvcr.io#nvidia/pytorch@{digest}", f"docker://nvcr.io#nvidia/pytorch:{digest}"),
        (f"localhost:5000/image:1@{digest}", f"docker://localhost:5000#image:{digest}"),
    ):
        assert import_uri(reference) == uri, reference
    for reference in ("docker://alpine@sha256:abc", "oras://registry/x", "/images/task.sif"):
        with pytest.raises(ValueError):
            import_uri(reference)


def test_whiteout_wrapper_keeps_same_layer_entries(tmp_path):
    layer = tmp_path / "layer"
    fixture = layer / "testdata"
    fixture.mkdir(parents=True)
    (fixture / "layer2.txt").write_text("2\n")
    (fixture / ".wh.layer2.txt").write_text("")  # same-layer file: the file wins
    (fixture / ".wh.lower-only").write_text("")  # hides a lower layer's entry: kept
    (fixture / ".wh..wh..opq").write_text("")
    (fixture / ".wh.nonempty.txt").write_text("not an OCI whiteout")
    (fixture / "nonempty.txt").write_text("payload")
    (fixture / "directory-target").mkdir()
    (fixture / ".wh.directory-target").write_text("")  # same-layer directory: made opaque
    (fixture / "directory-target").chmod(0o555)
    (fixture / ".wh..wh.payload").write_text("")
    (fixture / "symlink-target").mkdir()
    (fixture / "directory-link").symlink_to(fixture / "symlink-target", target_is_directory=True)
    (fixture / ".wh.directory-link").write_text("")
    fixture.chmod(0o555)
    marker = tmp_path / "converter-ran"
    converter = tmp_path / "real-enroot-aufs2ovlfs"
    converter.write_text(
        f"""#!/bin/sh
set -eu
d="$1/testdata"
test ! -e "$d/.wh.layer2.txt" && test -f "$d/layer2.txt"
test -f "$d/.wh.lower-only" && test -f "$d/.wh..wh..opq" && test -f "$d/.wh.nonempty.txt"
test ! -e "$d/.wh.directory-target" && test -f "$d/directory-target/.wh..wh..opq"
test -f "$d/.wh..wh.payload" && test ! -e "$d/.wh.directory-link"
rm "$d/directory-target/.wh..wh..opq"
printf converted > {marker}
"""
    )
    converter.chmod(0o700)
    wrapper = tmp_path / "enroot-aufs2ovlfs"
    wrapper.write_text(AUFS2OVLFS_WRAPPER)
    wrapper.chmod(0o700)
    try:
        result = subprocess.run(
            [str(wrapper), str(layer)],
            capture_output=True,
            text=True,
            env={**os.environ, "HPC_SANDBOX_REAL_AUFS2OVLFS": str(converter)},
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert marker.read_text() == "converted"
        assert stat.S_IMODE(fixture.stat().st_mode) == 0o555
        assert stat.S_IMODE((fixture / "directory-target").stat().st_mode) == 0o555
    finally:
        for directory in (fixture, fixture / "directory-target"):
            directory.chmod(0o755)


def test_provider_capabilities_follow_the_options(tmp_path, fake_enroot):
    provider = EnrootProvider(scratch_root=tmp_path / "scratch", gpus="all")
    assert provider.capabilities == {"snapshot", "isolation", "network_none", "gpu"}
    session = Session("hpcsb-" + "3" * 32, tmp_path / "session", None)
    provider.runtime.sysconf_dir = tmp_path / "sysconf"  # as if prepared
    environment = provider.runtime.container_environment(session, remap_root=True, gpus="all")
    assert environment["NVIDIA_VISIBLE_DEVICES"] == "all"
    assert "compute" in environment["NVIDIA_DRIVER_CAPABILITIES"]


async def test_async_with_stops_the_sandbox(enroot_provider, sqsh):
    async with await enroot_provider.start(SandboxSpec(image=str(sqsh))) as sandbox:
        owner = json.loads((sandbox.session_dir / "owner.json").read_text())
    assert owned_live_start_time(owner["init_pid"]) != owner["init_start_time"]
    assert not enroot_provider.sandboxes


async def test_sessions_record_their_provider_and_user(enroot_provider, sqsh):
    assert enroot_provider.session_root == enroot_provider.scratch_root / "sessions-enroot"
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    owner = json.loads((sandbox.session_dir / "owner.json").read_text())
    assert (owner["provider"], owner["remap_root"], owner["container_uid"]) == ("enroot", True, 0)
    snapshot = await sandbox.snapshot()
    assert (snapshot.metadata["remap_root"], snapshot.metadata["container_uid"]) == (True, 0)
    await enroot_provider.delete_snapshot(snapshot)


async def test_a_command_that_never_started_is_an_infrastructure_error(
    enroot_provider, sqsh, monkeypatch
):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    with monkeypatch.context() as patch:  # a runtime that never ran the prologue
        for status in (3, 126):  # 126 alone is not a missing working directory
            failing = ["/bin/sh", "-c", f"echo runtime failed >&2; exit {status}"]
            patch.setattr("hpc_sandbox.sandbox.prologue", lambda *_, failing=failing, **__: failing)
            with pytest.raises(SandboxError, match=rf"did not start \(exit {status}\)"):
                await sandbox.exec("true")
    missing = await sandbox.exec(["/no/such/program"])
    assert missing.exit_code == 127 and "command started" not in missing.stderr
    result = await sandbox.exec("echo out; echo err >&2")
    assert (result.stdout, result.stderr) == ("out\n", "err\n")


async def test_wrong_images_are_refused_before_anything_is_created(tmp_path, fake_enroot):
    provider = make_enroot_provider(tmp_path)
    sif = tmp_path / "task.sif"
    sif.write_bytes(b"#!/usr/bin/env run-singularity\n\0SIF_MAGIC\0")
    renamed = tmp_path / "task.img"
    renamed.write_bytes(sif.read_bytes())
    for image in (
        str(sif),
        str(renamed),
        "oras://registry/image:1",
        "docker://x@sha256:abc",
    ):
        with pytest.raises(ValueError):
            await provider.start(SandboxSpec(image=image))
    assert not provider.scratch_root.exists() and not fake_enroot.calls()


async def test_imports_get_only_what_pulling_needs(tmp_path, fake_enroot, monkeypatch):
    monkeypatch.setenv("HPCS_CANARY", "secret")
    monkeypatch.setenv("SLURM_JOB_ID", "123")
    monkeypatch.setenv("https_proxy", "http://proxy:3128")
    monkeypatch.setenv("ENROOT_SQUASH_OPTIONS", "-comp zstd")
    provider = make_enroot_provider(tmp_path)
    await provider.images.resolve("docker://example/task:1")
    (call,) = fake_enroot.calls("import")
    env = call["env"]
    assert "HPCS_CANARY" not in env and "SLURM_JOB_ID" not in env
    assert env["https_proxy"] == "http://proxy:3128"
    assert env["ENROOT_SQUASH_OPTIONS"] == "-comp zstd"


def test_singularity_option_names_are_accepted(tmp_path, fake_enroot):
    def provider(**options):
        return make_enroot_provider(tmp_path, **options)

    assert provider(gpu="nv").gpus == "all"
    assert "network_none" not in provider(allow_network_none=False).capabilities
    assert "network_none" in provider(allow_network_none=True, stop_grace_s=5).capabilities
    for options in ({"gpu": "rocm"}, {"gpu": "nv", "gpus": "0"}, {"cgroup_limits": True}):
        with pytest.raises(ValueError):
            provider(**options)


async def test_network_none_can_be_turned_off(tmp_path, sqsh):
    provider = make_enroot_provider(tmp_path, allow_network_none=False)
    with pytest.raises(ValueError, match="allow_network_none"):
        await provider.start(SandboxSpec(image=str(sqsh), network="none"))


async def test_downloads_do_not_need_to_preserve_unmapped_owners(enroot_provider, sqsh, tmp_path):
    shared = Path("/usr/share")  # root-owned on the host: an unmapped owner in the sandbox
    sandbox = await enroot_provider.start(
        SandboxSpec(image=str(sqsh), mounts=[Mount(str(shared), "/mnt/shared")])
    )
    owner = (await sandbox.exec("stat -c %u /mnt/shared")).stdout.strip()
    assert owner != "0"  # not mapped, so not root inside
    source = next(
        path for path in sorted(shared.iterdir()) if path.is_file() and os.access(path, os.R_OK)
    )
    await sandbox.download(f"/mnt/shared/{source.name}", tmp_path / "copy")
    assert (tmp_path / "copy").read_bytes() == source.read_bytes()


async def test_sandboxes_start_with_enroots_standard_configuration_only(
    tmp_path, sqsh, fake_enroot, monkeypatch
):
    site = tmp_path / "site-enroot"
    (site / "hooks.d").mkdir(parents=True)
    for hook in ("10-devices.sh", "10-shadow.sh", "10-aptfix.sh", "10-home.sh", "98-nvidia.sh"):
        (site / "hooks.d" / hook).write_text(f"#!/bin/sh\n# {hook}\n")
        (site / "hooks.d" / hook).chmod(0o755)
    (site / "hooks.d" / "10-aptfix.sh").chmod(0o644)  # disabled by the site
    monkeypatch.setenv("ENROOT_SYSCONF_PATH", str(site))
    provider = make_enroot_provider(tmp_path)
    async with await provider.start(SandboxSpec(image=str(sqsh))):
        pass
    (start,) = fake_enroot.calls("start")
    sysconf = Path(start["env"]["ENROOT_SYSCONF_PATH"])
    assert sysconf.parent == provider.scratch_root / "enroot-sysconf"
    hooks = sysconf / "hooks.d"
    assert sorted(path.name for path in hooks.iterdir()) == [
        "10-hpc-sandbox-dev.sh",
        "10-shadow.sh",
        "98-nvidia.sh",
    ]
    assert (hooks / "10-shadow.sh").read_text() == "#!/bin/sh\n# 10-shadow.sh\n"
    assert all(os.access(path, os.X_OK) for path in hooks.iterdir())
    restore_hooks = provider.runtime.restore_sysconf_dir / "hooks.d"
    assert sorted(path.name for path in restore_hooks.iterdir()) == [
        "10-hpc-sandbox-dev.sh",
        "98-nvidia.sh",
    ]
    fstab = (sysconf / "mounts.d" / "10-system.fstab").read_text()
    assert "/proc" in fstab and "/etc/hosts" in fstab and "hostname" not in fstab
    assert "/etc/localtime" not in fstab  # images link it into their own zoneinfo
    assert "tmpfs        /dev " in fstab and "/dev        /dev" not in fstab  # never the host's
    assert not any((sysconf / "environ.d").iterdir())
    resolv = [m for m in option_values(start["argv"], "--mount") if ":/etc/resolv.conf:" in m]
    assert len(resolv) == int(Path("/etc/resolv.conf").is_file())


# A copy of the configuration section of enroot 3.5's script.
ENROOT_SCRIPT = """\
#! /usr/bin/env bash
set -euo pipefail

# ---- BEGIN CONFIG ----

config::export() {
    [ -n "${3-}" ] && export "$1=$2" || export "$1=${!1:-$2}"
}

config::load() {
    local -r file="$1" overwrite="${2-}"

    if [ -e "${file}" ]; then
        while IFS=$' \t=' read -r key value; do
            if [[ "${key}" =~ ^ENROOT_[[:upper:]_]+$ ]]; then
                config::export "${key}" "$(eval echo "${value}")" "${overwrite}"
            fi
        done < "${file}"
   fi
}

config::init() {
    local -r file="$1" overwrite="${2-}"

    config::load "${file}" "${overwrite}"
    if [ -d "${file}.d" ]; then
      shopt -s nullglob
      for dropin in "${file}.d"/*".conf"; do
        config::load "${dropin}" "${overwrite}"
      done
      shopt -u nullglob
    fi
}

config::fini() {
    for var in $(compgen -e "ENROOT_"); do
        if [[ "${!var}" =~ ^(no?|N[oO]?|[fF](alse)?|FALSE)$ ]]; then
            unset "${var}"
        fi
    done
}

config::init "@CONF@"

config::export ENROOT_SYSCONF_PATH     "/usr/local/etc/enroot"
config::export ENROOT_RUNTIME_PATH     "@RUNTIME@"
config::export ENROOT_REMAP_ROOT       false

config::fini

(umask 077 && mkdir -p "${ENROOT_RUNTIME_PATH}")

# ----- END CONFIG -----
"""


def test_the_installations_configuration_is_found_like_enroot_finds_it(tmp_path, monkeypatch):
    monkeypatch.delenv("ENROOT_SYSCONF_PATH", raising=False)
    conf = tmp_path / "etc" / "enroot.conf"
    conf.parent.mkdir()
    script = tmp_path / "enroot"
    runtime_path = tmp_path / "runtime"
    script.write_text(
        ENROOT_SCRIPT.replace("@CONF@", str(conf)).replace("@RUNTIME@", str(runtime_path))
    )
    script.chmod(0o755)
    runtime = EnrootProvider(scratch_root=tmp_path / "scratch", binary=str(script)).runtime
    assert runtime.site_sysconf() == Path("/usr/local/etc/enroot")
    for line in (
        "ENROOT_SYSCONF_PATH /opt/enroot/etc",
        "ENROOT_SYSCONF_PATH\t/opt/enroot/etc",
        "ENROOT_SYSCONF_PATH=/opt/enroot/etc",
        'ENROOT_SYSCONF_PATH "/opt/enroot/etc"   # site hooks',
        "ENROOT_SYSCONF_PATH /opt/enroot/etc\nENROOT_SYSCONF_PATH /opt/other",
        'ENROOT_SYSCONF_PATH ""\nENROOT_SYSCONF_PATH /opt/enroot/etc',  # empty means unset
    ):
        conf.write_text(line + "\n")
        assert runtime.site_sysconf() == Path("/opt/enroot/etc"), line
    conf.write_text("ENROOT_SYSCONF_PATH ${HOME}/enroot\n")
    assert runtime.site_sysconf() == Path.home() / "enroot"
    conf.write_text("")
    (conf.parent / "enroot.conf.d").mkdir()
    (conf.parent / "enroot.conf.d" / "10-site.conf").write_text("ENROOT_SYSCONF_PATH /opt/d\n")
    assert runtime.site_sysconf() == Path("/opt/d")
    monkeypatch.setenv("ENROOT_SYSCONF_PATH", "/from/env")
    assert runtime.site_sysconf() == Path("/from/env")
    assert runtime.setting("ENROOT_REMAP_ROOT") is None  # false: enroot unsets it
    assert runtime.setting("ENROOT_RUNTIME_PATH") == str(runtime_path)
    assert not runtime_path.exists()  # reading settings creates nothing
    script.write_text('config::export ENROOT_SYSCONF_PATH "/usr/local/etc/enroot"\n')
    assert runtime.site_sysconf() == Path("/from/env")
    monkeypatch.delenv("ENROOT_SYSCONF_PATH")
    assert runtime.site_sysconf() == Path("/usr/local/etc/enroot")


async def test_gpus_need_the_nvidia_hook(tmp_path, sqsh, fake_enroot, monkeypatch):
    monkeypatch.setenv("ENROOT_SYSCONF_PATH", str(tmp_path / "no-hooks"))
    provider = make_enroot_provider(tmp_path, gpus="all")
    with pytest.raises(SandboxError, match="98-nvidia.sh"):
        await provider.start(SandboxSpec(image=str(sqsh)))


async def test_concurrent_first_starts_share_one_configuration(tmp_path, sqsh, fake_enroot):
    provider = make_enroot_provider(tmp_path)
    runtime, prepared = provider.runtime, []
    original = runtime.prepare_sysconf

    def counting(**options):
        prepared.append(options)
        return original(**options)

    runtime.prepare_sysconf = counting
    sandboxes = await asyncio.gather(
        *(provider.start(SandboxSpec(image=str(sqsh))) for _ in range(6))
    )
    assert len({sandbox.id for sandbox in sandboxes}) == 6 and len(prepared) == 1
    other = make_enroot_provider(tmp_path)  # same scratch root: the same, unchanged directory
    await other._prepare()
    assert other.runtime.sysconf_dir == runtime.sysconf_dir


async def test_restores_do_not_let_the_sandbox_steer_enroot(
    enroot_provider, sqsh, fake_enroot, tmp_path
):
    secret = tmp_path / "host-secret.env"
    secret.write_text("API_TOKEN=host-token\n")
    spec = SandboxSpec(image=str(sqsh), env={"FROM_SPEC": "1"})
    sandbox = await enroot_provider.start(spec)
    planted = (
        f"echo '{tmp_path} /mnt/host none x-create=dir,bind,rw 0 -1' >> /etc/fstab && "
        f"ln -sf {secret} /etc/environment && ln -s {secret} /.lock && echo planted"
    )
    assert (await sandbox.exec(planted)).stdout == "planted\n"
    snapshot = await sandbox.snapshot()
    assert json.loads(Path(snapshot.path, "environment.json").read_text())["PATH"]
    restored = await enroot_provider.restore(snapshot, spec)
    starts = fake_enroot.starts()
    assert starts[0]["rootfs_config"] == ["etc/environment"]  # the image's own
    assert starts[-1]["rootfs_config"] == []  # hidden from enroot while it starts
    (*_, restore_start) = fake_enroot.calls("start")
    assert restore_start["env"]["ENROOT_SYSCONF_PATH"] == str(
        enroot_provider.runtime.restore_sysconf_dir
    )
    environment = (await restored.exec("env")).stdout
    assert "API_TOKEN" not in environment and "FROM_SPEC=1" in environment
    assert (await restored.exec("tail -n 1 /etc/fstab")).stdout.startswith(str(tmp_path))
    assert not (restored.session_dir / "rootfs" / ".lock").is_symlink()
    await enroot_provider.delete_snapshot(snapshot)


async def test_a_symlinked_etc_is_refused(enroot_provider, sqsh, tmp_path):
    spec = SandboxSpec(image=str(sqsh))
    sandbox = await enroot_provider.start(spec)
    await sandbox.exec(f"mv /etc /etc.real && ln -s {tmp_path} /etc")
    snapshot = await sandbox.snapshot()
    with pytest.raises(SandboxStartError, match="/etc in the sandbox's rootfs"):
        await enroot_provider.restore(snapshot, spec)


async def test_the_environment_never_reaches_host_helpers(enroot_provider, sqsh, tmp_path):
    not_a_library = tmp_path / "not-a-library.so"  # exists on the host only
    not_a_library.write_text("text")
    sandbox = await enroot_provider.start(
        SandboxSpec(image=str(sqsh), env={"LD_PRELOAD": str(not_a_library)})
    )
    result = await sandbox.exec('echo "$LD_PRELOAD"')
    assert result.stdout == f"{not_a_library}\n"
    assert "ELF" not in result.stderr  # the host's nsenter did not try to load it


async def test_downloads_of_unreadable_files_and_special_files(enroot_provider, sqsh, tmp_path):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    await sandbox.exec(
        "mkdir -p out/locked && echo secret > out/locked/f && chmod 000 out/locked/f out/locked"
        " && echo private > zero && chmod 000 zero && mkfifo pipe"
    )
    await sandbox.download("/app/zero", tmp_path / "zero")
    assert (tmp_path / "zero").read_text() == "private\n"
    await sandbox.download("/app/out", tmp_path / "out")
    assert (tmp_path / "out" / "locked" / "f").read_text() == "secret\n"
    with pytest.raises(SandboxError, match="pipe"):
        await sandbox.download("/app/pipe", tmp_path / "pipe")


async def test_extra_start_args_that_remap_root_are_recorded(tmp_path, sqsh):
    provider = make_enroot_provider(tmp_path, remap_root=False, extra_start_args=["--root"])
    sandbox = await provider.start(SandboxSpec(image=str(sqsh)))
    owner = json.loads((sandbox.session_dir / "owner.json").read_text())
    assert (owner["remap_root"], owner["container_uid"]) == (True, 0)
    assert (await sandbox.exec("id -u")).stdout == "0\n"
    await sandbox.stop()


@pytest.mark.parametrize("singularity_installed", [True, False])
async def test_enroot_sessions_of_earlier_versions_are_swept(
    tmp_path, fake_enroot, monkeypatch, singularity_installed
):
    import shutil

    real_which = shutil.which
    monkeypatch.setattr(
        "hpc_sandbox.enroot.shutil.which",
        lambda name, *args, **kwargs: (
            f"/usr/bin/{name}"
            if name in ("singularity", "apptainer") and singularity_installed
            else None
            if name in ("singularity", "apptainer")
            else real_which(name, *args, **kwargs)
        ),
    )
    provider = make_enroot_provider(tmp_path)
    legacy = tmp_path / "scratch" / "sessions"
    names = ["hpcsb-" + "4" * 32, "hpcsb-" + "5" * 32, "hpcsb-" + "6" * 32]
    for name, identity in zip(
        names, ({"anchor_pid": 1, "init_pid": 2}, {"instance_pid_file": "x"}, {}), strict=True
    ):
        session = open_session(legacy, name)
        session.write_owner_record(labels={}, image="x", **identity)
        record = json.loads((session.path / "owner.json").read_text())
        record["owner_start_time"] = "0"
        (session.path / "owner.json").write_text(json.dumps(record))
        session.release()
    report = await provider.reap()
    unknown = names[2]  # cannot tell whose: left to a Singularity janitor if there is one
    assert sorted(report.removed) == sorted(
        [names[0], *([] if singularity_installed else [unknown])]
    )
    assert not report.errors and (legacy / names[1]).is_dir()
    assert (legacy / unknown).is_dir() == singularity_installed


async def test_transfers_never_follow_what_the_sandbox_plants(enroot_provider, sqsh, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
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


async def test_environment_values_stay_off_command_lines(enroot_provider, sqsh):
    secret = "tok-" + "s3cret" * 4
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh), env={"API_TOKEN": secret}))
    # 200 kB in all: more than one command-line argument may hold (each variable stays
    # below the kernel's 128 KiB limit for a single string).
    large = {f"LARGE_{index}": "x" * 10_000 for index in range(20)}
    task = asyncio.create_task(sandbox.exec("sleep 1; echo $API_TOKEN", env=large))
    seen = []
    while not task.done():
        for proc in Path("/proc").iterdir():
            try:
                seen.append(secret in (proc / "cmdline").read_text(errors="replace"))
            except OSError:
                continue
        await asyncio.sleep(0.05)
    result = await task
    assert result.stdout == f"{secret}\n" and not any(seen)


async def test_snapshots_of_a_busy_sandbox_fail_cleanly_if_at_all(enroot_provider, sqsh):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    churn = asyncio.create_task(
        sandbox.exec(
            "i=0; while [ $i -lt 4000 ]; do : > /app/f$i; rm -f /app/f$i; i=$((i+1)); done"
        )
    )
    for _ in range(3):
        try:
            snapshot = await sandbox.snapshot()
        except SandboxError:
            continue  # a file vanished under the copy; take snapshots while quiescent
        await enroot_provider.delete_snapshot(snapshot)
    await churn


def test_descriptor_helpers_never_follow_symlinks(tmp_path):
    from hpc_sandbox import safefs

    outside = tmp_path / "outside"
    (outside / "secret").parent.mkdir()
    (outside / "secret").write_text("host\n")
    (outside / "secret").chmod(0o600)
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "dir-link").symlink_to(outside, target_is_directory=True)
    (tree / "file-link").symlink_to(outside / "secret")
    with safefs.opened_dir(tree) as fd:
        assert safefs.set_mode("file-link", fd, exact=0o000) is None
        with pytest.raises(NotADirectoryError):
            safefs.open_dir("dir-link", fd)
        safefs.copy_out("dir-link", fd, tmp_path / "copy")
        safefs.remove("dir-link", fd)
    assert (tmp_path / "copy").is_symlink() and (outside / "secret").read_text() == "host\n"
    assert stat.S_IMODE((outside / "secret").stat().st_mode) == 0o600


def test_snapshot_preparation_opens_only_enroots_placeholders(tmp_path):
    from hpc_sandbox import safefs
    from hpc_sandbox.enroot import make_placeholders_readable

    rootfs = tmp_path / "rootfs"
    (rootfs / "mnt" / "target").mkdir(parents=True)
    (rootfs / "etc").mkdir()
    (rootfs / "etc" / "resolv.conf").touch()
    (rootfs / "private").mkdir()
    (rootfs / "notes").write_text("x")
    (rootfs / "deep").mkdir()
    make_deep_tree(rootfs / "deep", 3 * sys.getrecursionlimit())
    modes = {
        rootfs / "mnt" / "target": (0, 0o500),
        rootfs / "etc" / "resolv.conf": (0, 0o400),
        rootfs / "private": (0o300, 0o300),  # the sandbox's own choice
        rootfs / "notes": (0, 0),  # not empty: not enroot's
    }
    for path, (before, _) in modes.items():
        path.chmod(before)
    try:
        make_placeholders_readable(rootfs)
        for path, (_, after) in modes.items():
            assert stat.S_IMODE(path.lstat().st_mode) == after, path
    finally:
        with safefs.opened_dir(tmp_path) as fd:
            safefs.remove("rootfs", fd)  # too deep for pytest's recursive cleanup


def test_the_time_zone_replaces_symlinks_without_following_them(tmp_path):
    from hpc_sandbox.enroot import install_time_zone

    zone = tmp_path / "host-zone"
    zone.write_bytes(b"TZif host")
    image_zone = tmp_path / "image-utc"
    image_zone.write_bytes(b"TZif utc")
    rootfs = tmp_path / "rootfs"
    (rootfs / "etc").mkdir(parents=True)
    (rootfs / "etc" / "localtime").symlink_to(image_zone)
    install_time_zone(rootfs, zone)
    localtime = rootfs / "etc" / "localtime"
    assert not localtime.is_symlink() and localtime.read_bytes() == b"TZif host"
    assert image_zone.read_bytes() == b"TZif utc"
    install_time_zone(tmp_path / "no-etc", zone)
    install_time_zone(rootfs, tmp_path / "no-zone")
    assert localtime.read_bytes() == b"TZif host"


async def test_fresh_sandboxes_get_the_hosts_time_zone_and_restores_keep_theirs(
    enroot_provider, sqsh
):
    host = Path("/etc/localtime")
    if not host.exists():
        pytest.skip("the host has no /etc/localtime")
    spec = SandboxSpec(image=str(sqsh))
    sandbox = await enroot_provider.start(spec)
    localtime = sandbox.session_dir / "rootfs" / "etc" / "localtime"
    assert not localtime.is_symlink() and localtime.read_bytes() == host.read_bytes()
    assert (await sandbox.exec("echo kept > /etc/localtime")).exit_code == 0
    snapshot = await sandbox.snapshot()
    restored = await enroot_provider.restore(snapshot, spec)
    assert (restored.session_dir / "rootfs" / "etc" / "localtime").read_text() == "kept\n"
    await enroot_provider.delete_snapshot(snapshot)


async def test_commands_need_nothing_from_the_sandboxs_tmp(enroot_provider, sqsh):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh), env={"FROM_SPEC": "1"}))
    mounted = await sandbox.exec("mount -t tmpfs tmpfs /tmp && echo mounted")
    if mounted.exit_code != 0:
        pytest.skip(f"cannot mount in the fake container: {mounted.stderr}")
    result = await sandbox.exec('echo "$FROM_SPEC $EXTRA"; cat', env={"EXTRA": "x"}, stdin=b"in\n")
    assert result.stdout == "1 x\nin\n" and result.exit_code == 0, result
    assert (await sandbox.exec("umount /tmp && echo unmounted")).stdout == "unmounted\n"


async def test_restores_of_snapshots_from_earlier_versions_get_the_hosts_time_zone(
    enroot_provider, sqsh
):
    host = Path("/etc/localtime")
    if not host.exists():
        pytest.skip("the host has no /etc/localtime")
    spec = SandboxSpec(image=str(sqsh))
    sandbox = await enroot_provider.start(spec)
    assert (await sandbox.exec("echo old > /etc/localtime")).exit_code == 0
    snapshot = await sandbox.snapshot()
    Path(snapshot.path, "environment.json").unlink()  # as earlier versions wrote them
    restored = await enroot_provider.restore(snapshot, spec)
    localtime = restored.session_dir / "rootfs" / "etc" / "localtime"
    assert localtime.read_bytes() == host.read_bytes()
    await enroot_provider.delete_snapshot(snapshot)


def test_hanging_settings_are_abandoned_with_everything_they_started(tmp_path, monkeypatch):
    marker = f"{os.getpid()}.4321"
    conf = tmp_path / "enroot.conf"
    conf.write_text(f"ENROOT_SYSCONF_PATH $(sleep {marker}; echo /never)\n")
    script = tmp_path / "enroot"
    script.write_text(
        ENROOT_SCRIPT.replace("@CONF@", str(conf)).replace("@RUNTIME@", str(tmp_path / "rt"))
    )
    script.chmod(0o755)
    monkeypatch.delenv("ENROOT_SYSCONF_PATH", raising=False)
    monkeypatch.setattr("hpc_sandbox.enroot._SETTING_TIMEOUT_S", 1.0)
    runtime = EnrootProvider(scratch_root=tmp_path / "scratch", binary=str(script)).runtime
    assert runtime.setting("ENROOT_SYSCONF_PATH") is None

    def sleeping() -> bool:
        for proc in Path("/proc").iterdir():
            with contextlib.suppress(OSError):
                if marker in (proc / "cmdline").read_text():
                    return True
        return False

    assert not sleeping()


async def test_imports_read_enroots_settings_off_the_event_loop(tmp_path, fake_enroot):
    import threading

    provider = make_enroot_provider(tmp_path)
    threads = []
    original = provider.runtime.import_environment

    def recording() -> dict[str, str]:
        threads.append(threading.current_thread())
        return original()

    provider.runtime.import_environment = recording
    await provider.images.resolve("docker://alpine:3.20")
    assert threads and threading.main_thread() not in threads


async def test_commands_without_stdin_read_dev_null(enroot_provider, sqsh):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    result = await sandbox.exec("[ -c /proc/self/fd/0 ] && echo null; cat")
    assert result.stdout == "null\n", result
    piped = await sandbox.exec("[ -p /proc/self/fd/0 ] && echo pipe; cat", stdin=b"in")
    assert piped.stdout == "pipe\nin", piped


async def test_host_side_transfer_errors_stay_os_errors(enroot_provider, sqsh, tmp_path):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh)))
    locked = tmp_path / "locked"
    locked.mkdir(mode=0o500)
    unreadable = tmp_path / "unreadable"
    unreadable.write_text("x")
    unreadable.chmod(0)
    try:
        with pytest.raises(PermissionError):
            await sandbox.download("/app/hello.txt", locked / "hello.txt")
        with pytest.raises(PermissionError):
            await sandbox.upload(unreadable, "/app/unreadable")
        fifo = tmp_path / "fifo"
        os.mkfifo(fifo)
        with pytest.raises(shutil.SpecialFileError):
            await sandbox.upload(fifo, "/app/fifo")
        await sandbox.upload("/dev/null", "/app/empty")
        assert (await sandbox.exec("wc -c < /app/empty")).stdout.strip() == "0"
    finally:
        locked.chmod(0o700)
        unreadable.chmod(0o600)


async def test_a_missing_working_directory_fails_the_command_not_the_sandbox(
    enroot_provider, sqsh, tmp_path
):
    sandbox = await enroot_provider.start(SandboxSpec(image=str(sqsh), workdir="/work"))
    assert (await sandbox.exec("rm -rf /work && echo payload > /out.txt", cwd="/")).exit_code == 0
    for cwd, reason in ((None, "it does not exist"), ("/work", "it does not exist")):
        result = await sandbox.exec("echo ran", cwd=cwd)
        assert result.exit_code == 126 and result.stdout == "", (cwd, result)
        assert "cannot enter the working directory /work: " + reason in result.stderr, result
    result = await sandbox.exec("echo ran", cwd="/out.txt")
    assert (result.exit_code, result.stderr.split(": ")[-1]) == (126, "not a directory\n")
    assert (await sandbox.exec("pwd", cwd="/")).stdout == "/\n"
    # Transfers of absolute paths do not need the workdir; relative ones resolve from it.
    await sandbox.download("/out.txt", tmp_path / "out.txt")
    assert (tmp_path / "out.txt").read_text() == "payload\n"
    await sandbox.upload(tmp_path / "out.txt", "/back.txt")
    assert (await sandbox.exec("cat /back.txt", cwd="/")).stdout == "payload\n"
    await sandbox.exec("mkdir /work", cwd="/")
    await sandbox.upload(tmp_path / "out.txt", "relative.txt")
    assert (await sandbox.exec("cat /work/relative.txt", cwd="/")).stdout == "payload\n"
    await sandbox.download("relative.txt", tmp_path / "relative.txt")
    assert (tmp_path / "relative.txt").read_text() == "payload\n"
    # A relative cwd resolves from the workdir too, whatever CDPATH says.
    await sandbox.exec("mkdir -p /work/sub /opt/sub", cwd="/")
    moved = await sandbox.exec("pwd", cwd="sub", env={"CDPATH": "/opt"})
    assert moved.stdout == "/work/sub\n", moved
    with pytest.raises(FileNotFoundError):
        await sandbox.download("", tmp_path / "everything")


async def test_digest_pinned_images_are_imported_by_digest(tmp_path, fake_enroot):
    provider = make_enroot_provider(tmp_path)
    digest = "b" * 64
    sandbox = await provider.start(SandboxSpec(image=f"docker://example/task:1@sha256:{digest}"))
    (pulled,) = fake_enroot.calls("import")
    assert pulled["argv"][-1] == f"docker://example/task:sha256:{digest}"
    assert (provider.images.cache_dir / f"oci-{digest}.sqsh.json").is_file()
    await sandbox.stop()


async def test_refreshed_tags_reach_new_sandboxes_only(tmp_path, fake_enroot):
    provider = make_enroot_provider(tmp_path)
    old = await provider.start(SandboxSpec(image="docker://example/task:1"))
    release = "cat /etc/os-release"
    fake_enroot.set("image-revision", "2")  # the tag moved
    cached = await provider.start(SandboxSpec(image="docker://example/task:1"))
    assert len(fake_enroot.calls("import")) == 1  # still the cached image
    await provider.images.pull("docker://example/task:1", refresh=True)
    assert len(fake_enroot.calls("import")) == 2
    new = await provider.start(SandboxSpec(image="docker://example/task:1"))
    assert (await new.exec(release)).stdout == "fake 2\n"
    assert (await old.exec(release)).stdout == (await cached.exec(release)).stdout == "fake\n"
    for sandbox in (old, cached, new):
        await sandbox.stop()


async def test_pulls_create_the_scratch_directories_they_need(tmp_path, fake_enroot):
    provider = make_enroot_provider(tmp_path)
    await provider.images.pull("docker://example/task:1")
    assert provider.runtime.temp_dir.is_dir() and provider.runtime.cache_dir.is_dir()
