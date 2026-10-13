from __future__ import annotations

import asyncio
import json
import os
import re
import signal
import stat
from pathlib import Path

import pytest
from conftest import make_provider

from hpc_sandbox import (
    Mount,
    Sandbox,
    SandboxError,
    SandboxLostError,
    SandboxProvider,
    SandboxSpec,
    SandboxStartError,
    SingularityProvider,
)
from hpc_sandbox.procfs import owned_live_start_time

PYTHON = "/usr/bin/python3"


def option_values(argv: list[str], flag: str) -> list[str]:
    return [argv[index + 1] for index, arg in enumerate(argv[:-1]) if arg == flag]


async def test_provider_and_sandbox_satisfy_the_protocols(provider, sif):
    assert isinstance(provider, SandboxProvider)
    assert provider.name == "singularity"
    assert provider.capabilities == {"snapshot", "isolation"}
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    assert isinstance(sandbox, Sandbox)


async def test_start_uses_the_abci_safe_instance_flags(provider, sif, fake_runtime):
    sandbox = await provider.start(SandboxSpec(image=str(sif), labels={"rollout_id": "r1"}))
    assert re.fullmatch(r"hpcsb-[0-9a-f]{32}", sandbox.id)
    (start,) = fake_runtime.calls("instance", "start")
    for flag in ("--cleanenv", "--containall", "--no-home", "--no-eval", "--writable"):
        assert flag in start
    assert "--writable-tmpfs" not in start and "--fakeroot" not in start
    assert option_values(start, "--no-mount") == ["home,cwd,bind-paths"]
    session = sandbox.session_dir
    assert session is not None and session.parent == provider.session_root
    assert stat.S_IMODE(session.stat().st_mode) == 0o700
    binds = option_values(start, "--bind")
    assert f"{session}/tmp:/tmp:rw" in binds and f"{session}/var-tmp:/var/tmp:rw" in binds
    assert option_values(start, "--pid-file") == [str(session / "instance.pid")]
    assert start[-2:] == [str(session / "rootfs"), sandbox.id]
    owner = json.loads((session / "owner.json").read_text())
    assert owner["labels"] == {"rollout_id": "r1"}
    assert owner["instance_pid"] == fake_runtime.instance_pid(sandbox.id)
    assert owner["owner_pid"] == os.getpid()
    startscript = session / "rootfs" / ".singularity.d" / "startscript"
    assert "sleep" in startscript.read_text() and os.access(startscript, os.X_OK)
    cache = session / "rootfs" / "private-cache"
    assert stat.S_IMODE(cache.stat().st_mode) & 0o700 == 0o700


async def test_exec_passes_env_verbatim_cwd_and_stdin(provider, sif):
    value = "two words; $(echo injected) 'quoted' \"double\""
    sandbox = await provider.start(SandboxSpec(image=str(sif), env={"GREETING": value}))
    result = await sandbox.exec('printf "%s\\n" "$GREETING"; cat hello.txt; pwd')
    assert result.exit_code == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[0] == value
    assert lines[1] == "hello"
    assert lines[2].endswith("/rootfs/app")
    result = await sandbox.exec(["cat"], stdin=b"from stdin")
    assert (result.exit_code, result.stdout) == (0, "from stdin")
    result = await sandbox.exec("echo $EXTRA; exit 3", env={"EXTRA": "x"})
    assert (result.exit_code, result.stdout) == (3, "x\n")


async def test_exec_timeout_kills_the_command_and_keeps_the_sandbox(provider, sif):
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    result = await sandbox.exec("echo started; sleep 30", timeout_s=0.5)
    assert result.timed_out and result.exit_code == 124
    assert result.stdout == "started\n"
    assert "timed out after 0.5 s" in result.stderr
    assert (await sandbox.exec("echo alive")).stdout == "alive\n"


async def test_cancelled_exec_kills_its_process_tree(provider, sif, tmp_path):
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    pid_file = tmp_path / "sleep.pid"
    task = asyncio.create_task(sandbox.exec(f"sleep 30 & echo $! > {pid_file}; wait"))
    while not pid_file.exists() or not pid_file.read_text().strip():
        await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.2)
    assert owned_live_start_time(int(pid_file.read_text())) is None
    assert (await sandbox.exec("echo alive")).stdout == "alive\n"


async def test_upload_and_download_files_and_directories(provider, sif, tmp_path):
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    source = tmp_path / "in.txt"
    source.write_text("payload\n")
    await sandbox.upload(source, "/app/data/in.txt")
    assert (await sandbox.exec("cat data/in.txt")).stdout == "payload\n"

    tree = tmp_path / "tree"
    (tree / "nested").mkdir(parents=True)
    (tree / "nested" / "a.txt").write_text("a\n")
    (tree / ".hidden").write_text("h\n")
    (tree / "link").symlink_to("nested/a.txt")
    await sandbox.upload(tree, "/app/tree")
    back = tmp_path / "back"
    await sandbox.download("/app/tree", back)
    assert (back / "nested" / "a.txt").read_text() == "a\n"
    assert (back / ".hidden").read_text() == "h\n"
    assert os.readlink(back / "link") == "nested/a.txt"

    await sandbox.download("/app/hello.txt", tmp_path / "out" / "hello.txt")
    assert (tmp_path / "out" / "hello.txt").read_text() == "hello\n"
    with pytest.raises(FileNotFoundError):
        await sandbox.download("/app/missing.txt", tmp_path / "missing.txt")
    with pytest.raises(FileNotFoundError):
        await sandbox.upload(tmp_path / "absent", "/app/absent")
    assert not any((sandbox.session_dir / "tmp" / ".hpc-sandbox-transfer").iterdir())


async def test_snapshot_and_restore_copy_files(provider, sif):
    spec = SandboxSpec(image=str(sif), labels={"rollout_id": "r1"})
    sandbox = await provider.start(spec)
    await sandbox.exec("echo v1 > state.txt && echo scratch > /tmp/note")
    snapshot = await sandbox.snapshot()
    await sandbox.exec("echo v2 > state.txt")
    assert snapshot.provider == "singularity" and snapshot.image == str(sif)
    assert Path(snapshot.path, "snapshot.json").exists()

    restored = await provider.restore(snapshot, spec)
    assert restored.id != sandbox.id
    assert (await restored.exec("cat state.txt /tmp/note")).stdout == "v1\nscratch\n"
    assert (await sandbox.exec("cat state.txt")).stdout == "v2\n"

    with pytest.raises(ValueError, match="not"):
        await provider.restore(snapshot, SandboxSpec(image="docker://other:1"))
    await provider.delete_snapshot(snapshot)
    assert not Path(snapshot.path).exists()


async def test_stop_is_idempotent_and_removes_the_session(provider, sif, fake_runtime):
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    pid = fake_runtime.instance_pid(sandbox.id)
    session = sandbox.session_dir
    await asyncio.gather(sandbox.stop(), sandbox.stop())
    await sandbox.stop()
    assert owned_live_start_time(pid) is None
    assert session is not None and not session.exists()
    assert sandbox not in provider.sandboxes
    with pytest.raises(SandboxError, match="not running"):
        await sandbox.exec("true")
    assert len(fake_runtime.calls("instance", "stop")) == 1


async def test_a_dead_instance_is_reported_as_lost(provider, sif, fake_runtime):
    sandbox = await provider.start(SandboxSpec(image=str(sif)))
    os.kill(fake_runtime.instance_pid(sandbox.id), signal.SIGKILL)
    await asyncio.sleep(0.2)
    with pytest.raises(SandboxLostError):
        await sandbox.exec("true")
    with pytest.raises(SandboxLostError):
        await sandbox.exec("true")
    await sandbox.stop()
    assert not provider.session_root.joinpath(sandbox.id).exists()


async def test_failed_start_leaves_nothing_behind(provider, sif, monkeypatch):
    monkeypatch.setenv("FAKE_SINGULARITY_INSTANCE_EXITS", "1")
    with pytest.raises(SandboxStartError, match="exited right after starting"):
        await provider.start(SandboxSpec(image=str(sif)))
    assert not any(provider.session_root.glob("hpcsb-*"))
    assert not provider.sandboxes


async def test_spec_validation(provider, sif, tmp_path):
    with pytest.raises(ValueError, match="allow_network_none"):
        await provider.start(SandboxSpec(image=str(sif), network="none"))
    with pytest.raises(ValueError, match="reserved"):
        await provider.start(SandboxSpec(image=str(sif), mounts=[Mount(str(tmp_path), "/tmp/x")]))
    with pytest.raises(ValueError, match="does not exist"):
        await provider.start(SandboxSpec(image=str(sif), mounts=[Mount("/nonexistent", "/data")]))
    with pytest.raises(ValueError, match="environment variable name"):
        await provider.start(SandboxSpec(image=str(sif), env={"BAD-NAME": "x"}))
    with pytest.raises(ValueError, match="absolute"):
        await provider.start(SandboxSpec(image=str(sif), workdir="app"))


async def test_mounts_are_bound_with_targets_created(provider, sif, tmp_path, fake_runtime):
    data = tmp_path / "data"
    data.mkdir()
    (data / "input.txt").write_text("mounted\n")
    sandbox = await provider.start(
        SandboxSpec(image=str(sif), mounts=[Mount(str(data), "/mnt/data", read_only=True)])
    )
    (start,) = fake_runtime.calls("instance", "start")
    assert f"{data}:/mnt/data:ro" in option_values(start, "--bind")
    assert (sandbox.session_dir / "rootfs" / "mnt" / "data").is_dir()
    assert (await sandbox.exec(["cat", "/mnt/data/input.txt"])).stdout == "mounted\n"


async def test_optional_runtime_flags(tmp_path, sif, fake_runtime):
    provider = make_provider(tmp_path, allow_network_none=True, gpu="nv", cgroup_limits=True)
    assert provider.capabilities == {"snapshot", "isolation", "network_none", "gpu"}
    sandbox = await provider.start(
        SandboxSpec(image=str(sif), network="none", cpus=2, memory_mb=512)
    )
    try:
        (start,) = fake_runtime.calls("instance", "start")
        assert start[start.index("--net") : start.index("--net") + 3] == [
            "--net",
            "--network",
            "none",
        ]
        assert "--nv" in start
        assert option_values(start, "--cpus") == ["2"]
        assert option_values(start, "--memory") == [str(512 * 1024 * 1024)]
        assert not any(b.endswith(":/etc/resolv.conf:ro") for b in option_values(start, "--bind"))
    finally:
        await sandbox.stop()


@pytest.mark.skipif(not Path(PYTHON).exists(), reason=f"needs {PYTHON}")
async def test_memory_watchdog_kills_the_largest_process(provider, sif):
    sandbox = await provider.start(SandboxSpec(image=str(sif), memory_mb=100))
    hog = "import time; data = bytearray(300 * 1024 * 1024); time.sleep(30)"
    result = await sandbox.exec([PYTHON, "-c", hog], timeout_s=20)
    assert result.exit_code == 137
    assert "memory limit exceeded" in result.stderr
    assert sandbox.memory_kills and sandbox.memory_kills[0].pss_bytes > 100 * 1024 * 1024
    assert (await sandbox.exec("echo survived")).stdout == "survived\n"


async def test_reap_stops_only_matching_sandboxes(provider, sif):
    first = await provider.start(SandboxSpec(image=str(sif), labels={"rollout_id": "a"}))
    second = await provider.start(SandboxSpec(image=str(sif), labels={"rollout_id": "b"}))
    report = await provider.reap({"rollout_id": "a"})
    assert report.stopped == [first.id] and not report.errors
    assert provider.sandboxes == {second}
    assert (await second.exec("echo b")).stdout == "b\n"


async def test_images_are_resolved_and_pulled_once(provider, fake_runtime):
    spec = SandboxSpec(image="docker://example/task:1")
    first = await provider.start(spec)
    second = await provider.start(spec)
    assert len(fake_runtime.calls("pull")) == 1
    assert first.id != second.id


def test_provider_defaults_follow_the_pbs_job(monkeypatch, tmp_path, fake_runtime):
    monkeypatch.setenv("PBS_LOCALDIR", str(tmp_path / "local"))
    assert SingularityProvider().scratch_root == tmp_path / "local" / "hpc-sandbox"
    monkeypatch.delenv("PBS_LOCALDIR")
    monkeypatch.setenv("TMPDIR", str(tmp_path / "tmpdir"))
    assert SingularityProvider().scratch_root == tmp_path / "tmpdir" / f"hpc-sandbox-{os.getuid()}"


async def test_runtime_environment_drops_implicit_binds(provider, monkeypatch):
    monkeypatch.setenv("SINGULARITY_BIND", "/groups")
    monkeypatch.setenv("APPTAINERENV_SECRET", "x")
    monkeypatch.setenv("SINGULARITY_CONFIGDIR", "/elsewhere")
    env = provider.runtime.environment()
    assert "SINGULARITY_BIND" not in env and "APPTAINERENV_SECRET" not in env
    assert "SINGULARITY_CONFIGDIR" not in env
    assert env["SINGULARITY_TMPDIR"] == str(provider.scratch_root / "runtime-tmp")
    assert env["APPTAINER_CACHEDIR"] == str(provider.scratch_root / "runtime-cache")
