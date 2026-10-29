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
import threading
import time
from pathlib import Path

import pytest

from conftest import hide_from_path, make_gvisor_provider
from hpc_sandbox import (
    Mount,
    PodSpec,
    RunscRuntime,
    Sandbox,
    SandboxError,
    SandboxLostError,
    SandboxProvider,
    SandboxSpec,
    SandboxStartError,
)
from hpc_sandbox.anchor import cpu_list
from hpc_sandbox.cli import _janitor_runtimes, _parser, resolve_runtime
from hpc_sandbox.cli import main as cli_main
from hpc_sandbox.gvisor import (
    CAPABILITIES,
    KEEPALIVE,
    MOUNT_POINTS,
    SNAPSHOT_FORMAT,
    TRANSFER_TARGET,
    UPPER_ANNOTATION,
    SentryMemory,
    command_groups,
    plan_binds,
    resolve_in_root,
    runsc_processes,
)
from hpc_sandbox.process import run_process
from hpc_sandbox.procfs import owned_live_start_time
from hpc_sandbox.sessions import open_session
from hpc_sandbox.templates import TEMPLATE_USERS, _remove_extractions

SRC = str(Path(__file__).resolve().parents[1] / "src")
ANCHOR = str(Path(SRC) / "hpc_sandbox" / "anchor.py")


def global_options(call: dict) -> list[str]:
    """runsc's options before the subcommand of a recorded call."""
    options = []
    for argument in call["argv"]:
        if not argument.startswith("-"):
            break
        options.append(argument)
    return options


async def gone(pid: int, start_time: str | None = None) -> bool:
    for _ in range(200):
        current = owned_live_start_time(pid)
        if current is None or (start_time is not None and current != start_time):
            return True
        await asyncio.sleep(0.05)
    return False


def interfaces(proc_net_dev: str) -> list[str]:
    return [line.split(":", 1)[0].strip() for line in proc_net_dev.splitlines()[2:] if ":" in line]


def lower_of(sandbox) -> Path:
    return Path(json.loads((sandbox.session_dir / "config.json").read_text())["root"]["path"])


def tree(root: Path) -> list[tuple[str, int, int]]:
    """Every path below ``root`` with its mode and size, but the provider's mount points, which
    starts add as binds need them."""
    points = root / ".hpc-sandbox" / "mounts"
    return sorted(
        (str(path.relative_to(root)), path.lstat().st_mode, path.lstat().st_size)
        for path in root.rglob("*")
        if points not in (path, path.parent)
    )


def binds(sandbox) -> dict[str, dict]:
    """The bundle's binds, by source."""
    bundle = json.loads((sandbox.session_dir / "config.json").read_text())
    return {mount["source"]: mount for mount in bundle["mounts"] if mount["type"] == "bind"}


@pytest.fixture
def image(tmp_path, fake_runtime):
    files = {
        "app/hello.txt": "hello\n",
        "opt/tool/README": "the image's\n",
        "etc/app.conf": "the image's\n",
        ".singularity.d/env/90-environment.sh": "export FROM_IMAGE=image-value\n",
        ".hpc-sandbox/planted": "the image's own\n",
    }
    return fake_runtime.make_sif(tmp_path / "images" / "task.sif", files)


async def test_provider_and_sandbox_satisfy_the_protocols(gvisor_provider, image):
    assert isinstance(gvisor_provider, SandboxProvider)
    assert gvisor_provider.name == "gvisor"
    assert gvisor_provider.capabilities == {"snapshot", "isolation", "network_none"}
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image)))
    assert isinstance(sandbox, Sandbox)


async def test_runsc_runs_rootless_with_a_state_directory_of_the_session(
    gvisor_provider, image, fake_runsc, monkeypatch
):
    monkeypatch.setenv("HPCS_CANARY", "1")
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image), labels={"run": "r1"}))
    session = sandbox.session_dir
    assert session is not None and session.parent == gvisor_provider.session_root
    (run,) = fake_runsc.calls("run")
    assert global_options(run) == [
        "--rootless",
        f"--root={session / 'runsc'}",
        "--platform=systrap",
        "--network=host",
        "--signal-unkillable-policy=linux",
        f"--overlay2=root:dir={session / 'overlay'}",
        "--allow-suid",
    ]
    assert run["argv"][-4:] == ["run", "--bundle", str(session), sandbox.id]
    for call in fake_runsc.calls():
        # The launcher's Python sets LC_CTYPE when it coerces the C locale; the wrapper's shell
        # sets the others.
        allowed = {"PATH", "LC_CTYPE", "FAKE_RUNSC_STATE", "PWD", "OLDPWD", "SHLVL", "_"}
        assert set(call["env"]) <= allowed, call
    for name in ("runsc", "overlay", "exec"):
        assert stat.S_IMODE((session / name).stat().st_mode) == 0o700
    owner = json.loads((session / "owner.json").read_text())
    assert owner["provider"] == "gvisor" and owner["labels"] == {"run": "r1"}
    assert (owner["remap_root"], owner["container_uid"]) == (True, 0)
    for role in ("launcher", "sentry"):
        assert owned_live_start_time(owner[f"{role}_pid"]) == owner[f"{role}_start_time"]
    assert sandbox.id in (Path(f"/proc/{owner['sentry_pid']}/cmdline").read_text())


async def test_the_bundle_runs_the_template_as_root_with_every_capability(gvisor_provider, image):
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image)))
    session = sandbox.session_dir
    spec = json.loads((session / "config.json").read_text())
    templates = gvisor_provider.scratch_root / "templates-gvisor"
    lower = Path(spec["root"]["path"])
    assert lower.name == "rootfs" and lower.parent.parent == templates
    assert spec["root"]["readonly"] is False  # gVisor's overlay keeps what the sandbox writes
    process = spec["process"]
    assert process["user"] == {"uid": 0, "gid": 0} and process["terminal"] is False
    assert process["args"] == ["/bin/sh", "-c", KEEPALIVE, sandbox.id]
    for kind in ("bounding", "effective", "permitted"):
        assert process["capabilities"][kind] == list(CAPABILITIES)
    assert "CAP_SYS_ADMIN" in CAPABILITIES and len(CAPABILITIES) == 41
    mounts = {mount["destination"]: mount for mount in spec["mounts"]}
    transfer = mounts[TRANSFER_TARGET]
    assert transfer["source"] == str(session / "tmp" / ".hpc-sandbox-transfer")
    assert transfer["options"] == ["rbind", "rw"]
    resolv_conf = Path("/etc/resolv.conf")
    if resolv_conf.is_file():  # the image has no /etc/resolv.conf: a mount point of the provider's
        bind = binds(sandbox)[str(resolv_conf.resolve())]
        assert (bind["destination"], bind["options"]) == (f"{MOUNT_POINTS}/f0", ["rbind", "ro"])
        seen = (await sandbox.exec("cat /etc/resolv.conf")).stdout
        assert seen == resolv_conf.read_text(errors="replace")
    assert "/tmp" not in mounts and "/var/tmp" not in mounts  # in the overlay, private
    # The provider's directory replaced the image's, and /tmp is not empty, else gVisor would
    # mount a tmpfs over it, out of the overlay.
    provider_dir = sorted(path.name for path in (lower / ".hpc-sandbox").iterdir())
    assert provider_dir == ["mounts", "transfer"]
    assert [path.name for path in (lower / "tmp").iterdir()] == [".hpc-sandbox"]
    assert "network" not in {namespace["type"] for namespace in spec["linux"]["namespaces"]}
    assert "annotations" not in spec


async def test_commands_run_as_root_with_the_images_environment(gvisor_provider, image):
    value = "two words; $(echo injected) 'quoted'"
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image), env={"GREETING": value}))
    result = await sandbox.exec(
        'id -u; printf "%s\\n" "$GREETING"; cat hello.txt; pwd; echo "$HOME"; echo "$FROM_IMAGE"'
    )
    assert result.exit_code == 0, result.stderr
    assert result.stdout.splitlines() == ["0", value, "hello", "/app", "/root", "image-value"]
    assert (await sandbox.exec(["cat"], stdin=b"from stdin")).stdout == "from stdin"
    result = await sandbox.exec("echo $EXTRA; exit 3", env={"EXTRA": "x"}, cwd="/tmp")
    assert (result.exit_code, result.stdout) == (3, "x\n")
    assert (await sandbox.exec("cat; echo done")).stdout == "done\n"  # stdin is /dev/null


async def test_network_none_is_a_network_namespace_with_loopback(
    gvisor_provider, image, fake_runsc
):
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image), network="none"))
    spec = json.loads((sandbox.session_dir / "config.json").read_text())
    assert {"type": "network"} in spec["linux"]["namespaces"]
    assert "/etc/resolv.conf" not in {mount["destination"] for mount in spec["mounts"]}
    (run,) = fake_runsc.calls("run")
    assert "--network=none" in global_options(run)
    assert interfaces((await sandbox.exec("cat /proc/net/dev")).stdout) == ["lo"]


async def test_options_become_runsc_options_and_mounts(tmp_path, image, fake_runsc):
    provider = make_gvisor_provider(
        tmp_path,
        platform="ptrace",
        host_uds="open",
        net_raw=True,
        guest_cgroups="v2",
        bind_resolv_conf=False,
        overlay_size_mb=64,
        extra_start_args=["--debug"],
    )
    data = tmp_path / "data"
    data.mkdir()
    (data / "in.txt").write_text("host data\n")
    spec = SandboxSpec(
        image=str(image),
        mounts=[Mount(str(data), "/srv/data", read_only=True), Mount(str(tmp_path), "/out", False)],
    )
    sandbox = await provider.start(spec)
    try:
        (run,) = fake_runsc.calls("run")
        options = global_options(run)
        assert "--platform=ptrace" in options and "--host-uds=open" in options
        assert "--net-raw" in options and "--allow-packet-socket-write" in options
        assert "--in-sandbox-cgroup=v2" in options and options[-1] == "--debug"
        overlay = f"--overlay2=root:dir={sandbox.session_dir / 'overlay'},size=64m"
        assert overlay in options
        (command,) = fake_runsc.calls("exec")[:1]
        assert "--debug" not in global_options(command)  # extra_start_args are run's alone
        bundle = json.loads((sandbox.session_dir / "config.json").read_text())
        mounts = {mount["destination"]: mount for mount in bundle["mounts"]}
        assert mounts["/sys/fs/cgroup"]["type"] == "cgroup"
        # Neither target is in the image: runsc binds them at the provider's mount points.
        by_source = binds(sandbox)
        assert by_source[str(data)]["destination"] == f"{MOUNT_POINTS}/d0"
        assert by_source[str(data)]["options"] == ["rbind", "ro"]
        assert by_source[str(tmp_path)]["destination"] == f"{MOUNT_POINTS}/d1"
        assert by_source[str(tmp_path)]["options"] == ["rbind", "rw"]
        assert "/etc/resolv.conf" not in mounts and len(by_source) == 3  # with the transfer's
        assert (await sandbox.exec("cat /srv/data/in.txt")).stdout == "host data\n"
        assert (await sandbox.exec("touch /srv/data/x")).exit_code != 0
        assert (await sandbox.exec("echo out > /out/written")).exit_code == 0
        assert (tmp_path / "written").read_text() == "out\n"
    finally:
        await sandbox.stop()


async def test_options_are_checked(tmp_path, image, fake_runsc):
    wrong = (
        {"platform": "vm"},
        {"host_uds": "yes"},
        {"guest_cgroups": "v3"},
        {"overlay_size_mb": 0},
        {"overlay_size_mb": 1.5},
        {"overlay_size_mb": True},
    )
    for options in wrong:
        with pytest.raises(ValueError):
            make_gvisor_provider(tmp_path, **options)
    with pytest.raises(ValueError, match="squashfs image; the gvisor provider runs SIF"):
        make_gvisor_provider(tmp_path).images.format.check(str(tmp_path / "task.sqsh"))
    provider = make_gvisor_provider(tmp_path)
    for target in ("/.hpc-sandbox", "/.hpc-sandbox/transfer/x"):
        spec = SandboxSpec(image=str(image), mounts=[Mount(str(tmp_path), target)])
        with pytest.raises(ValueError, match="reserved for hpc-sandbox"):
            await provider.start(spec)
    for target in ("//.hpc-sandbox/transfer", "//tmp/x"):  # the transfer directory, and /tmp
        spec = SandboxSpec(image=str(image), mounts=[Mount(str(tmp_path), target)])
        with pytest.raises(ValueError, match="invalid mount target"):
            await provider.start(spec)
    assert not fake_runsc.calls("run")


async def test_a_scratch_root_that_runsc_would_misread_fails_starts_not_janitors(
    tmp_path, image, fake_runsc
):
    provider = make_gvisor_provider(tmp_path, scratch_root=tmp_path / "a:b")
    report = await provider.reap()  # what hpc-sandbox janitor does for every user
    assert not report.errors, report
    with pytest.raises(ValueError, match="cannot contain ',' or ':'"):
        await provider.start(SandboxSpec(image=str(image)))
    assert not fake_runsc.calls("run")


async def test_runsc_is_never_downloaded(tmp_path, image, fake_runsc, monkeypatch):
    assert RunscRuntime().path.endswith("/runsc")
    with pytest.raises(SandboxError, match="'.*no-runsc' is not an executable file$"):
        _ = RunscRuntime(str(tmp_path / "no-runsc")).path
    with pytest.raises(SandboxError, match="'no-runsc' is not an executable on PATH$"):
        _ = RunscRuntime("no-runsc").path
    hide_from_path(monkeypatch, "runsc")
    provider = make_gvisor_provider(tmp_path)
    with pytest.raises(SandboxStartError, match="runsc"):
        await provider.start(SandboxSpec(image=str(image)))
    # Found missing before anything was extracted or created.
    assert not (provider.scratch_root / "templates-gvisor").exists()
    assert not provider.session_root.exists() or not any(provider.session_root.glob("hpcsb-*"))


async def test_the_template_is_shared_and_never_written(gvisor_provider, image, tmp_path):
    first = await gvisor_provider.start(SandboxSpec(image=str(image)))
    second = await gvisor_provider.start(SandboxSpec(image=str(image)))
    assert lower_of(first) == lower_of(second)
    lower = lower_of(first)
    before = tree(lower)
    assert (await first.exec("echo mine > /app/hello.txt && mkdir -p /etc/new")).exit_code == 0
    assert (await second.exec("cat hello.txt")).stdout == "hello\n"
    assert tree(lower) == before
    assert (lower / "app" / "hello.txt").read_text() == "hello\n"


async def test_binds_never_make_their_mount_points_in_the_template(
    gvisor_provider, image, tmp_path
):
    """runsc's gofer would create a missing mount point in the template (the fake does too), where
    every later sandbox would find it: binds whose targets the image lacks go through the
    provider's own mount points instead, and are bound at their targets inside the sandbox."""
    first = await gvisor_provider.start(SandboxSpec(image=str(image)))
    lower = lower_of(first)
    (lower / "etc" / "run").symlink_to("../opt")  # resolved inside the template, as runsc does
    before = tree(lower)
    data = tmp_path / "data"
    data.mkdir()
    (data / "in.txt").write_text("data\n")
    config = tmp_path / "config.yaml"
    config.write_text("key: value\n")
    mounts = [
        Mount(str(data), "/srv/new/data"),  # missing: a mount point of the provider's
        Mount(str(config), "/etc/newapp/config.yaml"),  # a missing file
        Mount(str(data), "/opt/tool", read_only=False),  # in the image: bound there
        Mount(str(config), "/etc/app.conf"),  # a file in the image
        Mount(str(data), "/etc/run/tool/sub"),  # inside the bind above, through a symlink
        Mount(str(data), "/etc/run/other"),  # missing, through a symlink
    ]
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image), mounts=mounts))
    by_target = {mount.target: mount for mount in mounts}
    destinations = {
        mount["destination"]
        for mount in json.loads((sandbox.session_dir / "config.json").read_text())["mounts"]
    }
    assert {"/opt/tool", "/etc/app.conf"} <= destinations
    assert not {"/srv/new/data", "/etc/newapp/config.yaml", "/etc/run/other"} & destinations
    seen = await sandbox.exec(
        "cat /srv/new/data/in.txt /etc/newapp/config.yaml /opt/tool/in.txt /etc/app.conf "
        "/etc/run/tool/sub/in.txt /etc/run/other/in.txt"
    )
    assert seen.exit_code == 0, seen.stderr
    assert seen.stdout == "data\nkey: value\ndata\nkey: value\ndata\ndata\n"
    assert (await sandbox.exec("touch /srv/new/data/x")).exit_code != 0  # read-only
    assert (await sandbox.exec("echo rw > /opt/tool/written")).exit_code == 0
    assert by_target["/opt/tool"].read_only is False and (data / "written").is_file()
    assert sorted(path.name for path in data.iterdir()) == ["in.txt", "sub", "written"]
    assert tree(lower) == before  # nothing was made in the template
    points = sorted(path.name for path in (lower / ".hpc-sandbox" / "mounts").iterdir())
    assert "d2" in points and set(points) <= {"d0", "d1", "d2", "f0", "f1"}
    # A sandbox without the binds finds none of their mount points.
    plain = await gvisor_provider.start(SandboxSpec(image=str(image)))
    missing = "ls -d /srv/new /etc/newapp /opt/other 2>/dev/null; echo end"
    assert (await plain.exec(missing)).stdout == "end\n"
    assert (await plain.exec("cat /opt/tool/README /etc/app.conf")).stdout == "the image's\n" * 2


def test_plan_binds_resolves_targets_as_runsc_does(tmp_path):
    root = tmp_path / "rootfs"
    for directory in ("etc", "opt/tool", "var", ".hpc-sandbox/transfer"):
        (root / directory).mkdir(parents=True)
    (root / "etc" / "hosts").write_text("")
    (root / "var" / "run").symlink_to("/opt")
    (root / "etc" / "loop").symlink_to("loop")
    (root / "escape").symlink_to("../../..")
    assert resolve_in_root(str(root), "/var/run/tool") == str(root / "opt" / "tool")
    assert resolve_in_root(str(root), "/../../etc/hosts") == str(root / "etc" / "hosts")
    assert resolve_in_root(str(root), "/escape/etc") == str(root / "etc")
    with pytest.raises(ValueError):
        resolve_in_root(str(root), "/etc/loop/x")
    source = tmp_path / "source"
    source.mkdir()
    file = tmp_path / "file"
    file.write_text("")
    planned = plan_binds(
        root,
        [
            (source, "/var/run/tool", False),  # a directory through a symlink: there
            (source, "/opt/tool/inner", True),  # inside the bind above: a mount point
            (file, "/etc/hosts", True),  # a file: there
            (file, "/etc", True),  # a file over a directory: a mount point
            (source, "/etc/hosts", True),  # a directory over a file: a mount point
            (source, "/etc/loop", True),  # unresolvable: a mount point
            (source, "/escape", True),  # resolves to the root, over the provider's directory
            (file, "/etc/new.conf", True),
        ],
    )
    assert [bind.point for bind in planned] == [
        None,
        f"{MOUNT_POINTS}/d0",
        None,
        f"{MOUNT_POINTS}/f0",
        f"{MOUNT_POINTS}/d1",
        f"{MOUNT_POINTS}/d2",
        f"{MOUNT_POINTS}/d3",
        f"{MOUNT_POINTS}/f1",
    ]


async def test_transfers_round_trip_through_the_transfer_directory(
    gvisor_provider, image, tmp_path
):
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image)))
    tree = tmp_path / "tree"
    (tree / "nested").mkdir(parents=True)
    (tree / "nested" / "data.bin").write_bytes(bytes(range(256)) * 100)
    await sandbox.upload(tree, "/app/tree")
    assert (await sandbox.exec("ls tree/nested")).stdout == "data.bin\n"
    await sandbox.download("/app/tree", tmp_path / "back")
    assert (tmp_path / "back" / "nested" / "data.bin").read_bytes() == bytes(range(256)) * 100
    transfer = sandbox.session_dir / "tmp" / ".hpc-sandbox-transfer"
    assert transfer.is_dir() and not any(transfer.iterdir())
    # Root may mount over /tmp: the transfer directory is not there.
    mounted = await sandbox.exec("mount -t tmpfs tmpfs /tmp && echo mounted")
    assert mounted.stdout == "mounted\n", mounted.stderr
    await sandbox.upload(tree / "nested" / "data.bin", "/tmp/again.bin")
    assert (await sandbox.exec("wc -c < /tmp/again.bin")).stdout.strip() == "25600"


async def test_stop_kills_runsc_and_everything_it_started(gvisor_provider, image):
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image)))
    session = sandbox.session_dir
    owner = json.loads((session / "owner.json").read_text())
    state = session / "runsc"
    running = runsc_processes(state)
    assert {owner["sentry_pid"], owner["launcher_pid"]} <= {pid for pid, _ in running}
    assert len(running) == 4  # runsc, its /proc/self/exe, the gofer and the Sentry
    await sandbox.stop()
    await sandbox.stop()  # idempotent
    for pid, started in running:
        assert await gone(pid, started)
    assert await gvisor_provider.wait_for_deletions(30)
    assert not session.exists()
    with pytest.raises(SandboxError, match="not running"):
        await sandbox.exec("true")


async def test_stop_waits_for_a_sentry_that_outlives_its_launcher(
    gvisor_provider, image, fake_runsc
):
    """A Sentry that held much memory takes a while to go, its arguments gone: stop() waits for
    the recorded process, instead of failing at once."""
    fake_runsc.set("slow-exit")
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image)))
    owner = json.loads((sandbox.session_dir / "owner.json").read_text())
    await sandbox.stop()
    assert owned_live_start_time(owner["sentry_pid"]) != owner["sentry_start_time"]
    assert await gvisor_provider.wait_for_deletions(30)


def test_sandboxes_outlive_the_thread_that_started_them(tmp_path, image, fake_runsc):
    """A launcher dies with the thread that forked it: launchers are forked from one thread that
    lives as long as the process, not from a caller's that ends."""
    provider = make_gvisor_provider(tmp_path)
    started = []
    thread = threading.Thread(
        target=lambda: started.append(asyncio.run(provider.start(SandboxSpec(image=str(image)))))
    )
    thread.start()
    thread.join()
    (sandbox,) = started
    owner = json.loads((sandbox.session_dir / "owner.json").read_text())
    time.sleep(0.5)
    for role in ("launcher", "sentry"):
        assert owned_live_start_time(owner[f"{role}_pid"]) == owner[f"{role}_start_time"], role

    async def stop() -> None:
        assert (await sandbox.exec("echo alive")).stdout == "alive\n"
        await sandbox.stop()
        assert await provider.wait_for_deletions(30)

    asyncio.run(stop())


async def test_timeouts_kill_the_command_inside_the_sandbox(gvisor_provider, image, fake_runsc):
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image)))
    result = await sandbox.exec(
        "sleep 300 & setsid sleep 301 & echo started; sleep 302", timeout_s=1
    )
    assert result.timed_out and result.exit_code == 124 and result.stdout == "started\n"
    kills = [call["argv"] for call in fake_runsc.calls("kill")]
    assert kills and all(argv[-1] == "KILL" and "--pgid=" in " ".join(argv) for argv in kills)
    assert len(kills) == 2  # the command's group, and the one setsid made
    listing = (await sandbox.exec("ps -o args 2>/dev/null || ps")).stdout
    assert "sleep 30" not in listing, listing
    assert not list((sandbox.session_dir / "exec").iterdir())
    assert (await sandbox.exec("echo alive")).stdout == "alive\n"


async def test_a_timeout_kills_many_process_groups_at_once(gvisor_provider, image, fake_runsc):
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image)))
    fake_runsc.set_value("kill-delay", "1")
    detached = "for i in 1 2 3 4 5 6 7 8 9; do setsid sleep 300 & done; echo started; sleep 301"
    began = time.monotonic()
    result = await sandbox.exec(detached, timeout_s=1)
    elapsed = time.monotonic() - began
    assert result.timed_out and result.stdout == "started\n"
    assert len(fake_runsc.calls("kill")) == 10  # the command's group and nine setsid made
    assert elapsed < 1 + 5, elapsed  # not 10 kills of a second one after another
    listing = (await sandbox.exec("ps -o args 2>/dev/null || ps")).stdout
    assert "sleep 30" not in listing, listing


async def test_a_cancelled_command_is_killed_inside_the_sandbox(gvisor_provider, image, fake_runsc):
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image)))
    task = asyncio.create_task(sandbox.exec("echo $$ > /tmp/pid.txt; exec sleep 300"))
    while (await sandbox.exec("cat /tmp/pid.txt 2>/dev/null")).stdout == "":
        await asyncio.sleep(0.05)
    pid = int((await sandbox.exec("cat /tmp/pid.txt")).stdout)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert fake_runsc.calls("kill")
    probe = f"kill -0 {pid} 2>/dev/null && echo running || echo gone"
    for _ in range(100):
        if (await sandbox.exec(probe)).stdout == "gone\n":
            break
        await asyncio.sleep(0.05)
    assert (await sandbox.exec(probe)).stdout == "gone\n"


async def test_a_dead_sandbox_is_reported_as_lost(gvisor_provider, image):
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image)))
    owner = json.loads((sandbox.session_dir / "owner.json").read_text())
    command = asyncio.create_task(sandbox.exec("sleep 300"))
    await asyncio.sleep(0.5)
    os.kill(owner["sentry_pid"], signal.SIGKILL)
    with pytest.raises(SandboxLostError):  # though runsc exec may report success
        await command
    with pytest.raises(SandboxLostError):
        await sandbox.exec("true")
    await sandbox.stop()


async def test_the_memory_limit_stops_the_whole_sandbox(gvisor_provider, image):
    # The fake's Sentry alone, a Python interpreter, holds more than a MiB.
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image), memory_mb=1))
    owner = json.loads((sandbox.session_dir / "owner.json").read_text())
    for _ in range(100):
        if sandbox.memory_kills:
            break
        await asyncio.sleep(0.05)
    (kill,) = sandbox.memory_kills
    assert kill.pid == owner["sentry_pid"] and kill.limit_bytes == 1 << 20
    assert kill.exec_root is None and kill.pss_bytes > kill.limit_bytes
    assert await gone(owner["sentry_pid"])
    with pytest.raises(SandboxLostError, match="memory limit exceeded.*whole sandbox"):
        await sandbox.exec("true")


async def test_the_memory_limit_counts_memory_that_no_process_maps(
    gvisor_provider, image, fake_runsc
):
    """gVisor keeps a sandbox's memory in its memory file, and an idle process's memory is mapped
    by no host process: the watchdog counts the file's allocated size."""
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image), memory_mb=96))
    await asyncio.sleep(0.5)
    assert not sandbox.memory_kills  # the fake Sentry's own memory is below the limit
    fake_runsc.set_value("memory-file-mb", "128")
    for _ in range(100):
        if sandbox.memory_kills:
            break
        await asyncio.sleep(0.05)
    (kill,) = sandbox.memory_kills
    assert kill.total_bytes > 128 << 20 and "gVisor's memory" in kill.message()
    with pytest.raises(SandboxLostError, match="memory limit exceeded"):
        await sandbox.exec("true")


def test_sentry_memory_counts_each_memory_file_once():
    memory_file = os.memfd_create("runsc-memory")
    duplicate = os.dup(memory_file)
    try:
        held = SentryMemory(os.getpid(), owned_live_start_time(os.getpid()) or "")
        baseline = held.read()
        assert baseline is not None
        os.posix_fallocate(memory_file, 0, 64 << 20)  # allocated, mapped by no one
        for _ in range(SentryMemory._RESCAN + 1):  # whether it lists the descriptors again or not
            grown = held.read()
            assert grown is not None and 64 << 20 <= grown - baseline < 80 << 20
    finally:
        os.close(memory_file)
        os.close(duplicate)
    assert SentryMemory(os.getpid(), "0").read() is None  # not that process


async def test_snapshots_keep_the_upper_layer_over_the_same_template(
    gvisor_provider, image, fake_runsc
):
    spec = SandboxSpec(image=str(image))
    sandbox = await gvisor_provider.start(spec)
    assert (await sandbox.exec("echo v1 > /app/state && echo note > /tmp/note")).exit_code == 0
    snapshot = await sandbox.snapshot()
    lower = json.loads((sandbox.session_dir / "config.json").read_text())["root"]["path"]
    assert snapshot.metadata["format"] == SNAPSHOT_FORMAT and snapshot.metadata["lower"] == lower
    assert (Path(snapshot.path) / "rootfs-upper.tar").is_file()
    assert not (Path(snapshot.path) / "tmp" / ".hpc-sandbox-transfer").exists()
    await sandbox.exec("echo v2 > /app/state")
    restored = await gvisor_provider.restore(snapshot, spec)
    assert (await restored.exec("cat /app/state /tmp/note")).stdout == "v1\nnote\n"
    bundle = json.loads((restored.session_dir / "config.json").read_text())
    assert bundle["root"]["path"] == lower
    assert bundle["annotations"] == {
        UPPER_ANNOTATION: str(restored.session_dir / "rootfs-upper.tar")
    }
    first, second = fake_runsc.calls("run")
    assert "--allow-rootfs-tar-annotation" not in global_options(first)
    assert "--allow-rootfs-tar-annotation" in global_options(second)
    assert second["argv"][-1] == restored.id
    await gvisor_provider.delete_snapshot(snapshot)
    assert not Path(snapshot.path).exists()


async def test_restores_need_the_template_the_snapshot_lies_over(gvisor_provider, image, tmp_path):
    spec = SandboxSpec(image=str(image))
    sandbox = await gvisor_provider.start(spec)
    snapshot = await sandbox.snapshot()
    snapshot.metadata["lower"] = str(tmp_path)
    with pytest.raises(ValueError, match="not a template of this node"):
        await gvisor_provider.restore(snapshot, spec)
    other = SandboxSpec(image=str(image) + "x.sif")
    with pytest.raises(ValueError):
        await gvisor_provider.restore(snapshot, other)


async def test_sandboxes_die_with_their_owner_and_janitors_remove_them(tmp_path, image, fake_runsc):
    crash = (
        "import asyncio, os, signal, sys\n"
        "from hpc_sandbox import GvisorProvider, SandboxSpec\n"
        "async def main():\n"
        "    scratch, cache, image = sys.argv[1:]\n"
        "    provider = GvisorProvider(\n"
        "        scratch_root=scratch, image_cache=cache, stage_images=False\n"
        "    )\n"
        "    sandbox = await provider.start(SandboxSpec(image=image, labels={'run': 'a'}))\n"
        "    print(sandbox.id, sandbox._launcher.pid, sandbox._sentry_pid, flush=True)\n"
        "    os.kill(os.getpid(), signal.SIGKILL)\n"
        "asyncio.run(main())\n"
    )
    result = await run_process(
        [sys.executable, "-c", crash, str(tmp_path / "scratch"), str(tmp_path / "cache"), image],
        timeout_s=120,
        env={**os.environ, "PYTHONPATH": SRC},
    )
    assert result.returncode == 128 + signal.SIGKILL, result.detail()
    name, launcher, sentry = result.stdout.decode().split()
    assert await gone(int(launcher)) and await gone(int(sentry))
    provider = make_gvisor_provider(tmp_path)
    assert not runsc_processes(provider.session_root / name / "runsc")
    report = await provider.reap({"run": "a"})
    assert (report.stopped, report.removed, report.errors) == ([], [name], [])
    assert await provider.wait_for_deletions(30)
    assert not (provider.session_root / name).exists()


def abandoned_session(root: Path, name: str, **identity: object) -> Path:
    """A gVisor session whose owner is gone, recording ``identity`` as its processes."""
    session = open_session(root, name)
    session.write_owner_record(provider="gvisor", labels={}, image="x", **identity)
    record = json.loads((session.path / "owner.json").read_text())
    record["owner_start_time"] = "0"  # no live process started at boot time zero
    (session.path / "owner.json").write_text(json.dumps(record))
    session.release()
    return session.path


async def test_janitors_kill_only_what_names_the_sessions_state_directory(tmp_path, fake_runsc):
    provider = make_gvisor_provider(tmp_path)
    provider.session_root.mkdir(parents=True)
    name, other = "hpcsb-" + "1" * 32, "hpcsb-" + "2" * 32
    state = provider.session_root / name / "runsc"
    sleeper = "import time; time.sleep(300)"
    survivor = subprocess.Popen(
        [sys.executable, "-c", sleeper, f"--root={state}", name], start_new_session=True
    )
    stranger = subprocess.Popen(
        [sys.executable, "-c", sleeper, f"--root={state}x", other], start_new_session=True
    )
    try:
        abandoned_session(provider.session_root, name)
        abandoned_session(provider.session_root, other)
        report = await provider.reap()
        assert sorted(report.removed) == sorted([name, other]) and not report.errors, report
        assert report.stopped == [name]
        assert survivor.wait(timeout=10) == -signal.SIGKILL
        assert stranger.poll() is None
    finally:
        for process in (survivor, stranger):
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()
    assert await provider.wait_for_deletions(30)


async def test_janitors_keep_a_template_that_a_sandbox_still_uses(gvisor_provider, image):
    sandbox = await gvisor_provider.start(SandboxSpec(image=str(image)))
    lower = Path(json.loads((sandbox.session_dir / "config.json").read_text())["root"]["path"])
    template = lower.parent
    assert (template / TEMPLATE_USERS).is_file()
    aside = template.with_name(f".{template.name}.1-1.00000000.damaged")
    os.rename(template, aside)  # as a start does with a template that lost its marker
    assert _remove_extractions([aside]) == [] and aside.is_dir()
    assert (await sandbox.exec("cat hello.txt")).stdout == "hello\n"
    await sandbox.stop()
    assert _remove_extractions([aside]) == [] and not aside.exists()


async def test_cpus_are_chosen_among_all_your_sandboxes_on_the_node(tmp_path, image, fake_runsc):
    """Each provider stands for a process of its own: none knows the others' sandboxes, yet the
    CPUs they choose do not overlap."""
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 5:
        pytest.skip("needs 5 CPUs")
    providers = [
        make_gvisor_provider(tmp_path, scratch_root=tmp_path / f"scratch-{index}")
        for index in range(2)
    ]
    sandboxes = []
    try:
        for provider in providers:
            sandboxes.append(await provider.start(SandboxSpec(image=str(image), cpus=1)))
            sandboxes.append(await provider.start(SandboxSpec(image=str(image), cpus=1.5)))
        cpus = []
        for sandbox in sandboxes:
            owner = json.loads((sandbox.session_dir / "owner.json").read_text())
            launcher, sentry = owner["launcher_pid"], owner["sentry_pid"]
            assert os.sched_getaffinity(launcher) == os.sched_getaffinity(sentry)
            cpus.append(os.sched_getaffinity(sentry))
        assert [len(found) for found in cpus] == [1, 2, 1, 2]
        if len(allowed) >= 6:
            assert len(set().union(*cpus)) == 6, cpus  # no CPU twice
    finally:
        for sandbox in sandboxes:
            await sandbox.stop()
    assert providers[0]._choose_cpus(len(allowed)) is None


async def test_what_gvisor_does_not_do_is_refused(gvisor_provider, image, tmp_path):
    manifest = tmp_path / "owners"
    manifest.write_text("0 0 /app\n")
    with pytest.raises(ValueError, match="file owners"):
        await gvisor_provider.start(SandboxSpec(image=str(image), file_owners=str(manifest)))
    # It names the providers that run its services in their pods.
    with pytest.raises(ValueError, match=r"no pods of its own.*SingularityProvider\(gvisor="):
        await gvisor_provider.start_pod(PodSpec())


async def test_a_failed_start_leaves_nothing_behind(gvisor_provider, image, fake_runsc):
    fake_runsc.set("start-fails")
    with pytest.raises(SandboxStartError, match="simulated start failure"):
        await gvisor_provider.start(SandboxSpec(image=str(image)))
    assert await gvisor_provider.wait_for_deletions(30)
    assert not any(gvisor_provider.session_root.glob("hpcsb-*"))


def test_command_groups_follow_the_tree_of_a_command():
    listing = """\
UID       PID       PPID      PGID      C         TTY       STIME     TIME      CMD
0         1         0         1         0         ?         00:00     0s        sh
0         2         1         1         0         ?         00:00     0s        sleep
0         20        0         20        0         ?         00:00     0s        sh
0         21        20        20        0         ?         00:00     0s        sleep
0         22        21        30        0         ?         00:00     0s        daemon
0         23        22        30        0         ?         00:00     0s        child
0         24        0         24        0         ?         00:00     0s        other
"""
    assert command_groups(listing, 20) == [20, 30]
    assert command_groups(listing, 24) == [24]
    assert command_groups(listing, 99) == []
    # A name with a line break cannot forge a row, a PID listed twice counts for nothing, and the
    # keep-alive loop's group is never killed.
    forged = listing + (
        "0         25        24        25        0         ?         00:00     0s        x\n"
        "0 9 24 9 0\n"
        "0         26        24        26        0         ?         00:00     0s        y\n"
        "0         26        24        28        0         ?         00:00     0s        y\n"
        "0         27        24        1         0         ?         00:00     0s        z\n"
    )
    assert command_groups(forged, 24) == [24, 25]


def test_runsc_processes_name_exactly_one_state_directory(tmp_path):
    state = tmp_path / "hpcsb-x" / "runsc"
    sleeper = "import time; time.sleep(60)"
    processes = [
        subprocess.Popen([sys.executable, "-c", sleeper, f"--root={state}"]),
        subprocess.Popen([sys.executable, "-c", sleeper, "--root", str(state)]),
        subprocess.Popen([sys.executable, "-c", sleeper, f"--root={state}/more"]),
    ]
    try:
        for _ in range(100):
            found = {pid for pid, _ in runsc_processes(state)}
            if len(found) == 2:
                break
        assert found == {processes[0].pid, processes[1].pid}
    finally:
        for process in processes:
            process.kill()
            process.wait()


def test_the_launchers_exec_form_dies_with_its_parent_and_keeps_to_its_cpus(tmp_path):
    allowed = sorted(os.sched_getaffinity(0))
    cpu = allowed[-1]
    script = (
        "import os, subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-I', '-S', sys.argv[1], str(os.getpid()),\n"
        "    'exec', '--cpus', sys.argv[2], '--', 'sh', '-c', 'echo $$; grep Cpus_allowed_list"
        " /proc/self/status; exec sleep 300'], stdout=subprocess.PIPE, text=True)\n"
        "print(child.stdout.readline().strip(), child.stdout.readline().split()[-1], flush=True)\n"
        "os._exit(0)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, ANCHOR, str(cpu)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    pid, cpus = result.stdout.split()
    assert cpu_list(cpus) == {cpu}
    for _ in range(100):
        if owned_live_start_time(int(pid)) is None:
            break
        time.sleep(0.05)
    assert owned_live_start_time(int(pid)) is None  # SIGKILLed with its parent


def test_cpu_lists():
    assert cpu_list("0,2-4,7") == {0, 2, 3, 4, 7}
    for wrong in ("", "a", "1-", "-2", "1,,2"):
        with pytest.raises(ValueError):
            cpu_list(wrong)


def test_the_command_line_selects_gvisor_only_when_asked(monkeypatch):
    assert resolve_runtime("gvisor") == "gvisor"
    assert resolve_runtime("auto", "/opt/gvisor/runsc") == "gvisor"
    args = _parser().parse_args(["janitor"])
    args.runtime, args.binary = "auto", None
    assert _janitor_runtimes(args)[-2:] == ["enroot", "gvisor"]
    if shutil.which("singularity") is None and shutil.which("apptainer") is None:
        assert resolve_runtime("auto") != "gvisor"


async def test_hpc_sandbox_run_and_doctor_on_gvisor(tmp_path, image, fake_runsc, capsys):
    scratch = str(tmp_path / "scratch")
    common = ["--runtime", "gvisor", "--scratch-root", scratch, "--image-cache", str(tmp_path)]
    code = await asyncio.to_thread(
        cli_main, ["run", *common, "--network", "none", str(image), "--", "cat", "hello.txt"]
    )
    out = capsys.readouterr()
    assert code == 0, out.err
    assert out.out == "hello\n"
    code = await asyncio.to_thread(cli_main, ["doctor", *common, "--image", str(image)])
    out = capsys.readouterr().out
    assert code == 0, out
    for line in ("runtime:", "images:", "user namespaces:", "rootless runsc:", "sandbox stop"):
        assert line in out, out
    assert "runsc do true on systrap" in out and "pods:" not in out
    assert ["--network=none", "do", "true"] == fake_runsc.calls("do")[0]["argv"][-3:]
    code = await asyncio.to_thread(
        cli_main, ["janitor", "--scratch-root", str(tmp_path / "with:colon")]
    )
    assert code == 0, capsys.readouterr()
