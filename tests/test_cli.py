from __future__ import annotations

import json
import os
import shutil
import stat
from pathlib import Path

import pytest

from conftest import FakeSubids, hide_from_path
from hpc_sandbox import SandboxSpec, subids
from hpc_sandbox.cli import main, resolve_runtime


def common(tmp_path: Path) -> list[str]:
    return ["--scratch-root", str(tmp_path / "scratch"), "--image-cache", str(tmp_path / "cache")]


def test_run_prints_the_output_and_returns_the_exit_code(tmp_path, sif, fake_runtime, capsys):
    code = main(
        [
            "run",
            str(sif),
            *common(tmp_path),
            "--env",
            "WORDS=two words",
            "--label",
            "job=cli",
            "--",
            "/bin/sh",
            "-c",
            'echo "$WORDS"; echo oops >&2; exit 4',
        ]
    )
    captured = capsys.readouterr()
    assert code == 4
    assert captured.out == "two words\n" and "oops" in captured.err
    assert not list((tmp_path / "scratch" / "sessions").glob("hpcsb-*"))


def test_run_needs_a_command(tmp_path, sif, fake_runtime, capsys):
    assert main(["run", str(sif), *common(tmp_path), "--"]) == 2
    assert "give the command" in capsys.readouterr().err


def test_doctor_checks_the_site_and_smoke_tests_an_image(tmp_path, sif, fake_runtime, capsys):
    config = fake_runtime.state / "etc" / "singularity.conf"
    config.parent.mkdir()
    config.write_text("# site\nallow setuid = no\nsessiondir max size = 64\nother = 1\n")
    assert main(["doctor", *common(tmp_path), "--image", str(sif)]) == 0
    output = capsys.readouterr().out
    assert "singularity-ce version 4.4.1-fake" in output
    assert "[warn] site: sessiondir max size: 64 (never use --writable-tmpfs here)" in output
    # User-namespace mode, which runs sandboxes as well as setuid mode does.
    assert "[info] site: allow setuid: no\n" in output and "other" not in output
    for check in ("sandbox start", "exec", "upload/download", "sandbox stop"):
        assert f"[  ok] {check}" in output


def test_doctor_reports_the_subordinate_ids_that_pods_would_map(
    tmp_path, fake_runtime, fake_subids, capsys, monkeypatch
):
    name = "pods: subordinate ids"
    refused = "pods with users='subids' cannot start here"
    assert main(["doctor", *common(tmp_path)]) == 0
    first, last = FakeSubids.START, FakeSubids.START + FakeSubids.COUNT - 1
    tools = f"{fake_subids.directory / 'newuidmap'}, {fake_subids.directory / 'newgidmap'}"
    expected = f"[  ok] {name}: uids {first}-{last}, gids {first}-{last}; {tools}\n"
    assert expected in capsys.readouterr().out
    hide_from_path(monkeypatch, "newgidmap")  # the site may install it: a warning
    assert main(["doctor", *common(tmp_path)]) == 0
    assert f"[warn] {name}: {refused}: no newgidmap" in capsys.readouterr().out
    monkeypatch.setattr(subids, "SUBUID", tmp_path / "none")  # only the site gives ranges: info
    assert main(["doctor", *common(tmp_path)]) == 0
    assert f"[info] {name}: {refused}: no range of subordinate ids" in capsys.readouterr().out


def test_doctor_fails_without_a_runtime(tmp_path, capsys):
    assert main(["doctor", *common(tmp_path), "--binary", str(tmp_path / "missing")]) == 1
    assert "[FAIL] runtime" in capsys.readouterr().out


def test_janitor_prints_a_json_report(tmp_path, fake_runtime, capsys):
    assert main(["janitor", *common(tmp_path), "--label", "rollout_id=x"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report == {"stopped": [], "removed": [], "live": [], "errors": []}


def test_run_uses_enroot_when_asked(tmp_path, sqsh, fake_enroot, capsys):
    command = ["/bin/sh", "-c", "id -u; exit 5"]
    assert main(["run", str(sqsh), "--runtime", "enroot", *common(tmp_path), "--", *command]) == 5
    assert capsys.readouterr().out == "0\n"
    assert fake_enroot.calls("start")


def test_doctor_checks_enroot_and_smoke_tests_an_image(
    tmp_path, sqsh, fake_enroot, capsys, monkeypatch
):
    hide_from_path(monkeypatch, "slirp4netns")  # the host's
    monkeypatch.setenv("HPC_SANDBOX_TOOLS", str(tmp_path / "no-tools"))  # and the pinned one
    arguments = ["doctor", "--runtime", "enroot", *common(tmp_path), "--image", str(sqsh)]
    assert main(arguments) == 0  # pods without egress are no failure
    output = capsys.readouterr().out
    assert "3.5.0-fake" in output and "[  ok] namespaces" in output
    assert "[warn] pods: egress: pods with egress need slirp4netns" in output
    assert "pods: subordinate ids" not in output  # enroot's pods map none
    for check in ("sandbox start", "exec", "upload/download", "sandbox stop"):
        assert f"[  ok] {check}" in output


def whiteout_converter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Put first on PATH an enroot-aufs2ovlfs that converts whiteouts, whatever the host has."""
    directory = tmp_path / "converter"
    directory.mkdir()
    converter = directory / "enroot-aufs2ovlfs"
    converter.write_text('#!/bin/sh\ncd "$1" && rm opaque/.wh..wh..opq && mv .wh.deleted deleted\n')
    converter.chmod(0o755)
    monkeypatch.setenv("PATH", f"{directory}{os.pathsep}{os.environ['PATH']}")
    return converter


def test_doctor_creates_a_private_scratch_root_before_the_enroot_checks(
    tmp_path, fake_enroot, capsys, monkeypatch
):
    converter = whiteout_converter(tmp_path, monkeypatch)  # its check writes in the scratch root
    scratch = tmp_path / "fresh" / "scratch"
    arguments = ["doctor", "--runtime", "enroot", "--scratch-root", str(scratch)]
    previous = os.umask(0o022)  # directories created without a mode would be readable by all
    try:
        assert main([*arguments, "--image-cache", str(tmp_path / "cache")]) == 0
    finally:
        os.umask(previous)
    output = capsys.readouterr().out
    assert f"[  ok] image imports: {converter} converts whiteouts" in output
    assert f"[  ok] scratch root: {scratch} (" in output
    assert stat.S_IMODE(scratch.stat().st_mode) == 0o700


def test_doctor_reports_directories_it_cannot_write_as_failures(
    tmp_path, fake_enroot, capsys, monkeypatch
):
    converter = whiteout_converter(tmp_path, monkeypatch)
    scratch, cache = tmp_path / "scratch", tmp_path / "cache"
    for directory in (scratch, cache):
        directory.mkdir()
        directory.chmod(0o500)
    try:
        assert main(["doctor", "--runtime", "enroot", *common(tmp_path)]) == 1
    finally:
        for directory in (scratch, cache):
            directory.chmod(0o700)
    output = capsys.readouterr().out
    assert f"[warn] image imports: could not test whether {converter} converts whiteouts" in output
    assert f"[FAIL] scratch root: {scratch} is not writable: " in output
    assert f"[FAIL] image cache: {cache} is not writable: " in output
    assert output.count("[FAIL]") == 2 and "pods: egress" in output  # the other checks ran


def test_doctor_reports_a_start_its_scratch_root_prevents_as_a_failure(
    tmp_path, sqsh, fake_enroot, capsys, monkeypatch
):
    whiteout_converter(tmp_path, monkeypatch)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    scratch.chmod(0o500)
    try:
        arguments = ["doctor", "--runtime", "enroot", *common(tmp_path), "--image", str(sqsh)]
        assert main(arguments) == 1
    finally:
        scratch.chmod(0o700)
    output = capsys.readouterr().out
    assert f"[FAIL] scratch root: {scratch} is not writable: " in output
    assert "[FAIL] sandbox start: " in output


def test_runtime_resolution(monkeypatch):
    assert resolve_runtime("enroot") == "enroot"
    assert resolve_runtime("auto", "/opt/enroot/bin/enroot") == "enroot"
    assert resolve_runtime("auto", "apptainer") == "singularity"
    monkeypatch.setattr(shutil, "which", lambda name, *_: "/bin/x" if name == "enroot" else None)
    assert resolve_runtime("auto") == "enroot"
    monkeypatch.setattr(shutil, "which", lambda name, *_: "/bin/x")
    assert resolve_runtime("auto") == "singularity"


@pytest.mark.parametrize(
    "command",
    [["doctor"], ["run", "task.sif", "--", "true"], ["pull", "docker://alpine:3.20"], ["janitor"]],
    ids=lambda command: command[0],
)
def test_every_command_with_a_runtime_refuses_an_unknown_one_from_the_environment(
    tmp_path, fake_runtime, command, capsys, monkeypatch
):
    monkeypatch.setenv("HPC_SANDBOX_RUNTIME", "Enroot")  # argparse checks no default
    assert main([command[0], *common(tmp_path), *command[1:]]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "hpc-sandbox: HPC_SANDBOX_RUNTIME must be one of auto, singularity, enroot, not 'Enroot'\n"
    )
    assert not (tmp_path / "scratch").exists()  # the command did not start


def test_the_runtime_from_the_environment_is_only_a_default(
    tmp_path, fake_runtime, capsys, monkeypatch
):
    from hpc_sandbox import tools

    monkeypatch.setenv("HPC_SANDBOX_RUNTIME", "bogus")
    assert main(["janitor", "--runtime", "singularity", *common(tmp_path)]) == 0
    monkeypatch.setattr(tools, "fetch_slirp4netns", lambda arch: tmp_path / "slirp4netns")
    assert main(["fetch-tools"]) == 0  # which has no --runtime
    monkeypatch.setenv("HPC_SANDBOX_RUNTIME", "")  # unset, as with the other variables
    assert main(["janitor", *common(tmp_path)]) == 0
    assert "HPC_SANDBOX_RUNTIME" not in capsys.readouterr().err


async def _abandon(provider, image):
    """Start a sandbox and forget it, as a crashed owner would."""
    sandbox = await provider.start(SandboxSpec(image=str(image)))
    session = sandbox.session_dir
    record = json.loads((session / "owner.json").read_text())
    record["owner_start_time"] = "0"
    (session / "owner.json").write_text(json.dumps(record))
    sandbox._session.release()
    return session


def test_janitor_sweeps_every_installed_runtime(
    tmp_path, sif, sqsh, fake_runtime, fake_enroot, capsys
):
    import asyncio

    from conftest import make_enroot_provider, make_provider

    async def abandon_both():
        singularity = await _abandon(make_provider(tmp_path), sif)
        enroot_provider = make_enroot_provider(tmp_path)
        enroot = await _abandon(enroot_provider, sqsh)
        sandbox = next(iter(enroot_provider.sandboxes))
        await sandbox._stop_container(sandbox._session)  # its processes die with their owner
        return singularity, enroot

    singularity, enroot = asyncio.run(abandon_both())
    assert main(["janitor", *common(tmp_path)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert sorted(report["removed"]) == sorted([singularity.name, enroot.name])


def test_a_janitor_sweeps_the_other_runtimes_when_one_cannot_be_set_up(
    tmp_path, sqsh, fake_runtime, fake_enroot, capsys, monkeypatch
):
    import asyncio

    from conftest import make_enroot_provider

    async def abandon():
        provider = make_enroot_provider(tmp_path)
        session = await _abandon(provider, sqsh)
        sandbox = next(iter(provider.sandboxes))
        await sandbox._stop_container(sandbox._session)  # its processes die with their owner
        return session

    enroot = asyncio.run(abandon())
    monkeypatch.setenv("HPC_SANDBOX_ROOTFS_LAYOUT", "fuse")  # a typo only Singularity reads
    assert main(["janitor", *common(tmp_path)]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["removed"] == [enroot.name]
    assert len(report["errors"]) == 1 and "singularity" in report["errors"][0]


def test_run_waits_for_deletions_even_when_the_start_failed(
    tmp_path, sif, fake_runtime, capsys, monkeypatch
):
    from hpc_sandbox.sandbox import SessionProvider

    waited = []

    async def waiting(self, timeout_s: float = 300.0) -> bool:
        waited.append(timeout_s)
        return True

    monkeypatch.setattr(SessionProvider, "wait_for_deletions", waiting)
    monkeypatch.setenv("FAKE_SINGULARITY_INSTANCE_EXITS", "1")
    assert main(["run", str(sif), *common(tmp_path), "--", "true"]) != 0
    assert waited


def test_pull_fills_the_cache_and_refreshes_tags(tmp_path, fake_runtime, capsys, monkeypatch):
    arguments = ["pull", "--runtime", "singularity", *common(tmp_path), "docker://example/task:1"]
    assert main(arguments) == 0
    first = json.loads(capsys.readouterr().out)
    assert Path(first["path"]).parent == tmp_path / "cache"
    monkeypatch.setenv("FAKE_SINGULARITY_PULL_REVISION", "2")  # the tag moved
    assert main(arguments) == 0
    assert json.loads(capsys.readouterr().out) == first
    assert main([*arguments, "--refresh"]) == 0
    refreshed = json.loads(capsys.readouterr().out)
    assert refreshed["sha256"] != first["sha256"] and len(fake_runtime.calls("pull")) == 2


def test_pull_reports_local_image_files_and_prints_only_json(tmp_path, sif, fake_runtime, capsys):
    import hashlib

    assert main(["pull", "--runtime", "singularity", *common(tmp_path), str(sif)]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert [json.loads(line) for line in lines] == [
        {
            "reference": str(sif),
            "path": str(sif.resolve()),
            "sha256": hashlib.sha256(sif.read_bytes()).hexdigest(),
            "manifest_digest": None,
        }
    ]
