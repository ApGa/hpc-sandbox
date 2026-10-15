from __future__ import annotations

import json
import shutil
from pathlib import Path

from hpc_sandbox import SandboxSpec
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
    config.write_text("# site\nallow setuid = yes\nsessiondir max size = 64\nother = 1\n")
    assert main(["doctor", *common(tmp_path), "--image", str(sif)]) == 0
    output = capsys.readouterr().out
    assert "singularity-ce version 4.4.1-fake" in output
    assert "[warn] site: sessiondir max size: 64" in output
    assert "[info] site: allow setuid: yes" in output and "other" not in output
    for check in ("sandbox start", "exec", "upload/download", "sandbox stop"):
        assert f"[  ok] {check}" in output


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


def test_doctor_checks_enroot_and_smoke_tests_an_image(tmp_path, sqsh, fake_enroot, capsys):
    arguments = ["doctor", "--runtime", "enroot", *common(tmp_path), "--image", str(sqsh)]
    assert main(arguments) == 0
    output = capsys.readouterr().out
    assert "3.5.0-fake" in output and "[  ok] namespaces" in output
    for check in ("sandbox start", "exec", "upload/download", "sandbox stop"):
        assert f"[  ok] {check}" in output


def test_runtime_resolution(monkeypatch):
    assert resolve_runtime("enroot") == "enroot"
    assert resolve_runtime("auto", "/opt/enroot/bin/enroot") == "enroot"
    assert resolve_runtime("auto", "apptainer") == "singularity"
    monkeypatch.setattr(shutil, "which", lambda name, *_: "/bin/x" if name == "enroot" else None)
    assert resolve_runtime("auto") == "enroot"
    monkeypatch.setattr(shutil, "which", lambda name, *_: "/bin/x")
    assert resolve_runtime("auto") == "singularity"


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
