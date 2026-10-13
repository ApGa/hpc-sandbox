from __future__ import annotations

import json
from pathlib import Path

from hpc_sandbox.cli import main


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
