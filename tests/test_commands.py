"""How commands get their environment, stdin and working directory: the shell prologue."""

from __future__ import annotations

import os
import shutil
import subprocess

import pytest

from hpc_sandbox.sandbox import START_MARKER, check_env, environment_script, prologue


@pytest.mark.parametrize("shell", ["sh", "bash"])
def test_environment_lines_set_any_value_verbatim(shell):
    if shutil.which(shell) is None:
        pytest.skip(f"no {shell}")
    raw = os.fsdecode(b"caf\xe9 \xff")  # not UTF-8, as os.environ would hold it
    env = {"GREETING": "it's $(not) run\n", "RAW": raw, "UID": "5", "EUID": "7"}  # read-only last
    check_env(env)
    line = environment_script(env)
    assert line.count(b"\n") == 1 and line.endswith(b"\n")
    script = (
        "hpc_sandbox_nl='\n'; IFS= read -r line || exit 9; eval \"$line\" || exit 8; "
        'printf "%s|%s" "$GREETING" "$RAW"'
    )
    result = subprocess.run([shell, "-c", script], input=line, capture_output=True, check=False)
    assert (result.returncode, result.stdout) == (0, b"it's $(not) run\n|caf\xe9 \xff"), result
    with pytest.raises(ValueError, match="RAW"):
        check_env({"RAW": "\ud800"})


def test_the_prologue_takes_the_environment_from_the_first_line_of_stdin():
    environments = (
        {"UID": "5", "GREETING": "it's $(not) run"},
        {"GREETING": "it's $(not) run", "PATH": "/nowhere", "IFS": "x", "EUID": "7"},
        {"hpc_sandbox_nl": "mine", "GREETING": "it's\n$(not)\n\nrun\n"},
        {"GREETING": "hi", "BASHPID": "1", "LC_CTYPE": "no-such-locale", "LC_ALL": "nope"},
        {"GREETING": "hi", "OPTIND": "not a number"},  # dash refuses it
    )
    command = ["/bin/sh", "-c", 'printf "%s|" "$GREETING"; exec /bin/cat']
    bash = shutil.which("bash")
    shells = [["/bin/sh"], *([[bash], [bash, "--posix"]] if bash else [])]
    shells += [[shell] for shell in (shutil.which("dash"), shutil.which("busybox")) if shell]
    shells = [[*shell, "sh"] if shell[0].endswith("busybox") else shell for shell in shells]

    def run(
        shell: list[str], stdin: bytes | None, env: dict[str, str]
    ) -> subprocess.CompletedProcess:
        argv = [*shell, *prologue(command, "/", stdin=stdin is not None)[1:]]
        return subprocess.run(
            argv, input=environment_script(env) + (stdin or b""), capture_output=True, check=False
        )

    for shell in shells:
        for env in environments:
            result = run(shell, b"line 1\nline 2", env)
            assert result.stdout == f"{env['GREETING']}|line 1\nline 2".encode(), (shell, env)
            # Nothing from the prologue itself (a bash command may still warn about locales).
            assert result.stderr.startswith(f"{START_MARKER}\n".encode()), (shell, env, result)
        result = run(shell, None, {"GREETING": "none"})
        assert result.stdout == b"none|", (shell, result)  # cat read /dev/null
        null = [*shell, *prologue(["sh", "-c", "[ -c /proc/self/fd/0 ]"], "/", stdin=False)[1:]]
        assert subprocess.run(null, input=b"\n", check=False).returncode == 0, shell
        # Without an environment line, the command never starts.
        argv = [*shell, *prologue(command, "/", stdin=True)[1:]]
        result = subprocess.run(argv, input=b"", capture_output=True, check=False)
        assert result.returncode != 0 and START_MARKER.encode() not in result.stderr


def test_only_the_prologues_own_line_marks_a_working_directory_failure():
    from hpc_sandbox import CWD_ERROR_PREFIX
    from hpc_sandbox.sandbox import _cwd_error

    for cwd in ("/work", "/no\nsuch", "/a\x85b\u2028c", "/d\re"):
        line = f"{CWD_ERROR_PREFIX}{cwd}: permission denied\n"
        noisy = f"WARNING: runtime noise\n{line}"
        assert _cwd_error(126, noisy) == line, cwd  # runtime warnings are not the command's
        assert _cwd_error(1, noisy) is None
    assert _cwd_error(126, "some command failed with 126\n") is None
    assert _cwd_error(126, f"{CWD_ERROR_PREFIX}/work: made up\n") is None
