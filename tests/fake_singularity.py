"""A stand-in for the ``singularity`` CLI, used by the unit tests.

It records every invocation and simulates the subset of the CLI hpc-sandbox
uses: ``pull``, ``sif list``, ``build --sandbox``, ``instance start|list|stop``,
``exec instance://``, ``--version`` and ``buildcfg``. Instances are real host
processes titled "Singularity instance: <user> [<name>]", like the real
ones, so PID identity checks run for real. ``exec`` runs commands on the host:
``--cwd`` and absolute-path arguments (except the script after ``-c``) are
mapped onto the instance's binds and rootfs; system directories such as /bin
and /usr stay host paths.

State lives in $FAKE_SINGULARITY_STATE. Knobs (environment variables):
  FAKE_SINGULARITY_PULL_FAILURES   fail this many pulls first (default 0)
  FAKE_SINGULARITY_PULL_DELAY      seconds each pull takes (default 0)
  FAKE_SINGULARITY_INSTANCE_EXITS  "1": instances exit right after starting
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

STATE = Path(os.environ["FAKE_SINGULARITY_STATE"])
INSTANCES = STATE / "instances"
MAGIC = b"FAKESIF\n"
SYSTEM_DIRS = {"bin", "sbin", "usr", "lib", "lib64", "proc", "sys", "dev"}
VALUED = {
    "--bind",
    "-B",
    "--pid-file",
    "--no-mount",
    "--network",
    "--cpus",
    "--memory",
    "--cwd",
    "--pwd",
    "--env",
    "--env-file",
    "--timeout",
    "--workdir",
    "-W",
}
INSTANCE_LOOP = """\
import signal, sys, time
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
while True:
    time.sleep(3600)
"""


def fail(message: str, code: int = 255) -> int:
    print(f"FATAL:   {message}", file=sys.stderr)
    return code


def parse(args: list[str]) -> tuple[dict[str, list[str]], list[str]]:
    options: dict[str, list[str]] = {}
    index = 0
    while index < len(args) and args[index].startswith("-"):
        flag = args[index]
        if flag in VALUED:
            options.setdefault(flag, []).append(args[index + 1])
            index += 2
        else:
            options.setdefault(flag, []).append("")
            index += 1
    return options, args[index:]


def is_sif(path: Path) -> bool:
    try:
        return path.read_bytes()[: len(MAGIC)] == MAGIC
    except OSError:
        return False


def alive(pid: int) -> bool:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    return stat[stat.rfind(")") + 2] != "Z"


def pull(args: list[str]) -> int:
    _, (out, uri) = parse(args)
    counter = STATE / "pull-count"
    count = int(counter.read_text()) if counter.exists() else 0
    counter.write_text(str(count + 1))
    time.sleep(float(os.environ.get("FAKE_SINGULARITY_PULL_DELAY", "0")))
    if count < int(os.environ.get("FAKE_SINGULARITY_PULL_FAILURES", "0")):
        return fail(f"simulated pull failure for {uri}")
    Path(out).write_bytes(MAGIC + json.dumps({"uri": uri}).encode())
    return 0


def build(args: list[str]) -> int:
    options, (target, source) = parse(args)
    if "--sandbox" not in options:
        return fail("only build --sandbox is simulated", 99)
    if not is_sif(Path(source)):
        return fail(f"{source} is not a SIF image")
    root = Path(target)
    if root.exists():
        return fail(f"{target} already exists")
    meta = json.loads(Path(source).read_bytes()[len(MAGIC) :] or b"{}")
    for directory in (".singularity.d/env", "etc", "tmp", "var/tmp"):
        (root / directory).mkdir(parents=True)
    (root / ".singularity.d" / "startscript").write_text("#!/bin/sh\n")
    for name, content in meta.get("files", {}).items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(content)
    # Images carry root-private build caches; extraction keeps their modes.
    cache = root / "private-cache"
    cache.mkdir()
    (cache / "state").write_text("cached\n")
    (cache / "state").chmod(0o400)
    cache.chmod(0o500)
    return 0


def instance_start(args: list[str]) -> int:
    options, (rootfs, name) = parse(args)
    if "--writable-tmpfs" in options or "--fakeroot" in options:
        return fail("--writable-tmpfs and --fakeroot break on ABCI", 98)
    root = Path(rootfs)
    if "--writable" not in options or not root.is_dir():
        return fail("expected --writable with a sandbox directory")
    binds = []
    for spec in options.get("--bind", []):
        source, target, mode = spec.split(":")
        if not Path(source).exists():
            return fail(f"bind source {source} does not exist")
        if not (root / target.lstrip("/")).exists():
            return fail(f"destination {target} doesn't exist in container")
        binds.append([source, target, mode])
    startscript = root / ".singularity.d" / "startscript"
    keeps_running = os.access(startscript, os.X_OK) and "sleep" in startscript.read_text()
    title = f"Singularity instance: {os.environ.get('USER', 'user')} [{name}]"
    process = subprocess.Popen(
        [sys.executable, "-c", INSTANCE_LOOP, title],
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    Path(options["--pid-file"][0]).write_text(f"{process.pid}\n")
    INSTANCES.mkdir(exist_ok=True)
    record = {"pid": process.pid, "rootfs": str(root), "binds": binds, "options": options}
    (INSTANCES / f"{name}.json").write_text(json.dumps(record))
    if not keeps_running or os.environ.get("FAKE_SINGULARITY_INSTANCE_EXITS") == "1":
        process.kill()
        process.wait()
    return 0


def instance_list(args: list[str]) -> int:
    _, names = parse(args)
    found = []
    for path in sorted(INSTANCES.glob("*.json")) if INSTANCES.is_dir() else []:
        if names and path.stem not in names:
            continue
        record = json.loads(path.read_text())
        if alive(record["pid"]):
            found.append({"instance": path.stem, "pid": record["pid"], "img": record["rootfs"]})
        else:
            path.unlink(missing_ok=True)
    print(json.dumps({"instances": found}, indent=4))
    return 0


def instance_stop(args: list[str]) -> int:
    options, names = parse(args)
    if "--all" in options or "-a" in options:
        return fail("instance stop --all would stop other jobs' instances", 97)
    (name,) = names
    path = INSTANCES / f"{name}.json"
    if not path.exists():
        return fail(f"no instance found with name {name}")
    pid = json.loads(path.read_text())["pid"]
    os.kill(pid, signal.SIGKILL if "--force" in options else signal.SIGTERM)
    deadline = time.monotonic() + float(options.get("--timeout", ["10"])[0])
    while alive(pid) and time.monotonic() < deadline:
        time.sleep(0.02)
    if alive(pid):
        os.kill(pid, signal.SIGKILL)
    path.unlink()
    return 0


class Mapper:
    """Maps container paths onto the instance's binds and rootfs."""

    def __init__(self, record: dict) -> None:
        self.rootfs = record["rootfs"]
        self.binds = sorted(((t, s) for s, t, _ in record["binds"]), key=lambda b: -len(b[0]))

    def host(self, path: str) -> str:
        for target, source in self.binds:
            if path == target or path.startswith(target.rstrip("/") + "/"):
                return source + path[len(target) :]
        if path.lstrip("/").split("/", 1)[0] in SYSTEM_DIRS:
            return path
        return self.rootfs + path


def exec_(args: list[str]) -> int:
    options, rest = parse(args)
    target, command = rest[0], rest[1:]
    if not target.startswith("instance://"):
        return fail("only instance:// execs are simulated", 99)
    path = INSTANCES / f"{target.removeprefix('instance://')}.json"
    record = json.loads(path.read_text()) if path.exists() else None
    if record is None or not alive(record["pid"]):
        return fail(f"no instance found with name {target}")
    mapper = Mapper(record)
    try:
        os.chdir(mapper.host(options.get("--cwd", ["/"])[0]))
    except OSError as error:
        return fail(f"could not change directory: {error}")
    command = [
        arg
        if index == 0 or command[index - 1] == "-c" or not arg.startswith("/")
        else mapper.host(arg)
        for index, arg in enumerate(command)
    ]
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C"}
    if "--cleanenv" not in options:
        env = dict(os.environ)
    os.execvpe(command[0], command, env)
    return 0


def main(argv: list[str]) -> int:
    with (STATE / "calls.jsonl").open("a") as calls:
        calls.write(json.dumps(argv) + "\n")
    if argv == ["--version"]:
        print("singularity-ce version 4.4.1-fake")
        return 0
    if argv[:1] == ["buildcfg"]:
        print(f"SINGULARITY_CONFDIR={STATE / 'etc'}")
        return 0
    if argv[:2] == ["sif", "list"]:
        return 0 if is_sif(Path(argv[2])) else fail(f"{argv[2]} is not a SIF")
    handlers = {
        ("pull",): pull,
        ("build",): build,
        ("instance", "start"): instance_start,
        ("instance", "list"): instance_list,
        ("instance", "stop"): instance_stop,
        ("exec",): exec_,
    }
    for prefix, handler in handlers.items():
        if tuple(argv[: len(prefix)]) == prefix:
            return handler(argv[len(prefix) :])
    return fail(f"fake singularity does not simulate {argv}", 99)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
