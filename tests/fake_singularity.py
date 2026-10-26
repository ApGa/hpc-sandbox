"""A stand-in for the ``singularity`` CLI, used by the unit tests.

It records every invocation and simulates the subset of the CLI hpc-sandbox
uses: ``pull``, ``sif list``, ``build --sandbox``, ``instance start|list|stop``,
``exec instance://``, ``exec DIRECTORY``, ``--version`` and ``buildcfg``.
Instances are real host processes titled "Singularity instance: <user>
[<name>]", like the real ones, so PID identity checks run for real. ``exec
instance://`` runs commands on the host: ``--cwd`` and absolute-path arguments
(except the script after ``-c``) are mapped onto the instance's binds and
rootfs; system directories such as /bin and /usr stay host paths. ``exec
DIRECTORY`` (as pod services run) needs to run as root of a user namespace,
as it does under hpc-sandbox's launcher, or with ``--userns``, which first
makes a user namespace mapping the caller's uid and gid to themselves, as
Singularity's does: like ``--containall``, it gives the command new mount and
PID namespaces, the host's system directories, a fresh /proc and the binds,
then chroots into the directory; a shim init runs the command as its child,
as SingularityCE's does. ``exec --fakeroot DIRECTORY``
(how a stopped --fakeroot sandbox's files are given back) checks its flags
and binds and runs nothing: without subordinate ids, everything a sandbox of
the fake wrote belongs to the caller already.

State lives in $FAKE_SINGULARITY_STATE. Knobs (environment variables):
  FAKE_SINGULARITY_PULL_FAILURES   fail this many pulls first (default 0)
  FAKE_SINGULARITY_PULL_DELAY      seconds each pull takes (default 0)
  FAKE_SINGULARITY_PULL_REVISION   pulls write a different image, as after a tag moved
  FAKE_SINGULARITY_INSTANCE_EXITS  "1": instances exit right after starting
  FAKE_SINGULARITY_START_DELAY     seconds each instance start takes (default 0)
  FAKE_SINGULARITY_EXIT_DELAY      seconds a shim waits to pass its command's status on (default 0)
  FAKE_SINGULARITY_FAKEROOT_FAILS  "1": exec --fakeroot DIRECTORY fails
"""

from __future__ import annotations

import ctypes
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

STATE = Path(os.environ["FAKE_SINGULARITY_STATE"])
ALWAYS_PASSED = {
    "TERM",
    *(f"{name}_proxy" for name in ("http", "https", "ftp", "all", "no")),
    *(f"{name}_PROXY" for name in ("HTTP", "HTTPS", "FTP", "ALL", "NO")),
}
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
RECORDED_ENV = ("NVIDIA_VISIBLE_DEVICES",)
HOST_DIRS = ("bin", "sbin", "usr", "lib", "lib32", "lib64", "libx32")
HOST_FILES = ("etc/ld.so.cache", "etc/passwd", "etc/group", "etc/nsswitch.conf")
CLONE_NEWNS, CLONE_NEWUSER, CLONE_NEWPID = 0x00020000, 0x10000000, 0x20000000
MS_RDONLY, MS_NOSUID, MS_NODEV, MS_NOEXEC = 0x1, 0x2, 0x4, 0x8
MS_REMOUNT, MS_BIND, MS_REC, MS_PRIVATE = 0x20, 0x1000, 0x4000, 0x40000
libc = ctypes.CDLL(None, use_errno=True)
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
    image = {"uri": uri}
    if revision := os.environ.get("FAKE_SINGULARITY_PULL_REVISION"):
        image["revision"] = revision  # the tag moved
    Path(out).write_bytes(MAGIC + json.dumps(image).encode())
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
    os.umask(0o022)  # an image's directories have its modes, whatever the caller's umask
    for directory in (".singularity.d/env", "etc", "tmp", "var/tmp", "proc", "dev", "sys"):
        (root / directory).mkdir(parents=True)
    for name in HOST_DIRS:  # mount points for the host's, as exec DIRECTORY binds them
        host = Path("/", name)
        if host.is_symlink():
            (root / name).symlink_to(os.readlink(host))
        elif host.is_dir():
            (root / name).mkdir()
    for name in HOST_FILES:
        if Path("/", name).is_file():
            shutil.copyfile(Path("/", name), root / name)
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
    time.sleep(float(os.environ.get("FAKE_SINGULARITY_START_DELAY", "0")))
    if "--writable-tmpfs" in options:
        return fail("hpc-sandbox must not use --writable-tmpfs", 98)
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


def check(result: int, what: str) -> None:
    if result != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, f"{what}: {os.strerror(errno)}")


def mount(source: str, target: Path, fstype: str | None, flags: int, data: str = "") -> None:
    check(
        libc.mount(
            source.encode(),
            str(target).encode(),
            fstype.encode() if fstype else None,
            flags,
            data.encode() or None,
        ),
        f"mount {source} on {target}",
    )


def exec_directory(options: dict[str, list[str]], root: Path, command: list[str]) -> int:
    if "--writable" not in options or "--containall" not in options:
        return fail("expected --writable --containall with a sandbox directory")
    binds = []
    for spec in options.get("--bind", []):
        source, target, mode = spec.split(":")
        if not Path(source).exists():
            return fail(f"bind source {source} does not exist")
        if not (root / target.lstrip("/")).exists():
            return fail(f"destination {target} doesn't exist in container")
        binds.append((source, root / target.lstrip("/"), mode))
    if "--userns" in options:  # as the caller, not root: its uid and gid mapped to themselves
        uid, gid = os.getuid(), os.getgid()
        check(libc.unshare(CLONE_NEWUSER), "unshare --userns")
        Path("/proc/self/setgroups").write_text("deny")
        Path("/proc/self/uid_map").write_text(f"{uid} {uid} 1")
        Path("/proc/self/gid_map").write_text(f"{gid} {gid} 1")
    check(libc.unshare(CLONE_NEWNS | CLONE_NEWPID), "unshare (exec needs a user namespace)")
    mount("none", Path("/"), None, MS_REC | MS_PRIVATE)
    # Like SingularityCE: this process forwards signals to the container's
    # shim init (PID 1 of its PID namespace), which runs the command as its
    # child, forwards signals to it, reaps orphans and exits with its status.
    shim = os.fork()
    if shim:
        return _supervise(shim)
    try:
        for name in HOST_DIRS:
            target = root / name
            if target.is_dir() and not target.is_symlink():
                mount(f"/{name}", target, None, MS_BIND | MS_REC)
        mount("proc", root / "proc", "proc", MS_NOSUID | MS_NODEV | MS_NOEXEC)
        mount("/dev", root / "dev", None, MS_BIND | MS_REC)
        for source, target, mode in binds:
            mount(source, target, None, MS_BIND)
            if mode == "ro":
                flags = MS_REMOUNT | MS_BIND | MS_RDONLY | MS_NOSUID | MS_NODEV
                mount("none", target, None, flags)
        os.chroot(root)
        os.chdir("/")
        env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C", "SINGULARITY_CONTAINER": "x"}
        payload = os.fork()
        if payload == 0:
            os.execvpe(command[0], command, env)
        os._exit(_supervise(payload, reap_all=True))
    except OSError as error:
        print(f"FATAL:   {error}", file=sys.stderr)
    os._exit(255)


def _supervise(child: int, *, reap_all: bool = False) -> int:
    """Forward SIGTERM, SIGINT and SIGHUP to ``child``; return its status as a shell would."""
    for forwarded in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(forwarded, lambda signum, _frame: _forward(child, signum))
    while True:
        try:
            pid, status = os.waitpid(-1 if reap_all else child, 0)
        except InterruptedError:
            continue
        except ChildProcessError:
            return 255
        if pid == child:
            code = os.waitstatus_to_exitcode(status)
            time.sleep(float(os.environ.get("FAKE_SINGULARITY_EXIT_DELAY", "0")))
            return 128 - code if code < 0 else code


def _forward(child: int, signum: int) -> None:
    try:
        os.kill(child, signum)
    except ProcessLookupError:
        pass


def exec_fakeroot(options: dict[str, list[str]], root: Path) -> int:
    if "--writable" not in options or "--containall" not in options:
        return fail("expected --writable --containall with a sandbox directory")
    for spec in options.get("--bind", []):
        source, target, _ = spec.split(":")
        if not Path(source).exists():
            return fail(f"bind source {source} does not exist")
        if not (root / target.lstrip("/")).exists():
            return fail(f"destination {target} doesn't exist in container")
    if os.environ.get("FAKE_SINGULARITY_FAKEROOT_FAILS") == "1":
        return fail("simulated --fakeroot failure")
    return 0


def exec_(args: list[str]) -> int:
    options, rest = parse(args)
    target, command = rest[0], rest[1:]
    if "--fakeroot" in options and Path(target).is_dir():
        return exec_fakeroot(options, Path(target))
    if Path(target).is_dir():
        return exec_directory(options, Path(target), command)
    if not target.startswith("instance://"):
        return fail("only instance:// and directory execs are simulated", 99)
    path = INSTANCES / f"{target.removeprefix('instance://')}.json"
    record = json.loads(path.read_text()) if path.exists() else None
    if record is None or not alive(record["pid"]):
        return fail(f"no instance found with name {target}")
    mapper = Mapper(record)
    try:
        os.chdir(mapper.host(options.get("--cwd", ["/"])[0]))
    except OSError:
        os.chdir(mapper.host("/"))  # like Singularity CE: a missing --cwd falls back to /
    command = [
        arg
        if index == 0 or command[index - 1] == "-c" or not arg.startswith("/")
        else mapper.host(arg)
        for index, arg in enumerate(command)
    ]
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C"}
    # Like Singularity CE and Apptainer, --cleanenv still passes these on.
    env.update({key: value for key, value in os.environ.items() if key in ALWAYS_PASSED})
    if "--cleanenv" not in options:
        env = dict(os.environ)
    os.execvpe(command[0], command, env)
    return 0


def main(argv: list[str]) -> int:
    with (STATE / "calls.jsonl").open("a") as calls:
        calls.write(json.dumps(argv) + "\n")
    with (STATE / "environments.jsonl").open("a") as environments:
        recorded = {key: os.environ.get(key) for key in RECORDED_ENV}
        environments.write(json.dumps({"argv": argv, "env": recorded}) + "\n")
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
