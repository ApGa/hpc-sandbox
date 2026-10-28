"""A stand-in for gVisor's ``runsc``, used by the unit tests.

It records every invocation (its arguments, and the names of the variables in its environment)
and simulates the subset hpc-sandbox uses, with real processes and namespaces where runsc has
them:

- ``run --bundle DIR ID`` in the foreground, as runsc does it: a child titled ``/proc/self/exe``
  that starts a ``runsc-gofer`` and a ``runsc-sandbox``, the Sentry, each dying with its parent.
  First, as runsc's gofer does, it creates the missing mount point of each of the spec's binds in
  the bundle's root, resolved there as the gofer resolves it. The overlay is a copy of the
  bundle's root, ``root/`` in ``--overlay2``'s directory, over which the archive of the
  ``dev.gvisor.tar.rootfs.upper`` annotation is extracted (only with
  ``--allow-rootfs-tar-annotation``, as runsc demands). The Sentry holds a memory file, a memfd
  named ``runsc-memory``, makes user (root mapped to the caller), mount and PID namespaces, and a
  network namespace with only loopback for ``--network=none``; binds the host's system
  directories, ``/dev`` and the spec's binds into the copy, and a tmpfs over an empty ``/tmp``
  that the spec does not mount, as runsc does; chroots into it; and runs the container's command
  as PID 1, with the spec's environment. The command's output is the run's;
- ``exec [--cwd DIR] [--internal-pid-file FILE] ID CMD...``: the Sentry runs CMD as a child of its
  own, in its own process group and with the client's stdin, stdout and stderr, which the client
  passes it over a Unix socket. So, as with runsc, killing the client leaves the command running.
  The client writes the command's PID to FILE, and exits with its status, or with 0 when the
  Sentry dies first, as runsc was seen to;
- ``ps ID``: the table of the sandbox's processes (UID PID PPID PGID ... CMD);
- ``kill [--pgid=N|--pid=N] ID SIGNAL``;
- ``tar rootfs-upper --file=FILE ID``: an archive of the overlay's copy;
- ``state ID``, ``delete [-force] ID``, ``do CMD...`` (runs CMD on the host) and ``--version``.

Sandbox-side PIDs are the host's. Knobs are files in $FAKE_RUNSC_STATE:
  start-fails      the Sentry exits before the container's command starts
  memory-file-mb   the Sentry allocates this many MiB of its memory file, as gVisor does for the
                   sandbox's memory, which no process maps
  slow-exit        the Sentry outlives its parent by half a second, under another command line, as
                   a Sentry that held much memory does while the kernel tears it down
  kill-delay       each ``kill`` takes this many seconds
"""

from __future__ import annotations

import contextlib
import ctypes
import fcntl
import json
import os
import select
import signal
import socket
import stat
import struct
import subprocess
import sys
import time
from pathlib import Path

STATE = Path(os.environ["FAKE_RUNSC_STATE"])
FAKE = str(Path(__file__).resolve())
HOST_DIRS = ("bin", "sbin", "usr", "lib", "lib32", "lib64", "libx32")
CLONE_NEWNS, CLONE_NEWUSER, CLONE_NEWPID, CLONE_NEWNET = 0x20000, 0x10000000, 0x20000000, 0x40000000
MS_RDONLY, MS_NOSUID, MS_NODEV, MS_NOEXEC = 0x1, 0x2, 0x4, 0x8
MS_REMOUNT, MS_BIND, MS_REC, MS_PRIVATE = 0x20, 0x1000, 0x4000, 0x40000
MS_NOATIME, MS_NODIRATIME, MS_RELATIME = 0x400, 0x800, 0x200000
# statvfs flag -> mount flag, for flags a bind remount inside a user namespace must keep.
LOCKED_FLAGS = {
    0x2: MS_NOSUID,
    0x4: MS_NODEV,
    0x8: MS_NOEXEC,
    0x400: MS_NOATIME,
    0x800: MS_NODIRATIME,
    0x1000: MS_RELATIME,
}
PR_SET_PDEATHSIG = 1
PS_HEADER = "UID       PID       PPID      PGID      C         TTY       STIME     TIME      CMD"
libc = ctypes.CDLL(None, use_errno=True)


def fail(message: str, code: int = 128) -> int:
    print(message, file=sys.stderr, flush=True)
    return code


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


def die_with_parent() -> None:
    check(libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0), "prctl")


def parse_globals(argv: list[str]) -> tuple[dict[str, str], list[str]]:
    """runsc's global flags (``--name=value`` or a bare ``--name``), and the rest."""
    flags: dict[str, str] = {}
    index = 0
    while index < len(argv) and argv[index].startswith("-"):
        name, _, value = argv[index].lstrip("-").partition("=")
        flags[name] = value
        index += 1
    return flags, argv[index:]


def parse(args: list[str], valued: set[str]) -> tuple[dict[str, str], list[str]]:
    options: dict[str, str] = {}
    index = 0
    while index < len(args) and args[index].startswith("-"):
        name, equals, value = args[index].lstrip("-").partition("=")
        if not equals and name in valued:
            index += 1
            value = args[index]
        options[name] = value
        index += 1
    return options, args[index:]


def socket_name(identifier: str) -> bytes:
    return b"\0fake-runsc-" + identifier.encode()


def state_file(flags: dict[str, str], identifier: str) -> Path:
    return Path(flags["root"]) / f"{identifier}.json"


def read_state(flags: dict[str, str], identifier: str) -> dict | None:
    try:
        return json.loads(state_file(flags, identifier).read_text())
    except (OSError, ValueError):
        return None


def alive(pid: int) -> bool:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    return stat[stat.rfind(")") + 2] != "Z"


# run: the launcher's runsc, its /proc/self/exe child, the gofer and the Sentry.


def resolve_symlinks(root: str, base: str, rel: str, follow: int = 255) -> str:
    """runsc's ResolveSymlinks: ``rel`` walked from ``base`` with ``root`` as the root directory."""
    if follow == 0:
        raise OSError(40, f"too many symlinks: {rel}")
    for name in os.path.normpath(rel).split("/"):
        if not name:
            continue
        path = os.path.normpath(os.path.join(base, name))
        if not path.startswith(root):
            base = root
            continue
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            base = path
            continue
        if stat.S_ISLNK(info.st_mode):
            link = os.readlink(path)
            base = resolve_symlinks(root, root if link.startswith("/") else base, link, follow - 1)
            continue
        base = path
    return base


def gofer_mount_points(root: str, mounts: list[dict]) -> None:
    """What runsc's gofer does before it binds each mount into the root filesystem it serves, and
    before it makes that read-only: create the mount point there, as a directory for a directory
    and an empty file for anything else."""
    for entry in mounts:
        if entry["type"] != "bind":
            continue
        target = resolve_symlinks(root, root, entry["destination"])
        if os.path.isdir(entry["source"]):
            os.makedirs(target, 0o777, exist_ok=True)
        else:
            os.makedirs(os.path.dirname(target), 0o777, exist_ok=True)
            os.close(os.open(target, os.O_CREAT | os.O_RDONLY, 0o777))


def run(flags: dict[str, str], args: list[str], argv: list[str]) -> int:
    options, (identifier,) = parse(args, {"bundle"})
    bundle = Path(options["bundle"])
    spec = json.loads((bundle / "config.json").read_text())
    if "dev.gvisor.tar.rootfs.upper" in spec.get("annotations", {}) and (
        "allow-rootfs-tar-annotation" not in flags
    ):
        return fail(
            "FetchSpec failed: reading spec: rootfs tar annotation is disabled, use "
            "--allow-rootfs-tar-annotation to enable it"
        )
    overlay = flags["overlay2"].removeprefix("root:dir=").split(",")[0]
    merged = Path(overlay) / "root"
    gofer_mount_points(spec["root"]["path"], spec["mounts"])
    subprocess.run(["cp", "-a", "--", spec["root"]["path"], str(merged)], check=True)
    if upper := spec.get("annotations", {}).get("dev.gvisor.tar.rootfs.upper"):
        subprocess.run(["tar", "-C", str(merged), "-xf", upper], check=True)
    child = os.fork()
    if child == 0:
        die_with_parent()
        os.execv(sys.executable, ["/proc/self/exe", FAKE, "_exe", *argv, str(merged)])
    _, status = os.waitpid(child, 0)
    return os.waitstatus_to_exitcode(status) & 0xFF


def exe(argv: list[str]) -> int:
    """``/proc/self/exe``: the gofer and the Sentry, as children that die with it."""
    merged = argv[-1]
    flags, rest = parse_globals(argv[:-1])
    identifier = rest[-1]
    root = f"--root={flags['root']}"
    gofer = os.fork()
    if gofer == 0:
        die_with_parent()
        os.execv(sys.executable, ["runsc-gofer", FAKE, "_gofer", root, identifier])
    sentry = os.fork()
    if sentry == 0:
        die_with_parent()
        command = ["runsc-sandbox", FAKE, "_sentry", root, json.dumps(argv[:-1]), merged]
        os.execv(sys.executable, [*command, identifier])
    _, status = os.waitpid(sentry, 0)
    os.kill(gofer, signal.SIGKILL)
    return os.waitstatus_to_exitcode(status) & 0xFF


def gofer() -> int:
    while True:
        time.sleep(3600)


def bind(source: str, target: Path, *, read_only: bool) -> None:
    if not target.exists():
        if Path(source).is_dir():
            target.mkdir(parents=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.touch()
    mount(source, target, None, MS_BIND | MS_REC)
    if read_only:
        locked = os.statvfs(target).f_flag
        flags = MS_REMOUNT | MS_BIND | MS_RDONLY
        flags |= sum(flag for bit, flag in LOCKED_FLAGS.items() if locked & bit)
        mount("none", target, None, flags)


def loopback_up() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        request = struct.pack("16sh", b"lo", 0)
        flags = struct.unpack("16sh", fcntl.ioctl(sock, 0x8913, request))[1]
        fcntl.ioctl(sock, 0x8914, struct.pack("16sh", b"lo", flags | 0x1))


def sentry(argv: list[str]) -> int:
    """The Sentry: the namespaces, the container's command as PID 1, and the commands of
    ``exec``, which it runs as its children."""
    root, run_argv, merged, identifier = argv[0], json.loads(argv[1]), Path(argv[2]), argv[3]
    flags, rest = parse_globals(run_argv)
    options, _ = parse(rest[1:], {"bundle"})
    bundle = Path(options["bundle"])
    spec = json.loads((bundle / "config.json").read_text())
    state_dir = os.open(root.removeprefix("--root="), os.O_RDONLY | os.O_DIRECTORY)
    knobs = os.open(STATE, os.O_RDONLY | os.O_DIRECTORY)  # still reachable once chrooted
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)  # in the host's network
    server.bind(socket_name(identifier))
    server.listen(64)
    if (STATE / "start-fails").exists():
        return fail("simulated start failure", 1)
    memory_file = os.memfd_create("runsc-memory")
    if (STATE / "slow-exit").exists():
        slow_exit()
    uid, gid = os.getuid(), os.getgid()
    namespaces = CLONE_NEWUSER | CLONE_NEWNS | CLONE_NEWPID
    if flags.get("network") == "none":
        namespaces |= CLONE_NEWNET
    check(libc.unshare(namespaces), "unshare")
    Path("/proc/self/setgroups").write_text("deny")
    Path("/proc/self/uid_map").write_text(f"0 {uid} 1")
    Path("/proc/self/gid_map").write_text(f"0 {gid} 1")
    mount("none", Path("/"), None, MS_REC | MS_PRIVATE)
    if flags.get("network") == "none":
        loopback_up()
    for name in HOST_DIRS:
        target = merged / name
        if target.is_dir() and not target.is_symlink():
            mount(f"/{name}", target, None, MS_BIND | MS_REC)
    (merged / "dev").mkdir(exist_ok=True)
    mount("/dev", merged / "dev", None, MS_BIND | MS_REC)
    for entry in spec["mounts"]:
        if entry["type"] == "bind":
            target = merged / entry["destination"].lstrip("/")
            bind(entry["source"], target, read_only="ro" in entry.get("options", []))
    # As runsc does: a tmpfs of its own over an empty /tmp, unless the spec mounts one.
    tmp = merged / "tmp"
    if all(entry["destination"] != "/tmp" for entry in spec["mounts"]) and (
        not tmp.is_dir() or not any(tmp.iterdir())
    ):
        tmp.mkdir(exist_ok=True)
        mount("tmpfs", tmp, "tmpfs", 0, "mode=1777")
    os.chroot(merged)
    os.chdir("/")
    environment = dict(item.split("=", 1) for item in spec["process"]["env"])
    init = os.fork()
    if init == 0:
        try:
            die_with_parent()
            Path("/proc").mkdir(exist_ok=True)
            mount("proc", Path("/proc"), "proc", MS_NOSUID | MS_NODEV | MS_NOEXEC)
            os.execve(spec["process"]["args"][0], spec["process"]["args"], environment)
        finally:
            os._exit(127)
    record = {"sentry": os.getpid(), "init": init, "bundle": str(bundle), "merged": str(merged)}
    descriptor = os.open(
        f"{identifier}.json", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600, dir_fd=state_dir
    )
    os.write(descriptor, json.dumps(record).encode())
    os.close(descriptor)
    clients: dict[int, socket.socket] = {}  # command PID -> its client's connection
    allocated = 0
    while True:
        try:
            descriptor = os.open("memory-file-mb", os.O_RDONLY, dir_fd=knobs)
            with os.fdopen(descriptor) as knob:
                wanted = int(knob.read()) << 20
        except (OSError, ValueError):
            wanted = 0
        if wanted > allocated:
            os.posix_fallocate(memory_file, 0, wanted)
            allocated = wanted
        readable, _, _ = select.select([server], [], [], 0.02)
        if readable:
            connection, _ = server.accept()
            message, descriptors, _, _ = socket.recv_fds(connection, 65536, 3)
            request = json.loads(message)
            command = os.fork()
            if command == 0:
                try:
                    die_with_parent()
                    os.setpgid(0, 0)
                    for target, descriptor in enumerate(descriptors):
                        os.dup2(descriptor, target)
                    os.chdir(request["cwd"])
                    os.execvpe(
                        request["argv"][0], request["argv"], {**environment, "HOME": "/root"}
                    )
                finally:
                    os._exit(127)
            for descriptor in descriptors:
                os.close(descriptor)
            connection.sendall(json.dumps({"pid": command}).encode() + b"\n")
            clients[command] = connection
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return 0
            if pid == 0:
                break
            if pid == init:
                return os.waitstatus_to_exitcode(status) & 0xFF
            if (connection := clients.pop(pid, None)) is not None:
                code = os.waitstatus_to_exitcode(status)
                code = 128 - code if code < 0 else code
                try:
                    connection.sendall(json.dumps({"status": code}).encode() + b"\n")
                except OSError:
                    pass
                connection.close()


def slow_exit() -> None:
    """Outlive the parent by half a second, under another command line: at its death, become a
    process that sleeps that long."""
    PR_SET_PDEATHSIG_ = 1
    check(libc.prctl(PR_SET_PDEATHSIG_, signal.SIGTERM, 0, 0, 0), "prctl")

    def linger(_signum: int, _frame: object) -> None:
        os.execv("/bin/sleep", ["lingering", "0.5"])  # in the chroot, which has the host's /bin

    signal.signal(signal.SIGTERM, linger)


# The other subcommands.


def exec_(flags: dict[str, str], args: list[str]) -> int:
    options, rest = parse(args, {"cwd", "internal-pid-file", "pid-file"})
    identifier, command = rest[0], rest[1:]
    if read_state(flags, identifier) is None:
        return fail(f'cannot execute in container "{identifier}" in state stopped')
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.connect(socket_name(identifier))
    except OSError:
        return fail(f'cannot execute in container "{identifier}" in state stopped')
    request = {"argv": command, "cwd": options.get("cwd", "/")}
    socket.send_fds(client, [json.dumps(request).encode()], [0, 1, 2])
    replies = client.makefile("rb")
    started = replies.readline()
    if not started:
        return 0
    if pid_file := options.get("internal-pid-file"):
        Path(pid_file).write_text(f"{json.loads(started)['pid']}\n")
    finished = replies.readline()
    return json.loads(finished)["status"] if finished else 0


def processes(record: dict) -> list[tuple[int, int, int, str]]:
    """The sandbox's processes as (PID, PPID, PGID, name): the Sentry's descendants, with the
    commands it runs reported as children of none, as runsc reports them."""
    sentry = record["sentry"]
    found = []
    pending = [sentry]
    while pending:
        parent = pending.pop()
        try:
            children = Path(f"/proc/{parent}/task/{parent}/children").read_text().split()
        except OSError:
            continue
        for child in map(int, children):
            try:
                stat = Path(f"/proc/{child}/stat").read_text()
            except OSError:
                continue
            name = stat[stat.find("(") + 1 : stat.rfind(")")]
            fields = stat[stat.rfind(")") + 2 :].split()
            found.append((child, 0 if parent == sentry else parent, int(fields[2]), name))
            pending.append(child)
    return found


def ps(flags: dict[str, str], args: list[str]) -> int:
    _, (identifier,) = parse(args, {"format"})
    record = read_state(flags, identifier)
    if record is None:
        return fail(f"container {identifier} does not exist")
    print(PS_HEADER)
    for pid, parent, group, name in processes(record):
        row = ("0", str(pid), str(parent), str(group), "0", "?", "00:00", "0s", name)
        print(" ".join(f"{field:<9}" for field in row).rstrip())
    return 0


def kill(flags: dict[str, str], args: list[str]) -> int:
    with contextlib.suppress(OSError, ValueError):
        time.sleep(float((STATE / "kill-delay").read_text()))
    options, rest = parse(args, {"pgid", "pid"})
    identifier, name = rest[0], rest[1] if len(rest) > 1 else "TERM"
    record = read_state(flags, identifier)
    if record is None:
        return fail(f"container {identifier} does not exist")
    signum = int(name) if name.isdigit() else getattr(signal, f"SIG{name}")
    try:
        if "pgid" in options:
            os.killpg(int(options["pgid"]), signum)
        elif "pid" in options:
            os.kill(int(options["pid"]), signum)
        else:
            os.kill(record["init"], signum)
    except ProcessLookupError:
        return fail("no such process")
    return 0


def tar(flags: dict[str, str], args: list[str]) -> int:
    options, rest = parse(args[1:], {"file"})
    identifier = rest[0]
    record = read_state(flags, identifier)
    if args[0] != "rootfs-upper" or record is None:
        return fail(f"cannot archive {identifier}")
    print(f"Serializing rootfs upper layer into a tar archive for container: {identifier}")
    command = ["tar", "-C", record["merged"], "-cf", options["file"], "."]
    return subprocess.run(command, check=False).returncode


def state(flags: dict[str, str], args: list[str]) -> int:
    (identifier,) = args
    record = read_state(flags, identifier)
    if record is None:
        return fail(f"container {identifier} does not exist")
    running = alive(record["sentry"])
    print(
        json.dumps(
            {
                "id": identifier,
                "status": "running" if running else "stopped",
                "pid": record["sentry"] if running else -1,
                "bundle": record["bundle"],
            }
        )
    )
    return 0


def do(flags: dict[str, str], args: list[str]) -> int:
    return subprocess.run(args, check=False).returncode


def delete(flags: dict[str, str], args: list[str]) -> int:
    _, (identifier,) = parse(args, set())
    state_file(flags, identifier).unlink(missing_ok=True)
    return 0


def main(argv: list[str]) -> int:
    with (STATE / "calls.jsonl").open("a") as calls:
        calls.write(json.dumps({"argv": argv, "env": sorted(os.environ)}) + "\n")
    if argv == ["--version"]:
        print("runsc version release-fake\nspec: 1.2.1")
        return 0
    flags, rest = parse_globals(argv)
    if not rest:
        return fail("no subcommand", 2)
    if "rootless" not in flags or "root" not in flags:
        return fail("hpc-sandbox must run runsc with --rootless and its own --root", 97)
    handlers = {
        "exec": exec_,
        "ps": ps,
        "kill": kill,
        "tar": tar,
        "state": state,
        "delete": delete,
        "do": do,
    }
    if rest[0] == "run":
        return run(flags, rest[1:], argv)
    if rest[0] in handlers:
        return handlers[rest[0]](flags, rest[1:])
    return fail(f"fake runsc does not simulate {rest[0]}", 99)


if __name__ == "__main__":
    role = sys.argv[1] if len(sys.argv) > 1 else ""
    if role == "_exe":
        sys.exit(exe(sys.argv[2:]))
    if role == "_gofer":
        sys.exit(gofer())
    if role == "_sentry":
        sys.exit(sentry(sys.argv[2:]))
    sys.exit(main(sys.argv[1:]))
