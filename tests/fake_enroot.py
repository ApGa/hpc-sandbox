"""A stand-in for the ``enroot`` CLI, used by the unit tests.

It records every invocation (argv and the environment variables enroot and its
hooks read) and implements the subset hpc-sandbox uses:

- ``import --output FILE URI`` writes a fake image: the squashfs magic
  followed by JSON naming the URI;
- ``create --name NAME IMAGE`` makes ``$ENROOT_DATA_PATH/NAME`` from a fake
  image: its files, the host's system directories as mount points (or the
  same symlinks), and copies of the host files needed to run host binaries;
- ``start [--rw] [--root] [--rc RC] [--mount SPEC]... NAME CMD...`` does what
  enroot does, with real kernel interfaces: a new user namespace (root-mapped
  with ``--root``) and mount namespace, the host's system directories,
  ``/proc``, ``/dev`` and the requested mounts bound into the rootfs (missing
  targets created with mode 0, like enroot), chroot, the environment from the
  rootfs's ``/etc/environment``, then the rc script running CMD. It needs
  unprivileged user namespaces;
- ``version``.

State lives in $FAKE_ENROOT_STATE, which the wrapper on PATH sets because
hpc-sandbox runs ``enroot start`` with a minimal environment. Knobs are files
in that directory:
  import-failures   fail this many imports first (default 0)
  start-fails       start fails before entering the rootfs
"""

from __future__ import annotations

import ctypes
import json
import os
import shutil
import sys
from pathlib import Path

STATE = Path(os.environ["FAKE_ENROOT_STATE"])
MAGIC = b"hsqs"
SYSTEM_DIRS = ("bin", "sbin", "usr", "lib", "lib32", "lib64", "libx32")
HOST_FILES = ("etc/ld.so.cache", "etc/passwd", "etc/group", "etc/nsswitch.conf")
RECORDED_ENV = (
    "PATH",
    "TAR_OPTIONS",
    "HPC_SANDBOX_REAL_AUFS2OVLFS",
    "NVIDIA_VISIBLE_DEVICES",
    "MELLANOX_VISIBLE_DEVICES",
    "SLURM_JOB_ID",
    "HPCS_CANARY",
    "https_proxy",
)
VALUED = {"--output", "-o", "--name", "-n", "--rc", "--mount", "-m", "--env", "-e"}
CLONE_NEWNS, CLONE_NEWUSER = 0x00020000, 0x10000000
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

libc = ctypes.CDLL(None, use_errno=True)


def knob(name: str) -> str | None:
    path = STATE / name
    return path.read_text().strip() if path.exists() else None


def fail(message: str, code: int = 1) -> int:
    print(f"[ERROR] {message}", file=sys.stderr)
    return code


def check(result: int, what: str) -> None:
    if result != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, f"{what}: {os.strerror(errno)}")


def mount(
    source: str, target: Path, fstype: str | None, flags: int, data: str | None = None
) -> None:
    check(
        libc.mount(
            source.encode(),
            str(target).encode(),
            fstype.encode() if fstype else None,
            flags,
            data.encode() if data else None,
        ),
        f"mount {source} on {target}",
    )


def parse(args: list[str]) -> tuple[dict[str, list[str]], list[str]]:
    options: dict[str, list[str]] = {}
    index = 0
    while index < len(args) and args[index].startswith("-"):
        flag = args[index]
        if flag == "--":
            index += 1
            break
        if flag in VALUED:
            options.setdefault(flag, []).append(args[index + 1])
            index += 2
        else:
            options.setdefault(flag, []).append("")
            index += 1
    return options, args[index:]


def write_image(path: Path, payload: dict) -> None:
    path.write_bytes(MAGIC + json.dumps(payload).encode())


def read_image(path: Path) -> dict | None:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    return json.loads(data[len(MAGIC) :] or b"{}") if data[: len(MAGIC)] == MAGIC else None


def import_(args: list[str]) -> int:
    options, (uri,) = parse(args)
    counter = STATE / "import-count"
    count = int(counter.read_text()) if counter.exists() else 0
    counter.write_text(str(count + 1))
    if count < int(knob("import-failures") or "0"):
        return fail(f"simulated import failure for {uri}")
    output = Path(options["--output"][0])
    if output.exists():
        return fail(f"File already exists: {output}")
    write_image(output, {"uri": uri, "files": {"etc/os-release": "fake\n"}})
    return 0


def create(args: list[str]) -> int:
    options, (image,) = parse(args)
    payload = read_image(Path(image))
    if payload is None:
        return fail(f"Invalid image format: {image}")
    rootfs = Path(os.environ["ENROOT_DATA_PATH"]) / options["--name"][0]
    if rootfs.exists():
        return fail(f"File already exists: {rootfs}")
    for directory in ("etc", "tmp", "var/tmp", "proc", "dev", "sys", "root"):
        (rootfs / directory).mkdir(parents=True)
    for name in SYSTEM_DIRS:
        host = Path("/", name)
        if host.is_symlink():
            (rootfs / name).symlink_to(os.readlink(host))
        elif host.is_dir():
            (rootfs / name).mkdir()
    for name in HOST_FILES:
        if Path("/", name).is_file():
            shutil.copyfile(Path("/", name), rootfs / name)
    (rootfs / "etc" / "environment").write_text(
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n"
    )
    for name, content in payload.get("files", {}).items():
        (rootfs / name).parent.mkdir(parents=True, exist_ok=True)
        (rootfs / name).write_text(content)
    # Images carry root-private build caches; extraction keeps their modes.
    cache = rootfs / "private-cache"
    cache.mkdir()
    (cache / "state").write_text("cached\n")
    (cache / "state").chmod(0o400)
    cache.chmod(0o500)
    return 0


def mount_point(target: Path, *, directory: bool) -> None:
    """Create a missing bind target the way enroot does: empty, with mode 0."""
    if target.exists() or target.is_symlink():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if directory:
        target.mkdir()
    else:
        target.touch()
    target.chmod(0)


def bind(source: str, target: Path, *, read_only: bool, recursive: bool = False) -> None:
    mount(source, target, None, MS_BIND | (MS_REC if recursive else 0))
    if read_only:
        locked = os.statvfs(target).f_flag
        flags = MS_REMOUNT | MS_BIND | MS_RDONLY
        flags |= sum(flag for bit, flag in LOCKED_FLAGS.items() if locked & bit)
        mount("none", target, None, flags)


def start(args: list[str]) -> int:
    options, rest = parse(args)
    name, command = rest[0], rest[1:]
    if knob("start-fails"):
        return fail("simulated start failure")
    rootfs = Path(os.environ["ENROOT_DATA_PATH"]) / name
    if not rootfs.is_dir():
        return fail(f"No such file or directory: {rootfs}")
    # What enroot would read from the rootfs on the host side.
    seen = [f"etc/{n}" for n in ("fstab", "environment") if os.path.lexists(rootfs / "etc" / n)]
    with (STATE / "starts.jsonl").open("a") as starts:
        starts.write(json.dumps({"rootfs_config": seen}) + "\n")
    rc = Path(options["--rc"][-1]).read_text() if "--rc" in options else 'exec "$@"\n'
    uid, gid = os.getuid(), os.getgid()
    inside_uid, inside_gid = (0, 0) if "--root" in options else (uid, gid)
    check(libc.unshare(CLONE_NEWUSER | CLONE_NEWNS), "unshare")
    Path("/proc/self/setgroups").write_text("deny")
    Path("/proc/self/uid_map").write_text(f"{inside_uid} {uid} 1")
    Path("/proc/self/gid_map").write_text(f"{inside_gid} {gid} 1")
    mount("none", Path("/"), None, MS_REC | MS_PRIVATE)
    for system in SYSTEM_DIRS:
        target = rootfs / system
        if target.is_dir() and not target.is_symlink():
            bind(f"/{system}", target, read_only=False, recursive=True)
    bind("/proc", rootfs / "proc", read_only=False, recursive=True)
    bind("/dev", rootfs / "dev", read_only=False, recursive=True)
    for spec in options.get("--mount", []):
        source, destination, fstype, flags = spec.split(":")
        target = rootfs / destination.lstrip("/")
        settings = set(flags.split(","))
        if fstype == "tmpfs":
            target.mkdir(parents=True, exist_ok=True)
            mount("tmpfs", target, "tmpfs", MS_NOSUID | MS_NODEV, "mode=1777")
            continue
        mount_point(target, directory="x-create=dir" in settings)
        bind(source, target, read_only="ro" in settings)
    os.chroot(rootfs)
    os.chdir("/")
    environment = {}
    environ = Path("/etc/environment")  # optional, as for enroot
    for line in (environ.read_text() if environ.is_file() else "").splitlines():
        key, separator, value = line.partition("=")
        if separator:
            environment[key] = value
    for item in options.get("--env", []):
        key, _, value = item.partition("=")
        environment[key] = value
    os.execve("/bin/sh", ["/bin/sh", "-c", rc, "sh", *command], environment)
    return 127


def main(argv: list[str]) -> int:
    record = {
        "argv": argv,
        "env": {
            key: value
            for key, value in os.environ.items()
            if key.startswith("ENROOT_") or key in RECORDED_ENV
        },
    }
    with (STATE / "calls.jsonl").open("a") as calls:
        calls.write(json.dumps(record) + "\n")
    if argv == ["version"]:
        print("3.5.0-fake")
        return 0
    handlers = {"import": import_, "create": create, "start": start}
    if argv and argv[0] in handlers:
        return handlers[argv[0]](argv[1:])
    return fail(f"fake enroot does not simulate {argv}", 99)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
