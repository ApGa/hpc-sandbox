"""Create a sandbox's namespaces and run its container as their PID 1.

:mod:`hpc_sandbox.enroot` runs this file as a script in a fresh interpreter::

    python -I -S anchor.py PARENT_PID NETWORK -- COMMAND [ARG...]
    python -I -S anchor.py PARENT_PID join HOLDER_PID [OPTION...] -- COMMAND [ARG...]
    python -I -S anchor.py PARENT_PID file-owners MANIFEST ROOTFS
    python -I -S anchor.py PARENT_PID exec [--cpus LIST] -- COMMAND [ARG...]

The first form starts a sandbox of its own. It

1. arranges to be SIGKILLed when its parent (the provider process) dies, and
   exits at once if that already happened;
2. creates new user, mount, PID and IPC namespaces, mapping the calling user
   and group to themselves, plus a network namespace whose only interface is
   an up loopback when NETWORK is ``none``;
3. forks the PID namespace's init, which arranges to be SIGKILLed if this
   process dies, mounts a fresh ``/proc`` and executes COMMAND (``enroot
   start ...`` with a keep-alive loop as the container's command);
4. waits for it and exits with its status.

When the init process dies, the kernel kills every process in its PID
namespace, so killing it stops the whole sandbox. Only unprivileged user
namespaces are needed: the ``unshare`` command line is avoided because the
options this needs are missing from older util-linux releases.

The ``join`` form starts a service of a pod (see :mod:`hpc_sandbox.holder`):
it joins the user and network namespaces of the pod's holder, creates mount,
PID, IPC and UTS namespaces, and forks an init of its own as their PID 1. The
init sets the service's hostname (``--hostname NAME``), resource limits
(``--rlimit NAME=SOFT:HARD``, within the hard limits it has) and an optional
sized tmpfs (``--shm DIR:BYTES``, for the runtime to bind at ``/dev/shm``),
then runs COMMAND as its child, with its output through a pipe this process
copies to its own (so the service cannot truncate the log, and no file size
limit of the service applies to it). With ``--userns MODE`` the child runs it
in a nested user namespace, which holds no capability over the pod's network:
``root`` maps root to the pod's root, the caller, and nothing else;
``caller=UID:GID`` maps the caller's own uid and gid, so that COMMAND runs as
them, without capabilities; ``subids`` maps every id the pod's namespace maps
to itself, which the init writes for the child, so that root there can switch
to the caller's subordinate ids. The init forwards SIGTERM, SIGINT and SIGHUP
to the child, reaps orphans, and exits with the child's status; the runtime is
never PID 1, so killing the init always ends the service. With
``--file-owners MANIFEST ROOTFS``, it first gives the paths below the service's
root filesystem that the file owners manifest lists their owners, as root of
the pod's user namespace.

The ``file-owners`` form does only that, in the user namespace it is run in:
the Singularity provider runs it with ``nsenter`` in a ``--fakeroot``
instance's, once the instance runs and before any command does. Image
extraction gives every file to the caller, and a file owners manifest
(:func:`read_file_owners`, which the package also uses to check one before
anything starts) says which paths the image gives to other users
(:func:`apply_file_owners`).

The ``exec`` form arranges to be SIGKILLed when its parent dies, exits at once
if that already happened, keeps to the CPUs that ``--cpus`` lists
(``0,2-3``), if given, and then runs COMMAND in its own place: the gVisor
provider's ``runsc run``, which makes namespaces of its own and whose sandbox
dies with it (:mod:`hpc_sandbox.gvisor`).
"""

from __future__ import annotations

import ctypes
import errno
import fcntl
import os
import re
import resource
import select
import signal
import socket
import stat
import struct
import sys

CLONE_NEWNS = 0x00020000
CLONE_NEWUTS = 0x04000000
CLONE_NEWIPC = 0x08000000
CLONE_NEWUSER = 0x10000000
CLONE_NEWPID = 0x20000000
CLONE_NEWNET = 0x40000000
MS_NOSUID = 0x2
MS_NODEV = 0x4
MS_NOEXEC = 0x8
MS_REC = 0x4000
MS_PRIVATE = 0x40000
PR_SET_PDEATHSIG = 1
SIOCGIFFLAGS = 0x8913
SIOCSIFFLAGS = 0x8914
IFF_UP = 0x1
# How the report of the entries of a file owners manifest that were skipped starts, on stdout or
# stderr.
FILE_OWNERS_REPORT = "hpc-sandbox: file owners: "
# What a sandbox mounts its own over: the kernel's file systems, and the private /tmp and /var/tmp
# (sandbox.RESERVED_TARGETS). The files of the root filesystem at or below them are never seen
# inside, and a --fakeroot sandbox's give-back, which sees the same mounts, cannot reach them.
MOUNTED_OVER = ("/dev", "/proc", "/sys", "/tmp", "/var/tmp")
_MOUNTED_OVER = tuple(tuple(path.encode().split(b"/")[1:]) for path in MOUNTED_OVER)
# A file owners manifest's line: UID GID PATH, the path absolute and running to the end of the line.
# A carriage return fails the line: CRLF line ends would leave one at the end of every path.
_FILE_OWNERS_LINE = re.compile(rb"[ \t]*([0-9]{1,10})[ \t]+([0-9]{1,10})[ \t]+(/[^\0\r]*)")
_MAX_ID = 0xFFFFFFFE  # chown reads (uid_t) -1 as "leave it"
_SET_ID = stat.S_ISUID | stat.S_ISGID  # the bits that a chown clears on a regular file
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_NAMES_NOTHING = (errno.ENOENT, errno.ENOTDIR, errno.ELOOP)  # also a link, or a file, on the way
_REPORTED = 10

_libc = ctypes.CDLL(None, use_errno=True)


def _check(result: int, what: str) -> None:
    if result != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, f"{what}: {os.strerror(errno)}")


def _write(path: str, text: str) -> None:
    """Write ``text`` with one system call, as the kernel's id maps need."""
    fd = os.open(path, os.O_WRONLY | os.O_CLOEXEC)
    try:
        os.write(fd, text.encode())
    finally:
        os.close(fd)


def _die_with_parent() -> None:
    _check(_libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0), "prctl(PR_SET_PDEATHSIG)")


def _loopback_up() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        request = struct.pack("16sh", b"lo", 0)
        flags = struct.unpack("16sh", fcntl.ioctl(sock, SIOCGIFFLAGS, request))[1]
        fcntl.ioctl(sock, SIOCSIFFLAGS, struct.pack("16sh", b"lo", flags | IFF_UP))


def _set_rlimit(name: str, soft: int, hard: int) -> None:
    """Set a limit (-1: unlimited) within the hard limit this process already has."""
    which = getattr(resource, f"RLIMIT_{name.upper()}", None)
    if which is None:
        raise OSError(22, f"unknown resource limit {name}")
    infinity = resource.RLIM_INFINITY
    soft, hard = (infinity if value < 0 else value for value in (soft, hard))
    current = resource.getrlimit(which)[1]
    if current != infinity and (hard == infinity or hard > current):
        hard = current
    if soft == infinity or (hard != infinity and soft > hard):
        soft = hard
    try:
        resource.setrlimit(which, (soft, hard))
    except ValueError as error:
        raise OSError(22, f"limit {name}: {error}") from None


def _setns(path: str, nstype: int) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        _check(_libc.setns(fd, nstype), f"setns({path})")
    finally:
        os.close(fd)


FileOwner = tuple[int, int, tuple[bytes, ...]]  # a uid, a gid, and the components of a path


def read_file_owners(path: str) -> list[FileOwner]:
    """The entries of the file owners manifest at ``path``, in its order.

    Each line is ``UID GID PATH``: two decimal ids, then an absolute path in the image, which runs
    to the end of the line and may hold spaces, but no carriage return. Empty and ``.`` components
    are dropped, so ``/srv/app/`` is ``/srv/app``. Blank lines and lines that start with ``#`` are
    skipped. Raises ``ValueError`` for the first line that is none of these, or whose path climbs
    with ``..``, and ``OSError`` when the file cannot be read.
    """
    with open(path, "rb") as stream:
        lines = stream.read().split(b"\n")
    entries = []
    for number, line in enumerate(lines, 1):
        if not line.strip() or line.lstrip().startswith(b"#"):
            continue
        match = _FILE_OWNERS_LINE.fullmatch(line)
        if match is not None:
            uid, gid = int(match[1]), int(match[2])
            parts = tuple(part for part in match[3].split(b"/") if part not in (b"", b"."))
            if max(uid, gid) <= _MAX_ID and b".." not in parts:
                entries.append((uid, gid, parts))
                continue
        shown = line[:200].decode(errors="replace")
        crlf = " (CRLF line ends?)" if b"\r" in line else ""
        raise ValueError(
            f"file owners manifest {path}, line {number}: expected UID GID /PATH, with ids up to "
            f"{_MAX_ID} and no .. or carriage return in the path, not {shown!r}{crlf}"
        )
    return entries


def apply_file_owners(entries: list[FileOwner], rootfs: str) -> list[str]:
    """Give each path that ``entries`` list below the directory ``rootfs`` its uid and gid.

    Run it as root of a user namespace that maps those ids: an id it does not map raises
    ``OSError`` before anything changes. Entries apply in order, so a later one for the same path
    wins, and a regular file keeps the set-user-ID and set-group-ID bits that the chown clears. A
    symbolic link is never followed: a link gets the owner itself, and a path below a link or a
    file names nothing, as do a missing path and one on another filesystem. A path at or below
    one of :data:`MOUNTED_OVER` is left alone, since the sandbox never sees it. Returns the paths
    of the entries skipped for either reason.
    """
    mapped = {name: _mapped_ids(f"/proc/self/{name}_map") for name in ("uid", "gid")}
    for uid, gid, parts in entries:
        for name, wanted in (("uid", uid), ("gid", gid)):
            if not any(start <= wanted < start + count for start, count in mapped[name]):
                ranges = ", ".join(f"{start}-{start + count - 1}" for start, count in mapped[name])
                raise OSError(
                    errno.EINVAL,
                    f"the file owners manifest gives {_shown(parts)} to {name} {wanted}, which "
                    f"this user namespace does not map (its {name}s: {ranges})",
                )
    root = os.open(rootfs, _DIRECTORY)
    chain: list[tuple[bytes, int]] = []  # the open directories of the last parent, from the root
    skipped = []
    try:
        device = os.fstat(root).st_dev
        for uid, gid, parts in entries:
            if any(parts[: len(mount)] == mount for mount in _MOUNTED_OVER):
                skipped.append(_shown(parts))
                continue
            try:
                parent = _open_parent(root, device, parts[:-1], chain)
                if not parts:
                    os.chown(root, uid, gid)
                    continue
                if parent is None:
                    skipped.append(_shown(parts))
                    continue
                info = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
                if info.st_dev != device:
                    skipped.append(_shown(parts))
                    continue
                os.chown(parts[-1], uid, gid, dir_fd=parent, follow_symlinks=False)
                if stat.S_ISREG(info.st_mode) and info.st_mode & _SET_ID:
                    _keep_mode(parent, parts[-1], info)
            except OSError as error:
                if error.errno not in _NAMES_NOTHING:
                    detail = error.strerror or str(error)
                    raise OSError(
                        error.errno, f"cannot give {_shown(parts)} to {uid}:{gid}: {detail}"
                    ) from None
                skipped.append(_shown(parts))
    finally:
        for _, fd in chain:
            os.close(fd)
        os.close(root)
    return skipped


def file_owners_report(skipped: list[str], total: int) -> str:
    """A line that names the paths of the ``total`` entries of a file owners manifest that
    :func:`apply_file_owners` skipped."""
    shown = ", ".join(skipped[:_REPORTED]) + (", ..." if len(skipped) > _REPORTED else "")
    mounts = ", ".join(MOUNTED_OVER[:-1]) + f" or {MOUNTED_OVER[-1]}"
    return (
        f"{FILE_OWNERS_REPORT}skipped {len(skipped)} of {total} entries, whose paths name "
        "nothing in the root filesystem (missing, below a link or a file, or on another "
        f"filesystem) or lie where the sandbox mounts its own (at or below {mounts}): {shown}"
    )


def _keep_mode(parent: int, name: bytes, info: os.stat_result) -> None:
    """Give the file ``name`` of the directory ``parent`` the mode ``info`` read, if it is still
    that file: a chown clears its set-user-ID and set-group-ID bits, which Docker keeps."""
    fd = os.open(name, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
    try:
        now = os.fstat(fd)
        if stat.S_ISREG(now.st_mode) and (now.st_dev, now.st_ino) == (info.st_dev, info.st_ino):
            os.chmod(f"/proc/self/fd/{fd}", stat.S_IMODE(info.st_mode))  # that inode, not a path
    finally:
        os.close(fd)


def _open_parent(
    root: int, device: int, names: tuple[bytes, ...], chain: list[tuple[bytes, int]]
) -> int | None:
    """A descriptor of the directory ``names`` below ``root``, or None when that names no
    directory of ``root``'s filesystem. ``chain`` holds the directories that the last call opened,
    which serve the next as far as the two paths share them."""
    shared = 0
    while shared < min(len(chain), len(names)) and chain[shared][0] == names[shared]:
        shared += 1
    for _, fd in chain[shared:]:
        os.close(fd)
    del chain[shared:]
    for name in names[shared:]:
        try:
            fd = os.open(name, _DIRECTORY, dir_fd=chain[-1][1] if chain else root)
        except OSError as error:
            if error.errno in _NAMES_NOTHING:
                return None
            raise
        if os.fstat(fd).st_dev != device:
            os.close(fd)
            return None
        chain.append((name, fd))
    return chain[-1][1] if chain else root


def _mapped_ids(path: str) -> list[tuple[int, int]]:
    """The ids that a ``uid_map`` or ``gid_map`` maps: the first of each extent, and how many."""
    with open(path) as stream:
        extents = [line.split() for line in stream if line.strip()]
    return [(int(inside), int(count)) for inside, _, count in extents]


def _shown(parts: tuple[bytes, ...]) -> str:
    return "/" + b"/".join(parts).decode(errors="replace")


def _join(argv: list[str]) -> int:
    """The ``join`` form: a pod service below the holder ``argv[0]``."""
    holder, options, command = argv[0], argv[1 : argv.index("--")], argv[argv.index("--") + 1 :]
    hostname, rlimits, shm, userns, file_owners = None, [], None, None, None
    while options:
        option = options.pop(0)
        if option == "--hostname":
            hostname = options.pop(0)
        elif option == "--rlimit":
            name, _, values = options.pop(0).partition("=")
            soft, _, hard = values.partition(":")
            rlimits.append((name, int(soft), int(hard)))
        elif option == "--shm":
            directory, _, size = options.pop(0).rpartition(":")
            shm = (directory, int(size))
        elif option == "--userns":
            userns = options.pop(0)
            mode, _, ids = userns.partition("=")
            uid, _, gid = ids.partition(":")
            if not (
                userns in ("root", "subids")
                or (mode == "caller" and uid.isdigit() and gid.isdigit())
            ):
                raise OSError(22, f"unknown user namespace {userns}")
        elif option == "--file-owners":
            manifest, rootfs = options.pop(0), options.pop(0)
            file_owners = (read_file_owners(manifest), rootfs)  # read as the caller, who wrote it
        else:
            raise OSError(22, f"unknown option {option}")
    # The user namespace first: it owns the network namespace.
    _setns(f"/proc/{holder}/ns/user", CLONE_NEWUSER)
    _setns(f"/proc/{holder}/ns/net", CLONE_NEWNET)
    if file_owners is not None and (skipped := apply_file_owners(*file_owners)):  # as pod root
        print(file_owners_report(skipped, len(file_owners[0])), file=sys.stderr, flush=True)
    _check(_libc.unshare(CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWIPC | CLONE_NEWUTS), "unshare")
    _check(_libc.mount(b"none", b"/", None, MS_REC | MS_PRIVATE, None), "make / private")
    # The service writes to a pipe this process copies to its own output, as
    # Docker's shim does: what the service opens as /dev/stdout is the pipe.
    output_read, output_write = os.pipe()
    alive_read, alive_write = os.pipe()
    init = os.fork()
    if init == 0:
        os.close(alive_write)
        os.close(output_read)
        try:
            _die_with_parent()
            if select.select([alive_read], [], [], 0)[0]:
                os._exit(1)
            os.close(alive_read)
            os.dup2(output_write, 1)
            os.dup2(output_write, 2)
            os.close(output_write)
            os._exit(_service_init(hostname, rlimits, shm, userns, command))
        except OSError as error:
            print(f"hpc-sandbox: {error}", file=sys.stderr)
            os._exit(1)
    os.close(alive_read)
    os.close(output_write)
    # Until everything in the service is gone (no threads here: this process
    # can no longer start any, its children being in another PID namespace).
    _copy(output_read, 1)
    return _wait(init)


def _copy(source: int, target: int) -> None:
    """Copy ``source`` to ``target`` until its end, dropping what cannot be written."""
    while chunk := os.read(source, 65536):
        view = memoryview(chunk)
        while view:
            try:
                written = os.write(target, view)
            except OSError:
                break
            view = view[written:]


def _service_init(
    hostname: str | None,
    rlimits: list[tuple[str, int, int]],
    shm: tuple[str, int] | None,
    userns: str | None,
    command: list[str],
) -> int:
    """PID 1 of a service: set it up, run COMMAND as a child, pass on signals, reap."""
    _check(
        _libc.mount(b"proc", b"/proc", b"proc", MS_NOSUID | MS_NODEV | MS_NOEXEC, None),
        "mount /proc",
    )
    if hostname:
        name = hostname.encode()
        _check(_libc.sethostname(name, len(name)), "sethostname")
    for name, soft, hard in rlimits:
        _set_rlimit(name, soft, hard)
    if shm is not None:
        directory, size = shm
        options = f"size={size},mode=1777".encode()
        _check(
            _libc.mount(b"tmpfs", directory.encode(), b"tmpfs", MS_NOSUID | MS_NODEV, options),
            f"mount tmpfs at {directory}",
        )
    # For a namespace the init maps: the child made it, and its maps are written.
    unshared_read, unshared_write = os.pipe()
    mapped_read, mapped_write = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(unshared_read)
        os.close(mapped_write)
        try:
            _die_with_parent()
            if userns is not None:
                _nested_userns(userns, unshared_write, mapped_read)
            for ignored in (signal.SIGPIPE, signal.SIGXFSZ):
                signal.signal(ignored, signal.SIG_DFL)
            os.execvp(command[0], command)
        except OSError as error:
            print(f"hpc-sandbox: cannot run {command[0]}: {error}", file=sys.stderr)
        os._exit(127)
    os.close(unshared_write)
    os.close(mapped_read)
    if userns == "subids":
        _map_like_the_pod(child, unshared_read, mapped_write)
    os.close(unshared_read)
    os.close(mapped_write)
    for forwarded in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(forwarded, lambda signum, _frame: _forward(child, signum))
    status = 0
    while True:  # reap orphans until the command itself exits
        try:
            pid, raw = os.waitpid(-1, 0)
        except InterruptedError:
            continue
        except ChildProcessError:
            break
        if pid == child:
            status = 128 + os.WTERMSIG(raw) if os.WIFSIGNALED(raw) else os.WEXITSTATUS(raw)
            break
    return status


def _nested_userns(mode: str, unshared: int, mapped: int) -> None:
    """In the init's child: make the user namespace that ``--userns MODE`` asks for."""
    _check(_libc.unshare(CLONE_NEWUSER), "unshare (nested user namespace)")
    if mode == "subids":  # the init maps it: only a process outside may write several extents
        os.write(unshared, b"1")
        if os.read(mapped, 1) != b"1":
            raise OSError(1, "the service's user namespace could not be mapped")
        # The host's supplementary groups stay, unmapped, as in the other modes: the runtime enters
        # the caller's directory and binds the service's mounts through them (project storage that
        # only a group opens), and a switch to another user drops them.
        return
    uid, gid = mode.removeprefix("caller=").split(":") if mode != "root" else ("0", "0")
    _write("/proc/self/setgroups", "deny")
    _write("/proc/self/uid_map", f"{uid} 0 1")
    _write("/proc/self/gid_map", f"{gid} 0 1")


def _map_like_the_pod(child: int, unshared: int, mapped: int) -> None:
    """Map the child's new user namespace as the pod's is, each id to itself: root to the pod's
    root, the caller, and the subordinate ids to themselves."""
    if os.read(unshared, 1) != b"1":
        return  # the child failed first; its status and output say why
    try:
        for name in ("uid_map", "gid_map"):
            with open(f"/proc/self/{name}") as stream:
                extents = [line.split() for line in stream if line.strip()]
            mirror = "".join(f"{inside} {inside} {count}\n" for inside, _, count in extents)
            _write(f"/proc/{child}/{name}", mirror)
    except OSError as error:
        print(f"hpc-sandbox: cannot map the service's user namespace: {error}", file=sys.stderr)
        os.write(mapped, b"0")
        return
    os.write(mapped, b"1")


def _forward(child: int, signum: int) -> None:
    try:
        os.kill(child, signum)
    except ProcessLookupError:
        pass


def _wait(child: int) -> int:
    while True:
        try:
            _, status = os.waitpid(child, 0)
            break
        except InterruptedError:
            continue
    if os.WIFSIGNALED(status):
        return 128 + os.WTERMSIG(status)
    return os.WEXITSTATUS(status)


def cpu_list(text: str) -> set[int]:
    """The CPUs that a list such as ``0,2-3`` names; ``ValueError`` for anything else."""
    cpus: set[int] = set()
    for part in text.split(","):
        first, dash, last = part.partition("-")
        if not first.isdigit() or (dash and not last.isdigit()):
            raise ValueError(f"invalid CPU list {text!r}")
        cpus.update(range(int(first), int(last if dash else first) + 1))
    return cpus


def _exec(parent: int, argv: list[str]) -> int:
    """The ``exec`` form: COMMAND in this process's place, dying with ``parent``."""
    options, command = argv[: argv.index("--")], argv[argv.index("--") + 1 :]
    cpus = None
    while options:
        option = options.pop(0)
        if option == "--cpus" and options:
            cpus = cpu_list(options.pop(0))
        else:
            raise OSError(22, f"unknown option {option}")
    if not command:
        raise OSError(22, "no command to run")
    _die_with_parent()
    if os.getppid() != parent:
        return 1  # the parent died before PR_SET_PDEATHSIG took effect
    if cpus is not None:
        os.sched_setaffinity(0, cpus)
    for ignored in (signal.SIGPIPE, signal.SIGXFSZ):  # ignored by Python, not by runtimes
        signal.signal(ignored, signal.SIG_DFL)
    os.execvp(command[0], command)
    return 127  # not reached: execvp raises when it fails


def main(argv: list[str]) -> int:
    if len(argv) >= 4 and argv[1] == "exec" and "--" in argv[2:]:
        return _exec(int(argv[0]), argv[2:])
    if len(argv) >= 5 and argv[1] == "join" and "--" in argv[3:]:
        _die_with_parent()
        if os.getppid() != int(argv[0]):
            return 1  # the parent died before PR_SET_PDEATHSIG took effect
        return _join(argv[2:])
    if len(argv) == 4 and argv[1] == "file-owners":
        _die_with_parent()
        if os.getppid() != int(argv[0]):
            return 1
        entries = read_file_owners(argv[2])
        if skipped := apply_file_owners(entries, argv[3]):
            print(file_owners_report(skipped, len(entries)))
        return 0
    if len(argv) < 4 or argv[2] != "--" or argv[1] not in ("host", "none"):
        print(
            "usage: anchor.py PARENT_PID host|none -- COMMAND [ARG...]\n"
            "       anchor.py PARENT_PID join HOLDER_PID [OPTION...] -- COMMAND [ARG...]\n"
            "       anchor.py PARENT_PID file-owners MANIFEST ROOTFS\n"
            "       anchor.py PARENT_PID exec [--cpus LIST] -- COMMAND [ARG...]",
            file=sys.stderr,
        )
        return 2
    parent, network, command = int(argv[0]), argv[1], argv[3:]
    _die_with_parent()
    if os.getppid() != parent:
        return 1  # the parent died before PR_SET_PDEATHSIG took effect
    uid, gid = os.getuid(), os.getgid()
    flags = CLONE_NEWUSER | CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWIPC
    if network == "none":
        flags |= CLONE_NEWNET
    _check(_libc.unshare(flags), "unshare (are unprivileged user namespaces enabled?)")
    _write("/proc/self/setgroups", "deny")
    _write("/proc/self/uid_map", f"{uid} {uid} 1")
    _write("/proc/self/gid_map", f"{gid} {gid} 1")
    _check(_libc.mount(b"none", b"/", None, MS_REC | MS_PRIVATE, None), "make / private")
    if network == "none":
        _loopback_up()
    # The init process watches this pipe: EOF means this process is already gone.
    alive_read, alive_write = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(alive_write)
        _die_with_parent()
        if select.select([alive_read], [], [], 0)[0]:
            os._exit(1)
        os.close(alive_read)
        _check(
            _libc.mount(b"proc", b"/proc", b"proc", MS_NOSUID | MS_NODEV | MS_NOEXEC, None),
            "mount /proc",
        )
        for ignored in (signal.SIGPIPE, signal.SIGXFSZ):  # ignored by Python, not by containers
            signal.signal(ignored, signal.SIG_DFL)
        try:
            os.execvp(command[0], command)
        except OSError as error:
            print(f"hpc-sandbox: cannot run {command[0]}: {error}", file=sys.stderr)
            os._exit(127)
    os.close(alive_read)
    while True:
        try:
            _, status = os.waitpid(child, 0)
            break
        except InterruptedError:
            continue
    if os.WIFSIGNALED(status):
        return 128 + os.WTERMSIG(status)
    return os.WEXITSTATUS(status)


def _message(error: Exception) -> str:
    """``error`` as this script reports it: an ``OSError`` without the ``[Errno N]`` that
    ``str()`` puts first."""
    if not isinstance(error, OSError) or not error.strerror:
        return str(error)
    names = [repr(name) for name in (error.filename, error.filename2) if name is not None]
    return f"{error.strerror}: {' -> '.join(names)}" if names else error.strerror


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except (OSError, ValueError) as error:
        print(f"hpc-sandbox: {_message(error)}", file=sys.stderr)
        sys.exit(1)
