"""Create a sandbox's namespaces and run its container as their PID 1.

:mod:`hpc_sandbox.enroot` runs this file as a script in a fresh interpreter::

    python -I -S anchor.py PARENT_PID NETWORK -- COMMAND [ARG...]
    python -I -S anchor.py PARENT_PID join HOLDER_PID [OPTION...] -- COMMAND [ARG...]

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
then runs COMMAND as its child, in a nested user namespace mapping root to
root with ``--userns``, with its output through a pipe this process copies to
its own (so the service cannot truncate the log, and no file size limit of the
service applies to it). Root in a nested user namespace holds no capability
over the pod's network. The init forwards SIGTERM, SIGINT and SIGHUP to the
child, reaps orphans, and exits with the child's status; the runtime is never
PID 1, so killing the init always ends the service.
"""

from __future__ import annotations

import ctypes
import fcntl
import os
import resource
import select
import signal
import socket
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

_libc = ctypes.CDLL(None, use_errno=True)


def _check(result: int, what: str) -> None:
    if result != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, f"{what}: {os.strerror(errno)}")


def _write(path: str, text: str) -> None:
    with open(path, "w") as stream:
        stream.write(text)


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


def _join(argv: list[str]) -> int:
    """The ``join`` form: a pod service below the holder ``argv[0]``."""
    holder, options, command = argv[0], argv[1 : argv.index("--")], argv[argv.index("--") + 1 :]
    hostname, rlimits, shm, nested = None, [], None, False
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
            nested = True
        else:
            raise OSError(22, f"unknown option {option}")
    # The user namespace first: it owns the network namespace.
    _setns(f"/proc/{holder}/ns/user", CLONE_NEWUSER)
    _setns(f"/proc/{holder}/ns/net", CLONE_NEWNET)
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
            os._exit(_service_init(hostname, rlimits, shm, nested, command))
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
    nested: bool,
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
    child = os.fork()
    if child == 0:
        try:
            _die_with_parent()
            if nested:
                _check(_libc.unshare(CLONE_NEWUSER), "unshare (nested user namespace)")
                _write("/proc/self/setgroups", "deny")
                _write("/proc/self/uid_map", "0 0 1")
                _write("/proc/self/gid_map", "0 0 1")
            for ignored in (signal.SIGPIPE, signal.SIGXFSZ):
                signal.signal(ignored, signal.SIG_DFL)
            os.execvp(command[0], command)
        except OSError as error:
            print(f"hpc-sandbox: cannot run {command[0]}: {error}", file=sys.stderr)
        os._exit(127)
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


def main(argv: list[str]) -> int:
    if len(argv) >= 5 and argv[1] == "join" and "--" in argv[3:]:
        _die_with_parent()
        if os.getppid() != int(argv[0]):
            return 1  # the parent died before PR_SET_PDEATHSIG took effect
        return _join(argv[2:])
    if len(argv) < 4 or argv[2] != "--" or argv[1] not in ("host", "none"):
        print(
            "usage: anchor.py PARENT_PID host|none -- COMMAND [ARG...]\n"
            "       anchor.py PARENT_PID join HOLDER_PID [OPTION...] -- COMMAND [ARG...]",
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


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except OSError as error:
        print(f"hpc-sandbox: {error}", file=sys.stderr)
        sys.exit(1)
