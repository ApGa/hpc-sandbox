"""Create a sandbox's namespaces and run its container as their PID 1.

:mod:`hpc_sandbox.enroot` runs this file as a script in a fresh interpreter::

    python -I -S anchor.py PARENT_PID NETWORK -- COMMAND [ARG...]

It

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
"""

from __future__ import annotations

import ctypes
import fcntl
import os
import select
import signal
import socket
import struct
import sys

CLONE_NEWNS = 0x00020000
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


def main(argv: list[str]) -> int:
    if len(argv) < 4 or argv[2] != "--" or argv[1] not in ("host", "none"):
        print("usage: anchor.py PARENT_PID host|none -- COMMAND [ARG...]", file=sys.stderr)
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
