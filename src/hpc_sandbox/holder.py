"""Hold a pod's user and network namespaces, and relay its upstreams.

:mod:`hpc_sandbox.pods` runs this file as a script in a fresh interpreter::

    python -I -S holder.py PARENT_PID CONTROL_FD [POD_ID [USERS]]

(``POD_ID`` only names the process for janitors). It

1. arranges to be SIGKILLed when its parent (the provider process) dies, and
   exits at once if that already happened;
2. creates new user and network namespaces. With USERS ``single``, the
   default, it maps the calling user and group to root (``setgroups``
   denied). With ``subids``, it answers ``unshared`` on the control socket and
   waits for ``mapped``: meanwhile the provider maps root to the calling user
   and group, and 1 to N to its subordinate ids, with ``newuidmap`` and
   ``newgidmap``, which leave ``setgroups`` allowed. With ``nested:FD``, it
   first joins the user namespace that the descriptor FD names, one mapped so,
   and makes its own inside it, which a child it forks for this, and which
   stays outside, maps as that one is, each id to itself: only a process
   outside may write more than one extent. Then it brings the loopback
   interface up;
3. lets the pod's root bind ports below 1024 and every mapped group open ICMP
   echo sockets, as Docker does for containers: services sit in nested user
   namespaces, which hold no capability over this network;
4. reads ``relay PORT PATH`` lines from the control socket, binds each
   ``127.0.0.1:PORT`` without ``SO_REUSEPORT`` (so nothing in the pod can share
   the port), forwards each connection to the host's Unix socket ``PATH``, and
   answers ``ready`` once all are bound (``start`` ends the list);
5. keeps the file descriptors a ``hold`` message passes with ``SCM_RIGHTS``
   (the write end of slirp4netns's exit pipe, so slirp4netns dies with this
   process) until ``release``;
6. exits when the control socket closes.

The pod's services and the commands entered into them join these namespaces,
so this process is the pod: when it dies, the pod's network is gone.
"""

from __future__ import annotations

import asyncio
import ctypes
import errno
import fcntl
import os
import signal
import socket
import struct
import sys
import threading

CLONE_NEWUSER = 0x10000000
CLONE_NEWNET = 0x40000000
PR_SET_PDEATHSIG = 1
SIOCGIFFLAGS = 0x8913
SIOCSIFFLAGS = 0x8914
IFF_UP = 0x1
_COPY_CHUNK = 256 * 1024

_libc = ctypes.CDLL(None, use_errno=True)


def _check(result: int, what: str) -> None:
    if result != 0:
        code = ctypes.get_errno()
        raise OSError(code, f"{what}: {os.strerror(code)}")


def _write(path: str, text: str) -> None:
    """Write ``text`` with one system call, as the kernel's id maps need."""
    fd = os.open(path, os.O_WRONLY | os.O_CLOEXEC)
    try:
        os.write(fd, text.encode())
    finally:
        os.close(fd)


def _loopback_up() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        request = struct.pack("16sh", b"lo", 0)
        flags = struct.unpack("16sh", fcntl.ioctl(sock, SIOCGIFFLAGS, request))[1]
        fcntl.ioctl(sock, SIOCSIFFLAGS, struct.pack("16sh", b"lo", flags | IFF_UP))


def _read_line(control: socket.socket) -> str:
    """One line from the control socket, a byte at a time: what follows it is the serve loop's."""
    line = b""
    while not line.endswith(b"\n"):
        byte = control.recv(1)
        if not byte:
            return ""
        line += byte
    return line[:-1].decode(errors="replace")


def _highest_mapped(path: str) -> int:
    """The highest id that a ``uid_map`` or ``gid_map`` of this process maps."""
    with open(path) as stream:
        extents = [line.split() for line in stream if line.strip()]
    return max((int(inside) + int(count) - 1 for inside, _, count in extents), default=0)


def _nest(fd: int) -> None:
    """Join the user namespace ``fd`` and make new user and network namespaces inside it, mapped
    by a child that stays outside as the joined one is, each id to itself."""
    _check(_libc.setns(fd, CLONE_NEWUSER), "setns (the user namespace of the subordinate ids)")
    os.close(fd)
    unshared_read, unshared_write = os.pipe()
    mapper = os.fork()
    if mapper == 0:  # root of the joined namespace, which may write any of its ids
        os.close(unshared_write)
        try:
            _check(_libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0), "prctl")
            if os.read(unshared_read, 1) != b"1":
                os._exit(1)
            for name in ("uid_map", "gid_map"):
                with open(f"/proc/self/{name}") as stream:
                    extents = [line.split() for line in stream if line.strip()]
                mirror = "".join(f"{inside} {inside} {count}\n" for inside, _, count in extents)
                _write(f"/proc/{os.getppid()}/{name}", mirror)
        except OSError as error:
            print(f"hpc-sandbox: cannot map the pod's user namespace: {error}", file=sys.stderr)
            os._exit(1)
        os._exit(0)
    os.close(unshared_read)
    _check(_libc.unshare(CLONE_NEWUSER | CLONE_NEWNET), "unshare (are user namespaces enabled?)")
    os.write(unshared_write, b"1")
    os.close(unshared_write)
    _, status = os.waitpid(mapper, 0)
    if os.waitstatus_to_exitcode(status) != 0 or os.getuid() != 0:  # the caller is root once mapped
        raise OSError(errno.EPERM, "the pod's user namespace could not be mapped")


def _enter_namespaces(users: str, control: socket.socket) -> list[str]:
    """Create the pod's namespaces; return what could not be set up (for the log)."""
    uid, gid = os.getuid(), os.getgid()
    if users.startswith("nested:"):
        _nest(int(users.partition(":")[2]))
    else:
        _check(
            _libc.unshare(CLONE_NEWUSER | CLONE_NEWNET), "unshare (are user namespaces enabled?)"
        )
    if users == "subids":
        control.sendall(b"unshared\n")
        if _read_line(control) != "mapped" or os.getuid() != 0:  # the caller is root once mapped
            raise OSError(errno.EPERM, "the provider did not map the pod's user namespace")
    elif users == "single":
        _write("/proc/self/setgroups", "deny")
        _write("/proc/self/uid_map", f"0 {uid} 1")
        _write("/proc/self/gid_map", f"0 {gid} 1")
    _loopback_up()
    problems = []
    for path, value in (
        ("/proc/sys/net/ipv4/ip_unprivileged_port_start", "0"),
        ("/proc/sys/net/ipv4/ping_group_range", f"0 {_highest_mapped('/proc/self/gid_map')}"),
    ):
        try:
            _write(path, value)
        except OSError as error:
            problems.append(f"{path}: {error.strerror}")
    return problems


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Copy one direction; at its end, pass the end on and leave the other direction open."""
    while data := await reader.read(_COPY_CHUNK):
        writer.write(data)
        await writer.drain()
    if writer.can_write_eof():
        writer.write_eof()


def _relay(path: str):
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            upstream_reader, upstream_writer = await asyncio.open_unix_connection(path)
        except OSError as error:
            print(f"hpc-sandbox: relay to {path}: {error}", file=sys.stderr, flush=True)
            writer.close()
            return
        directions = [
            asyncio.ensure_future(_pipe(reader, upstream_writer)),
            asyncio.ensure_future(_pipe(upstream_reader, writer)),
        ]
        try:
            # Both directions to their end (a half-closed one keeps the other
            # open), or either one's failure ends both.
            _, pending = await asyncio.wait(directions, return_when=asyncio.FIRST_EXCEPTION)
            for task in pending:
                task.cancel()
            await asyncio.gather(*directions, return_exceptions=True)
        finally:
            for side in (writer, upstream_writer):
                side.close()

    return handle


async def _listen(handler, port: int) -> asyncio.Server:
    """Listen on ``port`` of both loopback addresses, so that no service can take
    the IPv6 one (``localhost`` may resolve to it first); IPv4 only without IPv6."""
    try:
        return await asyncio.start_server(handler, ["127.0.0.1", "::1"], port)
    except OSError as error:
        if error.errno not in (errno.EADDRNOTAVAIL, errno.EAFNOSUPPORT):
            raise
    return await asyncio.start_server(handler, "127.0.0.1", port)


def _control_reader(control: socket.socket, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue):
    """Read control lines (and the descriptors passed with them) in a thread."""
    buffer = b""
    while True:
        try:
            data, fds, _, _ = socket.recv_fds(control, 4096, 16)
        except OSError:
            data, fds = b"", []
        if not data:
            loop.call_soon_threadsafe(queue.put_nowait, (None, []))
            return
        buffer += data
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            loop.call_soon_threadsafe(queue.put_nowait, (line.decode(), fds))
            fds = []


async def _serve(control: socket.socket, problems: list[str]) -> None:
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()
    threading.Thread(target=_control_reader, args=(control, loop, queue), daemon=True).start()
    servers = []
    held: list[int] = []

    def answer(text: str) -> None:
        control.sendall(text.encode() + b"\n")

    while True:
        line, fds = await queue.get()
        if line is None:  # the provider closed the control socket: the pod is over
            return
        command, _, rest = line.partition(" ")
        if command == "relay":
            port, path = rest.split(" ", 1)
            try:
                servers.append(await _listen(_relay(path), int(port)))
            except OSError as error:
                answer(f"error relay {port}: {error.strerror}")
        elif command == "start":
            answer("ready " + "; ".join(problems))
        elif command == "hold":
            held.extend(fds)
            answer("held")
        elif command == "release":
            for fd in held:
                os.close(fd)
            held.clear()
            answer("released")
        else:
            for fd in fds:
                os.close(fd)
            answer(f"error unknown command {command}")


def main(argv: list[str]) -> int:
    users = argv[3] if len(argv) == 4 else "single"
    nested, _, fd = users.partition(":")
    if len(argv) not in (2, 3, 4) or (
        users not in ("single", "subids") and not (nested == "nested" and fd.isdigit())
    ):
        print(
            "usage: holder.py PARENT_PID CONTROL_FD [POD_ID [single|subids|nested:FD]]",
            file=sys.stderr,
        )
        return 2
    parent, control_fd = int(argv[0]), int(argv[1])
    _check(_libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0), "prctl(PR_SET_PDEATHSIG)")
    if os.getppid() != parent:
        return 1  # the parent died before PR_SET_PDEATHSIG took effect
    control = socket.socket(fileno=control_fd)
    problems = _enter_namespaces(users, control)
    asyncio.run(_serve(control, problems))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except OSError as error:
        print(f"hpc-sandbox: {error}", file=sys.stderr)
        sys.exit(1)
