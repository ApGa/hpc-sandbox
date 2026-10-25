"""Hold a pod's user and network namespaces, and relay its upstreams and exposed ports.

:mod:`hpc_sandbox.pods` runs this file as a script in a fresh interpreter::

    python -I -S holder.py PARENT_PID CONTROL_FD [POD_ID]

(``POD_ID`` only names the process for janitors). It

1. arranges to be SIGKILLed when its parent (the provider process) dies, and
   exits at once if that already happened;
2. creates new user and network namespaces, mapping the calling user and group
   to root (``setgroups`` denied), and brings the loopback interface up;
3. lets the pod's root bind ports below 1024 and every mapped group open ICMP
   echo sockets, as Docker does for containers: services sit in nested user
   namespaces, which hold no capability over this network;
4. reads ``relay PORT PATH`` lines from the control socket, binds each
   ``127.0.0.1:PORT`` without ``SO_REUSEPORT`` (so nothing in the pod can share
   the port), forwards each connection to the host's Unix socket ``PATH``, and
   answers ``ready`` once all are bound (``start`` ends the list);
5. for an ``expose PORT`` line, which passes a directory's file descriptor with
   ``SCM_RIGHTS``, listens on the Unix socket ``PORT.sock`` in that directory,
   with no access for other users, and forwards each connection to
   ``127.0.0.1:PORT`` in the pod, or to ``[::1]:PORT`` when nothing listens at
   127.0.0.1; it answers ``exposed PORT``, also for a port it already exposes.
   This process stays in the host's mount namespace, so the socket is on the
   host, and it binds the socket through the descriptor, so the directory's
   path may be longer than a socket address can hold;
6. keeps the file descriptors a ``hold`` message passes with ``SCM_RIGHTS``
   (the write end of slirp4netns's exit pipe, so slirp4netns dies with this
   process) until ``release``;
7. exits when the control socket closes.

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
    with open(path, "w") as stream:
        stream.write(text)


def _loopback_up() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        request = struct.pack("16sh", b"lo", 0)
        flags = struct.unpack("16sh", fcntl.ioctl(sock, SIOCGIFFLAGS, request))[1]
        fcntl.ioctl(sock, SIOCSIFFLAGS, struct.pack("16sh", b"lo", flags | IFF_UP))


def _enter_namespaces() -> list[str]:
    """Create the pod's namespaces; return what could not be set up (for the log)."""
    uid, gid = os.getuid(), os.getgid()
    _check(_libc.unshare(CLONE_NEWUSER | CLONE_NEWNET), "unshare (are user namespaces enabled?)")
    _write("/proc/self/setgroups", "deny")
    _write("/proc/self/uid_map", f"0 {uid} 1")
    _write("/proc/self/gid_map", f"0 {gid} 1")
    _loopback_up()
    problems = []
    for path, value in (
        ("/proc/sys/net/ipv4/ip_unprivileged_port_start", "0"),
        ("/proc/sys/net/ipv4/ping_group_range", "0 0"),
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


async def _splice(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    other_reader: asyncio.StreamReader,
    other_writer: asyncio.StreamWriter,
) -> None:
    """Copy a connection both ways until both directions end (a half-closed one
    keeps the other open), or either one fails, which ends both."""
    directions = [
        asyncio.ensure_future(_pipe(reader, other_writer)),
        asyncio.ensure_future(_pipe(other_reader, writer)),
    ]
    try:
        _, pending = await asyncio.wait(directions, return_when=asyncio.FIRST_EXCEPTION)
        for task in pending:
            task.cancel()
        await asyncio.gather(*directions, return_exceptions=True)
    finally:
        for side in (writer, other_writer):
            side.close()


def _relay(path: str):
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            upstream_reader, upstream_writer = await asyncio.open_unix_connection(path)
        except OSError as error:
            print(f"hpc-sandbox: relay to {path}: {error}", file=sys.stderr, flush=True)
            writer.close()
            return
        await _splice(reader, writer, upstream_reader, upstream_writer)

    return handle


async def _connect_loopback(port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Connect to ``port`` at 127.0.0.1 or, when nothing listens there, at ::1, where
    a service that listens on ``localhost`` alone may be (``localhost`` may resolve to
    ::1 first)."""
    try:
        return await asyncio.open_connection("127.0.0.1", port)
    except ConnectionRefusedError as refused:
        try:
            return await asyncio.open_connection("::1", port)
        except OSError:  # refused too, or no IPv6
            raise refused from None


def _forward(port: int):
    """Forward a connection to an exposed port's socket to that port in the pod."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            service_reader, service_writer = await _connect_loopback(port)
        except OSError as error:
            if not isinstance(error, ConnectionRefusedError):  # nothing listens yet: no news
                print(f"hpc-sandbox: exposed port {port}: {error}", file=sys.stderr, flush=True)
            writer.close()
            return
        await _splice(reader, writer, service_reader, service_writer)

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


async def _listen_in(handler, directory: int, name: str) -> asyncio.Server:
    """Listen on the Unix socket ``name`` in the directory open as ``directory``,
    accessible to its owner only. The directory goes by its descriptor, so its path
    may be longer than a socket address holds; keep the descriptor open while the
    server runs, since the server knows its socket by that path."""
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        mask = os.umask(0o077)
        try:
            listener.bind(f"/proc/self/fd/{directory}/{name}")
        finally:
            os.umask(mask)
        return await asyncio.start_unix_server(handler, sock=listener)
    except BaseException:
        listener.close()
        raise


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
    exposed: dict[int, int] = {}  # port -> the descriptor of its socket's directory

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
        elif command == "expose":
            try:
                port = int(rest)
                if port not in exposed:
                    if len(fds) != 1:
                        raise ValueError("needs the descriptor of one directory")
                    servers.append(await _listen_in(_forward(port), fds[0], f"{port}.sock"))
                    exposed[port] = fds.pop()
                answer(f"exposed {port}")
            except (OSError, ValueError) as error:
                answer(f"error expose {rest}: {getattr(error, 'strerror', None) or error}")
            finally:
                for fd in fds:
                    os.close(fd)
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
    if len(argv) not in (2, 3):
        print("usage: holder.py PARENT_PID CONTROL_FD [POD_ID]", file=sys.stderr)
        return 2
    parent, control_fd = int(argv[0]), int(argv[1])
    _check(_libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0), "prctl(PR_SET_PDEATHSIG)")
    if os.getppid() != parent:
        return 1  # the parent died before PR_SET_PDEATHSIG took effect
    control = socket.socket(fileno=control_fd)
    problems = _enter_namespaces()
    asyncio.run(_serve(control, problems))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except OSError as error:
        print(f"hpc-sandbox: {error}", file=sys.stderr)
        sys.exit(1)
