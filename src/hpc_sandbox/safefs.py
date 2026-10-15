"""File operations in directories that code in a sandbox can change.

A sandbox's ``/tmp`` and root filesystem are host directories that its
processes, running as the calling user, can change at any moment. Host-side
code working there must never follow a symlink a sandbox planted, never block
on a FIFO swapped in, and never recurse as deep as a sandbox can nest
directories, or it could read, write or change permissions anywhere the user
can, or hang. These helpers walk such trees with directory file descriptors
(one per level), ``O_NOFOLLOW`` and ``O_NONBLOCK``, without recursion: a
swapped-in symlink or special file makes an operation fail instead of escaping.

Long operations take an optional :class:`threading.Event`; once it is set,
they stop with :class:`Stopped` at the next entry or chunk of data.
"""

from __future__ import annotations

import errno
import os
import stat
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from shutil import SpecialFileError

from .types import SandboxError

# Open a directory for reading, never through a symlink.
DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_READ = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
_CREATE = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
_WRITE = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NONBLOCK | os.O_CLOEXEC
_NOT_A_DIRECTORY = (errno.ENOTDIR, errno.ELOOP)
_CHUNK = 8 * 1024 * 1024
MAX_DEPTH = 256  # directories deeper than this are refused rather than walked


class Stopped(Exception):
    """The operation was asked to stop."""


def _check(stop: threading.Event | None) -> None:
    if stop is not None and stop.is_set():
        raise Stopped


def open_dir(name: str | Path, dir_fd: int | None = None) -> int:
    """Open a directory, never through a symlink; a mode 000 one is opened up first."""
    try:
        return os.open(name, DIRECTORY, dir_fd=dir_fd)
    except PermissionError:
        set_mode(name, dir_fd, add=0o700)
        return os.open(name, DIRECTORY, dir_fd=dir_fd)


@contextmanager
def opened_dir(name: str | Path, dir_fd: int | None = None) -> Iterator[int]:
    fd = open_dir(name, dir_fd)
    try:
        yield fd
    finally:
        os.close(fd)


def set_mode(
    name: str | Path, dir_fd: int | None, *, add: int = 0, exact: int | None = None
) -> os.stat_result | None:
    """Change the mode of ``name`` itself, never of what a symlink points to.

    Adds the ``add`` bits, or sets ``exact``. Returns the entry's previous
    status, or None if it is a symlink (left alone) or gone.
    """
    try:
        fd = os.open(name, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=dir_fd)
    except FileNotFoundError:
        return None
    try:
        info = os.fstat(fd)
        if stat.S_ISLNK(info.st_mode):
            return None
        mode = exact if exact is not None else stat.S_IMODE(info.st_mode) | add
        if mode != stat.S_IMODE(info.st_mode):
            os.chmod(f"/proc/self/fd/{fd}", mode)  # the inode opened above, not a path
        return info
    finally:
        os.close(fd)


def open_up(fd: int) -> None:
    """Give the owner back full access to the open directory ``fd``."""
    mode = stat.S_IMODE(os.fstat(fd).st_mode)
    if mode & 0o700 != 0o700:
        os.fchmod(fd, mode | 0o700)


def make_dir(name: str, dir_fd: int, mode: int = 0o700) -> int:
    """Open the directory ``name``, creating it, or replacing whatever else is there."""
    for _ in range(3):
        try:
            os.mkdir(name, mode, dir_fd=dir_fd)
        except FileExistsError:
            pass
        try:
            return open_dir(name, dir_fd)
        except OSError as error:
            if error.errno not in _NOT_A_DIRECTORY:
                raise
            remove(name, dir_fd)  # a symlink or file was planted there
    raise SandboxError(f"could not create the directory {name}: it keeps being replaced")


def write_file(name: str, dir_fd: int, data: bytes, mode: int = 0o600) -> None:
    """Create the new file ``name`` with ``data``; fails if anything is already there."""
    with os.fdopen(os.open(name, _CREATE, mode, dir_fd=dir_fd), "wb") as stream:
        stream.write(data)


def listing(dir_fd: int) -> list[str]:
    """The names in the open directory ``dir_fd``."""
    with os.scandir(dir_fd) as entries:
        return [entry.name for entry in entries]


def _copy_data(
    source: int, target: int, stop: threading.Event | None, limit: int | None = None
) -> None:
    """Copy from ``source`` to ``target`` until EOF or ``limit`` bytes, a chunk at a time."""
    copied, sendfile = 0, True
    while limit is None or copied < limit:
        _check(stop)
        count = _CHUNK if limit is None else min(_CHUNK, limit - copied)
        if sendfile:
            try:
                done = os.sendfile(target, source, None, count)
            except OSError as error:
                if copied or error.errno not in (errno.EINVAL, errno.ENOSYS):
                    raise
                sendfile = False  # not supported between these files
                continue
        else:
            data = memoryview(os.read(source, count))
            done = len(data)
            while data:
                data = data[os.write(target, data) :]
        if not done:
            return
        copied += done


def copy_in(source: Path, name: str, dir_fd: int, stop: threading.Event | None = None) -> None:
    """Copy the host file or directory ``source`` to the new entry ``name``.

    Like ``shutil.copytree(symlinks=True)``/``copy2``: a symlink given as
    ``source`` is followed, symlinks below it are copied as symlinks, modes
    and times are kept. Other special files raise SpecialFileError, except
    a character device given as ``source`` itself, which is read to its end
    (``/dev/null`` gives an empty file).
    """
    info = os.stat(source)
    if not stat.S_ISDIR(info.st_mode):
        _copy_in_entry(source, name, dir_fd, info, stop, top=True)
        return
    # Depth-first with one open descriptor per level; a directory gets its
    # mode and times once its contents are complete.
    stack: list[tuple[int, os.stat_result, Iterator[tuple[Path, os.stat_result]]]] = []
    try:
        children = _children(source)
        os.mkdir(name, 0o700, dir_fd=dir_fd)
        stack.append((open_dir(name, dir_fd), info, children))
        while stack:
            _check(stop)
            fd, info, children = stack[-1]
            child = next(children, None)
            if child is None:
                stack.pop()
                try:
                    os.fchmod(fd, stat.S_IMODE(info.st_mode))
                    os.utime(fd, ns=(info.st_atime_ns, info.st_mtime_ns))
                finally:
                    os.close(fd)
                continue
            path, child_info = child
            if not stat.S_ISDIR(child_info.st_mode):
                _copy_in_entry(path, path.name, fd, child_info, stop, top=False)
                continue
            grandchildren = _children(path)
            os.mkdir(path.name, 0o700, dir_fd=fd)
            stack.append((open_dir(path.name, fd), child_info, grandchildren))
    finally:
        for fd, *_ in stack:
            os.close(fd)


def _children(directory: Path) -> Iterator[tuple[Path, os.stat_result]]:
    with os.scandir(directory) as entries:
        return iter([(Path(entry.path), entry.stat(follow_symlinks=False)) for entry in entries])


def _copy_in_entry(
    path: Path,
    name: str,
    dir_fd: int,
    info: os.stat_result,
    stop: threading.Event | None,
    *,
    top: bool,
) -> None:
    if stat.S_ISLNK(info.st_mode):
        os.symlink(os.readlink(path), name, dir_fd=dir_fd)
        return
    if not (stat.S_ISREG(info.st_mode) or (top and stat.S_ISCHR(info.st_mode))):
        raise SpecialFileError(f"cannot upload the special file {path}")
    with path.open("rb") as reader:
        writer = os.open(name, _CREATE, 0o600, dir_fd=dir_fd)
        try:
            _copy_data(reader.fileno(), writer, stop)
            os.fchmod(writer, stat.S_IMODE(info.st_mode))
            os.utime(writer, ns=(info.st_atime_ns, info.st_mtime_ns))
        finally:
            os.close(writer)


def copy_out(name: str, dir_fd: int, target: Path, stop: threading.Event | None = None) -> None:
    """Copy the entry ``name`` to the host path ``target``.

    The caller chose ``target``, so it is treated like ``shutil.copytree``
    and ``copy2`` would: a symlink there is followed, a directory receives a
    directory's contents, nothing replaces an existing file except a file
    (FileExistsError), and a FIFO is refused (SpecialFileError). Below it,
    names come from the sandbox: nothing is read through a symlink or blocks
    on a FIFO, symlinks are copied as symlinks, existing non-directories are
    replaced, special files raise SandboxError, and files are copied up to
    the size they had when opened. The caller gets read access to what it
    copies; modes and times are kept otherwise.
    """
    info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    if stat.S_ISDIR(info.st_mode):
        if os.path.lexists(target) and not target.is_dir():
            raise FileExistsError(f"download target exists and is not a directory: {target}")
    elif target.is_dir():
        raise IsADirectoryError(f"download target is a directory: {target}")
    else:
        _copy_entry(name, dir_fd, target, info, stop, top=True)
        return
    # Depth-first with one open descriptor per level, at most MAX_DEPTH of them.
    stack: list[list] = []  # [descriptor, host directory, status, names or None]
    try:
        stack.append([open_dir(name, dir_fd), target, info, None])
        target.mkdir(exist_ok=True)
        stack[-1][3] = iter(listing(stack[-1][0]))
        while stack:
            _check(stop)
            fd, directory, info, names = stack[-1]
            child = next(names, None)
            if child is None:
                stack.pop()
                os.close(fd)
                os.chmod(directory, stat.S_IMODE(info.st_mode) | 0o700)
                os.utime(directory, ns=(info.st_atime_ns, info.st_mtime_ns))
                continue
            child_info = os.stat(child, dir_fd=fd, follow_symlinks=False)
            child_target = directory / child
            if not stat.S_ISDIR(child_info.st_mode):
                _copy_entry(child, fd, child_target, child_info, stop, top=False)
                continue
            if len(stack) >= MAX_DEPTH:
                raise SandboxError(f"directory tree deeper than {MAX_DEPTH} levels: {name}")
            stack.append([open_dir(child, fd), child_target, child_info, None])
            if child_target.is_symlink() or (child_target.exists() and not child_target.is_dir()):
                child_target.unlink()
            child_target.mkdir(exist_ok=True)
            stack[-1][3] = iter(listing(stack[-1][0]))
    finally:
        for fd, *_ in stack:
            os.close(fd)


def _copy_entry(
    name: str,
    dir_fd: int,
    target: Path,
    info: os.stat_result,
    stop: threading.Event | None,
    *,
    top: bool,
) -> None:
    """Copy one non-directory entry; ``top`` writes through the caller's own target."""
    if stat.S_ISLNK(info.st_mode):
        if not top and (target.is_symlink() or target.exists()):
            target.unlink()
        os.symlink(os.readlink(name, dir_fd=dir_fd), target)
        return
    if not stat.S_ISREG(info.st_mode):
        raise SandboxError(f"cannot copy the special file {name}")
    try:
        source = os.open(name, _READ, dir_fd=dir_fd)
    except PermissionError:
        set_mode(name, dir_fd, add=0o400)  # root in the sandbox could read it
        source = os.open(name, _READ, dir_fd=dir_fd)
    try:
        opened = os.fstat(source)
        if not stat.S_ISREG(opened.st_mode):  # swapped in after the stat
            raise SandboxError(f"cannot copy the special file {name}")
        os.set_blocking(source, True)
        writer = _open_target(target, top=top)
        try:
            _copy_data(source, writer, stop, limit=opened.st_size)
            os.fchmod(writer, stat.S_IMODE(info.st_mode) | 0o400)
            os.utime(writer, ns=(info.st_atime_ns, info.st_mtime_ns))
        finally:
            os.close(writer)
    finally:
        os.close(source)


def _open_target(target: Path, *, top: bool) -> int:
    """Open the host file ``target`` for writing, never blocking on a FIFO."""
    flags = _WRITE
    if not top:  # a name below the caller's target: replace whatever is there
        if target.is_symlink() or (target.exists() and not target.is_dir()):
            target.unlink()
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(target, flags, 0o600)
    except OSError as error:
        if error.errno == errno.ENXIO:  # a FIFO nobody reads
            raise SpecialFileError(f"download target is a named pipe: {target}") from None
        raise
    if stat.S_ISFIFO(os.fstat(fd).st_mode):
        os.close(fd)
        raise SpecialFileError(f"download target is a named pipe: {target}")
    os.set_blocking(fd, True)
    return fd


def remove(
    name: str,
    dir_fd: int,
    stop: threading.Event | None = None,
    *,
    deadline: float | None = None,
) -> None:
    """Remove the entry ``name`` and everything below it, whatever its modes or depth.

    Subdirectories are moved up next to ``name`` before they are emptied, so
    only one directory is open at a time and depth costs nothing. Code in a
    sandbox can keep refilling a tree, so past ``deadline`` (a
    :func:`time.monotonic` value) this gives up with SandboxError.
    """
    try:
        info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(info.st_mode):
        os.unlink(name, dir_fd=dir_fd)
        return
    pending = [name]
    while pending:
        current = pending.pop()
        for _ in range(3):
            set_mode(current, dir_fd, add=0o700)
            try:
                fd = open_dir(current, dir_fd)
            except FileNotFoundError:
                break
            try:
                for child in listing(fd):
                    _check(stop)
                    if deadline is not None and time.monotonic() > deadline:
                        raise SandboxError(f"gave up removing {name}: it keeps being refilled")
                    child_info = os.stat(child, dir_fd=fd, follow_symlinks=False)
                    if stat.S_ISDIR(child_info.st_mode):
                        moved = f".hpc-sandbox-removing-{uuid.uuid4().hex}"
                        set_mode(child, fd, add=0o700)  # moving a directory updates its ".."
                        os.rename(child, moved, src_dir_fd=fd, dst_dir_fd=dir_fd)
                        pending.append(moved)
                    else:
                        os.unlink(child, dir_fd=fd)
            except FileNotFoundError:
                pass  # removed concurrently
            finally:
                os.close(fd)
            try:
                os.rmdir(current, dir_fd=dir_fd)
                break
            except FileNotFoundError:
                break
            except OSError as error:
                if error.errno != errno.ENOTEMPTY:
                    raise
        else:
            raise SandboxError(f"could not remove {current}: it keeps being refilled")
