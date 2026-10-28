"""The node's templates: images extracted once per node, on the scratch root.

The Singularity provider's copy layout copies a template into each sandbox (``rootfs_templates``),
and its fuse-overlay layout and the gVisor provider use one as the read-only lower layer of each
sandbox's overlay. Each provider keeps its templates in a directory of its own on the scratch root
(``templates/`` for Singularity, ``templates-gvisor/`` for gVisor), one per image file, each a
private directory with the image's root filesystem (``rootfs/``) and a marker that says it is
complete. A template that a sandbox still uses is never deleted: a fuse-overlayfs process holds its
directories open, and a gVisor sandbox holds its users lock shared.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import re
import stat
import uuid
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path

from . import procfs
from .images import ResolvedImage
from .locks import locked, open_lock_file
from .runtime import ContainerRuntime
from .sandbox import make_owner_writable
from .sessions import remove_tree
from .types import SandboxError, SandboxStartError

log = logging.getLogger(__name__)

TEMPLATE_DIR = "templates"
TEMPLATE_MARKER = ".complete"
# In a template, beside its root filesystem: a lock that the sandboxes using the template as their
# lower layer for as long as their owner lives (gVisor's) hold shared.
TEMPLATE_USERS = ".users.lock"
# An extraction of a template, or a damaged template moved aside, by process PID started at START.
_EXTRACTION = re.compile(
    r"\.(?P<key>.+)\.(?P<pid>[0-9]+)-(?P<start>[0-9]+)\.[0-9a-f]{8}(?P<damaged>\.damaged)?"
)
#: What returns the directories that processes use as layers (fuse-overlayfs's): a template that
#: holds one of them is kept.
InUse = Callable[[], Sequence[Path]]


def template_ready(path: Path) -> bool:
    """Whether ``path`` is a complete template: a private directory of this uid with its marker."""
    try:
        info = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISDIR(info.st_mode)
        and info.st_uid == os.getuid()
        and not stat.S_IMODE(info.st_mode) & 0o077
        and (path / TEMPLATE_MARKER).is_file()
        and (path / "rootfs").is_dir()
    )


async def extract_image(
    runtime: ContainerRuntime, image: ResolvedImage, rootfs: Path, *, timeout_s: float
) -> None:
    """Extract the SIF ``image`` into the new directory ``rootfs`` with ``build --sandbox
    --fix-perms``, and give your uid read and write access to everything in it."""
    result = await runtime.run(
        "build",
        "--sandbox",
        "--fix-perms",
        str(rootfs),
        str(image.path),
        timeout_s=timeout_s,
    )
    if result.returncode != 0 or result.timed_out:
        raise SandboxStartError(f"could not extract {image.path}: {result.detail()}")
    await asyncio.to_thread(make_owner_writable, rootfs)


async def rootfs_template(
    image: ResolvedImage,
    root: Path,
    extract: Callable[[ResolvedImage, Path], Awaitable[None]],
    *,
    in_use: InUse = tuple,
) -> Path:
    """This node's extracted root filesystem of ``image``, in the templates directory ``root``.

    Extracted once per image file with ``extract(image, rootfs)``, under a lock per image, and
    published by a rename once complete. Like a staged image, a template is trusted later when it
    is a private directory of this uid holding its marker; one that lost it (to an age-based
    cleaner, or a removal cut short) is moved aside and extracted again, and deleted unless
    ``in_use`` or a sandbox's users lock says it is still used. No sandbox writes to a template: a
    copy (reflinked where the filesystem can) is the sandbox's own, and an overlay writes to its
    upper layer.
    """
    key = f"sha256-{image.sha256}" if image.sha256 else _stat_name(image.path)
    final = root / key
    if template_ready(final):
        return final / "rootfs"
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    damaged = None
    async with locked(root / f"{key}.lock"):
        if template_ready(final):
            return final / "rootfs"
        if os.path.lexists(final):
            damaged = root / f"{_extraction_name(key)}.damaged"
            os.rename(final, damaged)
        work = root / _extraction_name(key)
        work.mkdir(mode=0o700)
        try:
            await extract(image, work / "rootfs")
            marker = {
                "image": str(image.path),
                "reference": image.reference,
                "sha256": image.sha256,
            }
            (work / TEMPLATE_MARKER).write_text(json.dumps(marker, sort_keys=True) + "\n")
            os.rename(work, final)
        except BaseException:
            await asyncio.shield(asyncio.to_thread(remove_tree, work))
            raise
    if damaged is not None:  # unless a sandbox still uses it: then a janitor deletes it later
        for error in await asyncio.to_thread(_remove_extractions, [damaged], in_use):
            log.warning("hpc-sandbox: %s", error)
    return final / "rootfs"


async def hold_template(rootfs: Path) -> int:
    """A descriptor that holds the users lock of the template whose root filesystem is ``rootfs``
    shared, for a sandbox that uses it as its lower layer: janitors never delete a template whose
    lock is held, even once it is moved aside. Close the descriptor to let go."""
    descriptor = open_lock_file(rootfs.parent / TEMPLATE_USERS)
    try:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
                return descriptor
            except BlockingIOError:  # a janitor's probe holds it, for an instant
                await asyncio.sleep(0.02)
    except BaseException:
        os.close(descriptor)
        raise


def remove_abandoned_extractions(root: Path, in_use: InUse = tuple) -> list[str]:
    """Delete the extractions in the templates directory ``root`` whose process died, and the
    damaged templates that nothing uses; returns errors."""
    try:
        names = [entry.name for entry in os.scandir(root)]
    except OSError:
        return []
    abandoned = []
    for name in names:  # damaged templates once unused, whoever moved them aside
        match = _EXTRACTION.fullmatch(name)
        if match and (
            match["damaged"] or not procfs.identity_matches(int(match["pid"]), match["start"])
        ):
            abandoned.append(root / name)
    return _remove_extractions(abandoned, in_use) if abandoned else []


def _remove_extractions(paths: list[Path], in_use: InUse = tuple) -> list[str]:
    """Delete these extractions, but none that holds a directory of ``in_use`` or whose users lock
    a sandbox holds; returns errors."""
    layers = in_use()
    errors = []
    for path in paths:
        real = Path(os.path.realpath(path))  # as /proc names open directories
        if any(layer == real or real in layer.parents for layer in layers) or _template_held(path):
            continue
        try:
            remove_tree(path)
        except (OSError, SandboxError) as error:
            errors.append(f"could not delete {path}: {error}")
    return errors


def _template_held(path: Path) -> bool:
    """Whether a sandbox holds the users lock of the template, or moved-aside template, ``path``."""
    try:
        descriptor = os.open(path / TEMPLATE_USERS, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError:
        return False
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    finally:
        os.close(descriptor)
    return False


def _extraction_name(key: str) -> str:
    """A name for an extraction of template ``key`` by this process, or a damaged template it moved
    aside."""
    return f".{key}.{os.getpid()}-{procfs.start_time(os.getpid())}.{uuid.uuid4().hex[:8]}"


def _stat_name(path: Path) -> str:
    """A template name for an image file whose digest is unknown: its stat identity."""
    info = path.stat()  # a rewrite in place can keep the size and mtime; the ctime changes
    return f"stat-{info.st_dev}-{info.st_ino}-{info.st_size}-{info.st_mtime_ns}-{info.st_ctime_ns}"
