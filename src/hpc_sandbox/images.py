"""Image resolution: local image files, and remote references pulled once into a cache.

An :class:`ImageStore` serves one :class:`ImageFormat` (SIF files for
Singularity and Apptainer, squashfs files for enroot) and accepts

- a local image path (``/x/task.sif``, ``./task.sif``, ``file:///x/task.sif``),
  optionally pinned to the file's SHA-256 with an ``@sha256:<hex>`` suffix;
- a remote reference (``docker://repo:tag``, ``docker://repo@sha256:<hex>``, a
  bare ``repo:tag`` and whatever else the format's runtime can pull), pulled
  once into the image cache under a file lock.

A pulled image is validated, then published read-only under its own SHA-256
(``sha256-<hex>.<ext>``), so cached image files never change. What a
reference resolves to is a small pointer file next to it
(``uri-<hash>.<ext>.json``, or ``oci-<digest>.<ext>.json`` for a
digest-pinned reference), replaced atomically.
A tag keeps the image it named when first pulled until :meth:`ImageStore.pull`
refreshes it. With a stage directory (node-local scratch), the image is then
copied there, verified against its digest and used from there, so many
sandbox starts read local disk instead of shared storage.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .locks import locked, open_lock_file
from .process import ProcessResult
from .runtime import ContainerRuntime
from .types import SandboxError

DIGEST = re.compile(r"@sha256:([0-9a-f]{64})$")
_REMOTE_SCHEMES = ("docker://", "oras://", "library://", "shub://", "http://", "https://")
LOCAL_PREFIXES = ("/", "./", "../", "~", "file://")
SQUASHFS_MAGIC = b"hsqs"
SQUASHFS_SUFFIXES = (".sqsh", ".squashfs")
SIF_MAGIC = b"SIF_MAGIC"


@dataclass(frozen=True)
class ResolvedImage:
    reference: str
    path: Path
    sha256: str | None  # SHA-256 of the image file, when known


class ImageFormat(Protocol):
    """How one kind of image file is recognized, validated and pulled."""

    name: str  # for messages, e.g. "SIF"
    suffix: str  # of cached and staged files, e.g. ".sif"

    @property
    def tool(self) -> str:
        """The program that pulls images, recorded in each image's pointer file."""
        ...

    def is_local(self, reference: str) -> bool:
        """Whether ``reference`` names a local file rather than a remote image."""
        ...

    def check(self, reference: str) -> None:
        """Raise ValueError if ``reference`` can never resolve to an image of this format.

        Cheap and side-effect free: called before a sandbox creates anything.
        """
        ...

    def pull_uri(self, reference: str) -> str:
        """The URI to pull ``reference`` from; raise ValueError if it cannot be pulled."""
        ...

    async def validate(self, path: Path) -> bool: ...

    async def pull(self, uri: str, target: Path, *, timeout_s: float) -> ProcessResult:
        """Write the image at ``uri`` to the new file ``target``."""
        ...


class SifImages:
    """SIF images, validated with ``sif list`` and pulled by Singularity or Apptainer.

    ``provider`` names the provider that runs them, in the error that refuses a squashfs image.
    """

    name = "SIF"
    suffix = ".sif"

    def __init__(
        self,
        runtime: ContainerRuntime,
        *,
        validate_timeout_s: float = 120.0,
        provider: str = "Singularity",
    ) -> None:
        self.runtime = runtime
        self.validate_timeout_s = validate_timeout_s
        self.provider = provider

    @property
    def tool(self) -> str:
        return self.runtime.path

    def is_local(self, reference: str) -> bool:
        return is_local(reference)

    def check(self, reference: str) -> None:
        if self.is_local(reference):
            path = local_path(reference)
            if path.suffix in SQUASHFS_SUFFIXES or file_magic(path, 0, 4) == SQUASHFS_MAGIC:
                raise ValueError(
                    f"{reference!r} is a squashfs image; the {self.provider} provider runs SIF "
                    "images"
                )
        elif DIGEST.sub("", reference).endswith(SQUASHFS_SUFFIXES):
            raise ValueError(
                f"{reference!r} is a squashfs image; the {self.provider} provider runs SIF images"
            )
        elif "://" in reference and not reference.startswith(_REMOTE_SCHEMES):
            scheme = reference.split("://", 1)[0]
            raise ValueError(f"Singularity cannot pull {scheme}:// references: {reference!r}")

    def pull_uri(self, reference: str) -> str:
        return pull_uri(reference)

    async def validate(self, path: Path) -> bool:
        result = await self.runtime.run("sif", "list", str(path), timeout_s=self.validate_timeout_s)
        return result.returncode == 0 and not result.timed_out

    async def pull(self, uri: str, target: Path, *, timeout_s: float) -> ProcessResult:
        return await self.runtime.run("pull", "--force", str(target), uri, timeout_s=timeout_s)


def local_path(reference: str) -> Path:
    """The file a local image reference names, without its ``@sha256:`` pin."""
    return Path(DIGEST.sub("", reference).removeprefix("file://")).expanduser().resolve()


def file_magic(path: Path, offset: int, size: int) -> bytes:
    """``size`` bytes of ``path`` at ``offset``, or b"" if it cannot be read."""
    try:
        with path.open("rb") as stream:
            stream.seek(offset)
            return stream.read(size)
    except OSError:
        return b""


def is_sif(path: Path) -> bool:
    """Whether ``path`` starts like a SIF file (launch script, then ``SIF_MAGIC``)."""
    return file_magic(path, 32, len(SIF_MAGIC)) == SIF_MAGIC


def is_local(reference: str) -> bool:
    """Whether a SIF reference names a local file."""
    return reference.startswith(LOCAL_PREFIXES) or DIGEST.sub("", reference).endswith(".sif")


def pull_uri(reference: str) -> str:
    """A URI that both Singularity CE and Apptainer accept for ``pull``.

    Enroot's ``registry#path`` spelling is accepted for convenience.
    """
    if reference.startswith("docker://"):
        return "docker://" + reference.removeprefix("docker://").replace("#", "/", 1)
    if reference.startswith(_REMOTE_SCHEMES):
        return reference
    return "docker://" + reference.replace("#", "/", 1)


def sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def copy_and_hash(source: Path, target: Path) -> str:
    """Copy ``source`` to a new file ``target`` in one pass, returning its SHA-256."""
    digest = hashlib.sha256()
    with source.open("rb") as reader, target.open("xb") as writer:
        while chunk := reader.read(8 * 1024 * 1024):
            digest.update(chunk)
            writer.write(chunk)
    return digest.hexdigest()


def _try_lock(descriptor: int) -> bool:
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _number(record: dict[str, object] | None, key: str) -> float:
    value = record.get(key) if record else None
    return float(value) if isinstance(value, int | float) else 0.0


def _stat_key(path: Path) -> tuple[int, int, int, int] | None:
    try:
        info = path.stat()
    except OSError:
        return None
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


class ImageStore:
    """Resolve references to image files of one format.

    Pulled images are cached by reference, so a tag that moves keeps the image
    it named when it was first pulled, until ``pull(reference, refresh=True)``
    pulls it again. Better, pin images by digest or by file content.
    """

    def __init__(
        self,
        image_format: ImageFormat,
        cache_dir: Path,
        *,
        stage_dir: Path | None = None,
        pull_attempts: int = 3,
        pull_retry_delay_s: float = 2.0,
        pull_timeout_s: float = 1800.0,
        prepare: Callable[[], None] | None = None,
    ) -> None:
        self.format = image_format
        self.prepare = prepare  # run in a worker thread before each pull (scratch directories)
        self.cache_dir = cache_dir
        self.stage_dir = stage_dir
        self.pull_attempts = max(1, pull_attempts)
        self.pull_retry_delay_s = pull_retry_delay_s
        self.pull_timeout_s = pull_timeout_s
        # Files validated (or hashed) by this process, keyed by their stat identity.
        self._valid: set[tuple[int, int, int, int]] = set()
        self._digests: dict[tuple[int, int, int, int], str] = {}

    async def resolve(self, reference: str) -> ResolvedImage:
        """The image file a sandbox starts from: pulled if needed, staged if configured."""
        image = await self.pull(reference)
        path, sha256 = image.path, image.sha256
        if self.stage_dir is not None and path.parent != self.stage_dir:
            sha256 = sha256 or await self.digest(path, remember=True)
            path = await self._stage(path, sha256)
        return ResolvedImage(reference, path, sha256)

    async def pull(self, reference: str, *, refresh: bool = False) -> ResolvedImage:
        """The cached image file for ``reference``, pulled unless it is cached already.

        ``refresh`` pulls a remote reference again, for a tag that may have
        moved; sandboxes that start afterwards get the new image, and those
        already running keep theirs. A refresh takes a pull another process
        started after it was asked for, so refreshes asked for while one pull
        runs share the next one.
        Digest-pinned and local references never change, so they are only
        checked. Earlier image files stay in the cache until deleted.
        """
        if self.format.is_local(reference):
            path, sha256 = await self._local(reference)
        else:
            path, sha256 = await self._pulled(reference, refresh=refresh)
        return ResolvedImage(reference, path, sha256)

    async def digest(self, path: Path, *, remember: bool = False) -> str:
        """The SHA-256 of ``path``; with ``remember``, kept on node-local scratch for later
        processes."""
        key = _stat_key(path)
        if key is None:
            raise SandboxError(f"image {path} disappeared")
        if key not in self._digests:
            self._digests[key] = await asyncio.to_thread(self._hash, path, key, remember)
        return self._digests[key]

    def _hash(self, path: Path, key: tuple[int, int, int, int], remember: bool) -> str:
        # Every process that stages a local image would otherwise read it in full
        # from shared storage. The record is keyed by device, inode, size and
        # modification time, so a changed file is hashed again, and staging still
        # checks what it copies.
        memo = None
        if remember and self.stage_dir is not None:
            memo = self._make_stage_dir() / ".digests" / "-".join(map(str, key))
            with suppress(OSError):
                if re.fullmatch(r"[0-9a-f]{64}", recorded := memo.read_text().strip()):
                    return recorded
        digest = sha256_file(path)
        if memo is not None:
            with suppress(OSError):
                memo.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                temporary = memo.with_name(f".{memo.name}.{os.getpid()}.{uuid.uuid4().hex}")
                temporary.write_text(digest + "\n")
                os.replace(temporary, memo)
        return digest

    async def _is_valid(self, path: Path) -> bool:
        key = _stat_key(path)
        if key is None or key[2] == 0:
            return False
        if key in self._valid:
            return True
        if not await self.format.validate(path):
            return False
        self._valid.add(key)
        return True

    async def _local(self, reference: str) -> tuple[Path, str | None]:
        pinned = match.group(1) if (match := DIGEST.search(reference)) else None
        path = local_path(reference)
        if not path.is_file():
            raise SandboxError(f"image {reference!r} is not a file")
        if not await self._is_valid(path):
            raise SandboxError(f"image {path} is not a valid {self.format.name}")
        if pinned is not None and (actual := await self.digest(path, remember=True)) != pinned:
            raise SandboxError(f"image {path} has SHA-256 {actual}, expected {pinned}")
        return path, pinned

    async def _pulled(self, reference: str, *, refresh: bool) -> tuple[Path, str]:
        requested = time.time()
        uri = self.format.pull_uri(reference)
        match = DIGEST.search(reference) or DIGEST.search(uri)
        key = (
            f"oci-{match.group(1)}"
            if match
            else f"uri-{hashlib.sha256(uri.encode()).hexdigest()[:40]}"
        )
        refresh = refresh and match is None  # a digest always names the same image
        if not refresh and (cached := await self._cached(key, uri)) is not None:
            return cached
        if self.prepare is not None:
            await asyncio.to_thread(self.prepare)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        descriptor = open_lock_file(self.cache_dir / f"{key}.lock", private=False)
        try:
            while not _try_lock(descriptor):
                # Another process pulls or refreshes this reference. A start never
                # waits for that while an image for it is cached, even one cached
                # by hpc-sandbox 0.1.0.
                if not refresh and (cached := await self._cached_or_legacy(key, uri)):
                    return cached
                await asyncio.sleep(0.05)
            # Another process may have pulled while this one waited; a refresh
            # takes only a pull that started after it was requested.
            started = _number(self._pointer(key), "started_at")
            if (not refresh or started >= requested) and (
                cached := await self._cached(key, uri, migrate=True)
            ) is not None:
                return cached
            return await self._pull(uri, key)
        finally:
            os.close(descriptor)

    async def _cached_or_legacy(self, key: str, uri: str) -> tuple[Path, str] | None:
        """The image the pointer names, else one hpc-sandbox 0.1.0 cached (used as is)."""
        if (cached := await self._cached(key, uri)) is not None:
            return cached
        legacy = self.cache_dir / f"{key}{self.format.suffix}"
        if self._pointer(key) is None and await self._is_valid(legacy):
            return legacy, await self.digest(legacy)
        return None

    async def _cached(
        self, key: str, uri: str, *, migrate: bool = False
    ) -> tuple[Path, str] | None:
        """The valid image file ``key``'s pointer names, if any.

        With ``migrate`` (under the key's lock), an image hpc-sandbox 0.1.0
        cached under the key itself is adopted instead.
        """
        pointer = self._pointer(key)
        sha256 = pointer.get("sha256") if pointer else None
        if isinstance(sha256, str) and re.fullmatch(r"[0-9a-f]{64}", sha256):
            path = self._content_path(sha256)
            if await self._is_valid(path):
                return path, sha256
        # Once a pointer exists, a file of hpc-sandbox 0.1.0 is superseded.
        return await self._migrate(key, uri) if migrate and pointer is None else None

    async def _migrate(self, key: str, uri: str) -> tuple[Path, str] | None:
        legacy = self.cache_dir / f"{key}{self.format.suffix}"
        if not await self._is_valid(legacy):
            return None
        sha256 = await self.digest(legacy)
        path = self._content_path(sha256)
        if not await self._is_valid(path):
            try:
                os.link(legacy, path)
            except FileExistsError:
                pass
            except OSError:  # e.g. another user's file, with fs.protected_hardlinks
                await self._publish_copy(legacy, path, sha256)
            if not await self._is_valid(path):
                return None
        # When that pull started is unknown, so any refresh pulls again.
        record = {"source": uri, "sha256": sha256, "pulled_at": legacy.stat().st_mtime}
        self._set_pointer(key, {**record, "started_at": 0, "runtime": self.format.tool})
        return path, sha256

    async def _publish_copy(self, source: Path, path: Path, sha256: str) -> None:
        partial = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}")
        try:
            if await asyncio.to_thread(copy_and_hash, source, partial) == sha256:
                partial.chmod(0o444)
                os.replace(partial, path)
        finally:
            partial.unlink(missing_ok=True)

    def _content_path(self, sha256: str) -> Path:
        return self.cache_dir / f"sha256-{sha256}{self.format.suffix}"

    async def _pull(self, uri: str, key: str) -> tuple[Path, str]:
        started = time.time()
        suffix = self.format.suffix
        partial = self.cache_dir / f".{key}.{os.getpid()}.{uuid.uuid4().hex}{suffix}"
        errors: list[str] = []
        try:
            for attempt in range(1, self.pull_attempts + 1):
                partial.unlink(missing_ok=True)
                result = await self.format.pull(uri, partial, timeout_s=self.pull_timeout_s)
                if result.returncode == 0 and await self._is_valid(partial):
                    # chmod and rename keep the stat identity, so the file stays
                    # known-valid (and its digest memoized) under its final name.
                    digest = await self.digest(partial)
                    path = self._content_path(digest)
                    if not await self._is_valid(path):  # else cached for another reference
                        partial.chmod(0o444)
                        os.replace(partial, path)
                    self._set_pointer(
                        key,
                        {
                            "source": uri,
                            "sha256": digest,
                            "started_at": started,
                            "pulled_at": time.time(),
                            "runtime": self.format.tool,
                        },
                    )
                    return path, digest
                status = "timed out" if result.timed_out else f"exited {result.returncode}"
                detail = result.detail() or f"invalid {self.format.name}"
                errors.append(f"attempt {attempt}: {status}: {detail}")
                if attempt < self.pull_attempts:
                    await asyncio.sleep(self.pull_retry_delay_s)
        finally:
            partial.unlink(missing_ok=True)
        raise SandboxError(f"could not pull {uri}:\n" + "\n".join(errors))

    def _pointer_path(self, key: str) -> Path:
        # Per format: providers of both formats may share a cache.
        return self.cache_dir / f"{key}{self.format.suffix}.json"

    def _pointer(self, key: str) -> dict[str, object] | None:
        try:
            record = json.loads(self._pointer_path(key).read_text())
        except (OSError, json.JSONDecodeError):
            return None
        return record if isinstance(record, dict) else None

    def _set_pointer(self, key: str, record: dict[str, object]) -> None:
        pointer = self._pointer_path(key)
        temporary = pointer.with_name(f".{pointer.name}.{os.getpid()}.{uuid.uuid4().hex}")
        try:
            temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
            os.replace(temporary, pointer)
        finally:
            temporary.unlink(missing_ok=True)

    def _make_stage_dir(self) -> Path:
        """The stage directory, created private (and the scratch root before it)."""
        assert self.stage_dir is not None
        if self.prepare is not None:
            self.prepare()
        self.stage_dir.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.stage_dir.mkdir(mode=0o700, exist_ok=True)
        return self.stage_dir

    async def _stage(self, source: Path, sha256: str) -> Path:
        """Copy ``source`` to node-local scratch as ``sha256-<digest><suffix>``, verified.

        A staged file is verified once, when it is written; later processes
        trust a read-only, same-size copy under the digest name.
        """
        assert self.stage_dir is not None
        suffix = self.format.suffix
        staged = self.stage_dir / f"sha256-{sha256}{suffix}"
        if _stat_key(staged) in self._valid:
            return staged
        await asyncio.to_thread(self._make_stage_dir)
        async with locked(self.stage_dir / f"sha256-{sha256}.lock"):
            info = staged.stat() if staged.is_file() else None
            if info is None or info.st_size != source.stat().st_size or info.st_mode & 0o222:
                staged.unlink(missing_ok=True)
                partial = staged.with_name(
                    f".{staged.stem}.{os.getpid()}.{uuid.uuid4().hex}{suffix}"
                )
                try:
                    copied = await asyncio.to_thread(copy_and_hash, source, partial)
                    if copied != sha256:
                        raise SandboxError(
                            f"image {source} has SHA-256 {copied}, expected {sha256}"
                        )
                    partial.chmod(0o444)
                    os.replace(partial, staged)
                finally:
                    partial.unlink(missing_ok=True)
            if (key := _stat_key(staged)) is not None:
                self._valid.add(key)
        return staged
