"""Image resolution: local SIF files, and OCI references pulled once into a cache.

:meth:`ImageStore.resolve` accepts

- a local SIF path (``/x/task.sif``, ``./task.sif``, ``file:///x/task.sif``),
  optionally pinned to the file's SHA-256 with an ``@sha256:<hex>`` suffix;
- an OCI or ORAS reference (``docker://repo:tag``, ``docker://repo@sha256:<hex>``,
  ``oras://...``, ``library://...``, or a bare ``repo:tag``), pulled once into the
  image cache under a file lock. Digest-pinned references are cached under
  their manifest digest, tag references under a hash of the URI; pin
  references by digest for reproducible runs.

A pulled SIF is validated with ``sif list`` before it is atomically published
read-only, next to a sidecar recording its own SHA-256. With a stage directory
(node-local scratch), the SIF is then copied there, verified against that
digest and used from there, so many sandbox starts read local NVMe instead of
shared storage.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from .locks import locked
from .runtime import ContainerRuntime
from .types import SandboxError

_DIGEST = re.compile(r"@sha256:([0-9a-f]{64})$")
_REMOTE_SCHEMES = ("docker://", "oras://", "library://", "shub://", "http://", "https://")
_LOCAL_PREFIXES = ("/", "./", "../", "~", "file://")


@dataclass(frozen=True)
class ResolvedImage:
    reference: str
    path: Path
    sha256: str | None  # SHA-256 of the SIF file, when known


def is_local(reference: str) -> bool:
    return reference.startswith(_LOCAL_PREFIXES) or _DIGEST.sub("", reference).endswith(".sif")


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


def _stat_key(path: Path) -> tuple[int, int, int, int] | None:
    try:
        info = path.stat()
    except OSError:
        return None
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


class ImageStore:
    def __init__(
        self,
        runtime: ContainerRuntime,
        cache_dir: Path,
        *,
        stage_dir: Path | None = None,
        pull_attempts: int = 3,
        pull_retry_delay_s: float = 2.0,
        pull_timeout_s: float = 1800.0,
        validate_timeout_s: float = 120.0,
    ) -> None:
        self.runtime = runtime
        self.cache_dir = cache_dir
        self.stage_dir = stage_dir
        self.pull_attempts = max(1, pull_attempts)
        self.pull_retry_delay_s = pull_retry_delay_s
        self.pull_timeout_s = pull_timeout_s
        self.validate_timeout_s = validate_timeout_s
        # Files validated (or hashed) by this process, keyed by their stat identity.
        self._valid: set[tuple[int, int, int, int]] = set()
        self._digests: dict[tuple[int, int, int, int], str] = {}

    async def resolve(self, reference: str) -> ResolvedImage:
        if is_local(reference):
            path, sha256 = await self._local(reference)
        else:
            path, sha256 = await self._pulled(reference)
        if self.stage_dir is not None and path.parent != self.stage_dir:
            sha256 = sha256 or await self.digest(path)
            path = await self._stage(path, sha256)
        return ResolvedImage(reference, path, sha256)

    async def digest(self, path: Path) -> str:
        key = _stat_key(path)
        if key is None:
            raise SandboxError(f"image {path} disappeared")
        if key not in self._digests:
            self._digests[key] = await asyncio.to_thread(sha256_file, path)
        return self._digests[key]

    async def _is_valid(self, path: Path) -> bool:
        key = _stat_key(path)
        if key is None or key[2] == 0:
            return False
        if key in self._valid:
            return True
        result = await self.runtime.run("sif", "list", str(path), timeout_s=self.validate_timeout_s)
        if result.returncode != 0 or result.timed_out:
            return False
        self._valid.add(key)
        return True

    async def _local(self, reference: str) -> tuple[Path, str | None]:
        pinned = match.group(1) if (match := _DIGEST.search(reference)) else None
        path = Path(_DIGEST.sub("", reference).removeprefix("file://")).expanduser().resolve()
        if not path.is_file():
            raise SandboxError(f"image {reference!r} is not a file")
        if not await self._is_valid(path):
            raise SandboxError(f"image {path} is not a valid SIF")
        if pinned is not None and (actual := await self.digest(path)) != pinned:
            raise SandboxError(f"image {path} has SHA-256 {actual}, expected {pinned}")
        return path, pinned

    async def _pulled(self, reference: str) -> tuple[Path, str | None]:
        uri = pull_uri(reference)
        match = _DIGEST.search(uri)
        key = (
            f"oci-{match.group(1)}"
            if match
            else f"uri-{hashlib.sha256(uri.encode()).hexdigest()[:40]}"
        )
        target = self.cache_dir / f"{key}.sif"
        if _stat_key(target) not in self._valid:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            async with locked(self.cache_dir / f"{key}.lock", private=False):
                if not await self._is_valid(target):
                    await self._pull(uri, target)
        return target, self._recorded_digest(target)

    async def _pull(self, uri: str, target: Path) -> None:
        target.unlink(missing_ok=True)
        partial = target.with_name(f".{target.stem}.{os.getpid()}.{uuid.uuid4().hex}.sif")
        errors: list[str] = []
        try:
            for attempt in range(1, self.pull_attempts + 1):
                partial.unlink(missing_ok=True)
                result = await self.runtime.run(
                    "pull", "--force", str(partial), uri, timeout_s=self.pull_timeout_s
                )
                if result.returncode == 0 and await self._is_valid(partial):
                    # chmod and rename keep the stat identity, so the file stays
                    # known-valid (and its digest memoized) under its final name.
                    digest = await self.digest(partial)
                    partial.chmod(0o444)
                    os.replace(partial, target)
                    self._write_sidecar(
                        target,
                        {
                            "source": uri,
                            "sha256": digest,
                            "pulled_at": time.time(),
                            "runtime": self.runtime.path,
                        },
                    )
                    return
                status = "timed out" if result.timed_out else f"exited {result.returncode}"
                errors.append(f"attempt {attempt}: {status}: {result.detail() or 'invalid SIF'}")
                if attempt < self.pull_attempts:
                    await asyncio.sleep(self.pull_retry_delay_s)
        finally:
            partial.unlink(missing_ok=True)
        raise SandboxError(f"could not pull {uri}:\n" + "\n".join(errors))

    @staticmethod
    def _write_sidecar(image: Path, record: dict[str, object]) -> None:
        sidecar = image.with_suffix(".json")
        temporary = sidecar.with_name(f".{sidecar.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, sidecar)

    @staticmethod
    def _recorded_digest(image: Path) -> str | None:
        try:
            digest = json.loads(image.with_suffix(".json").read_text()).get("sha256")
        except (OSError, json.JSONDecodeError, AttributeError):
            return None
        return digest if isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) else None

    async def _stage(self, source: Path, sha256: str) -> Path:
        """Copy ``source`` to node-local scratch as ``sha256-<digest>.sif``, verified.

        A staged file is verified once, when it is written; later processes
        trust a read-only, same-size copy under the digest name.
        """
        assert self.stage_dir is not None
        staged = self.stage_dir / f"sha256-{sha256}.sif"
        if _stat_key(staged) in self._valid:
            return staged
        self.stage_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        async with locked(self.stage_dir / f"sha256-{sha256}.lock"):
            info = staged.stat() if staged.is_file() else None
            if info is None or info.st_size != source.stat().st_size or info.st_mode & 0o222:
                staged.unlink(missing_ok=True)
                partial = staged.with_name(f".{staged.stem}.{os.getpid()}.{uuid.uuid4().hex}.sif")
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
