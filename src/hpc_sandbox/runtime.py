"""The Singularity/Apptainer command line, run with a deterministic environment."""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .process import DEFAULT_OUTPUT_LIMIT, ProcessResult, run_process
from .types import SandboxError

RUNTIME_CANDIDATES = ("singularity", "apptainer")
QUERY_TIMEOUT_S = 30.0


class ContainerRuntime:
    """One Singularity CE / Apptainer binary; both share the CLI used here.

    ``binary`` is a name on ``PATH`` or a path. When it is unset, ``singularity``
    is preferred over ``apptainer``. ``tmp_dir`` and ``cache_dir`` become the
    runtime's TMPDIR/CACHEDIR unless the caller's environment already sets them,
    so pulls and builds never fill a small ``/tmp`` or a quota-limited home.
    """

    def __init__(
        self,
        binary: str | None = None,
        *,
        tmp_dir: Path | None = None,
        cache_dir: Path | None = None,
    ) -> None:
        self.binary = binary
        self.tmp_dir = tmp_dir
        self.cache_dir = cache_dir
        self._path: str | None = None

    @property
    def path(self) -> str:
        if self._path is None:
            candidates = (self.binary,) if self.binary else RUNTIME_CANDIDATES
            for candidate in candidates:
                if resolved := shutil.which(candidate):
                    self._path = resolved
                    break
            else:
                wanted = repr(self.binary) if self.binary else "singularity or apptainer"
                raise SandboxError(f"container runtime {wanted} is not an executable on PATH")
        return self._path

    def environment(self) -> dict[str, str]:
        """The caller's environment minus everything that would add implicit binds.

        Environment-driven binds would defeat ``--no-mount bind-paths`` (never
        inherit a launcher's bind of datasets or home), and ``*ENV_`` variables
        would leak host settings past ``--cleanenv``. CONFIGDIR is dropped too:
        unprivileged instance metadata is published below the passwd home
        regardless (Singularity CE 4.x), and a CONFIGDIR makes
        ``instance list`` look in an empty directory.
        """
        env = dict(os.environ)
        for flavor in ("APPTAINER", "SINGULARITY"):
            for suffix in ("BIND", "BINDPATH", "CONFIGDIR"):
                env.pop(f"{flavor}_{suffix}", None)
        for key in [key for key in env if key.startswith(("APPTAINERENV_", "SINGULARITYENV_"))]:
            env.pop(key)
        for suffix, default in (("TMPDIR", self.tmp_dir), ("CACHEDIR", self.cache_dir)):
            value = env.get(f"SINGULARITY_{suffix}") or env.get(f"APPTAINER_{suffix}")
            if value is None and default is not None:
                value = str(default)
            if value is not None:
                env.setdefault(f"SINGULARITY_{suffix}", value)
                env.setdefault(f"APPTAINER_{suffix}", value)
        return env

    async def run(
        self,
        *args: str,
        timeout_s: float,
        stdin: bytes | None = None,
        output_limit: int = DEFAULT_OUTPUT_LIMIT,
        on_start: Callable[[int], None] | None = None,
    ) -> ProcessResult:
        try:
            return await run_process(
                [self.path, *args],
                timeout_s=timeout_s,
                stdin=stdin,
                env=self.environment(),
                output_limit=output_limit,
                on_start=on_start,
            )
        except OSError as error:
            raise SandboxError(f"could not run {self.path}: {error}") from error

    async def version(self) -> str:
        result = await self.run("--version", timeout_s=QUERY_TIMEOUT_S)
        if result.returncode != 0:
            raise SandboxError(f"{self.path} --version failed: {result.detail()}")
        return result.stdout.decode(errors="replace").strip()

    async def instance_record(self, name: str) -> dict[str, Any] | None:
        """The runtime's record of exactly ``name``, or None if it is not running."""
        result = await self.run("instance", "list", "--json", name, timeout_s=QUERY_TIMEOUT_S)
        if result.returncode != 0 or result.timed_out:
            raise SandboxError(f"could not query instance {name!r}: {result.detail()}")
        try:
            payload = json.loads(result.stdout or b"{}")
        except json.JSONDecodeError as error:
            raise SandboxError(f"invalid instance list JSON for {name!r}") from error
        instances = payload.get("instances") if isinstance(payload, dict) else None
        if not isinstance(instances, list):
            raise SandboxError(f"instance list for {name!r} has no instances array")
        matches = [
            item
            for item in instances
            if isinstance(item, dict) and item.get("instance", item.get("name")) == name
        ]
        if len(matches) > 1:
            raise SandboxError(f"several runtime records match instance {name!r}")
        return matches[0] if matches else None


def instance_pid(record: dict[str, Any]) -> int:
    """The instance process PID from an ``instance list --json`` record."""
    for key in ("pid", "PID", "Pid"):
        if key in record:
            try:
                pid = int(record[key])
            except (TypeError, ValueError):
                break
            if pid > 0:
                return pid
            break
    raise SandboxError(f"instance record has no valid PID: {record}")
