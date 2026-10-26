"""Subordinate ids: the uids and gids beyond your own that a pod with ``users="subids"`` maps.

A process can map only its own uid and gid into a user namespace it creates. The ranges that
``/etc/subuid`` and ``/etc/subgid`` give a user (``user:start:count``) can be mapped as well by
``newuidmap`` and ``newgidmap``, the setuid helpers of shadow-utils that rootless Docker, Podman
and Singularity's ``--fakeroot`` use. A pod's namespace then maps root to your uid and gid, and
uids and gids 1 to ``count`` to the first range given to your user, by name or by uid.
"""

from __future__ import annotations

import os
import pwd
import shutil
from dataclasses import dataclass
from pathlib import Path

from .process import run_process
from .types import SandboxError, SandboxStartError

SUBUID = Path("/etc/subuid")
SUBGID = Path("/etc/subgid")
TOOLS = ("newuidmap", "newgidmap")


@dataclass(frozen=True)
class IdRange:
    """``count`` ids from ``start`` on, as the host numbers them."""

    start: int
    count: int

    @property
    def ids(self) -> range:
        return range(self.start, self.start + self.count)


@dataclass(frozen=True)
class SubordinateIds:
    """Your first ranges of subordinate uids and gids, and the tools that map them."""

    uids: IdRange
    gids: IdRange
    newuidmap: str
    newgidmap: str


def ranges() -> tuple[IdRange | None, IdRange | None]:
    """The first range that ``/etc/subuid`` and ``/etc/subgid`` each give your user, or None."""
    names = _names()
    return first_range(SUBUID, names), first_range(SUBGID, names)


def find() -> SubordinateIds:
    """Your subordinate ids and the tools that map them; :class:`SandboxError` says what is
    missing."""
    uids, gids = ranges()
    tools = {tool: shutil.which(tool) for tool in TOOLS}
    listings = [str(path) for path, found in ((SUBUID, uids), (SUBGID, gids)) if found is None]
    absent = [tool for tool, path in tools.items() if path is None]
    missing = [
        *([f"no range of subordinate ids for your user in {_join(listings)}"] if listings else []),
        *([f"no {_join(absent)} (shadow-utils) on PATH"] if absent else []),
    ]
    if missing:
        raise SandboxError(f"pods with users='subids' cannot start here: {'; '.join(missing)}")
    assert uids is not None and gids is not None
    return SubordinateIds(uids, gids, str(tools["newuidmap"]), str(tools["newgidmap"]))


def first_range(path: Path, names: set[str]) -> IdRange | None:
    """The first ``owner:start:count`` line of ``path`` whose owner is one of ``names``."""
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return None
    for line in lines:
        owner, _, rest = line.strip().partition(":")
        start, _, count = rest.partition(":")
        if owner in names and start.isdigit() and count.isdigit() and int(count) > 0:
            return IdRange(int(start), int(count))
    return None


async def map_ids(pid: int, ids: SubordinateIds, timeout_s: float) -> None:
    """Map the user namespace of ``pid``, which has none yet: root to your uid and gid, and 1 to
    ``count`` to your subordinate ranges, each tool allowed ``timeout_s``. Raises
    :class:`SandboxStartError` with the tools' output when they refuse."""
    for tool, own, extent in (
        (ids.newuidmap, os.getuid(), ids.uids),
        (ids.newgidmap, os.getgid(), ids.gids),
    ):
        argv = [tool, str(pid), "0", str(own), "1", "1", str(extent.start), str(extent.count)]
        result = await run_process(argv, timeout_s=timeout_s)
        if result.timed_out:
            raise SandboxStartError(f"{Path(tool).name} did not finish within {timeout_s:g} s")
        if result.returncode != 0:
            raise SandboxStartError(
                f"{Path(tool).name} could not map the user namespace of process {pid}: "
                f"{result.detail() or f'exit {result.returncode}'}"
            )


def _names() -> set[str]:
    uid = os.getuid()
    names = {str(uid)}
    try:
        names.add(pwd.getpwuid(uid).pw_name)
    except KeyError:  # no passwd entry: only lines that name the uid count
        pass
    return names


def _join(items: list[str]) -> str:
    return items[0] if len(items) == 1 else f"{', '.join(items[:-1])} and {items[-1]}"
