"""Default locations for scratch state and the image cache."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def default_scratch_root() -> Path:
    """Node-local scratch for sessions, snapshots and staged images.

    ``HPC_SANDBOX_SCRATCH`` wins. Inside a PBS job, on sites that give jobs a
    local directory (``$PBS_LOCALDIR``, else ``/local/$PBS_JOBID`` where that
    exists), it is ``hpc-sandbox`` in that directory; otherwise
    ``hpc-sandbox-<uid>`` in ``$TMPDIR`` (Slurm sites often point it at local
    disk), else in the system's temporary directory. Never the runtime's
    session directory, which sites often cap at a few tens of MiB.
    """
    if configured := os.environ.get("HPC_SANDBOX_SCRATCH"):
        return Path(configured)
    if local := os.environ.get("PBS_LOCALDIR"):
        return Path(local) / "hpc-sandbox"
    if (job := os.environ.get("PBS_JOBID")) and Path("/local", job).is_dir():
        return Path("/local", job, "hpc-sandbox")
    return Path(os.environ.get("TMPDIR") or tempfile.gettempdir()) / f"hpc-sandbox-{os.getuid()}"


def default_image_cache() -> Path:
    """The shared image cache, of SIF or squashfs files keyed by their content.

    ``HPC_SANDBOX_IMAGE_CACHE`` wins; otherwise ``hpc-sandbox/images`` in
    ``$XDG_CACHE_HOME``, else in ``~/.cache``.
    """
    if configured := os.environ.get("HPC_SANDBOX_IMAGE_CACHE"):
        return Path(configured)
    return (
        Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "hpc-sandbox" / "images"
    )


def in_batch_job() -> bool:
    return bool(os.environ.get("PBS_JOBID") or os.environ.get("SLURM_JOB_ID"))


def _device(path: Path) -> int:
    for candidate in (path, *path.parents):
        try:
            return candidate.stat().st_dev
        except OSError:
            continue
    return -1


def should_stage(cache_dir: Path, scratch_root: Path) -> bool:
    """Stage images inside batch jobs whose image cache is on another filesystem."""
    return in_batch_job() and _device(cache_dir) != _device(scratch_root)
