"""Default locations for scratch state and the image cache."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def default_scratch_root() -> Path:
    """Node-local scratch for sessions, snapshots and staged images.

    ``HPC_SANDBOX_SCRATCH`` wins. Inside a PBS job it is the job's local
    directory (``$PBS_LOCALDIR``, else ``/local/$PBS_JOBID`` as on ABCI);
    otherwise ``$TMPDIR`` (Slurm sites usually point it at local disk).
    Never the Singularity session directory, which ABCI caps at 64 MiB.
    """
    if configured := os.environ.get("HPC_SANDBOX_SCRATCH"):
        return Path(configured)
    if local := os.environ.get("PBS_LOCALDIR"):
        return Path(local) / "hpc-sandbox"
    if (job := os.environ.get("PBS_JOBID")) and Path("/local", job).is_dir():
        return Path("/local", job, "hpc-sandbox")
    return Path(os.environ.get("TMPDIR") or tempfile.gettempdir()) / f"hpc-sandbox-{os.getuid()}"


def default_image_cache() -> Path:
    """Shared, content-keyed SIF cache (``HPC_SANDBOX_IMAGE_CACHE`` or the XDG cache)."""
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
