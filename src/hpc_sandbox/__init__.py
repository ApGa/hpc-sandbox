"""Rootless sandboxes for HPC clusters, on Singularity/Apptainer or enroot.

The names in ``__all__`` are the public API. ``hpc_sandbox.__version__`` is the running
hpc-sandbox's version: a source checkout's ``pyproject.toml``, else the installed distribution's.
"""

from pathlib import Path
from typing import TYPE_CHECKING

from .enroot import EnrootProvider, EnrootRuntime, EnrootSandbox, EnrootService, SquashfsImages
from .images import ImageFormat, ImageStore, ResolvedImage, SifImages
from .pods import PodService, SessionPod
from .runtime import ContainerRuntime
from .sessions import JanitorReport
from .singularity import SingularityProvider, SingularitySandbox, SingularityService
from .types import (
    CWD_ERROR_EXIT_CODE,
    CWD_ERROR_PREFIX,
    CWD_ERROR_REASONS,
    DownloadReport,
    DroppedLink,
    ExecResult,
    HealthCheck,
    Mount,
    Pod,
    PodNetwork,
    PodProvider,
    PodSpec,
    Sandbox,
    SandboxError,
    SandboxLostError,
    SandboxProvider,
    SandboxSnapshot,
    SandboxSpec,
    SandboxStartError,
    ServiceHealth,
    ServiceSpec,
)

__all__ = [
    "CWD_ERROR_EXIT_CODE",
    "CWD_ERROR_PREFIX",
    "CWD_ERROR_REASONS",
    "ContainerRuntime",
    "DownloadReport",
    "DroppedLink",
    "EnrootProvider",
    "EnrootRuntime",
    "EnrootSandbox",
    "EnrootService",
    "ExecResult",
    "HealthCheck",
    "ImageFormat",
    "ImageStore",
    "JanitorReport",
    "Mount",
    "Pod",
    "PodNetwork",
    "PodProvider",
    "PodService",
    "PodSpec",
    "ResolvedImage",
    "Sandbox",
    "SandboxError",
    "SandboxLostError",
    "SandboxProvider",
    "SandboxSnapshot",
    "SandboxSpec",
    "SandboxStartError",
    "ServiceHealth",
    "ServiceSpec",
    "SessionPod",
    "SifImages",
    "SingularityProvider",
    "SingularitySandbox",
    "SingularityService",
    "SquashfsImages",
]

# Where a source checkout keeps its pyproject.toml: this file is src/hpc_sandbox/__init__.py.
# Not parents[2], which raises IndexError when the package sits right below / (/hpc_sandbox).
_CHECKOUT_PYPROJECT = Path(__file__).resolve().parent.parent.parent / "pyproject.toml"


def _running_version(pyproject: Path = _CHECKOUT_PYPROJECT) -> str:
    """The version of the running hpc-sandbox: that of ``pyproject`` when it is hpc-sandbox's (a
    source checkout, which may shadow an installed copy whose metadata is stale), else the installed
    distribution's, else ``"0+unknown"``."""
    import importlib.metadata
    import tomllib

    try:
        project = tomllib.loads(pyproject.read_text(encoding="utf-8")).get("project")
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError):
        project = None
    if (
        isinstance(project, dict)
        and project.get("name") == "hpc-sandbox"
        and project.get("version")
    ):
        return str(project["version"])
    try:
        return importlib.metadata.version("hpc-sandbox")
    except importlib.metadata.PackageNotFoundError:
        return "0+unknown"


def __getattr__(name: str) -> str:
    if name == "__version__":  # read on first use, then kept as a plain attribute
        version = globals()["__version__"] = _running_version()
        return version
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


if TYPE_CHECKING:
    __version__: str
