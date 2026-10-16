"""Rootless HPC sandboxes on Singularity/Apptainer instances and enroot containers."""

from .enroot import EnrootProvider, EnrootRuntime, EnrootSandbox, SquashfsImages
from .images import ImageFormat, ImageStore, ResolvedImage, SifImages
from .runtime import ContainerRuntime
from .sessions import JanitorReport
from .singularity import SingularityProvider, SingularitySandbox
from .types import (
    CWD_ERROR_EXIT_CODE,
    CWD_ERROR_PREFIX,
    CWD_ERROR_REASONS,
    DownloadReport,
    DroppedLink,
    ExecResult,
    Mount,
    Sandbox,
    SandboxError,
    SandboxLostError,
    SandboxProvider,
    SandboxSnapshot,
    SandboxSpec,
    SandboxStartError,
)

__version__ = "0.1.0"

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
    "ExecResult",
    "ImageFormat",
    "ImageStore",
    "JanitorReport",
    "Mount",
    "ResolvedImage",
    "Sandbox",
    "SandboxError",
    "SandboxLostError",
    "SandboxProvider",
    "SandboxSnapshot",
    "SandboxSpec",
    "SandboxStartError",
    "SifImages",
    "SingularityProvider",
    "SingularitySandbox",
    "SquashfsImages",
]
