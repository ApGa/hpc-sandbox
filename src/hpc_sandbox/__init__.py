"""Rootless HPC sandboxes on Singularity/Apptainer instances."""

from .images import ImageFormat, ImageStore, ResolvedImage, SifImages
from .runtime import ContainerRuntime
from .sessions import JanitorReport
from .singularity import SingularityProvider, SingularitySandbox
from .types import (
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
    "ContainerRuntime",
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
]
