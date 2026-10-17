"""Rootless HPC sandboxes on Singularity/Apptainer instances and enroot containers."""

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

__version__ = "0.2.0"

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
