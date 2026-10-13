"""Enroot provider: a documented stub (NVIDIA Slurm sites with pyxis).

Not implemented yet; :meth:`EnrootProvider.start` raises ``NotImplementedError``.
The design to port is the production SWE-rebench ``EnrootSandbox``:

- **Images.** ``enroot import --output <cache>/<sha256(uri)>.sqsh <uri>`` under
  an exclusive ``flock``, published with an atomic rename and retried. Enroot
  wants Docker Hub shorthand (``docker://ns/image``) and ``registry#path`` for
  other registries. Run imports with ``TAR_OPTIONS=--delay-directory-restore``:
  Enroot 3.5 extracts layers with GNU tar, and images that revisit a read-only
  directory otherwise fail with ``Permission denied``.
- **Rootfs.** ``enroot create --name hpcsb-<uuid> <sqsh>`` per sandbox, removed
  with ``enroot remove --force`` at stop.
- **Commands.** ``enroot start --rw --root`` with ``--mount`` binds for a
  session-private ``/tmp`` and an empty ``/etc/rc.local`` (keeps the image
  WORKDIR, skips its entrypoint), and ``ENROOT_MOUNT_HOME=""`` so a site hook
  cannot bind the host home. Enroot has no instances, so commands run inside a
  persistent anchor: a PID-1 reaper started with ``unshare --user --mount
  --pid`` under a ``PR_SET_PDEATHSIG`` launcher, entered per command with
  ``nsenter --target <anchor> --user --preserve-credentials --mount --pid``.
  Before each command the anchor's identity is re-proved (PID start time,
  namespace links, NSpid 1); stopping kills the anchor, which kills every
  process in the sandbox.
- **Limits.** Host networking only (no network namespace for users); memory
  through the same PSS watchdog as :mod:`hpc_sandbox.memory`.
"""

from __future__ import annotations

from .types import SandboxSnapshot, SandboxSpec


class EnrootProvider:
    name = "enroot"
    capabilities: frozenset[str] = frozenset()

    async def start(self, spec: SandboxSpec) -> None:
        raise NotImplementedError("the enroot provider is not implemented yet; see this module")

    async def restore(self, snapshot: SandboxSnapshot, spec: SandboxSpec) -> None:
        raise NotImplementedError("the enroot provider is not implemented yet; see this module")
