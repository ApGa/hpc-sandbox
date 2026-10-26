"""The ``hpc-sandbox`` command, which ``python -m hpc_sandbox`` runs too.

- ``doctor``: check whether this node can run sandboxes; with ``--image``, start one and use it.
- ``run``: run one command in a fresh sandbox and exit with its status.
- ``pull``: pull images into the image cache, or check local image files, and print JSON lines.
- ``fetch-tools``: download the pinned slirp4netns that pods use for egress.
- ``janitor``: stop and remove what dead processes left on this node, and print a JSON report.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

from . import subids
from .enroot import ANCHOR_SCRIPT, STANDARD_HOOKS, EnrootProvider
from .images import DIGEST
from .paths import in_batch_job
from .process import run_process
from .runtime import RUNTIME_CANDIDATES, ContainerRuntime
from .sandbox import SessionProvider
from .sessions import JanitorReport
from .singularity import SingularityProvider
from .types import Mount, SandboxError, SandboxSpec

# Site settings that decide whether this provider works (Singularity CE/Apptainer config).
_SITE_KEYS = (
    "allow setuid",
    "allow user ns",
    "sessiondir max size",
    "max loop devices",
    "enable overlay",
    "enable underlay",
    "mount home",
)
# enroot.conf settings worth showing; the enroot provider overrides them per sandbox.
_ENROOT_KEYS = (
    "ENROOT_MOUNT_HOME",
    "ENROOT_REMAP_ROOT",
    "ENROOT_RESTRICT_DEV",
    "ENROOT_ROOTFS_WRITABLE",
    "ENROOT_DATA_PATH",
    "ENROOT_CACHE_PATH",
    "ENROOT_TEMP_PATH",
)
_SHARED_FILESYSTEMS = {"lustre", "nfs", "nfs4", "gpfs", "beegfs", "cifs", "fuse.sshfs"}
RUNTIMES = ("auto", "singularity", "enroot")
# How long a command waits for the deletions of the sessions it stopped before it exits.
_DELETION_WAIT_S = 60.0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    # Everything after the first "--" is the command for ``run``, never options.
    split = arguments.index("--") if "--" in arguments else len(arguments)
    args = _parser().parse_args(arguments[:split])
    args.command = arguments[split + 1 :]
    try:
        # argparse checks a --runtime given on the command line, never the default it takes from
        # $HPC_SANDBOX_RUNTIME: a typo there would quietly select Singularity, and a janitor
        # would skip enroot's sessions. fetch-tools has no --runtime.
        if getattr(args, "runtime", "auto") not in RUNTIMES:
            raise ValueError(
                f"HPC_SANDBOX_RUNTIME must be one of {', '.join(RUNTIMES)}, not {args.runtime!r}"
            )
        return asyncio.run(args.handler(args))
    except (SandboxError, ValueError) as error:
        print(f"hpc-sandbox: {error}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    from . import __version__

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--runtime",
        choices=RUNTIMES,
        # Empty counts as unset, as with the other HPC_SANDBOX_* variables.
        default=os.environ.get("HPC_SANDBOX_RUNTIME") or "auto",
        help="container runtime (default: $HPC_SANDBOX_RUNTIME, else auto: the one --binary "
        "names, else the first of singularity, apptainer and enroot on PATH)",
    )
    common.add_argument(
        "--binary", help="the runtime's executable, a name on PATH or a path (default: detect)"
    )
    common.add_argument(
        "--scratch-root",
        help="node-local scratch for sessions, snapshots and staged images (default: "
        "$HPC_SANDBOX_SCRATCH, else hpc-sandbox in the PBS job's local directory, else "
        "$TMPDIR/hpc-sandbox-UID)",
    )
    common.add_argument(
        "--image-cache",
        help="where pulled images are cached, as SIF or squashfs files (default: "
        "$HPC_SANDBOX_IMAGE_CACHE, else ${XDG_CACHE_HOME:-~/.cache}/hpc-sandbox/images)",
    )

    parser = argparse.ArgumentParser(
        prog="hpc-sandbox",
        description="Rootless sandboxes for HPC clusters, on Singularity/Apptainer or enroot.",
        epilog="Run 'hpc-sandbox COMMAND --help' for the options of a command.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="print hpc-sandbox's version and exit",
    )
    commands = parser.add_subparsers(required=True, metavar="COMMAND")

    doctor = commands.add_parser(
        "doctor",
        parents=[common],
        help="check whether this node can run sandboxes",
        description="Check whether this node can run sandboxes, one line per check; exit with "
        "status 1 if a check failed.",
    )
    doctor.add_argument("--image", help="also start a sandbox from IMAGE and exercise it")
    doctor.set_defaults(handler=_doctor)

    run = commands.add_parser(
        "run",
        parents=[common],
        help="run one command in a fresh sandbox: run IMAGE -- CMD [ARG...]",
        usage="%(prog)s [OPTIONS] IMAGE -- CMD [ARG...]",
        description="Start a fresh sandbox from IMAGE, run CMD in it, stop the sandbox and exit "
        "with the command's status. Everything after the first -- is the command, run without a "
        "shell. When the sandbox cannot start or the command cannot run, the reason follows "
        "'hpc-sandbox: ' on stderr and the exit status is 2.",
    )
    run.add_argument(
        "image",
        metavar="IMAGE",
        help="a remote reference such as docker://alpine:3.20, or a local .sif (Singularity) or "
        ".sqsh (enroot) file; @sha256:<hex> pins it",
    )
    run.add_argument(
        "--workdir", default="/app", help="the command's working directory (default: /app)"
    )
    run.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="an environment variable of the command; repeat it for more",
    )
    run.add_argument(
        "--bind",
        action="append",
        default=[],
        metavar="SRC:DST[:ro|rw]",
        help="bind the host path SRC at DST, read-only unless :rw; repeat it for more",
    )
    run.add_argument(
        "--label",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="a label of the sandbox, which janitor --label selects by; repeat it for more",
    )
    run.add_argument(
        "--network",
        choices=("host", "none"),
        default="host",
        help="the host's network, or none: a loopback interface only (default: host)",
    )
    run.add_argument(
        "--allow-network-none",
        action="store_true",
        help="let Singularity use --network none (enroot always can)",
    )
    run.add_argument(
        "--memory-mb", type=int, help="memory limit in MiB, which the memory watchdog enforces"
    )
    run.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="seconds after which the command is killed, with exit status 124 (default: 600)",
    )
    run.set_defaults(handler=_run)

    pull = commands.add_parser(
        "pull",
        parents=[common],
        help="pull images into the cache: pull IMAGE [IMAGE...]",
        description="Pull each IMAGE into the image cache unless it is cached, or check a local "
        "image file, and print a JSON line for each.",
    )
    pull.add_argument(
        "references",
        nargs="+",
        metavar="IMAGE",
        help="a remote reference, or a local image file to check",
    )
    pull.add_argument(
        "--refresh",
        action="store_true",
        help="pull tags again even if cached (sandboxes started afterwards get the new image)",
    )
    pull.set_defaults(handler=_pull)

    fetch = commands.add_parser(
        "fetch-tools",
        help="download the pinned slirp4netns pods use for egress when the host has none",
        description="Download the pinned slirp4netns that pods use for egress when the host has "
        "none, check its SHA-256, and print a JSON line.",
    )
    fetch.add_argument("--arch", help="x86_64 or aarch64 (default: this machine's)")
    fetch.set_defaults(handler=_fetch_tools)

    janitor = commands.add_parser(
        "janitor",
        parents=[common],
        help="stop and remove abandoned sessions on this node",
        description="Stop and remove the sandboxes and pods that dead processes left on this "
        "node, and print a JSON report; exit with status 1 if it lists errors. Unless --runtime, "
        "$HPC_SANDBOX_RUNTIME or --binary selects one runtime, it sweeps enroot's sessions, and "
        "Singularity's when singularity or apptainer is on PATH.",
    )
    janitor.add_argument(
        "--label",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="only sessions with this label; repeat it for more, which must all match",
    )
    janitor.set_defaults(handler=_janitor)
    return parser


def resolve_runtime(choice: str, binary: str | None = None) -> str:
    """``singularity`` or ``enroot``: the explicit choice, else what ``binary`` or PATH has."""
    if choice != "auto":
        return choice
    if binary:
        return "enroot" if Path(binary).name.startswith("enroot") else "singularity"
    if any(shutil.which(candidate) for candidate in RUNTIME_CANDIDATES):
        return "singularity"
    return "enroot" if shutil.which("enroot") else "singularity"


def _provider(
    args: argparse.Namespace, *, allow_network_none: bool = False
) -> SessionProvider[Any]:
    common: dict[str, Any] = {
        "binary": args.binary,
        "scratch_root": args.scratch_root,
        "image_cache": args.image_cache,
    }
    if resolve_runtime(args.runtime, args.binary) == "enroot":
        return EnrootProvider(**common)
    return SingularityProvider(**common, allow_network_none=allow_network_none)


def _pairs(values: list[str], what: str) -> dict[str, str]:
    pairs = {}
    for value in values:
        key, separator, rest = value.partition("=")
        if not separator or not key:
            raise ValueError(f"{what} must look like KEY=VALUE: {value!r}")
        pairs[key] = rest
    return pairs


def _mount(value: str) -> Mount:
    source, _, rest = value.partition(":")
    target, _, mode = rest.partition(":")
    if not source or not target or mode not in ("", "ro", "rw"):
        raise ValueError(f"--bind must look like SRC:DST[:ro|rw]: {value!r}")
    return Mount(source=str(Path(source).resolve()), target=target, read_only=mode != "rw")


async def _run(args: argparse.Namespace) -> int:
    command = args.command
    if not command:
        raise ValueError("give the command after --, e.g. run IMAGE -- echo hello")
    provider = _provider(args, allow_network_none=args.allow_network_none)
    spec = SandboxSpec(
        image=args.image,
        workdir=args.workdir,
        env=_pairs(args.env, "--env"),
        mounts=[_mount(value) for value in args.bind],
        network=args.network,
        memory_mb=args.memory_mb,
        labels=_pairs(args.label, "--label"),
    )
    try:
        sandbox = await provider.start(spec)
        try:
            result = await sandbox.exec(command, timeout_s=args.timeout)
        finally:
            await sandbox.stop()
    finally:  # a failed start's session too: the end of a Slurm step kills its deletion
        await provider.wait_for_deletions(_DELETION_WAIT_S)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.exit_code


async def _pull(args: argparse.Namespace) -> int:
    images = _provider(args).images
    for reference in args.references:
        image = await images.pull(reference, refresh=args.refresh)
        pin = None if images.format.is_local(reference) else DIGEST.search(reference)
        record = {
            "reference": reference,
            "path": str(image.path),
            "sha256": image.sha256 or await images.digest(image.path),  # of the image file
            "manifest_digest": f"sha256:{pin.group(1)}" if pin else None,  # when pinned
        }
        print(json.dumps(record), flush=True)
    return 0


async def _fetch_tools(args: argparse.Namespace) -> int:
    from .tools import SLIRP4NETNS_VERSION, fetch_slirp4netns

    path = await asyncio.to_thread(fetch_slirp4netns, args.arch)
    print(json.dumps({"tool": "slirp4netns", "version": SLIRP4NETNS_VERSION, "path": str(path)}))
    return 0


async def _janitor(args: argparse.Namespace) -> int:
    labels = _pairs(args.label, "--label") or None
    report = JanitorReport()
    for runtime in _janitor_runtimes(args):
        args.runtime = runtime
        try:
            swept = await _provider(args).reap(labels)
        except (ValueError, SandboxError) as error:  # one runtime's setup spares no other
            report.errors.append(f"could not sweep the {runtime} sessions: {error}")
            continue
        report.stopped += swept.stopped
        report.removed += swept.removed
        report.live += swept.live
        report.errors += swept.errors
    print(json.dumps(asdict(report), indent=2))
    return 1 if report.errors else 0


def _janitor_runtimes(args: argparse.Namespace) -> list[str]:
    """The runtimes whose sessions a janitor sweeps, unless one is chosen.

    Singularity's when it is installed (stopping its instances needs it), and
    always enroot's: its sandboxes die with their owners, so sweeping them
    needs no enroot binary.
    """
    if args.runtime != "auto" or args.binary:
        return [resolve_runtime(args.runtime, args.binary)]
    singularity = any(shutil.which(binary) for binary in RUNTIME_CANDIDATES)
    return [*(["singularity"] if singularity else []), "enroot"]


class _Report:
    def __init__(self) -> None:
        self.failures = 0

    def line(self, status: str, name: str, detail: str) -> None:
        self.failures += status == "FAIL"
        print(f"[{status:>4}] {name}: {detail}")


async def _doctor(args: argparse.Namespace) -> int:
    report = _Report()
    provider = _provider(args)
    runtime = provider.runtime
    try:
        report.line("ok", "runtime", f"{runtime.path} ({await runtime.version()})")
    except SandboxError as error:
        report.line("FAIL", "runtime", str(error))
        return 1
    # The runtime's checks write in the scratch root: create it and the image cache before them,
    # as a start and a pull do, so that the scratch root is private. Their lines keep their place.
    cache = provider.images.cache_dir
    directories = [
        _check_directory(
            "scratch root", provider.scratch_root, provider._make_scratch_directories, local=True
        ),
        _check_directory(
            "image cache", cache, lambda: cache.mkdir(parents=True, exist_ok=True), local=False
        ),
    ]
    if isinstance(provider, EnrootProvider):
        await _enroot_checks(report, provider)
    else:
        assert isinstance(runtime, ContainerRuntime)
        for key, value in (await site_config(runtime)).items():
            # allow setuid = no is user-namespace mode: sandboxes run there as in setuid mode.
            risky = key == "sessiondir max size" and value.isdigit() and int(value) < 1024
            note = " (never use --writable-tmpfs here)" if risky else ""
            report.line("warn" if risky else "info", f"site: {key}", value + note)
    job = os.environ.get("PBS_JOBID") or os.environ.get("SLURM_JOB_ID")
    report.line("info", "batch job", job or "none (login node or laptop)")
    for line in directories:
        report.line(*line)
    staging = provider.images.stage_dir
    report.line("info", "image staging", f"to {staging}" if staging else "off")
    report.line(
        "info",
        "reflink on scratch",
        "yes (snapshots are cheap)"
        if _reflink_works(provider.scratch_root)
        else "no (snapshots copy data)",
    )
    try:
        report.line("ok", "pods: egress", f"slirp4netns {provider.slirp4netns()}")
    except SandboxError as error:
        report.line("warn", "pods: egress", str(error))
    if "subids" in provider.service_type.run_modes:  # enroot's containers map one id
        report.line(*_subids_check())
    if args.image:
        await _smoke(report, provider, args.image)
    return 1 if report.failures else 0


async def _enroot_checks(report: _Report, provider: EnrootProvider) -> None:
    try:
        report.line("ok", "nsenter", provider.nsenter)
    except SandboxError as error:
        report.line("FAIL", "nsenter", str(error))
    argv = [sys.executable, "-I", "-S", str(ANCHOR_SCRIPT), str(os.getpid()), "none", "--", "true"]
    try:
        probe = await run_process(argv, timeout_s=60)
    except OSError as error:  # such as a fork refused at the process limit
        report.line("FAIL", "namespaces", f"could not run the probe: {error}")
    else:
        if probe.returncode == 0:
            report.line("ok", "namespaces", "user, mount, PID, IPC and network namespaces")
        else:
            report.line("FAIL", "namespaces", probe.detail() or f"exit {probe.returncode}")
    if not outside_user_namespaces():
        report.line(
            "warn",
            "user namespace",
            "this process runs inside a container's user namespace: enroot import cannot "
            "use its file capabilities here; run on the host or pass .sqsh images",
        )
    report.line(*_whiteout_check(provider.runtime.temp_dir))
    for key, value in enroot_site_config(provider.runtime.enroot_conf()).items():
        report.line("info", f"site: {key}", f"{value} (overridden per sandbox)")
    runtime = provider.runtime
    site_hooks = runtime.site_sysconf() / "hooks.d"
    hooks = sorted(path.name for path in site_hooks.glob("*.sh"))
    report.line("info", "site hooks", f"{site_hooks}: {', '.join(hooks) or 'none'}")
    enabled = [hook for hook in STANDARD_HOOKS if os.access(site_hooks / hook, os.X_OK)]
    missing = [hook for hook in STANDARD_HOOKS if hook not in enabled]
    detail = ", ".join(enabled) or "none"
    if missing:
        report.line("warn", "sandbox hooks", f"{detail}; not enabled, so skipped: {missing}")
    else:
        report.line("ok", "sandbox hooks", detail)


def _subids_check() -> tuple[str, str, str]:
    """Whether pods with ``users="subids"`` can start here: your subordinate ids, and the tools that
    map them. Their lack is no failure: the other pods need neither."""
    name = "pods: subordinate ids"
    uids, gids = subids.ranges()
    try:
        ids = subids.find()
    except SandboxError as error:  # only the site gives ranges; the tools may just be missing
        return "info" if uids is None or gids is None else "warn", name, str(error)
    spans = (f"{extent.start}-{extent.start + extent.count - 1}" for extent in (ids.uids, ids.gids))
    return "ok", name, "uids {}, gids {}; {}, {}".format(*spans, ids.newuidmap, ids.newgidmap)


def _whiteout_check(temp_dir: Path) -> tuple[str, str, str]:
    """Whether ``enroot-aufs2ovlfs`` can convert layer whiteouts (needed by imports)."""
    name = "image imports"
    converter = shutil.which("enroot-aufs2ovlfs")
    if converter is None:
        return "warn", name, "enroot-aufs2ovlfs is not on PATH; only local .sqsh images work"
    try:  # in the runtime's temporary directory, which doctor created with the scratch root
        with tempfile.TemporaryDirectory(dir=temp_dir) as scratch:
            layer = Path(scratch)
            (layer / "opaque").mkdir()
            (layer / "opaque" / ".wh..wh..opq").touch()
            (layer / ".wh.deleted").touch()
            result = subprocess.run(
                [converter, str(layer)], capture_output=True, timeout=60, check=False
            )
            converted = (
                result.returncode == 0
                and not (layer / "opaque" / ".wh..wh..opq").exists()
                and not (layer / ".wh.deleted").exists()
                and (layer / "deleted").exists()
            )
    except (OSError, subprocess.TimeoutExpired) as error:
        # Such as a scratch root that cannot be written, which its own line reports as FAIL.
        return "warn", name, f"could not test whether {converter} converts whiteouts: {error}"
    if converted:
        return "ok", name, f"{converter} converts whiteouts"
    detail = result.stderr.decode(errors="replace").strip() or f"exit {result.returncode}"
    advice = "install enroot's +caps package, run outside nested user namespaces, or pass .sqsh"
    return "warn", name, f"{converter} cannot convert whiteouts ({detail}); {advice} images"


def outside_user_namespaces() -> bool:
    """Whether this process runs in the initial user namespace (not inside a container)."""
    try:
        mapping = Path("/proc/self/uid_map").read_text().split()
    except OSError:
        return True
    return mapping[:3] == ["0", "0", "4294967295"]


def enroot_site_config(conf: Path) -> dict[str, str]:
    """The provider-relevant settings of the site's enroot.conf and its drop-ins."""
    values = {}
    for path in [conf, *sorted(conf.parent.glob(f"{conf.name}.d/*.conf"))]:
        try:
            lines = path.read_text().splitlines()
        except OSError:
            continue
        for line in lines:
            key, _, value = line.strip().partition(" ")
            if key in _ENROOT_KEYS:
                values[key] = value.strip()
    return values


def _check_directory(
    name: str, path: Path, create: Callable[[], None], *, local: bool
) -> tuple[str, str, str]:
    """Whether ``path``, which ``create`` creates if need be, can be written, and where it is."""
    try:
        create()
        with tempfile.NamedTemporaryFile(dir=path):
            pass
    except SandboxError as error:  # a scratch root that another user owns
        return "FAIL", name, str(error)
    except OSError as error:
        return "FAIL", name, f"{path} is not writable: {error}"
    usage = os.statvfs(path)
    free_gib = usage.f_bavail * usage.f_frsize / 2**30
    filesystem = filesystem_type(path)
    detail = f"{path} ({filesystem}, {free_gib:.0f} GiB free)"
    shared = filesystem in _SHARED_FILESYSTEMS or filesystem == "tmpfs"
    return "warn" if local and shared and in_batch_job() else "ok", name, detail


def _reflink_works(directory: Path) -> bool:
    try:
        with tempfile.TemporaryDirectory(dir=directory) as scratch:
            source = Path(scratch, "source")
            source.write_bytes(b"x")
            result = subprocess.run(
                ["cp", "--reflink=always", str(source), str(Path(scratch, "copy"))],
                capture_output=True,
                timeout=30,
                check=False,
            )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


async def _smoke(report: _Report, provider: SessionProvider[Any], image: str) -> None:
    started = time.monotonic()
    try:
        sandbox = await provider.start(SandboxSpec(image=image, labels={"hpc-sandbox": "doctor"}))
    except (SandboxError, OSError) as error:  # OSError: a scratch root it cannot create
        report.line("FAIL", "sandbox start", str(error))
        await provider.wait_for_deletions(_DELETION_WAIT_S)  # what its cleanup left to delete
        return
    report.line("ok", "sandbox start", f"{sandbox.id} in {time.monotonic() - started:.1f} s")
    try:
        result = await sandbox.exec("echo hpc-sandbox-ok", timeout_s=60)
        good = result.exit_code == 0 and "hpc-sandbox-ok" in result.stdout
        report.line("ok" if good else "FAIL", "exec", f"exit {result.exit_code}")
        with tempfile.TemporaryDirectory() as scratch:
            source, back = Path(scratch, "in.txt"), Path(scratch, "out.txt")
            source.write_text("round trip\n")
            await sandbox.upload(source, "/tmp/doctor/in.txt")
            await sandbox.download("/tmp/doctor/in.txt", back)
            same = back.read_text() == source.read_text()
            report.line("ok" if same else "FAIL", "upload/download", "round trip")
    except SandboxError as error:
        report.line("FAIL", "sandbox use", str(error))
    finally:
        await sandbox.stop()
        deleted = await provider.wait_for_deletions(_DELETION_WAIT_S)
        report.line(
            "ok" if deleted else "FAIL",
            "sandbox stop",
            "session deleted"
            if deleted
            else f"session not deleted within {_DELETION_WAIT_S:.0f} s",
        )


async def site_config(runtime: ContainerRuntime) -> dict[str, str]:
    """The provider-relevant keys of the site's singularity.conf / apptainer.conf."""
    result = await runtime.run("buildcfg", timeout_s=30)
    directories = [
        line.split("=", 1)[1]
        for line in result.stdout.decode(errors="replace").splitlines()
        if line.split("=", 1)[0].endswith("_CONFDIR") and "=" in line
    ]
    for directory in directories:
        for name in ("singularity.conf", "apptainer.conf"):
            try:
                text = Path(directory, name).read_text()
            except OSError:
                continue
            values = {}
            for line in text.splitlines():
                key, separator, value = line.partition("=")
                if separator and not key.lstrip().startswith("#") and key.strip() in _SITE_KEYS:
                    values[key.strip()] = value.strip()
            return values
    return {}


def filesystem_type(path: Path) -> str:
    """The type of the filesystem holding ``path`` (from /proc/self/mounts)."""
    best, kind = "", "unknown"
    resolved = str(path.resolve())
    try:
        mounts = Path("/proc/self/mounts").read_text().splitlines()
    except OSError:
        return kind
    for line in mounts:
        fields = line.split()
        if len(fields) < 3:
            continue
        point = fields[1].replace("\\040", " ")
        inside = resolved == point or resolved.startswith(point.rstrip("/") + "/")
        if inside and len(point) >= len(best):
            best, kind = point, fields[2]
    return kind
