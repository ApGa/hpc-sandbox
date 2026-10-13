"""``python -m hpc_sandbox``: check a node, run one command in a sandbox, sweep sessions."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

from .paths import in_batch_job
from .runtime import ContainerRuntime
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
_SHARED_FILESYSTEMS = {"lustre", "nfs", "nfs4", "gpfs", "beegfs", "cifs", "fuse.sshfs"}


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    # Everything after the first "--" is the command for ``run``, never options.
    split = arguments.index("--") if "--" in arguments else len(arguments)
    args = _parser().parse_args(arguments[:split])
    args.command = arguments[split + 1 :]
    try:
        return asyncio.run(args.handler(args))
    except (SandboxError, ValueError) as error:
        print(f"hpc-sandbox: {error}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--binary", help="singularity or apptainer executable (default: detect)")
    common.add_argument("--scratch-root", help="node-local scratch (default: job-local dir)")
    common.add_argument("--image-cache", help="SIF cache directory")

    parser = argparse.ArgumentParser(
        prog="python -m hpc_sandbox",
        description="Rootless HPC sandboxes on Singularity/Apptainer instances.",
    )
    commands = parser.add_subparsers(required=True, metavar="COMMAND")

    doctor = commands.add_parser("doctor", parents=[common], help="check this node")
    doctor.add_argument("--image", help="also start a sandbox from IMAGE and exercise it")
    doctor.set_defaults(handler=_doctor)

    run = commands.add_parser(
        "run", parents=[common], help="run a command in a fresh sandbox: run IMAGE -- CMD [ARG...]"
    )
    run.add_argument("image")
    run.add_argument("--workdir", default="/app")
    run.add_argument("--env", action="append", default=[], metavar="KEY=VALUE")
    run.add_argument("--bind", action="append", default=[], metavar="SRC:DST[:ro|rw]")
    run.add_argument("--label", action="append", default=[], metavar="KEY=VALUE")
    run.add_argument("--network", choices=("host", "none"), default="host")
    run.add_argument("--allow-network-none", action="store_true")
    run.add_argument("--memory-mb", type=int)
    run.add_argument("--timeout", type=float, default=600.0, help="seconds (default 600)")
    run.set_defaults(handler=_run)

    janitor = commands.add_parser(
        "janitor", parents=[common], help="stop and remove abandoned sessions on this node"
    )
    janitor.add_argument("--label", action="append", default=[], metavar="KEY=VALUE")
    janitor.set_defaults(handler=_janitor)
    return parser


def _provider(args: argparse.Namespace, **options: object) -> SingularityProvider:
    return SingularityProvider(
        binary=args.binary,
        scratch_root=args.scratch_root,
        image_cache=args.image_cache,
        **options,  # type: ignore[arg-type]
    )


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
    sandbox = await provider.start(spec)
    try:
        result = await sandbox.exec(command, timeout_s=args.timeout)
    finally:
        await sandbox.stop()
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.exit_code


async def _janitor(args: argparse.Namespace) -> int:
    labels = _pairs(args.label, "--label") or None
    report = await _provider(args).reap(labels)
    print(json.dumps(asdict(report), indent=2))
    return 1 if report.errors else 0


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
    for key, value in (await site_config(runtime)).items():
        risky = (key == "allow setuid" and value == "no") or (
            key == "sessiondir max size" and value.isdigit() and int(value) < 1024
        )
        note = " (never use --writable-tmpfs here)" if key == "sessiondir max size" else ""
        report.line("warn" if risky else "info", f"site: {key}", value + (note if risky else ""))
    job = os.environ.get("PBS_JOBID") or os.environ.get("SLURM_JOB_ID")
    report.line("info", "batch job", job or "none (login node or laptop)")
    for name, path in (
        ("scratch root", provider.scratch_root),
        ("image cache", provider.images.cache_dir),
    ):
        _check_directory(report, name, path, local=name == "scratch root")
    staging = provider.images.stage_dir
    report.line("info", "image staging", f"to {staging}" if staging else "off")
    report.line(
        "info",
        "reflink on scratch",
        "yes (snapshots are cheap)"
        if _reflink_works(provider.scratch_root)
        else "no (snapshots copy data)",
    )
    if args.image:
        await _smoke(report, provider, args.image)
    return 1 if report.failures else 0


def _check_directory(report: _Report, name: str, path: Path, *, local: bool) -> None:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path):
            pass
    except OSError as error:
        report.line("FAIL", name, f"{path} is not writable: {error}")
        return
    usage = os.statvfs(path)
    free_gib = usage.f_bavail * usage.f_frsize / 2**30
    filesystem = filesystem_type(path)
    detail = f"{path} ({filesystem}, {free_gib:.0f} GiB free)"
    shared = filesystem in _SHARED_FILESYSTEMS or filesystem == "tmpfs"
    report.line("warn" if local and shared and in_batch_job() else "ok", name, detail)


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


async def _smoke(report: _Report, provider: SingularityProvider, image: str) -> None:
    started = time.monotonic()
    try:
        sandbox = await provider.start(SandboxSpec(image=image, labels={"hpc-sandbox": "doctor"}))
    except SandboxError as error:
        report.line("FAIL", "sandbox start", str(error))
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
        report.line("ok", "sandbox stop", "session removed")


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
