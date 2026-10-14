# hpc-sandbox

Rootless sandboxes for HPC clusters, built on Singularity CE / Apptainer
instances. One sandbox is one persistent instance with its own writable root
filesystem on node-local disk; you run commands in it, copy files in and out,
snapshot it and restore copies of it.

It is extracted from the sandbox backend that ran the SWE-rebench and
Frontier-CS environments in production on ABCI (setup p50 2.2 s, p99 23 s over
792 sessions at 384-way concurrency), and implements the sandbox contract of
platoon (`SandboxSpec`, `Sandbox`, `SandboxProvider`; PLAN-v1 section 4.2)
without depending on platoon. Python 3.11+, standard library only.

## Quick start

```bash
pip install -e .              # or: uv pip install -e .
python -m hpc_sandbox doctor  # is this node usable?
python -m hpc_sandbox run docker://alpine:3.20 -- sh -c 'id; uname -a'
```

```python
from hpc_sandbox import Mount, SandboxSpec, SingularityProvider

provider = SingularityProvider()  # defaults suit a PBS or Slurm job
spec = SandboxSpec(
    image="docker://python:3.12-slim",          # or a local .sif path
    workdir="/app",
    env={"PYTHONUNBUFFERED": "1"},
    mounts=[Mount("/groups/data", "/data")],    # read-only by default
    memory_mb=4096,                             # PSS watchdog
    labels={"run": "exp1", "rollout_id": "r42"},
)
sandbox = await provider.start(spec)
try:
    result = await sandbox.exec("python -c 'print(6 * 7)'", timeout_s=60)
    print(result.exit_code, result.stdout, result.stderr, result.timed_out)
    await sandbox.upload("solution.py", "/app/solution.py")
    await sandbox.download("/app/out", "./out")  # files and directories
    snapshot = await sandbox.snapshot()           # files, not processes
    clone = await provider.restore(snapshot, spec)
    await clone.stop()
    await provider.delete_snapshot(snapshot)
finally:
    await sandbox.stop()                          # idempotent

await provider.reap({"rollout_id": "r42"})        # stop leftovers by label
```

`exec` takes a shell string (run with `/bin/sh -c`) or an argv list, plus
per-call `timeout_s`, `env`, `cwd` and `stdin` bytes. Every call has a timeout
(`default_timeout_s`, 600 s, when none is given); on expiry the command's
process tree is killed and the result has `timed_out=True` and exit code 124.
Infrastructure failures raise `SandboxError` (`SandboxStartError` when a
sandbox cannot start, `SandboxLostError` when its instance died) instead of
masquerading as command failures.

## Command line

| Command | What it does |
|---|---|
| `python -m hpc_sandbox doctor [--image IMAGE]` | Reports the runtime and version, the site settings that matter (`allow setuid`, `allow user ns`, `sessiondir max size`, loop devices), the scratch and cache filesystems, free space, staging and reflink support; with `--image`, starts a sandbox and exercises exec and transfers. Exits non-zero on failure. |
| `python -m hpc_sandbox run IMAGE [--env K=V] [--bind SRC:DST[:ro\|rw]] [--label K=V] [--network host\|none] [--memory-mb N] [--timeout S] -- CMD...` | Runs one command in a fresh sandbox, prints its output and exits with its status. |
| `python -m hpc_sandbox janitor [--label K=V]...` | Stops and removes sandboxes abandoned by dead processes on this node (optionally only those with the labels) and prints a JSON report. Use it in job epilogues. |

All commands take `--binary`, `--scratch-root` and `--image-cache`.

## How a sandbox is built

1. **Image.** `docker://repo:tag`, `docker://repo@sha256:...`, `oras://`,
   `library://` or a bare `repo:tag` is pulled once into the image cache under
   a file lock, validated with `sif list` and published read-only, keyed by the
   manifest digest when the reference has one (pin production images by
   digest). A local SIF path may carry an `@sha256:<hex>` suffix to pin the
   file's content. Inside a batch job whose image cache is on another
   filesystem (Lustre), the SIF is staged to node-local scratch as
   `sha256-<digest>.sif`, verified while copying.
2. **Session.** A private directory `sessions/hpcsb-<uuid>` (mode 0700) on
   node-local scratch, leased with `flock` and holding an owner record: owner
   PID and start time, instance PID and start time, hostname and labels.
3. **Root filesystem.** `build --sandbox --fix-perms` extracts the image into
   the session as a directory owned by the calling uid. No loop devices, no
   `--writable-tmpfs`. Bind targets are created inside it (refusing symlinks
   that would lead out of it) and a keep-alive `%startscript` is written into
   it: OCI-derived images ship an empty one, and an instance whose startscript
   exits is removed at once.
4. **Instance.** `instance start --cleanenv --containall --no-home --no-eval
   --no-mount home,cwd,bind-paths --writable --pid-file ...` with
   session-private `/tmp` and `/var/tmp` binds, `/etc/resolv.conf` read-only
   (host networking), the spec's mounts and an exact name `hpcsb-<uuid>`.
   Setuid mode without `--fakeroot`: the sandbox runs as your uid. Startup
   checks that the runtime's PID, the PID file and the process start time
   agree, then probes that the rootfs and `workdir` are writable.
5. **Commands.** `exec --cleanenv --cwd ... instance://hpcsb-<uuid>`. The
   host environment never reaches the sandbox; `SandboxSpec.env`, per-call
   `env` and `HOME=/root` are exported by a tiny `sh` prologue with POSIX
   quoting, so values pass verbatim (`--env` splits on commas, `--env-file` is
   shell-evaluated). Output is bounded (head and tail of each stream, 16 MiB);
   a command that leaves a background process holding stdout still returns.
6. **Transfers.** Through a fresh directory in the session's `/tmp`, so they
   work for binary files of any size and see the container's view of paths
   (including its mounts).
7. **Memory.** With `memory_mb`, a watchdog samples the proportional set size
   of the instance's processes and of every running command once a second. On
   a breach it kills the largest process, like the kernel OOM killer, and the
   command that owned it returns 137 with an explanation in stderr; a harness
   running in the same sandbox survives a runaway tool call. This is a sampled
   guard, not a cgroup: usage can overshoot between samples.
8. **Snapshots** copy the rootfs and the private tmp directories with
   `cp -a --reflink=auto` (near free on reflink filesystems), after a `sync`.
   Processes and memory are not captured; take snapshots while the sandbox is
   quiescent. Snapshots live on the node that took them.
9. **Teardown and janitors.** `stop()` kills running commands, stops the exact
   instance (`instance stop --timeout`), then signals the instance process
   directly if it survived, re-proving its identity first, and deletes the
   session. Teardown is shielded from cancellation. A janitor considers only
   `hpcsb-<uuid>` sessions under its root whose lease is free and whose owner
   process is dead, optionally filtered by labels; it fails closed (reports and
   leaves the session) whenever identities disagree. Nothing uses wildcards or
   `instance stop --all`. Each provider sweeps abandoned sessions before its
   first start.

## Configuration

`SingularityProvider(...)` keyword options:

| Option | Default | Meaning |
|---|---|---|
| `binary` | `singularity`, else `apptainer` | Runtime executable (name or path). |
| `scratch_root` | see below | Node-local directory for sessions, snapshots, staged images and runtime temp/cache. |
| `image_cache` | `$HPC_SANDBOX_IMAGE_CACHE` or `~/.cache/hpc-sandbox/images` | SIF cache; may be shared storage with `flock`. |
| `stage_images` | auto | Stage SIFs to scratch; auto = inside a batch job when the cache is on another filesystem. |
| `default_timeout_s` | 600 | Timeout of `exec` and transfers without an explicit one. |
| `start_timeout_s` | 1800 | Pull, extraction, instance start and snapshot copy timeout. |
| `stop_grace_s` / `stop_timeout_s` | 10 / 60 | `instance stop --timeout`, and the bound on the whole stop. |
| `allow_network_none` | False | Permit `network="none"` (`--net --network none`); only where users may create network namespaces. Adds the `network_none` capability. |
| `gpu` | None | `"nv"` or `"rocm"` (`--nv` / `--rocm`); adds the `gpu` capability. |
| `cgroup_limits` | False | Pass `cpus` and `memory_mb` as `--cpus` / `--memory` where rootless cgroups work. Otherwise `cpus` is advisory. |
| `bind_resolv_conf` | True | Bind the host's `/etc/resolv.conf` with host networking. |
| `extra_start_args` | () | Extra `instance start` flags. |
| `output_limit_bytes` | 16 MiB | Captured bytes per stream and command. |
| `memory_poll_s` | 1.0 | Watchdog sampling interval. |

Default scratch root: `$HPC_SANDBOX_SCRATCH`, else `$PBS_LOCALDIR/hpc-sandbox`,
else `/local/$PBS_JOBID/hpc-sandbox` when it exists, else
`$TMPDIR/hpc-sandbox-<uid>`. Unless already set, the runtime's
`SINGULARITY_TMPDIR`/`APPTAINER_TMPDIR` and `*_CACHEDIR` point into it, so
pulls and builds never fill a small `/tmp` or a quota-limited home. Bind
variables (`SINGULARITY_BIND`, ...), `SINGULARITYENV_*`/`APPTAINERENV_*` and
`*_CONFIGDIR` are removed from the runtime's environment.

Capabilities: `{"snapshot", "isolation"}`, plus `network_none` and `gpu` when
enabled. Isolation means private PID/IPC namespaces, root filesystem, `/tmp`
and environment; it is not a security boundary against code running as your
uid with host networking.

## ABCI notes

What the production backend learned on ABCI (SingularityCE 4.4.1, setuid):

- **No fakeroot.** `--fakeroot` with overlays forces squashfuse, whose
  `allow_other` mount the site rejects; Singularity then silently falls back to
  a read-only rootfs. OCI-mode fakeroot has no instances. Startup probes
  writability and fails loudly if this regresses.
- **No `--writable-tmpfs`.** `sessiondir max size = 64` (MiB) caps it; package
  installs and builds exceed that at once. Harbor's stock Singularity provider
  uses `--writable-tmpfs --fakeroot` and does not work here.
- **Writable directories, not overlays.** A per-session directory on
  `/local/$PBS_JOBID` (about 14 TB NVMe) needs no loop devices; `/tmp` is only
  8 GB.
- **Instance metadata lives in the passwd home**, ignoring
  `SINGULARITY_CONFIGDIR` and `HOME`, so listings cannot be scoped per job.
  Hence exact UUID names, `--pid-file`, PID start times and leases, and never
  `instance stop --all`, which would stop other jobs' instances.
- **No cgroups for users**, hence the PSS watchdog. The setuid starter is not
  dumpable; its RSS is charged instead of PSS.
- **Host networking.** `allow net networks` is unset, so `--network none` is
  reserved for root in setuid mode; leave `allow_network_none` off unless
  `doctor`/the integration test shows it works.
- **Paths.** Keep the image cache on `/groups` (Lustre, supports `flock`) and
  let staging copy SIFs to `/local`. `pbsdsh` tasks start with a minimal
  environment: pass `PBS_JOBID`/`PBS_LOCALDIR` through, and run
  `python -m hpc_sandbox janitor` in the job epilogue.

## Slurm and AMD notes

- **Apptainer.** Pass `binary="apptainer"` (or let detection find it). The
  same CLI is used; in unprivileged user-namespace mode the writable
  directory, binds, `--containall` and `--net --network none` normally work
  without setuid, so `allow_network_none=True` is often fine there (verify with
  `HPC_SANDBOX_TEST_NETWORK_NONE=1`).
- **Scratch.** Slurm sites usually point `$TMPDIR` at job-local disk; otherwise
  set `HPC_SANDBOX_SCRATCH` (for example to `/local/$SLURM_JOB_ID`). Staging
  turns on automatically inside `SLURM_JOB_ID` jobs when the cache is on another
  filesystem.
- **GPUs.** `gpu="nv"` for NVIDIA (`--nv`), `gpu="rocm"` for AMD (`--rocm`).
- **Hard limits.** Where Apptainer has rootless cgroups v2 (systemd user
  delegation), `cgroup_limits=True` turns `cpus`/`memory_mb` into real limits.
- **enroot (NVIDIA Slurm with pyxis).** Not implemented yet;
  `hpc_sandbox.enroot.EnrootProvider` documents the design to port.

## Tests

```bash
python -m pytest                       # unit tests, no container runtime needed
HPC_SANDBOX_INTEGRATION=1 python -m pytest tests/test_integration.py -v   # compute node
```

Unit tests put a fake `singularity` (`tests/fake_singularity.py`) first on
`PATH`. It records every invocation and simulates instances as real host
processes with the real process title, so identity checks, the janitor, the
watchdog, timeouts and cancellation are exercised with real processes. The
integration tests need a compute node (never run instances on a login node);
`HPC_SANDBOX_TEST_IMAGE` selects the image (default `docker://alpine:3.20`).

## Not yet

- ext3 overlay mode (the production backend's alternative to directory mode);
- the Harbor `BaseEnvironment` and OpenReward `SandboxBackend` faces from the
  platoon design;
- memory snapshots, and per-node PID namespaces inside one shared sandbox.

## License

MIT, see [LICENSE](LICENSE).
