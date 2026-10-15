# hpc-sandbox

Rootless sandboxes for HPC clusters, built on the container runtime the site
already provides: Singularity CE / Apptainer instances or enroot containers.
A sandbox is a persistent container with its own writable root filesystem on
node-local disk; you run commands in it, copy files in and out, snapshot it
and restore copies of it.

It is meant for workloads that run many short-lived tasks inside a batch
job — agent rollouts, code evaluation, benchmark harnesses — on clusters where
Docker is not available and users have no root. It needs no daemon and no
privileges beyond what the site's runtime grants, and it cleans up after
itself even when the process that started the sandboxes is killed.
Python 3.11+, standard library only.

## Quick start

```bash
pip install -e .              # or: uv pip install -e .
python -m hpc_sandbox doctor  # is this node usable?
python -m hpc_sandbox run docker://alpine:3.20 -- sh -c 'id; uname -a'
```

```python
from hpc_sandbox import EnrootProvider, Mount, SandboxSpec, SingularityProvider

provider = EnrootProvider()  # or SingularityProvider(); defaults suit a PBS or Slurm job
spec = SandboxSpec(
    image="docker://python:3.12-slim",          # or a local .sqsh / .sif path
    workdir="/app",
    env={"PYTHONUNBUFFERED": "1"},
    mounts=[Mount("/shared/data", "/data")],    # read-only by default
    memory_mb=4096,                             # PSS watchdog
    labels={"run": "exp1", "rollout_id": "r42"},
)
async with await provider.start(spec) as sandbox:  # stopped when the block exits
    result = await sandbox.exec("python -c 'print(6 * 7)'", timeout_s=60)
    print(result.exit_code, result.stdout, result.stderr, result.timed_out)
    await sandbox.upload("solution.py", "/app/solution.py")
    await sandbox.download("/app/out", "./out")  # files and directories
    snapshot = await sandbox.snapshot()           # files, not processes
    clone = await provider.restore(snapshot, spec)
    await clone.stop()                            # idempotent
    await provider.delete_snapshot(snapshot)

await provider.reap({"rollout_id": "r42"})        # stop leftovers by label
```

`exec` takes a shell string (run with `/bin/sh -c`) or an argv list, plus
per-call `timeout_s`, `env`, `cwd` and `stdin` bytes. Every call has a timeout
(`default_timeout_s`, 600 s, when none is given); on expiry the command's
process tree is killed and the result has `timed_out=True` and exit code 124.
Infrastructure failures raise `SandboxError` (`SandboxStartError` when a
sandbox cannot start, `SandboxLostError` when its container died) instead of
masquerading as command failures.

Both providers satisfy the `SandboxProvider` and `Sandbox` protocols in
`hpc_sandbox.types`, so code written against the protocols does not depend on
the runtime underneath.

| | `SingularityProvider` | `EnrootProvider` |
|---|---|---|
| Runtime | Singularity CE / Apptainer, setuid or user-namespace mode | enroot 3.x, unprivileged |
| Images | SIF (pulled with `singularity pull`) | squashfs (imported with `enroot import`) |
| Inside the sandbox | your uid | root mapped to your uid (or your uid) |
| Per-command overhead | one `singularity exec` | about 2 ms (`nsenter`) |
| `network="none"` | where the site allows network namespaces | always |
| After the owner process dies | instances keep running until a janitor stops them | everything is killed at once; a janitor removes the files |

## Command line

| Command | What it does |
|---|---|
| `python -m hpc_sandbox doctor [--image IMAGE]` | Reports the runtime and version, the scratch and cache filesystems, free space, staging and reflink support, and what matters for the runtime: for Singularity/Apptainer the site settings (`allow setuid`, `allow user ns`, `sessiondir max size`, loop devices); for enroot whether namespaces can be created, whether imports can convert layer whiteouts, whether this process runs inside a container, and the site's `enroot.conf` and hooks. With `--image`, starts a sandbox and exercises exec and transfers. Exits non-zero on failure. |
| `python -m hpc_sandbox run IMAGE [--env K=V] [--bind SRC:DST[:ro\|rw]] [--label K=V] [--network host\|none] [--memory-mb N] [--timeout S] -- CMD...` | Runs one command in a fresh sandbox, prints its output and exits with its status. |
| `python -m hpc_sandbox janitor [--label K=V]...` | Stops and removes sandboxes abandoned by dead processes on this node (optionally only those with the labels) and prints a JSON report. Unless `--runtime` names one, it sweeps Singularity sessions when Singularity or Apptainer is installed and enroot sessions always (sweeping them needs no enroot binary). Use it in job epilogues. |

All commands take `--runtime auto|singularity|enroot` (default
`$HPC_SANDBOX_RUNTIME`, else the first of `singularity`, `apptainer` and
`enroot` found on `PATH`), `--binary`, `--scratch-root` and `--image-cache`.

## How a sandbox is built

What both providers share:

1. **Image.** A reference the provider can never run (a SIF image for enroot,
   a squashfs image for Singularity, a scheme the runtime cannot pull) raises
   `ValueError` before anything is created. A remote reference is pulled once
   into the image cache under a file lock, validated and published read-only.
   The cache is keyed by reference, so a tag that moves keeps the image it
   named when first pulled: pin images by digest (or by file content), or
   delete the cached `uri-<hash>` file and its `.json` sidecar to pull again.
   A local image path may carry an `@sha256:<hex>` suffix to pin the file's
   content. Inside a batch job whose image cache is on another filesystem
   (for example Lustre or NFS),
   the image is staged to node-local scratch as `sha256-<digest>.<ext>`,
   verified while copying.
2. **Session.** A private directory `sessions/hpcsb-<uuid>` (mode 0700) on
   node-local scratch (`sessions-enroot/hpcsb-<uuid>` for enroot), leased
   with `flock` and holding an owner record: the provider, owner PID and
   start time, the container's process identities, who commands run as
   (`remap_root`, `container_uid`), hostname and labels. The writable root
   filesystem is extracted into it as a directory
   owned by the calling uid: no loop devices, overlays or size-capped tmpfs.
3. **Commands.** The host environment never reaches the sandbox. The
   container's own environment (the image's `ENV`), `HOME=/root`,
   `SandboxSpec.env` and per-call `env` are sent, POSIX-quoted, as one line
   ahead of the command's stdin, which a short `sh` prologue inside the
   container reads and evaluates before the command gets the rest (or
   `/dev/null` when there is no `stdin`). Values pass verbatim, appear neither
   on command lines (which every user of a node can read) nor in the host-side
   processes that enter the container, and need nothing from the sandbox's
   filesystem. Names the image's `/bin/sh` reserves cannot be set: with bash,
   `UID`, `EUID`, `PPID`, `BASHPID`, `GROUPS`, `FUNCNAME` and the like; with
   dash, an `OPTIND` that is not a number. The prologue then enters the
   working directory and announces the start on stderr (the announcement is
   removed from the result); a command that never started — a missing working
   directory, a runtime that could not enter the container — raises
   `SandboxError` instead of returning an exit status.
   Output is bounded (head and tail of each stream, 16 MiB); a command that
   leaves a background process holding stdout still returns. Startup probes
   that the rootfs and `workdir` are writable.
4. **Transfers.** Through a fresh directory in the session's private `/tmp`,
   so they work for binary files of any size and see the container's view of
   paths (including its mounts). Code in the sandbox can change that directory
   at any time, so the host side works in it only through file descriptors,
   never follows a symlink it finds there and never blocks on a FIFO.
   Downloads keep modes and timestamps but not owners (inside a user
   namespace, files on host mounts belong to owners the namespace does not
   map), and the copy is always readable by you; special files such as FIFOs
   and directories nested more than 256 levels deep raise `SandboxError`.
   Uploads refuse special files on the host with `SpecialFileError`, except a
   character device given as the source itself (`/dev/null` gives an empty
   file). The destination is yours: a symlink there is followed, a directory
   receives a directory's contents, and only a file replaces an existing file.
5. **Memory.** With `memory_mb`, a watchdog samples the proportional set size
   of the container's processes and of every running command once a second.
   On a breach it kills the largest process, like the kernel OOM killer, and
   the command that owned it returns 137 with an explanation in stderr; a
   harness running in the same sandbox survives a runaway tool call. This is
   a sampled guard, not a cgroup: usage can overshoot between samples.
6. **Snapshots** copy the rootfs and the private tmp directories with
   `cp -a --reflink=auto` (near free on reflink filesystems), after a `sync`.
   Processes and memory are not captured; take snapshots while the sandbox is
   quiescent. Snapshots live on the node that took them; their metadata
   records `remap_root` and `container_uid` like the owner record.
7. **Teardown and janitors.** `stop()` kills running commands, stops the
   container, re-proving process identities before signalling anything, and
   deletes the session. Teardown is shielded from cancellation. A janitor
   considers only `hpcsb-<uuid>` sessions under its root whose lease is free
   and whose owner process is dead, optionally filtered by labels, and leaves
   sessions that belong to another provider; it fails closed (reports and
   leaves the session) whenever identities disagree. Each provider sweeps
   abandoned sessions before its first start.

### Singularity CE / Apptainer

- **Images.** `docker://repo:tag`, `docker://repo@sha256:...`, `oras://`,
  `library://` or a bare `repo:tag`, pulled as SIF and validated with
  `sif list`; cached under the manifest digest when the reference has one
  (pin images by digest for reproducible runs).
- **Root filesystem.** `build --sandbox --fix-perms`. Bind targets are created
  inside it (refusing symlinks that would lead out of it) and a keep-alive
  `%startscript` is written into it: OCI-derived images ship an empty one, and
  an instance whose startscript exits is removed at once.
- **Instance.** `instance start --cleanenv --containall --no-home --no-eval
  --no-mount home,cwd,bind-paths --writable --pid-file ...` with
  session-private `/tmp` and `/var/tmp` binds, `/etc/resolv.conf` read-only
  (host networking), the spec's mounts and an exact name `hpcsb-<uuid>`. No
  `--fakeroot`: the sandbox runs as your uid. Startup checks that the
  runtime's PID, the PID file and the process start time agree.
- **Commands.** `exec --cleanenv --cwd ... instance://hpcsb-<uuid>` running
  the prologue, which also enters the working directory itself: Singularity CE
  silently falls back to another directory when `--cwd` is missing. The
  runtime's own `--env` is not used: it splits on commas, and `--env-file` is
  shell-evaluated.
- **Teardown.** `instance stop --timeout` on the exact instance, then a
  direct signal if it survived. Nothing uses wildcards or
  `instance stop --all`.

### enroot

Enroot has no daemon and no running instances: `enroot start` runs one
command and the container ends with it. A sandbox is therefore one
long-lived `enroot start` whose command is a keep-alive loop.

- **Images.** `docker://repo:tag` (Docker's `registry/path` or enroot's
  `registry#path` spelling), a bare `repo:tag`, `dockerd://` and `podman://`,
  imported with `enroot import` into `<cache>/uri-<hash>.sqsh`. Imports get
  the variables pulling needs and no others: `PATH`, home and locale, XDG
  directories, proxies, CA bundles, `SOURCE_DATE_EPOCH`, `ENROOT_*`, the
  Docker and Podman variables (`DOCKER_*`, `CONTAINER_HOST`, `CONTAINERS_*`,
  `REGISTRY_AUTH_FILE`), every variable the registry credentials file names
  (enroot evaluates `$NGC_API_KEY` or `$(command)` password fields), and the
  names or `PREFIX*` patterns in `import_env` (for example `AWS_*` when a
  password field runs the AWS CLI). Imports run with
  `TAR_OPTIONS=--delay-directory-restore` and use a wrapper around
  `enroot-aufs2ovlfs` that resolves whiteouts enroot 3.5 aborts on, so images
  that revisit read-only directories or shadow their own entries import.
- **Digest pins.** Enroot 3.x misparses `@sha256:` references, so they raise
  `ValueError`. To pin an image, import it once and pass the file with a
  content pin, which is checked before every use:

  ```bash
  enroot import --output /shared/images/task.sqsh docker://registry#org/task:tag
  sha256sum /shared/images/task.sqsh   # -> <hex>
  ```

  ```python
  SandboxSpec(image="/shared/images/task.sqsh@sha256:<hex>")
  ```
- **Root filesystem.** `enroot create` into the session directory.
- **Namespaces.** A small launcher creates new user, mount, PID and IPC
  namespaces — plus a network namespace with only an up loopback interface
  for `network="none"` — and runs `enroot start --rw --root` in them as the
  PID namespace's init. The container's command is the keep-alive loop, so
  the init is that loop: tasks cannot signal it, it reaps orphans, and
  killing it from the host kills every process in the sandbox. The launcher
  and the init are SIGKILLed when the process that started them dies.
- **What enroot is allowed to add.** Sites extend their enroot configuration
  for interactive jobs — home directories, cgroups, Slurm and PMIx variables,
  entrypoints, terminal settings — and none of that belongs in a sandbox.
  `enroot start` therefore uses a system configuration the provider writes
  (`ENROOT_SYSCONF_PATH`): enroot's standard system mounts (`/proc`, `/sys`
  read-only, `/var/run`, `/var/lock`), a minimal `/dev` (a tmpfs with `null`,
  `zero`, `full`, `random`, `urandom`, `tty`, a private `pts` and the usual
  symlinks; never the host's), the host's `/etc/hosts`, no environment files,
  and the standard enroot hooks sandboxes need, copied from the installation
  when it has them enabled: `10-shadow` (passwd and group entries),
  `10-aptfix` (apt works as remapped root) and `98-nvidia` (only acts when
  `gpus` is set, which requires it). The installation's configuration
  directory is found as enroot finds it, by running the configuration section
  of the installed `enroot` script: the environment, `enroot.conf`, its
  drop-ins and enroot's defaults combine exactly as they do for enroot. A new
  sandbox gets the host's time zone as a regular `/etc/localtime` file,
  replacing the image's symlink into its own zoneinfo (a restore keeps the
  snapshot's, unless an earlier version took it). It runs with a minimal
  environment besides: no home directory mount (`ENROOT_MOUNT_HOME=no`; an
  empty value would fall back to the site default, which often mounts it), no
  login shell, no user-level enroot configuration, and no batch-job variables.
  `/tmp` and `/var/tmp` are session-private directories, `/dev/shm` a private
  tmpfs, and `/etc/resolv.conf` the host's, read-only, with host networking
  and `bind_resolv_conf`. The image's entrypoint and `/etc/rc.local` are
  skipped.
- **Commands** enter the init's namespaces and root directory with
  `nsenter` — twice, because the PID and IPC namespaces belong to the
  launcher's user namespace and the mounts to enroot's nested one — and run
  as root (your uid outside) in a couple of milliseconds. The init's
  identity is re-proved before each command.
- **Restores** start from a rootfs that code in the sandbox wrote, and enroot
  works on the rootfs from the host side while it starts. So a restore sets
  the rootfs's `/etc/fstab` and `/etc/environment` aside until the container
  runs (they would otherwise be mounted and loaded with your host
  privileges), skips the hooks that write into the rootfs with host-side
  shell code (their results are in the snapshot; the NVIDIA hook still runs,
  because `nvidia-container-cli` is built to configure untrusted root
  filesystems), refuses a rootfs whose `/etc` or `/dev` is not a directory,
  and gives commands the environment recorded when the snapshot was taken.
  Images are trusted the way enroot trusts them.
- **Teardown.** SIGKILL the init after re-proving its identity; its parent,
  the launcher, reaps it and exits. A janitor finds the processes of an
  abandoned session already gone, kills any survivor whose start time and
  command line still match, and removes the files.

## Configuration

Options both providers take:

| Option | Default | Meaning |
|---|---|---|
| `binary` | `singularity`, else `apptainer` / `enroot` | Runtime executable (name or path). |
| `scratch_root` | see below | Node-local directory for sessions, snapshots, staged images and runtime temp/cache. |
| `image_cache` | `$HPC_SANDBOX_IMAGE_CACHE` or `~/.cache/hpc-sandbox/images` | Image cache; may be shared storage with `flock`. |
| `stage_images` | auto | Stage images to scratch; auto = inside a batch job when the cache is on another filesystem. |
| `default_timeout_s` | 600 | Timeout of `exec` and transfers without an explicit one. |
| `start_timeout_s` | 1800 | Pull, extraction, container start and snapshot copy timeout. |
| `stop_timeout_s` | 60 | Bound on the whole stop. |
| `bind_resolv_conf` | True | Bind the host's `/etc/resolv.conf` with host networking. |
| `extra_start_args` | () | Extra `instance start` / `enroot start` flags. |
| `output_limit_bytes` | 16 MiB | Captured bytes per stream and command. |
| `memory_poll_s` | 1.0 | Watchdog sampling interval. |

`SingularityProvider` only:

| Option | Default | Meaning |
|---|---|---|
| `stop_grace_s` | 10 | `instance stop --timeout`. |
| `allow_network_none` | False | Permit `network="none"` (`--net --network none`); only where users may create network namespaces. Adds the `network_none` capability. |
| `gpu` | None | `"nv"` or `"rocm"` (`--nv` / `--rocm`); adds the `gpu` capability. |
| `cgroup_limits` | False | Pass `cpus` and `memory_mb` as `--cpus` / `--memory` where rootless cgroups work. Otherwise `cpus` is advisory. |

`EnrootProvider` only:

| Option | Default | Meaning |
|---|---|---|
| `remap_root` | True | Run as root inside the sandbox (`enroot start --root`); False runs as your uid. |
| `import_env` | () | Extra variables for `enroot import`: names, or prefixes such as `"AWS_*"`. |
| `gpus` | None | Handed to enroot's NVIDIA hook as `NVIDIA_VISIBLE_DEVICES`: `"all"` for every GPU the process may use (inside a Slurm allocation, the allocated ones), or a list such as `"0,1"` numbered as the job sees its GPUs (like `CUDA_VISIBLE_DEVICES`, not the host indices in `SLURM_STEP_GPUS`). Adds the `gpu` capability. |

Default scratch root: `$HPC_SANDBOX_SCRATCH`, else `$PBS_LOCALDIR/hpc-sandbox`,
else `/local/$PBS_JOBID/hpc-sandbox` when it exists, else
`$TMPDIR/hpc-sandbox-<uid>`. Unless already set, the runtime's temporary and
cache directories (`SINGULARITY_TMPDIR`/`APPTAINER_TMPDIR` and `*_CACHEDIR`,
or `ENROOT_TEMP_PATH` and `ENROOT_CACHE_PATH`) point into it, so pulls and
builds never fill a small `/tmp` or a quota-limited home. Singularity bind
variables (`SINGULARITY_BIND`, ...), `SINGULARITYENV_*`/`APPTAINERENV_*` and
`*_CONFIGDIR` are removed from the runtime's environment.

Capabilities: `{"snapshot", "isolation"}`, plus `network_none` and `gpu` as
described above. Isolation means private PID/IPC namespaces, root filesystem,
`/tmp` and environment; it is not a security boundary against code running
as your uid with host networking.

## Site notes

Run `python -m hpc_sandbox doctor --image IMAGE` on a compute node of a new
site first.

Singularity CE / Apptainer:

- **Setuid or user-namespace mode.** Both work. In setuid mode, network
  namespaces (`--network none`) are usually reserved for root, so leave
  `allow_network_none` off unless `doctor` or the integration tests show it
  works; in unprivileged user-namespace mode (common with Apptainer) it
  normally does (verify with `HPC_SANDBOX_TEST_NETWORK_NONE=1`).
- **No `--fakeroot`, no `--writable-tmpfs`.** Some sites force squashfuse for
  fakeroot overlays and the runtime then silently falls back to a read-only
  rootfs; `sessiondir max size` often caps writable tmpfs at a few tens of
  MiB. Writable rootfs directories on scratch avoid both, and startup probes
  writability so a regression fails loudly.
- **Instance metadata is per user, not per job.** Unprivileged instance
  records live below the passwd home and ignore `SINGULARITY_CONFIGDIR`, so
  every job of a user on a node sees the same instance list. Hence exact UUID
  names, PID files and start-time identities, and never `instance stop --all`,
  which would stop other jobs' instances.
- **Cgroups.** Most sites give users none, hence the PSS watchdog. Where
  Apptainer has rootless cgroups v2 (systemd user delegation),
  `cgroup_limits=True` turns `cpus`/`memory_mb` into real limits. The setuid
  starter is not dumpable; its RSS is charged instead of its PSS.
- **GPUs.** `gpu="nv"` for NVIDIA (`--nv`), `gpu="rocm"` for AMD (`--rocm`).

enroot:

- **Requirements.** enroot 3.x, util-linux `nsenter`, unprivileged user
  namespaces (`/proc/sys/user/max_user_namespaces` above zero), and `/bin/sh`
  and `sleep` in the image. Pulling images needs enroot's `+caps` package:
  `enroot-aufs2ovlfs` converts layer whiteouts with file capabilities.
- **Run on the host, not inside a container.** File capabilities are ignored
  in nested user namespaces, so `enroot import` fails inside a pyxis or
  enroot container (and `enroot` is usually not installed there). Run the
  process that owns the sandboxes as a plain Slurm step, or import images
  beforehand and pass `.sqsh` paths.
- **Sandboxes die with their owner.** The launcher and the container's init
  are killed when the owning process exits, even by SIGKILL; there are no
  orphaned containers to stop, only files for the janitor to remove.
- **Upgrading from an earlier version.** Enroot sessions used to live in
  `sessions` next to the Singularity ones; enroot janitors also sweep the
  enroot sessions they find there, and, where no Singularity or Apptainer is
  installed, sessions there whose owner record is missing.
- **Site configuration.** `enroot.conf` settings such as
  `ENROOT_MOUNT_HOME yes`, `ENROOT_REMAP_ROOT` and the data and runtime paths
  are overridden per sandbox, and the site's mounts, environment files and
  hooks are replaced by the provider's own system configuration (see above).
  `doctor` lists the site's hooks and which standard ones sandboxes get; a
  standard hook the installation lacks or has disabled is skipped.

Both:

- **Paths.** Keep the image cache on shared storage that supports `flock` and
  let staging copy images to node-local disk. Point `HPC_SANDBOX_SCRATCH` at
  job-local disk when `$TMPDIR` is small or shared (for example
  `/local/$SLURM_JOB_ID`). Tasks launched with `pbsdsh` or `srun` may start
  with a minimal environment: pass the variables the defaults rely on
  (`PBS_JOBID`, `PBS_LOCALDIR`, `SLURM_JOB_ID`, `TMPDIR`) through, and run
  `python -m hpc_sandbox janitor` in the job epilogue.

## Tests

```bash
python -m pytest                       # unit tests, no container runtime needed
HPC_SANDBOX_INTEGRATION=1 python -m pytest tests/test_integration.py -v   # compute node
```

Unit tests put a fake `singularity` (`tests/fake_singularity.py`) and a fake
`enroot` (`tests/fake_enroot.py`) first on `PATH`. The fake Singularity
simulates instances as real host processes with the real process title; the
fake enroot builds containers from real user and mount namespaces, binds and
`chroot`, so the enroot launcher and `nsenter` paths run for real (these tests
are skipped where unprivileged user namespaces are unavailable). Identity
checks, the janitor, the watchdog, timeouts and cancellation are exercised
with real processes. The integration tests need a compute node (never run
containers on a login node); `HPC_SANDBOX_TEST_RUNTIME` selects the runtime
(`singularity`, `enroot`, or `auto` as on the command line) and
`HPC_SANDBOX_TEST_IMAGE` the image (default `docker://alpine:3.20`).

## Not yet

- an ext3 overlay mode as an alternative to rootfs directories;
- memory snapshots, and per-node PID namespaces inside one shared sandbox.

## License

MIT, see [LICENSE](LICENSE).
