# Command line

The `hpc-sandbox` command checks whether a node can run sandboxes, pulls images, runs one command in
a fresh sandbox, fetches the helper that pods use for egress, and sweeps up what dead processes left
behind. `python -m hpc_sandbox` runs the same program, for example from a virtual environment whose
scripts are not on `PATH`.

```text
hpc-sandbox [-h] [--version] COMMAND ...
```

Sandboxes and their files live on node-local scratch, so run `doctor`, `run` and `janitor` on the
compute node they concern, inside your batch job. `pull` works wherever the runtime is installed and
the image cache can be reached, and `fetch-tools` wherever there is internet access.

## Commands

| Command | What it does |
|---|---|
| `hpc-sandbox doctor [--image IMAGE]` | Checks whether this node can run sandboxes and, with `--image`, starts one and exercises it. |
| `hpc-sandbox run [OPTIONS] IMAGE -- CMD [ARG...]` | Runs one command in a fresh sandbox and exits with its status. |
| `hpc-sandbox pull [--refresh] IMAGE [IMAGE...]` | Pulls images into the image cache, or checks local image files, and prints a JSON line for each. |
| `hpc-sandbox fetch-tools [--arch ARCH]` | Downloads the pinned slirp4netns that pods use for egress when the host has none. |
| `hpc-sandbox janitor [--label KEY=VALUE]...` | Stops and removes the sandboxes and pods that dead processes left on this node, and prints a JSON report. |

`hpc-sandbox --version` prints `hpc-sandbox <version>`: the version that a source checkout's
`pyproject.toml` sets when hpc-sandbox runs from one, else the installed distribution's.
`hpc-sandbox COMMAND --help` lists a command's options.

## Common options

`doctor`, `run`, `pull` and `janitor` take these options; `fetch-tools` takes none of them.

| Option | Default | Meaning |
|---|---|---|
| `--runtime {auto,singularity,enroot,gvisor}` | `$HPC_SANDBOX_RUNTIME`, else `auto` | The container runtime, and with it the provider. `auto` takes the first of `singularity`, `apptainer` and `enroot` found on `PATH`, and never gVisor, which runs only when it is chosen. A `$HPC_SANDBOX_RUNTIME` that is none of the four ends the command with status 2. |
| `--binary BINARY` | detected | The runtime's executable, a name on `PATH` or a path: runsc for gVisor. With `--runtime auto`, a file name that starts with `enroot` selects enroot, one that starts with `runsc` gVisor, and any other Singularity. |
| `--scratch-root SCRATCH_ROOT` | `$HPC_SANDBOX_SCRATCH`, else `hpc-sandbox` in the PBS job's local directory, else `$TMPDIR/hpc-sandbox-<uid>` | Node-local scratch for sessions, snapshots, staged images and the runtime's temporary files; see [The scratch root](configuration.md#the-scratch-root). |
| `--image-cache IMAGE_CACHE` | `$HPC_SANDBOX_IMAGE_CACHE`, else `${XDG_CACHE_HOME:-~/.cache}/hpc-sandbox/images` | Where pulled images, SIF or squashfs files, are cached; see [The image cache](configuration.md#the-image-cache). |

A command builds one provider from these options and leaves every other provider option at its
default (`run --allow-network-none` aside). `HPC_SANDBOX_ROOTFS_LAYOUT` still chooses the layout of
Singularity sandboxes; [Environment variables](configuration.md#environment-variables) lists every
variable.

## `doctor`

```text
hpc-sandbox doctor [--image IMAGE] [common options]
```

Checks whether this node can run sandboxes with the selected runtime, and prints one line per check:
its status (`[  ok]`, `[info]`, `[warn]` or `[FAIL]`), its name and what it found. Run it first on a
compute node of a new site, with the image you plan to use.

| Option | Meaning |
|---|---|
| `--image IMAGE` | Also start a sandbox from IMAGE, labelled `hpc-sandbox=doctor`; run `echo` in it, copy a file in and out, stop it, and wait up to 60 seconds for its files to be deleted. |

| Check | Runtime | What it reports |
|---|---|---|
| `runtime` | all | The runtime's path and version. `FAIL` when it cannot be run, which ends the report. |
| `runsc helpers` | gVisor | The `gvisor-bin/` directory beside runsc, or `info` that there is none, which releases that ship `gvisor.tar` need. |
| `images` | gVisor | The Singularity CE or Apptainer that pulls and extracts the images, and its version. `FAIL` when it cannot be run. |
| `user namespaces` | gVisor | Whether unprivileged user namespaces can be created, as rootless runsc needs. `FAIL` if not. |
| `rootless runsc` | gVisor | Whether `runsc --rootless do true` runs, on the default platform (`systrap`), with a state directory on the scratch root. `FAIL` with runsc's output if not. |
| `nsenter` | enroot | util-linux `nsenter`, which every command uses. `FAIL` when it is missing. |
| `namespaces` | enroot | Whether new user, mount, PID, IPC and network namespaces can be created. `FAIL` if not. |
| `user namespace` | enroot | `warn` when this process runs inside a container's user namespace, where `enroot import` cannot use its file capabilities. |
| `image imports` | enroot | Whether `enroot-aufs2ovlfs` can convert layer whiteouts on the scratch root, as imports need. `warn` when it cannot, so that only local `.sqsh` images work, or when the scratch root does not let `doctor` test it. |
| `site: <setting>` | Singularity, enroot | Singularity CE and Apptainer: `allow setuid`, `allow user ns`, `sessiondir max size`, `max loop devices`, `enable overlay`, `enable underlay` and `mount home` from the installation's configuration file; a `sessiondir max size` below 1024 is shown as `warn`. enroot: the `enroot.conf` settings the provider overrides for every sandbox, such as `ENROOT_MOUNT_HOME`. |
| `site hooks` | enroot | The hooks the enroot installation runs. |
| `sandbox hooks` | enroot | Which of the hooks sandboxes use (`10-aptfix.sh`, `10-shadow.sh` and `98-nvidia.sh`) the installation has enabled. `warn` names those it lacks, which sandboxes start without. |
| `batch job` | all | `$PBS_JOBID` or `$SLURM_JOB_ID`, or none. |
| `scratch root` | all | Its path, filesystem type and free space. `FAIL` when it cannot be created or written, or belongs to another user; inside a batch job, `warn` when it is on a shared filesystem or a tmpfs. |
| `image cache` | all | Its path, filesystem type and free space. `FAIL` when it cannot be created or written. |
| `image staging` | all | The directory images are staged to, or `off`. |
| `reflink on scratch` | all | Whether the scratch root's filesystem makes reflink copies: snapshots are cheap with them, and full copies without. |
| `pods: egress` | Singularity, enroot | The slirp4netns that pods with egress would run, or `warn` with how to get one. |
| `pods: subordinate ids` | Singularity | The uids and gids that pods with `users="subids"` would map, from your first ranges in `/etc/subuid` and `/etc/subgid`, and the `newuidmap` and `newgidmap` that map them. `info` when your user has no range, `warn` when either tool is missing: other pods need neither ([Pods](pods.md#subordinate-ids)). |
| `sandbox start`, `exec`, `upload/download`, `sandbox stop` | all | With `--image` only: the steps above, each `ok` or `FAIL`. |

Once it has found the runtime, `doctor` creates the scratch root, with mode 0700, and the image
cache when they do not exist, as a start does. One that cannot be created or written is a `[FAIL]`
line. `doctor` exits with status 1 when a check failed, else 0: warnings do not fail it. An abridged
example from an enroot node:

```text
[  ok] runtime: /usr/bin/enroot (3.5.0)
[  ok] nsenter: /usr/bin/nsenter
[  ok] namespaces: user, mount, PID, IPC and network namespaces
[  ok] image imports: /usr/bin/enroot-aufs2ovlfs converts whiteouts
[info] site: ENROOT_MOUNT_HOME: yes (overridden per sandbox)
[  ok] sandbox hooks: 10-aptfix.sh, 10-shadow.sh, 98-nvidia.sh
[info] batch job: 123456
[  ok] scratch root: /local/scratch/hpc-sandbox (xfs, 850 GiB free)
[  ok] image cache: /home/<user>/.cache/hpc-sandbox/images (nfs4, 120 GiB free)
[info] image staging: to /local/scratch/hpc-sandbox/images
[info] reflink on scratch: yes (snapshots are cheap)
[  ok] pods: egress: slirp4netns /home/<user>/.cache/hpc-sandbox/tools/slirp4netns-1.3.6-x86_64
[  ok] sandbox start: hpcsb-b6a17d7363c2477bb9c675b4dc1371f6 in 0.4 s
[  ok] exec: exit 0
[  ok] upload/download: round trip
[  ok] sandbox stop: session deleted
```

## `run`

```text
hpc-sandbox run [OPTIONS] IMAGE -- CMD [ARG...]
```

Starts a fresh sandbox from IMAGE, runs one command in it, stops it, and exits with the command's
status. Use it to try an image or a site; programs use the [Python API](python-api.md).

| Option | Default | Meaning |
|---|---|---|
| `--workdir WORKDIR` | `/app` | The command's working directory, an absolute path; created when the image lacks it. |
| `--env KEY=VALUE` | none | A variable for the command. Repeat it for more. |
| `--bind SRC:DST[:ro\|rw]` | none | Bind the host path SRC at DST, read-only unless `:rw`. SRC is resolved from the current directory, symlinks included. Repeat it for more. |
| `--label KEY=VALUE` | none | A label recorded with the sandbox, which `janitor --label` selects by. Repeat it for more. |
| `--network {host,none}` | `host` | `none` gives the sandbox only a loopback interface. |
| `--allow-network-none` | off | Let Singularity use `--network none`, where the site lets users create network namespaces. enroot and gVisor always can. |
| `--memory-mb MEMORY_MB` | none | Arm the memory watchdog with this limit, in MiB. A gVisor sandbox is stopped whole when it exceeds it. |
| `--timeout TIMEOUT` | `600` | Seconds after which the command is killed. |

Everything after the first `--` is the command, run as an argv without a shell; the options and
IMAGE come before it. For shell syntax, run a shell:

```bash
hpc-sandbox run --env NAME=world docker://alpine:3.20 -- sh -c 'echo "hello $NAME from $(pwd)"'
```

```text
hello world from /app
```

The command's output is printed once it has exited and the sandbox has stopped: its stdout on
stdout and its stderr on stderr, each kept to 16 MiB (the head and the tail). hpc-sandbox's own
notes, such as the timeout note or a report of the memory watchdog, are on stderr. Before it exits,
`run` waits up to 60 seconds for the sandbox's files to be deleted.

`run` exits with the command's own status: `124` when `--timeout` killed it, `126` when the
working directory could not be entered, `137` when the memory watchdog killed it, and whatever the
command returned otherwise (see [Exit codes](python-api.md#exit-codes)). With gVisor, the memory
watchdog stops the whole sandbox instead, and `run` exits with status 2. When the sandbox cannot
start or the command cannot run, it prints `hpc-sandbox: ` and the reason on stderr, and exits with
status 2, which a command can return too: that message tells the two apart.

## `pull`

```text
hpc-sandbox pull [--refresh] [common options] IMAGE [IMAGE...]
```

For each IMAGE in turn, pulls a remote reference into the image cache unless it is cached already,
or checks a local image file (that it exists, is an image of the runtime's format and matches its
`@sha256:` pin), then prints one JSON line on stdout. Errors and log messages go to stderr, so
stdout holds only these lines. Use it to fill the cache before a job's sandboxes start, or to
refresh a tag.

| Option | Meaning |
|---|---|
| `--refresh` | Pull remote tags again even if they are cached. Sandboxes started afterwards get the new image, running sandboxes keep theirs, and starts never wait for a refresh. It changes nothing for digest-pinned references and local files, which always name the same image. |

| Key | Value |
|---|---|
| `reference` | The IMAGE argument, as given. |
| `path` | The image file: in the image cache, or the local file. |
| `sha256` | The SHA-256 of the image file. |
| `manifest_digest` | `"sha256:<hex>"` for a digest-pinned remote reference, else `null`. |

These keys are stable. For example, on an enroot node:

```bash
hpc-sandbox pull docker://alpine:3.20
```

```text
{"reference": "docker://alpine:3.20", "path": "/home/<user>/.cache/hpc-sandbox/images/sha256-ca08dbe62eacd34c6e75dc30f23c4c508277b23073e9607ca6430e09834931f9.sqsh", "sha256": "ca08dbe62eacd34c6e75dc30f23c4c508277b23073e9607ca6430e09834931f9", "manifest_digest": null}
```

- `pull` stops at the first image that fails, with status 2; the lines it printed before stay valid.
- With enroot, every refresh imports a new image file, since squashfs files are not reproducible to
  the byte, so `sha256` changes even when the tag did not move. Earlier files stay in the cache
  until you delete them; see [The image cache](configuration.md#the-image-cache).
- With enroot, imports need enroot's `+caps` package and must run on the host rather than in a
  container. enroot builds each image in `$ENROOT_TEMP_PATH`, by default a directory of the scratch
  root, which must be on local disk.

## `fetch-tools`

```text
hpc-sandbox fetch-tools [--arch ARCH]
```

Downloads the static slirp4netns release that this version of hpc-sandbox pins (1.3.6) from the
slirp4netns project's GitHub releases, checks it against its pinned SHA-256, and makes it
executable. Pods use it for egress when the provider's `slirp4netns` option is not set and `PATH`
has no slirp4netns with the options pods need. The file goes to `$HPC_SANDBOX_TOOLS`, else
`${XDG_CACHE_HOME:-~/.cache}/hpc-sandbox/tools`, as `slirp4netns-<version>-<arch>`; when it is there
already, it is only checked. Run it once where there is internet access, into a directory the
compute nodes share: they need no internet then. The download honours the usual proxy variables,
such as `https_proxy`.

| Option | Default | Meaning |
|---|---|---|
| `--arch ARCH` | this machine's | The machine architecture to fetch for: `x86_64` or `aarch64`. Providers use the file of their own node's architecture. |

It prints one JSON line on stdout, with the keys `tool`, `version` and `path`:

```text
{"tool": "slirp4netns", "version": "1.3.6", "path": "/home/<user>/.cache/hpc-sandbox/tools/slirp4netns-1.3.6-x86_64"}
```

It exits with status 2 when no release is pinned for the architecture, or when the file's digest
does not match. A download that fails ends with a Python traceback and status 1.

## `janitor`

```text
hpc-sandbox janitor [--label KEY=VALUE]... [common options]
```

Stops and removes the sandboxes and pods that dead processes left on this node, and prints a JSON
report. A session is left behind when no process holds its lease and the process that started it
is gone. Sessions of live processes are never touched: the report lists them as `live`.

| Option | Meaning |
|---|---|
| `--label KEY=VALUE` | Only sessions whose labels include this pair. Repeat it for more pairs. |

- With `--runtime singularity`, `--runtime enroot` or `--runtime gvisor` (or
  `$HPC_SANDBOX_RUNTIME`), or with `--binary`, it sweeps that runtime's sessions only. Otherwise it
  sweeps Singularity's sessions when `singularity` or `apptainer` is on `PATH`, and enroot's and
  gVisor's always: their sandboxes die with their owners, so sweeping them needs neither enroot nor
  runsc.
- It finishes the deletions that killed processes left in a trash. With Singularity's sessions, it
  also unmounts the fuse-overlays that killed jobs left dead on the node, and, with Singularity's or
  gVisor's, deletes the extractions of templates whose process died.
- It waits for other janitors of the same scratch root, as `reap()` does and unlike the sweep at a
  provider's first start, and it deletes what it removes before it exits.

The report merges those of the runtimes it swept. Each key holds a list of strings; sandboxes and
pods are named by their ids, `hpcsb-` and 32 hexadecimal digits:

| Key | Holds |
|---|---|
| `stopped` | The sandboxes and pods whose processes it stopped. |
| `removed` | The sessions it deleted. |
| `live` | The sessions it left alone because a live process holds them. |
| `errors` | One message for each thing it could not do, such as `could not sweep the singularity sessions: ...` when a runtime's sessions could not be swept at all. The sessions concerned are kept. |

These keys are stable. After a process that had started one enroot sandbox was killed:

```text
{
  "stopped": [],
  "removed": [
    "hpcsb-c14e790911444d919ccef4bbb06f20ea"
  ],
  "live": [],
  "errors": []
}
```

`janitor` exits with status 0 when `errors` is empty, 1 when it is not, and 2 for a malformed
`--label` or an invalid `HPC_SANDBOX_RUNTIME`. Run it at the end of your batch script, with the
scratch root your sandboxes used, for example from a trap:

```bash
trap 'hpc-sandbox janitor' EXIT
```

Scheduler epilogues are set up by administrators and usually run as root, whose janitor would sweep
root's sessions, not yours. [Getting started](getting-started.md) has complete Slurm and PBS batch
scripts.

## Exit codes

| Status | Commands | Meaning |
|---|---|---|
| `0` | all | Success. For `doctor`, no check failed; warnings are allowed. For `janitor`, the report has no errors. |
| `1` | `doctor` | At least one check failed: a `[FAIL]` line. |
| `1` | `janitor` | The report lists errors. Everything else was swept. |
| `1` | all | An unexpected error, with a Python traceback, such as a download of `fetch-tools` that failed. |
| `2` | all | Invalid arguments or values: argparse's usage errors, an `HPC_SANDBOX_RUNTIME` that is not one of `--runtime`'s choices, a `KEY=VALUE` without `=`, a relative `--workdir`, an image the runtime can never run. Or an infrastructure failure: a runtime that is missing, a pull that failed, a sandbox that could not start. The reason follows `hpc-sandbox: ` on stderr. |
| the command's | `run` | The command's own status, including `124` (timed out), `126` (working directory) and `137` (killed by the memory watchdog). |

## Next steps

- [Getting started](getting-started.md): install hpc-sandbox and run it inside Slurm and PBS jobs.
- [Configuration](configuration.md): the scratch root, the image cache and every environment
  variable.
- [Python API](python-api.md): what `run` does, from your own program.
- [Pods](pods.md): what `fetch-tools` is for.
