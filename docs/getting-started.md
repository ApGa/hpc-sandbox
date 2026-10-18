# Getting started

This page takes you from installation to sandboxes in a batch job: what a node needs, how to
install hpc-sandbox and check a compute node, your first sandbox from Python and from the command
line, and Slurm and PBS batch scripts that leave nothing behind.

## Requirements

hpc-sandbox is a Python library and a command for Linux. It needs Python 3.11 or later and nothing
outside Python's standard library, no daemon and no root: it runs sandboxes with the container
runtime your site provides, on its compute nodes. What else a node needs depends on what you use:

| Requirement | Needed for |
|---|---|
| Linux and Python 3.11 or later | everything |
| Singularity CE or Apptainer, installed setuid or in user-namespace mode, or enroot 3.x | sandboxes: one of these runtimes on the compute nodes ([Singularity CE and Apptainer](singularity.md#requirements), [enroot](enroot.md#requirements)) |
| a directory on the node's local disk | the scratch root, which holds the root filesystem of every sandbox ([Put the sandboxes on local disk](#put-the-sandboxes-on-local-disk)) |
| an image with `/bin/sh` and the basic tools `sleep`, `mkdir`, `rmdir`, `rm`, `cp` and `dirname` | every sandbox ([Image references](python-api.md#image-references)) |
| unprivileged user namespaces | enroot sandboxes, pods with either runtime, and Singularity's fuse-overlay layout |
| enroot's `+caps` package, whose `enroot-aufs2ovlfs` converts layer whiteouts | importing images with enroot; local `.sqsh` files need none |
| util-linux `nsenter` | every command in an enroot sandbox or a pod service |
| util-linux `prlimit` | commands in pod services that set `ulimits` |
| util-linux `flock` | optional: with it, the background deletions of stopped sandboxes run one at a time per session root; without it, all at once |
| GNU coreutils and `find` | copies with `cp -a --reflink=auto` (snapshots, restores and Singularity's templates), and deleting stopped sandboxes with `find` and `rm --one-file-system`, in the background under `nice -n 19` |
| slirp4netns 0.4 or later, or the one `hpc-sandbox fetch-tools` downloads, and a `/dev/net/tun` that users may open | pods with egress ([Pods](pods.md#egress)) |
| `fuse-overlayfs`, `fusermount3` or `fusermount`, and `/dev/fuse` | Singularity's fuse-overlay layout; janitors need only `fusermount3` or `fusermount` ([Singularity CE and Apptainer](singularity.md#the-fuse-overlay-layout)) |

`hpc-sandbox doctor` checks the runtime, the scratch root and the image cache on a node, and with
enroot also the namespaces, `nsenter` and image imports ([Check the
installation](#check-the-installation)).

## Install

Install hpc-sandbox into a virtual environment on storage that the compute nodes can read, such as
your home or project directory:

```bash
python3 -m venv .venv          # Python 3.11 or later
source .venv/bin/activate
pip install hpc-sandbox
```

Where the node's `python3` is older than 3.11, load a newer one (sites often provide Python as an
environment module) or use [uv](https://docs.astral.sh/uv/): `uv venv --python 3.12` creates the
environment, with a Python that uv downloads if the node has none, and `uv pip install hpc-sandbox`
installs into it. In a uv project, run `uv add hpc-sandbox`. hpc-sandbox has no dependencies and no
compiled code.

This installs the `hpc_sandbox` package and the `hpc-sandbox` command. `python -m hpc_sandbox` runs
the same command, for example with an environment that is not activated:
`.venv/bin/python -m hpc_sandbox doctor`.

hpc-sandbox's version is below 1.0: a minor release (0.4.0) may change the Python API or the command
line, and a patch release (0.3.1) only fixes bugs. In a project that depends on hpc-sandbox, require
one minor release, such as `hpc-sandbox>=0.3,<0.4`. [CHANGELOG.md](../CHANGELOG.md) lists what each
release changes, and [Public API and versioning](python-api.md#public-api-and-versioning) says what
the API is.

### Install from source

To run the code of the main branch, install it from GitHub:

```bash
pip install git+https://github.com/ApGa/hpc-sandbox.git
```

Or clone the repository and install the checkout, with pip or with uv:

```bash
git clone https://github.com/ApGa/hpc-sandbox.git
cd hpc-sandbox
pip install .                  # or: uv pip install .
```

To change hpc-sandbox and run its tests, set up a development environment as
[CONTRIBUTING.md](../CONTRIBUTING.md#set-up-a-development-environment) describes.

## Check the installation

Both of these print the installed version:

```bash
hpc-sandbox --version
python -c "import hpc_sandbox; print(hpc_sandbox.__version__)"
```

Then check a compute node: hpc-sandbox starts containers and fills node-local disk, which login
nodes are not for, so run it on compute nodes, inside a batch job. Start an interactive job, for
example with `srun --nodes=1 --pty bash` on Slurm or `qsub -I` on PBS, adding the account,
partition or queue and resources your site asks for. In the job, activate the environment and run:

```bash
hpc-sandbox doctor --image docker://alpine:3.20
```

`doctor` prints one line per check, each starting with its status: `[  ok]`, `[info]`, `[warn]` or
`[FAIL]`. It reports the runtime and its version, the site settings that matter to the provider,
the scratch root and the image cache with their filesystems and free space, and the slirp4netns
that pods would use. With `--image`, it also pulls the image, starts a sandbox from it, runs a
command in it, copies a file in and out, and stops it. On a node that runs sandboxes, the last lines
read like these:

```text
[  ok] sandbox start: hpcsb-a8fca638875449f5b10f3b722f1a6b83 in 0.2 s
[  ok] exec: exit 0
[  ok] upload/download: round trip
[  ok] sandbox stop: session deleted
```

`doctor` exits with status 1 when a check failed, and 0 otherwise: warnings do not fail it.
[Command line](cli.md#doctor) explains every check. These lines are the ones that most often need
something from you:

| Line | What to do |
|---|---|
| `[FAIL] runtime` | No runtime was found, or it does not run. Put `singularity`, `apptainer` or `enroot` on `PATH` (sites often provide them as environment modules), or name it with `--binary`. |
| `[FAIL] namespaces` | The node does not let users create the namespaces that enroot sandboxes need. Ask your site's administrators. |
| `[warn] image imports` | enroot cannot import images here. Install enroot's `+caps` package, run on the host rather than inside a container, or use local `.sqsh` images. |
| `[warn] scratch root` | Inside a batch job, the scratch root is on a shared filesystem or a tmpfs. Set `HPC_SANDBOX_SCRATCH` to a directory on the node's local disk ([Put the sandboxes on local disk](#put-the-sandboxes-on-local-disk)). |
| `[FAIL] scratch root`, `[FAIL] image cache` | The directory cannot be written. Choose another with `HPC_SANDBOX_SCRATCH` or `HPC_SANDBOX_IMAGE_CACHE`. |
| `[warn] pods: egress` | No slirp4netns that pods can use was found. Only pods with egress need one: run `hpc-sandbox fetch-tools` once, where there is internet access ([Pods](pods.md#egress)). |
| `[FAIL] sandbox start` | The message says why. When the compute nodes cannot reach the registry, run `hpc-sandbox pull IMAGE` on a machine that can, into an image cache the compute nodes share ([The image cache](configuration.md#the-image-cache)). |

## Your first sandbox

Save this as `first_sandbox.py` and run it with `python first_sandbox.py` on a compute node, in the
interactive job from above. It starts a sandbox from the `python:3.12-slim` image, copies a script
into it, runs the script twice, and copies a result out:

```python
import asyncio
from pathlib import Path

from hpc_sandbox import SandboxSpec, SingularityProvider


async def main() -> None:
    provider = SingularityProvider()  # EnrootProvider() on a site with enroot
    spec = SandboxSpec(image="docker://python:3.12-slim", memory_mb=2048)
    Path("square.py").write_text("import sys\nprint(int(sys.argv[1]) ** 2)\n")
    async with await provider.start(spec) as sandbox:  # stopped when the block exits
        await sandbox.upload("square.py", "/app/square.py")
        result = await sandbox.exec(["python", "square.py", "12"], timeout_s=60)
        print(result.exit_code, result.stdout, end="")
        await sandbox.exec("python square.py 25 > result.txt")
        await sandbox.download("/app/result.txt", "result.txt")
    print(Path("result.txt").read_text(), end="")
    await provider.wait_for_deletions()  # the sandbox's files are deleted in the background


asyncio.run(main())
```

```text
0 144
625
```

- `SingularityProvider()` and `EnrootProvider()` take the same `SandboxSpec` and return sandboxes
  with the same methods: use the one for your site's runtime. Their defaults suit a batch job
  ([Configuration](configuration.md)).
- The first start pulls the image into the image cache, once. Later starts use the cached file, and
  so do other jobs that share the cache.
- `exec` runs an argv list as it is, and a string with `/bin/sh -c`, in the working directory
  `/app`. Relative paths in the sandbox resolve from there, in `upload` and `download` too. It
  returns the command's `exit_code`, `stdout` and `stderr`; when the sandbox itself fails, it raises
  `SandboxError` instead ([Running commands](python-api.md#running-commands)).
- Every command has a timeout: its `timeout_s`, else the provider's `default_timeout_s`, 600
  seconds. A command that times out is killed and returns exit code 124.
- `memory_mb` arms the memory watchdog, which kills the branch of processes that holds the memory
  when the sandbox uses more than that ([Memory limits](python-api.md#memory-limits)).
- `async with` stops the sandbox when the block exits, however it exits. The stop returns once the
  sandbox's processes are gone, and a background process deletes its files;
  `wait_for_deletions()` waits for that ([Wait for deletions before the program
  exits](#wait-for-deletions-before-the-program-exits)).

From the command line, `hpc-sandbox run` does the same for one command. It starts a fresh sandbox,
runs the command that follows `--` (an argv, without a shell), prints its output, stops the sandbox,
and exits with the command's status:

```bash
hpc-sandbox run docker://python:3.12-slim -- python -c 'print(12 ** 2)'
```

```text
144
```

Its options, such as `--env KEY=VALUE`, `--bind SRC:DST[:ro|rw]`, `--memory-mb N` and
`--timeout S`, come before `--`; [Command line](cli.md#run) lists them all.

## Run in a batch job

hpc-sandbox runs inside your batch jobs, on the compute nodes they get. Three things make a job's
sandboxes run well and leave nothing behind: a scratch root on the node's local disk, a program that
waits for its deletions before it exits, and a janitor at the end of the batch script.

### Put the sandboxes on local disk

The root filesystem of every sandbox, and everything it writes, lives on the scratch root, which
must be on the node's local disk. Unless `HPC_SANDBOX_SCRATCH` names it, the scratch root is the
job's directory on local disk on PBS sites that provide one as `$PBS_LOCALDIR` or as
`/local/$PBS_JOBID`, else a directory in `$TMPDIR` ([The scratch
root](configuration.md#the-scratch-root) has the exact rules). Many Slurm sites point `TMPDIR` at a
directory of the job's own on local disk. Where yours does not, or where that directory is small,
shared or a tmpfs, set `HPC_SANDBOX_SCRATCH` to a directory on the node's local disk, such as one
your site gives each job. `hpc-sandbox doctor`, run inside a job, shows where the scratch root is,
its filesystem and its free space, and warns when it is on a shared filesystem or a tmpfs.

A per-job directory that the site deletes when the job ends takes whatever sandboxes left in it
along. A scratch root that outlives jobs keeps staged copies of images and, with Singularity,
templates; [The scratch root](configuration.md#the-scratch-root) says what you can remove.

The image cache is in your home directory by default (`~/.cache/hpc-sandbox/images`, or below
`$XDG_CACHE_HOME`). Point `HPC_SANDBOX_IMAGE_CACHE` at project storage when images are large, and
pull images before a run with `hpc-sandbox pull IMAGE...`, so that no job waits for a pull
([The image cache](configuration.md#the-image-cache)).

### Wait for deletions before the program exits

A stop hands the deletion of a sandbox's files to a detached process. The end of a batch job, or of
the Slurm step that runs your program, kills such processes and leaves the rest of their work to a
janitor. So end your program with `await provider.wait_for_deletions()`, as the examples on this
page do. It waits until the deletions that the process started have finished, and returns `False`
if they take longer than its `timeout_s`, 300 seconds by default. `hpc-sandbox run` and
`hpc-sandbox doctor` wait for their own deletions.

### Run the janitor at the end of the script

A program that crashes or is killed leaves its sandboxes behind: enroot sandboxes, and pods with
either runtime, die with it but leave their files, and Singularity instances keep running.
`hpc-sandbox janitor` stops what still runs for them and deletes their files, on the node it runs
on and in the scratch root that its environment names (`HPC_SANDBOX_SCRATCH`, else the default). It
touches only sandboxes whose process is gone, so it never disturbs a program that still runs. Run it
at the end of the batch script, from a trap, so that it runs however the script ends:

```bash
trap 'hpc-sandbox janitor' EXIT
```

Bash runs this trap when the script exits, also on the SIGTERM that the scheduler sends when it
cancels the job or the time limit passes; the janitor then has until the scheduler's SIGKILL to
finish. Do not rely on a scheduler epilogue instead: the site's administrators configure it, and it
usually runs as root, whose janitor would look for root's sandboxes, not yours.

### A Slurm batch script

```bash
#!/bin/bash
#SBATCH --job-name=sandboxes
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00

cd "$SLURM_SUBMIT_DIR"
source .venv/bin/activate

# Sandboxes go below $TMPDIR. Where it is small, shared or a tmpfs, name a directory on the
# node's local disk instead, such as the one your site gives each job:
# export HPC_SANDBOX_SCRATCH="/path/on/local/disk/$SLURM_JOB_ID"

trap 'hpc-sandbox janitor' EXIT    # removes what run.py leaves behind, however the script ends
python run.py
```

`run.py` is your program, such as `first_sandbox.py` from [Your first sandbox](#your-first-sandbox).
Submit the batch script with `sbatch`. The script exits with the status of `run.py`; the janitor
prints its report into the job's output. With enroot, run the program on the host, as here, not
inside a container that pyxis starts (`srun --container-image`): enroot cannot import images there
([enroot](enroot.md#site-notes)).

### A PBS batch script

```bash
#!/bin/bash
#PBS -N sandboxes
#PBS -l select=1:ncpus=16:mem=64gb
#PBS -l walltime=02:00:00
#PBS -j oe
#PBS -S /bin/bash

cd "$PBS_O_WORKDIR"
source .venv/bin/activate

# The scratch root defaults to the job's local directory where the site provides $PBS_LOCALDIR or
# /local/$PBS_JOBID. Where it provides neither, name a directory on the node's local disk:
# export HPC_SANDBOX_SCRATCH="/path/on/local/disk/$PBS_JOBID"

trap 'hpc-sandbox janitor' EXIT    # removes what run.py leaves behind, however the script ends
python run.py
```

The resource line follows PBS Pro; adapt it to your site. `-S /bin/bash` makes PBS run the script
with bash, whatever your login shell. Submit the script with `qsub`.

### Jobs on several nodes

Sandboxes, scratch roots and janitors belong to one node. In a job on several nodes, run one copy of
your program on each node, each with sandboxes of its own, and the janitor on each node after it.
A script that each node runs does both:

```bash
#!/bin/bash
# node.sh: the program and the janitor of one node
cd "$(dirname "$0")"
source .venv/bin/activate
trap 'hpc-sandbox janitor' EXIT
python run.py
```

Make it executable (`chmod +x node.sh`). With Slurm, request one task per node (for example
`#SBATCH --nodes=4` and `#SBATCH --ntasks-per-node=1`) and start `node.sh` with `srun` from the
batch script. `srun` passes the batch script's environment to its tasks, including
`HPC_SANDBOX_SCRATCH`, unless `--export` or `SLURM_EXPORT_ENV` limits what it passes:

```bash
export HPC_SANDBOX_SCRATCH="/path/on/local/disk/$SLURM_JOB_ID"   # where $TMPDIR will not do
srun ./node.sh
```

With PBS, start `node.sh` on each node with `pbsdsh`, once per node (how to ask for that depends on
your PBS and your resource request). Tasks that `pbsdsh` starts do not get the environment of your
batch script, so pass the variables they need with `env`: `PBS_JOBID` tells hpc-sandbox that it
runs in a batch job (which turns image staging on when the image cache is on another filesystem),
and `HPC_SANDBOX_SCRATCH` names the scratch root. Set it explicitly in such jobs:

```bash
export HPC_SANDBOX_SCRATCH="/path/on/local/disk/$PBS_JOBID"
pbsdsh -- /usr/bin/env PBS_JOBID="$PBS_JOBID" HPC_SANDBOX_SCRATCH="$HPC_SANDBOX_SCRATCH" \
    "$PBS_O_WORKDIR/node.sh"
```

Pass paths and identifiers this way, never secrets: every user of a node can read the command lines
of its processes.

## Next steps

- [Python API](python-api.md): commands, transfers, snapshots, janitors, memory limits and errors,
  and what each promises.
- [Command line](cli.md): `doctor`, `run`, `pull`, `fetch-tools` and `janitor`, their options and
  their output.
- [Configuration](configuration.md): every provider option, where files go, and every environment
  variable.
- [Pods](pods.md): services that share one network.
- [Singularity CE and Apptainer](singularity.md) and [enroot](enroot.md): how each provider works,
  and what to check on a new cluster.
