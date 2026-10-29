<img src="assets/hpc_sandbox_icon.png" alt="hpc-sandbox" width="320">

Rootless sandboxes for HPC clusters, on Singularity/Apptainer, enroot or gVisor.

hpc-sandbox runs disposable, isolated environments from container images on compute nodes that have
no Docker and give you no root, with the container runtime the site already provides: Singularity CE
or Apptainer instances, or enroot containers, or, opt-in, gVisor sandboxes. A sandbox has its own
writable root filesystem on node-local disk; you run commands in it, copy files in and out,
snapshot it and restore copies of it. It is built for batch jobs that run many short tasks at once,
such as agent rollouts, code evaluation and benchmark harnesses, and it is a Python library with a
command-line interface.

What it gives you:

- **One API for three runtimes.** `SingularityProvider`, `EnrootProvider` and `GvisorProvider` start
  sandboxes from the same `SandboxSpec`, and every sandbox has the same `exec`, `upload`,
  `download`, `snapshot` and `stop`.
- **Root without root, with gVisor.** Root in a gVisor sandbox can mount file systems, create
  namespaces, switch users and run Docker, as workloads ask Docker's privileged mode for, on
  gVisor's kernel instead of the host's.
- **No daemon and no root.** Sandboxes run as your user, or as root mapped to it, with no privileges
  beyond what the site's runtime grants. The library needs nothing outside Python's standard
  library.
- **Commands you can rely on.** Every command has a timeout, output is bounded, exit codes keep
  their meaning, and failures of the infrastructure raise exceptions instead of passing for failed
  commands.
- **Memory limits without cgroups.** A sampling watchdog kills the branch of processes that holds
  the memory, so a command that runs away dies and the harness that started it survives.
- **Pods.** Services that share one network, like a docker-compose project: a sandbox reaches the
  databases, tool servers or graders it works with by name, and the host reaches the ports you
  expose through Unix sockets. A service that asks for Docker's privileged mode, such as a judge
  that sandboxes what it grades, can run in gVisor while the others stay on Singularity or enroot.
- **Clean nodes.** Teardown proves each process's identity before signalling it, and janitors
  recover what a crashed process or job left behind, from running containers to files and mounts.
- **Built for busy nodes.** Images are cached and staged once per node, sessions are deleted in the
  background, and an optional overlay layout makes a Singularity sandbox's start and stop cost the
  same whatever the size of its image.

Sandboxes keep their commands apart from other sandboxes and from the host's files and environment,
but they are not a security boundary: they run as your user, on the host's network unless you
choose otherwise, and, except in gVisor, on the host's kernel. [Security model](docs/security.md)
explains what the isolation covers.

## Requirements

hpc-sandbox runs on Linux with Python 3.11 or later, on compute nodes that have one of these
runtimes:

| Runtime | What it needs |
|---|---|
| Singularity CE or Apptainer | the runtime, in setuid or user-namespace mode |
| enroot 3.x or 4.x | unprivileged user namespaces, util-linux `nsenter`, and enroot's `+caps` package to import images |
| gVisor (`runsc`), opt-in | unprivileged user namespaces, and Singularity CE or Apptainer, or enroot, for its images |

Pods need unprivileged user namespaces and util-linux `nsenter`, with Singularity or enroot.
[Getting started](docs/getting-started.md#requirements) lists every tool and what needs it.

## Install

```bash
pip install hpc-sandbox
```

This installs the `hpc_sandbox` package and the `hpc-sandbox` command (`python -m hpc_sandbox` runs
the same). Below 1.0, a minor release may change the API, so pin one (`hpc-sandbox>=0.6,<0.7`);
[CHANGELOG.md](CHANGELOG.md) has the release notes.

## Quickstart

Run these on a compute node, inside a batch job: hpc-sandbox starts containers, which login nodes
are not for. Check what the node supports, then run a command in a fresh sandbox:

```bash
hpc-sandbox doctor --image docker://python:3.12-slim    # the runtime, scratch and a test sandbox
hpc-sandbox run docker://python:3.12-slim -- python -c 'print(6 * 7)'
```

From Python:

```python
import asyncio
from pathlib import Path

from hpc_sandbox import SandboxSpec, SingularityProvider


async def main() -> None:
    provider = SingularityProvider()  # or EnrootProvider(): the same API on enroot
    spec = SandboxSpec(image="docker://python:3.12-slim", memory_mb=4096)
    Path("hello.py").write_text("print('hello from a sandbox')\n")
    async with await provider.start(spec) as sandbox:  # stopped when the block exits
        await sandbox.upload("hello.py", "/app/hello.py")
        result = await sandbox.exec("python /app/hello.py", timeout_s=60)
        print(result.exit_code, result.stdout)
    await provider.wait_for_deletions()  # its files are deleted in the background


asyncio.run(main())
```

[The Python API](docs/python-api.md) covers commands, transfers, snapshots, labels and janitors,
memory limits and errors; [pods](docs/pods.md) start services next to a sandbox.

## Choosing a runtime

| | `SingularityProvider` | `EnrootProvider` | `GvisorProvider` |
|---|---|---|---|
| Runtime | Singularity CE or Apptainer, in setuid or user-namespace mode | enroot 3.x or 4.x, unprivileged | rootless `runsc` |
| Images | SIF (`singularity pull`) | squashfs (`enroot import`) | SIF, as the node's templates |
| Inside the sandbox | your user | root mapped to your user (or your user) | root of gVisor's kernel, with every capability, which can switch users |
| Per command | one `singularity exec`, which starts the runtime | `nsenter` into the running container | one `runsc exec` |
| `network="none"` | with `allow_network_none=True`, where the site lets users create network namespaces | always, unless `allow_network_none=False` | always |
| `memory_mb` | kills the branch of processes that holds the memory | the same | stops the whole sandbox |
| Pods | yes | yes | as services of Singularity and enroot pods (`gvisor=`, [gVisor services](docs/pods.md#gvisor-services)) |
| When the owning process dies | instances run until a janitor stops them | everything dies with it; a janitor removes the files | the same as enroot |

[Singularity CE and Apptainer](docs/singularity.md), [enroot](docs/enroot.md) and
[gVisor](docs/gvisor.md) describe each provider and what to check on a new cluster.

## Commands

| Command | What it does |
|---|---|
| `hpc-sandbox doctor [--image IMAGE]` | reports what the node supports for the runtime; with an image, starts a sandbox and exercises it |
| `hpc-sandbox run [OPTIONS] IMAGE -- CMD [ARG...]` | runs one command in a fresh sandbox and exits with its status |
| `hpc-sandbox pull [--refresh] IMAGE [IMAGE...]` | pulls images into the cache and prints one JSON line per image |
| `hpc-sandbox janitor [--label KEY=VALUE]...` | stops and removes what dead processes left on this node and prints a JSON report |
| `hpc-sandbox fetch-tools [--arch ARCH]` | downloads the pinned slirp4netns that pods use for egress where the host has none |

Run `hpc-sandbox janitor` at the end of batch scripts that use hpc-sandbox.
[docs/cli.md](docs/cli.md) has every option, the JSON outputs and the exit codes.

## Documentation

- [docs/README.md](docs/README.md): the table of contents.
- [Getting started](docs/getting-started.md): requirements, install, the first sandbox, Slurm and
  PBS batch scripts, and what to do when something fails.
- [Python API](docs/python-api.md), [command line](docs/cli.md) and
  [configuration](docs/configuration.md): the reference.
- [Pods](docs/pods.md): services that share one network.
- [Singularity CE and Apptainer](docs/singularity.md), [enroot](docs/enroot.md) and
  [gVisor](docs/gvisor.md): each provider, its layouts and site notes.
- [Architecture](docs/architecture.md): how a sandbox is built, watched and torn down.
- [Security](docs/security.md): what the isolation covers, and what it does not.

## Contributing and development

[CONTRIBUTING.md](CONTRIBUTING.md) is the full guide: the development setup, the tests, the checks
CI runs, commits and pull requests, and releases. In short:

```bash
git clone https://github.com/ApGa/hpc-sandbox.git
cd hpc-sandbox
uv sync --group dev
uv run pre-commit install    # lint each commit and check its message
uv run pytest                # the unit tests, with fake runtimes
```

The unit tests run wherever unprivileged user namespaces work; the integration tests need a compute
node with a runtime. Commit messages and pull request titles follow Conventional Commits, such as
`fix: Keep a session whose overlay cannot be unmounted`.

Report bugs and ask questions in [GitHub issues](https://github.com/ApGa/hpc-sandbox/issues), with
the output of `hpc-sandbox doctor --image IMAGE` from the node where the problem happens. Report
security issues privately, as [SECURITY.md](SECURITY.md) describes.

## License

hpc-sandbox is released under the MIT License; see [LICENSE](LICENSE).

## Citation

If hpc-sandbox is useful in your work, please cite it:

```bibtex
@misc{gandhi2026hpcsandbox,
  author       = {Gandhi, Apurva},
  title        = {{hpc-sandbox}: Rootless Sandboxes for {HPC} Clusters},
  howpublished = {\url{https://github.com/ApGa/hpc-sandbox}},
  year         = {2026}
}
```
