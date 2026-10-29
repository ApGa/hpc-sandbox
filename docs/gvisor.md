# gVisor

`GvisorProvider` runs each sandbox in [gVisor](https://gvisor.dev), with rootless `runsc`. The
sandbox's processes run on gVisor's own kernel, the Sentry, which is a process of yours on the
host. Root in a sandbox holds every capability, but only towards the Sentry: it can mount file
systems, create namespaces, switch users, trace processes and set firewall rules, which workloads
ask Docker's privileged mode for, and none of it reaches the host's kernel. The provider is opt-in:
nothing picks it unless you ask for it (`GvisorProvider()`, or `--runtime gvisor` on the command
line). It also runs services of Singularity and enroot pods, whose other services stay on their
runtime ([Pod services](#pod-services)). [Architecture](architecture.md) describes what every
provider shares.

## What gVisor gives, and what it does not

These were measured with runsc release-20261026.0 on an el9 5.14 cluster with setuid SingularityCE
and unprivileged user namespaces, whose batch jobs get cgroup v1 without delegation and cannot open
`/dev/kvm`, on gVisor's `systrap` platform:

| Workload | In a gVisor sandbox |
|---|---|
| Mounts: `tmpfs`, `proc`, `sysfs`, bind mounts, and overlays whose upper layer is a tmpfs | works |
| Namespaces: user (nested), PID, mount, IPC and UTS; `chroot` and `pivot_root` | works |
| Network namespaces made in the sandbox | work with `network="none"`; with host networking a new one has no interfaces, and configuring it (`ip link set lo up`) hangs |
| Other users: `useradd`, `su`, `chown` to any uid, setuid binaries, apt dropping to `_apt` | works |
| `strace -f` and `strace -p`, `ptrace`, seccomp filters, `mknod` | works |
| A firewall: `iptables-legacy`, raw and packet sockets (with `net_raw`), veth pairs and bridges | works with `network="none"`; not with host networking |
| Docker 29 in the sandbox: `dockerd`, `docker run` with bridge networking, `exec`, `build`, `--privileged`, `--pids-limit` | works with `guest_cgroups` and a tmpfs mounted at `/var/lib/docker` (measured with `net_raw=True`, which Docker's own networking does not need); `docker run -m` is accepted but not enforced |
| go-judge 1.11 in the sandbox | runs as a pod service with `securebits="ignore"`, which makes the securebits that gVisor refuses look set ([Site notes](#site-notes)) |
| `iptables` with the nftables backend, loop devices, kernel modules | does not work |
| An overlay whose upper layer is on the root filesystem | does not work: the root filesystem is gVisor's overlay already; use a tmpfs |
| Memory and CPU limits of cgroups in the sandbox | accepted, not enforced; pids limits are enforced with `guest_cgroups="v2"` |
| KVM | not where the batch system's device cgroup hides `/dev/kvm`; `systrap` needs no device |

What it costs, on `systrap`, against the same root filesystem outside gVisor: a loop of 20
`g++ -O2` compiles took 1.6 times as long, a CPU-bound program 1.2 times. System calls cost the
most: `getppid` took 3.3 µs instead of 87 ns, `fork` and `wait` 2.2 ms instead of 0.17 ms, and 200
`exec`s of `/bin/true` six times as long. `ptrace` was slower than `systrap` on most workloads (the
compiles took 1.9 times as long, opening and closing a file 9 times), and faster on `fork` and
`wait` (0.86 ms against 2.2 ms). A sandbox starts in about 50 to 100 ms once its image's template
exists, and each command's `runsc exec` takes about 7 ms.

## Requirements

| Requirement | Needed for |
|---|---|
| `runsc` on `PATH`, or the `binary` option, with the `gvisor-bin/` directory of its release beside it | every sandbox |
| unprivileged user namespaces | every sandbox: rootless runsc makes one |
| Singularity CE or Apptainer: `singularity`, else `apptainer`, on `PATH`, or the `singularity_binary` option | pulling SIF images and extracting them ([Images](#images)) |
| `/bin/sh`, `sleep` and `cat` in the image | the keep-alive loop, and reading the image's environment |
| `mkdir` and `mount` in the image | binds whose targets the image lacks ([Root filesystem](#root-filesystem)) |

Run the process that starts the sandboxes on the host, or in a container of
`singularity exec --userns`: in a setuid Singularity container without `--userns`, runsc cannot
start its gofer. [Getting started](getting-started.md) lists what every provider needs.

### Installing runsc

hpc-sandbox never downloads runsc: it runs the one that `binary` names, or the first `runsc` on
`PATH`. gVisor's releases ship a tarball, `gvisor.tar.bz2`, that holds `runsc` and a `gvisor-bin/`
directory of helpers, which must stay beside it. Check the tarball against its published SHA-512,
and pin the release you tested; this installs it in `~/opt/gvisor`:

```bash
release=20261026.0
url="https://storage.googleapis.com/gvisor/releases/release/$release/$(uname -m)"
mkdir -p ~/opt/gvisor && cd ~/opt/gvisor
curl -fsSLO "$url/gvisor.tar.bz2" -O "$url/gvisor.tar.bz2.sha512"
sha512sum -c gvisor.tar.bz2.sha512 && tar -xjf gvisor.tar.bz2
export PATH="$HOME/opt/gvisor:$PATH"
runsc --version
```

## Images

The provider runs SIF images, with the references, the image cache and the staging of the
Singularity provider ([Singularity CE and Apptainer](singularity.md#images)): Singularity CE or
Apptainer pulls them and validates them, as `singularity_binary` names it, and with its temporary
and cache directories on the scratch root. A squashfs image raises `ValueError` before anything is
created.

## Root filesystem

Each image is extracted once per node with `singularity build --sandbox --fix-perms`, into the
node's gVisor template of it in `templates-gvisor/` on scratch, as the Singularity provider makes
its own templates in `templates/` ([Templates](singularity.md#templates)). The template is the
sandbox's read-only lower layer. gVisor's overlay (`--overlay2=root:dir=<session>/overlay`) keeps
what the sandbox writes in a file of the session, so:

- a start copies nothing, whatever the size of the image;
- root in the sandbox can give files to any user, since the overlay keeps their owners, which a
  directory of the host, where your uid owns everything, could not;
- `/tmp` and `/var/tmp` are part of the overlay, private to the sandbox, and every user in it can
  write there. gVisor mounts a tmpfs of its own over a `/tmp` that is empty, which would keep `/tmp`
  out of snapshots and count it as memory, so a template whose image has an empty `/tmp` gets an
  empty file there, `/tmp/.hpc-sandbox`;
- the overlay's file is on the scratch filesystem, unlinked as soon as gVisor creates it: `du` does
  not see what a sandbox wrote to its root filesystem, and it is freed when the sandbox stops. What
  a sandbox writes there does not count towards its memory. `overlay_size_mb` caps it, `/tmp`
  included (writes past the cap fail with `ENOSPC`); without it, a sandbox can fill the scratch
  filesystem. The cap covers the root filesystem only: the transfer directory below, on scratch,
  and read-write mounts are directories of the host outside it, which a sandbox can fill as far as
  their filesystems let it.

runsc never writes to a template. Before it makes the root filesystem it serves read-only, runsc's
gofer creates the mount point of each bind there when it is missing, where every later sandbox would
find it. So runsc is only given mount points that the template has:

- each template has the provider's own directory, `/.hpc-sandbox`, which replaces any that the
  image has, and which mount targets cannot name (`ValueError`);
- a bind whose target the image has, as a directory for a directory or as a regular file for a
  file, is bound there, resolved in the template as runsc resolves it, symlinks included;
- any other bind, such as one at a path the image lacks, is bound at one of the provider's mount
  points, `/.hpc-sandbox/mounts/d<N>` for a directory or `f<N>` for a file, which a start adds to
  the template when it has too few, and then at its target from inside the sandbox, once the
  sandbox runs: as root, with the image's `mkdir` and `mount`, which make the target in the
  sandbox's overlay, as Docker makes a missing mount point. A read-only bind stays read-only on
  the host, whatever the sandbox remounts.

The host reaches the sandbox's files through one directory of the session,
`tmp/.hpc-sandbox-transfer`, bound at `/.hpc-sandbox/transfer`, which transfers go through as with
the other providers ([Transfers](architecture.md#transfers)), even once root in the sandbox mounts
a file system over `/tmp`. While a sandbox runs, its process holds the template's `.users.lock`
shared, and janitors never delete a template whose lock is held, even when a start has moved it
aside because it lost its marker.

A gVisor session holds, besides the shared files:

| Path | What it holds |
|---|---|
| `config.json` | the OCI spec of the sandbox's container: the session is runsc's bundle |
| `rootfs.json` | the template, and the upper layer's archive of a restore |
| `runsc/` | runsc's state (`--root`), private to the sandbox |
| `overlay/` | the overlay's file |
| `exec/` | the sandbox-side PIDs of running commands |
| `runsc.log` | runsc's output |
| `tmp/.hpc-sandbox-transfer/` | the transfer directory, bound at `/.hpc-sandbox/transfer` |

## Inside a sandbox

Commands run as root, uid 0 of gVisor's kernel, with every capability that gVisor knows
(`CAP_NET_RAW` only with `net_raw=True`); on the host, every file the sandbox writes through a
mount, and every process gVisor runs, is your uid's.
The container's command is a keep-alive loop that is the sandbox's PID 1: with
`--signal-unkillable-policy=linux`, as on Linux, a SIGKILL that the sandbox sends its own PID 1 does
nothing, so commands cannot end the sandbox with `kill -9 -1`, and software that kills itself as a
PID namespace's init, as go-judge's does, keeps running. Setuid binaries work (`--allow-suid`). The
hostname is the host's.

gVisor reports its own kernel (`uname -r` ends in `gvisor`), a `/proc` and `/sys` of its own, and
the host's memory as the sandbox's (`/proc/meminfo`).

## Commands

Each command is one `runsc exec` that runs the prologue ([Commands](architecture.md#commands)),
with `--cwd /`. runsc runs with a minimal environment: a fixed `PATH`, and none of your variables.
The image's environment comes from its Singularity environment scripts
(`/.singularity.d/env/*.sh`), which a start sources inside the sandbox, once, and reads back with
`cat /proc/self/environ`; every command gets it, then `HOME`, the spec's `env` and the call's
`env`.

Killing a `runsc exec` leaves its command running in the sandbox, since the command is a process of
gVisor's kernel, not a child of `runsc exec` on the host. So, on a timeout or a cancellation,
hpc-sandbox lists the sandbox's processes (`runsc ps`) and kills the process group of the command
and the process group of every process below it (`runsc kill --pgid`, several at once), never PID
1's. A process that has left the command's tree, because its parent exited before it, escapes, as
it does on the other providers. A command whose Sentry died raises `SandboxLostError`, even when
`runsc exec` reports success.

## Networking

| `network` | runsc | What the sandbox gets |
|---|---|---|
| `"host"` (the default) | `--network=host` | the host's network through gVisor's host stack: it reaches what the node reaches, the node's loopback included, with the host's `/etc/resolv.conf` read-only unless `bind_resolv_conf=False`; no firewall, raw sockets, bridges or new network namespaces (configuring one hangs) |
| `"none"` | `--network=none` | gVisor's own network stack with a loopback interface only, in which `iptables-legacy`, veth pairs, bridges and, with `net_raw=True`, raw and packet sockets work |

`network="none"` always works: the provider has the `network_none` capability. To reach a service
on the host from a sandbox without a network, such as a model gateway, mount its Unix socket into
the sandbox and set `host_uds="open"`; with `"create"`, the host can connect to Unix sockets that
the sandbox creates in a mounted directory, and `"all"` allows both. `runsc port-forward` does not
work rootless. Egress without host networking, through slirp4netns as pods have it, is on the
[roadmap](roadmap.md).

## Limits

- **`memory_mb`.** gVisor enforces no memory limit: neither the OCI spec's nor those of cgroups in
  the sandbox. The sandbox's memory is the Sentry's, on the host: gVisor keeps it in a memory file,
  a memfd that the Sentry (`runsc-sandbox`) holds open, and maps into a host process only while the
  sandbox's process runs, so no process's PSS shows an idle sandbox's memory. Every `memory_poll_s`,
  the watchdog adds up the allocated size of the memory files the Sentry holds, each once, and the
  Sentry's own anonymous memory, and when that exceeds the limit, it stops the whole sandbox: the
  sandbox's processes are not host processes, so it cannot pick one. The running commands, and
  every later call, raise `SandboxLostError` with the watchdog's report ("gVisor's memory"), which
  `sandbox.memory_kills` records too. gVisor's own memory counts: about 15 MiB for an idle
  sandbox, and more after file-heavy work (about 50 MiB after `apt-get install g++`), so leave room
  for it. Files on a tmpfs in the sandbox count. The host's files that the sandbox reads or maps,
  from its image or its mounts, do not: a sandbox that read and mapped a 1.5 GiB file of a mount
  held about 20 MiB. Neither does what it writes to its root filesystem. Inside the sandbox,
  `ulimit -v` (`RLIMIT_AS`) limits a process, and `RLIMIT_DATA` does not limit `mmap`.
- **`cpus`.** The launcher, and so the Sentry, runs on as many of the CPUs this process may use as
  `cpus` rounded up, and gVisor reports them as the sandbox's CPUs (`nproc`). A start picks the CPUs
  that your gVisor sandboxes on the node use least, whichever process started them (it reads their
  processes' CPU affinity), from a random one among equals; starts on one scratch root take turns
  with a lock, `cpus.lock`. There is no quota of CPU time: an OCI CPU limit is an error under
  rootless runsc, and cgroups in the sandbox do not enforce theirs. Without `cpus`, a sandbox may
  use every CPU this process may use.
- **Disk.** `overlay_size_mb` caps what a sandbox writes to its root filesystem
  ([Root filesystem](#root-filesystem)), and nothing else: what it writes to the transfer
  directory, `/.hpc-sandbox/transfer`, or to a read-write mount goes to that directory of the host,
  which only its filesystem caps.
- **Processes.** `pids` limits of cgroups in the sandbox are enforced with `guest_cgroups="v2"`; in
  `"v1"` mode they were not (180 of 200 forks went through a limit of 10).

## Snapshots and restores

A snapshot holds the overlay's upper layer, as `runsc tar rootfs-upper` archives it
(`rootfs-upper.tar`), and records the template it lies over (`lower`, with the format
`hpc-sandbox/gvisor-overlay-v1`). `/tmp` and `/var/tmp` are in that archive, and the owners and
modes of the files too: unlike the other providers' snapshots, a restore gives the files that the
sandbox's users wrote their owners back. A restore starts a sandbox over the same template with a
copy of the archive, which gVisor lays over it (the `dev.gvisor.tar.rootfs.upper` annotation, with
`--allow-rootfs-tar-annotation`). It works only on the node whose template the snapshot lies over,
while that template exists: anything else raises `ValueError`.

## Teardown

The launcher (`anchor.py`'s `exec` form, run with the same Python interpreter) arranges to be
killed with SIGKILL when the process that started it dies, then runs `runsc run` in the
foreground, in its place. The kernel sends that signal when the thread that forked the launcher
ends, so every launcher is forked from one thread of hpc-sandbox's that lives as long as the
process, not from the caller's thread, which may end first. runsc re-executes itself in a user
namespace, and starts the gofer and the Sentry, each of which runsc arranges to die with its
parent. So:

- `stop()` kills the launcher with SIGKILL and waits for it, which takes the sandbox down. It waits
  for the recorded Sentry too, which the kernel may take a while to tear down after it held much
  memory (up to `stop_timeout_s`), then kills any process of yours whose arguments still name the
  session's `runsc/` directory as runsc's `--root`, which no other sandbox uses. runsc keeps its
  state in the session, so nothing is left for `runsc delete`, and the session goes to the trash
  as on the other providers.
- A sandbox dies with the process that started it, even when that process is killed with SIGKILL,
  and lives as long as that process, whichever of its threads started it.
  A janitor finds only files: it kills any survivor whose recorded identity (PID and start time)
  still matches, or whose arguments name the session's state directory, and deletes the session.
  `hpc-sandbox janitor` sweeps gVisor's sessions without runsc.

## Pod services

The gVisor provider starts no pods of its own: `start_pod()` raises `ValueError`. It runs services
of the pods of a Singularity or enroot provider instead, as that provider's `gvisor` option:
`SingularityProvider(gvisor=GvisorProvider(...))` runs the services whose `ServiceSpec` says
`runtime="gvisor"` in gVisor, and the pod's other services on Singularity, all in the pod's
network ([gVisor services](pods.md#gvisor-services)); any service may, so a pod can run every
service in gVisor ([Every service in gVisor](pods.md#every-service-in-gvisor)). Such a service runs
as a sandbox does, with these differences:

- Its launcher (`anchor.py`'s `gvisor-service` form) joins the pod's user and network namespaces
  and nests a user namespace that maps root alone, in which it runs `runsc run`, always with
  `--network=host`: gVisor's host network stack is the pod's network, so the service's sockets are
  on the pod's loopback, and gVisor's kernel holds no capability over that network.
- Its command, under the pods' start protocol, is the container's PID 1, in place of the
  keep-alive loop, and its hostname is the service's name. Its `/etc/hosts` and `/etc/resolv.conf`
  are the pod's, its `/dev/shm` a tmpfs of `shm_mb`, and its `ulimits` are the resource limits of
  the container's spec.
- `securebits="ignore"` and `guest_cgroups` are settings of the service
  ([Securebits](pods.md#securebits)).

`net_raw` changes nothing for a service: the pod's network gives it no raw or packet sockets.

## Options

```python
# gvisor_options.py: runsc from ~/opt/gvisor, Docker's needs, and the limits
import asyncio
from pathlib import Path

from hpc_sandbox import GvisorProvider, SandboxSpec


async def main() -> None:
    runsc = str(Path.home() / "opt" / "gvisor" / "runsc")
    provider = GvisorProvider(binary=runsc, net_raw=True, guest_cgroups="v2", overlay_size_mb=8192)
    spec = SandboxSpec(image="docker://alpine:3.20", network="none", memory_mb=2048, cpus=2)
    async with await provider.start(spec) as sandbox:
        result = await sandbox.exec("mount -t tmpfs none /mnt && nproc")
        print(result.stdout, end="")
    await provider.wait_for_deletions()


asyncio.run(main())
```

[Configuration](configuration.md#gvisorprovider-options) lists every option.

## Site notes

Run `hpc-sandbox doctor --runtime gvisor --image IMAGE` on a compute node of a new site first. For
gVisor, it also checks runsc's helpers, the Singularity CE or Apptainer that images need,
unprivileged user namespaces, and that rootless runsc runs a command (`runsc do true`) on the
default platform.

- **Platforms.** `systrap`, the default, needs nothing from the node. `ptrace` works too, and is
  slower. `kvm` needs a `/dev/kvm` that the job may open: batch systems that confine a job's devices
  with a cgroup often hide it, whatever its mode.
- **Rootless runsc.** gVisor documents `--rootless` mainly for `runsc do`; hpc-sandbox runs
  `run`, `exec`, `ps`, `kill` and `tar` with it, which the measurements above and its integration
  tests check, release by release. It refuses `create` and checkpoints, and network namespaces
  other than `none` and `host`.
- **Nesting.** runsc runs in the host's user namespace, in one that `unshare --user` made, and in a
  container of `singularity exec --userns`, but not in a setuid Singularity container without
  `--userns`.
- **Docker in a sandbox** needs `guest_cgroups` (`"v1"` or `"v2"`),
  `network="none"` (host networking has no bridges), and a tmpfs at `/var/lib/docker`, which root in
  the sandbox can mount itself: `mount -t tmpfs none /var/lib/docker`. The measurements started
  `dockerd` with `--iptables=false --ip6tables=false`, as gVisor's guide to Docker does.
- **go-judge** needs two changes to run in gVisor. go-judge's container init sends SIGKILL to
  itself as the PID 1 of a new PID namespace, which Linux ignores, and gVisor too with
  `--signal-unkillable-policy=linux`, which hpc-sandbox always sets. Before every program, go-judge
  also sets locked and `NOROOT` securebits, for which gVisor returns `EPERM`, so every run fails
  with `keep_capability: operation not permitted`. A pod service with `securebits="ignore"` makes
  that call succeed without setting anything, which [Securebits](pods.md#securebits) weighs; as
  root, go-judge also needs `guest_cgroups`. Measured in a pod, go-judge 1.11.1 then ran programs,
  and enforced their CPU time limits, with `guest_cgroups="v2"`.
- **Memory.** A sandbox's memory limit stops it whole, and counts the Sentry's own memory
  ([Limits](#limits)). Keep `memory_mb` well above what the workload needs, and check
  `sandbox.memory_kills` when a sandbox is lost.

## Next steps

- [Architecture](architecture.md): sessions, commands, transfers and janitors.
- [Singularity CE and Apptainer](singularity.md): the images this provider shares, and templates.
- [Configuration](configuration.md): every provider option, and where files go.
- [Security model](security.md#gvisor-sandboxes): what gVisor adds to isolation, and what it does
  not.
