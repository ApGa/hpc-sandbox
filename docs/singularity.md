# Singularity CE and Apptainer

`SingularityProvider` runs each sandbox as one persistent Singularity CE or Apptainer instance,
started from a writable root filesystem directory on node-local scratch. It works with the runtime
installed setuid or in unprivileged user-namespace mode, and needs no `--fakeroot`,
`--writable-tmpfs`, overlay image or loop device: commands run as your uid. Both runtimes share the
command line used here, so this page writes `singularity` for either.
[Architecture](architecture.md) describes what both providers share.

## Requirements

| Requirement | Needed for |
|---|---|
| Singularity CE or Apptainer: `singularity`, else `apptainer`, on `PATH`, or the `binary` option | every sandbox |
| `/bin/sh` and `sleep` in the image | the start script that keeps each instance running |
| `fuse-overlayfs`, `fusermount3` or `fusermount`, `/dev/fuse`, and unprivileged user namespaces | the fuse-overlay layout only; janitors need only `fusermount3` or `fusermount` |
| unprivileged user namespaces and util-linux `nsenter` | pods only ([Pods](pods.md)) |

[Getting started](getting-started.md) lists what every provider needs.

## Images

The provider runs SIF images. A reference that starts with `/`, `./`, `../`, `~` or `file://`, or
that ends in `.sif`, names a local file; any other is pulled:

| Reference | Example | How it is used |
|---|---|---|
| a local SIF file | `/images/task.sif`, `file:///images/task.sif` | in place, once `singularity sif list` validates it |
| a local SIF file pinned by content | `/images/task.sif@sha256:<hex>` | in place, and refused unless the file's SHA-256 matches |
| a `docker://` reference | `docker://python:3.12-slim` | pulled with `singularity pull` into the image cache |
| a digest-pinned `docker://` reference | `docker://python@sha256:<hex>` | pulled once, and cached under the digest |
| an `oras://`, `library://`, `shub://`, `http://` or `https://` reference | `oras://ghcr.io/org/task:1` | pulled with `singularity pull` |
| a bare reference | `python:3.12-slim` | read as `docker://python:3.12-slim` |

`docker://` and bare references may also use enroot's `registry#path` spelling. A pulled image is
validated with `sif list` and cached as `sha256-<hex>.sif`, named by the pointer
`uri-<hash>.sif.json`, or `oci-<digest>.sif.json` for a digest-pinned reference ([Images and the
image cache](architecture.md#images-and-the-image-cache)). Pin images by digest for reproducible
runs. A squashfs image (a name ending in `.sqsh` or `.squashfs`, or a local file that starts like
one) and a scheme that the runtime cannot pull raise `ValueError` before anything is created.

## Root filesystem

Each sandbox's root filesystem is a directory owned by your uid, in its session on node-local
scratch. Its size is bounded only by the scratch filesystem, unlike `--writable-tmpfs`, which many
sites cap at a few tens of MiB. The `rootfs_layout` option chooses between two layouts.

### The copy layout

The copy layout is the default. The image is extracted with
`singularity build --sandbox --fix-perms` into the session, and the provider gives your uid read and
write access to everything in it.

With `rootfs_templates=True`, each image is instead extracted once per node, into a template in
`templates/` on scratch, and each sandbox gets a copy of that template (`cp -a --reflink=auto`: a
reflink clone on XFS or Btrfs, a plain copy elsewhere). This is off by default, because a copy
creates as many inodes as an extraction: on XFS, many copies at once took longer than as many
extractions.

### Templates

A template is extracted under a lock per image, into a directory that is renamed into place once the
extraction is complete, with a `.complete` marker in it. Later sandboxes trust a template that is a
private directory of your uid holding its marker. A template that lost its marker, to an age-based
cleaner or a removal cut short, is moved aside and extracted again. No sandbox writes to a template:
a copy is the sandbox's own, and an overlay writes to its upper layer.

A template is named after the SHA-256 of its image or, for a local image whose digest is unknown,
after the file's device, inode, size and times, so an image rewritten in place gets a new template.
Janitors delete the extractions of processes that died, and templates moved aside as damaged, once
no overlay uses them. Templates are never evicted ([Site notes](#site-notes)).

### The fuse-overlay layout

With `rootfs_layout="fuse-overlay"`, or `HPC_SANDBOX_ROOTFS_LAYOUT=fuse-overlay` for a whole job,
the root filesystem is a `fuse-overlayfs` mount made by your uid: the node's template of the image
is the read-only lower layer, and the session's `overlay/upper` holds what the sandbox writes, with
Docker's semantics (whiteouts, opaque and renamed directories). The instance runs it unprivileged
(`--userns --writable`), so:

- a start and a stop cost the same whatever the image's size;
- deleting a session deletes only what its sandbox wrote;
- a snapshot copies only the upper layer, and a restore copies only that back.

File operations in the sandbox go through FUSE: metadata-heavy ones (`find`, many small files) are
slower than on a copy; Python imports are not. Before it mounts an overlay, the provider writes the
mount's record (`fuse-overlay.json` in the session), so that a stop or a janitor unmounts whatever
may be mounted.

```python
# fuse_overlay.py: sandboxes whose root filesystem is an overlay over the node's template
import asyncio

from hpc_sandbox import SandboxSpec, SingularityProvider


async def main():
    provider = SingularityProvider(rootfs_layout="fuse-overlay")
    async with await provider.start(SandboxSpec(image="docker://python:3.12-slim")) as sandbox:
        result = await sandbox.exec(["python", "-c", "print(6 * 7)"])
        print(result.stdout, end="")
    await provider.wait_for_deletions()


asyncio.run(main())
```

GPUs (`gpu`), `cgroup_limits` and `--fakeroot` (in `extra_start_args`) need the copy layout, and the
fuse-overlay layout needs templates. Giving `rootfs_layout="fuse-overlay"` together with one of
them, or with `rootfs_templates=False`, raises `ValueError`; `HPC_SANDBOX_ROOTFS_LAYOUT` leaves such
a provider on the copy layout. Pod services always get the copy layout.

## Instances

Before it starts the instance, the provider prepares the root filesystem:

- It writes a keep-alive start script into `/.singularity.d/startscript`: images built from OCI
  images ship an empty one, and an instance whose start script exits is removed at once. A root
  filesystem without a `/.singularity.d` directory fails the start.
- It creates the bind targets inside the root filesystem: `/tmp`, `/var/tmp`, the spec's mount
  targets and `/etc/resolv.conf`. It refuses a target whose path crosses a symlink, which could lead
  out of the root filesystem to anywhere on the host. When the image links `/etc/resolv.conf`
  elsewhere, the sandbox keeps the image's own.
- It creates `/root`, the default `HOME`, when the image has none.

Then it runs `singularity instance start` under the session's instance-control lock, with:

| Flag | Why |
|---|---|
| `--cleanenv` | the host's environment does not reach the container |
| `--containall` | private PID and IPC namespaces, a minimal `/dev`, and empty directories instead of the host's `/tmp` and home |
| `--no-home`, `--no-mount home,cwd,bind-paths` | no home directory, no working directory, and none of the site's `bind path` entries |
| `--no-eval` | the runtime does not evaluate environment values as shell |
| `--writable` | the root filesystem directory is writable |
| `--userns` | the fuse-overlay layout only: the instance runs unprivileged |
| `--pid-file <session>/instance.pid` | the instance's PID, checked against the runtime's own record |
| `--bind <session>/tmp:/tmp:rw`, `--bind <session>/var-tmp:/var/tmp:rw` | private `/tmp` and `/var/tmp` |
| `--bind /etc/resolv.conf:/etc/resolv.conf:ro` | the host's resolver configuration, with host networking and `bind_resolv_conf` |
| `--bind SRC:DST:ro` or `--bind SRC:DST:rw` | the spec's `mounts` |
| `--net --network none` | `network="none"`, which needs `allow_network_none=True` |
| `--nv` or `--rocm` | the `gpu` option |
| `--cpus`, `--memory` | the spec's `cpus` and `memory_mb`, with `cgroup_limits=True` |

`extra_start_args` come next, then the root filesystem and the instance's name, which is the
sandbox's `id`. The provider never adds `--fakeroot`. With `gpu="nv"`, it sets
`NVIDIA_VISIBLE_DEVICES=all` for the start unless your environment sets it ([Site
notes](#site-notes)).

Startup then checks that the PID file appears and is safe (a regular file of yours that others
cannot write), that the runtime lists exactly this instance (`instance list --json`) with the PID
that the PID file holds, and reads that process's start time: this identity goes into the owner
record. An instance that exits right after it starts means that the image lacks `/bin/sh` or
`sleep`. Starting and stopping hold only that instance's own control lock, so instances start and
stop in parallel.

### The runtime's environment

The runtime runs with your environment, minus what would add binds or leak host settings past
`--cleanenv`:

| Variables | What the provider does | Why |
|---|---|---|
| `SINGULARITY_BIND`, `SINGULARITY_BINDPATH`, `APPTAINER_BIND`, `APPTAINER_BINDPATH` | removes them | binds from the environment would defeat `--no-mount bind-paths`, for example a launcher's bind of datasets or home directories |
| `SINGULARITYENV_*`, `APPTAINERENV_*` | removes them | they would set variables in the container despite `--cleanenv` |
| `SINGULARITY_CONFIGDIR`, `APPTAINER_CONFIGDIR` | removes them | unprivileged instance records live below your home directory regardless, and with a configuration directory set, `instance list` looks in an empty one |
| `TERM` and the proxy variables (`http_proxy`, `HTTPS_PROXY` and the like) | removes them for every runtime call that enters a container | the runtime passes them into containers despite `--cleanenv`, and proxy URLs may hold credentials |
| `SINGULARITY_TMPDIR`, `APPTAINER_TMPDIR`, `SINGULARITY_CACHEDIR`, `APPTAINER_CACHEDIR` | points them at `runtime-tmp/` and `runtime-cache/` on scratch, unless your environment sets them | pulls and builds never fill a small `/tmp` or a quota-limited home |

Pulls keep the proxy variables. A sandbox that needs `TERM` or a proxy gets it through
`SandboxSpec.env`.

## Commands

Each command is one `singularity exec --cleanenv --cwd / instance://<id>` that runs the prologue
([Commands](architecture.md#commands)). The prologue enters the working directory itself, because
Singularity CE silently falls back to another directory when the one `--cwd` names is missing. The
runtime's own `--env` is not used, since it splits values on commas, and neither is `--env-file`,
which the runtime evaluates as shell. The runtime sets up the image's environment; the prologue adds
`HOME`, the spec's `env` and the call's `env`. Every command starts the runtime once, which costs
more than entering a namespace does on [enroot](enroot.md#commands).

## Snapshots and restores

A snapshot of a copy-layout sandbox holds its whole root filesystem. A snapshot of a fuse-overlay
sandbox holds only its upper layer and records the template it lies over (`lower` in the metadata,
with the format `hpc-sandbox/fuse-overlay-v1`). A restore copies that upper layer into the new
session and mounts it over the same template, so neither copies the image. Such a snapshot restores
only with a provider whose layout is fuse-overlay, and only on the node whose template it lies over:
anything else raises `ValueError`.

## Teardown

`stop()` stops exactly the sandbox's instance, under its control lock, and acts only when the
runtime's record, the PID file, the recorded PID and the process's start time all agree; otherwise
it fails closed and keeps the session. It runs `singularity instance stop --timeout <stop_grace_s>`
on the instance's own name, checks that the runtime no longer lists it, and, if the instance process
survived, signals it directly (SIGTERM, then SIGKILL) after re-proving its identity, its process
title included. Nothing uses wildcards or `instance stop --all`. Once the instance is gone, the stop
unmounts the session's overlay (`fusermount3 -u`, then a lazy unmount); a session whose overlay
stays mounted is kept, never deleted through the mount.

Instances do not die with the process that started them: an instance whose owner died keeps running
until a janitor stops it, which is the next `hpc-sandbox janitor` or `provider.reap()` on the node,
or the first start of a later Singularity provider there. A janitor stops an abandoned instance only
with an owner record bound to its session, and refuses to touch an instance that runs without one.
Janitors and first starts also:

- unmount the overlays whose `fuse-overlayfs` died, wherever their sessions lie on the node. A
  `fuse-overlayfs` killed with SIGKILL, as Slurm kills what outlives a step or a job, leaves its
  mount behind, dead. An overlay is dead when it is one of your session overlays, no lease holds its
  session, no `fuse-overlayfs` of yours serves it, and `statfs` on it fails with `ENOTCONN`; the
  probe runs in a child process with a time limit, since a probe of a stopped daemon's mount would
  wait for it;
- delete the template extractions of processes that died, and templates moved aside as damaged, once
  no overlay uses them.

## Pod services

A pod's service runs as one long-lived `singularity exec` rather than an instance, with
`--cleanenv`, `--no-eval`, `--writable`, `--containall`, `--no-home` and
`--no-mount home,cwd,bind-paths`, on a copy-layout root filesystem, with the service's own `/tmp`,
`/var/tmp`, `/dev/shm`, `/etc/hosts` and `/etc/resolv.conf` bound in. The launcher runs it in a
nested user namespace of its own, so the runtime runs unprivileged (its non-setuid starter) even
where it is installed setuid, and root in the container holds no capability over the pod's network.
Pods never use `--network none`. [Pods](pods.md) describes them.

## Site notes

Run `hpc-sandbox doctor --image IMAGE` on a compute node of a new site first. For Singularity CE and
Apptainer, it also prints the site settings that matter here: `allow setuid`, `allow user ns`,
`sessiondir max size`, `max loop devices`, `enable overlay`, `enable underlay` and `mount home`.

- **Setuid or user-namespace mode.** Both work. In setuid mode, network namespaces
  (`--network none`) are usually reserved for root, so leave `allow_network_none` off unless a test
  shows that it works: `hpc-sandbox run --network none --allow-network-none IMAGE -- true` on a
  compute node, or the integration tests with `HPC_SANDBOX_TEST_NETWORK_NONE=1`
  ([Contributing](../CONTRIBUTING.md)). In unprivileged user-namespace mode, common with Apptainer,
  it normally works.
- **No `--fakeroot`, no `--writable-tmpfs`.** Some sites force squashfuse for fakeroot overlays, and
  the runtime then silently falls back to a read-only root filesystem; `sessiondir max size` often
  caps a writable tmpfs at a few tens of MiB. Writable root filesystem directories on scratch avoid
  both, and the start probes that the root filesystem is writable, so a regression fails loudly.
- **Instance metadata is per user, not per job.** Unprivileged instance records live below your home
  directory as the password database names it, and ignore `SINGULARITY_CONFIGDIR`, so every job of a
  user on a node sees the same instance list. Hence exact instance names, PID files and start-time
  identities, and never `instance stop --all`, which would stop other jobs' instances.
- **Cgroups.** Most sites give users none, hence the [memory
  watchdog](architecture.md#the-memory-watchdog). Where Apptainer has rootless cgroups v2 (systemd
  user delegation), `cgroup_limits=True` turns `cpus` and `memory_mb` into real limits; otherwise
  `cpus` is advisory. The setuid starter is not dumpable, so the watchdog charges its resident set
  size instead of its PSS.
- **GPUs.** `gpu="nv"` passes NVIDIA GPUs (`--nv`), and `gpu="rocm"` AMD GPUs (`--rocm`). Where
  `singularity.conf` sets `use nvidia-container-cli = yes`, `--nv` with `--containall` exposes only
  the GPUs that `NVIDIA_VISIBLE_DEVICES` names, so the provider sets it to `all`, every GPU the
  process may use (inside a Slurm allocation, the allocated ones), unless it is already set.
- **The fuse-overlay layout** needs `fuse-overlayfs` and `fusermount3` (or `fusermount`) on `PATH`,
  `/dev/fuse`, and unprivileged user namespaces, which its instances run in (`--userns`); janitors
  need only `fusermount3` (or `fusermount`). Each overlay counts towards fusermount's `mount_max`
  (`/etc/fuse.conf`, 1000 by default), a limit on the FUSE mounts of all the node's users together.
  An overlay whose `fuse-overlayfs` gets SIGKILL stays mounted, dead, until a janitor or a
  provider's first start on the node unmounts it.
- **Templates are never evicted.** Every image version used on a node keeps an extracted root
  filesystem in `templates/` on scratch, as staged images do in `images/`. A per-job scratch root
  goes with the job. On a scratch root that outlives jobs, keep age-based cleaners such as
  `systemd-tmpfiles` out of `templates/`: a template that lost its marker is extracted again, but
  one that lost other files is not noticed.
- **Variables passed despite `--cleanenv`.** The runtime passes `TERM` and the proxy variables into
  containers regardless, and proxy URLs may hold credentials, so the provider removes them from its
  environment for `instance start` and `exec`. Pass them in `SandboxSpec.env` if a sandbox needs
  them.

## Next steps

- [Architecture](architecture.md): sessions, commands, transfers, the memory watchdog and janitors.
- [enroot](enroot.md): the other runtime.
- [Configuration](configuration.md): every provider option, and where files go.
- [Pods](pods.md): services that share one network.
- [Security model](security.md): what isolation covers.
