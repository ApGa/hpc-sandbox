# enroot

`EnrootProvider` runs each sandbox as an enroot container. Since enroot has no daemon and no running
instances, and `enroot start` runs one command in a container that ends with it, a sandbox is one
long-lived `enroot start` whose command is a keep-alive loop, in namespaces that a small launcher
creates; commands enter it with `nsenter`. Sandboxes die with the process that started them.
[Architecture](architecture.md) describes what both providers share. For root's privileges in a
sandbox, with no capability on the host's kernel, the [gVisor](gvisor.md) provider runs the same
images in gVisor, on a cluster that has enroot and no Singularity too ([Images](gvisor.md#images)).

## Requirements

| Requirement | Needed for |
|---|---|
| enroot 3.x or 4.x: `enroot` on `PATH`, or the `binary` option | every sandbox |
| enroot's `+caps` package, whose `enroot-aufs2ovlfs` converts layer whiteouts with file capabilities | importing images; local `.sqsh` files need none |
| util-linux `nsenter` | every command |
| unprivileged user namespaces (`/proc/sys/user/max_user_namespaces` above zero) | the launcher's namespaces |
| `/bin/sh` and `sleep` in the image | the keep-alive loop |

Run the process that starts the sandboxes on the host, not inside a container ([Site
notes](#site-notes)). [Getting started](getting-started.md) lists what every provider needs.

## Images

The provider runs squashfs images. A reference that starts with `/`, `./`, `../`, `~` or `file://`,
or that ends in `.sqsh` or `.squashfs`, names a local file; any other is imported:

| Reference | Example | How it is used |
|---|---|---|
| a local squashfs file | `/images/task.sqsh`, `file:///images/task.sqsh` | in place, once its magic number shows it is squashfs |
| a local squashfs file pinned by content | `/images/task.sqsh@sha256:<hex>` | in place, and refused unless the file's SHA-256 matches |
| a `docker://` reference, in Docker's or enroot's spelling | `docker://ghcr.io/org/task:1`, `docker://ghcr.io#org/task:1` | imported with `enroot import` into the image cache |
| a bare reference | `python:3.12-slim`, `ghcr.io/org/task:1` | read as a `docker://` reference |
| a digest-pinned reference | `docker://python:3.12-slim@sha256:<hex>` | imported by digest, and cached under it |
| a `dockerd://` or `podman://` reference | `dockerd://task:1` | imported by `enroot import` from the local Docker or Podman daemon |

Docker Hub (`docker.io`, `index.docker.io` or `registry-1.docker.io`) is spelled as enroot expects
it, without a registry. Imported images are cached as `sha256-<hex>.sqsh`, named by the pointer
`uri-<hash>.sqsh.json`, or `oci-<digest>.sqsh.json` for a digest-pinned reference ([Images and the
image cache](architecture.md#images-and-the-image-cache)). A SIF image (a name ending in `.sif`, or
a local file that starts like one), a scheme that enroot cannot import and a malformed digest raise
`ValueError` before anything is created.

### Digest pins

Since enroot reads `@` in a reference as a user name, the provider imports
`docker://repo[:tag]@sha256:<hex>` as `repo:sha256:<hex>`: enroot passes that to the registry where
a tag would go, and a registry serves a manifest by its digest there. A tag next to the digest is
dropped, since the digest decides.

A local file can be pinned by its content too:

```bash
enroot import --output /shared/images/task.sqsh docker://ghcr.io#org/task:1
sha256sum /shared/images/task.sqsh     # prints <hex>
hpc-sandbox run --runtime enroot /shared/images/task.sqsh@sha256:<hex> -- cat /etc/os-release
```

In Python, give the same string as `SandboxSpec.image`. The pin is checked whenever a sandbox starts
from the file, and the file is hashed again whenever its device, inode, size or modification time
changes.

### Imports

`enroot import` runs with what pulling needs from your environment, and nothing else:

| Kept | Variables |
|---|---|
| basics | `PATH`, `HOME`, `USER`, `LOGNAME`, `LANG`, `LC_ALL`, `TMPDIR` |
| XDG directories | `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, `XDG_DATA_HOME`, `XDG_RUNTIME_DIR` |
| proxies | `http_proxy`, `https_proxy`, `all_proxy` and `no_proxy`, and their upper-case forms |
| CA bundles | `SSL_CERT_FILE`, `SSL_CERT_DIR`, `CURL_CA_BUNDLE` |
| archives | `SOURCE_DATE_EPOCH`, `TAR_OPTIONS` |
| enroot's settings | `ENROOT_*` |
| Docker and Podman | `DOCKER_*`, `CONTAINER_HOST`, `CONTAINERS_*`, `REGISTRY_AUTH_FILE` |
| registry credentials | every variable that enroot's credentials file (`.credentials` in enroot's configuration directory) refers to, since enroot evaluates `$VARIABLE` and `$(command)` password fields |
| yours | the names and `PREFIX*` patterns in the `import_env` option, for example `AWS_*` when a password field runs the AWS CLI |

Batch-job, GPU and other variables are dropped. Besides:

- `ENROOT_CACHE_PATH` and `ENROOT_TEMP_PATH` point at `runtime-cache/` and `runtime-tmp/` on
  scratch, unless your environment sets them, and so do `ENROOT_RUNTIME_PATH` and
  `ENROOT_DATA_PATH`, at `runtime-tmp/`: enroot makes both before every command, by default in
  `$XDG_RUNTIME_DIR`, else `/run`, and in your home directory, which a batch job or a CI runner may
  lack or not let you write. An import uses neither. `enroot version`, which `doctor` runs, gets
  the same four.
- `TAR_OPTIONS` gets `--delay-directory-restore`, so images that revisit a read-only directory
  extract instead of failing with `Permission denied`.
- `enroot import` runs with umask 022. It makes the image's root directory and `/etc`, and writes
  `/etc/environment`, `/etc/rc` and the like, with the importer's umask, so with a stricter one
  only root could enter the image's root directory: no other user of a
  [gVisor](gvisor.md#images) sandbox could run anything. The files of the image's layers keep
  their own modes.
- When `enroot-aufs2ovlfs` is on `PATH`, a wrapper around it comes first on `PATH`, from
  `enroot-tools/` on scratch. The whiteout converter of enroot 3.5 aborts when a layer holds both a
  whiteout and an entry of the same name, and cannot remove whiteouts below a depth of five in
  read-only directories. The wrapper resolves those cases as the OCI image format defines them (an
  entry of the same layer wins over its whiteout, and a directory of the same layer becomes opaque),
  then runs the real converter, whose file capabilities stay in effect. So images that revisit
  read-only directories or shadow their own entries import. The provider tells the wrapper where the
  real converter is with `HPC_SANDBOX_REAL_AUFS2OVLFS`, which is not a setting.

## Root filesystem

`enroot create` extracts the image into the session (`rootfs/`), with enroot's data path set to the
session directory, and the provider gives your uid read and write access to everything in it. Every
sandbox gets an extraction of its own, which is the copy layout: `rootfs_layout="copy"` and
`rootfs_templates` are accepted and change nothing, and `rootfs_layout="fuse-overlay"` raises
`ValueError`.

Before the container starts, the provider:

- refuses a root filesystem whose `/etc` or `/dev` is not a directory (a symlink included), since
  enroot and its hooks write below them from the host side;
- removes a `/.lock` that is not a regular file, since enroot opens it for writing before it enters
  the root filesystem;
- creates `/root`, the default `HOME`, when the image has none;
- gives a new sandbox the host's time zone, as a regular `/etc/localtime` file that replaces the
  image's symlink into its own zoneinfo database without writing through it; a restore keeps the
  snapshot's;
- skips the image's entrypoint and `/etc/rc.local`: enroot's `--rc` script is `exec "$@"`.

Besides the shared files, an enroot session holds that `rc` script, the launcher's output
(`anchor.log`), enroot's runtime directory (`runtime/`) and, in a restored sandbox, the recorded
environment (`environment.json`).

## Namespaces

The launcher (`anchor.py`, run with the same Python interpreter) first arranges to be killed when
the process that started it dies. It then creates new user, mount, PID and IPC namespaces, mapping
your uid and gid to themselves with `setgroups` denied; for `network="none"`, it also creates a
network namespace whose only interface is an up loopback. It forks the PID namespace's init, which
mounts a fresh `/proc` and runs `enroot start --rw` (with `--root` unless `remap_root=False`), with
the keep-alive loop as the container's command. So the init is that loop:

- tasks cannot kill or stop it: as the PID namespace's init, it gets only the signals it handles
  from inside the namespace, and the loop ignores the usual ones;
- it reaps orphans;
- killing it from the host kills every process in the sandbox.

The launcher and the init are killed with SIGKILL when the process that started them dies, even by
SIGKILL: there are no orphaned containers to stop, only files for a janitor to remove. Since the
launcher creates the network namespace itself, `network="none"` always works on enroot;
`allow_network_none=False` refuses it.

## What enroot is allowed to add

Sites extend their enroot configuration for interactive jobs, with home directories, cgroups, Slurm
and PMIx variables, entrypoints and terminal settings, and none of that belongs in a sandbox. So
`enroot create` and `enroot start` run with a system configuration that the provider writes
(`ENROOT_SYSCONF_PATH`), an empty user configuration (`ENROOT_CONFIG_PATH`) and a minimal
environment.

The system configuration gives sandboxes:

| Part | What sandboxes get |
|---|---|
| mounts | enroot's standard system mounts: `/proc`, `/sys` (read-only), `/var/run` and `/var/lock` |
| `/dev` | a minimal one, never the host's: a tmpfs with `null`, `zero`, `full`, `random`, `urandom`, `tty`, a private `pts`, and the usual symlinks (`fd`, `stdin`, `stdout`, `stderr`, `ptmx`) |
| `/etc/hosts` | the host's, read-only |
| environment files | none |
| hooks | the standard enroot hooks that sandboxes need, copied from the installation when it has them enabled: `10-shadow` (passwd and group entries), `10-aptfix` (apt works as remapped root) and `98-nvidia` (it acts only when `gpus` is set, which requires it) |

Besides these, only the provider's own hook runs, which creates the `/dev` symlinks: the site's
other hooks, the Mellanox hook among them, never do. The provider finds the installation's
configuration directory as enroot finds it, by running the configuration section of the installed
`enroot` script: the environment, `enroot.conf`, its drop-ins and enroot's defaults combine exactly
as they do for enroot. Each configuration is written once into `enroot-sysconf/` on scratch, in a
directory named after its content, and never changed, so providers of other enroot installations or
versions that share the scratch root cannot alter it.

Each sandbox's `/tmp` and `/var/tmp` are session-private directories, `/dev/shm` is a private tmpfs,
and with host networking and `bind_resolv_conf`, `/etc/resolv.conf` is the host's, read-only. The
spec's mounts are bound read-only or writable as they ask, and their paths cannot contain
whitespace.

`enroot create` and `enroot start` get `PATH`, `HOME`, `USER`, `LOGNAME` and `ENROOT_LIBRARY_PATH`
from your environment, and these settings, which win over `enroot.conf`:

| Setting | Value |
|---|---|
| `ENROOT_DATA_PATH` | the session directory |
| `ENROOT_RUNTIME_PATH` | `runtime/` in the session |
| `ENROOT_CACHE_PATH`, `ENROOT_TEMP_PATH` | `runtime-cache/` and `runtime-tmp/` on scratch |
| `ENROOT_CONFIG_PATH` | an empty directory, so no user-level hooks, mounts or environment files apply |
| `ENROOT_SYSCONF_PATH` | the provider's system configuration |
| `ENROOT_MOUNT_HOME` | `no`: an empty value would fall back to the site default, which often mounts the home directory |
| `ENROOT_LOGIN_SHELL` | `no` |
| `ENROOT_RESTRICT_DEV` | `yes` |
| `ENROOT_ROOTFS_WRITABLE` | `yes` |
| `ENROOT_REMAP_ROOT` | `yes`, or `no` with `remap_root=False` |
| `NVIDIA_VISIBLE_DEVICES` | the `gpus` option, else `void` |
| `NVIDIA_DRIVER_CAPABILITIES` | yours, else `compute,utility`, when `gpus` is set |
| `MELLANOX_VISIBLE_DEVICES` | `void` |

Other `enroot.conf` settings apply as the site set them. Batch-job variables never reach enroot or
its hooks.

## Commands

A command enters the init's namespaces and root directory with `nsenter`, twice: the PID and IPC
namespaces, and the network namespace of a `network="none"` sandbox, belong to the launcher's user
namespace, and the mounts to enroot's nested one. The command runs as root of that nested namespace,
which is your uid outside, or as your uid with `remap_root=False`, without starting enroot again.
Before each command, the provider re-proves the init's identity, so `nsenter` never enters a
recycled PID.

The host-side `nsenter` processes run with only a fixed `PATH`. The container's environment,
recorded from the init when the sandbox starts, reaches the command through the prologue's stdin
([Commands](architecture.md#commands)), never through host-side processes, where a variable such as
`LD_PRELOAD` would act on the host.

## Snapshots and restores

Before a snapshot, the provider makes readable the empty files and directories of mode 0 that enroot
creates as mount points, so that a copy as your uid can open them; inside the container they stay
hidden below their mounts, and nothing else changes. A snapshot also records the environment that
the sandbox's commands get (`environment.json`).

A restore starts from a root filesystem that code in a sandbox wrote, and enroot works on the root
filesystem from the host side while it starts. So a restore:

- sets the root filesystem's `/etc/fstab` and `/etc/environment` aside until the container runs:
  enroot would otherwise mount what the first lists, and load the second, with your host privileges;
- runs only the NVIDIA hook of the standard hooks: the others write into the root filesystem with
  host-side shell code that follows its symlinks, and their results are in the snapshot anyway,
  while `nvidia-container-cli` is built to configure untrusted root filesystems;
- refuses a root filesystem whose `/etc` or `/dev` is not a directory, as every start does;
- gives commands the environment recorded with the snapshot, not the root filesystem's
  `/etc/environment`, which code in the sandbox may have rewritten.

Images are trusted the way enroot trusts them ([Security model](security.md)).

## Teardown

`stop()` kills the init with SIGKILL once it has checked that the init is still the launcher's
child. The kernel then kills every process in the init's PID namespace, and the launcher reaps the
init and exits; a launcher that has not exited within five seconds is killed too. A janitor finds
the processes of an abandoned session already gone, since they died with their owner. It kills any
survivor whose start time and command line still match the session, the init first, and removes the
files.

## Pod services

A pod's service runs `enroot start` on an extraction of its own, under the launcher's `join` form:
the container joins the pod's user and network namespaces, with mount, PID, IPC and UTS namespaces
of its own. Root lives in enroot's own user namespace, nested in the pod's, so commands in the
service hold no capability over the pod's network. A service with `run_as="caller"` runs as your
uid: the launcher nests a user namespace that maps your uid and gid to the pod's root, and enroot
runs in it without `--root`, so the node must let users nest three user namespaces: the pod's, the
launcher's and enroot's. enroot maps one id into a container, so `run_as="subids"` raises
`ValueError`, as `start_pod()` does for `PodSpec(users="subids")`. [Pods](pods.md) describes them.
With `EnrootProvider(gvisor=GvisorProvider())`, a service with `runtime="gvisor"` runs in gVisor,
from a squashfs image that enroot imports and extracts, as the pod's other services' images are
([gVisor services](pods.md#gvisor-services)).

## Site notes

Run `hpc-sandbox doctor --image IMAGE` on a compute node of a new site first. For enroot, it also
checks that the launcher can create its namespaces, that `enroot-aufs2ovlfs` converts whiteouts, and
whether the process runs inside a container; it lists the site's `enroot.conf` settings that the
provider overrides, the site's hooks, and which of the standard hooks sandboxes get.

- **Requirements.** See [Requirements](#requirements): pulling images needs enroot's `+caps`
  package, since `enroot-aufs2ovlfs` converts layer whiteouts with file capabilities.
- **Run on the host, not inside a container.** File capabilities are ignored in nested user
  namespaces, so `enroot import` fails inside a pyxis or enroot container, where `enroot` is usually
  not installed either. Run the process that owns the sandboxes as a plain Slurm step, or import
  images beforehand and pass `.sqsh` paths.
- **Sandboxes die with their owner.** The launcher and the container's init are killed when the
  owning process exits, even by SIGKILL. There are no orphaned containers to stop, only files for
  the janitor to remove.
- **Site configuration.** `enroot.conf` settings such as `ENROOT_MOUNT_HOME yes`,
  `ENROOT_REMAP_ROOT` and the data and runtime paths are overridden per sandbox, and the site's
  mounts, environment files and hooks are replaced by the provider's own system configuration ([What
  enroot is allowed to add](#what-enroot-is-allowed-to-add)). A standard hook that the installation
  lacks or has disabled is skipped, and `gpus` needs the NVIDIA hook.
- **GPUs.** `gpus="all"` passes every GPU the process may use (inside a Slurm allocation, the
  allocated ones), and a list such as `"0,1"` selects GPUs numbered as the job sees them, like
  `CUDA_VISIBLE_DEVICES`, not by the host's numbers in `SLURM_STEP_GPUS`. `gpu="nv"` means
  `gpus="all"`.

## Next steps

- [Architecture](architecture.md): sessions, commands, transfers, the memory watchdog and janitors.
- [Singularity CE and Apptainer](singularity.md): the other runtime.
- [gVisor](gvisor.md): sandboxes in gVisor, from enroot's images too.
- [Configuration](configuration.md): every provider option, and where files go.
- [Pods](pods.md): services that share one network.
- [Security model](security.md): what isolation covers.
