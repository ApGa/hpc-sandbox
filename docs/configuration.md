# Configuration

hpc-sandbox has no configuration file. Providers take keyword arguments, the command line takes
options, and a few environment variables set defaults for both. The defaults suit a batch job:
sandboxes on the job's local disk, and pulled images in a cache below your home directory.

## Provider options

`SingularityProvider(**options)` and `EnrootProvider(**options)` take keyword arguments only, and
every option has a default. `EnrootProvider` also accepts the option names of `SingularityProvider`,
so one set of options works for both runtimes; see
[Singularity option names on enroot](#singularity-option-names-on-enroot).

### Options of both providers

| Option | Default | Meaning |
|---|---|---|
| `binary` | Singularity: `singularity`, else `apptainer`; enroot: `enroot` | The runtime's executable, a name on `PATH` or a path. It is looked up when first needed, so a missing runtime raises `SandboxError` then, not when the provider is created. |
| `scratch_root` | See [The scratch root](#the-scratch-root). | The node-local directory for sessions, snapshots, staged images, templates and the runtime's temporary files. A relative path is made absolute. |
| `image_cache` | `$HPC_SANDBOX_IMAGE_CACHE`, else `${XDG_CACHE_HOME:-~/.cache}/hpc-sandbox/images` | Where pulled images are cached; see [The image cache](#the-image-cache). |
| `stage_images` | `None`: staging is on inside a batch job whose image cache is on another filesystem than the scratch root | `True` or `False` turns staging on or off; see [Image staging](#image-staging). |
| `default_timeout_s` | `600.0` | The timeout of `exec` calls that give none, and of the copy inside the sandbox that each transfer makes. |
| `start_timeout_s` | `1800.0` | The time limit of each slow step of a start: each pull attempt, extracting an image, copying a template or a snapshot, starting the container. A pod's egress, the start of a pod service and `wait="healthy"` get as long. |
| `stop_timeout_s` | `60.0` | How long a stop waits for the container to go: Singularity's `instance stop`, or the exit of enroot's launcher after SIGKILL. |
| `bind_resolv_conf` | `True` | With `network="host"`, bind the host's `/etc/resolv.conf` read-only into each sandbox. |
| `extra_start_args` | `()` | Extra arguments for `singularity instance start` or `enroot start`, after the provider's own. |
| `output_limit_bytes` | `16777216` (16 MiB) | How many bytes of each stream of a command are kept: its head and its tail. At least 4096, or `ValueError`. |
| `memory_poll_s` | `1.0` | Seconds between two samples of the memory watchdog. |
| `slirp4netns` | `None` | The slirp4netns that pods use for egress, a name on `PATH` or a path. `None` means the one on `PATH` if it has the options pods need, else the one that `hpc-sandbox fetch-tools` downloaded. |
| `service_stop_grace_s` | `0.0` | How long the commands of pod services get between SIGTERM and SIGKILL when their pod stops. |

### `SingularityProvider` options

| Option | Default | Meaning |
|---|---|---|
| `stop_grace_s` | `10.0` | How long `instance stop` gives an instance after SIGTERM before it kills it (`--timeout`). |
| `allow_network_none` | `False` | Allow `network="none"`, run as `--net --network none`, and add the `network_none` capability. Set it only where users may create network namespaces: setuid installations usually reserve them for root. |
| `gpu` | `None` | `"nv"` or `"rocm"` passes GPUs through with `--nv` or `--rocm`, and adds the `gpu` capability. With `"nv"`, `NVIDIA_VISIBLE_DEVICES` is set to `all` (inside a Slurm allocation, the allocated GPUs) unless your environment sets it. |
| `cgroup_limits` | `False` | Pass `cpus` and `memory_mb` as `--cpus` and `--memory`, where rootless cgroups work (Apptainer with systemd's user delegation). Otherwise `cpus` is advisory. The memory watchdog runs either way. |
| `rootfs_layout` | `$HPC_SANDBOX_ROOTFS_LAYOUT`, else `"copy"` | `"copy"` gives each sandbox a root filesystem of its own; `"fuse-overlay"` mounts a `fuse-overlayfs` of the node's template of the image and a layer of the sandbox's own; see [Singularity CE and Apptainer](singularity.md). Any other value raises `ValueError`. |
| `rootfs_templates` | `True` with `"fuse-overlay"`, else `False` | Extract each image once per node into `templates` on the scratch root. The copy layout then gives each sandbox a copy of the template (`cp -a --reflink=auto`) instead of an extraction of its own. |
| `fakeroot` | `False` | Start instances with `--fakeroot`: commands run as root of a user namespace that maps root to your uid and your subordinate ids to the users after it, so they can switch users. Needs the copy layout and subordinate ids for your user (`/etc/subuid`, `/etc/subgid`). A working directory the image lacks is made with mode 755, and what other users write is given back to your uid before a session is deleted; see [Root in a sandbox](singularity.md#root-in-a-sandbox). `--fakeroot` (or `-f`) in `extra_start_args` means the same. |

`rootfs_layout="fuse-overlay"` given together with `gpu`, `cgroup_limits=True`, `fakeroot=True` (or
`--fakeroot` or `-f` in `extra_start_args`), or `rootfs_templates=False` raises `ValueError`: those
need the copy layout. When the layout comes from `HPC_SANDBOX_ROOTFS_LAYOUT` instead, such a provider stays on the
copy layout.

### `EnrootProvider` options

| Option | Default | Meaning |
|---|---|---|
| `remap_root` | `True` | Run commands as root of the sandbox's user namespace (enroot's `--root`), which is your uid outside it. `False` runs them as your uid. |
| `gpus` | `None` | Enables enroot's NVIDIA hook and adds the `gpu` capability. `"all"` passes every GPU this process may use (inside a Slurm allocation, the allocated ones); a list such as `"0,1"` selects GPUs numbered as the job sees them, like `CUDA_VISIBLE_DEVICES`, not by the host's indices. The installation's `98-nvidia.sh` hook must be enabled, or a start raises `SandboxError`. |
| `import_env` | `()` | Extra variables passed to `enroot import`: names, or prefixes ending in `*` such as `"AWS_*"`. Imports get only the variables pulling needs otherwise; see [enroot](enroot.md). |

### Singularity option names on enroot

| Given to `EnrootProvider` | Effect |
|---|---|
| `gpu="nv"` | The same as `gpus="all"`. |
| `gpu="rocm"` | `ValueError`: the enroot provider passes NVIDIA GPUs only. |
| `gpu` together with `gpus` | `ValueError`. |
| `allow_network_none=False` | Refuses `network="none"`, and drops the `network_none` capability. Otherwise `network="none"` is always available: the launcher of an enroot sandbox creates its namespaces in a user namespace of its own, and can always add a network namespace. |
| `stop_grace_s` | Ignored: enroot sandboxes are stopped with SIGKILL. |
| `cgroup_limits=True` | `ValueError`: the enroot provider applies no cgroup limits, and `memory_mb` arms the memory watchdog. |
| `rootfs_layout="copy"` | Accepted: `enroot create` extracts each sandbox's root filesystem from its image, which is the copy layout. Any other layout raises `ValueError`, and `HPC_SANDBOX_ROOTFS_LAYOUT` is not read. |
| `rootfs_templates` | Ignored, for the same reason. |
| `fakeroot=True` | `ValueError`: enroot maps only your uid into a sandbox, so root there cannot switch users. `remap_root` runs commands as root without them. |

## Where files go

Sandboxes and everything they write live on the scratch root, on the node's local disk. Pulled
images go to the image cache, which nodes may share. `hpc-sandbox fetch-tools` downloads into a
third directory, `$HPC_SANDBOX_TOOLS` (see [Environment variables](#environment-variables)).

### The scratch root

A provider's scratch root is, in order:

1. its `scratch_root` option, or the command line's `--scratch-root`;
2. `$HPC_SANDBOX_SCRATCH`;
3. `$PBS_LOCALDIR/hpc-sandbox`, when `PBS_LOCALDIR` is set;
4. `/local/$PBS_JOBID/hpc-sandbox`, when `PBS_JOBID` is set and `/local/$PBS_JOBID` is a directory;
5. `$TMPDIR/hpc-sandbox-<uid>`, or, when `TMPDIR` is unset, `hpc-sandbox-<uid>` in the system's
   temporary directory (usually `/tmp`).

Items 3 and 4 follow conventions of sites that give each PBS job a directory on local disk; they
are not PBS features. Many Slurm sites point `TMPDIR` at a per-job directory on local disk. Where
yours does neither, or where that directory is small, shared or a tmpfs, set `HPC_SANDBOX_SCRATCH`
to a directory on the node's local disk. [Getting started](getting-started.md) shows this for Slurm
and PBS batch scripts, whose tasks may start without these variables.

The scratch root must be on local disk: it holds the root filesystem of every sandbox, and enroot
builds imported images in it unless you set `ENROOT_TEMP_PATH`. It must belong to you, or starts
raise `SandboxError`; hpc-sandbox creates it when it does not exist. Inside a batch job,
`hpc-sandbox doctor` warns about a scratch root on a shared filesystem or a tmpfs. Several providers
and processes can share one scratch root: janitors tell their sessions apart by leases and owner
records.

| Path below the scratch root | What it holds | Deleted |
|---|---|---|
| `sessions/` | One directory per Singularity sandbox, `hpcsb-` and 32 hexadecimal digits, with its root filesystem, its `/tmp` and `/var/tmp`, and an owner record. | By `stop()` or a janitor. |
| `sessions-enroot/` | The same, for enroot sandboxes. | By `stop()` or a janitor. |
| `pods-singularity/`, `pods-enroot/` | One directory per pod, holding its services' sessions. | By `pod.stop()` or a janitor. |
| `snapshots/` | One directory per snapshot, `snap-` and 32 hexadecimal digits. | By `delete_snapshot()`. |
| `images/` | Staged image files; see [Image staging](#image-staging). | Never. |
| `templates/` | Root filesystems extracted once per image, for Singularity's templates. | Never; janitors delete extractions cut short. |
| `runtime-tmp/`, `runtime-cache/` | The runtime's temporary and cache files; see [The runtime's temporary and cache directories](#the-runtimes-temporary-and-cache-directories). | Never. |
| `enroot-config/`, `enroot-sysconf/`, `enroot-tools/` | The enroot configuration sandboxes start with, and a helper that imports run. | Never. |

Each session root (`sessions/`, `sessions-enroot/` and the pod directories) also holds a janitor
lock, a deletion lock and a `.trash` directory, through which stopped sessions pass on their way to
deletion; [Architecture](architecture.md) describes them.

A scratch root that belongs to a job goes with the job. One that outlives jobs keeps staged images
and templates for every image version used on the node: remove them when no job of yours uses the
node, after `hpc-sandbox janitor` has run. Keep age-based cleaners, such as `systemd-tmpfiles`, out
of `templates/`: a template that lost its marker file is extracted again, but one that lost other
files is not noticed.

### The image cache

A provider's image cache is its `image_cache` option, or the command line's `--image-cache`, else
`$HPC_SANDBOX_IMAGE_CACHE`, else `${XDG_CACHE_HOME:-~/.cache}/hpc-sandbox/images`. Big images can
exceed a home directory's quota: point the cache at project storage then.

The cache may be on storage that many nodes share, if that storage supports `flock`. A remote image
is pulled once, under a lock file per reference: other processes that need it wait for that pull,
but a start never waits while an image for its reference is cached. Pulled files are validated,
then published read-only under their own SHA-256, and a small pointer file says which file a
reference names:

| File | What it is |
|---|---|
| `sha256-<hex>.sif`, `sha256-<hex>.sqsh` | An image file, named by its content. It never changes. |
| `uri-<hash>.sif.json`, `uri-<hash>.sqsh.json` | The pointer of a reference by tag: its source, the `sha256` of its image file, and when it was pulled. |
| `oci-<digest>.sif.json`, `oci-<digest>.sqsh.json` | The pointer of a digest-pinned reference. |
| `*.lock` | The lock files of pulls. |

Pointers are kept per image format, so Singularity and enroot can share one cache. A tag keeps the
image it named when it was first pulled until `hpc-sandbox pull --refresh IMAGE`, or
`await provider.images.pull(image, refresh=True)`, pulls it again; run that once before the
sandboxes that should get the new image. Running sandboxes keep their image.

hpc-sandbox never deletes image files. Earlier ones stay in the cache after a refresh, and with
enroot every refresh adds one, since squashfs files are not reproducible to the byte. Once no job
uses them, delete the image files that no pointer names; this lists them:

```bash
cd "${HPC_SANDBOX_IMAGE_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/hpc-sandbox/images}"
for image in sha256-*; do
    digest=${image#sha256-}
    grep -qs "\"${digest%.*}\"" ./*.json || echo "$image"
done
```

### Image staging

With staging on, a start copies its image file from the cache to `images/` on the scratch root, as
`sha256-<hex>.sif` or `sha256-<hex>.sqsh`, checks the copy's digest while it copies, and starts from
there. Later starts on the node use that copy, so many sandboxes read local disk instead of shared
storage.

The `stage_images` option turns staging on (`True`) or off (`False`). By default it is on inside a
batch job (`PBS_JOBID` or `SLURM_JOB_ID` is set) whose image cache and scratch root are on different
filesystems. A local image file is hashed once per node for staging: its digest is remembered in
`images/.digests/`, keyed by the file's device, inode, size and modification time.

### The runtime's temporary and cache directories

Unless your environment already sets them, the runtime's temporary and cache directories point into
the scratch root, so that pulls and extractions never fill a small `/tmp` or a home directory's
quota:

| Runtime | Variables | Set to |
|---|---|---|
| Singularity, Apptainer | `SINGULARITY_TMPDIR`, `APPTAINER_TMPDIR` | `runtime-tmp/` on the scratch root, or the value of whichever of the two you set. |
| Singularity, Apptainer | `SINGULARITY_CACHEDIR`, `APPTAINER_CACHEDIR` | `runtime-cache/` on the scratch root, or the value of whichever of the two you set. |
| enroot imports | `ENROOT_TEMP_PATH`, `ENROOT_CACHE_PATH` | `runtime-tmp/` and `runtime-cache/` on the scratch root, unless you set them. |

`enroot create` and `enroot start` always run with the provider's own data, runtime, cache,
temporary and configuration paths, whatever your environment and the site's `enroot.conf` say;
[enroot](enroot.md) describes what else the provider sets for them.

Some variables are removed from the environment Singularity CE and Apptainer run in, because they
would add binds, environment or configuration that sandboxes must not get: `SINGULARITY_BIND`,
`SINGULARITY_BINDPATH`, `APPTAINER_BIND`, `APPTAINER_BINDPATH`, `SINGULARITY_CONFIGDIR`,
`APPTAINER_CONFIGDIR`, and every `SINGULARITYENV_*` and `APPTAINERENV_*` variable. The commands
that enter a container also lose `TERM` and the proxy variables (`http_proxy`, `https_proxy`,
`ftp_proxy`, `all_proxy`, `no_proxy` and their upper-case forms), which the runtime passes into
containers even with `--cleanenv`, and whose URLs may hold credentials. Give them in
`SandboxSpec.env` if a sandbox needs them.

## Environment variables

| Variable | Read by | Meaning |
|---|---|---|
| `HPC_SANDBOX_SCRATCH` | Both providers, every command but `fetch-tools` | The scratch root when no `scratch_root` or `--scratch-root` is given; see [The scratch root](#the-scratch-root). |
| `HPC_SANDBOX_IMAGE_CACHE` | Both providers, every command but `fetch-tools` | The image cache when no `image_cache` or `--image-cache` is given. |
| `HPC_SANDBOX_RUNTIME` | The command line | The default of `--runtime`: `auto`, `singularity` or `enroot`. An empty value counts as unset; with any other value, `doctor`, `run`, `pull` and `janitor` exit with status 2 unless `--runtime` is given. The Python API does not read it. |
| `HPC_SANDBOX_ROOTFS_LAYOUT` | `SingularityProvider` | The default of `rootfs_layout`, for a whole job: `copy` or `fuse-overlay`. Another value raises `ValueError`. A provider whose options need the copy layout stays on it, and `EnrootProvider` ignores the variable. |
| `HPC_SANDBOX_TOOLS` | Pods, `hpc-sandbox fetch-tools` | Where `fetch-tools` puts the slirp4netns it downloads, and where pods look for it; default `${XDG_CACHE_HOME:-~/.cache}/hpc-sandbox/tools`. |
| `HPC_SANDBOX_REAL_AUFS2OVLFS` | Internal | Set by `EnrootProvider` in the environment of `enroot import`, for the whiteout converter it wraps. Never read from yours. |
| `HPC_SANDBOX_INTEGRATION` | Tests only | `1` runs the integration tests, which need a compute node with a container runtime. |
| `HPC_SANDBOX_TEST_RUNTIME` | Tests only | The runtime of the integration tests: `singularity`, `enroot`, or `auto` (the default). |
| `HPC_SANDBOX_TEST_IMAGE` | Tests only | The image of the integration tests; default `docker://alpine:3.20`. |
| `HPC_SANDBOX_TEST_NETWORK_NONE` | Tests only | `1` also tests `network="none"` with Singularity, where the site allows it. enroot always tests it. |
| `HPC_SANDBOX_TEST_FAKEROOT` | Tests only | `1` also tests Singularity's `fakeroot` option, where `--fakeroot` works for the user. |
| `HPC_SANDBOX_TEST_FILE_OWNERS_IMAGE` | Tests only | The image of the integration tests of file owners manifests, which run with `HPC_SANDBOX_TEST_FAKEROOT=1` or `HPC_SANDBOX_TEST_SUBIDS=1` and list the owners that the layers of `node:20-alpine` give; default `docker://node:20-alpine`. |
| `HPC_SANDBOX_TEST_GPUS` | Tests only | Tests GPU passthrough: `all` with enroot, `nv` or `rocm` with Singularity. |
| `HPC_SANDBOX_TEST_POD_IMAGE` | Tests only | The image of the pod integration tests' services; default `docker://alpine:3.20`. |
| `HPC_SANDBOX_TEST_APT_IMAGE` | Tests only | The image of the pod integration test that runs apt; default `docker://python:3.12-slim`. |
| `HPC_SANDBOX_TEST_REDIS_IMAGE`, `HPC_SANDBOX_TEST_POSTGRES_IMAGE` | Tests only | The database images that the pod integration tests run as each `run_as`, with their entrypoints; default `docker://redis:7-alpine` and `docker://postgres:16-alpine`. |
| `HPC_SANDBOX_TEST_SUBIDS` | Tests only | `1` also tests pods with `users="subids"`, with Singularity, where your user has subordinate ids that `newuidmap` and `newgidmap` map. |
| `HPC_SANDBOX_TEST_PODS` | Tests only | How many pods the pod integration tests start at once; default 16. |

[CONTRIBUTING.md](../CONTRIBUTING.md) describes how to run the tests.

### Other variables hpc-sandbox reads

| Variable | Effect |
|---|---|
| `PBS_LOCALDIR`, `PBS_JOBID` | The default scratch root on PBS sites that provide a per-job local directory; see [The scratch root](#the-scratch-root). |
| `PBS_JOBID`, `SLURM_JOB_ID` | Either one means a batch job: staging is then on by default when the image cache is on another filesystem, and `doctor` reports the job and warns about a shared or tmpfs scratch root. |
| `TMPDIR` | The last default of the scratch root. |
| `XDG_CACHE_HOME` | The base of the default image cache and tools directory, instead of `~/.cache`. |
| `XDG_CONFIG_HOME` | Where enroot's credentials file is looked for, in `enroot/.credentials`, unless enroot's configuration sets `ENROOT_CONFIG_PATH`. Imports get the variables that file names. |
| `SINGULARITY_TMPDIR`, `APPTAINER_TMPDIR`, `SINGULARITY_CACHEDIR`, `APPTAINER_CACHEDIR` | Kept when set; see [The runtime's temporary and cache directories](#the-runtimes-temporary-and-cache-directories). |
| `ENROOT_TEMP_PATH`, `ENROOT_CACHE_PATH` | Kept for `enroot import` when set. |
| `NVIDIA_VISIBLE_DEVICES` | With Singularity and `gpu="nv"`: kept when set, else `all`. |
| `NVIDIA_DRIVER_CAPABILITIES` | With enroot and `gpus`: kept when set, else `compute,utility`. |

## Next steps

- [Getting started](getting-started.md): requirements, installation, and batch scripts that set
  these variables.
- [Python API](python-api.md): providers, sandboxes and what they promise.
- [Command line](cli.md): the commands and their options.
- [Singularity CE and Apptainer](singularity.md) and [enroot](enroot.md): how each provider builds a
  sandbox, with notes for sites.
