# Changelog

What each release of hpc-sandbox changes. Its version is below 1.0: a minor release (0.4.0) may
change the Python API or the command line, and a patch release (0.3.1) only fixes bugs. To stay on
one minor release, require a range such as `hpc-sandbox>=0.4,<0.5`.

## 0.5.0 (unreleased)

### Added

- **Pod services can run as your own uid, or as a root that switches to your subordinate ids**:
  `ServiceSpec(run_as=...)` says who a service's command, and the commands entered into it, run as.
  `"root"`, the default, is root of a namespace that maps no other user, as before. `"caller"` runs
  them as your uid and gid, without capabilities (Singularity's `exec --userns`; enroot without
  `--root`), so images that switch to a user of their own only when they start as root, such as the
  official PostgreSQL and Redis images, run instead of exiting at once with
  `chown: Invalid argument` (or, with enroot, whose seccomp filter fakes that `chown` and the
  switch, starting over and over as root without getting ready).
  `"subids"`, with Singularity CE and Apptainer, gives a root that can
  switch users, as under Docker, in a pod started with `PodSpec(users="subids")`, which maps your
  subordinate ids (`/etc/subuid`, `/etc/subgid`) into its user namespace: `newuidmap` and
  `newgidmap` map them once per process, which can take seconds on some sites, and later pods reuse
  that mapping, as do those of the processes it forks afterwards. `provider.map_subordinate_ids()`
  maps them ahead, before a fork server forks its workers, for example. `start_pod()` raises
  `SandboxError` where your user has none or the tools are missing, and `ValueError` with enroot,
  which maps one id into a container. What such services write as other users is given back to
  your uid before the pod's files are deleted: at a stop, when a service stops before its pod, and
  when a janitor sweeps an abandoned pod. With Singularity, `hpc-sandbox doctor` shows your
  subordinate ids on a new `pods: subordinate ids` line. See [Users](docs/pods.md#users).

## 0.4.0 (2026-10-19)

### Changed behaviour

- **A `--fakeroot` sandbox's start makes a missing working directory as Docker does**: the
  directory and its missing parents get mode 755 whatever your umask, so that the sandbox's other
  users can enter them. One that the image has keeps its mode. Before, a umask such as 027 made them
  750, and a command that switched to another user could not enter its working directory. Sandboxes
  without `--fakeroot` make it with your umask, as before. See
  [Root in a sandbox](docs/singularity.md#root-in-a-sandbox).

### Added

- **`fakeroot=True` runs a Singularity sandbox's commands as root that can switch users**: it
  starts the instance with `--fakeroot`, on the copy layout, so that root maps to your uid and your
  subordinate ids (`/etc/subuid`, `/etc/subgid`) to the users after it. What commands write as
  those other users belongs to your subordinate ids on the host, which you cannot delete: once the
  instance has stopped, a `singularity exec --fakeroot` over the session's root filesystem, `/tmp`
  and `/var/tmp` gives it back to your uid before the session is deleted, and a janitor does the
  same for an abandoned session that ran so. `--fakeroot` in `extra_start_args` means the same.
  `EnrootProvider` takes the name, as it takes every Singularity option, and raises `ValueError`
  for `fakeroot=True`: enroot maps only your uid into a sandbox. See
  [Root in a sandbox](docs/singularity.md#root-in-a-sandbox).

### Fixed

- **A sandbox started with `--fakeroot` in `extra_start_args` is deleted after it stops, also when
  its commands wrote files as other users.** Those files belong to your subordinate ids on the host,
  and the deletion refused every directory of theirs, so the whole session, its root filesystem copy
  included, stayed in the trash, and every later janitor's sweep failed on it. They are now given
  back to your uid first, as with `fakeroot=True`. See
  [Root in a sandbox](docs/singularity.md#root-in-a-sandbox).

## 0.3.0 (2026-10-18)

The first release on PyPI. It adds pods, a fuse-overlay root filesystem layout and per-node image
templates for Singularity, deletes stopped sessions in the background, and replaces the memory
watchdog's victim rule. [Upgrading](#upgrading) lists what callers of 0.2.0 may need to change.

### Changed behaviour

- **A stop returns once its session left the session root**: it is moved into the root's `.trash`
  under the janitor lock, and a detached `rm` deletes it afterwards, one deletion per session root
  at a time, at the lowest CPU priority (while 16 of a process's deletions are pending, its stops
  delete their sessions themselves). `await provider.wait_for_deletions()` waits for them; call it
  before your program exits inside a batch job, since the end of the job or of its Slurm step kills
  them, and before removing a scratch root yourself. On a scratch too full even to move a session,
  the stop deletes it in place, and a stop that failed is retried by the next. Janitors take over
  the trash entries that no deletion holds: at once when the process that moved them died, a minute
  after the move otherwise. [Stopping and deleting](docs/python-api.md#stopping-and-deleting)
  describes it.
- **Singularity instances start and stop under their own control lock**, not a node-wide one, so
  they start and stop in parallel; see [Instances](docs/singularity.md#instances).
- **A provider sweeps abandoned sessions at its first start that finds the janitor lock free**, and
  at a later start otherwise: a start never waits for the janitor lock. Janitors and `reap()` still
  do. A first start stops and moves every abandoned session it finds, but deletes at most 64 trash
  entries; `reap()` and `hpc-sandbox janitor` take the rest, holding at most 64 entries at a time.
  See [Janitors](docs/architecture.md#janitors).
- **The memory watchdog kills the branch of processes that holds the memory**, not the largest
  single process. It starts from the heaviest of the running commands and the processes left running
  in the background, and descends into the child whose death frees the most, while that child alone
  frees the excess over the limit. A fork copy of its parent with no fork copies of its own counts
  only its private memory towards what killing its branch frees, since the pages it shares with its
  parent stay; one with fork copies below it counts its whole branch. So a harness survives a tool
  command that ran away, even when the harness is the largest process; a child it joins (as
  code-evaluation graders run each sample, with a `multiprocessing.Manager`) dies alone; and a pool
  owner that grows dies with its fork workers. As with the kernel, a pool's worker that holds the
  excess alone dies alone: a `multiprocessing.Pool`, which never notices a lost worker, then waits
  for its task until the command times out. The branch dies whole, with what it forks meanwhile.
  `oom_score_adj` counts as for the kernel's OOM killer, relative to the parent's value and for each
  process alone: tool commands started with `choom -n 1000` go first, the marks of a browser's
  renderers do not add up, and a value every process inherited changes nothing.
  [The memory watchdog](docs/architecture.md#the-memory-watchdog) has the complete rules.
- **A command whose descendants the watchdog killed keeps its own exit status**, with the
  explanation appended to stderr (also when it then times out); only a command the watchdog killed
  itself returns 137. See [Memory limits](docs/python-api.md#memory-limits).
- **Processes that commands leave running in a Singularity sandbox can be killed by the watchdog.**
  Only the processes that make up the sandbox when it starts are spared; see
  [What it measures](docs/architecture.md#what-it-measures).

### Added

- **Pods**: services sharing one network, like a docker-compose project.
  `provider.start_pod(PodSpec(...))` returns a pod; `pod.start_service(ServiceSpec(...),
  wait="started" | "healthy")` starts each service, with compose healthchecks (`HealthCheck`),
  `health()`, `logs()`, upstream relays to host Unix sockets (`pod.routes`),
  `set_network("none" | "egress")` and `stop()`. Both providers have the `pods` capability; see
  [docs/pods.md](docs/pods.md).
- **`hpc-sandbox fetch-tools`** downloads the pinned slirp4netns that pods use for egress, for
  hosts that have none; providers take a `slirp4netns` path and a `service_stop_grace_s`. See
  [`fetch-tools`](docs/cli.md#fetch-tools).
- **A fuse-overlay layout for Singularity sandboxes**: `rootfs_layout="fuse-overlay"` (or
  `HPC_SANDBOX_ROOTFS_LAYOUT` for a whole job) mounts each sandbox's root filesystem as a
  `fuse-overlayfs` of the node's template of the image and a per-session upper layer, run by an
  unprivileged instance. Starts and stops no longer depend on the image's size, and a snapshot holds
  only what the sandbox wrote. It needs `fuse-overlayfs`, `fusermount3` (or `fusermount`),
  `/dev/fuse` and unprivileged user namespaces. GPUs, cgroup limits and `--fakeroot` need the copy
  layout: with an explicit `rootfs_layout="fuse-overlay"` they raise `ValueError`, and
  `HPC_SANDBOX_ROOTFS_LAYOUT` leaves such a provider on the copy layout. Pod services always get the
  copy layout. Janitors unmount the overlays of abandoned sessions, and the dead overlays that
  killed `fuse-overlayfs` processes left anywhere on the node; see
  [The fuse-overlay layout](docs/singularity.md#the-fuse-overlay-layout).
- **Templates** (`rootfs_templates`): each image extracted once per node, the fuse-overlay layout's
  lower layer; with the copy layout, sandboxes copy it. A template that lost its marker is
  extracted again, and extractions cut short are deleted by janitors. `EnrootProvider` accepts
  `rootfs_templates` and `rootfs_layout="copy"` too. See [Templates](docs/singularity.md#templates).
- **`provider.wait_for_deletions()`** on both providers, which waits for the background deletions
  of the sessions that the process stopped; see
  [Stopping and deleting](docs/python-api.md#stopping-and-deleting).
- **`MemoryKill.pids`, `MemoryKill.processes` and `MemoryKill.tmpfs_bytes`** (in
  `sandbox.memory_kills`): the processes of the killed branch, how many they were, and how much the
  sandbox's own tmpfs mounts held (memory no kill frees); see
  [Memory limits](docs/python-api.md#memory-limits).
- **`hpc-sandbox --version`**, `hpc_sandbox.__version__` and a `py.typed` marker; see
  [Public API and versioning](docs/python-api.md#public-api-and-versioning).
- **Documentation**: the README is a landing page, and [`docs/`](docs/README.md) holds the guides
  and the reference; `CONTRIBUTING.md` and `SECURITY.md` are new. The JSON keys of `pull` and
  `janitor` are documented as stable ([Command line](docs/cli.md)).

### Fixed

- **Cancelling a command while it starts no longer hangs.** On Python 3.11, asyncio waits forever
  when the creation of a subprocess is cancelled while what it started keeps its output open (as
  commands behind `nsenter` do); cancelled `exec` calls and teardowns could hang that way. See
  [Running commands](docs/python-api.md#running-commands).
- **A janitor sweep no longer stalls a process starting or stopping many sandboxes.** Session
  directories were created and removed in worker threads that waited for the janitor's lock, while
  the sweep holding it needed a worker thread too. See [Sessions](docs/architecture.md#sessions).
- **A start cancelled while its session directory is created leaves nothing**: the directory and
  its lease were kept until the process exited. See [Providers](docs/python-api.md#providers).
- **The memory watchdog measures processes on kernels before 4.14**, which have no `smaps_rollup`:
  it reads `smaps` there. 0.2.0 measured every process as zero on them, and never killed. See
  [What it measures](docs/architecture.md#what-it-measures).
- **An `HPC_SANDBOX_RUNTIME` other than `auto`, `singularity` or `enroot` is an error**: `doctor`,
  `run`, `pull` and `janitor` exit with status 2 unless `--runtime` is given. 0.2.0 took such a
  value for Singularity, so that a typo made `hpc-sandbox janitor` skip enroot's sessions. See
  [Environment variables](docs/configuration.md#environment-variables).
- **`hpc-sandbox doctor` creates the scratch root as a start does**, with mode 0700, and reports a
  scratch root or image cache that it cannot create or write as a `[FAIL]` line. With enroot, 0.2.0
  could leave a scratch root it created readable by other users, and ended with a traceback where
  it could not write. `allow setuid = no` is now shown as `info`: sandboxes run in user-namespace
  mode too. See [`doctor`](docs/cli.md#doctor).

### Upgrading

- **Stops return before their sessions are deleted.** Code that removes a scratch root, or ends its
  process or Slurm step, right after stopping sandboxes should first
  `await provider.wait_for_deletions()`; otherwise the removal races the deletions, and what the end
  of a step kills is left for a janitor. See
  [Stopping and deleting](docs/python-api.md#stopping-and-deleting).
- **Run `hpc-sandbox janitor` from the batch script, not from an epilogue.** Epilogues, which the
  README of 0.2.0 suggested, usually run as root, whose janitor sweeps root's sessions, not yours;
  see [Getting started](docs/getting-started.md#run-the-janitor-at-the-end-of-the-script).
- **`hpc_sandbox.sessions.remove_session` is gone**: it blocked a worker thread on the janitor lock.
  Stopping a sandbox deletes its session; see
  [Stopping and deleting](docs/python-api.md#stopping-and-deleting).
- **`MemoryKill.pid` and `MemoryKill.pss_bytes` describe the killed branch**: its root and what the
  whole branch held, where 0.2.0 recorded the single process it killed. See
  [Memory limits](docs/python-api.md#memory-limits).
- **A relative `scratch_root` is made absolute.** Sessions and snapshots that 0.2.0 recorded under a
  relative scratch root are not recognised as bound to it: stop such sandboxes with 0.2.0, or
  delete their files by hand. See
  [Options of both providers](docs/configuration.md#options-of-both-providers).
- **The `test` extra is gone**: development tools are in the `dev` dependency group
  (`uv sync --group dev`; see [CONTRIBUTING.md](CONTRIBUTING.md)).

## 0.2.0

Changes since 0.1.0.

### Changed behaviour

- **A working directory that is missing or cannot be entered fails the
  command, not the sandbox.** `exec` returns exit code 126 (as `docker exec`
  does), empty stdout and, on stderr, only
  `CWD_ERROR_PREFIX + "<dir>: <reason>"` with a reason from
  `CWD_ERROR_REASONS`. 0.1.0 raised nothing on enroot (the shell's own `cd`
  error and status) and silently ran in `/` on Singularity CE. Nothing falls
  back to another directory. A relative `cwd` resolves from the workdir.
- **Commands that never started raise `SandboxError`**, or
  `SandboxLostError` when the container is gone, instead of returning the
  runtime's exit status as the command's.
- **Transfers run from `/`**, so uploads and downloads of absolute paths work
  once the workdir is gone; relative paths resolve from the workdir.
- **Downloads no longer carry**:
  - owners (files on host mounts have owners the user namespace does not map);
  - setuid, setgid and sticky bits; the caller's umask applies;
  - symlinks that could lead outside the destination: absolute ones, `..`
    climbing above it, or `..` after a name. They are left out, logged, and
    returned by `download()` in a `DownloadReport`.
- **`download()` returns a `DownloadReport`** (it returned `None`).
- **Host-side transfer errors stay `OSError`s**, and special files on the host
  raise `SpecialFileError` on upload (a character device given as the source,
  such as `/dev/null`, is read to its end).
- **The environment reaches commands on stdin**, never on a command line or in
  host-side processes, and commands without `stdin` read `/dev/null`. Names
  the image's `/bin/sh` reserves (bash's `UID`, `EUID`, `PPID`, `BASHPID`, …;
  a non-numeric `OPTIND` in dash) cannot be set.
- **Images a provider can never run raise `ValueError` before anything is
  created**: a SIF image for enroot, a squashfs image for Singularity, a
  scheme the runtime cannot pull.
- **enroot sandboxes use a system configuration the provider writes**: the
  standard system mounts, a minimal `/dev`, the host's `/etc/hosts`, no
  environment files, and only the `10-shadow`, `10-aptfix` and `98-nvidia`
  hooks. Site hooks, mounts and environment files (home directories, Slurm and
  PMIx variables, terminal settings) no longer apply. A new sandbox gets the
  host's time zone as a regular `/etc/localtime` file.
- **`enroot import` gets only the variables pulling needs**, plus `import_env`.
- **Singularity sandboxes no longer see the host's `TERM` or proxy
  variables**, and `gpu="nv"` sets `NVIDIA_VISIBLE_DEVICES=all` unless set.
- **enroot sessions live in `sessions-enroot`**, and janitors leave other
  providers' sessions alone. Without `--runtime`, the CLI janitor
  (`python -m hpc_sandbox janitor`) sweeps enroot sessions always and
  Singularity sessions when Singularity or Apptainer is installed, and prints
  one merged report (0.1.0 swept only the runtime it detected).
- **`output_limit_bytes` must be at least 4096**; smaller values raise
  `ValueError`.

### Added

- **Digest pins on enroot**: `docker://repo[:tag]@sha256:<hex>` (imported in
  the `repo:sha256:<hex>` form enroot 3.x understands).
- **Content-addressed image cache with explicit refresh**:
  - Images are stored as `sha256-<hex>.<ext>`, each named by a per-reference
    pointer (`uri-<hash>.<ext>.json`, `oci-<digest>.<ext>.json`).
  - `ImageStore.pull(reference, refresh=True)` and
    `python -m hpc_sandbox pull [--refresh] IMAGE...` pull a tag again; the
    command prints one JSON line per image with its `sha256` and
    `manifest_digest`.
  - Sandbox starts read the current pointer and never wait for a refresh.
  - `ResolvedImage.sha256` is set for every pulled image, and the digests of
    local images are remembered on node-local scratch across processes.
- **Stable error contract**: `CWD_ERROR_EXIT_CODE`, `CWD_ERROR_PREFIX`,
  `CWD_ERROR_REASONS`, and `DownloadReport` / `DroppedLink`.
- **Owner records and snapshot metadata name the provider** and record
  `remap_root` and `container_uid`.
- **enroot**: accepts the Singularity provider's option names (`gpu`,
  `allow_network_none`, `stop_grace_s`, `cgroup_limits`); new `import_env` and
  `bind_resolv_conf` options.

### Fixed and hardened

- **Transfers go through directory descriptors** and never follow a symlink,
  block on a FIFO or recurse into deep trees that code in the sandbox planted.
  They can be cancelled mid-file, and their cleanup is bounded.
- **Restores**: a restored rootfs cannot make enroot mount its `/etc/fstab` or
  load its `/etc/environment` from the host side.
- **Snapshot preparation** opens up only enroot's mode-0 mount points.

### Upgrading

- **Image caches written by 0.1.0 are reused without pulling.** An image
  stored as `uri-<hash>.<ext>` or `oci-<digest>.<ext>` is linked (or copied)
  under its digest name the first time 0.2.0 resolves it.
- **0.1.0 keeps working on a cache it shares with 0.2.0.** 0.2.0 never
  removes or rewrites 0.1.0's `uri-…`/`oci-…` image files or their `.json`
  records, and keeps its own pointers in differently named files. 0.1.0
  keeps using the images it pulled, and pulls a reference itself when only
  0.2.0 has pulled it; for a tag that has moved, the two versions can then
  run different images. Pin images by digest while versions are mixed.
- **Earlier image files are never deleted automatically**: remove
  `sha256-*` files no pointer names once no job uses them. With enroot every
  refresh adds one, since squashfs images are not byte-reproducible.
- **Sessions**: enroot janitors still sweep the enroot sessions 0.1.0 left in
  `sessions`.
