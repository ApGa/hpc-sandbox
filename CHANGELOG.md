# Changelog

What each release of hpc-sandbox changes. Its version is below 1.0: a minor release (0.4.0) may
change the Python API or the command line, and a patch release (0.3.1) only fixes bugs. To stay on
one minor release, require a range such as `hpc-sandbox>=0.3,<0.4`.

## 0.3.0 (unreleased)

The first release on PyPI. It adds pods, a fuse-overlay root filesystem layout and per-node image
templates for Singularity, deletes stopped sessions in the background, and replaces the memory
watchdog's victim rule. [Upgrading](#upgrading) lists what callers of 0.2.0 may need to change.

### Changed behaviour

- **A stop returns once its session left the session root**: it is moved into the root's `.trash`
  under the janitor lock, and a detached `rm` deletes it afterwards, one deletion per session root
  at a time, at the lowest CPU priority (while 16 of a process's deletions are pending, its stops
  delete their sessions themselves). `await provider.wait_for_deletions()` waits for them; call it
  before a process or a Slurm step ends, which kills them, and before removing a scratch root
  yourself. On a scratch too full even to move a session, the stop deletes it in place, and a stop
  that failed is retried by the next. Janitors take over the trash entries no deletion holds once
  the process that moved them died.
- **Singularity instances start and stop under their own control lock**, not a node-wide one, so
  they start and stop in parallel.
- **A provider sweeps abandoned sessions at its first start that finds the janitor lock free**, and
  at a later start otherwise: a start never waits to sweep. Janitors and `reap()` still do.
- **The memory watchdog kills the branch of processes that holds the memory**, not the largest
  single process. It starts from the heaviest of the running commands and the processes left running
  in the background, and descends into the child branch that holds the most while that child alone
  holds the excess over the limit. What a branch holds is what killing it frees: a fork copy of its
  parent with no children of its own counts its private memory only. So a harness survives a tool
  command that ran away, even when the harness is the largest process; a child it joins (as
  code-eval graders run each sample, with a `multiprocessing.Manager`) dies alone; and a pool owner
  that grows dies with its fork workers. As with the kernel, a pool's worker that holds the excess
  alone dies alone: a `multiprocessing.Pool`, which never notices a lost worker, then waits for its
  task until the command times out. The branch dies whole, with what it forks meanwhile.
  `oom_score_adj` counts as for the kernel's OOM killer, relative to the parent's value and for each
  process alone: tool commands started with `choom -n 1000` go first, the marks of a browser's
  renderers do not add up, and a value every process inherited changes nothing.
- **A command whose descendants the watchdog killed keeps its own exit status**, with the
  explanation appended to stderr (also when it then times out); only a command the watchdog killed
  itself returns 137.
- **Processes that commands leave running in a Singularity sandbox can be killed by the watchdog.**
  Only the processes that make up the sandbox when it starts are spared.

### Added

- **Pods**: services sharing one network, like a docker-compose project.
  `provider.start_pod(PodSpec(...))` returns a pod; `pod.start_service(ServiceSpec(...),
  wait="started" | "healthy")` starts each service, with compose healthchecks (`HealthCheck`),
  `health()`, `logs()`, upstream relays to host Unix sockets (`pod.routes`),
  `set_network("none" | "egress")` and `stop()`. Both providers have the `pods` capability; see
  [docs/pods.md](docs/pods.md).
- **`hpc-sandbox fetch-tools`** downloads the pinned slirp4netns that pods use for egress, for
  hosts that have none; providers take a `slirp4netns` path and a `service_stop_grace_s`.
- **A fuse-overlay layout for Singularity sandboxes**: `rootfs_layout="fuse-overlay"` (or
  `HPC_SANDBOX_ROOTFS_LAYOUT` for a whole job) mounts each sandbox's root filesystem as a
  `fuse-overlayfs` of the node's template of the image and a per-session upper layer, run by an
  unprivileged instance. Starts and stops no longer depend on the image's size, and a snapshot holds
  only what the sandbox wrote. It needs `fuse-overlayfs`, `fusermount3` and unprivileged user
  namespaces. GPUs, cgroup limits and `--fakeroot` need the copy layout: with an explicit
  `rootfs_layout="fuse-overlay"` they raise `ValueError`, and `HPC_SANDBOX_ROOTFS_LAYOUT` leaves
  such a provider on the copy layout. Pod services always get the copy layout. Janitors unmount the
  overlays of abandoned sessions, and the dead overlays that killed `fuse-overlayfs` processes left
  anywhere on the node; see [docs/singularity.md](docs/singularity.md).
- **Templates** (`rootfs_templates`): each image extracted once per node, the fuse-overlay layout's
  lower layer; with the copy layout, sandboxes copy it. A template that lost its marker is
  extracted again, and extractions cut short are deleted by janitors. `EnrootProvider` accepts
  `rootfs_templates` and `rootfs_layout="copy"` too.
- **`provider.wait_for_deletions()`** on both providers, and `MemoryKill.pids` (in
  `sandbox.memory_kills`), the processes of the killed branch.
- **`MemoryKill.processes` and `MemoryKill.tmpfs_bytes`**: how many processes the killed branch
  held, and how much the sandbox's own tmpfs mounts held (memory no kill frees). `MemoryKill.pid`
  and `pss_bytes` now describe the killed branch.
- **`hpc-sandbox --version`**, `hpc_sandbox.__version__` and a `py.typed` marker.
- **Documentation**: the README is a landing page, and `docs/` holds the guides and the reference;
  `CONTRIBUTING.md` and `SECURITY.md` are new. `pull`'s JSON keys are documented as stable.

### Fixed

- **Cancelling a command while it starts no longer hangs.** On Python 3.11, asyncio waits forever
  when the creation of a subprocess is cancelled while what it started keeps its output open (as
  commands behind `nsenter` do); cancelled `exec` calls and teardowns could hang that way.
- **A janitor sweep no longer stalls a process starting or stopping many sandboxes.** Session
  directories were created and removed in worker threads that waited for the janitor's lock, while
  the sweep holding it needed a worker thread too.
- **A start cancelled while its session directory is created leaves nothing**: the directory and
  its lease were kept until the process exited.

### Upgrading

- **Stops return before their sessions are deleted.** Code that removes a scratch root, or ends its
  process or Slurm step, right after stopping sandboxes should first
  `await provider.wait_for_deletions()`; otherwise the removal races the deletions, and what the end
  of a step kills is left for a janitor.
- **`hpc_sandbox.sessions.remove_session` is gone**: it blocked a worker thread on the janitor lock.
  Stopping a sandbox deletes its session.
- **A relative `scratch_root` is made absolute.** Sessions and snapshots that 0.2.0 recorded under a
  relative scratch root are not recognised as bound to it: stop such sandboxes with 0.2.0, or
  delete their files by hand.
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
- **Older versions sharing a cache with 0.2.0 keep working.** 0.2.0 never
  removes or rewrites their `uri-…`/`oci-…` image files or their `.json`
  records, and keeps its own pointers in differently named files. An older
  version keeps using the images it pulled, and pulls a reference itself
  when only 0.2.0 has pulled it; for a tag that has moved, the two versions
  can then run different images. Pin images by digest while versions are
  mixed.
- **Earlier image files are never deleted automatically**: remove
  `sha256-*` files no pointer names once no job uses them. With enroot every
  refresh adds one, since squashfs images are not byte-reproducible.
- **Sessions**: enroot janitors still sweep the enroot sessions 0.1.0 left in
  `sessions`.
