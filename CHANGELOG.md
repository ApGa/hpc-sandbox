# Changelog

## Unreleased

### Fixed

- **Cancelling a command while it starts no longer hangs.** On Python 3.11,
  asyncio waits forever when the creation of a subprocess is cancelled while
  what it started keeps its output open (as commands behind `nsenter` do);
  cancelled `exec` calls and teardowns could hang that way.

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
