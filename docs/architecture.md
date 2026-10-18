# Architecture

This page explains how hpc-sandbox builds a sandbox, runs commands in it and tears it down: the
mechanics that both providers share. It is for contributors, and for users who want to know what
happens on the node. What is specific to each runtime is in [Singularity CE and
Apptainer](singularity.md) and [enroot](enroot.md), the contract your code relies on is in the
[Python API](python-api.md), and what isolation means is in the [security model](security.md).

## Terms

| Term | Meaning |
|---|---|
| provider | `SingularityProvider` or `EnrootProvider`: starts, restores and reaps sandboxes on one container runtime |
| sandbox | one running container with its own writable root filesystem, returned by `provider.start(spec)` |
| scratch root | the node-local directory that holds sessions, snapshots, staged images and templates |
| session | a sandbox's private directory: its root filesystem, its `/tmp` and `/var/tmp`, its lease and its owner record |
| session root | the directory that holds one provider's sessions, with its janitor lock and its trash |
| lease | an `flock` that the process that started a sandbox holds on the sandbox's session |
| owner record | `owner.json` in a session: who started the sandbox, and which processes make up its container |
| image cache | where remote images are kept after their first pull, under their SHA-256; it may be on shared storage |
| template | Singularity only: an image extracted once per node, which sandboxes copy or mount as an overlay's lower layer |
| trash | a session root's `.trash/` directory, where stopped sessions wait for their deletion |
| janitor | the sweep that stops and deletes the sessions of processes that died |
| launcher | the small process (`anchor.py`) that creates the namespaces of an enroot sandbox or a pod service and runs its init |
| pod, holder | a group of services sharing one network; the holder is the process that owns that network |

## Overview

Starting a sandbox takes these steps:

1. **Check the spec.** An invalid spec raises `ValueError` before anything is created: an image the
   provider can never run, a network mode it does not offer, a relative `workdir`, a `memory_mb` or
   `cpus` that is not positive, a mount whose target is not absolute, is `/` or lies in `/tmp` or
   `/var/tmp`, a mount whose source is missing, or an invalid environment variable.
2. **Prepare the node.** Every start makes sure the scratch directories exist. The first start of
   each provider also sweeps the sessions of dead processes ([Janitors](#janitors)), and an enroot
   provider writes its system configuration ([enroot](enroot.md#what-enroot-is-allowed-to-add)).
3. **Resolve the image** to an image file: a local file, or one pulled once into the image cache
   ([Images and the image cache](#images-and-the-image-cache)).
4. **Open a session** on node-local scratch, lease it and write its owner record
   ([Sessions](#sessions)).
5. **Fill the root filesystem** from the image: an extraction, a copy of a template, or a FUSE
   overlay.
6. **Start the container**: a Singularity CE or Apptainer instance, or one long-lived
   `enroot start`. The provider records the identities of the container's processes in the owner
   record.
7. **Probe** that the root filesystem and the working directory are writable, creating the working
   directory if it is missing.

The first three steps create no session, and their failures raise `ValueError` or `SandboxError` (an
image that cannot be pulled, for example). A start that fails later stops what it started, discards
its session and raises `SandboxStartError`. Once started, a sandbox runs [commands](#commands) and
[transfers](#transfers), takes [snapshots](#snapshots-and-restores), and runs under the [memory
watchdog](#the-memory-watchdog) when its spec sets `memory_mb`. [Teardown](#teardown) stops the
container and deletes the session in the background, and [janitors](#janitors) clean up after
processes that died before they could stop their sandboxes.

## The scratch root

Everything a sandbox writes is on node-local scratch, below the scratch root:

| Path | What it holds |
|---|---|
| `sessions/` | the sessions of Singularity sandboxes |
| `sessions-enroot/` | the sessions of enroot sandboxes |
| `pods-singularity/`, `pods-enroot/` | one session per pod, holding its services' sessions in `services/` |
| `snapshots/` | snapshots, one `snap-<32 hex digits>` directory each |
| `images/` | images staged from an image cache on another filesystem, and the remembered digests of local images |
| `templates/` | Singularity templates |
| `runtime-tmp/`, `runtime-cache/` | the runtime's temporary and cache directories |
| `enroot-config/`, `enroot-sysconf/`, `enroot-tools/` | enroot's empty user configuration, the system configurations the provider writes, and its import wrapper |

The provider creates these directories with mode 0700 and refuses a scratch root that another user
owns. [Configuration](configuration.md) says how the scratch root is chosen.

## Images and the image cache

Each provider serves one image format: SIF files for Singularity CE and Apptainer, squashfs files
for enroot. A reference names either a local image file or a remote image; each runtime page lists
the forms it accepts. A reference the provider can never run (a SIF image for enroot, a squashfs
image for Singularity, a scheme the runtime cannot pull) raises `ValueError` before anything is
created.

A remote reference is pulled once into the image cache:

1. Holding a lock file for the reference in the cache, the runtime pulls the image into a partial
   file, trying up to three times.
2. The file is validated: with `sif list` for SIF, by its magic number for squashfs.
3. It is published read-only under its own SHA-256, as `sha256-<hex>.sif` or `sha256-<hex>.sqsh`.
   Image files in the cache never change.
4. A small pointer file says which image the reference names: `uri-<hash>.<ext>.json`, named after
   the pull URI, or `oci-<digest>.<ext>.json` for a digest-pinned reference. It is replaced
   atomically, and records the source, the image's SHA-256, when the pull started and ended, and the
   runtime that pulled it. Pointers are kept per format, so providers of both runtimes can share one
   cache.

A tag that moves keeps the image it named when it was first pulled until you refresh it, with
`hpc-sandbox pull --refresh IMAGE` or `await provider.images.pull(image, refresh=True)`, once,
before the sandboxes that should get the new image start. Sandboxes that start afterwards get it;
running sandboxes keep theirs. A start never waits for a refresh while an image for its reference is
cached, and a refresh takes only a pull that started after it was asked for, so refreshes asked for
during one pull share the next one. A refresh never pulls a digest-pinned reference again. Earlier
image files stay in the cache until you delete them; enroot's squashfs images are not
byte-reproducible, so every enroot refresh adds a file. Pin images by digest
(`docker://repo@sha256:<hex>`, with either runtime) or by file content for runs that must not
change.

A local image file is used in place. A provider validates it once, for as long as its device, inode,
size and modification time stay the same. A local reference may end in `@sha256:<hex>`, which pins
the file's content: a start from a file whose SHA-256 differs raises `SandboxError`.

Inside a batch job (`PBS_JOBID` or `SLURM_JOB_ID` is set) whose image cache is on another filesystem
than the scratch root, such as Lustre or NFS, images are staged: each one is copied to `images/` on
scratch as `sha256-<digest>.<ext>`, hashed while it is copied and refused if the digest differs, so
that many starts read local disk instead of shared storage. Later processes on the node trust a
read-only copy of the same size under that name. With staging, the SHA-256 of a local image is also
remembered on scratch, keyed by the file's device, inode, size and modification time, so that each
process does not read the whole file from shared storage again. The `stage_images` option turns
staging on or off.

## Sessions

Each sandbox owns one session directory in its provider's session root, named after the sandbox: its
`id`, `hpcsb-` followed by 32 hex digits.

```text
<session root>/
├── .janitor.lock      held while sessions are created, moved to the trash or swept
├── .deletions.lock    background deletions of this root take turns on it
├── .trash/            stopped sessions waiting to be deleted
└── hpcsb-<id>/        mode 0700
    ├── .lease         flock held by the process that started the sandbox
    ├── owner.json     the owner record
    ├── rootfs/        the writable root filesystem
    ├── tmp/           bound to /tmp
    └── var-tmp/       bound to /var/tmp
```

Each runtime adds files of its own ([Singularity CE and Apptainer](singularity.md),
[enroot](enroot.md)). A session is created and leased under the session root's janitor lock, so a
janitor never sees a session between its creation and its lease. The process that started the
sandbox holds the lease until the session has left the session root. The janitor lock is awaited on
the event loop, never in a worker thread: a sweep that holds it needs worker threads to finish.

The owner record is written atomically, with mode 0600, as soon as the session exists, and again
once the container's processes are known:

| Field | Meaning |
|---|---|
| `schema` | `hpc-sandbox/session-v1` |
| `instance_name`, `session_directory`, `hostname` | the session the record belongs to; janitors use a record only when all three match |
| `owner_pid`, `owner_start_time` | the process that started the sandbox |
| `provider` | `singularity` or `enroot` |
| `labels`, `image` | from the spec |
| `remap_root`, `container_uid` | who commands run as: root mapped to your uid (`true` and `0`), or your own uid |
| process identities | the container's processes: the instance's PID file, PID and start time on Singularity; the PIDs and start times of the launcher and the init on enroot |

A PID alone is not an identity: it can be reused as soon as its process is reaped. Everything that
signals a process it did not just start first proves the identity (PID, start time), reading the
start time from `/proc/<pid>/stat`.

The root filesystem is a directory owned by your uid, with no loop device or size-capped tmpfs: it
can grow as far as the scratch filesystem allows. After an extraction, the provider gives your uid
read and write access to every file and directory in it: images hold root-private build caches,
which you could otherwise neither change nor delete. A path in it that another uid owns fails the
start.

## Commands

`exec` runs a shell string with `/bin/sh -c`, or an argument list as it is. The command's
environment is the container's own (the image's `ENV`), then `HOME=/root`, then `SandboxSpec.env`,
then the call's `env`; the host's environment never reaches it.

- These variables travel as one line of POSIX-quoted shell ahead of the command's stdin. A short
  `sh` prologue inside the container reads exactly that line, evaluates it, enters the working
  directory, points stdin at `/dev/null` when the caller sent none, announces the start on stderr
  and executes the command, which reads the rest of stdin.
- Values pass verbatim, newlines included. They appear neither on command lines, which every user of
  a node can read, nor in the environment of the host-side processes that enter the container, and
  they need nothing from the sandbox's filesystem.
- Names that the image's `/bin/sh` reserves cannot be set: with bash, read-only names such as `UID`,
  `EUID`, `PPID` and `BASHPID`; with dash, an `OPTIND` that is not a number. The prologue skips them
  rather than let the shell exit.

The start announcement is how hpc-sandbox tells a command that ran from one that never started, and
it is removed from the result:

- A working directory that is missing or cannot be entered fails the command with exit code 126 and
  a one-line message on stderr, as `docker exec` does, without falling back to another directory: it
  is the sandbox's state, not a failure of the infrastructure.
- A command that never started for any other reason, such as a runtime that could not enter the
  container, raises `SandboxError`, or `SandboxLostError` when the container is gone.
- A command that exits with a non-zero status while the container is gone raises `SandboxLostError`.

Output is bounded: of each stream, the first and the last half of `output_limit_bytes` (16 MiB by
default) are kept, with a note of how many bytes were left out between them. A command that leaves a
background process holding its stdout or stderr still returns: the exit status is polled, and the
streams are drained and discarded in the background.

Every call has a timeout, `default_timeout_s` (600 s) when none is given. On expiry, the command's
process group and every process still below it in the process tree are killed with SIGKILL, and the
result has exit code 124, `timed_out=True` and a note on stderr. A cancelled `exec` kills its
process tree before `CancelledError` reaches the caller. The [Python API](python-api.md) describes
the results and the errors as your code sees them.

## Transfers

`upload` and `download` go through a fresh directory in the sandbox's private `/tmp`
(`/tmp/.hpc-sandbox-transfer/<random>`), which is a directory of the session on the host. So they
work for binary files of any size and see the container's view of paths, its mounts included. Inside
the sandbox, `cp` runs from `/`, so transfers work once the working directory is gone; relative
paths still resolve from the working directory, as a relative `cwd` of `exec` does.

Code in the sandbox can change that directory at any moment. The host side therefore works in it
only through directory file descriptors, one per level and without recursion: it never follows a
symlink it finds there, never blocks on a FIFO, and gives back your access to directories the
sandbox locked.

- **Uploads** copy the host file or directory into the transfer directory, then into place with
  `cp -p` (`cp -pR` for a directory), which keeps modes and timestamps. A symlink given as the
  source is followed, and symlinks below it are copied as symlinks. Special files on the host raise
  `shutil.SpecialFileError`, except a character device given as the source itself, which is read to
  its end (`/dev/null` gives an empty file). A directory's contents go into the destination
  directory, which is created if it is missing; uploading a file onto a directory fails.
- **Downloads** copy the source into the transfer directory with `cp`, following a symlink given as
  the source as `cp -H` does, then from there into the destination on the host. They keep modes and
  timestamps, but not owners (inside a user namespace, files on host mounts belong to owners the
  namespace does not map), and not what the sandbox must not hand you: setuid, setgid and sticky
  bits are dropped, your umask applies, and the copy is always readable by you. Files are copied up
  to the size they had when they were opened. Special files and directories nested more than 256
  levels deep raise `SandboxError`, and a missing source raises `FileNotFoundError`.
- **Symlinks inside a download** are copied only if they stay inside the downloaded tree on their
  own: relative, climbing with leading `..` no higher than the tree, and never after a name (`a/..`
  leads wherever `a` does, which a later download into the same directory could change). The others,
  absolute ones included, are left out, logged, and returned in the `DownloadReport` with each
  link's path, text and reason. Kept links can still reach what the destination already held, so
  download into a new or empty directory to keep the sandbox's links away from your own files.
- **The destination of a download is yours**: a symlink there is followed, a directory receives a
  directory's contents, only a file replaces an existing file, and a FIFO is refused.

Each transfer has the provider's `default_timeout_s` and can be cancelled in the middle of a file.
Its transfer directory is removed afterwards; if code in the sandbox keeps refilling it, the removal
gives up after a minute and the session's deletion takes the rest.

## The memory watchdog

With `memory_mb` set, a watchdog samples the sandbox's memory every `memory_poll_s` seconds (1 by
default) and, when the sandbox holds more than the limit, kills one branch of its processes: a
process with all its descendants. Most sites give users no cgroups, so this is a sampled guard, not
a cgroup: usage can overshoot between samples. Where rootless cgroups work, Singularity's
`cgroup_limits` also turns `memory_mb` into a real limit ([Singularity CE and
Apptainer](singularity.md#site-notes)).

### What it measures

Each sample walks the container's process tree, from the Singularity instance or from the launcher,
and the host-side process tree of every running command, transfers included. For each process it
reads the proportional set size (PSS) and the private memory from `/proc/<pid>/smaps_rollup`, the
`oom_score_adj`, the command line and the session ID. A process whose `smaps_rollup` cannot be read,
or that has none (kernels before 4.14), counts its resident set size instead: the setuid Singularity
starter is not dumpable, and charging the RSS is conservative. What the sandbox holds is the sum of
the PSS of these processes, and the excess is what it holds above the limit.

The watchdog never kills:

- the processes that make up the sandbox, recorded when it starts: the instance or the launcher, and
  its children at that moment. What later commands leave running below them is not protected;
- a process whose `oom_score_adj` is -1000 while the sandbox's processes inherited a higher value,
  which takes privileges on the host to set.

### Which branch dies

The watchdog weighs processes in three ways:

| Term | Meaning |
|---|---|
| what a branch holds | the PSS of its processes, where a process whose `oom_score_adj` is below its parent's counts that much less (a thousandth of the limit per point, never below zero) |
| a process's mark | a thousandth of the limit for each point by which its `oom_score_adj` is above its parent's; a process that is not above its parent has no mark |
| what a branch frees | the PSS of its processes that may be killed; when a child is weighed, a child that is a fork copy of its parent (the same command line, such as a `multiprocessing` fork worker) and has no children of its own counts only its private memory, never more than its PSS, since the pages it still shares with its parent stay; a copy's children share its pages too, and die with it |

It then chooses:

1. **The top-level branches** are the process tree of each running command, and each process tree
   below the sandbox's own processes, where earlier commands left processes running in the
   background. Processes that may not be killed are looked through, and branches that would free
   nothing are skipped. When none is left, nothing dies.
2. **It starts from the top-level branch that holds the most.**
3. **A marked process goes first** when what its branch holds plus its mark outweighs that branch:
   of the marked processes that may be killed, the watchdog starts from the one with the largest sum
   instead. As the kernel scores each process alone, marks never add up.
4. **It descends** from the current process, one level at a time, among its children, looking
   through processes that may not be killed. It moves:
   - to a child whose `oom_score_adj` is higher than the current process's, if that child's branch
     plus its mark outweighs the current process alone, or if that child alone frees the excess; of
     several such children, to the one whose branch plus mark is the largest. The current process
     alone weighs what the kernel would score it: its PSS, plus or minus a thousandth of the limit
     for each point by which its `oom_score_adj` differs from its parent's, and never below zero;
   - otherwise, to the child whose death frees the most, if that child alone frees the excess;
   - otherwise, it stops.
5. **The current process dies with its branch**, except the processes in the branch that may not be
   killed.

So the command that ran away dies and a harness survives a tool command it started, whatever their
sizes, even when the code it runs in a child forks workers of its own; when the memory is spread
over several children, their parent's branch dies whole. Because a pool's fork workers count only
their private memory, a pool owner that grows dies with its workers. As with the kernel's OOM
killer, a pool's worker that holds the excess alone dies alone: a harness that runs code in children
it joins (`multiprocessing.Process`, with a `Manager` or several at once, as code-evaluation graders
do) learns that one was killed, and so do a `ProcessPoolExecutor` and joblib, which raise an error,
but a `multiprocessing.Pool`, which never notices a lost worker, waits for its task until the
command times out. The process tree cannot tell such a pool from such a harness. The watchdog may
also kill a larger idle process before a smaller one that is growing.

`oom_score_adj` counts as it does for the kernel's OOM killer, relative to the parent's value, so a
value that every process inherited changes nothing. A harness that starts its tool commands with
`choom -n 1000 -- cmd`, or that writes `/proc/self/oom_score_adj`, makes them preferred victims here
and under the kernel alike. A process that lowered its value only weighs less itself.

### How a branch dies

The kill is recorded before the first signal, so a command whose process dies finds the record when
its caller sees the exit. A signal goes to a process only while its identity (PID and start time)
still matches:

1. Every process of the branch but its root is stopped with SIGSTOP, parents first, so that none can
   fork a replacement.
2. The root is killed with SIGKILL at once, never stopped: its parent (`nsenter`, the runtime or a
   job-control shell) sees a kill, not a stop.
3. Once the root is gone, or after a second, what the branch forked since the sample is stopped too:
   the new children of the stopped processes, and the orphans that the root left and the sandbox's
   own processes adopted. An orphan counts when it started after the sample began and is in one of
   the branch's sessions (orphans keep their session ID), so other commands' processes are left
   alone.
4. Everything stopped is killed with SIGKILL.

Processes left out of the branch stay out. A process that the root moves into a new session in the
instant before the kill survives, and stays watched.

### What the caller sees

- A command that the watchdog killed returns exit code 137. A command whose descendants it killed
  keeps its own exit status, or times out.
- When the branch was part of a running command, the explanation is appended to that command's
  stderr, for example:

  ```text
  hpc-sandbox: memory limit exceeded (PSS 4404019200 > 4294967296 bytes); killed pid 8817 and 3 descendant(s) using 1048576000 bytes; tmpfs files hold 0 bytes
  ```

- Every kill is logged as a warning and appended to `sandbox.memory_kills`, with the branch's root
  (`pid`), its processes (`pids`, `processes`), what the branch and the whole sandbox held
  (`pss_bytes`, `total_bytes`), the limit (`limit_bytes`), the command whose process tree held the
  branch (`exec_root`, `None` for processes left in the background), and how much the files on the
  sandbox's own tmpfs mounts hold (`tmpfs_bytes`). That memory, in `/dev/shm`, `/run` and the like,
  is in no process's PSS unless a process maps it, and no kill frees it.

## Snapshots and restores

`snapshot()` copies the sandbox's files into `snapshots/snap-<32 hex digits>/` on scratch:

1. It runs `sync` in the sandbox.
2. It copies the root filesystem and the private `/tmp` and `/var/tmp` with `cp -a --reflink=auto`:
   nearly free on filesystems with reflinks, such as XFS and Btrfs, a full copy elsewhere. Of a
   sandbox with Singularity's fuse-overlay layout, it copies only the upper layer, which holds what
   the sandbox wrote. The transfer directory is left out.
3. It writes the snapshot's record, `snapshot.json`.

Processes and memory are not captured, and files written during the copy may be captured partially:
take snapshots while the sandbox is quiescent. A snapshot that fails leaves nothing behind. Its
metadata records the snapshot's format, when it was taken, the source sandbox, the `workdir` and the
labels, and `remap_root` and `container_uid` as the owner record does.

A snapshot lives on the node that took it, in the scratch root of the provider that took it.
`provider.restore(snapshot, spec)` starts a new sandbox from a copy of it, which needs that
provider's scratch root and a spec with the same image; anything else raises `ValueError`. The new
sandbox starts like any other, except that its root filesystem was written by code in a sandbox,
which the enroot provider takes into account ([enroot](enroot.md#snapshots-and-restores)).
`provider.delete_snapshot(snapshot)` deletes a snapshot.

## Teardown

`stop()` is idempotent and shielded from cancellation: teardown always runs to its end, and a
cancelled caller sees `CancelledError` only afterwards.
`async with await provider.start(spec) as sandbox:` calls it when the block exits, however it exits.
A stop:

1. cancels the memory watchdog and kills the process trees of running commands, whose calls then
   raise `SandboxError`;
2. stops the container, re-proving the identity of each process before it signals it (each runtime
   page says how);
3. moves the session out of the session root by one rename into its `.trash/` directory, under the
   janitor lock, and releases the lease; the trash entry is named after the session and the process
   that moved it;
4. hands the trash entry to a deletion in a detached process, and returns.

If a step fails, for example because a process's identity cannot be proved, the session stays for a
janitor and a later `stop()` retries. On a scratch so full that even the rename fails (no space
left, or the quota exceeded), the stop deletes the session in place, under its lease and the janitor
lock.

The detached deletion is reparented to init and holds an `flock` on its trash entry until it ends;
janitors leave a held entry alone. Deletions of one session root run one at a time, in turn on the
root's `.deletions.lock`, at the lowest CPU priority (`nice -n 19`): in parallel they slowed starts,
whose extractions and copies create the metadata that the deletions remove. A deletion refuses a
tree that holds a directory owned by another uid, gives back your access to directories the sandbox
locked, and never crosses into another filesystem (`rm --one-file-system`). Its errors go to a log
next to its trash entry, which it removes when it succeeds. While 16 of a process's deletions are
pending, its stops delete their sessions themselves, at the process's own priority: under CPU load,
deletions at the lowest priority, one at a time, fall behind the starts, and the trash would grow
without bound.

`await provider.wait_for_deletions(timeout_s=300.0)` waits until the sessions this process stopped
are deleted, and returns `False` if the timeout passes first. Call it before your process exits and
before your Slurm step or batch job ends: the end of a step or a job kills the processes it started,
detached deletions included, and leaves the rest of their work to a janitor. Call it also before you
remove a scratch root yourself, whose files the deletions are still deleting. `hpc-sandbox run` and
`hpc-sandbox doctor` wait for their own deletions.

## Janitors

A janitor cleans up after a process that died without stopping its sandboxes: after a crash, a
SIGKILL, or the end of a batch job. It runs:

- at the first start of each provider. The start waits for that sweep, but never for the janitor
  lock: when another process holds the lock, the provider sweeps at a later start instead. When many
  providers start sandboxes on one node at once, waiting for the lock would queue every start behind
  every other sweep;
- in `await provider.reap(labels)`, which first stops this provider's own live sandboxes and pods
  whose labels include `labels`, then sweeps, waiting for the lock, and filtered by the labels when
  they are given;
- in `hpc-sandbox janitor`, which sweeps the sessions of the runtimes it selects ([Command
  line](cli.md)). Run it at the end of your batch script ([Getting started](getting-started.md)).

A sweep holds the janitor lock of a session root while it looks at each `hpcsb-<id>` directory
there:

1. It reports and keeps a session that is not yours, or that others may enter (mode bits beyond
   0700). It skips the sessions of the other provider, which only that provider knows how to stop.
2. A session whose lease is held, or whose owner process still runs (the same PID and start time),
   is live and left alone. With labels, a session whose recorded labels do not include them is left
   alone too.
3. The provider stops whatever still runs for the session, using the owner record only when it is
   bound to that session on this host (its `instance_name`, `session_directory` and `hostname`
   match). It fails closed: when an identity cannot be proved or disagrees, it reports the session
   and keeps it.
4. The session moves into the trash, and the janitor deletes it once it has released the lock.

A sweep also takes over the trash entries that no deletion holds, once the process that moved them
has died, or a minute after the move if it still lives. It holds at most 64 trash entries at once,
the sessions it moved and the entries it took over alike: past that, it deletes them after the
janitor lock and takes the lock again for the rest. A cancelled sweep lets go of what it holds, for
other janitors. Janitors never delete through a mount point. Singularity janitors also unmount the
overlays that killed jobs left dead on the node, and delete abandoned template extractions
([Singularity CE and Apptainer](singularity.md#teardown)).

Creating and removing sessions take the janitor lock briefly, so they wait while a sweep stops
abandoned sandboxes, or while a session that a full scratch left no room to move is deleted in
place.

A sweep's result is a `JanitorReport`: the sessions it `stopped` (something still ran for them),
`removed` and found `live`, and the `errors` it met. `hpc-sandbox janitor` prints it as JSON.

## Pods

A pod reuses this machinery. Its session, in `pods-singularity/` or `pods-enroot/`, holds the logs
of its holder and of slirp4netns and, in `services/`, the session root of its services: each service
is a sandbox, started by the launcher in the pod's user and network namespaces. Stopping a pod stops
its services, slirp4netns and the holder, then moves the pod's session, services and all, into the
trash, to be deleted in the background like a sandbox's. Janitors stop and remove abandoned pods
whole. [Pods](pods.md) describes them.

## Where the code is

The package is in `src/hpc_sandbox/`:

| Module | What it does |
|---|---|
| `types.py` | the public data types and protocols: `SandboxSpec`, `Mount`, `ExecResult`, `Sandbox`, `SandboxProvider`, the pod types and the exceptions |
| `sandbox.py` | what both providers share: spec checks, the start sequence, commands and their prologue, transfers, snapshots, the watchdog's wiring and teardown |
| `sessions.py` | session directories, leases, owner records, the trash and janitor sweeps |
| `images.py` | image references, the image cache and its pointers, refreshes and staging |
| `memory.py` | the memory watchdog |
| `safefs.py` | file operations in directories that a sandbox can change |
| `process.py` | bounded, killable host subprocesses |
| `procfs.py` | process identities, process trees, memory and mount tables, read from `/proc` |
| `locks.py` | `flock` helpers that never block the event loop |
| `paths.py` | the default scratch root and image cache, and when to stage images |
| `runtime.py` | the Singularity CE and Apptainer command line and its environment |
| `singularity.py` | `SingularityProvider`: instances, templates and the fuse-overlay layout |
| `enroot.py` | `EnrootProvider`: imports, the system configuration, and containers entered with `nsenter` |
| `anchor.py` | the launcher, run as a script: the namespaces and the init of an enroot sandbox or a pod service |
| `pods.py` | pods and their services |
| `holder.py` | the process that holds a pod's user and network namespaces and relays its upstreams |
| `tools.py` | finding and fetching slirp4netns |
| `cli.py` | the `hpc-sandbox` command line |

## Next steps

- [Singularity CE and Apptainer](singularity.md): instances, templates, the fuse-overlay layout and
  site notes.
- [enroot](enroot.md): imports, namespaces, the system configuration and site notes.
- [Security model](security.md): what isolation covers, and which problems to report.
- [Python API](python-api.md): the contract of `exec`, transfers, snapshots and errors.
- [Contributing](../CONTRIBUTING.md): set up a checkout and run the tests.
