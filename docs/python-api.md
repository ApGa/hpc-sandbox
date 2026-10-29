# Python API

hpc-sandbox is an asyncio library. A provider starts sandboxes and pods on one container runtime,
and every call that touches a sandbox is a coroutine. This page describes what the names in
`hpc_sandbox.__all__` do and what they promise. Run the examples on a compute node, inside a batch
job; [Getting started](getting-started.md) shows how.

## A first script

```python
import asyncio

from hpc_sandbox import EnrootProvider, SandboxSpec


async def main() -> None:
    provider = EnrootProvider()  # or SingularityProvider()
    spec = SandboxSpec(image="docker://python:3.12-slim", env={"PYTHONUNBUFFERED": "1"})
    async with await provider.start(spec) as sandbox:  # stopped when the block exits
        result = await sandbox.exec("python -c 'print(6 * 7)'", timeout_s=60)
        print(result.exit_code, result.stdout, end="")
    await provider.wait_for_deletions()  # waits for the background deletion of the sandbox's files


asyncio.run(main())
```

```text
0 42
```

`async with` stops the sandbox when the block exits, however it exits. A stop returns once the
sandbox's processes are gone and its files have left the session root. A detached process deletes
the files afterwards, and `wait_for_deletions()` waits for it (see
[Stopping and deleting](#stopping-and-deleting)).

## Providers

A provider starts, restores and reaps sandboxes on one container runtime. Every provider takes
keyword arguments only, and their defaults suit a batch job;
[Configuration](configuration.md#provider-options) lists every option.

| | `SingularityProvider` | `EnrootProvider` | `GvisorProvider` |
|---|---|---|---|
| Runtime | Singularity CE or Apptainer, in setuid or user-namespace mode | enroot 3.x, unprivileged | rootless `runsc`, opt-in |
| Images | SIF files, pulled with `singularity pull` | squashfs files, imported with `enroot import` | SIF files, as the Singularity provider's |
| Commands run as | your uid | root of the sandbox's user namespace, which is your uid outside (`remap_root=False`: your uid) | root of gVisor's kernel, with every capability; your uid outside |
| Each command | one `singularity exec` | `nsenter` into the running container | one `runsc exec` |
| `network="none"` | with `allow_network_none=True`, where the site lets users create network namespaces | always, unless `allow_network_none=False` | always |
| When the process that started them dies | instances keep running until a janitor stops them | everything in them is killed at once; a janitor removes the files | as with enroot |

[Singularity CE and Apptainer](singularity.md), [enroot](enroot.md) and [gVisor](gvisor.md) describe
how each provider builds a sandbox.

### Provider methods and attributes

| Member | What it does |
|---|---|
| `await start(spec)` | Starts a sandbox for a `SandboxSpec` and returns it once it runs. |
| `await restore(snapshot, spec)` | Starts a sandbox whose files come from a snapshot; see [Snapshots and restores](#snapshots-and-restores). |
| `await delete_snapshot(snapshot)` | Deletes a snapshot's files. |
| `await start_pod(spec)` | Starts a pod; see [Pods](pods.md). |
| `await map_subordinate_ids()` | Maps your subordinate ids for pods with `users="subids"` now, once per process, rather than at the first such pod; see [Subordinate ids](pods.md#subordinate-ids). |
| `await reap(labels=None)` | Stops this provider's sandboxes and pods that carry `labels`, sweeps abandoned sessions on the node and returns a `JanitorReport`; see [Reaping and janitors](#reaping-and-janitors). |
| `await wait_for_deletions(timeout_s=300.0)` | Waits until the files of the sandboxes and pods that this process stopped are deleted; returns `False` on timeout. |
| `name` | `"singularity"`, `"enroot"` or `"gvisor"`. |
| `capabilities` | A frozenset of strings; see [Capabilities](#capabilities). |
| `images` | The provider's `ImageStore`; see [Images](#images). |
| `scratch_root` | The absolute path of the [scratch root](configuration.md#the-scratch-root). |
| `sandboxes`, `pods` | Frozensets of the sandboxes and pods this provider started and has not stopped. |

`start()` validates the spec, resolves the image (pulling and staging it when needed), creates the
sandbox's session directory on the scratch root, fills its root filesystem from the image, starts
the container, and checks that the root filesystem and the working directory are writable. An
invalid spec raises `ValueError`, and an image that cannot be pulled `SandboxError`, before anything
is created. When a later step fails, the start stops what it started, hands its files to a
background deletion and raises `SandboxStartError`; a cancelled start does the same and lets
`CancelledError` through.

You can create as many providers as you need, in one process or in many, on one scratch root: each
sandbox has a session directory of its own.

## Describing a sandbox

`SandboxSpec` says what to start. Every field but `image` has a default.

| Field | Default | Meaning |
|---|---|---|
| `image` | required | The image to start from; see [Image references](#image-references). |
| `workdir` | `"/app"` | The working directory of commands, an absolute path. A start creates it when the image lacks it, with your umask, or with mode 755 in a `--fakeroot` sandbox ([Root in a sandbox](singularity.md#root-in-a-sandbox)) and a gVisor sandbox. |
| `env` | `{}` | Variables every command gets; see [Environment](#environment). |
| `mounts` | `[]` | Host paths bound into the sandbox, as `Mount` objects; see [Mounts](#mounts). |
| `network` | `"host"` | `"host"` shares the host's network. `"none"` gives the sandbox only a loopback interface, and needs the `network_none` capability. |
| `cpus` | `None` | Advisory, unless a `SingularityProvider` with `cgroup_limits=True` passes it as `--cpus`, or a `GvisorProvider` keeps the sandbox to that many CPUs, rounded up ([gVisor](gvisor.md#limits)). |
| `memory_mb` | `None` | Arms the memory watchdog; see [Memory limits](#memory-limits). A `GvisorProvider`'s stops the whole sandbox. |
| `labels` | `{}` | String pairs recorded with the sandbox. `reap()` and `hpc-sandbox janitor --label` select sandboxes by them. |
| `file_owners` | `None` | The path of a manifest of the users and groups that the image gives its files, which a sandbox whose user namespace maps other users applies before any command runs in it; see [File owners](#file-owners). |

An invalid spec raises `ValueError` before anything is created: a relative `workdir`, a `network`
other than `"host"` and `"none"`, a `memory_mb` or `cpus` that is not positive, an invalid variable
name, a mount that breaks the rules below, an image the provider can never run, or a
`file_owners` manifest that cannot be read, that is malformed, or that the provider's sandboxes
cannot apply. With
`network="host"`, the host's `/etc/resolv.conf` is bound read-only into the sandbox, unless the
provider was created with `bind_resolv_conf=False`.

### Image references

| Reference | `SingularityProvider` | `EnrootProvider` |
|---|---|---|
| `docker://repo:tag`, `docker://registry/repo:tag` | pulled as a SIF file | imported as a squashfs file |
| `docker://repo[:tag]@sha256:<hex>` | pinned to that manifest digest | pinned to that manifest digest |
| `repo:tag`, without a scheme | read as `docker://repo:tag` | read as `docker://repo:tag` |
| `oras://...`, `library://...`, `shub://...` | pulled by the runtime | `ValueError` |
| `dockerd://image`, `podman://image` | `ValueError` | imported from the local Docker or Podman daemon |
| A local file: `/path`, `./path`, `../path`, `~/path` or `file:///path` | a `.sif` file | a `.sqsh` or `.squashfs` file |
| A local file with `@sha256:<hex>` appended | the file, checked against that digest before use | the file, checked against that digest before use |
| An image of the other provider's format | `ValueError` | `ValueError` |

Every provider also accepts enroot's `registry#repo` spelling, in `docker://` references and in
references without a scheme. `GvisorProvider` takes the references that `SingularityProvider` takes,
and pulls them with Singularity CE or Apptainer.

A remote image is pulled once into the image cache. A tag keeps the image it named when it was first
pulled until you refresh it (see [Images](#images)), so pin images by digest, or use local files
pinned by content, for runs that must not change.

The image needs a `/bin/sh` with `sleep`, `mkdir`, `rmdir` and `rm`, which starting a sandbox uses,
and `cp` and `dirname`, which transfers use.

### Mounts

`Mount(source, target, read_only=True)` binds the host path `source` at `target` in the sandbox. Set
`read_only=False` for a writable bind. A spec's mounts must follow these rules, or `start()` raises
`ValueError`:

- `source` exists on the host. It is resolved, symlinks included, when the sandbox starts.
- `target` is an absolute path that does not start with `//`, without `..`, other than `/`, and
  neither `/tmp` nor `/var/tmp` nor a path below them: those are the sandbox's own.
- Neither path contains `:` or `,`. With enroot, neither contains whitespace either.

A start creates the target in the sandbox's root filesystem when the image lacks it. With
Singularity, a target whose path crosses a symlink in the image fails the start with
`SandboxStartError`, and the directories it creates get mode 755, and a file 644, whatever your
umask, as Docker makes a mount point, so that the users a command switches to can reach them.

### File owners

An image's layers give each file an owner, and Docker keeps them: in `redis:7-alpine`, `/data`
belongs to the image's user `redis`, uid 999, and its group, gid 1000. hpc-sandbox extracts every
image as your uid, which loses them: in a sandbox whose root can switch users, every file is
root's, and a command that switched to `redis` cannot write in its own data directory.
`file_owners` gives them back. It names a manifest that you write, with one line for each path that
the image gives to an owner other than root's user and group, for example from the tar headers of
the image's layers:

```text
# redis:7-alpine: some of the owners its layers give, other than 0 0
0 42 /etc/shadow
999 1000 /data
```

Each line is `UID GID PATH`: two decimal ids, then the path in the image, absolute, which runs to
the end of the line and may hold spaces, but no carriage return: a manifest with CRLF line ends
raises `ValueError` at its first entry. Blank lines and lines that start with `#` are skipped. Each
start reads the file, so it must stay readable until the start returns. Only sandboxes whose user
namespace maps other users take a manifest, and they apply it to their own root filesystem before
any command runs in them: Singularity sandboxes with `fakeroot=True`
([Root in a sandbox](singularity.md#root-in-a-sandbox)) and pod services with `run_as="subids"`
([Users](pods.md#users)). The others raise `ValueError`; so does a `GvisorProvider`, whose root
can switch users but does not apply manifests yet. Applying it:

- gives each path its uid and gid as root of the sandbox's user namespace, in the manifest's order,
  so a later line for the same path wins, and keeps a file's set-user-ID and set-group-ID bits,
  which a change of owner clears;
- never follows a symbolic link: a link gets the owner itself, and a path below a link or below a
  file, a path the root filesystem lacks and one on another filesystem name nothing. They are
  skipped, and a warning names them;
- skips the paths at or below `/dev`, `/proc`, `/sys`, `/tmp` and `/var/tmp`, which the warning
  names too: the sandbox mounts its own there, so it never sees what the image has in them;
- fails the start with `SandboxStartError`, before it changes anything, when the namespace does not
  map an id that the manifest gives, such as a uid beyond your subordinate ids.

The image, the image cache and the node's templates never change: each start applies the manifest to
its own copy. A snapshot keeps no owners, so a restore applies its spec's manifest again
([Snapshots and restores](#snapshots-and-restores)). What the manifest gives to other users belongs
to your subordinate ids on the host, and is given back to you before the sandbox's files are
deleted, with what its commands wrote as other users.

## Running commands

`await sandbox.exec(cmd, *, timeout_s=None, env=None, cwd=None, stdin=None)` runs one command in the
sandbox and returns an `ExecResult` once the command exits.

| Argument | Meaning |
|---|---|
| `cmd` | A string, run with `/bin/sh -c`, or an argv list, run as it is. |
| `timeout_s` | Seconds after which the command's process tree is killed. `None` means the provider's `default_timeout_s` (600 by default); otherwise it must be positive. |
| `env` | Variables for this call, on top of the spec's `env`. |
| `cwd` | The working directory, absolute or relative to the spec's `workdir`. `None` means `workdir`. |
| `stdin` | Bytes the command reads on its standard input. Without them, it reads `/dev/null`. |

```python
import asyncio

from hpc_sandbox import CWD_ERROR_EXIT_CODE, CWD_ERROR_PREFIX, EnrootProvider, SandboxSpec


async def main() -> None:
    provider = EnrootProvider()
    async with await provider.start(SandboxSpec(image="docker://alpine:3.20")) as sandbox:
        result = await sandbox.exec("echo $HOME $GREETING; pwd", env={"GREETING": "hi there"})
        print(result.stdout, end="")  # /root hi there, then /app
        result = await sandbox.exec(["wc", "-c"], stdin=b"12345")  # an argv list: no shell
        print(result.stdout, end="")  # 5
        result = await sandbox.exec("sleep 30", timeout_s=1)
        print(result.exit_code, result.timed_out)  # 124 True
        result = await sandbox.exec("ls", cwd="/no/such/directory")
        if result.exit_code == CWD_ERROR_EXIT_CODE and result.stderr.startswith(CWD_ERROR_PREFIX):
            print(result.stderr, end="")
    await provider.wait_for_deletions()


asyncio.run(main())
```

```text
/root hi there
/app
5
124 True
hpc-sandbox: cannot enter the working directory /no/such/directory: it does not exist or cannot be reached
```

| `ExecResult` field | Meaning |
|---|---|
| `exit_code` | The command's exit status; see [Exit codes](#exit-codes). |
| `stdout`, `stderr` | What the command printed, decoded as UTF-8 (bytes that are not UTF-8 become U+FFFD). |
| `timed_out` | `True` when the timeout killed the command. |

Several commands can run in one sandbox at once. Cancelling an `exec` call kills the command's
process tree before `CancelledError` reaches you.

### Output

Each stream is captured up to the provider's `output_limit_bytes` (16 MiB by default). Beyond that,
its head and its tail are kept, half the limit each, with a line
`[hpc-sandbox: output truncated, N bytes omitted]` between them. A command that leaves a background
process holding its output still returns when it exits.

hpc-sandbox adds lines of its own to `stderr`, each starting with `hpc-sandbox: `: the timeout note,
the memory watchdog's reports and the working directory error.

### Exit codes

| `exit_code` | Meaning |
|---|---|
| `0` to `255` | The command's own status. As usual, a shell returns `127` for a command it cannot find, and `126` for one it cannot execute. |
| `124` | The timeout killed the command: `timed_out` is `True`, and `stderr` ends with `hpc-sandbox: command timed out after N s`. |
| `126` | Also: the working directory could not be entered; see [Working directory errors](#working-directory-errors). |
| `137` | The command was killed with SIGKILL, for example by the memory watchdog, whose report is then on `stderr`. |
| `128 + N` | The command was killed by signal N. |

### Working directory errors

A working directory that is missing or cannot be entered is the sandbox's state, not a failure of
the infrastructure. As with `docker exec`, the command fails, and nothing falls back to another
directory. The result has `exit_code` equal to `CWD_ERROR_EXIT_CODE` (126), an empty `stdout`, and
on `stderr` only the line `CWD_ERROR_PREFIX + "<directory>: <reason>"`, where `CWD_ERROR_PREFIX` is
`"hpc-sandbox: cannot enter the working directory "` and the reason is one of `CWD_ERROR_REASONS`:

| Reason | When |
|---|---|
| `it does not exist or cannot be reached` | Nothing is at the path, or a directory on the way cannot be searched. |
| `not a directory` | Something other than a directory is at the path. |
| `permission denied` | The directory exists, but commands may not enter it. |

To tell this result from a command's own status 126, check that `stderr` starts with
`CWD_ERROR_PREFIX`.

### Infrastructure errors

`exec` raises instead of returning when the command could not run, so that a failure of the sandbox
never passes for a failure of the command:

- `SandboxLostError` when the sandbox's container died. Every later call raises it too.
- `SandboxError` when the command never started for another reason, such as a runtime that could
  not enter the container; when the sandbox was stopped while the command ran; and for calls on a
  stopped sandbox.
- `ValueError` for an empty command, a `timeout_s` that is not positive, or an invalid variable.

These exceptions, the working directory errors and `download()`'s `FileNotFoundError` for a missing
source are stable parts of the interface.

### Environment

A command's environment is made of, later entries winning:

1. the container's own environment, the image's `ENV` as the runtime sets it up (for a restored
   enroot sandbox, the environment recorded with the snapshot);
2. `HOME=/root`;
3. the spec's `env`;
4. the call's `env`.

Nothing else from the host's environment reaches the sandbox. The variables travel as one line of
POSIX-quoted shell ahead of the command's standard input, which a short `/bin/sh` prologue in the
container reads and exports before it runs the command. So values pass verbatim, newlines included,
and appear neither on command lines, which every user of a node can read, nor in the environment of
the host-side processes that enter the container.

Names must match `[A-Za-z_][A-Za-z0-9_]*`, and values may hold any text but NUL; anything else
raises `ValueError`. `BASHOPTS`, `BASHPID`, `BASH_VERSINFO`, `EUID`, `OPTIND`, `PPID`, `SHELLOPTS`
and `UID` are names that a shell may refuse to assign: bash keeps `UID` and `EUID` read-only, for
example, and dash takes only a number for `OPTIND`. The prologue sets them only where the image's
`/bin/sh` accepts them; otherwise it skips them without an error, and the command still runs. Any
other name that the shell refuses ends the prologue before the command starts, and the call raises
`SandboxError`.

## Transfers

`await sandbox.upload(src, dst)` copies the host file `src` to the path `dst` in the sandbox, or the
contents of the host directory `src` into the directory `dst`. `await sandbox.download(src, dst)`
copies a file or a directory out of the sandbox to the host path `dst`, and returns a
`DownloadReport` of the symlinks it left out.

```python
import asyncio
import tempfile
from pathlib import Path

from hpc_sandbox import EnrootProvider, SandboxSpec


async def main() -> None:
    provider = EnrootProvider()
    with tempfile.TemporaryDirectory() as directory:
        host = Path(directory)
        (host / "task").mkdir()
        (host / "task" / "solution.py").write_text("print('solved')\n")
        spec = SandboxSpec(image="docker://python:3.12-slim")
        async with await provider.start(spec) as sandbox:
            await sandbox.upload(host / "task", "/app/task")  # the directory's contents
            await sandbox.exec("python solution.py > out.txt; ln -s /etc/passwd leak", cwd="task")
            report = await sandbox.download("task", host / "results")  # relative to /app
        print(sorted(path.name for path in (host / "results").iterdir()))
        for link in report.dropped_links:
            print(f"left out {link.path} -> {link.target} ({link.reason})")
    await provider.wait_for_deletions()


asyncio.run(main())
```

```text
['out.txt', 'solution.py']
left out leak -> /etc/passwd (absolute)
```

Transfers go through a fresh directory in the sandbox's private `/tmp`, so they work for files of
any size that the scratch root has room for, and they see paths as commands in the sandbox do,
mounts included. They run from `/`, so they still work once the working directory is gone; relative
paths resolve from `workdir`. The copy inside the sandbox is bounded by the provider's
`default_timeout_s`. Code in the sandbox can change that directory at any moment, so the host side
works in it only through file descriptors: it never follows a symlink it finds there and never
blocks on a FIFO.

Uploads:

- A missing `src` raises `FileNotFoundError`. A symlink given as `src` is followed, and symlinks
  inside a directory are copied as symlinks. Modes and timestamps are kept.
- FIFOs, sockets and devices raise `shutil.SpecialFileError`, except a character device given as
  `src` itself, which is read to its end: `/dev/null` gives an empty file.
- A file goes to `dst`, whose parent directories are created, and replaces a file there. A file
  uploaded onto a directory raises `SandboxError`. A directory's contents are merged into `dst`,
  which is created when missing.

Downloads:

- A missing `src` raises `FileNotFoundError`. A symlink given as `src` is followed, as `cp -H` does.
- `dst` is yours, so it is treated as `cp` would treat it. Its parent directories are created, and a
  symlink at `dst` is followed. A directory's contents go into `dst`, created when missing; an
  existing `dst` that is not a directory raises `FileExistsError`. A file replaces a file at `dst`;
  a directory there raises `IsADirectoryError`, and a FIFO `shutil.SpecialFileError`.
- Timestamps and modes are kept, but not owners: inside a user namespace, files on host mounts
  belong to owners the namespace does not map. What the sandbox must not hand you is not kept
  either: setuid, setgid and sticky bits are dropped, your umask applies, and you can always read
  the copy.
- Special files in a downloaded directory, and directories nested more than 256 levels deep, raise
  `SandboxError`.
- Symlinks inside a downloaded directory are copied only if they stay inside it on their own:
  relative, climbing with leading `..` no higher than the directory, and never after a name. The
  others are left out, logged as a warning, and listed in the report as `DroppedLink` records with
  the link's `path` (relative to `dst`), its `target` text and one of these reasons:

| `reason` | The link |
|---|---|
| `absolute` | is absolute. |
| `absolute into the tree` | is absolute, and names something inside the downloaded directory as the sandbox sees it. |
| `escapes with ..` | climbs with leading `..` above the destination. |
| `climbs after a name` | has `..` after a name: `a/..` leads wherever `a` does, which a later download into the same directory could change. |

`DownloadReport.dropped_links` holds the first 100 links left out, and `dropped_total` how many
there were. Kept links can still reach what the destination held before the download, so download
into a new or empty directory to keep the sandbox's links away from your own files.

Errors on the host side stay `OSError`s, as above; failures inside the sandbox raise `SandboxError`.

## Snapshots and restores

`await sandbox.snapshot()` copies the sandbox's writable root filesystem and its private `/tmp` and
`/var/tmp` into a new directory on the scratch root, and returns a `SandboxSnapshot`.
`await provider.restore(snapshot, spec)` starts a new sandbox from those files, and
`await provider.delete_snapshot(snapshot)` deletes them.

```python
import asyncio

from hpc_sandbox import EnrootProvider, SandboxSpec


async def main() -> None:
    provider = EnrootProvider()
    spec = SandboxSpec(image="docker://alpine:3.20")
    async with await provider.start(spec) as sandbox:
        await sandbox.exec("echo prepared > /app/state")
        snapshot = await sandbox.snapshot()
    try:
        for _ in range(2):  # two independent copies of the prepared sandbox
            async with await provider.restore(snapshot, spec) as copy:
                print(copy.id, (await copy.exec("cat /app/state")).stdout, end="")
    finally:
        await provider.delete_snapshot(snapshot)
    await provider.wait_for_deletions()


asyncio.run(main())
```

```text
hpcsb-5cf9d1f57eb54267964ab2a07ac29c43 prepared
hpcsb-8e522ce924b84854b850e8142d581a09 prepared
```

- A snapshot holds files, never processes or memory. It runs `sync` in the sandbox, then copies with
  `cp -a --reflink=auto`, which is nearly free on filesystems with reflinks (XFS, Btrfs) and a full
  copy elsewhere. Files written during the copy may be captured partially, so take snapshots while
  the sandbox is quiescent.
- A snapshot's copies of the files are yours, so it keeps no owners, and a restore applies its
  spec's `file_owners` again ([File owners](#file-owners)). A Singularity sandbox with
  `fakeroot=True` is copied as root of its user namespace, which reads the files of every user
  there whatever their modes, as root inside does.
- A snapshot lives on the node that took it, in the scratch root's `snapshots` directory, until
  `delete_snapshot()` deletes it. Stops and janitors leave snapshots alone.
- A restore needs a provider of the same runtime and the same scratch root, on the same node, and a
  spec with the same `image`; anything else raises `ValueError`. The rest of the spec may differ.
  Each restore gets a copy of its own, and the snapshot stays.
- `SandboxSnapshot` is a plain dataclass, so another process on the node can restore a snapshot from
  its fields.
- A snapshot of a sandbox with Singularity's fuse-overlay layout holds only what the sandbox wrote,
  and restores over the same node's template of the image, with that layout (see
  [Singularity CE and Apptainer](singularity.md)).
- A snapshot of a gVisor sandbox holds only what the sandbox wrote, `/tmp` and `/var/tmp` included,
  as gVisor archives it, with the owners and modes of its files, and restores over the same node's
  template of the image ([gVisor](gvisor.md#snapshots-and-restores)).
- Pod services cannot be snapshotted yet: their `snapshot()` raises `SandboxError`.

| `SandboxSnapshot` field | Meaning |
|---|---|
| `id` | `snap-` and 32 hexadecimal digits. |
| `provider` | The name of the provider that took it. |
| `path` | Its directory on the node. |
| `image` | The image of the sandbox it was taken from. |
| `metadata` | `format`, `created_at` (a Unix time), `source_sandbox` (the sandbox's id), `workdir`, `labels`, and `remap_root` and `container_uid` (who commands ran as). A fuse-overlay or gVisor snapshot adds `lower`, the template it lies over. |

## Stopping and deleting

`await sandbox.stop()` kills the commands still running in the sandbox, whose `exec` calls raise
`SandboxError`, and stops the container, proving each process's identity again before it signals
it. It then moves the sandbox's session directory out of the session root, into the root's trash,
and returns. A detached `rm` deletes the directory afterwards, at the lowest CPU priority, one
deletion per session root at a time.

- `stop()` is idempotent and cancellation-safe: the teardown runs to completion, and a cancelled
  caller sees `CancelledError` only afterwards. When the teardown fails, for example because a
  process's identity cannot be proved, `stop()` raises `SandboxError` and keeps the session for a
  janitor; a later `stop()` tries again.
- While 16 of a process's deletions are pending, its stops delete their sessions themselves, and
  take that much longer.
- On a scratch root too full even to move a session, the stop deletes the session in place.

`await provider.wait_for_deletions(timeout_s=300.0)` waits until the deletions this process started
have finished, those of every provider in the process, and returns `True`; it returns `False` when
`timeout_s` passes first. Detached deletions outlive your program, but not the Slurm step or batch
job they run in, whose end kills them and leaves what they had not deleted to a janitor. So call it:

- before your program exits, inside a batch job;
- before you remove a scratch root yourself, since the deletions are still removing files from it.

`hpc-sandbox run` and `hpc-sandbox doctor` wait for their own deletions.

## Reaping and janitors

A session is abandoned when no process holds its lease and the process that started it is gone: its
owner crashed or was killed without stopping its sandboxes. Janitors stop what still runs for such
sessions and delete them.

`await provider.reap(labels=None)` is the janitor of a provider:

1. With `labels`, it first stops this provider's own sandboxes and pods whose labels include every
   pair in `labels`. An empty mapping matches all of them.
2. Then it sweeps the abandoned sessions of every process on this node in this provider's session
   roots, only those whose labels include `labels` when it is given. Sessions of live processes
   are never touched.
3. It returns a `JanitorReport`.

```python
import asyncio

from hpc_sandbox import EnrootProvider, SandboxSpec


async def main() -> None:
    provider = EnrootProvider()
    for worker in range(3):
        labels = {"run": "exp1", "worker": str(worker)}
        await provider.start(SandboxSpec(image="docker://alpine:3.20", labels=labels))
    report = await provider.reap({"run": "exp1"})  # stops the three, and sweeps leftovers
    print(len(report.stopped), report.errors)  # 3 []
    await provider.wait_for_deletions()


asyncio.run(main())
```

| `JanitorReport` field | Holds |
|---|---|
| `stopped` | The ids of the sandboxes and pods whose processes it stopped. |
| `removed` | The ids of the sessions it deleted. |
| `live` | The ids of the sessions it left alone because a live process holds them. |
| `errors` | One message for each thing it could not do. The sessions concerned are kept. |

- A sweep needs the janitor lock of each session root, and `reap()` waits for it. A provider also
  sweeps at its first start that finds the lock free, and a start never waits for it. That sweep
  stops every abandoned session and moves it into the trash, but deletes at most 64 trash entries:
  `reap()` and `hpc-sandbox janitor` delete the rest.
- Singularity instances whose owner died keep running until a janitor stops them. enroot sandboxes
  die with their owner, and a janitor only deletes their files, so they usually appear in `removed`
  but not in `stopped`.
- A janitor fails closed: whenever process identities disagree, it reports the session in `errors`
  and leaves it. It never deletes through a mount point.
- A janitor also takes over the trash entries that no deletion holds: at once when the process that
  moved them there died, a minute later when that process still lives.
- `hpc-sandbox janitor` runs the same sweep from the command line, for every runtime it can; run it
  at the end of your batch script (see [Command line](cli.md#janitor)).

## Memory limits

With `SandboxSpec.memory_mb`, a watchdog samples the proportional set size (PSS) of the sandbox's
processes and of every running command, every `memory_poll_s` seconds (a provider option, one by
default). When their sum exceeds the limit, it kills one branch of processes, never the sandbox's
own:

- It starts from the heaviest of the running commands and of the processes they left running in the
  background, and descends into the child whose death frees the most, while that child alone frees
  the excess over the limit. So the command that ran away dies, and a harness survives a tool
  command it started, whatever their sizes.
- A fork copy of its parent, such as a `multiprocessing` fork worker, with no fork copies of its own
  counts only its private memory towards what killing its branch frees, since the pages it shares
  with its parent stay.
- A process whose `oom_score_adj` is higher than its parent's (`choom -n 1000 -- cmd`) goes first
  when what it holds, plus a thousandth of the limit per point, outweighs the alternatives, as with
  the kernel's OOM killer.
- The branch dies whole, with what it forks meanwhile.

[Architecture](architecture.md) gives the complete rules. This is a sampled guard, not a cgroup:
usage can overshoot between samples, and files in the sandbox's own tmpfs mounts (`/dev/shm`,
`/run` and the like) hold memory that no kill frees.

A command that the watchdog killed returns 137. A command whose descendants it killed keeps its own
exit status, or times out. Either way the watchdog's report is appended to the command's `stderr`
and recorded in `sandbox.memory_kills`.

```python
import asyncio

from hpc_sandbox import EnrootProvider, SandboxSpec


async def main() -> None:
    provider = EnrootProvider()
    spec = SandboxSpec(image="docker://python:3.12-slim", memory_mb=256)
    async with await provider.start(spec) as sandbox:
        hog = "import time; data = b'x' * (512 << 20); time.sleep(60)"
        result = await sandbox.exec(["python", "-c", hog])
        print(result.exit_code)  # 137
        print(result.stderr, end="")  # hpc-sandbox: memory limit exceeded (...)
        for kill in sandbox.memory_kills:
            print(kill.pid, kill.processes, kill.pss_bytes, kill.limit_bytes)
    await provider.wait_for_deletions()


asyncio.run(main())
```

```text
137
hpc-sandbox: memory limit exceeded (PSS 555880448 > 268435456 bytes); killed pid 1872743 using 545747968 bytes; tmpfs files hold 0 bytes
1872743 1 545747968 268435456
```

`memory_kills` is a list of `hpc_sandbox.memory.MemoryKill` records, oldest first. It is an
attribute of the concrete sandbox classes (`SingularitySandbox`, `EnrootSandbox`, `GvisorSandbox`
and pod services), not of the `Sandbox` protocol.

A gVisor sandbox's processes run on gVisor's kernel, not as host processes, so its watchdog cannot
pick one: it samples the memory that gVisor's kernel, the Sentry, holds for the sandbox (its memory
files and its own memory, not PSS), and stops the whole sandbox when that exceeds the limit. The
running command and every later call then raise `SandboxLostError`, whose message is the
watchdog's report ("memory limit exceeded (gVisor's memory ...)"), and `memory_kills` records the
kill, with the Sentry's PID and `measure="gVisor's memory"` ([gVisor](gvisor.md#limits)).

| `MemoryKill` field | Meaning |
|---|---|
| `pid` | The host PID of the root of the killed branch. |
| `pss_bytes` | The PSS the branch held. |
| `total_bytes` | The PSS the whole sandbox held. |
| `limit_bytes` | The limit: `memory_mb` in bytes. |
| `exec_root` | The host PID of the process an `exec` call started, when the branch was in its tree; `None` for processes left running in the background. |
| `processes` | How many processes the branch held. |
| `tmpfs_bytes` | What files on the sandbox's tmpfs mounts held, or `None` when unknown. |
| `pids` | The host PIDs of the branch's processes as sampled, root first. |
| `measure` | What `pss_bytes` and `total_bytes` measure: `"PSS"`, or `"gVisor's memory"` for a gVisor sandbox, where both are what gVisor held for the whole sandbox. |

`kill.message()` returns the line appended to `stderr`.

## Images

`provider.images` is the provider's `ImageStore`. It resolves references to image files, pulls
remote images into the image cache once, and stages them to the scratch root. Starts use it on
their own; call it yourself to pull ahead of time, or to refresh a tag that moved.

```python
import asyncio

from hpc_sandbox import EnrootProvider


async def main() -> None:
    provider = EnrootProvider()
    image = await provider.images.pull("docker://alpine:3.20", refresh=True)
    print(image.path, image.sha256)


asyncio.run(main())
```

| Method | What it does |
|---|---|
| `await images.pull(reference, refresh=False)` | Returns the cached image of a remote reference as a `ResolvedImage`, pulling it unless it is cached, or checks a local image file. With `refresh=True` it pulls a tag again: sandboxes started afterwards get the new image, running ones keep theirs, and starts never wait for a refresh. `refresh` changes nothing for digest-pinned and local references, which always name the same image. |
| `await images.resolve(reference)` | What a start does: `pull()`, then staging the file to the scratch root when staging is on. |

A `ResolvedImage` has the `reference`, the `path` of the image file, and `sha256`, the file's
SHA-256 (`None` when unknown). A pull that still fails after three attempts raises `SandboxError`.
`hpc-sandbox pull` does the same from the command line, and
[The image cache](configuration.md#the-image-cache) describes where the files go and how to prune
them. `SifImages` and `SquashfsImages` are the two image formats, and `ImageFormat` is their
protocol.

## Pods

`await provider.start_pod(PodSpec(...))` starts a pod: services that share one network, like a
docker-compose project. It returns a `SessionPod`, whose `start_service(ServiceSpec(...))` returns a
service: a `SingularityService` or an `EnrootService`, both `PodService` sandboxes with `exec`,
`upload` and `download`. `HealthCheck`, `PodNetwork`, `PodUsers`, `ServiceHealth` and `ServiceRunAs`
complete the pod API. [Pods](pods.md) describes all of it.

## Capabilities

`provider.capabilities` says what a provider can do:

| Capability | Present when |
|---|---|
| `snapshot` | Always: `snapshot()` and `restore()` work. |
| `isolation` | Always: each sandbox has private PID and IPC namespaces, root filesystem, `/tmp` and environment. |
| `pods` | With Singularity and enroot: `start_pod()` works. |
| `network_none` | With Singularity, when `allow_network_none=True`; with enroot, unless `allow_network_none=False`; always with gVisor. |
| `gpu` | With Singularity, when `gpu` is set; with enroot, when `gpus` (or `gpu="nv"`) is set. |

Isolation keeps sandboxes apart from one another and from the host's files and processes. It is not
a security boundary against hostile code: commands run as your uid, on the host's kernel and, by
default, on the host's network; see the [Security model](security.md).

## Protocols

`SandboxProvider`, `Sandbox`, `PodProvider` and `Pod` are structural protocols, defined in
`hpc_sandbox.types`. Code typed against them works with either provider, and adapters to other
sandbox interfaces need only their names.

| Protocol | Members |
|---|---|
| `SandboxProvider` | `name`, `capabilities`, `start(spec)`, `restore(snapshot, spec)` |
| `Sandbox` | `id`, `exec(cmd, *, timeout_s, env, cwd, stdin)`, `upload(src, dst)`, `download(src, dst)`, `snapshot()`, `stop()` |
| `PodProvider` | `name`, `capabilities`, `start_pod(spec)` |
| `Pod` | `id`, `routes`, `services`, `start_service(spec, wait)`, `health(name)`, `logs(name)`, `set_network(mode)`, `stop()` |

They are runtime-checkable: `isinstance()` works with them, checking names only.
`delete_snapshot()`, `reap()`, `wait_for_deletions()`, `images`, `memory_kills`, a pod's
`endpoints`, `expose()` and `open_connection()`, and `async with` belong to the concrete classes,
not to the protocols.

```python
import asyncio

from hpc_sandbox import EnrootProvider, SandboxProvider, SandboxSpec


async def kernel_release(provider: SandboxProvider, image: str) -> str:
    """Works with any provider: it uses only the protocols' names."""
    sandbox = await provider.start(SandboxSpec(image=image))
    try:
        return (await sandbox.exec(["uname", "-r"])).stdout.strip()
    finally:
        await sandbox.stop()


async def main() -> None:
    provider = EnrootProvider()
    print(await kernel_release(provider, "docker://alpine:3.20"))
    await provider.wait_for_deletions()


asyncio.run(main())
```

## Exceptions

| Exception | Raised when |
|---|---|
| `SandboxError` | The sandbox infrastructure failed, as opposed to the command in it: a runtime that cannot be run or cannot enter the container, a pull that failed, a teardown that could not prove a process's identity. A subclass of `RuntimeError`. |
| `SandboxStartError` | A sandbox, a pod or a pod service could not start. What it started has been stopped, and its files are being deleted in the background. A subclass of `SandboxError`. |
| `SandboxLostError` | The container of a running sandbox died, or the holder of a pod. A subclass of `SandboxError`. |
| `ValueError` | A spec, an option or an argument is invalid, or an image is one the provider can never run. Raised before anything is created. |
| `OSError` | A problem on the host's side: a transfer's host path (`FileNotFoundError`, `FileExistsError`, `IsADirectoryError`, `shutil.SpecialFileError`, ...), or a scratch root or image cache that cannot be written. |

`start()` raises `SandboxError` when the image cannot be pulled, and `SandboxStartError` when the
sandbox then fails to start; catch `SandboxError` for both.

## Logging

hpc-sandbox logs with the standard `logging` module, through loggers named after its modules
(`hpc_sandbox.sandbox`, `hpc_sandbox.memory` and so on). It logs a warning when the memory watchdog
kills, when a download leaves symlinks out, and when a deletion or a sweep fails. Without any
logging configuration, Python prints warnings and errors on stderr;
`logging.getLogger("hpc_sandbox").setLevel(logging.ERROR)` keeps the warnings out.

## Public API and versioning

| Area | Names in `hpc_sandbox` |
|---|---|
| Providers | `SingularityProvider`, `EnrootProvider`, `GvisorProvider` |
| Sandboxes | `SingularitySandbox`, `EnrootSandbox`, `GvisorSandbox` |
| Specs | `SandboxSpec`, `Mount` |
| Results | `ExecResult`, `DownloadReport`, `DroppedLink`, `SandboxSnapshot`, `JanitorReport` |
| Error contract | `CWD_ERROR_EXIT_CODE`, `CWD_ERROR_PREFIX`, `CWD_ERROR_REASONS`, `SandboxError`, `SandboxStartError`, `SandboxLostError` |
| Protocols | `SandboxProvider`, `Sandbox`, `PodProvider`, `Pod` |
| Pods | `PodSpec`, `ServiceSpec`, `HealthCheck`, `PodNetwork`, `PodUsers`, `ServiceHealth`, `ServiceRunAs`, `SessionPod`, `PodService`, `SingularityService`, `EnrootService` |
| Images | `ImageStore`, `ImageFormat`, `ResolvedImage`, `SifImages`, `SquashfsImages` |
| Runtimes | `ContainerRuntime`, `EnrootRuntime` and `RunscRuntime`: the runtime command lines a provider runs, as its `runtime` attribute (a `GvisorProvider`'s `ContainerRuntime`, which pulls and extracts its images, is its `image_runtime`) |

The public API is the names in `hpc_sandbox.__all__`, listed above, with the behaviour these docs
describe, and the command-line output that [Command line](cli.md) documents. Other names, such as
those of the module `hpc_sandbox.sessions` and names that start with `_`, may change in any release;
the `MemoryKill` records in `memory_kills` keep the fields this page documents. The files on the
scratch root and in the image cache belong to hpc-sandbox: [Configuration](configuration.md)
describes them so that you can place, size and clean them, and a minor release may change them.

hpc-sandbox's version is below 1.0: a minor release (0.4.0) may change the public API, and a patch
release (0.3.1) only fixes bugs. To stay on one minor release, require a range such as
`hpc-sandbox>=0.6,<0.7`. [CHANGELOG.md](../CHANGELOG.md) lists what each release changes, and
`hpc_sandbox.__version__` holds the version of the running copy: the one that `pyproject.toml` sets
when it runs from a source checkout, else the installed distribution's.

## Next steps

- [Command line](cli.md): `doctor`, `run`, `pull`, `fetch-tools` and `janitor`.
- [Configuration](configuration.md): every provider option, where files go, and the environment
  variables.
- [Pods](pods.md): services that share one network.
- [Architecture](architecture.md): how a sandbox is built and torn down.
- [Security model](security.md): what isolation means here, and what it does not.
