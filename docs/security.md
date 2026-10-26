# Security model

hpc-sandbox keeps a sandbox's processes, files and environment apart from the rest of your work on
the node, so that the code you run in it, such as an agent's commands or a program under evaluation,
cannot disturb the process that runs it or the node by accident. It is not a security boundary
against hostile code: a sandbox runs as your uid, or as root mapped to your uid, on the host's
kernel and, by default, on the host's network. This page says what isolation covers, what it does
not, and which problems to report as vulnerabilities.

## What isolation covers

Both providers have the `isolation` capability. Each sandbox gets:

| Isolated | How |
|---|---|
| processes | private PID and IPC namespaces: commands see only the sandbox's own processes |
| root filesystem | a private, writable copy or overlay of the image, in a session directory of mode 0700 on node-local scratch |
| `/tmp` and `/var/tmp` | private directories of the session |
| environment | the host's environment never reaches a command, which gets the image's environment, `HOME=/root`, the spec's `env` and the call's `env` |
| host files | only what the runtime mounts into every container as the site configures it (such as `/proc`, `/sys` and `/dev`), the host's `/etc/resolv.conf` with host networking and, on enroot, its `/etc/hosts`, both read-only, and the `mounts` you give, read-only unless you ask otherwise; never your home directory, your working directory or the site's bind paths |
| network, with `network="none"` | a network namespace whose only interface is loopback: always on enroot, unless `allow_network_none=False`, and on Singularity with `allow_network_none=True`, where the site lets users create network namespaces |

The services of a [pod](pods.md) share a network namespace of their own: loopback only, or egress
through slirp4netns, which never reaches the host's loopback but does reach what the node's network
reaches, including services that listen on the node's own network addresses.

## What isolation does not cover

- **Your uid.** Commands run as your uid, or as root in a user namespace that maps root to your uid
  (on enroot by default, and in pod services). The kernel treats them as you: what they create on
  mounts belongs to you, they count against your process and quota limits, and they can use whatever
  your uid can reach through their mounts and the network. Anything else that runs as your uid on
  the node, other jobs included, can read and change a sandbox's files and signal its processes.
- **Your subordinate ids.** With Singularity's `fakeroot` and in pods with `users="subids"`, root
  can switch to the users of your subordinate ids. The kernel treats them as other users of the
  node: a command that switched reaches your files only as far as their modes let others, and what
  it writes belongs to that id until hpc-sandbox gives it back to you, before deleting it. Such
  namespaces allow `setgroups`, so their root can also leave your groups, and reach what a mode
  grants others but denies one of your groups.
- **The kernel.** Sandboxes share the host's kernel, so a kernel vulnerability, including one that
  unprivileged user namespaces expose, is a way out. Apart from the `ulimits` of pod services,
  hpc-sandbox does not limit the processes, CPU time, disk space or inodes a sandbox uses: `cpus` is
  advisory unless Singularity's `cgroup_limits` applies it, and a sandbox can fill the scratch
  filesystem.
- **The network.** `network="host"` is the default: a sandbox then reaches whatever the node
  reaches, including the services that listen on the node's loopback interface (yours and other
  users'), abstract Unix sockets, which belong to the network namespace, the cluster's network and
  the internet. A service that trusts connections from your uid trusts the sandbox too.
- **Mounts.** A sandbox can read everything you mount into it, and change what you mount writable
  (`read_only=False`).
- **Images.** hpc-sandbox trusts images the way the runtime does. For example, enroot reads an
  image's `/etc/fstab` from the host side when a container starts, so an image can bind host paths
  that you can read into its own container; restores, whose files code in a sandbox wrote, are
  guarded against this ([enroot](enroot.md#snapshots-and-restores)). A digest pin protects you from
  a tag that moves, not from someone who can write to your image cache: whoever can write to a
  shared cache can change what your sandboxes run.
- **Memory.** The memory limit is sampled, not enforced: usage can overshoot between two samples, so
  a fast allocation can exhaust the node's memory before the
  [watchdog](architecture.md#the-memory-watchdog) acts. Files on a sandbox's tmpfs mounts hold
  memory that no kill frees.
- **What you do with downloaded files.** A download leaves out symlinks that could lead outside the
  downloaded tree, and drops setuid, setgid and sticky bits, but the files are still the sandbox's:
  what you run or read from them is up to you.

## Reducing exposure

- Use `network="none"` for code that needs no network, or a pod whose `upstreams` relay only the
  host services the code needs.
- Mount only what the code needs, and keep mounts read-only, the default.
- Set `memory_mb`, and leave the node some headroom above it.
- Keep the image cache where only you can write, and pin images by digest.
- Give each job its own scratch root on job-local disk, so that what a job's sandboxes leave goes
  with the job.
- For code you do not trust at all, run it under an account that holds nothing else of value, or
  behind a stronger boundary, such as a virtual machine.

## How hpc-sandbox protects the host side

The host-side code works next to files and processes that code in a sandbox controls, and is written
to stay correct whatever that code does:

- **Files.** Session directories, root filesystems and the private `/tmp` are host directories that
  a sandbox can change at any moment. Host-side code works in them only through directory file
  descriptors, never follows a symlink it finds there, never blocks on a FIFO, refuses special files
  and trees nested deeper than 256 levels, and never deletes through a mount point or a directory
  owned by another uid. Downloads drop setuid, setgid and sticky bits and leave out symlinks that
  could lead outside the copy ([Transfers](architecture.md#transfers)).
- **Environment.** Variables reach commands through stdin: never on a command line, which every user
  of the node can read, and never in the environment of host-side processes such as `nsenter`. The
  Singularity provider removes the runtime's implicit binds, `SINGULARITYENV_*` and
  `APPTAINERENV_*`, and the variables the runtime passes into containers despite `--cleanenv`.
- **Processes.** A PID alone is not an identity. Everything that signals a process it did not just
  start first proves the process's identity: its PID and start time and, depending on what it
  signals, its parent, a command line that names the sandbox, or, for a Singularity instance, the
  runtime's record and the PID file. When identities disagree, stops and janitors fail closed and
  keep the session.
- **Restores.** A restored root filesystem was written by code in a sandbox, so enroot neither
  mounts its `/etc/fstab` nor loads its `/etc/environment` from the host side, and the hooks that
  write into it from the host do not run.
- **Cleanup.** Janitors touch only private sessions of your uid that their owner abandoned and that
  belong to their own provider. Nothing uses wildcards or `instance stop --all`, which would stop
  other jobs' instances.

## Reporting a vulnerability

Report as a vulnerability a problem that breaks one of these properties:

- **Host-side code fooled by files a sandbox controls**: a transfer, snapshot, restore, stop or
  janitor that reads, writes, changes the mode of or deletes something outside the sandbox, or that
  hangs, because of symlinks, FIFOs, special files or deep trees that code in the sandbox created.
- **Environment values leaking onto command lines**, where every user of the node can read them, or
  into host-side processes.
- **Processes signalled without a proven identity**: a stop, a janitor or the memory watchdog that
  signals a process that is not the sandbox's.
- **Deleting or unmounting what is not the sandbox's**: another session, another user's or another
  provider's files, or anything through a mount point.

The limits listed under [What isolation does not cover](#what-isolation-does-not-cover) are out of
scope: what code in a sandbox can do as your uid, through the shared kernel, over the host's
network, through the mounts you give it or with memory between two samples, and what an image does
when the runtime runs it. Report problems of the runtimes themselves (Singularity CE, Apptainer,
enroot, fuse-overlayfs, slirp4netns) to their projects.

Report a vulnerability privately, as [SECURITY.md](../SECURITY.md) describes: through the
repository's [Security tab](https://github.com/ApGa/hpc-sandbox/security), never in a public issue.
[SECURITY.md](../SECURITY.md) also says what to include, which versions get fixes and what happens
after you report.

## Next steps

- [Security policy](../SECURITY.md): how to report a vulnerability, and what happens next.
- [Architecture](architecture.md): how sandboxes are built, run and torn down.
- [Singularity CE and Apptainer](singularity.md) and [enroot](enroot.md): what each runtime adds.
- [Python API](python-api.md): `SandboxSpec`, with its network, mounts and memory limit.
- [Pods](pods.md): services that share a network of their own.
