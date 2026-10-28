# Security model

hpc-sandbox keeps a sandbox's processes, files and environment apart from the rest of your work on
the node, so that the code you run in it, such as an agent's commands or a program under evaluation,
cannot disturb the process that runs it or the node by accident. It is not a security boundary
against hostile code: a sandbox runs as your uid, or as root mapped to your uid, on the host's
kernel and, by default, on the host's network. A gVisor sandbox puts gVisor's kernel between its
code and the host's ([gVisor sandboxes](#gvisor-sandboxes)). This page says what isolation covers,
what it does not, and which problems to report as vulnerabilities.

## What isolation covers

Every provider has the `isolation` capability. Each sandbox gets:

| Isolated | How |
|---|---|
| processes | private PID and IPC namespaces: commands see only the sandbox's own processes |
| root filesystem | a private, writable copy or overlay of the image, in a session directory of mode 0700 on node-local scratch |
| `/tmp` and `/var/tmp` | private directories of the session, or, with gVisor, part of the sandbox's overlay |
| environment | the host's environment never reaches a command, which gets the image's environment, `HOME=/root`, the spec's `env` and the call's `env` |
| host files | only what the runtime mounts into every container as the site configures it (such as `/proc`, `/sys` and `/dev`), the host's `/etc/resolv.conf` with host networking and, on enroot, its `/etc/hosts`, both read-only, and the `mounts` you give, read-only unless you ask otherwise; never your home directory, your working directory or the site's bind paths |
| network, with `network="none"` | a network namespace whose only interface is loopback: always on enroot, unless `allow_network_none=False`, on Singularity with `allow_network_none=True`, where the site lets users create network namespaces, and always on gVisor, whose own network stack it is |

The services of a [pod](pods.md) share a network namespace of their own: loopback only, or egress
through slirp4netns, which never reaches the host's loopback but does reach what the node's network
reaches, including services that listen on the node's own network addresses. From the host, only
the ports you expose reach that network, each through a Unix socket in a directory of mode 0700 in
the pod's session ([Exposed ports](pods.md#exposed-ports)): what runs as your uid on the node can
connect to them, other users cannot. Any service of the pod can answer on an exposed port, not only
the one you mean to reach, since all of them share the pod's loopback as your uid: trust what a
host client reads there no more than the pod's least trusted service.

## What isolation does not cover

- **Your uid.** Commands run as your uid, or as root in a user namespace that maps root to your uid
  (on enroot by default, and in pod services). The kernel treats them as you: what they create on
  mounts belongs to you, they count against your process and quota limits, and they can use whatever
  your uid can reach through their mounts and the network. Anything else that runs as your uid on
  the node, other jobs included, can read and change a sandbox's files and signal its processes.
- **Your subordinate ids.** With Singularity's `fakeroot` and in pods with `users="subids"`, root
  can switch to the users of your subordinate ids. The kernel treats them as other users of the
  node: a command that switched reaches your files only as far as their modes let others, and what
  it writes, like the image's files that a file owners manifest gives to such a user, belongs to
  that id until hpc-sandbox gives it back to you, before deleting it. Such namespaces allow
  `setgroups`, so their root can also leave your groups, and reach what a mode grants others but
  denies one of your groups.
- **The kernel.** Sandboxes of Singularity and enroot share the host's kernel, so a kernel
  vulnerability, including one that unprivileged user namespaces expose, is a way out. A gVisor
  sandbox's code reaches the host's kernel only through gVisor
  ([gVisor sandboxes](#gvisor-sandboxes)). Apart from the `ulimits` of pod services, hpc-sandbox
  does not limit the processes, CPU time, disk space or inodes a sandbox uses: `cpus` is advisory
  unless Singularity's `cgroup_limits` applies it, or gVisor keeps the sandbox to that many CPUs,
  and a sandbox can fill the scratch filesystem.
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

## gVisor sandboxes

A sandbox of `GvisorProvider` ([gVisor](gvisor.md)) adds a boundary that the other providers do not
have: its processes run on gVisor's kernel, the Sentry, which serves their system calls, and do not
make system calls to the host's kernel themselves.

- **Root stays in the sandbox.** Root in a gVisor sandbox holds every capability, but towards the
  Sentry: its mounts, namespaces, users, traced processes and firewall rules are gVisor's, not the
  host's. The users that a sandbox switches to exist only in the Sentry; on the host, every process
  and every file of the sandbox is your uid's.
- **The host's kernel is further away.** The Sentry runs as your uid, in a user namespace that
  rootless runsc makes, under a seccomp filter that leaves it a small part of the host's system
  calls, and reaches the sandbox's files through runsc's gofer or the files that runsc opened for
  it. A way out of a gVisor sandbox needs a flaw in gVisor first, and then holds what the Sentry
  holds: your uid on the host, as a sandbox of the other providers does.

What gVisor does not change:

- **Your uid, your mounts and the network.** Mounts are your files, read and written as your uid as
  with the other providers, and `network="host"` reaches what the node reaches, its loopback
  included, through the host's network stack. `host_uds` lets the sandbox connect to the Unix
  sockets on its mounts (`"open"`), or create ones that host processes connect to (`"create"`), as
  your uid.
  `net_raw` lets root in the sandbox craft packets on its network, which is gVisor's own with
  `network="none"`.
- **Memory and disk.** The memory limit is still sampled, and stops the whole sandbox
  ([Limits](gvisor.md#limits)); what the sandbox writes to its root filesystem fills the scratch
  filesystem, unless `overlay_size_mb` caps it. That cap leaves out the transfer directory, which
  is on scratch too, and read-write mounts: they are directories of the host.
- **Whose promise it is.** hpc-sandbox's guarantees are those of its own host-side code, below.
  The boundary between a sandbox and the host's kernel is gVisor's: report a way out of gVisor to
  gVisor.

The provider keeps its host side to the same rules as the others, and:

- runsc runs with a minimal environment, a fixed `PATH` and none of your variables, and each
  sandbox gives runsc a private state directory in its session (`--root`), so runsc's commands for
  one sandbox never see another; container IDs are the sandboxes' fixed-length IDs, since runsc
  accepts unique prefixes of them;
- runsc never writes to the template that is a sandbox's lower layer: gVisor's overlay keeps the
  sandbox's writes in a file of its session, and runsc is given only mount points that the
  template has, so its gofer never creates one there; the provider adds its own mount points to
  its gVisor templates only, which no other provider uses
  ([Root filesystem](gvisor.md#root-filesystem));
- a bind that the sandbox makes at its target from one of those mount points stays read-only on
  the host when the mount is, whatever root in the sandbox remounts;
- a restore's archive was written by gVisor from a sandbox's files, and only gVisor reads it, inside
  the new sandbox;
- a stop or a janitor signals only processes of your uid whose recorded identity (PID and start
  time) still matches and whose command line names the sandbox, or whose arguments name the
  session's own state directory.

### gVisor pod services

A pod service with `runtime="gvisor"` ([gVisor services](pods.md#gvisor-services)) gets what a
gVisor sandbox gets, in the pod's network:

- **No hold on the pod's network.** Its launcher runs runsc in a user namespace nested in the
  pod's, which maps root alone, as a Singularity service's runtime runs: gVisor's kernel, and so
  root in the service, holds no capability over the pod's network namespace. It can neither
  reconfigure that network nor open raw or packet sockets there, even with `net_raw`, so it cannot
  watch the other services' traffic. Without that namespace, runsc would start gVisor's kernel in
  the pod's user namespace, which owns the network, and the service could do both: hpc-sandbox
  always nests it.
- **The shared loopback stays.** Its sockets are on the pod's loopback: the other services reach
  every port it listens on, a judge's administration ports included, and it reaches theirs, as for
  any two services of a pod ([Exposed ports](pods.md#exposed-ports)).
- **`securebits="ignore"`** weakens what a program in the service locks in with securebits, inside
  gVisor's kernel only: a process that runs as root, or runs a set-user-ID-root file, can regain
  capabilities that the bits would have kept dropped, unless it set `no_new_privs`, as go-judge
  does for its programs. The boundary between gVisor and the host does not change
  ([Securebits](pods.md#securebits)).

## Reducing exposure

- Use `network="none"` for code that needs no network, or a pod whose `upstreams` relay only the
  host services the code needs, and that exposes only the ports the host has to reach.
- Mount only what the code needs, and keep mounts read-only, the default.
- Set `memory_mb`, and leave the node some headroom above it.
- Keep the image cache where only you can write, and pin images by digest.
- Give each job its own scratch root on job-local disk, so that what a job's sandboxes leave goes
  with the job.
- For code that asks for root, mounts or other kernel features, prefer a
  [gVisor](gvisor.md) sandbox, or a [gVisor service](pods.md#gvisor-services) in a pod, to granting
  them on the host's kernel. For code you trust little, a pod can run every service in gVisor
  ([Every service in gVisor](pods.md#every-service-in-gvisor)).
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
enroot, gVisor, fuse-overlayfs, slirp4netns) to their projects.

Report a vulnerability privately, as [SECURITY.md](../SECURITY.md) describes: through the
repository's [Security tab](https://github.com/ApGa/hpc-sandbox/security), never in a public issue.
[SECURITY.md](../SECURITY.md) also says what to include, which versions get fixes and what happens
after you report.

## Next steps

- [Security policy](../SECURITY.md): how to report a vulnerability, and what happens next.
- [Architecture](architecture.md): how sandboxes are built, run and torn down.
- [Singularity CE and Apptainer](singularity.md), [enroot](enroot.md) and [gVisor](gvisor.md):
  what each runtime adds.
- [Python API](python-api.md): `SandboxSpec`, with its network, mounts and memory limit.
- [Pods](pods.md): services that share a network of their own.
