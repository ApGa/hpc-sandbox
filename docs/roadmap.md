# Roadmap

hpc-sandbox does not do these things yet. Each row says what happens today instead.
[CHANGELOG.md](../CHANGELOG.md) lists what each release added.

| Area | Not built yet | Today |
|---|---|---|
| templates | evicting templates | every image version used on a node keeps its extracted template in `templates/` on scratch until you remove it or the scratch root goes ([Singularity CE and Apptainer](singularity.md#site-notes)) |
| enroot | an overlay layout | every enroot sandbox extracts its own root filesystem with `enroot create`, which is the copy layout |
| root filesystems | an ext3 overlay mode, as an alternative to root filesystem directories | root filesystems are directories on node-local scratch, or fuse-overlay mounts over a directory |
| snapshots | memory snapshots | snapshots hold files (the root filesystem and the private `/tmp` and `/var/tmp`), never processes or memory |
| snapshots | keeping the owners of files | a snapshot's copies of the files are yours: a restore gives the image's paths their owners again from its spec's `file_owners`, and what commands wrote as other users is root's in the restored sandbox ([Snapshots and restores](python-api.md#snapshots-and-restores)) |
| sandboxes | separate PID namespaces for groups of commands inside one sandbox | each sandbox has one PID namespace, shared by all its commands |
| runtimes | a gVisor provider: sandboxes under rootless `runsc`, each with a user-space kernel of its own, where root holds every capability (mounts, namespaces, raw sockets, nested Docker) without holding any on the host | sandboxes share the host's kernel, and root in them is root of a user namespace that maps your uid ([Security model](security.md#what-isolation-does-not-cover)) |
| images | reading the owners of an image's files from its layers | extraction gives every file to your uid; a manifest that you write gives them their owners, in sandboxes whose user namespace maps other users ([File owners](python-api.md#file-owners)) |
| pods | snapshots of services | `snapshot()` on a pod service raises `SandboxError` |
| pods | GPUs | services get no GPUs |
| pods | services that switch to other users on enroot, or without subordinate ids | enroot maps one id into a container, and a pod without `users="subids"` maps only your uid: their services run as root or as your uid ([Users](pods.md#users)) |
| pods | host networking | a pod's network is loopback only, or egress through slirp4netns |
| pods | egress allowlists | egress reaches every host that slirp4netns can reach, except the host's loopback |

## Next steps

- [Architecture](architecture.md): how sandboxes work today.
- [Pods](pods.md): what pods do today.
- [Contributing](../CONTRIBUTING.md): how to propose a change.
