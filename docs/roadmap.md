# Roadmap

hpc-sandbox does not do these things yet. Each row says what happens today instead.
[CHANGELOG.md](../CHANGELOG.md) lists what each release added.

| Area | Not built yet | Today |
|---|---|---|
| templates | evicting templates | every image version used on a node keeps its extracted template in `templates/` on scratch until you remove it or the scratch root goes ([Singularity CE and Apptainer](singularity.md#site-notes)) |
| enroot | an overlay layout | every enroot sandbox extracts its own root filesystem with `enroot create`, which is the copy layout |
| root filesystems | an ext3 overlay mode, as an alternative to root filesystem directories | root filesystems are directories on node-local scratch, or fuse-overlay mounts over a directory |
| snapshots | memory snapshots | snapshots hold files (the root filesystem and the private `/tmp` and `/var/tmp`), never processes or memory |
| sandboxes | separate PID namespaces for groups of commands inside one sandbox | each sandbox has one PID namespace, shared by all its commands |
| pods | snapshots of services | `snapshot()` on a pod service raises `SandboxError` |
| pods | GPUs | services get no GPUs |
| pods | services that switch to other users | a pod maps one id, your uid, to root; more users would need subordinate id ranges |
| pods | host networking | a pod's network is loopback only, or egress through slirp4netns |
| pods | egress allowlists | egress reaches every host that slirp4netns can reach, except the host's loopback |

## Next steps

- [Architecture](architecture.md): how sandboxes work today.
- [Pods](pods.md): what pods do today.
- [Contributing](../CONTRIBUTING.md): how to propose a change.
