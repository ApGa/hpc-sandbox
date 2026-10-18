# Security policy

hpc-sandbox keeps a sandbox's processes, files and environment apart from the rest of your work,
but its sandboxes are not a security boundary against hostile code: they run as your uid, or as root
mapped to your uid, on the host's kernel and, by default, on the host's network. The
[security model](docs/security.md) describes what isolation covers, what it does not, and how the
host-side code protects itself from what runs in a sandbox.

## Supported versions

| Version | Security fixes |
|---|---|
| The latest minor release | Yes |
| Earlier minor releases | No; upgrade to the latest release |

PyPI's [release history](https://pypi.org/project/hpc-sandbox/#history) shows the latest release,
and [CHANGELOG.md](CHANGELOG.md) what each release changed.

## Report a vulnerability

Report a vulnerability privately, through GitHub's private vulnerability reporting: open the
repository's [Security tab](https://github.com/ApGa/hpc-sandbox/security), choose **Report a
vulnerability**, and fill in the form. Do not open a public issue, pull request or discussion about
it.

Include the hpc-sandbox version (`hpc-sandbox --version`), the runtime and its version, whether
Singularity CE or Apptainer is installed setuid or runs in user-namespace mode, and the steps that
reproduce the problem, ideally as a script, with what an attacker gains. The output of
`hpc-sandbox doctor --image IMAGE` on the compute node often helps.

## What to expect

1. You get a first answer within a week, in the private advisory that your report opens.
2. The maintainers confirm the problem with you there, and agree with you on a fix and on when to
   disclose it.
3. The fix comes out in a patch release of the latest minor release, or in the next minor release
   when it has to change the public API.
4. The advisory is published with that release, and credits you unless you ask not to be named.
   Where a CVE fits, the maintainers request one through GitHub.

Keep the details private until the advisory is published.

## Scope

In scope are flaws in what hpc-sandbox's host-side code guarantees whatever runs in a sandbox:
host-side code fooled by files that a sandbox controls (symlinks, FIFOs, special files or deep
trees met by a transfer, snapshot, restore, stop or janitor), environment values leaking onto
command lines or into host-side processes, processes signalled without a proven identity, and
deleting or unmounting what is not the sandbox's. Out of scope are the limits of the isolation:
what code in a sandbox can do as your uid, through the shared kernel, over the host's network,
through the mounts you give it or with memory it allocates between two samples of the memory
watchdog, and what an image does when the runtime runs it.
[Reporting a vulnerability](docs/security.md#reporting-a-vulnerability) in the security model
describes both. Report problems of the runtimes themselves (Singularity CE, Apptainer, enroot,
fuse-overlayfs, slirp4netns) to their projects.
