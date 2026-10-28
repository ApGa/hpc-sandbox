# Contributing to hpc-sandbox

Thank you for helping. This guide covers the development setup, the tests and the checks that CI
runs, the commit and pull request conventions, the changelog, and how a version is released. For
how a sandbox is built, watched and torn down, read the [architecture](docs/architecture.md).

Bug fixes, tests and documentation go straight to a pull request. Open an issue first for a new
feature, for a change to the public API or to the documented output of the command line, or for
support of another runtime or scheduler, so that the design is agreed on before you write the code.

- [Ways to contribute](#ways-to-contribute)
- [Set up a development environment](#set-up-a-development-environment)
- [Run the tests](#run-the-tests)
- [Lint and format](#lint-and-format)
- [Commits and pull requests](#commits-and-pull-requests)
- [The changelog](#the-changelog)
- [Release a version](#release-a-version)

## Ways to contribute

| You want to | Start with |
|---|---|
| Report a bug | A [bug report](https://github.com/ApGa/hpc-sandbox/issues/new?template=bug_report.yml), with the output of `hpc-sandbox doctor --image IMAGE` on the compute node where the problem happens, and the smallest script or command that shows it |
| Report a vulnerability | A private report, as [SECURITY.md](SECURITY.md) describes; never a public issue |
| Ask for a feature | A [feature request](https://github.com/ApGa/hpc-sandbox/issues/new?template=feature_request.yml) that describes your workload; the [roadmap](docs/roadmap.md) lists what is not built yet |
| Fix a bug or improve the docs | A pull request; a fix comes with a test that fails without it, and [docs/README.md](docs/README.md) lists the pages of the docs |
| Change the public API or the documented command-line output, or support another runtime or scheduler | An issue first; [Public API and versioning](docs/python-api.md#public-api-and-versioning) says what is public |

## Set up a development environment

**Goal:** a checkout where the `hpc-sandbox` command runs and the unit tests pass.

**Prerequisites:** Linux, git and [uv](https://docs.astral.sh/uv/). The unit tests need no
container runtime and no compute node, but most of them need unprivileged user namespaces;
[What the unit tests need](#what-the-unit-tests-need) lists what else they use.

1. Fork [ApGa/hpc-sandbox](https://github.com/ApGa/hpc-sandbox) on GitHub and clone your fork:

   ```bash
   git clone https://github.com/<your-user>/hpc-sandbox.git
   cd hpc-sandbox
   ```

2. Install hpc-sandbox in editable mode with the `dev` dependency group (pytest, pytest-asyncio,
   ruff and pre-commit), as CI does:

   ```bash
   uv sync --group dev
   ```

   uv creates the virtual environment `.venv` with Python 3.11 or later, which it downloads if the
   machine has none, and `uv run` runs commands in it. It also records the versions it resolved in
   `uv.lock`, which git ignores: the repository keeps no lock file, and CI resolves the versions
   afresh on every run. To work in an environment that you activate yourself, for example with the
   oldest Python that hpc-sandbox supports, create it and install into it instead, and leave
   `uv run` out of the commands below:

   ```bash
   uv venv --python 3.11          # the oldest version hpc-sandbox supports
   uv pip install -e . --group dev
   source .venv/bin/activate
   ```

   pip 25.1 and later read dependency groups too: `pip install -e . --group dev` installs the same
   into a virtual environment of your own.

3. Install the git hooks:

   ```bash
   uv run pre-commit install
   ```

   This installs both hooks that `default_install_hook_types` in `.pre-commit-config.yaml` names:
   the `pre-commit` hook runs ruff on the files of each commit, as CI's `lint` job does on every
   file, and the `commit-msg` hook checks each commit message (see
   [Commits and pull requests](#commits-and-pull-requests)).

4. Check that the command runs from your checkout:

   ```bash
   uv run hpc-sandbox --version
   ```

   It prints the version that `pyproject.toml` sets.

**Next:** [run the tests](#run-the-tests).

## Run the tests

CI runs the unit tests on Python 3.11, 3.12 and 3.13, after `uv sync --group dev`:

```bash
uv run pytest -v -ra
```

They start no real container: their fixtures put stand-ins for the runtimes first on `PATH`, and
the code under test runs against them with real processes, so identity checks, the janitor, the
memory watchdog, timeouts and cancellation are exercised for real:

| File | Stands in for |
|---|---|
| `tests/fake_singularity.py` | `singularity`: it records every call and simulates the commands hpc-sandbox uses; its instances are host processes with the runtime's process title |
| `tests/fake_enroot.py` | `enroot`: it builds containers from real user and mount namespaces, bind mounts and `chroot`, so the enroot provider's launcher and its `nsenter` calls run for real |
| `tests/fake_fuse.py` | `fuse-overlayfs` and `fusermount3`, for Singularity's fuse-overlay layout |
| `tests/fake_runsc.py` | `runsc`: its processes are titled as runsc's (`runsc-gofer`, `runsc-sandbox`) and die with their parents, it creates missing mount points in the template as runsc's gofer does, its Sentry holds a memory file and runs commands in real user, mount and PID namespaces, and its overlay is a copy of the template |

`-ra` ends the run with a summary line for each test that did not pass, with the reason of each
skip. The integration tests always skip in this run (see [Integration tests](#integration-tests)),
and so do the tests whose requirements the machine lacks; failures and errors are never expected.
On an 8-core machine the suite takes about three minutes, most of it spent waiting on real
processes.

While you work, run only what you touch:

```bash
uv run pytest tests/test_enroot.py -q                 # one file
uv run pytest tests/test_memory.py -q -k oom_score    # the tests whose names match
```

### What the unit tests need

| Need | Without it |
|---|---|
| Linux and Python 3.11 or later | Nothing runs. |
| Unprivileged user namespaces and util-linux `nsenter` | The enroot provider's tests, the gVisor provider's tests, the pod tests, and the tests that mount file systems in a namespace skip. `unshare --user --map-root-user --mount --pid --ipc --net --fork true` succeeds where user namespaces work. Ubuntu 24.04 and later restrict them with AppArmor; `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0` lifts that until the next boot. |
| util-linux `unshare` | The tests that mount file systems in a namespace skip (in `tests/test_trash.py` and `tests/test_fuse_overlay.py`). |
| `/usr/bin/python3` | The pod tests (`tests/test_pods.py`) skip, and so do the memory watchdog tests that run Python in a fake sandbox. |
| `newuidmap` and `newgidmap`, and subordinate ids for your user in `/etc/subuid` and `/etc/subgid` | The pod tests that switch users in a pod with `users="subids"` skip. The other tests of such pods use stand-ins for the tools and the ranges. |
| util-linux `flock` | One test of background deletions in `tests/test_trash.py` skips. |
| `bash` | The environment tests in `tests/test_commands.py` skip their `bash` cases; they test `dash` and `busybox` too where installed. |
| `/etc/localtime` | The enroot provider's time zone tests skip. |
| `sshfs`, an `sftp-server` (`/usr/lib/openssh/sftp-server`, `/usr/libexec/openssh/sftp-server` or `/usr/lib/sftp-server`), `fusermount3`, and a `/dev/fuse` you can read and write | The test that has janitors unmount real, dead FUSE mounts, which it makes with `sshfs` as `fuse-overlayfs` would, skips (`tests/test_fuse_overlay.py`). CI does not install `sshfs`: run it yourself when you change how overlays are found or unmounted. |

### Integration tests

The integration tests start sandboxes and pods with a real runtime. Run them on a compute node,
inside a batch job or an interactive one (for example `srun --nodes=1 --pty bash` on Slurm or
`qsub -I` on PBS, with the account, partition or queue your site asks for), never on a login node:
they start containers and fill node-local disk.

```bash
HPC_SANDBOX_INTEGRATION=1 uv run pytest tests/test_integration.py -v
HPC_SANDBOX_INTEGRATION=1 uv run pytest tests/test_pods_integration.py -v
HPC_SANDBOX_INTEGRATION=1 uv run pytest tests/test_gvisor_integration.py -v
```

The last runs what is the gVisor provider's own, which needs `runsc` on `PATH` (with its release's
`gvisor-bin/` beside it) and Singularity CE or Apptainer for its images, and skips without them.
`HPC_SANDBOX_TEST_RUNTIME=gvisor` runs the first file with gVisor too, but for its tests of the
per-process memory watchdog, which gVisor's whole-sandbox limit replaces; the pod tests skip with
it, since the gVisor provider runs no pods yet.

Their sandboxes live under the default scratch root
([The scratch root](docs/configuration.md#the-scratch-root)): where that is not on the node's local
disk, set `HPC_SANDBOX_SCRATCH` to a directory that is. They pull their images into the image cache,
so the node needs to reach the registry unless you give them local image files. These variables
choose what they test:

| Variable | Default | Meaning |
|---|---|---|
| `HPC_SANDBOX_INTEGRATION` | unset | `1` runs the integration tests; without it, they skip. |
| `HPC_SANDBOX_TEST_RUNTIME` | `auto` | `singularity`, `enroot`, `gvisor`, or `auto`: Singularity when `singularity` or `apptainer` is on `PATH`, else enroot (never gVisor). |
| `HPC_SANDBOX_TEST_IMAGE` | `docker://alpine:3.20` | The image of `tests/test_integration.py`: any image that hpc-sandbox can start ([Image references](docs/python-api.md#image-references)) and that has the usual command-line tools, as BusyBox or a distribution's base system provides them: the tests use `tail`, `find`, `xargs`, `stat` and others. A local `.sif` or `.sqsh` file works too. |
| `HPC_SANDBOX_TEST_NETWORK_NONE` | unset | `1` also tests `network="none"` with Singularity, where the site lets users create network namespaces. With enroot it is always tested. |
| `HPC_SANDBOX_TEST_FAKEROOT` | unset | `1` also tests Singularity's `fakeroot` option, where `--fakeroot` works for the user (subordinate ids in `/etc/subuid` and `/etc/subgid`): commands that switch users, the owners of an image's files, and that nothing they leave stays behind after a stop or a janitor's sweep. |
| `HPC_SANDBOX_TEST_GPUS` | unset | On a GPU node, tests GPU passthrough: `all` with enroot, `nv` or `rocm` with Singularity, with an image in which the runtime makes `nvidia-smi` or `rocm-smi` available. |
| `HPC_SANDBOX_TEST_POD_IMAGE` | `docker://alpine:3.20` | The image of the pod tests' services; its busybox serves and fetches HTTP. |
| `HPC_SANDBOX_TEST_APT_IMAGE` | `docker://python:3.12-slim` | The image of the pod test and the gVisor test that run apt, and of the gVisor tests that run Python (host Unix sockets, memory). |
| `HPC_SANDBOX_TEST_GVISOR_PLATFORM` | `systrap` | gVisor's platform in `tests/test_gvisor_integration.py`, and in `tests/test_integration.py` with `HPC_SANDBOX_TEST_RUNTIME=gvisor`: `systrap`, `ptrace` or `kvm`. |
| `HPC_SANDBOX_TEST_REDIS_IMAGE` | `docker://redis:7-alpine` | The Redis image that the pod tests run as each `run_as`, with its entrypoint. |
| `HPC_SANDBOX_TEST_POSTGRES_IMAGE` | `docker://postgres:16-alpine` | The PostgreSQL image that the pod tests run as each `run_as`, with its entrypoint. |
| `HPC_SANDBOX_TEST_SUBIDS` | unset | `1` also tests pods with `users="subids"`, with Singularity, where your user has subordinate ids (`/etc/subuid` and `/etc/subgid`) and `newuidmap` and `newgidmap` map them: services that switch users, the owners of an image's files, and that nothing they leave stays behind after a stop or a janitor's sweep. |
| `HPC_SANDBOX_TEST_FILE_OWNERS_IMAGE` | `docker://node:20-alpine` | The image of the tests of file owners manifests, which list the owners that its layers give: `node:20-alpine`, or a local copy of it. |
| `HPC_SANDBOX_TEST_PODS` | `16` | How many pods the pod tests start at once. |

The pod tests check egress where they find slirp4netns, the site's or the one that
`hpc-sandbox fetch-tools` downloads, and then reach PyPI and Debian's mirrors. The test that other
users cannot reach a pod's exposed ports runs where you have subordinate ids in `/etc/subuid` and
`/etc/subgid` and where `newuidmap` and `newgidmap` are installed, and skips elsewhere.
[Environment variables](docs/configuration.md#environment-variables) lists these variables with
every other one that hpc-sandbox reads.

A change to a provider, to pods, or to how sandboxes are stopped and swept should pass the
integration tests with the runtime it touches; the pull request says which runtime and version
they ran on.

### Write tests

- Test against the fakes: the fixtures in `tests/conftest.py` give you a provider on each fake
  runtime (`provider`, `enroot_provider` and `gvisor_provider`, whose `fake_runsc` fixture records
  runsc's calls and sets its knobs) and an image for it (`sif` and `sqsh`; gVisor's take SIF
  images too). Only the integration tests use a real runtime, and they run only with
  `HPC_SANDBOX_INTEGRATION=1`.
- Put a test with the tests of the code it covers, such as `tests/test_enroot.py` for the enroot
  provider. The test directory has no `__init__.py`, and test files import each other by their
  bare names (`from conftest import make_provider`), so give every test file a unique name.
- Keep what a test writes in `tmp_path`, set environment variables with `monkeypatch`, and use no
  network.
- When a test needs something a machine may lack, such as a tool or a kernel feature, skip it with
  a reason that names what is missing, as the tests above do.
- When a test needs a tool to be missing, hide it with `hide_from_path` (in `tests/conftest.py`)
  rather than by shortening `PATH`: a host may keep the tool in `/usr/bin` too, as GitHub's runners
  keep slirp4netns.

## Lint and format

`.pre-commit-config.yaml` defines three hooks. CI's `lint` job runs the two ruff hooks on every
file:

```bash
uvx pre-commit run --all-files --show-diff-on-failure
```

| Hook | What it checks | Configured in |
|---|---|---|
| `ruff-check` (ruff-pre-commit `v0.17.0`, with `--fix`) | Lint rules `E` (pycodestyle errors), `F` (pyflakes) and `I` (import order), in Python files; it fixes what it can | `pyproject.toml`: `line-length = 100` under `[tool.ruff]`, `select = ["E", "F", "I"]` under `[tool.ruff.lint]` |
| `ruff-format` | The formatting of Python files, and of the Python code blocks in Markdown files such as this guide and the docs | `line-length` under `[tool.ruff]` |
| `conventional-pre-commit` (`v4.0.0`, at the `commit-msg` stage, with `--verbose`) | The form of each commit message, when you commit; a rejection says what the message is missing. CI does not run it | Its default types, which are the types in [Commits and pull requests](#commits-and-pull-requests) |

To run ruff alone, on Python files or on Markdown files whose Python examples you changed:

```bash
uv run ruff check --fix path/to/file.py
uv run ruff format path/to/file.py docs/page.md
```

When a hook fixes a file during `git commit`, the commit stops; add the fixed files and commit
again.

Version notes. The ruff hook runs ruff `v0.17.0`, the version that the `dev` group pins, so
`uv run ruff` and the hook agree; upgrade both in the same commit. Keep the
`conventional-pre-commit` hook at `v4.0.0`, which this guide describes: later releases accept any
message that starts with `Merge`, and a type in any case (`Feat:`), which the pull request title
check rejects.

## Commits and pull requests

**Branches.** Branch from `main`, and keep one topic per pull request.

**Commit messages.** Commits follow [Conventional Commits](https://www.conventionalcommits.org/)
in the form `<type>: <summary>`, with a type from the table below. The summary is a short
imperative sentence in sentence case, with no final period. Conventional Commits allows a scope
(`fix(enroot): ...`), but hpc-sandbox leaves it out. After a blank line, a body says why:

```text
fix: Keep each path one argument when deleting trash in the background

The detached deletion built its flock command in a variable and expanded
it unquoted, so a session root whose path holds a space split the lock
file's path into several words, and every deletion failed. The command
is now built with set --, which keeps every path one argument.
```

| Type | For |
|---|---|
| `feat` | A new feature, option or command |
| `fix` | A bug fix |
| `docs` | Documentation only |
| `refactor` | A code change that neither fixes a bug nor adds a feature |
| `perf` | A performance improvement |
| `test` | Tests only |
| `build` | Packaging and dependencies |
| `ci` | The CI workflows and the git hooks |
| `chore` | Other maintenance, such as a release |
| `style` | Formatting only |
| `revert` | Reverting an earlier commit |

For example:

```text
feat: Add a fuse-overlay rootfs layout for Singularity sandboxes
fix: Fall back to copies where a site-wide fuse-overlay default cannot apply
perf: Snapshot fuse-overlay sandboxes as their upper layer
docs: Document templates, the fuse-overlay layout and background deletions
test: Run the integration tests against either runtime
```

A commit that breaks the public API or the documented output of the command line
([Public API and versioning](docs/python-api.md#public-api-and-versioning)) puts `!` after its type
and adds a `BREAKING CHANGE:` footer that says what users must change:

```text
perf!: Delete stopped sessions in a detached process

stop() now returns once the session has left the session root, and a
detached rm deletes it afterwards, so that stopping many sandboxes no
longer holds a worker thread for each deletion.

BREAKING CHANGE: code that removes a scratch root, or ends its process,
right after stopping sandboxes must first await
provider.wait_for_deletions().
```

The `conventional-pre-commit` hook, installed in
[step 3 of the setup](#set-up-a-development-environment), checks the message of every `git commit`
and `git merge`. It rejects a message whose subject is not one of these types, an optional scope, an
optional `!`, a colon, a space and a summary, or whose body does not follow a blank line. It
accepts a scope and does not check the summary's case or wording; reviewers check those. It accepts
`fixup!`, `squash!` and `amend!` commits, so `git rebase --autosquash` works as usual, and merge
commits whose message starts with `Merge branch`. Two of git's own messages need a fix:

- `git revert` writes `Revert "<subject>"` and commits without running the hook. Run
  `git revert --no-commit <commit>` and then `git commit`, which runs it, and write
  `revert: <the reverted commit's summary>`, keeping the `This reverts commit <hash>.` line that
  git puts in the body.
- `git merge origin/main` writes `Merge remote-tracking branch 'origin/main' into <branch>`, which
  the hook rejects, leaving the merge uncommitted. Rebase onto `main` instead, or merge a local
  `main`: `git merge main` writes `Merge branch 'main' into <branch>`.

To check a message without committing, save it to a file and run the hook on that file:

```bash
uvx pre-commit run conventional-pre-commit --hook-stage commit-msg --commit-msg-filename message.txt
```

**Pull request titles.** A pull request's title has the same form as a commit subject.
Maintainers squash-merge pull requests, so the title becomes the subject of the commit on `main`.
CI's `pr-title` job (`amannn/action-semantic-pull-request@v5`) checks that the title is a
Conventional Commit whose type is one of those above, with no scope and a summary that starts with
a capital letter and has no final period. When a pull request has a single commit, the job checks
that commit's subject too, because GitHub suggests it for the squashed commit. The check runs again
whenever you edit the title.

**What CI runs on a pull request.** These jobs run on GitHub's Ubuntu runners for every pull
request into `main`; the `lint`, `test` and `gvisor` jobs also run for every push to `main`:

| Job | Runs | Fails when |
|---|---|---|
| `pr-title` | On pull requests only | The title, or the subject of a single commit, is not a Conventional Commit in the form above |
| `lint` | `uvx pre-commit run --all-files --show-diff-on-failure` | A hook fails or changes a file |
| `test` | On Python 3.11, 3.12 and 3.13: `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0`, then `uv sync --group dev` and `uv run pytest -v -ra` | Unprivileged user namespaces do not work, or a test fails |
| `gvisor` | On Python 3.12: the same sysctl, runsc from its release and Apptainer from its, each checked against a pinned checksum, `runsc --rootless do true`, `hpc-sandbox doctor --runtime gvisor --image docker://alpine:3.20` (which starts a sandbox as the provider does), then, with `HPC_SANDBOX_INTEGRATION=1`, `tests/test_gvisor_integration.py` and `tests/test_integration.py` with `HPC_SANDBOX_TEST_RUNTIME=gvisor` (`tests/test_gvisor.py` runs in the `test` job) | A download's checksum differs, rootless runsc does not run, `doctor` fails, or a test fails |
| `build` (the release workflow) | The build and the checks of [Cut a release](#cut-a-release), without publishing | The sdist or the wheel does not build or does not pass `twine check --strict`, or the wheel's `hpc-sandbox --version` fails |

The `test` job has no container runtime and no `sshfs`: the fakes stand in for the runtimes, the
test that needs `sshfs` skips, and the integration tests never run there. The `gvisor` job runs
the integration tests with gVisor, which pull their images from Docker Hub; the others need a
compute node.

**The pull request itself.** The [pull request template](.github/pull_request_template.md) asks
for each of these:

- Say what changes and why, and how you tested it: the commands you ran and, for a change that the
  integration tests cover, the runtime and its version that they ran on.
- Add a test for every fix and feature; a fix's test should fail without the fix.
- Add an entry to the changelog for every user-visible change ([The changelog](#the-changelog)).
- Update the docs and the command-line help that the change affects.
- Keep secrets, host names, user names and your site's paths out of code, tests, docs and commit
  messages; tests read such settings from environment variables, as the integration tests do.
- Link what the change belongs to, such as `Fixes #123`.

Reviewers also check that the code keeps what hpc-sandbox promises:

- `src/hpc_sandbox` imports only Python's standard library, and runs on Python 3.11, the oldest
  version hpc-sandbox supports. Tools that only the tests need go in the `dev` group.
- The host side stays safe from what a sandbox controls, as the
  [security model](docs/security.md#how-hpc-sandbox-protects-the-host-side) describes: files that
  a sandbox can change are handled through directory file descriptors, environment values reach
  commands through stdin, and no process is signalled before its identity is proven.

hpc-sandbox is released under the [MIT License](LICENSE), and contributions are accepted under the
same license.

## The changelog

[CHANGELOG.md](CHANGELOG.md) lists what each release changes for users. Every user-visible change
gets an entry there, in the pull request that makes it, under the heading of the upcoming release,
`## X.Y.Z (unreleased)`, at the top of the file. When there is no such heading, add one: the next
patch version, such as `0.3.1`, for fixes, and the next minor version, such as `0.4.0`, for
anything else; rename it when a later change needs a minor release.

Within the release, put the entry in the subsection that fits, such as `### Changed behaviour`,
`### Added` or `### Fixed`, and say in `### Upgrading` what users of the previous release must
change. An entry is a bullet that starts with a bold sentence saying what changed for users, names
options, commands and exceptions in backticks, and links the section of the docs that describes
it. Tests, refactoring and CI need no entry.

## Release a version

This section is for maintainers. hpc-sandbox is published to PyPI as `hpc-sandbox` by the
[release workflow](.github/workflows/release.yml) with
[trusted publishing](https://docs.pypi.org/trusted-publishers/), so no API token is stored
anywhere: pushing the tag `vX.Y.Z` releases version `X.Y.Z`. The workflow also builds and checks
the distribution on every pull request, without publishing it.

### Versions

The version is set in one place, `version` in `pyproject.toml`; `hpc_sandbox.__version__` and
`hpc-sandbox --version` report it. While the version is below 1.0, a minor release (0.4.0) may
change the public API, and a patch release (0.3.1) only fixes bugs, so code built on hpc-sandbox
requires a range within one minor release, such as `hpc-sandbox>=0.5,<0.6`.
[Public API and versioning](docs/python-api.md#public-api-and-versioning) says what the public API
is.

### Set up the repository and PyPI once

1. On PyPI, add a GitHub trusted publisher: owner `ApGa`, repository `hpc-sandbox`, workflow
   `release.yml`, environment `pypi`. Until the first release creates the project on PyPI, add it
   as a pending publisher, under Publishing in your account's settings, for the project name
   `hpc-sandbox`; the first upload then creates the project with this publisher.
2. On GitHub, in the repository's settings (Environments), create the environment `pypi`. Limit it
   to tags that match `v*`, and add required reviewers if someone should approve each release
   before it is uploaded.
3. In the repository's security settings, enable private vulnerability reporting, which
   [SECURITY.md](SECURITY.md) sends reporters to.
4. In the repository's settings for pull requests, allow squash merging, with the pull request's
   title as the default commit message, and turn off the other merge methods.

### Cut a release

1. On a branch, set `version` in `pyproject.toml` and date the version's section in
   [CHANGELOG.md](CHANGELOG.md): its heading, such as `## 0.3.1 (unreleased)`, becomes
   `## 0.3.1 (2026-11-02)`. A minor release, such as 0.4.0, also moves every range that names the
   previous one in the README and the docs (`grep -rn "0\.3,<0\.4" --exclude-dir=.git .`). Open a
   pull request, such as `chore: Release 0.3.1`, and merge it once CI and the release workflow's
   checks pass.
2. Tag the merge commit and push the tag:

   ```bash
   git tag -a v0.3.1 -m "hpc-sandbox 0.3.1"
   git push origin v0.3.1
   ```

3. The release workflow builds the sdist and the wheel and checks them:

   ```bash
   uv build --no-sources
   uvx twine check --strict dist/*
   uvx --from dist/*.whl hpc-sandbox --version
   test -f "dist/hpc_sandbox-${GITHUB_REF_NAME#v}-py3-none-any.whl"
   ```

   The last check fails when the tag does not name the version in `pyproject.toml`, and then
   nothing is published: delete the tag (`git push origin --delete v0.3.1`, then
   `git tag -d v0.3.1`), fix the version and tag again. When every check passes, the workflow
   uploads both files to PyPI from the `pypi` environment, once its required reviewers, if any,
   approve.
4. Check the release in a fresh virtual environment (`pip install hpc-sandbox==0.3.1`, then
   `hpc-sandbox --version`), and create a GitHub release from the tag with the version's section
   of the changelog.

PyPI never takes the same version twice, even after a release is deleted. To fix a broken release,
publish the next patch release, and yank the broken one on PyPI.

### What the distribution holds

- **The wheel:** the `hpc_sandbox` package, including `anchor.py` and `holder.py`, which the
  providers run as scripts, and the `py.typed` marker; the `hpc-sandbox` command; and `LICENSE`, in
  its `.dist-info/licenses/`.
- **The sdist:** what builds the wheel: `src/` (with `py.typed`), `pyproject.toml`, `README.md`,
  `CHANGELOG.md` and `LICENSE`, as `[tool.hatch.build.targets.sdist]` in `pyproject.toml` lists
  them, plus the `PKG-INFO` and `.gitignore` that hatchling adds. Tests and docs stay in the
  repository.
- **The requirements:** none. hpc-sandbox needs Python 3.11 or later and nothing outside its
  standard library.

Release files come only from the release workflow, which builds them from a clean checkout of the
tag, never from a working tree with local files in it.
