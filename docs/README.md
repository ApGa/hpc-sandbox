# hpc-sandbox documentation

This page is the table of contents. New to hpc-sandbox? Start with
[Getting started](getting-started.md): it covers what a node needs, installation, your first
sandbox and batch scripts.

## Getting started

- [Getting started](getting-started.md): requirements, installation, `hpc-sandbox doctor`, your
  first sandbox, and Slurm and PBS batch scripts.
- [Changelog](../CHANGELOG.md): what each release changes, and what a version below 1.0 may break.

## Guides

- [Pods](pods.md): services that share one network, like a docker-compose project, with health
  checks, relays to host services and egress.
- [Singularity CE and Apptainer](singularity.md): `SingularityProvider`: instances, root filesystem
  layouts and templates, and site notes.
- [enroot](enroot.md): `EnrootProvider`: imports, namespaces, what enroot may add, restores, and
  site notes.
- [gVisor](gvisor.md): `GvisorProvider`, opt-in: root that stays in gVisor's kernel, what works in
  a sandbox and what does not, its overlay, limits and site notes.

## Reference

- [Python API](python-api.md): providers, sandboxes and their contract, pods, errors, and the
  public API and versioning policy.
- [Command line](cli.md): every `hpc-sandbox` command and option, the JSON outputs and the exit
  codes.
- [Configuration](configuration.md): provider options, where files go, and every environment
  variable.

## Background

- [Architecture](architecture.md): how a sandbox is built, run and torn down, for contributors
  and the curious.
- [Security model](security.md): what isolation covers and what it does not, and what to report.
- [Roadmap](roadmap.md): what hpc-sandbox does not do yet.

## Contributing and security

- [Contributing](../CONTRIBUTING.md): the development setup, the tests, linting, commits and pull
  requests, the changelog and releases.
- [Security policy](../SECURITY.md): which versions get security fixes, and how to report a
  vulnerability privately.

## Next steps

- [Getting started](getting-started.md): install hpc-sandbox and run your first sandbox.
- [The repository's README](../README.md): what hpc-sandbox is, in one page.
