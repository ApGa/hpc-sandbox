<!--
Title the pull request like a commit subject: a Conventional Commit with no scope and a summary in
sentence case without a final period, such as "fix: Keep each path one argument when deleting trash
in the background". CI checks it, and the title becomes the commit subject on main when the pull
request is squash-merged. CONTRIBUTING.md, under "Commits and pull requests", lists the types.
-->

## Summary

<!-- What changes, in a few sentences. -->

## Why

<!-- The problem this solves, or what it makes possible. -->

## Links

<!-- "Fixes #123". Delete this section when there is nothing to link. -->

## Tests

<!--
The tests you added, and the commands you ran with what they showed. Name the runtime and its
version that the integration tests ran on (Singularity CE, Apptainer or enroot, and for Singularity
CE and Apptainer whether setuid or in user-namespace mode), or say why they did not run.
-->

## Checklist

- [ ] The title is a Conventional Commit, `<type>: <summary>`, with no scope and a sentence-case
      summary without a final period
- [ ] Every fix and feature has a test, and a fix's test fails without the fix
- [ ] `uvx pre-commit run --all-files` and the unit tests (`uv run pytest`) pass
- [ ] The integration tests pass on a compute node with the runtime that the change affects, named
      under Tests, or the change cannot affect them
- [ ] A user-visible change has an entry under the upcoming `## X.Y.Z (unreleased)` heading of
      `CHANGELOG.md`
- [ ] The docs and command-line help that the change affects are updated
- [ ] No secrets, and no host names, user names or paths of your site, in code, tests, docs or
      commit messages
- [ ] A commit that breaks the public API or the documented command-line output has `!` after its
      type and a `BREAKING CHANGE:` footer
