"""Nothing the repository holds names the private infrastructure or the accounts of the people who
develop hpc-sandbox: no cluster, project group, host or account of theirs, and no path into their
storage. Nor does it name the downstream projects that use hpc-sandbox, which its users need not
know.

The scan covers every text file git tracks, and every new file it does not ignore, outside hidden
directories and caches. A file is text when its first 8000 bytes hold no NUL byte, as git judges
it. Without git, the scan walks the repository. A file may hold a banned word only in a text that
``ALLOWED`` lists, with the reason.

Banned words are stored as SHA-256 digests, so that this file does not publish them itself. A line
has its camelCase words split with underscores (``FooBar`` reads as ``foo_bar``), is lower-cased
and is split into words (letters, digits and underscores), which hyphens may join into names. A
word matches when it or any run of its underscore-separated parts is banned, also without its
trailing digits; a hyphenated name also matches when a run of two or more of its parts is; and each
of these matches without a trailing ``s`` too (a plural). Four path patterns catch Lustre paths,
home directories, and per-user directories of shared storage and scratch, whoever they belong to;
placeholders such as ``<user>`` and ``$USER``, and the generic ``/home/user``, stay allowed. File
names are scanned like file contents. To ban another word or name, add
``hashlib.sha256(word.lower().encode()).hexdigest()``.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import tomllib
from collections.abc import Iterable, Iterator
from functools import cache
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

BANNED_DIGESTS = frozenset(
    {
        "691c1224a07863335713b1c9f6b5d345529a6519636b7d52428234b0fef76b2e",  # a cluster
        "9ea18001c9a2b615fd322913d3748955b2f4d21d0eea3d9317135cc907ab1645",  # a project group
        "b07c73cd27f32e6240f7686066f458f3062bee992086562ba1807e1efc628fe1",  # the same group's team
        "d7f31cba99e98e883aed679e8267b2fd41bf712517b0a3367b987d5dca2650ed",  # a user account
        "a83e1c1b8a447b6fdc48ce823653f1b12de67d17756c35f0adf72e3c8381e716",  # a host name prefix
        "0eb56b9efa67556a510054cb5770767c71f83d02b5684e29512250a8bdbec0d1",  # a downstream project
    }
)

# An absolute path into Lustre or a home directory names the machine and often the account a run
# used, and a per-user directory of shared storage or scratch names the account. A URL path does
# not match, and neither does a placeholder for the account: <user>, $USER or the word user.
BANNED_PATHS = (
    re.compile(r"(?<![\w.~$-])/lustre\w*/"),
    re.compile(r"(?<![\w.~$-])/home/(?!<|user(?![\w.-]))[\w.-]+"),
    re.compile(r"/users/(?!<|user(?![\w.-]))[\w.-]+"),
    re.compile(r"(?<![\w.~$-])/scratch\d*/(?!<|user(?![\w.-]))[a-z][a-z0-9]*(?![\w.-])"),
)


def _author_addresses() -> dict[tuple[str, str], str]:
    pyproject = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    return {
        ("pyproject.toml", author["email"]): "the author's address, which PyPI shows"
        for author in pyproject["project"].get("authors", [])
        if "email" in author
    }


# (file, text) -> why the file may hold that text although it names something banned. Every entry
# must still be needed (see the last test). Keep the list short: reword the file instead wherever
# its readers lose nothing.
ALLOWED: dict[tuple[str, str], str] = {**_author_addresses()}

_NAME = re.compile(r"[a-z0-9_]+(?:-[a-z0-9_]+)*")
_CAMEL_CASE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
# Directories a checkout may hold untracked: hidden ones (environments, caches, git itself) and
# caches.
_SKIPPED_DIRECTORIES = {"__pycache__", "node_modules", "site-packages", "build", "dist"}


@cache
def _banned(name: str) -> bool:
    """Whether a lower-case word or hyphenated name is banned (see the module docstring)."""
    parts = name.split("-")
    candidates = {
        "-".join(parts[start:end])
        for start in range(len(parts))
        for end in range(start + 2, len(parts) + 1)
    }
    for part in parts:
        words = [word for word in part.split("_") if word]
        runs = {
            "_".join(words[start:end])
            for start in range(len(words))
            for end in range(start + 1, len(words) + 1)
        }
        candidates |= runs | {run.rstrip("0123456789") for run in runs}
    candidates |= {candidate.removesuffix("s") for candidate in candidates}
    return any(
        hashlib.sha256(candidate.encode()).hexdigest() in BANNED_DIGESTS for candidate in candidates
    )


def _hits(text: str, allowed: Iterable[str] = ()) -> list[str]:
    """``line N: 'match'`` for each banned word, name or path in ``text``, outside the ``allowed``
    texts."""
    for exempt in allowed:
        text = text.replace(exempt, "")
    text = _CAMEL_CASE.sub("_", text).lower()
    names = {name for name in set(_NAME.findall(text)) if _banned(name)}
    if not names and not any(pattern.search(text) for pattern in BANNED_PATHS):
        return []
    hits = []
    for number, line in enumerate(text.split("\n"), 1):
        found = [name for name in _NAME.findall(line) if name in names]
        found += [match[0] for pattern in BANNED_PATHS for match in pattern.finditer(line)]
        hits += [f"line {number}: {item!r}" for item in found]
    return hits


def _skipped(relative: Path) -> bool:
    if relative.name == ".git":  # a worktree's link to its repository
        return True
    return any(
        (part.startswith(".") and part != ".github") or part in _SKIPPED_DIRECTORIES
        for part in relative.parts[:-1]
    )


def _is_text(path: Path) -> bool:
    with path.open("rb") as file:
        return b"\0" not in file.read(8000)


def _git_files(*options: str) -> list[Path]:
    listed = subprocess.run(
        ["git", "ls-files", "-z", *options],
        cwd=REPO,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
        capture_output=True,
        check=True,
        text=True,
    ).stdout
    return [Path(name) for name in listed.split("\0") if name]


def _scanned_files(*, text_only: bool = True) -> list[Path]:
    """Tracked files, and new files that git does not ignore; without git, a walk of the
    repository. Only the text files, unless ``text_only`` is false."""
    try:
        tracked = set(_git_files("--cached"))
        new = _git_files("--others", "--exclude-standard")
    except (OSError, subprocess.CalledProcessError):
        tracked, new = set(), [path.relative_to(REPO) for path in REPO.rglob("*")]
    candidates = tracked | {path for path in new if not _skipped(path)}
    return sorted(
        path
        for path in candidates
        if (REPO / path).is_file() and (not text_only or _is_text(REPO / path))
    )


FILES = _scanned_files()


def test_the_scan_covers_every_text_file() -> None:
    names = {path.as_posix() for path in FILES}
    assert {"README.md", "CHANGELOG.md", "LICENSE", "pyproject.toml"} <= names
    assert {"src/hpc_sandbox/__init__.py", "tests/test_no_internal_references.py"} <= names
    for prefix in ("src/", "tests/"):
        assert any(name.startswith(prefix) for name in names), prefix
    assert not any(name.endswith(".png") for name in names)  # binary


# A cluster, a numbered host and a planning document that nothing real is called: the matcher test
# bans them, so that it needs no banned word.
MADE_UP_BANS = ("zorbix", "qnode1", "alpha-memo")


@pytest.fixture
def made_up_bans(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Ban ``MADE_UP_BANS`` as well, for one test."""
    digests = {hashlib.sha256(name.encode()).hexdigest() for name in MADE_UP_BANS}
    monkeypatch.setitem(globals(), "BANNED_DIGESTS", BANNED_DIGESTS | digests)
    _banned.cache_clear()
    yield
    _banned.cache_clear()


def test_the_matcher_finds_words_names_paths_and_numbered_hosts(made_up_bans: None) -> None:
    cluster, host, document = MADE_UP_BANS
    assert _banned(cluster) and _banned(f"{cluster}_cache") and _banned(f"{cluster}-node")
    assert _banned(f"{cluster}s") and not _banned(f"g{cluster}") and not _banned(f"g{cluster}s")
    assert _banned(f"{host}-00045") and _banned(f"{host}s") and not _banned("qnode2-00045")
    # A document's name; its words alone are fine.
    assert _banned(document) and _banned(f"the-{document}") and not _banned("alpha")
    assert not _banned("memo") and not _banned("hpc-sandbox") and not _banned("cpus")
    assert _hits(f"class {cluster.capitalize()}Bridge:") == [f"line 1: '{cluster}_bridge'"]
    assert _hits(f"x\nran on {host}-00045") == [f"line 2: '{host}-00045'"]
    assert not _hits(f"x {cluster} y", allowed=[f"{cluster} y"])

    def path(*parts: str) -> str:
        return "/".join(("", *parts))

    home, lustre = path("home", "someone", "data"), path("lustre", "fs1", "project")
    users, scratch = path("fs", "users", "someone", "x"), path("scratch", "someone", "runs")
    assert _hits(f"cache in {home}\nmore") == [f"line 1: {path('home', 'someone')!r}"]
    assert _hits(f"root = {lustre}") == [f"line 1: {path('lustre') + '/'!r}"]
    assert _hits(users) == [f"line 1: {path('users', 'someone')!r}"]
    assert _hits(f"HPC_SANDBOX_SCRATCH={scratch}") == [f"line 1: {path('scratch', 'someone')!r}"]
    assert not _hits(f"https://example.com{home} and ~{home} and $HOME{home}")
    assert not _hits(f"{path('home', '<user>', 'x')} {path('home', 'user', '.cache')}")
    assert not _hits(f"{path('home', '$USER')} {path('scratch', '$USER', 'x')}")
    assert not _hits(f"{path('scratch', 'hpcsb-1', 'tmp')} {path('local', 'scratch', 'x')}")
    assert not _hits(f"users/groups and {path('scratch', '<user>')}")
    assert not _hits(f"{path('scratch', 'user', 'runs')} {path('fs', 'users', 'user')}")
    assert _hits(path("home", "username")) == [f"line 1: {path('home', 'username')!r}"]


@pytest.mark.parametrize("path", FILES, ids=lambda path: path.as_posix())
def test_no_internal_references(path: Path) -> None:
    text = (REPO / path).read_text(encoding="utf-8", errors="replace")
    hits = _hits(text, [exempt for (file, exempt) in ALLOWED if file == path.as_posix()])
    assert not hits, f"{path} names internal infrastructure or accounts:\n" + "\n".join(hits)


def test_no_file_name_names_internal_work() -> None:
    hits = {path.as_posix(): _hits(path.as_posix()) for path in _scanned_files(text_only=False)}
    assert {path: found for path, found in hits.items() if found} == {}


def test_every_allowed_text_is_still_needed() -> None:
    for (file, exempt), reason in ALLOWED.items():
        assert reason.strip(), f"say why {file} may hold {exempt!r}"
        text = (REPO / file).read_text(encoding="utf-8", errors="replace")
        assert exempt in text, f"{file} no longer holds {exempt!r}"
        assert _hits(exempt), f"{exempt!r} names nothing banned: drop its ALLOWED entry"
