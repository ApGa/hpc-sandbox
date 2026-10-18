"""Packaging: what ``pip install hpc-sandbox`` gets from PyPI.

hpc-sandbox imports nothing but Python's standard library and itself, so it publishes no
requirement; the version is set once (``pyproject.toml``), and ``hpc_sandbox.__version__`` and
``hpc-sandbox --version`` give the running hpc-sandbox's; the files the distribution names exist;
and PyPI's copy of the README links into the repository at the release's tag.
"""

from __future__ import annotations

import ast
import importlib
import importlib.metadata
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

import hpc_sandbox
from hpc_sandbox.cli import main

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE = REPO_ROOT / "src" / "hpc_sandbox"
PYPROJECT = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
PROJECT = PYPROJECT["project"]
# Where the README's links and images point on PyPI: the repository at the release's tag.
INTO_REPOSITORY = re.compile(
    r"https://(?:github\.com/ApGa/hpc-sandbox/(?:blob|tree|raw)|raw\.githubusercontent\.com"
    r"/ApGa/hpc-sandbox)/"
)
# The version of ruff that .pre-commit-config.yaml's ruff hooks run: the rev of their repository.
RUFF_HOOK_REV = re.compile(
    r"repo: https://github\.com/astral-sh/ruff-pre-commit\n(?:[ \t]*#.*\n)*[ \t]*rev: v(\S+)"
)


def _imports(path: Path) -> set[str]:
    """The top-level modules that ``path`` imports: statically, or by a constant name passed to
    ``importlib.import_module``. Relative imports are of hpc_sandbox itself."""
    names = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), str(path))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "import_module"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            names.add(node.args[0].value)
    return {name.split(".")[0] for name in names}


def _version_output() -> str:
    return f"hpc-sandbox {PROJECT['version']}\n"


# --------------------------------------------------------------------------- requirements


def test_hpc_sandbox_requires_nothing_and_imports_only_the_standard_library():
    """An import outside the standard library would work only where something else installed it:
    hpc-sandbox has no requirements, and tools that only development needs are in the dev group."""
    assert PROJECT["dependencies"] == []
    assert "optional-dependencies" not in PROJECT
    outside = sorted(
        f"{path.relative_to(REPO_ROOT)}: {name}"
        for path in sorted(PACKAGE.rglob("*.py"))
        for name in _imports(path)
        if name not in sys.stdlib_module_names and name not in ("__future__", "hpc_sandbox")
    )
    assert outside == []


def test_the_dev_group_has_the_ruff_of_the_git_hooks():
    """``uv run ruff`` and the ruff hooks of ``.pre-commit-config.yaml`` must agree, or a file that
    one of them formats fails the other's check."""
    [pinned] = [
        line.removeprefix("ruff==")
        for line in PYPROJECT["dependency-groups"]["dev"]
        if re.match(r"ruff\b", line)
    ]
    hook = RUFF_HOOK_REV.search((REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    assert hook, "no ruff-pre-commit repository with a rev in .pre-commit-config.yaml"
    assert hook[1] == pinned


# --------------------------------------------------------------------------- the version


def test_the_version_is_set_once_and_every_reader_gives_it(capsys):
    """``pyproject.toml`` sets the version, and ``hpc_sandbox.__version__`` and ``hpc-sandbox
    --version`` give the running hpc-sandbox's: here a checkout, so that file's."""
    assert "version" not in PROJECT.get("dynamic", [])
    assert Path(hpc_sandbox.__file__).resolve().parent == PACKAGE  # this checkout's, as pytest runs
    set_in_the_code = [
        path.relative_to(REPO_ROOT).as_posix()
        for path in sorted(PACKAGE.rglob("*.py"))
        if re.search(r"^\s*__version__\s*=", path.read_text(encoding="utf-8"), re.MULTILINE)
    ]
    assert set_in_the_code == []
    assert hpc_sandbox.__version__ == PROJECT["version"]
    with pytest.raises(SystemExit) as exited:
        main(["--version"])
    assert exited.value.code == 0
    assert capsys.readouterr().out == _version_output()


def test_the_command_prints_the_version(tmp_path):
    """``python -m hpc_sandbox``, which runs what the ``hpc-sandbox`` script runs."""
    assert PROJECT["scripts"] == {"hpc-sandbox": "hpc_sandbox.cli:main"}
    result = subprocess.run(
        [sys.executable, "-m", "hpc_sandbox", "--version"],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(PACKAGE.parent)},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert (result.returncode, result.stdout, result.stderr) == (0, _version_output(), "")


def test_a_checkouts_version_wins_over_installed_metadata(monkeypatch, capsys):
    """A checkout on the path shadows an installed hpc-sandbox, whose metadata may be stale or
    another copy's: ``__version__`` and ``--version`` give the checkout's version."""
    real_version = importlib.metadata.version

    def stale_version(name: str) -> str:
        return "0.0.0" if name == "hpc-sandbox" else real_version(name)

    monkeypatch.setattr(importlib.metadata, "version", stale_version)
    monkeypatch.delattr(hpc_sandbox, "__version__", raising=False)  # read once, on first use
    assert hpc_sandbox.__version__ == PROJECT["version"]
    with pytest.raises(SystemExit):
        main(["--version"])
    assert capsys.readouterr().out == _version_output()


def test_without_a_checkout_the_installed_metadata_gives_the_version(tmp_path, monkeypatch):
    """An installed hpc-sandbox finds no ``pyproject.toml`` of its own where a checkout keeps it,
    or another project's: the installed metadata gives the version, else ``0+unknown``."""
    another = tmp_path / "another" / "pyproject.toml"
    another.parent.mkdir()
    another.write_text('[project]\nname = "another-project"\nversion = "9.9.9"\n')
    broken = tmp_path / "broken" / "pyproject.toml"
    broken.parent.mkdir()
    broken.write_text("[project\n")
    candidates = [another, broken, tmp_path / "missing" / "pyproject.toml"]

    monkeypatch.setattr(importlib.metadata, "version", {"hpc-sandbox": "1.2.3"}.__getitem__)
    assert [hpc_sandbox._running_version(path) for path in candidates] == ["1.2.3"] * 3

    def not_installed(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", not_installed)
    assert [hpc_sandbox._running_version(path) for path in candidates] == ["0+unknown"] * 3


# --------------------------------------------------------------------------- the distribution


def test_the_files_the_distribution_names_exist():
    assert (PACKAGE / "py.typed").is_file(), "type checkers read the annotations only with py.typed"
    assert "Typing :: Typed" in PROJECT["classifiers"]
    for path in PROJECT["license-files"]:
        assert (REPO_ROOT / path).is_file(), path
    targets = PYPROJECT["tool"]["hatch"]["build"]["targets"]
    for path in targets["wheel"]["packages"]:
        assert (REPO_ROOT / path / "__init__.py").is_file(), path
    for path in targets["sdist"]["only-include"]:
        assert (REPO_ROOT / path).exists(), path
    module, _, function = PROJECT["scripts"]["hpc-sandbox"].partition(":")
    assert getattr(importlib.import_module(module), function) is main


def test_pypi_s_readme_links_into_the_repository_at_the_release_tag():
    """PyPI cannot resolve README.md's relative links and images, so the build points them at the
    repository, at the tag the release workflow publishes from (``v`` and the version), so that a
    release's description keeps matching its code and docs after main moves on."""
    assert "hatch-fancy-pypi-readme>=24.1" in PYPROJECT["build-system"]["requires"]  # $HFPR_VERSION
    assert "readme" in PROJECT["dynamic"] and "readme" not in PROJECT
    hook = PYPROJECT["tool"]["hatch"]["metadata"]["hooks"]["fancy-pypi-readme"]
    assert hook["content-type"] == "text/markdown"
    [fragment] = hook["fragments"]
    text = (REPO_ROOT / fragment["path"]).read_text(encoding="utf-8")
    for substitution in hook["substitutions"]:
        text = re.sub(substitution["pattern"], substitution["replacement"], text)
    text = text.replace("$HFPR_VERSION", PROJECT["version"])  # as the hook does, once substituted
    prose = re.sub(r"^```.*?^```", "", text, flags=re.MULTILINE | re.DOTALL)
    links = re.findall(r"\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)", prose)
    images = re.findall(r"<img\b[^>]*?\ssrc=\"([^\"]*)\"", prose)
    relative = [
        target for target in links + images if not re.match(r"[a-z][a-z0-9+.-]*:|#", target)
    ]
    assert links and relative == []
    tag = f"v{PROJECT['version']}"
    into_repository = [target for target in links + images if INTO_REPOSITORY.match(target)]
    elsewhere = [target for target in into_repository if f"/{tag}/" not in target]
    assert into_repository and elsewhere == []
    # A browser shows an image from raw.githubusercontent.com, never from a page of github.com.
    shown_as_pages = [
        target
        for target in images + re.findall(r"!\[[^\]]*\]\(([^)\s]+)", prose)
        if INTO_REPOSITORY.match(target) and "raw.githubusercontent.com" not in target
    ]
    assert shown_as_pages == []
