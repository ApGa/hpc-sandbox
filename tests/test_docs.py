"""The Markdown pages hang together: every relative link and anchor resolves, and the table of
contents of the docs links every page of the docs.

Anchors follow GitHub's rules: a heading's anchor is its text, without its Markdown, lower-cased,
with what is neither a letter, a digit, an underscore, a hyphen nor a space removed and each space
turned into a hyphen; a repeated anchor gets ``-1``, ``-2``... in order. ``<a id="...">`` and
``<a name="...">`` make anchors too. Links are found outside code blocks and code spans: Markdown
links and images, reference definitions, and the ``href`` and ``src`` of HTML tags.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

import pytest

REPO = Path(__file__).resolve().parents[1]
DOCS = REPO / "docs"
ROOT_PAGES = ("README.md", "CONTRIBUTING.md", "SECURITY.md", "CHANGELOG.md")
# The Markdown the repository publishes: the root pages and every page of the docs.
PAGES = sorted(
    {REPO / name for name in ROOT_PAGES if (REPO / name).is_file()} | set(DOCS.rglob("*.md"))
)

_FENCE = re.compile(r"^\s*(`{3,}|~{3,})")  # also in list items, which indent their blocks
_CODE_SPAN = re.compile(r"(`+)(?!`)(.+?)(?<!`)\1(?!`)")
_LINK_TEXT = re.compile(r"!?\[([^\]]*)\]\([^)]*\)")
_INLINE_LINK = re.compile(r"\]\(\s*<?(?P<target>[^)\s>]+)>?(?:\s+(?:\"[^\"]*\"|'[^']*'))?\s*\)")
_REFERENCE = re.compile(r"^ {0,3}\[(?!\^)[^\]]+\]:\s*<?(?P<target>[^\s>]+)>?")  # not footnotes
_HTML_LINK = re.compile(r"<(?:a|img)\b[^>]*?\s(?:href|src)=\"(?P<target>[^\"]*)\"", re.IGNORECASE)
_HTML_ANCHOR = re.compile(r"<a\b[^>]*?\s(?:id|name)=\"(?P<anchor>[^\"]+)\"", re.IGNORECASE)
_HEADING = re.compile(r"^ {0,3}#{1,6}(?:\s+(?P<title>.*?))?(?:\s+#+)?\s*$")
_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*:", re.IGNORECASE)  # http:, https:, mailto: and the like


def _prose(path: Path) -> list[tuple[int, str]]:
    """The lines outside fenced code blocks, with their line numbers."""
    lines: list[tuple[int, str]] = []
    fence = ""
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if match := _FENCE.match(line):
            marker = match[1]
            if not fence:
                fence = marker
            elif (
                marker[0] == fence[0]
                and len(marker) >= len(fence)
                and not line.strip(marker[0] + " ")
            ):
                fence = ""
            continue
        if not fence:
            lines.append((number, line))
    return lines


def _slug(title: str) -> str:
    """GitHub's anchor for a heading: its text as GitHub shows it (links and images as their text,
    code as written, no HTML tags or emphasis markers), turned into an anchor."""
    pieces = _CODE_SPAN.split(_LINK_TEXT.sub(r"\1", title))  # prose, ticks, code, prose...
    text = "".join(
        piece if index % 3 == 2 else re.sub(r"<[^>]+>|\*+|(?<!\w)_+|_+(?!\w)", "", piece)
        for index, piece in enumerate(pieces)
        if index % 3 != 1
    )
    return re.sub(r"[^\w\- ]", "", text.strip().lower()).replace(" ", "-")


def _anchors(path: Path) -> set[str]:
    """The anchors of a page: one per heading, repeats numbered, and the ``<a id>`` ones."""
    anchors: set[str] = set()
    seen: dict[str, int] = {}
    for _, line in _prose(path):
        anchors.update(match["anchor"] for match in _HTML_ANCHOR.finditer(line))
        if heading := _HEADING.match(line):
            slug = _slug(heading["title"] or "")
            anchors.add(f"{slug}-{seen[slug]}" if slug in seen else slug)
            seen[slug] = seen.get(slug, 0) + 1
    return anchors


def _links(path: Path) -> list[tuple[int, str]]:
    """The relative link targets of a page, with their line numbers."""
    links = []
    for number, line in _prose(path):
        line = _CODE_SPAN.sub("", line)
        for pattern in (_INLINE_LINK, _REFERENCE, _HTML_LINK):
            links += [(number, match["target"]) for match in pattern.finditer(line)]
    return [(number, target) for number, target in links if not _SCHEME.match(target)]


def _resolve(page: Path, target: str) -> tuple[Path, str]:
    """The file a link target names (the page itself for an anchor alone), and its anchor."""
    path_text, _, anchor = target.partition("#")
    path = (page.parent / unquote(path_text)).resolve() if path_text else page
    return path, anchor


def test_the_root_pages_and_the_docs_exist():
    missing = [name for name in ROOT_PAGES if not (REPO / name).is_file()]
    assert missing == [] and (DOCS / "README.md").is_file()


@pytest.mark.parametrize("page", PAGES, ids=lambda path: path.relative_to(REPO).as_posix())
def test_relative_links_and_anchors_resolve(page: Path) -> None:
    broken = []
    for number, target in _links(page):
        path, anchor = _resolve(page, target)
        if not path.is_relative_to(REPO):
            broken.append(f"line {number}: {target} (outside the repository)")
        elif not path.exists():
            broken.append(f"line {number}: {target} (no such file)")
        elif anchor and path.suffix == ".md" and anchor not in _anchors(path):
            broken.append(f"line {number}: {target} (no such heading)")
    assert not broken, f"{page.relative_to(REPO)} has broken links:\n" + "\n".join(broken)


def test_the_docs_table_of_contents_links_every_page() -> None:
    index = DOCS / "README.md"
    linked = {_resolve(index, target)[0] for _, target in _links(index)}
    pages = {path.resolve() for path in DOCS.rglob("*.md")} - {index.resolve()}
    missing = sorted(path.relative_to(DOCS).as_posix() for path in pages - linked)
    assert not missing, f"docs/README.md does not link {missing}"


def test_anchors_follow_github_s_rules(tmp_path: Path) -> None:
    page = tmp_path / "page.md"
    page.write_text(
        "# hpc-sandbox's `run` command\n"
        "## Python API: [`SandboxSpec`](types.md) and **Mount**\n"
        "## Next steps\n"
        "```text\n## Not a heading\n```\n"
        "### Next steps ###\n"
        '<a id="custom"></a>\n'
        "## The `HPC_SANDBOX_SCRATCH` variable and `_private` names\n"
        "## Exit codes (`124`, `126`, `137`) of _the_ command\n"
    )
    assert _anchors(page) == {
        "hpc-sandboxs-run-command",
        "python-api-sandboxspec-and-mount",
        "next-steps",
        "next-steps-1",
        "custom",
        "the-hpc_sandbox_scratch-variable-and-_private-names",
        "exit-codes-124-126-137-of-the-command",
    }
