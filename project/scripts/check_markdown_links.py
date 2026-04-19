from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

_MARKDOWN_LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_EXTERNAL_SCHEMES = frozenset({"http", "https", "mailto", "tel", "ftp"})


@dataclass(frozen=True)
class MarkdownLinkIssue:
    markdown_path: str
    target: str
    reason: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _iter_markdown_files(root: Path, *, include_generated: bool) -> list[Path]:
    candidates: list[Path] = []
    for path in sorted(root.glob("*.md")):
        candidates.append(path)
    docs_root = root / "docs"
    if docs_root.exists():
        for path in sorted(docs_root.rglob("*.md")):
            if not include_generated and "generated" in path.relative_to(docs_root).parts:
                continue
            candidates.append(path)
    return sorted(set(candidates))


def _is_external_or_anchor(target: str) -> bool:
    if not target or target.startswith("#"):
        return True
    parsed = urlparse(target)
    return bool(parsed.scheme and parsed.scheme in _EXTERNAL_SCHEMES)


def _strip_target(target: str) -> str:
    cleaned = target.strip().strip("<>")
    return unquote(cleaned.split("#", 1)[0])


def _candidate_paths(root: Path, markdown_path: Path, target: str) -> list[Path]:
    path_text = _strip_target(target)
    if not path_text:
        return []
    path = Path(path_text)
    if path.is_absolute():
        return [path, root / path_text.lstrip("/")]
    return [markdown_path.parent / path, root / path]


def _target_exists(root: Path, markdown_path: Path, target: str) -> bool:
    if _is_external_or_anchor(target):
        return True
    return any(path.exists() for path in _candidate_paths(root, markdown_path, target))


def collect_markdown_link_issues(
    *,
    root: Path | None = None,
    include_generated: bool = False,
) -> list[MarkdownLinkIssue]:
    repo_root = (root or _repo_root()).resolve()
    issues: list[MarkdownLinkIssue] = []
    for markdown_path in _iter_markdown_files(repo_root, include_generated=include_generated):
        rel_md = markdown_path.relative_to(repo_root).as_posix()
        text = markdown_path.read_text(encoding="utf-8")
        for match in _MARKDOWN_LINK_RE.finditer(text):
            target = match.group(1).strip()
            if _target_exists(repo_root, markdown_path, target):
                continue
            issues.append(
                MarkdownLinkIssue(
                    markdown_path=rel_md,
                    target=target,
                    reason="target does not resolve relative to document or repo root",
                )
            )
    return issues


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check internal Markdown links.")
    parser.add_argument(
        "--include-generated",
        action="store_true",
        help="Also scan docs/generated Markdown artifacts.",
    )
    args = parser.parse_args(argv)

    issues = collect_markdown_link_issues(include_generated=bool(args.include_generated))
    if not issues:
        print("markdown links: OK")
        return 0
    for issue in issues:
        print(
            f"{issue.markdown_path}: broken link {issue.target!r}: {issue.reason}",
            file=sys.stderr,
        )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
