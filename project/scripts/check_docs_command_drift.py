#!/usr/bin/env python3
"""Docs command drift checker.

Scans markdown files for forbidden/stale strings that indicate docs have drifted
from the current CLI/Makefile surface. Exits 1 if any violations are found.

Forbidden strings:
  - cd /home/irene/Edge          (hardcoded developer path)
  - edge deploy paper            (removed subcommand; use paper-run)
  - EDGE_ENVIRONMENT=live        (invalid value; use paper or production)
  - live_live_<run_id>.yaml      (old naming; now live_trading_<run_id>.yaml)
  - project/configs/live_live_   (old config prefix)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Files and directories to scan
SCAN_PATHS = [
    REPO_ROOT / "README.md",
    REPO_ROOT / "AGENTS.md",
    REPO_ROOT / "CLAUDE.md",
    REPO_ROOT / "docs",
]

EXCLUDED_PATH_PARTS = {
    "generated",
}

FORBIDDEN: list[tuple[str, str]] = [
    ("cd /home/irene/Edge", "hardcoded developer path — use <repo> or 'from the repo root'"),
    ("EDGE_ENVIRONMENT=live", "invalid EDGE_ENVIRONMENT value — use 'paper' or 'production'"),
    ("live_live_<run_id>.yaml", "stale config name — now live_trading_<run_id>.yaml"),
    ("project/configs/live_live_", "stale config prefix — now project/configs/live_trading_"),
]

# Regex-based forbidden patterns: (compiled_pattern, reason)
FORBIDDEN_RE: list[tuple[re.Pattern[str], str]] = [
    # Catch 'edge deploy paper' not followed by '-' or '_' (so paper-run is OK)
    (
        re.compile(r"edge deploy paper(?![-_])"),
        "removed subcommand -- use 'edge deploy paper-run'",
    ),
]

# Lines containing any of these substrings are exempt from FORBIDDEN_RE checks.
# Allows warning sentences like: '`edge deploy paper` is not a current subcommand.'
SUPPRESSION_SUBSTRINGS: list[str] = [
    "is not a current subcommand",
    "not a current deploy subcommand",
]


def collect_md_files() -> list[Path]:
    files: list[Path] = []
    for p in SCAN_PATHS:
        if p.is_file() and p.suffix == ".md":
            files.append(p)
        elif p.is_dir():
            files.extend(
                path
                for path in sorted(p.rglob("*.md"))
                if not (set(path.relative_to(p).parts) & EXCLUDED_PATH_PARTS)
            )
    return files


def check_file(path: Path) -> list[tuple[int, str, str]]:
    """Return list of (line_number, forbidden_string, reason) for violations."""
    violations: list[tuple[int, str, str]] = []
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return violations

    lines = text.splitlines()
    for lineno, line in enumerate(lines, start=1):
        for forbidden, reason in FORBIDDEN:
            if forbidden in line:
                violations.append((lineno, forbidden, reason))
        for pattern, reason in FORBIDDEN_RE:
            if any(s in line for s in SUPPRESSION_SUBSTRINGS):
                continue
            m = pattern.search(line)
            if m:
                violations.append((lineno, m.group(0), reason))
    return violations


def main() -> int:
    files = collect_md_files()
    total_violations = 0

    for path in files:
        violations = check_file(path)
        if violations:
            rel = path.relative_to(REPO_ROOT)
            for lineno, needle, reason in violations:
                print(f"DRIFT: {rel}:{lineno}  [{needle!r}]  — {reason}", file=sys.stderr)
                total_violations += 1

    if total_violations:
        print(
            f"\nDocs drift checker found {total_violations} violation(s). "
            "Fix the docs before proceeding.",
            file=sys.stderr,
        )
        return 1

    print("Docs drift check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
