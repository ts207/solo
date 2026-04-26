#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PATTERN = re.compile(
    r"^(feat|fix|refactor|test|docs|chore|style|perf|build|ci|revert)"
    r"(\([\w\-]+\))?: .{8,}$"
)


def validate_message(message: str) -> tuple[bool, str]:
    first_line = message.strip().splitlines()[0] if message.strip() else ""
    if PATTERN.match(first_line):
        return True, ""
    return (
        False,
        "Commit message must follow Conventional Commits, e.g. "
        "'fix(research): fail zero-feasible discovery runs'.",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a Conventional Commit message.")
    parser.add_argument("message_file", nargs="?", help="commit message file passed by pre-commit")
    parser.add_argument("--message", help="message text, useful for tests")
    args = parser.parse_args(argv)

    if args.message is not None:
        message = args.message
    elif args.message_file:
        message = Path(args.message_file).read_text(encoding="utf-8")
    else:
        message = sys.stdin.read()

    ok, reason = validate_message(message)
    if not ok:
        print(reason)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
