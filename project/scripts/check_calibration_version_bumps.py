#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

VERSION_FIELDS = ("event_version", "threshold_version")
VERSIONED_CALIBRATION_FIELDS = (
    "calibration_mode",
    "symbol_group",
    "timeframe_group",
    "dataset_lineage",
    "training_period",
    "validation_period",
    "parameters",
)
DEFAULT_PATHS = ("project/events/calibration/artifacts/detectors",)


def _stable(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def changed_versioned_fields(
    old_payload: Mapping[str, Any],
    new_payload: Mapping[str, Any],
) -> tuple[str, ...]:
    return tuple(
        field
        for field in VERSIONED_CALIBRATION_FIELDS
        if _stable(old_payload.get(field)) != _stable(new_payload.get(field))
    )


def version_changed(
    old_payload: Mapping[str, Any],
    new_payload: Mapping[str, Any],
) -> bool:
    return any(
        str(old_payload.get(field, "")).strip() != str(new_payload.get(field, "")).strip()
        for field in VERSION_FIELDS
    )


def calibration_change_requires_version_bump(
    old_payload: Mapping[str, Any],
    new_payload: Mapping[str, Any],
) -> tuple[str, ...]:
    changed_fields = changed_versioned_fields(old_payload, new_payload)
    if not changed_fields or version_changed(old_payload, new_payload):
        return ()
    return changed_fields


def _run_git(repo_root: Path, args: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=check,
    )


def _repo_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode == 0 and result.stdout.strip():
        return Path(result.stdout.strip()).resolve()
    return Path.cwd().resolve()


def _changed_calibration_paths(
    repo_root: Path,
    *,
    base_ref: str,
    pathspecs: Sequence[str],
) -> list[Path]:
    diff = _run_git(
        repo_root,
        [
            "diff",
            "--name-only",
            "--diff-filter=ACMRT",
            base_ref,
            "--",
            *pathspecs,
        ],
    )
    paths = {Path(line.strip()) for line in diff.stdout.splitlines() if line.strip()}

    untracked = _run_git(
        repo_root,
        ["ls-files", "--others", "--exclude-standard", "--", *pathspecs],
    )
    paths.update(Path(line.strip()) for line in untracked.stdout.splitlines() if line.strip())
    return sorted(path for path in paths if path.name == "calibration.json")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_base_json(repo_root: Path, *, base_ref: str, rel_path: Path) -> dict[str, Any] | None:
    result = _run_git(repo_root, ["show", f"{base_ref}:{rel_path.as_posix()}"], check=False)
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return json.loads(result.stdout)


def find_calibration_version_bump_violations(
    *,
    repo_root: Path,
    base_ref: str,
    pathspecs: Sequence[str] = DEFAULT_PATHS,
) -> list[str]:
    violations: list[str] = []
    for rel_path in _changed_calibration_paths(repo_root, base_ref=base_ref, pathspecs=pathspecs):
        current_path = repo_root / rel_path
        if not current_path.exists():
            continue
        old_payload = _load_base_json(repo_root, base_ref=base_ref, rel_path=rel_path)
        if old_payload is None:
            continue
        new_payload = _load_json(current_path)
        changed_fields = calibration_change_requires_version_bump(old_payload, new_payload)
        if changed_fields:
            event_name = str(new_payload.get("event_name") or old_payload.get("event_name") or "").strip()
            violations.append(
                f"{rel_path}: {event_name or 'unknown event'} changed {', '.join(changed_fields)} "
                "without changing event_version or threshold_version"
            )
    return violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fail when calibration threshold payloads change without a version bump."
    )
    parser.add_argument("--base", default="HEAD", help="Git base ref to compare against.")
    parser.add_argument(
        "--path",
        action="append",
        dest="paths",
        help="Calibration artifact pathspec to check. May be passed multiple times.",
    )
    args = parser.parse_args(argv)

    repo_root = _repo_root()
    violations = find_calibration_version_bump_violations(
        repo_root=repo_root,
        base_ref=args.base,
        pathspecs=tuple(args.paths or DEFAULT_PATHS),
    )
    if violations:
        for violation in violations:
            print(f"calibration version bump required: {violation}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
