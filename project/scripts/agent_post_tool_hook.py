#!/usr/bin/env python3
"""Claude PostToolUse hook for research artifact refreshes."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
STATUS_PATH = ROOT / "data" / "reports" / "agent_hook_status.json"

_LIFECYCLE_RE = re.compile(
    r"("
    r"\b(project\.cli|edge)\s+(discover\s+(run|cells\s+run)|validate\s+run|promote\s+run)\b"
    r"|\bpython3?\s+-m\s+project\.cli\s+"
    r"(discover\s+(run|cells\s+run)|validate\s+run|promote\s+run)\b"
    r"|\bmake\s+(first-edge|discover|discover-proposal|discover-cells-run|validate|promote)(?=\s|$)"
    r"|\bproject\.pipelines\.run_all\b"
    r")"
)

UPDATE_SCRIPTS = (
    "project/scripts/update_results_index.py",
    "project/scripts/update_reflections.py",
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _extract_command(payload: dict[str, Any]) -> str:
    tool_input = payload.get("tool_input")
    if not isinstance(tool_input, dict):
        return ""
    command = tool_input.get("command", tool_input.get("cmd", ""))
    return command if isinstance(command, str) else ""


def _should_refresh(command: str) -> bool:
    return bool(_LIFECYCLE_RE.search(command))


def _tail(text: str, limit: int = 4000) -> str:
    return text[-limit:] if len(text) > limit else text


def _run_update(script: str, root: Path) -> dict[str, Any]:
    env = {**os.environ, "PYTHONPATH": "."}
    result = subprocess.run(
        [sys.executable, script],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "script": script,
        "returncode": result.returncode,
        "stdout_tail": _tail(result.stdout),
        "stderr_tail": _tail(result.stderr),
    }


def _write_status(status_path: Path, payload: dict[str, Any]) -> None:
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_hook(
    payload: dict[str, Any],
    *,
    root: Path = ROOT,
    status_path: Path = STATUS_PATH,
    runner: Callable[[str, Path], dict[str, Any]] = _run_update,
) -> int:
    command = _extract_command(payload)
    triggered = _should_refresh(command)

    status: dict[str, Any] = {
        "timestamp_utc": _utc_now(),
        "repo_root": str(root),
        "command": command,
        "triggered": triggered,
        "updates": [],
        "success": True,
    }

    if triggered:
        status["updates"] = [runner(script, root) for script in UPDATE_SCRIPTS]
        status["success"] = all(update.get("returncode") == 0 for update in status["updates"])

    _write_status(status_path, status)
    return 0 if status["success"] else 1


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        _write_status(
            STATUS_PATH,
            {
                "timestamp_utc": _utc_now(),
                "repo_root": str(ROOT),
                "triggered": False,
                "success": False,
                "error": f"invalid hook payload: {exc}",
            },
        )
        return 1

    if not isinstance(payload, dict):
        _write_status(
            STATUS_PATH,
            {
                "timestamp_utc": _utc_now(),
                "repo_root": str(ROOT),
                "triggered": False,
                "success": False,
                "error": "invalid hook payload: expected object",
            },
        )
        return 1

    return run_hook(payload)


if __name__ == "__main__":
    raise SystemExit(main())
