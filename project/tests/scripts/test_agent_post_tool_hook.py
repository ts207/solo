from __future__ import annotations

import json
from pathlib import Path

from project.scripts import agent_post_tool_hook


def test_agent_post_tool_hook_refreshes_after_lifecycle_command(tmp_path: Path) -> None:
    calls: list[tuple[str, Path]] = []

    def runner(script: str, root: Path) -> dict:
        calls.append((script, root))
        return {"script": script, "returncode": 0, "stdout_tail": "", "stderr_tail": ""}

    status_path = tmp_path / "data" / "reports" / "agent_hook_status.json"
    payload = {
        "tool_input": {
            "command": "PYTHONPATH=. python3 -m project.cli discover run --proposal spec/x.yaml"
        }
    }

    rc = agent_post_tool_hook.run_hook(
        payload,
        root=tmp_path,
        status_path=status_path,
        runner=runner,
    )

    assert rc == 0
    assert [call[0] for call in calls] == list(agent_post_tool_hook.UPDATE_SCRIPTS)
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["triggered"] is True
    assert status["success"] is True


def test_agent_post_tool_hook_skips_unrelated_command(tmp_path: Path) -> None:
    calls: list[tuple[str, Path]] = []

    def runner(script: str, root: Path) -> dict:
        calls.append((script, root))
        return {"script": script, "returncode": 0, "stdout_tail": "", "stderr_tail": ""}

    status_path = tmp_path / "agent_hook_status.json"
    payload = {"tool_input": {"command": "pytest -q project/tests/scripts"}}

    rc = agent_post_tool_hook.run_hook(
        payload,
        root=tmp_path,
        status_path=status_path,
        runner=runner,
    )

    assert rc == 0
    assert calls == []
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["triggered"] is False
    assert status["updates"] == []


def test_agent_post_tool_hook_does_not_refresh_after_discover_doctor(tmp_path: Path) -> None:
    calls: list[tuple[str, Path]] = []

    def runner(script: str, root: Path) -> dict:
        calls.append((script, root))
        return {"script": script, "returncode": 0, "stdout_tail": "", "stderr_tail": ""}

    status_path = tmp_path / "agent_hook_status.json"
    payload = {"tool_input": {"command": "make discover-doctor RUN_ID=abc DATA_ROOT=data"}}

    rc = agent_post_tool_hook.run_hook(
        payload,
        root=tmp_path,
        status_path=status_path,
        runner=runner,
    )

    assert rc == 0
    assert calls == []
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["triggered"] is False
