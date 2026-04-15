from __future__ import annotations

from pathlib import Path

from project.apps.chatgpt.handlers import invoke_codex_operator


def test_invoke_codex_operator_starts_codex_mcp_session(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []
    snapshots = iter(
        [
            {"data_root": str(tmp_path), "recent_run_ids": [], "proposal_counts": {}},
            {"data_root": str(tmp_path), "recent_run_ids": ["run-9"], "proposal_counts": {"prog": 2}},
        ]
    )

    def fake_which(name: str) -> str | None:
        assert name == "codex"
        return "/usr/bin/codex"

    async def fake_run_codex_mcp_tool(
        codex_path: str,
        tool_name: str,
        arguments: dict[str, object],
        timeout_sec: int,
    ) -> dict[str, object]:
        calls.append(
            {
                "codex_path": codex_path,
                "tool_name": tool_name,
                "arguments": dict(arguments),
                "timeout_sec": timeout_sec,
            }
        )
        return {
            "tool_name": tool_name,
            "thread_id": "thread-123",
            "final_message": "Codex completed task.",
            "structured_content": {"threadId": "thread-123", "content": "Codex completed task."},
            "content": [{"type": "text", "text": "Codex completed task."}],
            "is_error": False,
        }

    monkeypatch.setattr("project.apps.chatgpt.handlers.shutil.which", fake_which)
    monkeypatch.setattr("project.apps.chatgpt.handlers._run_codex_mcp_tool", fake_run_codex_mcp_tool)
    monkeypatch.setattr("project.apps.chatgpt.handlers._resolve_data_root", lambda _value: tmp_path)
    monkeypatch.setattr("project.apps.chatgpt.handlers._snapshot_operator_state", lambda _root: next(snapshots))
    monkeypatch.setattr(
        "project.apps.chatgpt.handlers._diff_operator_state",
        lambda before, after: {
            "data_root": str(tmp_path),
            "new_run_ids": ["run-9"],
            "proposal_memory_changes": [{"program_id": "prog", "before_count": 0, "after_count": 2, "delta": 2}],
            "dashboard_changed": True,
        },
    )

    result = invoke_codex_operator(
        task="Inspect the Edge operator surface.",
        sandbox="read-only",
        model="gpt-5-codex",
        profile="default",
        timeout_sec=90,
    )

    assert calls
    call = calls[0]
    assert call["codex_path"] == "/usr/bin/codex"
    assert call["tool_name"] == "codex"
    assert call["timeout_sec"] == 90
    assert call["arguments"] == {
        "prompt": "Inspect the Edge operator surface.",
        "cwd": str(Path(__file__).parents[3]),
        "sandbox": "read-only",
        "model": "gpt-5-codex",
        "profile": "default",
    }

    assert result["status"] == "success"
    assert result["timeout_sec"] == 90
    assert result["timed_out"] is False
    assert result["tool_name"] == "codex"
    assert result["thread_id"] == "thread-123"
    assert result["final_message"] == "Codex completed task."
    assert result["structured_content"]["threadId"] == "thread-123"
    assert result["post_run_probe"]["dashboard_changed"] is True
    assert result["post_run_probe"]["new_run_ids"] == ["run-9"]


def test_invoke_codex_operator_continues_existing_thread(monkeypatch, tmp_path: Path) -> None:
    snapshots = iter(
        [
            {"data_root": str(tmp_path), "recent_run_ids": [], "proposal_counts": {"prog": 1}},
            {"data_root": str(tmp_path), "recent_run_ids": ["run-timeout"], "proposal_counts": {"prog": 2}},
        ]
    )

    async def fake_run_codex_mcp_tool(
        codex_path: str,
        tool_name: str,
        arguments: dict[str, object],
        timeout_sec: int,
    ) -> dict[str, object]:
        assert codex_path == "/usr/bin/codex"
        assert timeout_sec == 45
        assert tool_name == "codex-reply"
        assert arguments == {"prompt": "Repair artifacts.", "threadId": "thread-timeout"}
        return {
            "tool_name": tool_name,
            "thread_id": "thread-timeout",
            "final_message": "Partial Codex result.",
            "structured_content": {"threadId": "thread-timeout", "content": "Partial Codex result."},
            "content": [{"type": "text", "text": "Partial Codex result."}],
            "is_error": False,
        }

    monkeypatch.setattr("project.apps.chatgpt.handlers.shutil.which", lambda _name: "/usr/bin/codex")
    monkeypatch.setattr("project.apps.chatgpt.handlers._run_codex_mcp_tool", fake_run_codex_mcp_tool)
    monkeypatch.setattr("project.apps.chatgpt.handlers._resolve_data_root", lambda _value: tmp_path)
    monkeypatch.setattr("project.apps.chatgpt.handlers._snapshot_operator_state", lambda _root: next(snapshots))
    monkeypatch.setattr(
        "project.apps.chatgpt.handlers._diff_operator_state",
        lambda before, after: {
            "data_root": str(tmp_path),
            "new_run_ids": ["run-timeout"],
            "proposal_memory_changes": [{"program_id": "prog", "before_count": 1, "after_count": 2, "delta": 1}],
            "dashboard_changed": True,
        },
    )

    result = invoke_codex_operator(
        task="Repair artifacts.",
        sandbox="workspace-write",
        thread_id="thread-timeout",
        timeout_sec=45,
    )

    assert result["status"] == "success"
    assert result["exit_code"] is None
    assert result["timeout_sec"] == 45
    assert result["timed_out"] is False
    assert result["tool_name"] == "codex-reply"
    assert result["thread_id"] == "thread-timeout"
    assert result["final_message"] == "Partial Codex result."
    assert result["post_run_probe"]["dashboard_changed"] is True
    assert result["post_run_probe"]["new_run_ids"] == ["run-timeout"]


def test_invoke_codex_operator_returns_timeout_payload(monkeypatch, tmp_path: Path) -> None:
    snapshots = iter(
        [
            {"data_root": str(tmp_path), "recent_run_ids": [], "proposal_counts": {}},
            {"data_root": str(tmp_path), "recent_run_ids": [], "proposal_counts": {}},
        ]
    )

    async def fake_run_codex_mcp_tool(
        codex_path: str,
        tool_name: str,
        arguments: dict[str, object],
        timeout_sec: int,
    ) -> dict[str, object]:
        assert codex_path == "/usr/bin/codex"
        assert tool_name == "codex"
        assert arguments["prompt"] == "Repair the repo."
        assert timeout_sec == 300
        raise TimeoutError

    monkeypatch.setattr("project.apps.chatgpt.handlers.shutil.which", lambda _name: "/usr/bin/codex")
    monkeypatch.setattr("project.apps.chatgpt.handlers._run_codex_mcp_tool", fake_run_codex_mcp_tool)
    monkeypatch.setattr("project.apps.chatgpt.handlers._resolve_data_root", lambda _value: tmp_path)
    monkeypatch.setattr("project.apps.chatgpt.handlers._snapshot_operator_state", lambda _root: next(snapshots))

    result = invoke_codex_operator(
        task="Repair the repo.",
        sandbox="workspace-write",
        timeout_sec=None,
    )

    assert result["status"] == "timeout"
    assert result["timeout_sec"] == 300
    assert result["timed_out"] is True
    assert result["tool_name"] == "codex"


def test_invoke_codex_operator_reports_missing_cli(monkeypatch) -> None:
    monkeypatch.setattr("project.apps.chatgpt.handlers.shutil.which", lambda _name: None)

    try:
        invoke_codex_operator(task="noop")
    except RuntimeError as exc:
        assert "`codex` CLI" in str(exc)
    else:
        raise AssertionError("expected RuntimeError when codex CLI is missing")
