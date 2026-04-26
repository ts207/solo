from __future__ import annotations

from project.scripts.check_commit_msg import validate_message


def test_conventional_commit_accepts_scoped_message() -> None:
    ok, reason = validate_message("fix(research): fail zero feasible runs")
    assert ok
    assert reason == ""


def test_conventional_commit_rejects_single_character_message() -> None:
    ok, reason = validate_message("l")
    assert not ok
    assert "Conventional Commits" in reason
