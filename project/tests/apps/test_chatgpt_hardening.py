import os
from pathlib import Path
import pytest
from project.apps.chatgpt.tool_catalog import get_tool_catalog
from project.apps.chatgpt.handlers import discover_run, promote_run, validate_run
from project.apps.chatgpt.handler_utils import RunLock, guard_mutation_path, check_app_mode

def test_tool_catalog_admin_filtering():
    # Without admin tools enabled
    os.environ.pop("EDGE_ENABLE_ADMIN_TOOLS", None)
    catalog = get_tool_catalog("operator")
    assert "edge_invoke_operator" not in [t.name for t in catalog]

    # With admin tools enabled
    os.environ["EDGE_ENABLE_ADMIN_TOOLS"] = "1"
    catalog = get_tool_catalog("operator")
    assert "edge_invoke_operator" in [t.name for t in catalog]

def test_discover_run_enforces_confirmations():
    # Missing confirmations
    result = discover_run(proposal="test.yaml")
    assert result["status"] == "blocked"
    assert "Missing required operator confirmations" in result["message"]

    # Partial confirmations
    result = discover_run(
        proposal="test.yaml",
        confirmations={"understands_writes_artifacts": True}
    )
    assert result["status"] == "blocked"

def test_promote_run_enforces_confirmations():
    # Missing confirmations
    result = promote_run(run_id="test_run", symbols="BTCUSDT")
    assert result["status"] == "blocked"
    assert "Missing required operator confirmations" in result["message"]

def test_run_lock(tmp_path):
    data_root = tmp_path
    run_id = "test_run_123"

    with RunLock(run_id, data_root):
        # Attempting to lock again should fail
        with pytest.raises(RuntimeError, match=f"Run {run_id} is currently locked"):
            with RunLock(run_id, data_root):
                pass

def test_guard_mutation_path(tmp_path):
    # This might be tricky because of _repo_root() resolve, but we can test blocked patterns
    with pytest.raises(PermissionError, match="Mutation blocked for protected path"):
        guard_mutation_path("data/live/theses/run123")

    with pytest.raises(PermissionError, match="Mutation blocked for protected path"):
        guard_mutation_path(".env")

    # Scratch dir should be allowed
    guard_mutation_path("/tmp/edge_chatgpt_123/out")

def test_check_app_mode():
    # Default is paper_only, so trading should be blocked
    os.environ["EDGE_CHATGPT_APP_MODE"] = "paper_only"
    with pytest.raises(PermissionError, match="forbidden pattern"):
        check_app_mode({"runtime_mode": "trading"})

    with pytest.raises(PermissionError, match="forbidden pattern"):
        check_app_mode({"command": "edge deploy live-run"})

    # Read-only mode
    os.environ["EDGE_CHATGPT_APP_MODE"] = "read_only"
    with pytest.raises(PermissionError, match="read-only mode"):
        check_app_mode({})

def test_ffbf_regression_research_pass_live_fail(tmp_path, monkeypatch):
    """
    Regression test for the FFBF failure mode:
    Validation passes, research promotion succeeds (candidates found),
    but promotion gates fail for live export.
    Ensure no live thesis batch is created.
    """
    from unittest.mock import MagicMock
    import project.promote

    # Ensure we are in a mode that allows mutations
    os.environ["EDGE_CHATGPT_APP_MODE"] = "paper_only"

    run_id = "ffbf_run_123"
    data_root = tmp_path
    theses_dir = data_root / "live" / "theses" / run_id

    # Mock the underlying promote.run to simulate the FFBF state:
    # Exit code 0 (success for research/report)
    # but thesis_count 0 (no live export because gates failed)
    mock_result = MagicMock()
    mock_result.exit_code = 0
    mock_result.thesis_count = 0
    mock_result.output_path = None
    mock_result.diagnostics = {
        "research_promoted_candidates_count": 5,
        "no_live_thesis_export_reason": "Promotion gates failed for all candidates",
    }

    monkeypatch.setattr(project.promote, "run", lambda **kwargs: mock_result)

    # Call promote_run with required confirmations
    result = promote_run(
        run_id=run_id,
        symbols="BTCUSDT",
        data_root=str(data_root),
        confirmations={
            "canonical_validation_passed": True,
            "promotion_gates_must_hold": True,
            "do_not_export_rejected_candidates": True,
            "confirm_governed_write": True,
        }
    )

    assert result["status"] == "promoted"
    assert result["thesis_count"] == 0
    assert result["diagnostics"]["research_promoted_candidates_count"] > 0
    assert "no_live_thesis_export_reason" in result["diagnostics"]

    # CRITICAL INVARIANT: The live theses directory must NOT contain the thesis file
    assert not (theses_dir / "promoted_theses.json").exists()
