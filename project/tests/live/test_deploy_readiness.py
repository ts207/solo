from __future__ import annotations

import json
from pathlib import Path

from project.live.deploy_readiness import build_deploy_readiness_report


def _thesis_payload(run_id: str, *, state: str = "monitor_only", allowed_modes: list[str] | None = None) -> dict:
    thesis_id = "thesis_1"
    return {
        "schema_version": "promoted_theses_v1",
        "run_id": run_id,
        "generated_at_utc": "2026-01-01T00:00:00Z",
        "thesis_count": 1,
        "active_thesis_count": 1,
        "pending_thesis_count": 0,
        "theses": [
            {
                "thesis_id": thesis_id,
                "promotion_class": "paper_promoted",
                "deployment_state": state,
                "deployment_mode_allowed": "paper_only",
                "status": "active",
                "timeframe": "5m",
                "primary_event_id": "TEST_EVENT",
                "runtime_manifest": {
                    "thesis_id": thesis_id,
                    "promotion_state": state,
                    "allowed_runtime_modes": allowed_modes or ["monitor_only"],
                },
                "evidence": {"sample_size": 30},
                "lineage": {"run_id": run_id, "candidate_id": "candidate_1"},
            }
        ],
    }


def test_deploy_readiness_reports_missing_thesis_artifact(tmp_path: Path) -> None:
    payload = build_deploy_readiness_report(run_id="missing", runtime_mode="monitor_only", data_root=tmp_path)

    assert payload["status"] == "fail"
    assert payload["checks"][0]["name"] == "thesis_artifact"


def test_deploy_readiness_passes_monitor_with_manifest(tmp_path: Path) -> None:
    run_id = "ready"
    thesis_path = tmp_path / "live" / "theses" / run_id / "promoted_theses.json"
    thesis_path.parent.mkdir(parents=True)
    thesis_path.write_text(json.dumps(_thesis_payload(run_id)), encoding="utf-8")

    payload = build_deploy_readiness_report(run_id=run_id, runtime_mode="monitor_only", data_root=tmp_path)

    assert payload["status"] == "pass"
    assert any(check["name"] == "runtime_manifest" and check["status"] == "pass" for check in payload["checks"])


def test_deploy_readiness_explains_runtime_mode_manifest_block(tmp_path: Path) -> None:
    run_id = "blocked"
    thesis_path = tmp_path / "live" / "theses" / run_id / "promoted_theses.json"
    thesis_path.parent.mkdir(parents=True)
    thesis_path.write_text(json.dumps(_thesis_payload(run_id, state="paper_enabled", allowed_modes=["monitor_only"])), encoding="utf-8")

    payload = build_deploy_readiness_report(run_id=run_id, runtime_mode="simulation", data_root=tmp_path)

    manifest_check = next(check for check in payload["checks"] if check["name"] == "runtime_manifest")
    assert payload["status"] == "fail"
    assert manifest_check["status"] == "fail"
    assert "does not allow" in manifest_check["details"]["theses"][0]["message"]
