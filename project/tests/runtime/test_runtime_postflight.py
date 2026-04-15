from __future__ import annotations

import json
from pathlib import Path

from project.tests.conftest import PROJECT_ROOT

from project.pipelines.pipeline_audit import apply_runtime_postflight_to_manifest
from project.pipelines.pipeline_audit import run_runtime_postflight_audit


def _write_events_csv(path: Path, rows: list[dict[str, object]]) -> None:
    import pandas as pd

    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_runtime_postflight_passes_for_causal_events(tmp_path):
    data_root = tmp_path / "data"
    run_id = "runtime_postflight_pass"
    events_path = data_root / "events" / run_id / "events.csv"
    _write_events_csv(
        events_path,
        [
            {
                "event_id": "e1",
                "event_type": "TREND_DECELERATION",
                "symbol": "BTCUSDT",
                "enter_ts": 1_700_000_000_000_000,
                "detected_ts": 1_700_000_003_000_000,
            },
            {
                "event_id": "e2",
                "event_type": "TREND_DECELERATION",
                "symbol": "BTCUSDT",
                "enter_ts": 1_700_000_005_000_000,
                "detected_ts": 1_700_000_006_000_000,
            },
        ],
    )

    out = run_runtime_postflight_audit(
        run_id=run_id,
        data_root=data_root,
        repo_root=PROJECT_ROOT.parent,
        determinism_replay_checks=False,
    )
    assert out["status"] == "pass"
    assert int(out["watermark_violation_count"]) == 0
    assert int(out["normalization_issue_count"]) == 0
    assert int(out["normalized_event_count"]) == 2


def test_runtime_postflight_fails_on_future_event_time(tmp_path):
    data_root = tmp_path / "data"
    run_id = "runtime_postflight_fail"
    events_path = data_root / "events" / run_id / "events.csv"
    _write_events_csv(
        events_path,
        [
            {
                "event_id": "e1",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                # Event time is 9s ahead of detection time. With 5s max lateness in alpha lane,
                # this violates both future-event and decision-before-watermark checks.
                "enter_ts": 1_700_000_010_000_000,
                "detected_ts": 1_700_000_001_000_000,
            }
        ],
    )

    out = run_runtime_postflight_audit(
        run_id=run_id,
        data_root=data_root,
        repo_root=PROJECT_ROOT.parent,
        determinism_replay_checks=True,
    )
    assert out["status"] == "failed"
    assert int(out["watermark_violation_count"]) >= 1
    by_type = dict(out["watermark_violations_by_type"])
    assert int(by_type.get("future_event_time", 0)) >= 1
    assert out["determinism_replay_checks_status"] == "pass"


def test_runtime_postflight_uses_eval_bar_time_for_next_bar_detector_rows(tmp_path):
    data_root = tmp_path / "data"
    run_id = "runtime_postflight_eval_bar"
    events_path = data_root / "events" / run_id / "events.csv"
    _write_events_csv(
        events_path,
        [
            {
                "event_id": "e1",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "eval_bar_ts": 1_700_000_000_000_000,
                "enter_ts": 1_700_000_300_000_000,
                "detected_ts": 1_700_000_000_000_000,
            }
        ],
    )

    out = run_runtime_postflight_audit(
        run_id=run_id,
        data_root=data_root,
        repo_root=PROJECT_ROOT.parent,
        determinism_replay_checks=False,
    )

    assert out["status"] == "pass"
    assert int(out["watermark_violation_count"]) == 0


def test_runtime_postflight_loads_runtime_replay_reports(tmp_path):
    data_root = tmp_path / "data"
    run_id = "runtime_postflight_replay_reports"
    runtime_dir = data_root / "runs" / run_id / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    events_path = data_root / "events" / run_id / "events.csv"
    _write_events_csv(
        events_path,
        [
            {
                "event_id": "e1",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "enter_ts": 1_700_000_000_000_000,
                "detected_ts": 1_700_000_001_000_000,
            }
        ],
    )
    (runtime_dir / "determinism_replay.json").write_text(
        json.dumps({"status": "failed", "replay_digest": "det-digest"}),
        encoding="utf-8",
    )
    (runtime_dir / "oms_replay_validation.json").write_text(
        json.dumps({"status": "failed", "violation_count": 3, "replay_digest": "oms-digest"}),
        encoding="utf-8",
    )

    out = run_runtime_postflight_audit(
        run_id=run_id,
        data_root=data_root,
        repo_root=PROJECT_ROOT.parent,
        determinism_replay_checks=True,
    )

    assert out["determinism_status"] == "failed"
    assert out["replay_digest"] == "det-digest"
    assert out["oms_replay_status"] == "failed"
    assert int(out["oms_replay_violation_count"]) == 3
    assert out["oms_replay_digest"] == "oms-digest"


def test_apply_runtime_postflight_copies_runtime_firewall_metric():
    manifest: dict[str, object] = {}
    status = apply_runtime_postflight_to_manifest(
        run_manifest=manifest,
        runtime_postflight={
            "status": "failed",
            "event_count": 10,
            "watermark_violation_count": 1,
            "firewall_violation_count": 2,
            "max_observed_lag_us": 42,
        },
    )

    assert status == "failed"
    assert int(manifest["runtime_firewall_violation_count"]) == 2
    assert "firewall_violation_count" not in manifest
