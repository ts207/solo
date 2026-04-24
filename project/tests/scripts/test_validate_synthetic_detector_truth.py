from __future__ import annotations

import json

import pandas as pd

from project.io.utils import ensure_dir, write_parquet
from project.scripts import validate_synthetic_detector_truth as truth_script
from project.scripts.validate_synthetic_detector_truth import validate_detector_truth


def _write_event_report(
    tmp_path, run_id: str, reports_dir: str, events_file: str, rows: list[dict]
) -> None:
    out_dir = tmp_path / "reports" / reports_dir / run_id
    ensure_dir(out_dir)
    write_parquet(pd.DataFrame(rows), out_dir / events_file)


def test_validate_synthetic_detector_truth_scores_expected_windows(tmp_path):
    run_id = "truth_run"
    truth_dir = tmp_path / "synthetic" / run_id
    ensure_dir(truth_dir)
    truth_path = truth_dir / "synthetic_regime_segments.json"
    truth_payload = {
        "run_id": run_id,
        "segments": [
            {
                "regime_type": "basis_desync",
                "symbol": "BTCUSDT",
                "start_ts": "2026-01-02T00:00:00Z",
                "end_ts": "2026-01-02T02:00:00Z",
                "expected_event_types": ["CROSS_VENUE_DESYNC"],
                "expected_detector_families": ["cross_venue_desync"],
                "intended_effect_direction": "desync_signaled",
            }
        ],
    }
    truth_path.write_text(json.dumps(truth_payload, indent=2), encoding="utf-8")

    _write_event_report(
        tmp_path,
        run_id,
        "cross_venue_desync",
        "cross_venue_desync_events.parquet",
        [
            {
                "symbol": "BTCUSDT",
                "event_type": "CROSS_VENUE_DESYNC",
                "enter_ts": "2026-01-02T00:30:00Z",
            },
            {
                "symbol": "BTCUSDT",
                "event_type": "CROSS_VENUE_DESYNC",
                "enter_ts": "2026-01-05T00:30:00Z",
            },
        ],
    )

    result = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_path,
        tolerance_minutes=30,
        max_off_regime_rate=0.75,
    )
    assert result["passed"] is True
    report = result["event_reports"][0]
    assert report["event_type"] == "CROSS_VENUE_DESYNC"
    assert report["per_symbol"][0]["windows_hit"] == 1
    assert report["per_symbol"][0]["off_regime_events"] == 1


def test_tolerance_minutes_accepts_dict():
    """validate_detector_truth must accept tolerance_minutes as a dict."""
    import inspect

    from project.scripts.validate_synthetic_detector_truth import validate_detector_truth

    sig = inspect.signature(validate_detector_truth)
    assert "tolerance_minutes" in sig.parameters


def test_tolerance_dict_uses_per_event_type_value(tmp_path):
    """When tolerance_minutes is a dict, event-type-specific values are used."""
    from project.scripts.validate_synthetic_detector_truth import validate_detector_truth

    truth_map = {
        "segments": [
            {
                "regime_type": "test",
                "symbol": "BTCUSDT",
                "start_ts": "2024-01-01T01:00:00+00:00",
                "end_ts": "2024-01-01T02:00:00+00:00",
                "sign": 1,
                "amplitude": 1.0,
                "intended_effect_direction": "test",
                "expected_event_types": ["VOL_SPIKE"],
                "expected_detector_families": [],
            }
        ]
    }
    truth_map_path = tmp_path / "truth.json"
    truth_map_path.write_text(json.dumps(truth_map))
    result = validate_detector_truth(
        data_root=tmp_path,
        run_id="test_run",
        truth_map_path=truth_map_path,
        tolerance_minutes={"VOL_SPIKE": 60, "BASIS_DISLOC": 15},
    )
    assert isinstance(result, dict)
    assert "passed" in result


def test_validate_synthetic_detector_truth_fails_when_expected_detector_misses(tmp_path):
    run_id = "truth_fail"
    truth_dir = tmp_path / "synthetic" / run_id
    ensure_dir(truth_dir)
    truth_path = truth_dir / "synthetic_regime_segments.json"
    truth_payload = {
        "run_id": run_id,
        "segments": [
            {
                "regime_type": "deleveraging_burst",
                "symbol": "ETHUSDT",
                "start_ts": "2026-01-03T00:00:00Z",
                "end_ts": "2026-01-03T01:00:00Z",
                "expected_event_types": ["DELEVERAGING_WAVE"],
                "expected_detector_families": ["positioning_extremes"],
                "intended_effect_direction": "forced_deleveraging",
            }
        ],
    }
    truth_path.write_text(json.dumps(truth_payload, indent=2), encoding="utf-8")

    _write_event_report(
        tmp_path,
        run_id,
        "positioning_extremes",
        "positioning_extremes_events.parquet",
        [
            {
                "symbol": "ETHUSDT",
                "event_type": "DELEVERAGING_WAVE",
                "enter_ts": "2026-01-07T00:30:00Z",
            },
        ],
    )

    result = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_path,
        tolerance_minutes=15,
        max_off_regime_rate=0.75,
    )
    assert result["passed"] is False
    assert result["event_reports"][0]["per_symbol"][0]["windows_hit"] == 0
    assert result["event_reports"][0]["per_symbol"][0]["passed_hit_requirement"] is False


def test_validate_synthetic_detector_truth_can_scope_to_selected_events(tmp_path):
    run_id = "truth_subset"
    truth_dir = tmp_path / "synthetic" / run_id
    ensure_dir(truth_dir)
    truth_path = truth_dir / "synthetic_regime_segments.json"
    truth_payload = {
        "run_id": run_id,
        "segments": [
            {
                "regime_type": "funding_dislocation",
                "symbol": "BTCUSDT",
                "start_ts": "2026-01-02T00:00:00Z",
                "end_ts": "2026-01-02T02:00:00Z",
                "expected_event_types": ["FND_DISLOC"],
            },
            {
                "regime_type": "deleveraging_burst",
                "symbol": "BTCUSDT",
                "start_ts": "2026-01-04T00:00:00Z",
                "end_ts": "2026-01-04T01:00:00Z",
                "expected_event_types": ["DELEVERAGING_WAVE"],
            },
        ],
    }
    truth_path.write_text(json.dumps(truth_payload, indent=2), encoding="utf-8")

    _write_event_report(
        tmp_path,
        run_id,
        "funding_dislocation",
        "funding_dislocation_events.parquet",
        [{"symbol": "BTCUSDT", "event_type": "FND_DISLOC", "enter_ts": "2026-01-02T00:30:00Z"}],
    )

    result = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_path,
        event_types=["FND_DISLOC"],
    )

    assert result["passed"] is True
    assert result["selected_event_types"] == ["FND_DISLOC"]
    assert [report["event_type"] for report in result["event_reports"]] == ["FND_DISLOC"]


def test_validate_synthetic_detector_truth_ignores_supporting_events_by_default(tmp_path):
    run_id = "truth_supporting_only"
    truth_dir = tmp_path / "synthetic" / run_id
    ensure_dir(truth_dir)
    truth_path = truth_dir / "synthetic_regime_segments.json"
    truth_payload = {
        "run_id": run_id,
        "segments": [
            {
                "regime_type": "liquidity_stress",
                "symbol": "BTCUSDT",
                "start_ts": "2026-01-02T00:00:00Z",
                "end_ts": "2026-01-02T02:00:00Z",
                "expected_event_types": ["LIQUIDITY_STRESS_DIRECT"],
                "supporting_event_types": ["PRICE_VOL_IMBALANCE_PROXY"],
            },
        ],
    }
    truth_path.write_text(json.dumps(truth_payload, indent=2), encoding="utf-8")

    _write_event_report(
        tmp_path,
        run_id,
        "liquidity_dislocation",
        "liquidity_dislocation_events.parquet",
        [
            {
                "symbol": "BTCUSDT",
                "event_type": "PRICE_VOL_IMBALANCE_PROXY",
                "enter_ts": "2026-01-02T00:30:00Z",
            }
        ],
    )

    result = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_path,
    )

    assert [report["event_type"] for report in result["event_reports"]] == [
        "LIQUIDITY_STRESS_DIRECT"
    ]
    assert result["supporting_event_reports"] == []


def test_validate_synthetic_detector_truth_can_report_supporting_events(tmp_path):
    run_id = "truth_supporting_report"
    truth_dir = tmp_path / "synthetic" / run_id
    ensure_dir(truth_dir)
    truth_path = truth_dir / "synthetic_regime_segments.json"
    truth_payload = {
        "run_id": run_id,
        "segments": [
            {
                "regime_type": "liquidity_stress",
                "symbol": "BTCUSDT",
                "start_ts": "2026-01-02T00:00:00Z",
                "end_ts": "2026-01-02T02:00:00Z",
                "expected_event_types": ["LIQUIDITY_STRESS_DIRECT"],
                "supporting_event_types": ["ABSORPTION_PROXY", "DEPTH_STRESS_PROXY"],
            },
        ],
    }
    truth_path.write_text(json.dumps(truth_payload, indent=2), encoding="utf-8")

    _write_event_report(
        tmp_path,
        run_id,
        "liquidity_dislocation",
        "liquidity_dislocation_events.parquet",
        [
            {
                "symbol": "BTCUSDT",
                "event_type": "ABSORPTION_PROXY",
                "enter_ts": "2026-01-02T00:30:00Z",
            },
            {
                "symbol": "BTCUSDT",
                "event_type": "DEPTH_STRESS_PROXY",
                "enter_ts": "2026-01-02T01:00:00Z",
            },
        ],
    )

    result = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_path,
        include_supporting_events=True,
    )

    assert result["passed"] is False
    assert [report["event_type"] for report in result["event_reports"]] == [
        "LIQUIDITY_STRESS_DIRECT"
    ]
    assert [report["event_type"] for report in result["supporting_event_reports"]] == [
        "ABSORPTION_PROXY",
        "DEPTH_STRESS_PROXY",
    ]
    assert all(
        report["truth_role"] == "supporting" for report in result["supporting_event_reports"]
    )
    assert result["supporting_event_reports"][0]["per_symbol"][0]["windows_hit"] == 1
    assert result["supporting_event_reports"][1]["per_symbol"][0]["windows_hit"] == 1


def test_validate_synthetic_detector_truth_prefers_event_specific_truth_windows(tmp_path):
    run_id = "truth_event_specific_windows"
    truth_dir = tmp_path / "synthetic" / run_id
    ensure_dir(truth_dir)
    truth_path = truth_dir / "synthetic_regime_segments.json"
    truth_payload = {
        "run_id": run_id,
        "segments": [
            {
                "regime_type": "liquidity_stress",
                "symbol": "BTCUSDT",
                "start_ts": "2026-01-02T00:00:00Z",
                "end_ts": "2026-01-02T08:00:00Z",
                "supporting_event_types": ["ABSORPTION_PROXY"],
                "event_truth_windows": {
                    "ABSORPTION_PROXY": [
                        {
                            "start_ts": "2026-01-02T05:00:00Z",
                            "end_ts": "2026-01-02T08:00:00Z",
                        }
                    ]
                },
            },
        ],
    }
    truth_path.write_text(json.dumps(truth_payload, indent=2), encoding="utf-8")

    _write_event_report(
        tmp_path,
        run_id,
        "liquidity_dislocation",
        "liquidity_dislocation_events.parquet",
        [
            {
                "symbol": "BTCUSDT",
                "event_type": "ABSORPTION_PROXY",
                "enter_ts": "2026-01-02T01:00:00Z",
            },
            {
                "symbol": "BTCUSDT",
                "event_type": "ABSORPTION_PROXY",
                "enter_ts": "2026-01-02T06:00:00Z",
            },
        ],
    )

    result = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_path,
        include_supporting_events=True,
        tolerance_minutes=0,
    )

    report = result["supporting_event_reports"][0]
    assert report["event_type"] == "ABSORPTION_PROXY"
    assert report["per_symbol"][0]["windows_hit"] == 1
    assert report["per_symbol"][0]["in_window_events"] == 1
    assert report["per_symbol"][0]["off_regime_events"] == 1


def _write_truth_map(tmp_path, content):
    import json

    p = tmp_path / "truth_map.json"
    p.write_text(json.dumps(content), encoding="utf-8")
    return p


def _write_vol_shock_events(data_root, run_id, rows):
    """Write VOL_SHOCK events into the directory structure expected by load_event_frame."""
    from project.events.registry import EVENT_REGISTRY_SPECS

    spec = EVENT_REGISTRY_SPECS["VOL_SHOCK"]
    out_dir = data_root / "reports" / spec.reports_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    import pandas as pd

    df = pd.DataFrame(rows)
    df.to_parquet(out_dir / spec.events_file)


def test_rejects_high_off_regime_rate(tmp_path):
    """TICKET-015: default max_off_regime_rate should reject 75% off-regime firing."""
    from project.scripts.validate_synthetic_detector_truth import validate_detector_truth

    truth_map = {
        "segments": [
            {
                "symbol": "BTCUSDT",
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-01T01:00:00Z",
                "regime_label": "stress",
                "expected_event_types": ["VOL_SHOCK"],
            }
        ]
    }
    truth_map_path = _write_truth_map(tmp_path, truth_map)
    run_id = "test_high_off"
    _write_vol_shock_events(
        tmp_path,
        run_id,
        [
            {"enter_ts": "2024-01-01T00:15:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-02T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-03T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-04T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
        ],
    )
    report = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_map_path,
        event_types=["VOL_SHOCK"],
    )
    per_symbol = report["event_reports"][0]["per_symbol"][0]
    assert not per_symbol["passed_off_regime_bound"], (
        f"Expected off-regime gate to fail at 75% rate with new default; got: {per_symbol}"
    )


def test_rejects_low_precision(tmp_path):
    """TICKET-015: default precision gate rejects detectors with low precision."""
    from project.scripts.validate_synthetic_detector_truth import validate_detector_truth

    truth_map = {
        "segments": [
            {
                "symbol": "BTCUSDT",
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-01T02:00:00Z",
                "regime_label": "stress",
                "expected_event_types": ["VOL_SHOCK"],
            }
        ]
    }
    truth_map_path = _write_truth_map(tmp_path, truth_map)
    run_id = "test_low_prec"
    _write_vol_shock_events(
        tmp_path,
        run_id,
        [
            {"enter_ts": "2024-01-01T00:30:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-05T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-06T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-07T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-08T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-09T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
        ],
    )
    report = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_map_path,
        event_types=["VOL_SHOCK"],
    )
    per_symbol = report["event_reports"][0]["per_symbol"][0]
    assert not per_symbol.get("passed_precision_bound", True), (
        f"Expected precision gate to fail at ~17%; got: {per_symbol}"
    )
    assert not report["passed"]


def test_calibrated_runs_treat_uncalibrated_events_as_hit_only(monkeypatch, tmp_path):
    monkeypatch.setattr(
        truth_script,
        "_load_detector_thresholds",
        lambda: {
            "BREAKOUT_TRIGGER": {
                "golden_synthetic_discovery": {"min_precision": 0.8, "min_recall": 0.6}
            }
        },
    )

    truth_map = {
        "segments": [
            {
                "symbol": "BTCUSDT",
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-01T01:00:00Z",
                "regime_label": "stress",
                "expected_event_types": ["VOL_SHOCK"],
            }
        ]
    }
    truth_map_path = _write_truth_map(tmp_path, truth_map)
    run_id = "golden_synthetic_discovery"
    _write_vol_shock_events(
        tmp_path,
        run_id,
        [
            {"enter_ts": "2024-01-01T00:15:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-02T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-03T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
        ],
    )

    report = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_map_path,
        event_types=["VOL_SHOCK"],
    )

    event_report = report["event_reports"][0]
    per_symbol = event_report["per_symbol"][0]
    assert report["run_has_calibrated_thresholds"]
    assert event_report["gate_mode"] == "hit_only"
    assert per_symbol["gate_mode"] == "hit_only"
    assert per_symbol["passed_hit_requirement"]
    assert report["passed"]


def test_calibrated_events_keep_generic_precision_and_off_regime_gates(monkeypatch, tmp_path):
    monkeypatch.setattr(
        truth_script,
        "_load_detector_thresholds",
        lambda: {
            "VOL_SHOCK": {
                "golden_synthetic_discovery": {"min_precision": 0.75, "min_recall": 0.75}
            }
        },
    )

    truth_map = {
        "segments": [
            {
                "symbol": "BTCUSDT",
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-01T02:00:00Z",
                "regime_label": "stress",
                "expected_event_types": ["VOL_SHOCK"],
            },
            {
                "symbol": "BTCUSDT",
                "start_ts": "2024-01-02T00:00:00Z",
                "end_ts": "2024-01-02T02:00:00Z",
                "regime_label": "stress",
                "expected_event_types": ["VOL_SHOCK"],
            },
        ]
    }
    truth_map_path = _write_truth_map(tmp_path, truth_map)
    run_id = "golden_synthetic_discovery"
    _write_vol_shock_events(
        tmp_path,
        run_id,
        [
            {"enter_ts": "2024-01-01T00:30:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-05T00:00:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
        ],
    )

    report = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_map_path,
        event_types=["VOL_SHOCK"],
    )

    event_report = report["event_reports"][0]
    per_symbol = event_report["per_symbol"][0]
    assert event_report["gate_mode"] == "generic"
    assert per_symbol["gate_mode"] == "generic"
    assert per_symbol["passed_hit_requirement"]
    assert per_symbol["precision"] == 0.5
    assert per_symbol["off_regime_rate"] == 0.5
    assert not per_symbol["passed_off_regime_bound"]
    assert per_symbol["passed_precision_bound"]
    assert not report["passed"]


def test_accepts_clean_detector(tmp_path):
    """TICKET-015: a clean detector with low off-regime rate passes new defaults."""
    from project.scripts.validate_synthetic_detector_truth import validate_detector_truth

    truth_map = {
        "segments": [
            {
                "symbol": "BTCUSDT",
                "start_ts": "2024-01-01T00:00:00Z",
                "end_ts": "2024-01-01T02:00:00Z",
                "regime_label": "stress",
                "expected_event_types": ["VOL_SHOCK"],
            }
        ]
    }
    truth_map_path = _write_truth_map(tmp_path, truth_map)
    run_id = "test_clean"
    _write_vol_shock_events(
        tmp_path,
        run_id,
        [
            {"enter_ts": "2024-01-01T00:20:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-01T00:50:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
            {"enter_ts": "2024-01-01T01:20:00Z", "symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
        ],
    )
    report = validate_detector_truth(
        data_root=tmp_path,
        run_id=run_id,
        truth_map_path=truth_map_path,
        event_types=["VOL_SHOCK"],
    )
    per_symbol = report["event_reports"][0]["per_symbol"][0]
    assert per_symbol["passed_off_regime_bound"]
    assert per_symbol["passed_precision_bound"]
    assert report["passed"]


def test_main_discovers_truth_map_under_artifacts_root(tmp_path, monkeypatch):
    run_id = "golden_synthetic_discovery"
    repo_root = tmp_path / "repo"
    data_root = repo_root / "data"
    artifact_root = repo_root / "artifacts" / "golden_synthetic_discovery"
    truth_path = artifact_root / "synthetic" / run_id / "synthetic_regime_segments.json"
    truth_path.parent.mkdir(parents=True, exist_ok=True)
    truth_path.write_text(json.dumps({"segments": []}), encoding="utf-8")

    captured: dict[str, object] = {}

    def _fake_validate_detector_truth(**kwargs):
        captured.update(kwargs)
        return {"passed": True}

    monkeypatch.setattr(truth_script, "get_data_root", lambda: data_root)
    monkeypatch.setattr(truth_script, "validate_detector_truth", _fake_validate_detector_truth)

    rc = truth_script.main(["--run_id", run_id])

    assert rc == 0
    assert captured["data_root"] == artifact_root
    assert captured["truth_map_path"] == truth_path
