from __future__ import annotations

import json
from pathlib import Path

from project.research.services.live_data_foundation_service import (
    build_live_data_foundation_payload,
    write_live_data_foundation_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_live_data_foundation_payload_ready(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "btc_live_ready"
    market = "perp"
    symbol = "BTCUSDT"
    timeframe = "5m"

    _write_json(
        data_root
        / "reports"
        / "data_quality"
        / run_id
        / "cleaned"
        / market
        / symbol
        / f"bars_{timeframe}_quality.json",
        {"overall": {"missing_ratio": 0.0, "gap_ratio": 0.0}},
    )
    _write_json(
        data_root
        / "reports"
        / "data_quality"
        / run_id
        / "validation"
        / f"validate_data_coverage_{timeframe}.json",
        {"failure_count": 0, "warning_count": 0},
    )
    _write_json(
        data_root
        / "reports"
        / "feature_quality"
        / run_id
        / market
        / symbol
        / timeframe
        / "feature_quality_feature_schema_v2.json",
        {"quality": {"feature_count": 4, "features_with_nulls": 0}},
    )
    _write_json(
        data_root
        / "reports"
        / "feature_quality"
        / run_id
        / "validation"
        / f"validate_feature_integrity_{timeframe}.json",
        {"status": "success", "symbols": {}},
    )
    _write_json(
        data_root
        / "reports"
        / "context_quality"
        / run_id
        / market
        / symbol
        / timeframe
        / "context_quality_report_v1.json",
        {"summary": {"dimensions": 6}},
    )

    payload = build_live_data_foundation_payload(
        data_root=data_root,
        run_id=run_id,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
    )

    assert payload["readiness"] == "ready"
    assert payload["missing_reports"] == []
    assert payload["data_quality"]["validation_failure_count"] == 0
    assert payload["feature_quality"]["validation_status"] == "success"
    assert payload["context_quality"]["dimensions"] == 6


def test_write_live_data_foundation_report_blocks_on_missing_reports(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "btc_live_blocked"

    out_path = write_live_data_foundation_report(
        data_root=data_root,
        run_id=run_id,
        symbol="BTCUSDT",
        timeframe="5m",
    )

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "live_data_foundation_report_v1"
    assert payload["readiness"] == "blocked"
    assert "cleaned_quality" in payload["missing_reports"]
