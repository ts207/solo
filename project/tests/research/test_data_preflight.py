from __future__ import annotations

from pathlib import Path

import yaml

from project.research.data_preflight import build_data_preflight_report


def test_data_preflight_fails_when_bars_and_features_missing(tmp_path: Path) -> None:
    proposal = tmp_path / "proposal.yaml"
    proposal.write_text(
        yaml.safe_dump(
            {
                "program_id": "x",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "hypothesis": {
                    "anchor": {"type": "event", "event_id": "VOL_SHOCK"},
                    "template": {"id": "continuation"},
                    "direction": "long",
                    "horizon_bars": 12,
                },
            }
        ),
        encoding="utf-8",
    )

    report = build_data_preflight_report(proposal_path=proposal, data_root=tmp_path / "data")

    assert report["status"] == "fail"
    assert {issue["code"] for issue in report["issues"]} == {"missing_bars", "missing_features"}


def test_data_preflight_passes_with_expected_roots(tmp_path: Path) -> None:
    root = tmp_path / "data"
    (root / "lake" / "cleaned" / "perp" / "BTCUSDT" / "bars_5m").mkdir(parents=True)
    (
        root
        / "lake"
        / "features"
        / "perp"
        / "BTCUSDT"
        / "5m"
        / "features_feature_schema_v2"
    ).mkdir(parents=True)
    proposal = tmp_path / "proposal.yaml"
    proposal.write_text(
        yaml.safe_dump(
            {
                "program_id": "x",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "hypothesis": {
                    "anchor": {"type": "event", "event_id": "VOL_SHOCK"},
                    "template": {"id": "continuation"},
                    "direction": "long",
                    "horizon_bars": 12,
                },
            }
        ),
        encoding="utf-8",
    )

    report = build_data_preflight_report(proposal_path=proposal, data_root=root)

    assert report["status"] == "pass"
    assert report["warnings"]
    assert report["checked"]["event_id"] == "VOL_SHOCK"
