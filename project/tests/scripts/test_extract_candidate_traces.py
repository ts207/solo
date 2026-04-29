from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.scripts import extract_candidate_traces


def test_extract_candidate_traces_script_writes_parquet_and_json(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    phase2 = data_root / "reports" / "phase2" / run_id
    phase2.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "hypothesis_id": "hyp",
                "event_type": "PRICE_DOWN_OI_DOWN",
                "symbol": "BTCUSDT",
                "rule_template": "mean_reversion",
                "direction": "long",
                "horizon": "1b",
                "entry_lag_bars": 1,
                "expected_cost_bps_per_trade": 2.0,
            }
        ]
    ).to_parquet(phase2 / "phase2_candidates.parquet", index=False)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "event_timestamp": "2024-01-01T00:00:00Z",
                "event_type": "PRICE_DOWN_OI_DOWN",
                "symbol": "BTCUSDT",
            }
        ]
    ).to_parquet(phase2 / "phase2_candidate_event_timestamps.parquet", index=False)
    feature_dir = (
        data_root
        / "lake"
        / "runs"
        / run_id
        / "features"
        / "perp"
        / "BTCUSDT"
        / "5m"
        / "market_context"
        / "year=2024"
        / "month=01"
    )
    feature_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"timestamp": "2024-01-01T00:00:00Z", "close": 100.0},
            {"timestamp": "2024-01-01T00:05:00Z", "close": 101.0},
            {"timestamp": "2024-01-01T00:10:00Z", "close": 102.0},
        ]
    ).to_parquet(feature_dir / "market_context_BTCUSDT_2024-01.parquet", index=False)

    exit_code = extract_candidate_traces.main(
        [
            "--run-id",
            run_id,
            "--candidate-id",
            "cand",
            "--data-root",
            str(data_root),
        ]
    )

    base = data_root / "reports" / "candidate_traces" / run_id
    payload = json.loads((base / "cand_traces.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert (base / "cand_traces.parquet").exists()
    assert payload["status"] == "extracted"
    assert payload["row_count"] == 1
