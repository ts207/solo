from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.reliability.contracts import reconcile_bundle_outputs, reconcile_portfolio_to_traces


def test_reconcile_bundle_outputs(tmp_path: Path):
    bundle_path = tmp_path / "bundles.jsonl"
    bundle_path.write_text(json.dumps({"candidate_id": "c1"}) + "\n", encoding="utf-8")
    summary = pd.DataFrame(
        {
            "candidate_id": ["c1"],
            "event_type": ["VOL_SHOCK"],
            "promotion_decision": ["promoted"],
            "promotion_track": ["standard"],
            "policy_version": ["p1"],
            "bundle_version": ["b1"],
            "is_reduced_evidence": [False],
        }
    )
    info = reconcile_bundle_outputs(bundle_path, summary)
    assert info["bundle_count"] == 1


def test_reconcile_portfolio_to_traces():
    portfolio = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC"),
            "gross_pnl": [0.0, 0.02],
            "net_pnl": [0.0, 0.01],
            "equity": [1.0, 1.01],
            "equity_return": [0.0, 0.01],
            "gross_exposure": [0.0, 1.0],
            "net_exposure": [0.0, 1.0],
            "turnover": [0.0, 1.0],
        }
    )
    trace = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC"),
            "strategy": ["s", "s"],
            "symbol": ["BTC", "BTC"],
            "signal_position": [0, 1],
            "target_position": [0, 1],
            "executed_position": [0, 1],
            "gross_pnl": [0.0, 0.015],
            "net_pnl": [0.0, 0.01],
            "equity_return": [0.0, 0.01],
        }
    )
    info = reconcile_portfolio_to_traces(portfolio, [trace])
    assert info["portfolio_total_net_pnl"] == info["trace_total_net_pnl"]
