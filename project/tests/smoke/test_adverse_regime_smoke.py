from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from project.reliability.contracts import validate_candidate_table
from project.reliability.smoke_data import (
    SMOKE_EVENT_TYPE,
    SMOKE_HORIZON_BARS,
    build_smoke_dataset,
    run_research_smoke,
)


def _make_crash_dense_events(symbol: str, seed: int) -> pd.DataFrame:
    """Crash-dense events: 40 events packed into 5 days, simulating a cascade.

    Uses the same schema as build_smoke_events (enter_ts, timestamp, symbol,
    event_type, return_{horizon}) so the pipeline can process them, but with
    negative/volatile returns and timestamps bunched into a short burst window
    to stress the pipeline under adverse-regime conditions.
    """
    rng = np.random.default_rng(seed)
    n = 40
    base = pd.Timestamp("2024-01-01", tz="UTC")
    # Burst: 28 events in first 6 hours, then 12 spread over 5 days
    burst_minutes = sorted(rng.integers(0, 60 * 6, size=28).tolist())
    spread_minutes = sorted(rng.integers(60 * 6, 60 * 24 * 5, size=12).tolist())
    all_minutes = sorted(burst_minutes + spread_minutes)
    timestamps = pd.to_datetime(
        [base + pd.Timedelta(minutes=int(m)) for m in all_minutes], utc=True
    )
    # Crash-like: negative returns with high noise
    base_ret = -0.0050
    noise = rng.normal(0.0, 0.0025, n)
    returns = base_ret + noise
    return pd.DataFrame(
        {
            "enter_ts": timestamps,
            "timestamp": timestamps,
            "symbol": [symbol] * n,
            "event_type": [SMOKE_EVENT_TYPE] * n,
            f"return_{SMOKE_HORIZON_BARS}": returns,
        }
    )


def test_adverse_regime_research_smoke_completes(tmp_path: Path):
    """TICKET-021: research pipeline must complete under crash-dense adverse event data."""
    dataset = build_smoke_dataset(tmp_path, seed=20260318, storage_mode="auto")

    adverse_btc = _make_crash_dense_events("BTCUSDT", seed=20260318)
    adverse_eth = _make_crash_dense_events("ETHUSDT", seed=20260319)
    adverse_by_symbol = {"BTCUSDT": adverse_btc, "ETHUSDT": adverse_eth}

    def patched_build_smoke_events(symbol: str, *, seed: int = 0) -> pd.DataFrame:
        return adverse_by_symbol.get(symbol, adverse_btc)

    with patch(
        "project.reliability.smoke_data.build_smoke_events", side_effect=patched_build_smoke_events
    ):
        research_result = run_research_smoke(dataset)

    out_dir = Path(research_result["output_dir"])
    candidate_files = list(out_dir.glob("phase2_candidates*"))
    assert candidate_files, f"No phase2_candidates artifact under {out_dir}"
    validate_candidate_table(candidate_files[0])

    combined = research_result["combined_candidates"]
    assert len(combined) > 0, "No candidates produced under adverse regime"

    if "gate_phase2_final" in combined.columns:
        assert combined["gate_phase2_final"].isin([True, False]).all()
