from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.research import candidate_traces


def _write_candidate(data_root: Path, run_id: str) -> None:
    phase2 = data_root / "reports" / "phase2" / run_id
    phase2.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "BTCUSDT::cand_abc",
                "hypothesis_id": "hyp",
                "event_type": "PRICE_DOWN_OI_DOWN",
                "symbol": "BTCUSDT",
                "context_signature": "{'VOL_REGIME': 'HIGH'}",
                "rule_template": "mean_reversion",
                "direction": "long",
                "horizon": "2b",
                "entry_lag_bars": 1,
                "expected_cost_bps_per_trade": 2.0,
            }
        ]
    ).to_parquet(phase2 / "phase2_candidates.parquet", index=False)


def _write_events(data_root: Path, run_id: str) -> None:
    phase2 = data_root / "reports" / "phase2" / run_id
    pd.DataFrame(
        [
            {
                "candidate_id": "BTCUSDT::cand_abc",
                "hypothesis_id": "hyp",
                "symbol": "BTCUSDT",
                "event_type": "PRICE_DOWN_OI_DOWN",
                "event_timestamp": "2024-01-01T00:00:00Z",
            }
        ]
    ).to_parquet(phase2 / "phase2_candidate_event_timestamps.parquet", index=False)


def _write_market_context(data_root: Path, run_id: str) -> None:
    path = (
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
    path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "close": 100.0,
                "symbol": "BTCUSDT",
                "vol_regime": "HIGH",
                "high_vol_regime": True,
            },
            {
                "timestamp": "2024-01-01T00:05:00Z",
                "close": 101.0,
                "symbol": "BTCUSDT",
                "vol_regime": "HIGH",
                "high_vol_regime": True,
            },
            {
                "timestamp": "2024-01-01T00:10:00Z",
                "close": 102.0,
                "symbol": "BTCUSDT",
                "vol_regime": "HIGH",
                "high_vol_regime": True,
            },
            {
                "timestamp": "2024-01-01T00:15:00Z",
                "close": 103.0,
                "symbol": "BTCUSDT",
                "vol_regime": "HIGH",
                "high_vol_regime": True,
            },
        ]
    ).to_parquet(path / "market_context_BTCUSDT_2024-01.parquet", index=False)


def test_extract_candidate_traces_from_event_timestamps_and_lake_prices(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    _write_candidate(data_root, run_id)
    _write_events(data_root, run_id)
    _write_market_context(data_root, run_id)

    report = candidate_traces.extract_candidate_traces(
        run_id=run_id,
        candidate_id="BTCUSDT_cand_abc",
        data_root=data_root,
    )

    parquet_path = (
        data_root / "reports" / "candidate_traces" / run_id / "BTCUSDT_cand_abc_traces.parquet"
    )
    traces = pd.read_parquet(parquet_path)
    assert report["status"] == "extracted"
    assert report["row_count"] == 1
    assert list(traces.columns) == candidate_traces.TRACE_COLUMNS
    assert traces.iloc[0]["entry_ts"] == pd.Timestamp("2024-01-01T00:05:00Z")
    assert traces.iloc[0]["exit_ts"] == pd.Timestamp("2024-01-01T00:15:00Z")
    assert round(float(traces.iloc[0]["gross_return_bps"]), 4) == 198.0198
    assert round(float(traces.iloc[0]["net_return_bps"]), 4) == 196.0198
    assert bool(traces.iloc[0]["context_pass"]) is True


def test_extract_candidate_traces_prefers_existing_trace_artifact(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    _write_candidate(data_root, run_id)
    phase2 = data_root / "reports" / "phase2" / run_id
    pd.DataFrame(
        [
            {
                "candidate_id": "BTCUSDT::cand_abc",
                "event_ts": "2024-01-01T00:00:00Z",
                "entry_ts": "2024-01-01T00:05:00Z",
                "exit_ts": "2024-01-01T00:15:00Z",
                "entry_price": 100.0,
                "exit_price": 101.0,
                "net_return_bps": 9.0,
                "cost_bps": 1.0,
            }
        ]
    ).to_parquet(phase2 / "edge_cell_pnl_traces.parquet", index=False)

    report = candidate_traces.extract_candidate_traces(
        run_id=run_id,
        candidate_id="BTCUSDT_cand_abc",
        data_root=data_root,
    )

    parquet_path = (
        data_root / "reports" / "candidate_traces" / run_id / "BTCUSDT_cand_abc_traces.parquet"
    )
    traces = pd.read_parquet(parquet_path)
    assert report["source_artifact"].endswith("edge_cell_pnl_traces.parquet")
    assert float(traces.iloc[0]["net_return_bps"]) == 9.0
    assert float(traces.iloc[0]["gross_return_bps"]) == 10.0


def test_extract_candidate_traces_blocks_without_trace_source(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_candidate(data_root, "run")

    report = candidate_traces.extract_candidate_traces(
        run_id="run",
        candidate_id="BTCUSDT_cand_abc",
        data_root=data_root,
    )

    assert report["status"] == "blocked"
    assert report["reason"] == "insufficient_trace_source"
    assert report["row_count"] == 0
