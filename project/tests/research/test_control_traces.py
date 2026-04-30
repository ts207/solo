from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.research import control_traces


def _write_base_trace(data_root: Path, run_id: str, candidate_id: str) -> None:
    trace_dir = data_root / "reports" / "candidate_traces" / run_id
    trace_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "candidate_id": candidate_id,
                "symbol": "BTCUSDT",
                "event_id": "PRICE_DOWN_OI_DOWN",
                "template_id": "mean_reversion",
                "context_key": "VOL_REGIME",
                "context_value": "HIGH",
                "direction": "long",
                "horizon_bars": 1,
                "event_ts": "2024-01-01T00:00:00Z",
                "entry_ts": "2024-01-01T00:00:00Z",
                "exit_ts": "2024-01-01T00:05:00Z",
                "entry_price": 100.0,
                "exit_price": 101.0,
                "gross_return_bps": 100.0,
                "cost_bps": 2.0,
                "net_return_bps": 98.0,
                "context_pass": True,
                "entry_lag_bars": 0,
                "source_artifact": "fixture",
            },
            {
                "run_id": run_id,
                "candidate_id": candidate_id,
                "symbol": "BTCUSDT",
                "event_id": "PRICE_DOWN_OI_DOWN",
                "template_id": "mean_reversion",
                "context_key": "VOL_REGIME",
                "context_value": "HIGH",
                "direction": "long",
                "horizon_bars": 1,
                "event_ts": "2025-01-01T00:00:00Z",
                "entry_ts": "2025-01-01T00:00:00Z",
                "exit_ts": "2025-01-01T00:05:00Z",
                "entry_price": 100.0,
                "exit_price": 101.0,
                "gross_return_bps": 100.0,
                "cost_bps": 2.0,
                "net_return_bps": 98.0,
                "context_pass": True,
                "entry_lag_bars": 0,
                "source_artifact": "fixture",
            },
        ]
    ).to_parquet(trace_dir / "cand_traces.parquet", index=False)
    (trace_dir / "cand_traces.json").write_text(json.dumps({"status": "extracted"}), encoding="utf-8")


def _write_bars(data_root: Path, run_id: str, *, include_event_flag: bool = True) -> None:
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
    rows = []
    for ts, close, quadrant, high_vol in [
        ("2024-01-01T00:00:00Z", 100.0, "price_down_oi_down", True),
        ("2024-01-01T00:05:00Z", 101.0, "price_up_oi_up", True),
        ("2024-01-01T00:10:00Z", 102.0, "price_down_oi_down", False),
        ("2024-01-01T00:15:00Z", 103.0, "price_up_oi_up", True),
        ("2024-01-01T00:20:00Z", 104.0, "price_up_oi_up", True),
        ("2025-01-01T00:00:00Z", 100.0, "price_down_oi_down", True),
        ("2025-01-01T00:05:00Z", 101.0, "price_up_oi_up", True),
        ("2025-01-01T00:10:00Z", 102.0, "price_up_oi_up", True),
        ("2025-01-01T00:15:00Z", 103.0, "price_up_oi_up", True),
    ]:
        row = {"timestamp": ts, "close": close, "high_vol_regime": high_vol}
        if include_event_flag:
            row["price_oi_quadrant"] = quadrant
        rows.append(row)
    pd.DataFrame(rows).to_parquet(feature_dir / "market_context_BTCUSDT.parquet", index=False)


def test_opposite_direction_inverts_return_sign() -> None:
    row = {column: None for column in control_traces.CONTROL_COLUMNS}
    row.update(
        {
            "control_type": "base",
            "gross_return_bps": 25.0,
            "net_return_bps": 23.0,
            "cost_bps": 2.0,
            "direction": "long",
            "source_artifact": "fixture",
        }
    )
    base = pd.DataFrame(
        [
            row,
        ]
    )

    out = control_traces.build_opposite_direction(base)

    assert float(out.iloc[0]["gross_return_bps"]) == -25.0
    assert float(out.iloc[0]["net_return_bps"]) == -27.0
    assert out.iloc[0]["direction"] == "short"


def test_lagged_entries_preserve_row_count_when_market_data_available(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    _write_base_trace(data_root, run_id, "cand")
    _write_bars(data_root, run_id)
    base = control_traces.load_base_candidate_traces(data_root=data_root, run_id=run_id, candidate_id="cand")
    profile = control_traces._base_profile(base, run_id, "cand")
    bars = control_traces.load_market_bars(data_root=data_root, run_id=run_id, symbol="BTCUSDT")

    out = control_traces.build_lagged_entries(base, bars, profile, (0,))

    assert len(out) == len(base)
    assert set(out["control_type"]) == {"entry_lag_0"}


def test_context_only_excludes_base_event_timestamps(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    _write_base_trace(data_root, run_id, "cand")
    _write_bars(data_root, run_id)
    base = control_traces._normalize_base(
        control_traces.load_base_candidate_traces(data_root=data_root, run_id=run_id, candidate_id="cand"),
        run_id=run_id,
        candidate_id="cand",
    )
    profile = control_traces._base_profile(base, run_id, "cand")
    bars = control_traces.load_market_bars(data_root=data_root, run_id=run_id, symbol="BTCUSDT")

    out = control_traces.build_context_only(base, bars, profile)

    assert "2024-01-01 00:00:00+00:00" not in {str(value) for value in out["event_ts"]}
    assert set(out["event_pass"]) == {False}
    assert set(out["context_pass"]) == {True}


def test_missing_event_source_returns_blocked_not_fake_controls(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    _write_base_trace(data_root, run_id, "cand")
    _write_bars(data_root, run_id, include_event_flag=False)

    result = control_traces.build_control_traces(run_id=run_id, candidate_id="cand", data_root=data_root)

    assert result.status == "blocked"
    assert result.reason == "missing_event_timestamp_source"
    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert payload["missing"] == ["event_only"]
