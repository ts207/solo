from __future__ import annotations

import json

import pandas as pd

from project.scripts.update_regime_scorecard import main


def _baseline_row(
    *,
    run_id: str,
    matrix_id: str = "core_v1",
    regime_id: str = "vol_regime=high",
    symbol: str = "BTCUSDT",
    direction: str = "long",
    horizon_bars: int = 24,
    classification: str = "stable_positive",
) -> dict:
    return {
        "run_id": run_id,
        "matrix_id": matrix_id,
        "regime_id": regime_id,
        "symbol": symbol,
        "direction": direction,
        "horizon_bars": horizon_bars,
        "classification": classification,
        "mean_net_bps": 1.5,
        "t_stat_net": 2.0,
        "max_year_pnl_share": 0.4,
        "effective_n": 75,
    }


def test_update_regime_scorecard_writes_outputs(tmp_path):
    run_dir = tmp_path / "reports" / "regime_baselines" / "baseline_run"
    run_dir.mkdir(parents=True)
    pd.DataFrame([_baseline_row(run_id="baseline_run")]).to_parquet(
        run_dir / "regime_baselines.parquet",
        index=False,
    )

    rc = main(["--data-root", str(tmp_path)])

    assert rc == 0
    out_dir = tmp_path / "reports" / "regime_baselines"
    for name in ["regime_scorecard.json", "regime_scorecard.parquet", "regime_scorecard.md"]:
        assert (out_dir / name).exists()

    payload = json.loads((out_dir / "regime_scorecard.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == "regime_scorecard_v1"
    assert payload["row_count"] == 1
    assert payload["rows"][0]["decision"] == "allow_event_lift"

    df = pd.read_parquet(out_dir / "regime_scorecard.parquet")
    assert len(df) == 1
    assert df.iloc[0]["next_action"] == "run_event_lift_for_best_tuple"


def test_update_regime_scorecard_honors_run_id_filter(tmp_path):
    base = tmp_path / "reports" / "regime_baselines"
    for run_id, classification in [("run_a", "negative"), ("run_b", "stable_positive")]:
        run_dir = base / run_id
        run_dir.mkdir(parents=True)
        pd.DataFrame(
            [_baseline_row(run_id=run_id, classification=classification)]
        ).to_parquet(run_dir / "regime_baselines.parquet", index=False)

    rc = main(["--data-root", str(tmp_path), "--run-id", "run_a"])

    assert rc == 0
    payload = json.loads((base / "regime_scorecard.json").read_text(encoding="utf-8"))
    row = payload["rows"][0]
    assert row["source_run_ids"] == ["run_a"]
    assert row["classification"] == "negative"
