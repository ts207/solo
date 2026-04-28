from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.scripts import update_reflections


def _write_eval(root: Path, rows: list[dict]) -> None:
    out = root / "data" / "artifacts" / "experiments" / "program_a" / "run_a"
    out.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(out / "evaluation_results.parquet", index=False)


def test_load_results_extracts_current_evaluation_schema(tmp_path, monkeypatch):
    _write_eval(
        tmp_path,
        [
            {
                "direction": "long",
                "horizon": "24b",
                "template_id": "mean_reversion",
                "trigger_payload": '{"trigger_type": "event", "event_id": "PRICE_DOWN_OI_DOWN"}',
                "trigger_key": "event:PRICE_DOWN_OI_DOWN",
                "n": 79,
                "t_stat_net": 2.3456,
                "robustness_score": 0.8387,
                "p_value_for_fdr": 0.009498,
                "after_cost_expectancy_bps": 41.9707,
            }
        ],
    )
    monkeypatch.setattr(update_reflections, "ROOT", tmp_path)

    results = update_reflections.load_results()

    assert len(results) == 1
    row = results.iloc[0]
    assert row["event_type"] == "PRICE_DOWN_OI_DOWN"
    assert row["n"] == 79
    assert row["t"] == 2.3456
    assert row["q"] == 0.009498
    assert row["exp_bps"] == 41.9707


def test_load_results_keeps_legacy_evaluation_schema(tmp_path, monkeypatch):
    _write_eval(
        tmp_path,
        [
            {
                "event_type": "VOL_SPIKE",
                "direction": "short",
                "horizon": "48b",
                "template_id": "continuation",
                "n_events": 42,
                "t_stat": 1.25,
                "robustness_score": 0.61,
                "q_value": 0.04,
                "after_cost_expectancy_per_trade": 0.0012,
            }
        ],
    )
    monkeypatch.setattr(update_reflections, "ROOT", tmp_path)

    results = update_reflections.load_results()

    assert len(results) == 1
    row = results.iloc[0]
    assert row["event_type"] == "VOL_SPIKE"
    assert row["n"] == 42
    assert row["t"] == 1.25
    assert row["q"] == 0.04
    assert abs(row["exp_bps"] - 12.0) < 1e-9
