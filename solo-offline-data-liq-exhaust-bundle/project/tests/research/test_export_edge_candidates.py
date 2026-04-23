from __future__ import annotations

import pandas as pd

from project.research import export_edge_candidates as module


def test_collect_phase2_candidates_reads_root_level_search_engine_output(
    tmp_path, monkeypatch
) -> None:
    run_id = "run_root_phase2"
    phase2_root = tmp_path / "reports" / "phase2" / run_id
    phase2_root.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "ETHUSDT::cand_root",
                "event_type": "LIQUIDITY_GAP_PRINT",
                "symbol": "ETHUSDT",
                "direction": "long",
                "horizon": "24b",
                "n_events": 32,
                "sample_size": 32,
                "after_cost_expectancy_per_trade": 0.00418,
                "gate_bridge_tradable": True,
            }
        ]
    ).to_parquet(phase2_root / "phase2_candidates.parquet", index=False)

    monkeypatch.setattr(module, "get_data_root", lambda: tmp_path)

    rows = module._collect_phase2_candidates(run_id, run_symbols=["ETHUSDT"])

    assert len(rows) == 1
    assert rows[0]["candidate_id"] == "ETHUSDT::cand_root"
    assert rows[0]["event"] == "LIQUIDITY_GAP_PRINT"
    assert rows[0]["candidate_symbol"] == "ETHUSDT"
