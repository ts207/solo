from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.research.agent_io.closed_loop import CampaignCycleRunner, CycleConfig


def _write_run_manifest(data_root: Path, run_id: str, *, program_id: str) -> None:
    run_dir = data_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "program_id": program_id,
                "status": "success",
                "run_mode": "research",
                "objective_name": "retail_profitability",
            }
        ),
        encoding="utf-8",
    )


def test_closed_loop_updates_event_statistics_with_recent_economics(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry_root = tmp_path / "registry"
    program_id = "program_1"
    run_id = "run_1"
    _write_run_manifest(data_root, run_id, program_id=program_id)

    phase2_dir = data_root / "reports" / "phase2" / run_id / "search_engine"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "hypothesis_id": "h1",
                "event_type": "EVENT_A",
                "trigger_type": "EVENT",
                "template_id": "mean_reversion",
                "direction": "long",
                "horizon": "12b",
                "entry_lag": 0,
                "symbol": "BTCUSDT",
                "q_value": 0.04,
                "train_n_obs": 120,
                "mean_return_bps": 4.0,
                "after_cost_expectancy_per_trade": 3.0,
                "stressed_after_cost_expectancy_per_trade": 2.0,
                "robustness_score": 0.8,
                "gate_bridge_tradable": "pass",
                "gate_promo_statistical": "pass",
                "promotion_decision": "promoted",
                "updated_at": "2026-01-02T00:00:00Z",
            },
            {
                "candidate_id": "c2",
                "hypothesis_id": "h2",
                "event_type": "EVENT_A",
                "trigger_type": "EVENT",
                "template_id": "mean_reversion",
                "direction": "long",
                "horizon": "12b",
                "entry_lag": 1,
                "symbol": "BTCUSDT",
                "q_value": 0.08,
                "train_n_obs": 120,
                "mean_return_bps": 2.0,
                "after_cost_expectancy_per_trade": -1.0,
                "stressed_after_cost_expectancy_per_trade": -2.0,
                "robustness_score": 0.6,
                "gate_bridge_tradable": "fail",
                "gate_promo_statistical": "fail",
                "promotion_decision": "rejected",
                "promotion_fail_gate_primary": "gate_promo_retail_net_expectancy",
                "updated_at": "2026-01-03T00:00:00Z",
            },
            {
                "candidate_id": "c3",
                "hypothesis_id": "h3",
                "event_type": "EVENT_A",
                "trigger_type": "EVENT",
                "template_id": "mean_reversion",
                "direction": "long",
                "horizon": "12b",
                "entry_lag": 2,
                "symbol": "BTCUSDT",
                "q_value": 0.03,
                "train_n_obs": 120,
                "mean_return_bps": 5.0,
                "after_cost_expectancy_per_trade": 6.0,
                "stressed_after_cost_expectancy_per_trade": 4.0,
                "robustness_score": 0.9,
                "gate_bridge_tradable": "pass",
                "gate_promo_statistical": "pass",
                "promotion_decision": "promoted",
                "updated_at": "2026-01-04T00:00:00Z",
            },
        ]
    ).to_parquet(phase2_dir / "phase2_candidates.parquet", index=False)

    runner = CampaignCycleRunner(
        CycleConfig(program_id=program_id, registry_root=registry_root, data_root=data_root)
    )
    runner._update_memory_from_run(run_id)

    event_stats = pd.read_parquet(
        data_root / "artifacts" / "experiments" / program_id / "memory" / "event_statistics.parquet"
    )
    row = event_stats.iloc[0]
    assert row["event_type"] == "EVENT_A"
    assert row["times_evaluated"] == 3
    assert row["times_promoted"] == 2
    assert row["promotion_rate"] == 2 / 3
    assert row["positive_after_cost_rate"] == 2 / 3
    assert row["positive_stressed_after_cost_rate"] == 2 / 3
    assert row["tradable_rate"] == 2 / 3
    assert row["statistical_pass_rate"] == 2 / 3
    assert row["recent_after_cost_expectancy"] == 8 / 3
    assert row["recent_stressed_after_cost_expectancy"] == 4 / 3
    assert row["last_tested_at"] == "2026-01-04T00:00:00Z"
