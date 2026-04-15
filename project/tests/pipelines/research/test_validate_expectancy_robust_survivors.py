from __future__ import annotations

import json
import numpy as np
import pandas as pd
import pytest
from types import SimpleNamespace

import project.research.validate_expectancy_traps as traps


def test_newey_west_t_stat_detects_positive_mean_signal():
    rng = np.random.default_rng(11)
    series = pd.Series(0.003 + rng.normal(0.0, 0.001, 300))
    t_stat, p_value, used_lag = traps._newey_west_t_stat(series, max_lag=12)
    assert used_lag == 12
    assert t_stat > 2.0
    assert p_value < 0.05


def test_circular_block_bootstrap_returns_low_p_for_shifted_mean():
    rng = np.random.default_rng(19)
    series = pd.Series(0.004 + rng.normal(0.0, 0.0012, 250))
    p_value = traps._circular_block_bootstrap_pvalue(series, block_size=8, n_boot=400, seed=7)
    assert 0.0 <= p_value <= 1.0
    assert p_value < 0.05


def test_apply_robust_survivor_gates_enforces_fdr_and_oos():
    df = pd.DataFrame(
        [
            {
                "condition": "compression",
                "horizon": 4,
                "event_samples": 180,
                "event_mean": 0.0012,
                "event_t": 2.4,
                "hac_t": 2.6,
                "hac_p": 0.012,
                "bootstrap_p": 0.018,
                "oos_samples": 70,
                "oos_mean": 0.0009,
                "oos_sign_consistent": True,
            },
            {
                "condition": "compression_plus_funding_low",
                "horizon": 96,
                "event_samples": 150,
                "event_mean": 0.0011,
                "event_t": 2.3,
                "hac_t": 2.2,
                "hac_p": 0.019,
                "bootstrap_p": 0.031,
                "oos_samples": 60,
                "oos_mean": -0.0001,
                "oos_sign_consistent": False,
            },
            {
                "condition": "compression_plus_htf_trend",
                "horizon": 16,
                "event_samples": 140,
                "event_mean": 0.0007,
                "event_t": 1.9,
                "hac_t": 2.1,
                "hac_p": 0.022,
                "bootstrap_p": 0.080,
                "oos_samples": 65,
                "oos_mean": 0.0007,
                "oos_sign_consistent": True,
            },
        ]
    )

    out = traps._apply_robust_survivor_gates(
        df,
        min_samples=100,
        legacy_tstat_threshold=2.0,
        robust_hac_t_threshold=1.96,
        bootstrap_alpha=0.05,
        fdr_q=0.05,
        oos_min_samples=40,
        require_oos_positive=1,
        require_oos_sign_consistency=1,
    )

    survivors = out[out["gate_robust_survivor"]]
    assert len(survivors) == 1
    assert survivors.iloc[0]["condition"] == "compression"

    # legacy still has at least one candidate and differs from robust gate.
    legacy = out[out["gate_legacy_survivor"]]
    assert len(legacy) >= 1
    assert len(legacy) >= len(survivors)


def test_apply_robust_survivor_gates_uses_hac_p_for_fdr():
    df = pd.DataFrame(
        [
            {
                "condition": "dependent_tests",
                "horizon": 4,
                "event_samples": 180,
                "event_mean": 0.0012,
                "event_t": 2.4,
                "hac_t": 2.6,
                "hac_p": 0.40,
                "bootstrap_p": 0.001,
                "oos_samples": 70,
                "oos_mean": 0.0009,
                "oos_sign_consistent": True,
            }
        ]
    )

    out = traps._apply_robust_survivor_gates(
        df,
        min_samples=100,
        legacy_tstat_threshold=2.0,
        robust_hac_t_threshold=1.96,
        bootstrap_alpha=0.05,
        fdr_q=0.05,
        oos_min_samples=40,
        require_oos_positive=1,
        require_oos_sign_consistency=1,
    )

    row = out.iloc[0]
    assert row["composite_p_value"] == pytest.approx(0.40)
    assert row["fdr_q_value"] == pytest.approx(0.40)
    assert bool(row["gate_robust_survivor"]) is False


def test_gate_profile_discovery_relaxes_thresholds():
    args = SimpleNamespace(
        gate_profile="discovery",
        min_samples=100,
        tstat_threshold=2.0,
        robust_hac_t_threshold=1.96,
        robust_bootstrap_alpha=0.10,
        robust_fdr_q=0.10,
        robust_hac_max_lag=12,
        robust_bootstrap_iters=800,
        robust_bootstrap_block_size=8,
        robust_bootstrap_seed=7,
        oos_min_samples=40,
        require_oos_positive=1,
        require_oos_sign_consistency=1,
    )
    out = traps._apply_gate_profile_defaults(args)
    assert out.tstat_threshold == 1.64
    assert out.robust_hac_t_threshold == 1.64
    assert out.robust_bootstrap_alpha == 0.20
    assert out.robust_fdr_q == 0.20
    assert out.oos_min_samples == 20
    assert out.require_oos_sign_consistency == 0


def test_main_short_circuits_when_expectancy_reports_no_evidence(tmp_path, monkeypatch):
    out_dir = tmp_path / "reports" / "expectancy" / "smoke_run"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "conditional_expectancy.json").write_text(
        json.dumps(
            {
                "run_id": "smoke_run",
                "expectancy_exists": False,
                "expectancy_evidence": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(traps, "get_data_root", lambda: tmp_path)

    rc = traps.main(
        [
            "--run_id",
            "smoke_run",
            "--symbols",
            "BTCUSDT",
        ]
    )

    assert rc == 0
    payload = json.loads(
        (out_dir / "conditional_expectancy_robustness.json").read_text(encoding="utf-8")
    )
    assert payload["skipped"] is True
    assert payload["skip_reason"] == "expectancy_analysis_reported_no_evidence"
    assert payload["survivors"] == []
    manifest = json.loads(
        (tmp_path / "runs" / "smoke_run" / "validate_expectancy_traps.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["status"] == "success"
    assert manifest["stats"]["skip_reason"] == "expectancy_analysis_reported_no_evidence"
