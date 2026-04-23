from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from project.research.services import phase2_support


def test_regime_ess_diagnostics_requires_min_regime_count():
    events = pd.DataFrame(
        {
            "vol_regime": ["low"] * 12 + ["high"] * 10 + ["mid"] * 8,
        }
    )
    diag = phase2_support.regime_ess_diagnostics(
        events,
        min_ess_per_regime=8.0,
        min_regimes_required=3,
    )
    assert diag["gate_regime_ess"] is True
    assert diag["pass_count"] == 3


def test_timeframe_consensus_gate_uses_cross_timeframe_alignment(monkeypatch):
    args = SimpleNamespace(
        shift_labels_k=0,
        entry_lag_bars=1,
        min_samples=1,
        enable_time_decay=0,
        time_decay_floor_weight=0.0,
        enable_regime_conditioned_decay=0,
        regime_tau_smoothing_alpha=0.0,
        regime_tau_min_days=1.0,
        regime_tau_max_days=1.0,
        enable_directional_asymmetry_decay=0,
        directional_tau_smoothing_alpha=0.0,
        directional_tau_min_ratio=1.0,
        directional_tau_max_ratio=1.0,
        directional_tau_default_up_mult=1.0,
        directional_tau_default_down_mult=1.0,
    )

    monkeypatch.setattr(
        phase2_support,
        "load_phase2_features",
        lambda run_id, symbol, timeframe="5m": pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=10, freq="5min", tz="UTC"),
                "close": [100.0] * 10,
            }
        ),
    )

    def _fake_expectancy(*_args, **kwargs):
        bars = int(kwargs.get("horizon_bars_override", 1))
        # Force a sign disagreement for one timeframe.
        sign = -1.0 if bars == 15 else 1.0
        return {"mean_return": sign * 0.001, "t_stat": sign * 2.0, "n_events": 20}

    monkeypatch.setattr(phase2_support, "calculate_expectancy_stats", _fake_expectancy)

    diag = phase2_support.timeframe_expectancy_consensus(
        run_id="r1",
        symbol="BTCUSDT",
        events_df=pd.DataFrame(
            {"enter_ts": pd.date_range("2025-01-01", periods=20, freq="5min", tz="UTC")}
        ),
        rule="continuation",
        horizon="15m",
        canonical_family="VOLATILITY_TRANSITION",
        side_policy="both",
        label_target="close_logret",
        args=args,
        base_sign=1,
        configured_timeframes=["1m", "5m", "15m"],
        min_consistency_ratio=0.60,
        min_timeframes_required=2,
        feature_cache={},
    )
    assert diag["available_count"] == 3
    assert diag["aligned_count"] == 2
    assert diag["gate_timeframe_consensus"] is True
