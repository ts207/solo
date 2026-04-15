from __future__ import annotations

import pandas as pd
import pytest

from project.engine.exchange_constraints import SymbolConstraints
from project.engine.strategy_executor import build_live_order_metadata, calculate_strategy_returns


class _DummyStrategy:
    def generate_positions(
        self, bars: pd.DataFrame, features: pd.DataFrame, params: dict
    ) -> pd.Series:
        out = pd.Series([0.0, 1.0, 1.0], index=pd.DatetimeIndex(bars["timestamp"]), dtype=float)
        out.attrs["strategy_metadata"] = {"family": "test"}
        return out


def _bars() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": idx,
            "open": [100.0, 100.0, 100.0],
            "high": [100.2, 100.2, 100.2],
            "low": [99.8, 99.8, 99.8],
            "close": [100.0, 100.0, 100.0],
        }
    )


def _features() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": idx,
            "spread_bps": [4.0, 4.0, 4.0],
            "quote_volume": [250000.0, 250000.0, 250000.0],
            "depth_usd": [50000.0, 50000.0, 50000.0],
            "tob_coverage": [1.0, 1.0, 1.0],
            "atr_14": [0.2, 0.2, 0.2],
        }
    )


def _dsl_features() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": idx,
            "spread_bps": [4.0, 4.0, 4.0],
            "quote_volume": [250000.0, 250000.0, 250000.0],
            "depth_usd": [50000.0, 50000.0, 50000.0],
            "tob_coverage": [1.0, 1.0, 1.0],
            "atr_14": [0.2, 0.2, 0.2],
            "event_detected": [True, True, False],
        }
    )


def _executable_spec_payload() -> dict:
    return {
        "metadata": {
            "proposal_id": "prop_1",
            "run_id": "r_exec",
            "hypothesis_id": "hyp_1",
            "blueprint_id": "bp_exec",
            "candidate_id": "cand_exec",
            "event_type": "VOL_SHOCK",
            "direction": "long",
            "retail_profile": "standard",
        },
        "research_origin": {
            "source_path": "reports/strategy_blueprints/r_exec/blueprints.jsonl",
            "compiler_version": "strategy_dsl_v1",
            "generated_at_utc": "2026-01-01T00:00:00Z",
            "ontology_spec_hash": "sha256:abc123",
            "promotion_track": "standard",
            "wf_status": "pass",
            "wf_evidence_hash": "sha256:wf123",
            "template_verb": "mean_reversion",
        },
        "entry": {
            "triggers": ["event_detected"],
            "conditions": [],
            "confirmations": [],
            "delay_bars": 0,
            "cooldown_bars": 0,
            "condition_logic": "all",
            "order_type_assumption": "market",
        },
        "exit": {
            "time_stop_bars": 10,
            "invalidation": {"metric": "close", "operator": ">", "value": 10000.0},
            "stop_type": "percent",
            "stop_value": 0.01,
            "target_type": "percent",
            "target_value": 0.02,
            "trailing_stop_type": "none",
            "trailing_stop_value": 0.0,
            "break_even_r": 0.0,
        },
        "risk": {
            "low_capital_contract": {},
            "cost_model": {
                "fees_bps_per_side": 2.0,
                "slippage_bps_per_fill": 2.0,
                "default_fee_tier": "standard",
            },
        },
        "sizing": {
            "mode": "fixed_risk",
            "risk_per_trade": 0.01,
            "max_gross_leverage": 1.0,
        },
        "execution": {
            "symbol_scope": {
                "mode": "single_symbol",
                "symbols": ["BTCUSDT"],
                "candidate_symbol": "BTCUSDT",
            },
            "execution": {
                "mode": "market",
                "urgency": "aggressive",
                "max_slippage_bps": 100.0,
                "fill_profile": "base",
                "retry_logic": {},
            },
            "policy_executor_config": {
                "entry_delay_bars": 0,
                "max_concurrent_positions": 1,
                "per_position_notional_cap_usd": 1000.0,
                "fee_tier": "standard",
            },
            "throttles": {
                "one_trade_per_episode": False,
                "cooldown_bars": 0,
                "max_concurrent_positions": 1,
            },
        },
        "portfolio_constraints": {
            "effective_per_position_notional_cap_usd": 1000.0,
            "effective_max_concurrent_positions": 1,
        },
    }


def test_calculate_strategy_returns_applies_execution_aware_sizing(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("project.engine.strategy_executor.get_strategy", lambda _: _DummyStrategy())
    monkeypatch.setattr(
        "project.engine.strategy_executor.load_symbol_constraints",
        lambda symbol, meta_dir: SymbolConstraints(
            tick_size=None, step_size=None, min_notional=None
        ),
    )

    bars = _bars()
    features = _features()
    base_params = {
        "position_scale": 1000.0,
        "execution_lag_bars": 0,
        "execution_model": {
            "cost_model": "dynamic",
            "base_fee_bps": 2.0,
            "base_slippage_bps": 1.0,
            "spread_weight": 0.5,
            "volatility_weight": 0.0,
            "liquidity_weight": 0.0,
            "impact_weight": 1.0,
            "min_tob_coverage": 0.8,
        },
        "event_score": 0.008,
        "expected_return_bps": 20.0,
        "expected_adverse_bps": 20.0,
        "target_vol": 0.1,
        "current_vol": 0.1,
    }

    legacy = calculate_strategy_returns(
        "BTCUSDT",
        bars,
        features,
        "dummy_strategy",
        dict(base_params),
        0.0,
        tmp_path,
    )
    aware = calculate_strategy_returns(
        "BTCUSDT",
        bars,
        features,
        "dummy_strategy",
        {**base_params, "execution_aware_sizing": 1},
        0.0,
        tmp_path,
    )

    legacy_scale = float(legacy.data["requested_position_scale"].iloc[0])
    aware_scale = float(aware.data["requested_position_scale"].iloc[0])

    assert legacy_scale == pytest.approx(1000.0)
    assert aware_scale < legacy_scale
    assert float(aware.data["target_position"].iloc[-1]) < float(
        legacy.data["target_position"].iloc[-1]
    )
    assert aware.strategy_metadata["execution_aware_scale"] == pytest.approx(aware_scale)
    assert aware.strategy_metadata["execution_aware_estimated_cost_bps"] > 0.0
    assert aware.strategy_metadata["live_order_metadata_template"][
        "expected_return_bps"
    ] == pytest.approx(20.0)
    assert aware.strategy_metadata["live_order_metadata_template"][
        "expected_adverse_bps"
    ] == pytest.approx(20.0)
    assert aware.strategy_metadata["live_order_metadata_template"]["expected_cost_bps"] > 0.0
    assert (
        aware.strategy_metadata["live_order_metadata_template"]["volatility_regime"] == "elevated"
    )
    assert (
        aware.strategy_metadata["live_order_metadata_template"]["microstructure_regime"]
        == "healthy"
    )


def test_build_live_order_metadata_uses_latest_strategy_row(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("project.engine.strategy_executor.get_strategy", lambda _: _DummyStrategy())
    monkeypatch.setattr(
        "project.engine.strategy_executor.load_symbol_constraints",
        lambda symbol, meta_dir: SymbolConstraints(
            tick_size=None, step_size=None, min_notional=None
        ),
    )

    result = calculate_strategy_returns(
        "BTCUSDT",
        _bars(),
        _features(),
        "dummy_strategy",
        {
            "position_scale": 1000.0,
            "execution_lag_bars": 0,
            "expected_return_bps": 30.0,
            "expected_adverse_bps": 5.0,
            "execution_model": {
                "cost_model": "static",
                "base_fee_bps": 2.0,
                "base_slippage_bps": 1.0,
            },
        },
        0.0,
        tmp_path,
    )

    metadata = build_live_order_metadata(result, realized_fee_bps=1.5)

    assert metadata["strategy"] == "dummy_strategy"
    assert metadata["signal_timestamp"].startswith("2024-01-01T00:10:00")
    assert metadata["volatility_regime"] == "elevated"
    assert metadata["microstructure_regime"] == "healthy"
    assert metadata["expected_entry_price"] == pytest.approx(100.0)
    assert metadata["expected_return_bps"] == pytest.approx(30.0)
    assert metadata["expected_adverse_bps"] == pytest.approx(5.0)
    assert metadata["expected_cost_bps"] == pytest.approx(3.0)
    assert metadata["expected_net_edge_bps"] == pytest.approx(22.0)
    assert metadata["realized_fee_bps"] == pytest.approx(1.5)


def test_calculate_strategy_returns_stamps_validated_executable_spec_provenance(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(
        "project.engine.strategy_executor.load_symbol_constraints",
        lambda symbol, meta_dir: SymbolConstraints(
            tick_size=None, step_size=None, min_notional=None
        ),
    )

    result = calculate_strategy_returns(
        "BTCUSDT",
        _bars().assign(volume=[10.0, 10.0, 10.0], quote_volume=[1_000_000.0] * 3),
        _dsl_features(),
        "dsl_interpreter_v1__bp_exec",
        {
            "executable_strategy_spec": _executable_spec_payload(),
            "execution_lag_bars": 0,
            "expected_return_bps": 30.0,
            "expected_adverse_bps": 5.0,
            "execution_model": {
                "cost_model": "static",
                "base_fee_bps": 2.0,
                "base_slippage_bps": 1.0,
            },
        },
        0.0,
        tmp_path,
    )

    metadata = result.strategy_metadata
    assert metadata["contract_source"] == "executable_strategy_spec"
    assert metadata["runtime_provenance_validated"] is True
    assert metadata["runtime_provenance_source"] == "executable_strategy_spec"
    assert metadata["proposal_id"] == "prop_1"
    assert metadata["run_id"] == "r_exec"
    assert metadata["hypothesis_id"] == "hyp_1"
    assert metadata["candidate_id"] == "cand_exec"
    assert metadata["blueprint_id"] == "bp_exec"
    assert metadata["ontology_spec_hash"] == "sha256:abc123"
    assert metadata["wf_evidence_hash"] == "sha256:wf123"
    assert metadata["source_path"] == "reports/strategy_blueprints/r_exec/blueprints.jsonl"
    assert metadata["live_order_metadata_template"]["strategy"] == "dsl_interpreter_v1__bp_exec"
