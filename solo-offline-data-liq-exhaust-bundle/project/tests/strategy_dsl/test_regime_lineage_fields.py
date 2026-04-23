from __future__ import annotations

from project.strategy.dsl.normalize import build_blueprint
from project.strategy.models.executable_strategy_spec import ExecutableStrategySpec


def _raw_blueprint() -> dict:
    return {
        "id": "bp_test",
        "run_id": "run_test",
        "event_type": "LIQUIDITY_STRESS_DIRECT",
        "candidate_id": "cand_test",
        "symbol_scope": {
            "mode": "single_symbol",
            "symbols": ["BTCUSDT"],
            "candidate_symbol": "BTCUSDT",
        },
        "direction": "long",
        "entry": {
            "triggers": ["event_detected"],
            "conditions": ["all"],
            "confirmations": [],
            "delay_bars": 1,
            "cooldown_bars": 2,
            "condition_logic": "all",
            "condition_nodes": [],
            "arm_bars": 1,
            "reentry_lockout_bars": 2,
        },
        "exit": {
            "time_stop_bars": 10,
            "invalidation": {"metric": "spread_bps", "operator": ">", "value": 10},
            "stop_type": "percent",
            "stop_value": 0.01,
            "target_type": "percent",
            "target_value": 0.02,
            "trailing_stop_type": "none",
            "trailing_stop_value": 0.0,
            "break_even_r": 0.0,
        },
        "execution": {
            "mode": "market",
            "urgency": "aggressive",
            "max_slippage_bps": 10.0,
            "fill_profile": "base",
            "retry_logic": {},
        },
        "sizing": {
            "mode": "fixed_risk",
            "risk_per_trade": 0.01,
            "target_vol": None,
            "max_gross_leverage": 1.0,
            "max_position_scale": 1.0,
            "portfolio_risk_budget": 1.0,
            "symbol_risk_budget": 1.0,
        },
        "overlays": [],
        "evaluation": {
            "min_trades": 1,
            "cost_model": {"fees_bps": 2.0, "slippage_bps": 2.0, "funding_included": True},
            "robustness_flags": {
                "oos_required": True,
                "multiplicity_required": True,
                "regime_stability_required": True,
            },
        },
        "lineage": {
            "source_path": "dummy",
            "compiler_version": "v1",
            "generated_at_utc": "2026-01-01T00:00:00Z",
            "canonical_regime": "LIQUIDITY_STRESS",
            "subtype": "liquidity_stress",
            "phase": "shock",
            "evidence_mode": "direct",
            "regime_bucket": "trade_generating",
            "recommended_bucket": "trade_generating",
            "routing_profile_id": "regime_routing_v1",
        },
    }


def test_executable_strategy_spec_preserves_regime_lineage_fields():
    blueprint = build_blueprint(_raw_blueprint())

    executable = ExecutableStrategySpec.from_blueprint(
        blueprint=blueprint,
        run_id="run_test",
        retail_profile="capital_constrained",
        low_capital_contract={},
        effective_max_concurrent_positions=2,
        effective_per_position_notional_cap_usd=1_000.0,
        default_fee_tier="vip0",
        fees_bps_per_side=2.0,
        slippage_bps_per_fill=2.0,
    )

    assert executable.research_origin.canonical_regime == "LIQUIDITY_STRESS"
    assert executable.research_origin.regime_bucket == "trade_generating"
    assert executable.to_blueprint_dict()["lineage"]["routing_profile_id"] == "regime_routing_v1"
