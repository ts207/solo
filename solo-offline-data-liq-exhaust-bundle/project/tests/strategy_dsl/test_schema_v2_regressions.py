from __future__ import annotations

from project.strategy.dsl.normalize import build_blueprint


def _raw_blueprint() -> dict:
    return {
        "id": "bp_test",
        "run_id": "r1",
        "event_type": "VOL_SHOCK",
        "candidate_id": "cand_1",
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
            "delay_bars": 0,
            "cooldown_bars": 0,
            "condition_logic": "all",
            "condition_nodes": [],
            "arm_bars": 0,
            "reentry_lockout_bars": 0,
        },
        "exit": {
            "time_stop_bars": 10,
            "invalidation": {"metric": "close", "operator": ">", "value": 10_000.0},
            "stop_type": "percent",
            "stop_value": 0.01,
            "target_type": "percent",
            "target_value": 0.02,
            "trailing_stop_type": "none",
            "trailing_stop_value": 0.0,
            "break_even_r": 0.0,
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
        "overlays": [{"name": "risk_throttle", "params": {"size_scale": 0.5}}],
        "evaluation": {
            "min_trades": 1,
            "cost_model": {
                "fees_bps": 2.0,
                "slippage_bps": 2.0,
                "funding_included": True,
            },
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
        },
    }


def test_schema_v2_build_blueprint_validates_without_name_errors() -> None:
    blueprint = build_blueprint(_raw_blueprint())

    assert blueprint.id == "bp_test"
    assert blueprint.entry.triggers == ["event_detected"]
    assert blueprint.overlays[0].name == "risk_throttle"


def test_schema_v2_blueprint_to_dict_returns_nested_payload() -> None:
    blueprint = build_blueprint(_raw_blueprint())

    payload = blueprint.to_dict()

    assert payload["id"] == "bp_test"
    assert payload["symbol_scope"]["candidate_symbol"] == "BTCUSDT"
    assert payload["overlays"][0]["params"]["size_scale"] == 0.5
