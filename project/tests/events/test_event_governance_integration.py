from __future__ import annotations

from pathlib import Path

from project.domain.compiled_registry import get_domain_registry
from project.events.governance import (
    default_planning_event_ids,
    get_event_governance_metadata,
)
from project.research.candidates.ranking import candidate_rank_key
from project.research.experiment_engine import (
    AgentExperimentRequest,
    ContextSelection,
    EvaluationConfig,
    InstrumentScope,
    PromotionConfig,
    RegistryBundle,
    SearchControl,
    TemplateSelection,
    TriggerSpace,
)
from project.research.experiment_engine_validators import _resolve_requested_event_ids
from project.research.validation.evidence_bundle import evaluate_promotion_bundle
from project.research.validation.schemas import PromotionDecision
from project.research.promotion.promotion_thresholds import _build_bundle_policy


REGISTRY_ROOT = Path("project/configs/registries")


def _request(*, trigger_space: TriggerSpace) -> AgentExperimentRequest:
    return AgentExperimentRequest(
        program_id="governance_test",
        run_mode="research",
        description="governance test",
        instrument_scope=InstrumentScope(
            instrument_classes=["crypto"],
            symbols=["BTCUSDT"],
            timeframe="5m",
            start="2024-01-01",
            end="2024-02-01",
        ),
        trigger_space=trigger_space,
        templates=TemplateSelection(include=["mean_reversion"]),
        evaluation=EvaluationConfig(horizons_bars=[12], directions=["long"], entry_lags=[1]),
        contexts=ContextSelection(include={}),
        search_control=SearchControl(
            max_hypotheses_total=100,
            max_hypotheses_per_template=100,
            max_hypotheses_per_event_family=100,
        ),
        promotion=PromotionConfig(enabled=False),
        artifacts={},
    )


def test_default_planning_event_set_excludes_context_and_repair_only_events() -> None:
    registry = get_domain_registry()
    planning = set(default_planning_event_ids(registry.default_executable_event_ids()))

    assert "BASIS_DISLOC" in planning
    assert "CROSS_ASSET_DESYNC_EVENT" not in planning
    assert "FUNDING_TIMESTAMP_EVENT" not in planning


def test_default_planning_event_set_excludes_hybridized_compatibility_events() -> None:
    registry = get_domain_registry()
    planning = set(default_planning_event_ids(registry.default_executable_event_ids()))

    assert "LIQUIDITY_STRESS_PROXY" not in planning
    assert "WICK_REVERSAL_PROXY" not in planning
    assert "PRICE_VOL_IMBALANCE_PROXY" not in planning
    assert "ABSORPTION_PROXY" not in planning
    assert "DEPTH_STRESS_PROXY" not in planning
    assert "FLOW_EXHAUSTION_PROXY" not in planning
    assert "ORDERFLOW_IMBALANCE_SHOCK" not in planning
    assert "SWEEP_STOPRUN" not in planning


def test_governance_filters_can_request_context_events_explicitly() -> None:
    registries = RegistryBundle(REGISTRY_ROOT)
    request = _request(
        trigger_space=TriggerSpace(
            allowed_trigger_types=["EVENT"],
            canonical_regimes=["TEMPORAL_STRUCTURE"],
            tiers=["C"],
            operational_roles=["context"],
        )
    )

    resolved = set(_resolve_requested_event_ids(request, registries))
    assert "FUNDING_TIMESTAMP_EVENT" in resolved
    assert "SESSION_OPEN_EVENT" in resolved
    assert "BASIS_DISLOC" not in resolved


def test_candidate_rank_key_penalizes_low_fidelity_event_contracts() -> None:
    common = {
        "selection_score": 1.0,
        "expectancy_after_multiplicity": 1.0,
        "robustness_score": 1.0,
        "source_type": "edge_candidate",
    }
    strong = {**common, "event": "BASIS_DISLOC", "strategy_candidate_id": "strong"}
    weak = {**common, "event": "SESSION_OPEN_EVENT", "strategy_candidate_id": "weak"}

    assert candidate_rank_key(strong) < candidate_rank_key(weak)


def test_reduced_evidence_bundle_fails_event_discipline_for_context_events() -> None:
    meta = get_event_governance_metadata("SESSION_OPEN_EVENT")
    policy = _build_bundle_policy(
        max_q_value=0.1,
        min_events=1,
        min_stability_score=0.0,
        min_sign_consistency=0.0,
        min_cost_survival_ratio=0.0,
        max_negative_control_pass_rate=1.0,
        min_tob_coverage=0.0,
        require_hypothesis_audit=False,
        allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=False,
        require_retail_viability=False,
        require_low_capital_viability=False,
        promotion_profile="research",
        enforce_baseline_beats_complexity=False,
        enforce_placebo_controls=False,
        enforce_timeframe_consensus=False,
        enforce_regime_stability=False,
    )
    bundle = {
        "candidate_id": "cand_1",
        "event_family": "TEMPORAL_STRUCTURE",
        "event_type": "SESSION_OPEN_EVENT",
        "run_id": "run_1",
        "sample_definition": {"n_events": 10, "symbol": "BTCUSDT"},
        "split_definition": {"split_scheme_id": "wf", "bar_duration_minutes": 5},
        "effect_estimates": {"estimate_bps": 5.0},
        "uncertainty_estimates": {
            "q_value": 0.01,
            "q_value_by": 0.01,
            "q_value_cluster": 0.01,
        },
        "stability_tests": {
            "stability_score": 1.0,
            "sign_consistency": 1.0,
            "delay_robustness_pass": True,
            "timeframe_consensus_pass": True,
        },
        "falsification_results": {"negative_control_pass": True, "passes_control": True},
        "cost_robustness": {
            "cost_survival_ratio": 1.0,
            "tob_coverage": 1.0,
            "microstructure_pass": True,
        },
        "multiplicity_adjustment": {"q_value_program": 0.01},
        "metadata": {
            "gate_stability": True,
            "gate_promo_oos_validation": True,
            "gate_promo_hypothesis_audit": True,
            "gate_promo_dsr": True,
            "gate_promo_robustness": True,
            "gate_promo_regime": True,
            "gate_delayed_entry_stress": True,
            "gate_after_cost_stressed_positive": True,
            "event_is_descriptive": meta["event_is_descriptive"],
            "event_is_trade_trigger": meta["event_is_trade_trigger"],
            "event_requires_stronger_evidence": meta["requires_stronger_evidence"],
            "is_reduced_evidence": True,
        },
        "promotion_decision": PromotionDecision(
            eligible=False,
            promotion_status="rejected",
            promotion_track="fallback_only",
            rank_score=0.0,
        ).to_dict(),
    }

    decision = evaluate_promotion_bundle(bundle, policy)
    assert decision["gate_results"]["event_discipline"] == "fail"
    assert "event_discipline" in decision["rejection_reasons"]
