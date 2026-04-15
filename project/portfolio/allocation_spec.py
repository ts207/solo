from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, field_validator

from project.strategy.dsl.schema import Blueprint


ALLOCATION_SPEC_VERSION = "allocation_spec_v1"


class AllocationMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    spec_version: str = ALLOCATION_SPEC_VERSION
    run_id: str
    blueprint_id: str
    candidate_id: str
    event_type: str
    retail_profile: str


class SizingPolicySpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    mode: str
    risk_per_trade: float | None = None
    max_gross_leverage: float
    portfolio_risk_budget: float = 1.0
    symbol_risk_budget: float = 1.0
    signal_scaling: Dict[str, Any] = Field(default_factory=dict)
    # Phase 4.4: sizing inputs populated from promotion audit data.
    # expected_return_bps — mean return from the promotion audit row (mean_return_bps).
    # expected_adverse_bps — stressed adverse scenario derived from
    #   stressed_after_cost_expectancy × 1.5 (per the vision spec).
    # Both default to None (unpopulated) so existing code paths are unaffected.
    expected_return_bps: float | None = None
    expected_adverse_bps: float | None = None

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        mode = str(v).strip().lower()
        allowed = {"fixed", "fixed_risk", "kelly", "vol_target"}
        if mode not in allowed:
            raise ValueError(
                "sizing_policy.mode must be one of 'fixed', 'fixed_risk', 'kelly', or 'vol_target', "
                f"got {v!r}"
            )
        # Preserve the historically used alias for fixed-risk sizing.
        return "fixed_risk" if mode == "fixed" else mode


class RiskControlSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    low_capital_contract: Dict[str, Any] = Field(default_factory=dict)
    max_concurrent_positions: int
    per_position_notional_cap_usd: float
    fee_tier: str
    cost_model: Dict[str, Any] = Field(default_factory=dict)


class AllocationPolicySpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol_scope: Dict[str, Any]
    constraints: Dict[str, Any] = Field(default_factory=dict)


class AllocationSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    metadata: AllocationMetadata
    sizing_policy: SizingPolicySpec
    risk_controls: RiskControlSpec
    allocation_policy: AllocationPolicySpec

    def to_allocator_params(self) -> Dict[str, Any]:
        constraints = dict(self.allocation_policy.constraints)
        strategy_family_map = dict(constraints.get("strategy_family_map", {}))
        family_risk_budgets = dict(constraints.get("family_risk_budgets", {}))
        strategy_risk_budgets = dict(constraints.get("strategy_risk_budgets", {}))
        strategy_thesis_map = dict(constraints.get("strategy_thesis_map", {}))
        thesis_overlap_group_map = dict(constraints.get("thesis_overlap_group_map", {}))
        overlap_group_risk_budgets = dict(constraints.get("overlap_group_risk_budgets", {}))
        thesis_evidence_multipliers = dict(constraints.get("thesis_evidence_multipliers", {}))
        overlap_mode = str(constraints.get("overlap_mode", "budgeted")).strip().lower()
        thesis_ranking_data = dict(constraints.get("thesis_ranking_data", {}))
        
        return {
            "allocator_mode": str(constraints.get("allocator_mode", "heuristic")).strip().lower(),
            "allocator_deterministic": bool(constraints.get("allocator_deterministic", True)),
            "allocator_turnover_penalty": float(constraints.get("allocator_turnover_penalty", 0.0)),
            "strategy_risk_budgets": strategy_risk_budgets,
            "family_risk_budgets": family_risk_budgets,
            "strategy_family_map": strategy_family_map,
            "strategy_thesis_map": strategy_thesis_map,
            "thesis_overlap_group_map": thesis_overlap_group_map,
            "overlap_group_risk_budgets": overlap_group_risk_budgets,
            "thesis_evidence_multipliers": thesis_evidence_multipliers,
            "overlap_mode": overlap_mode,
            "thesis_ranking_data": thesis_ranking_data,
            "portfolio_max_exposure": float(self.risk_controls.per_position_notional_cap_usd),
            "max_portfolio_gross": float(self.sizing_policy.portfolio_risk_budget),
            "max_strategy_gross": float(self.sizing_policy.max_gross_leverage),
            "max_symbol_gross": float(self.sizing_policy.symbol_risk_budget),
            "max_new_exposure_per_bar": float(constraints.get("max_new_exposure_per_bar", 10.0)),
            "enable_correlation_allocation": bool(
                constraints.get("enable_correlation_allocation", False)
            ),
            "max_pairwise_correlation": constraints.get("max_pairwise_correlation"),
            "drawdown_limit": constraints.get("drawdown_limit"),
            "portfolio_max_drawdown": constraints.get("portfolio_max_drawdown"),
            "max_symbol_exposure": constraints.get("max_symbol_exposure"),
            "pnl_mode": constraints.get("pnl_mode", "dollar"),
        }

    @classmethod
    def from_blueprint(
        cls,
        *,
        blueprint: Blueprint,
        run_id: str,
        retail_profile: str,
        low_capital_contract: Dict[str, Any],
        effective_max_concurrent_positions: int,
        effective_per_position_notional_cap_usd: float,
        default_fee_tier: str,
        fees_bps_per_side: float,
        slippage_bps_per_fill: float,
        # Phase 4.4: sizing inputs from promotion audit data.
        # When provided these replace None defaults in SizingPolicySpec,
        # enabling the live runner to use calibrated Kelly-adjusted sizing.
        expected_return_bps: float | None = None,
        expected_adverse_bps: float | None = None,
    ) -> "AllocationSpec":
        constraints = dict(blueprint.lineage.constraints or {})
        # Fallback: read sizing inputs from blueprint constraints if not explicitly provided.
        # This covers blueprints compiled via blueprint_compilation.py which embeds
        # expected_return_bps / expected_adverse_bps directly in the lineage constraints.
        if expected_return_bps is None:
            v = constraints.get("expected_return_bps")
            expected_return_bps = float(v) if v is not None else None
        if expected_adverse_bps is None:
            v = constraints.get("expected_adverse_bps")
            expected_adverse_bps = float(v) if v is not None else None
        return cls(
            metadata=AllocationMetadata(
                run_id=run_id,
                blueprint_id=blueprint.id,
                candidate_id=blueprint.candidate_id,
                event_type=blueprint.event_type,
                retail_profile=retail_profile,
            ),
            sizing_policy=SizingPolicySpec(
                mode=blueprint.sizing.mode,
                risk_per_trade=blueprint.sizing.risk_per_trade,
                max_gross_leverage=blueprint.sizing.max_gross_leverage,
                portfolio_risk_budget=blueprint.sizing.portfolio_risk_budget,
                symbol_risk_budget=blueprint.sizing.symbol_risk_budget,
                signal_scaling=dict(blueprint.sizing.signal_scaling),
                expected_return_bps=expected_return_bps,
                expected_adverse_bps=expected_adverse_bps,
            ),
            risk_controls=RiskControlSpec(
                low_capital_contract=dict(low_capital_contract),
                max_concurrent_positions=effective_max_concurrent_positions,
                per_position_notional_cap_usd=effective_per_position_notional_cap_usd,
                fee_tier=default_fee_tier,
                cost_model={
                    "fees_bps_per_side": fees_bps_per_side,
                    "slippage_bps_per_fill": slippage_bps_per_fill,
                },
            ),
            allocation_policy=AllocationPolicySpec(
                symbol_scope=blueprint.symbol_scope.model_dump(),
                constraints=constraints,
            ),
        )
