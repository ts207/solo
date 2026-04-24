from __future__ import annotations

from typing import Any, Dict, Literal

from pydantic import BaseModel, ConfigDict, Field

from project.strategy.dsl.schema import Blueprint

EXECUTABLE_STRATEGY_SPEC_VERSION = "executable_strategy_spec_v1"


class ExecutableStrategyMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    spec_version: str = EXECUTABLE_STRATEGY_SPEC_VERSION
    proposal_id: str = ""
    run_id: str
    hypothesis_id: str = ""
    blueprint_id: str
    candidate_id: str
    canonical_event_type: str = ""
    canonical_regime: str = ""
    routing_profile_id: str = ""
    event_type: str
    direction: str
    retail_profile: str


class ResearchOriginSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_path: str
    compiler_version: str
    generated_at_utc: str
    ontology_spec_hash: str = ""
    promotion_track: str = "standard"
    canonical_event_type: str = ""
    research_family: str = ""
    canonical_family: str = ""
    canonical_regime: str = ""
    subtype: str = ""
    phase: str = ""
    evidence_mode: str = ""
    regime_bucket: str = ""
    recommended_bucket: str = ""
    routing_profile_id: str = ""
    template_verb: str = ""
    wf_status: str = "pending"
    wf_evidence_hash: str = ""
    constraints: Dict[str, Any] = Field(default_factory=dict)


class ExecutableEntrySpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    triggers: list[str]
    conditions: list[str]
    confirmations: list[str]
    delay_bars: int
    cooldown_bars: int
    condition_logic: str
    order_type_assumption: Literal["market"] = "market"


class ExecutableExitSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    time_stop_bars: int
    stop_type: str
    stop_value: float
    target_type: str
    target_value: float
    trailing_stop_type: str
    trailing_stop_value: float
    break_even_r: float
    invalidation: Dict[str, Any]


class ExecutableRiskSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    low_capital_contract: Dict[str, Any] = Field(default_factory=dict)
    cost_model: Dict[str, Any] = Field(default_factory=dict)


class ExecutableSizingSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    mode: str
    risk_per_trade: float | None = None
    max_gross_leverage: float


class ExecutableExecutionSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol_scope: Dict[str, Any]
    execution: Dict[str, Any]
    policy_executor_config: Dict[str, Any]
    throttles: Dict[str, Any]


class PortfolioConstraintsSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    effective_per_position_notional_cap_usd: float
    effective_max_concurrent_positions: int


class ExecutableStrategySpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    metadata: ExecutableStrategyMetadata
    research_origin: ResearchOriginSpec
    entry: ExecutableEntrySpec
    exit: ExecutableExitSpec
    risk: ExecutableRiskSpec
    sizing: ExecutableSizingSpec
    execution: ExecutableExecutionSpec
    portfolio_constraints: PortfolioConstraintsSpec

    def to_blueprint_dict(self) -> Dict[str, Any]:
        execution_cfg = dict(self.execution.execution)
        cost_model = dict(self.risk.cost_model)
        return {
            "id": self.metadata.blueprint_id,
            "run_id": self.metadata.run_id,
            "event_type": self.metadata.event_type,
            "candidate_id": self.metadata.candidate_id,
            "symbol_scope": dict(self.execution.symbol_scope),
            "direction": self.metadata.direction,
            "entry": {
                "triggers": list(self.entry.triggers),
                "conditions": list(self.entry.conditions),
                "confirmations": list(self.entry.confirmations),
                "delay_bars": int(self.entry.delay_bars),
                "cooldown_bars": int(self.entry.cooldown_bars),
                "condition_logic": self.entry.condition_logic,
                "condition_nodes": [],
                "arm_bars": max(
                    0,
                    int(
                        self.execution.policy_executor_config.get(
                            "entry_delay_bars", self.entry.delay_bars
                        )
                    ),
                ),
                "reentry_lockout_bars": int(
                    self.execution.throttles.get("cooldown_bars", self.entry.cooldown_bars)
                ),
            },
            "exit": {
                "time_stop_bars": int(self.exit.time_stop_bars),
                "invalidation": dict(self.exit.invalidation),
                "stop_type": self.exit.stop_type,
                "stop_value": float(self.exit.stop_value),
                "target_type": self.exit.target_type,
                "target_value": float(self.exit.target_value),
                "trailing_stop_type": self.exit.trailing_stop_type,
                "trailing_stop_value": float(self.exit.trailing_stop_value),
                "break_even_r": float(self.exit.break_even_r),
            },
            "execution": execution_cfg,
            "sizing": {
                "mode": self.sizing.mode,
                "risk_per_trade": self.sizing.risk_per_trade,
                "target_vol": None,
                "max_gross_leverage": float(self.sizing.max_gross_leverage),
                "max_position_scale": 1.0,
                "portfolio_risk_budget": 1.0,
                "symbol_risk_budget": 1.0,
                "signal_scaling": {},
            },
            "overlays": [],
            "evaluation": {
                "min_trades": 0,
                "cost_model": {
                    "fees_bps": float(cost_model.get("fees_bps_per_side", 0.0)),
                    "slippage_bps": float(cost_model.get("slippage_bps_per_fill", 0.0)),
                    "funding_included": True,
                },
                "robustness_flags": {
                    "oos_required": False,
                    "multiplicity_required": False,
                    "regime_stability_required": False,
                },
            },
            "lineage": {
                "proposal_id": self.metadata.proposal_id,
                "hypothesis_id": self.metadata.hypothesis_id,
                "source_path": self.research_origin.source_path,
                "compiler_version": self.research_origin.compiler_version,
                "generated_at_utc": self.research_origin.generated_at_utc,
                "wf_evidence_hash": self.research_origin.wf_evidence_hash,
                "wf_status": self.research_origin.wf_status,
                "promotion_track": self.research_origin.promotion_track,
                "ontology_spec_hash": self.research_origin.ontology_spec_hash,
                "canonical_event_type": self.research_origin.canonical_event_type,
                "research_family": self.research_origin.research_family,
                "canonical_family": self.research_origin.canonical_family,
                "canonical_regime": self.research_origin.canonical_regime,
                "subtype": self.research_origin.subtype,
                "phase": self.research_origin.phase,
                "evidence_mode": self.research_origin.evidence_mode,
                "regime_bucket": self.research_origin.regime_bucket,
                "recommended_bucket": self.research_origin.recommended_bucket,
                "routing_profile_id": self.research_origin.routing_profile_id,
                "template_verb": self.research_origin.template_verb,
                "constraints": dict(self.research_origin.constraints),
            },
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
    ) -> "ExecutableStrategySpec":
        constraints = blueprint.lineage.constraints or {}
        entry_delay = blueprint.entry.delay_bars
        return cls(
            metadata=ExecutableStrategyMetadata(
                proposal_id=blueprint.lineage.proposal_id,
                run_id=run_id,
                hypothesis_id=blueprint.lineage.hypothesis_id,
                blueprint_id=blueprint.id,
                candidate_id=blueprint.candidate_id,
                canonical_event_type=blueprint.lineage.canonical_event_type,
                canonical_regime=blueprint.lineage.canonical_regime,
                routing_profile_id=blueprint.lineage.routing_profile_id,
                event_type=blueprint.event_type,
                direction=blueprint.direction,
                retail_profile=retail_profile,
            ),
            research_origin=ResearchOriginSpec(
                source_path=blueprint.lineage.source_path,
                compiler_version=blueprint.lineage.compiler_version,
                generated_at_utc=blueprint.lineage.generated_at_utc,
                ontology_spec_hash=blueprint.lineage.ontology_spec_hash,
                promotion_track=blueprint.lineage.promotion_track,
                canonical_event_type=blueprint.lineage.canonical_event_type,
                research_family=blueprint.lineage.research_family,
                canonical_family=blueprint.lineage.canonical_family,
                canonical_regime=blueprint.lineage.canonical_regime,
                subtype=blueprint.lineage.subtype,
                phase=blueprint.lineage.phase,
                evidence_mode=blueprint.lineage.evidence_mode,
                regime_bucket=blueprint.lineage.regime_bucket,
                recommended_bucket=blueprint.lineage.recommended_bucket,
                routing_profile_id=blueprint.lineage.routing_profile_id,
                template_verb=blueprint.lineage.template_verb,
                wf_status=blueprint.lineage.wf_status,
                wf_evidence_hash=blueprint.lineage.wf_evidence_hash,
                constraints=dict(constraints),
            ),
            entry=ExecutableEntrySpec(
                triggers=list(blueprint.entry.triggers),
                conditions=list(blueprint.entry.conditions),
                confirmations=list(blueprint.entry.confirmations),
                delay_bars=entry_delay,
                cooldown_bars=blueprint.entry.cooldown_bars,
                condition_logic=blueprint.entry.condition_logic,
            ),
            exit=ExecutableExitSpec(
                time_stop_bars=blueprint.exit.time_stop_bars,
                stop_type=blueprint.exit.stop_type,
                stop_value=blueprint.exit.stop_value,
                target_type=blueprint.exit.target_type,
                target_value=blueprint.exit.target_value,
                trailing_stop_type=blueprint.exit.trailing_stop_type,
                trailing_stop_value=blueprint.exit.trailing_stop_value,
                break_even_r=blueprint.exit.break_even_r,
                invalidation=dict(blueprint.exit.invalidation),
            ),
            risk=ExecutableRiskSpec(
                low_capital_contract=dict(low_capital_contract),
                cost_model={
                    "fees_bps_per_side": fees_bps_per_side,
                    "slippage_bps_per_fill": slippage_bps_per_fill,
                    "default_fee_tier": default_fee_tier,
                },
            ),
            sizing=ExecutableSizingSpec(
                mode=blueprint.sizing.mode,
                risk_per_trade=blueprint.sizing.risk_per_trade,
                max_gross_leverage=blueprint.sizing.max_gross_leverage,
            ),
            execution=ExecutableExecutionSpec(
                symbol_scope=blueprint.symbol_scope.model_dump(),
                execution=blueprint.execution.model_dump(),
                policy_executor_config={
                    "entry_delay_bars": entry_delay,
                    "max_concurrent_positions": effective_max_concurrent_positions,
                    "per_position_notional_cap_usd": effective_per_position_notional_cap_usd,
                    "fee_tier": default_fee_tier,
                },
                throttles={
                    "one_trade_per_episode": bool(
                        constraints.get("variant_one_trade_per_episode", False)
                    ),
                    "cooldown_bars": int(
                        constraints.get("variant_cooldown_bars", blueprint.entry.cooldown_bars)
                    ),
                    "max_concurrent_positions": effective_max_concurrent_positions,
                },
            ),
            portfolio_constraints=PortfolioConstraintsSpec(
                effective_per_position_notional_cap_usd=effective_per_position_notional_cap_usd,
                effective_max_concurrent_positions=effective_max_concurrent_positions,
            ),
        )
