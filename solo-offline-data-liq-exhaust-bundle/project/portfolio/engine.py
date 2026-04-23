from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Set, Tuple, Optional

import pandas as pd

from project.portfolio.admission_policy import AdmissionResult, PortfolioAdmissionPolicy
from project.portfolio.incubation import IncubationEvidence
from project.portfolio.covariance import covariance_exposure_multiplier
from project.portfolio.exposure_overlap import overlap_exposure_multiplier
from project.portfolio.marginal_risk import estimate_marginal_risk, marginal_risk_multiplier
from project.portfolio.risk_budget import (
    calculate_cluster_risk_multiplier,
    calculate_edge_risk_multiplier,
    calculate_execution_quality_multiplier,
    calculate_portfolio_risk_multiplier,
    get_asset_correlation_adjustment,
)


@dataclass(frozen=True)
class ThesisIntent:
    """Input to the engine: a thesis requesting capital allocation."""

    thesis_id: str
    symbol: str
    family: str
    overlap_group_id: str
    requested_notional: float
    cluster_id: int = 0
    asset_bucket: str = "default"
    incubation_state: str = "live"
    support_score: float = 0.0
    evidence_multiplier: float = 1.0
    expected_net_edge_bps: float | None = None
    expected_downside_bps: float | None = None
    fill_probability: float | None = None
    edge_confidence: float | None = None
    execution_quality: float | None = None
    marginal_volatility: float | None = None
    marginal_drawdown_contribution: float | None = None
    overlap_score: float | None = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PortfolioCapitalDecision:
    """One auditable capital decision per thesis with full reason chain."""

    thesis_id: str
    symbol: str
    family: str
    overlap_group_id: str
    requested_notional: float
    allocated_notional: float
    risk_multiplier: float
    cluster_multiplier: float
    correlation_adjustment: float
    incubation_state: str
    admission: AdmissionResult
    edge_multiplier: float = 1.0
    marginal_risk_multiplier: float = 1.0
    overlap_multiplier: float = 1.0
    execution_quality_multiplier: float = 1.0
    covariance_multiplier: float = 1.0
    decision_status: str = "blocked"
    priority_score: float = 0.0
    available_capacity_notional: float = 0.0
    clip_factors: Tuple[str, ...] = ()
    reasons: Tuple[str, ...] = ()

    @property
    def is_allocated(self) -> bool:
        return self.allocated_notional > 0.0

    def summary(self) -> str:
        status = "ALLOCATED" if self.is_allocated else "BLOCKED"
        return (
            f"[{status}] {self.thesis_id} symbol={self.symbol} "
            f"allocated={self.allocated_notional:.0f} "
            f"requested={self.requested_notional:.0f} "
            f"risk_mult={self.risk_multiplier:.3f} "
            f"edge_mult={self.edge_multiplier:.3f} "
            f"status={self.decision_status} reasons={self.reasons}"
        )


class PortfolioDecisionEngine:
    """Unified capital allocation engine.

    Wires together overlap gating, family budgets, symbol exposures, risk
    multipliers, cluster throttling, correlation adjustments, and incubation
    state into one auditable decision per thesis intent.

    All decisions are returned as immutable PortfolioCapitalDecision objects
    so callers can log, replay, or audit the full reason chain.
    """

    def __init__(
        self,
        *,
        family_budgets: Dict[str, float] | None = None,
        symbol_caps: Dict[str, float] | None = None,
        max_gross_leverage: float = 1.0,
        target_vol: float = 0.10,
        current_vol: float = 0.10,
        gross_exposure: float = 0.0,
        correlation_limit: float = 0.5,
        max_strategies_per_cluster: int = 3,
        thesis_covariance: pd.DataFrame | None = None,
        overlap_budgets: Dict[str, float] | None = None,
        max_portfolio_notional: float | None = None,
    ) -> None:
        self._admission = PortfolioAdmissionPolicy(family_budgets=family_budgets or {})
        self._symbol_caps: Dict[str, float] = dict(symbol_caps or {})
        self._max_gross_leverage = max_gross_leverage
        self._target_vol = target_vol
        self._current_vol = current_vol
        self._gross_exposure = gross_exposure
        self._correlation_limit = correlation_limit
        self._max_strategies_per_cluster = max_strategies_per_cluster
        self._thesis_covariance = thesis_covariance
        self._overlap_budgets = dict(overlap_budgets or {})
        self._max_portfolio_notional = (
            float(max_portfolio_notional) if max_portfolio_notional is not None else None
        )

    def decide(
        self,
        intents: List[ThesisIntent],
        *,
        active_overlap_groups: Set[str] | None = None,
        family_exposures: Dict[str, float] | None = None,
        bucket_exposures: Dict[str, float] | None = None,
        active_cluster_counts: Dict[int, int] | None = None,
        symbol_exposures: Dict[str, float] | None = None,
        thesis_exposures: Dict[str, float] | None = None,
        overlap_exposures: Dict[str, float] | None = None,
        gross_exposure: float | None = None,
        current_vol: float | None = None,
        available_portfolio_notional: float | None = None,
        incubation_evidence: Dict[str, IncubationEvidence] | None = None,
    ) -> List[PortfolioCapitalDecision]:
        """Produce one PortfolioCapitalDecision per intent, in priority order.

        Intents are processed in descending expected utility order when EV
        fields are present, with support_score retained as the compatibility
        fallback. Overlap, family, symbol, and gross caps still gate the final
        committed allocation state.
        """
        active_groups: Set[str] = set(active_overlap_groups or set())
        fam_exp: Dict[str, float] = dict(family_exposures or {})
        bucket_exp: Dict[str, float] = dict(bucket_exposures or {})
        cluster_counts: Dict[int, int] = dict(active_cluster_counts or {})
        sym_exp: Dict[str, float] = dict(symbol_exposures or {})
        thesis_exp: Dict[str, float] = dict(thesis_exposures or {})
        overlap_exp: Dict[str, float] = dict(overlap_exposures or {})

        current_gross_exposure = float(self._gross_exposure if gross_exposure is None else gross_exposure)
        current_realized_vol = float(self._current_vol if current_vol is None else current_vol)
        portfolio_risk_mult = calculate_portfolio_risk_multiplier(
            gross_exposure=current_gross_exposure,
            max_gross_leverage=self._max_gross_leverage,
            target_vol=self._target_vol,
            current_vol=current_realized_vol,
        )

        remaining_portfolio_capacity = None
        configured_capacity = self._max_portfolio_notional
        if available_portfolio_notional is not None:
            configured_capacity = float(available_portfolio_notional)
        if configured_capacity is not None:
            remaining_portfolio_capacity = max(0.0, float(configured_capacity))

        incubation_map: Dict[str, IncubationEvidence] = dict(incubation_evidence or {})

        sorted_intents = sorted(intents, key=self._priority_score, reverse=True)
        decisions: List[PortfolioCapitalDecision] = []

        for intent in sorted_intents:
            reasons: List[str] = []

            # --- incubation gate ---
            if intent.incubation_state == "incubating":
                evidence = incubation_map.get(intent.thesis_id)
                if evidence is None:
                    reasons.append("blocked:incubating_without_evidence")
                    decisions.append(
                        self._blocked(intent, reasons, portfolio_risk_mult, cluster_counts)
                    )
                    continue
                should_graduate, evidence_reason = evidence.evaluate_graduation()
                if not should_graduate:
                    reasons.append(f"blocked:incubation:{evidence_reason}")
                    decisions.append(
                        self._blocked(intent, reasons, portfolio_risk_mult, cluster_counts)
                    )
                    continue
                reasons.append(f"allow:incubation:{evidence_reason}")

            # --- overlap group gate ---
            admission = self._admission.is_thesis_admissible(
                intent.thesis_id, intent.overlap_group_id, active_groups
            )
            if not admission.admissible:
                reasons.append(f"overlap:{admission.reason}")
                decisions.append(
                    self._blocked(
                        intent, reasons, portfolio_risk_mult, cluster_counts, admission=admission
                    )
                )
                continue

            # --- family budget gate ---
            fam_admission = self._admission.is_family_admissible(intent.family, fam_exp)
            if not fam_admission.admissible:
                reasons.append(f"family:{fam_admission.reason}")
                decisions.append(
                    self._blocked(
                        intent,
                        reasons,
                        portfolio_risk_mult,
                        cluster_counts,
                        admission=fam_admission,
                    )
                )
                continue

            # --- symbol cap gate ---
            sym_cap = self._symbol_caps.get(intent.symbol, 0.0)
            if sym_cap > 0.0 and sym_exp.get(intent.symbol, 0.0) >= sym_cap:
                reasons.append(f"blocked:symbol_cap_exhausted:{intent.symbol}")
                decisions.append(
                    self._blocked(intent, reasons, portfolio_risk_mult, cluster_counts)
                )
                continue

            # --- multipliers ---
            cluster_mult = calculate_cluster_risk_multiplier(
                cluster_id=intent.cluster_id,
                active_cluster_counts=cluster_counts,
                max_strategies_per_cluster=self._max_strategies_per_cluster,
            )
            corr_adj = get_asset_correlation_adjustment(
                asset_bucket=intent.asset_bucket,
                bucket_exposures=bucket_exp,
                correlation_limit=self._correlation_limit,
            )
            covariance_mult = 1.0
            if self._thesis_covariance is not None:
                covariance_mult = covariance_exposure_multiplier(
                    intent.thesis_id,
                    self._thesis_covariance,
                    thesis_exp,
                    correlation_limit=self._correlation_limit,
                )
            edge_mult = self._edge_multiplier(intent)
            if edge_mult <= 0.0:
                reasons.append("edge:non_positive_post_cost_utility")
                decisions.append(
                    self._blocked(intent, reasons, portfolio_risk_mult, cluster_counts)
                )
                continue
            risk_estimate = estimate_marginal_risk(
                downside_bps=intent.expected_downside_bps,
                marginal_volatility=intent.marginal_volatility,
                marginal_drawdown_contribution=intent.marginal_drawdown_contribution,
            )
            marginal_mult = marginal_risk_multiplier(risk_estimate)
            overlap_mult = overlap_exposure_multiplier(
                overlap_score=intent.overlap_score,
                active_overlap_notional=overlap_exp.get(intent.overlap_group_id, 0.0),
                overlap_budget=self._overlap_budgets.get(intent.overlap_group_id),
            )
            execution_mult = calculate_execution_quality_multiplier(
                explicit_quality=intent.execution_quality,
            )
            combined_mult = (
                portfolio_risk_mult
                * cluster_mult
                * corr_adj
                * covariance_mult
                * edge_mult
                * marginal_mult
                * overlap_mult
                * execution_mult
                * intent.evidence_multiplier
            )
            raw_allocated = max(0.0, intent.requested_notional * combined_mult)

            clip_factors: List[str] = []
            remaining_caps: List[float] = []
            family_budget = self._admission.family_budgets.get(intent.family, 0.0)
            if family_budget > 0.0:
                remaining_family = max(0.0, family_budget - abs(fam_exp.get(intent.family, 0.0)))
                remaining_caps.append(remaining_family)
                if raw_allocated > remaining_family:
                    clip_factors.append("family_budget")
            if sym_cap > 0.0:
                remaining_symbol = max(0.0, sym_cap - abs(sym_exp.get(intent.symbol, 0.0)))
                remaining_caps.append(remaining_symbol)
                if raw_allocated > remaining_symbol:
                    clip_factors.append("symbol_cap")
            overlap_budget = self._overlap_budgets.get(intent.overlap_group_id)
            if overlap_budget is not None and float(overlap_budget) > 0.0:
                remaining_overlap = max(0.0, float(overlap_budget) - abs(overlap_exp.get(intent.overlap_group_id, 0.0)))
                remaining_caps.append(remaining_overlap)
                if raw_allocated > remaining_overlap:
                    clip_factors.append("overlap_budget")
            if remaining_portfolio_capacity is not None:
                remaining_caps.append(remaining_portfolio_capacity)
                if raw_allocated > remaining_portfolio_capacity:
                    clip_factors.append("portfolio_capacity")

            available_capacity = min(remaining_caps) if remaining_caps else raw_allocated
            if remaining_caps and available_capacity <= 0.0:
                reasons.append("blocked:no_remaining_capacity")
                decisions.append(
                    self._blocked(intent, reasons, portfolio_risk_mult, cluster_counts, admission=admission)
                )
                continue

            allocated = min(raw_allocated, available_capacity)
            decision_status = "allocated" if allocated >= max(1e-9, intent.requested_notional * 0.999) else ("reduced" if allocated > 0.0 else "blocked")

            reasons.append(f"risk_mult={combined_mult:.3f}")
            reasons.append(f"edge_mult={edge_mult:.3f}")
            reasons.append(f"marginal_risk_mult={marginal_mult:.3f}")
            reasons.append(f"execution_quality_mult={execution_mult:.3f}")
            for factor in clip_factors:
                reasons.append(f"clip:{factor}")
            decision = PortfolioCapitalDecision(
                thesis_id=intent.thesis_id,
                symbol=intent.symbol,
                family=intent.family,
                overlap_group_id=intent.overlap_group_id,
                requested_notional=intent.requested_notional,
                allocated_notional=allocated,
                risk_multiplier=portfolio_risk_mult,
                cluster_multiplier=cluster_mult,
                correlation_adjustment=corr_adj,
                edge_multiplier=edge_mult,
                marginal_risk_multiplier=marginal_mult,
                overlap_multiplier=overlap_mult,
                execution_quality_multiplier=execution_mult,
                covariance_multiplier=covariance_mult,
                incubation_state=intent.incubation_state,
                admission=admission,
                decision_status=decision_status,
                priority_score=self._priority_score(intent),
                available_capacity_notional=float(available_capacity),
                clip_factors=tuple(clip_factors),
                reasons=tuple(reasons),
            )
            decisions.append(decision)

            # commit allocation state for subsequent intents
            if intent.overlap_group_id:
                active_groups.add(intent.overlap_group_id)
            fam_exp[intent.family] = fam_exp.get(intent.family, 0.0) + allocated
            bucket_exp[intent.asset_bucket] = bucket_exp.get(intent.asset_bucket, 0.0) + allocated
            sym_exp[intent.symbol] = sym_exp.get(intent.symbol, 0.0) + allocated
            thesis_exp[intent.thesis_id] = thesis_exp.get(intent.thesis_id, 0.0) + allocated
            if intent.overlap_group_id:
                overlap_exp[intent.overlap_group_id] = (
                    overlap_exp.get(intent.overlap_group_id, 0.0) + allocated
                )
            cluster_counts[intent.cluster_id] = cluster_counts.get(intent.cluster_id, 0) + 1
            if remaining_portfolio_capacity is not None:
                remaining_portfolio_capacity = max(0.0, remaining_portfolio_capacity - allocated)

        return decisions

    def _priority_score(self, intent: ThesisIntent) -> float:
        if intent.expected_net_edge_bps is None:
            return float(intent.support_score)
        downside = max(1.0, abs(float(intent.expected_downside_bps or 100.0)))
        fill = float(intent.fill_probability if intent.fill_probability is not None else 1.0)
        confidence = float(intent.edge_confidence if intent.edge_confidence is not None else 1.0)
        execution = float(intent.execution_quality if intent.execution_quality is not None else 1.0)
        utility = float(intent.expected_net_edge_bps) / downside
        return utility * fill * confidence * execution

    def _edge_multiplier(self, intent: ThesisIntent) -> float:
        if intent.expected_net_edge_bps is None:
            return 1.0
        return calculate_edge_risk_multiplier(
            expected_net_edge_bps=float(intent.expected_net_edge_bps),
            expected_downside_bps=float(intent.expected_downside_bps or 100.0),
            fill_probability=float(
                intent.fill_probability if intent.fill_probability is not None else 1.0
            ),
            edge_confidence=float(
                intent.edge_confidence if intent.edge_confidence is not None else 1.0
            ),
        )

    def _blocked(
        self,
        intent: ThesisIntent,
        reasons: List[str],
        risk_multiplier: float,
        cluster_counts: Dict[int, int],
        *,
        admission: AdmissionResult | None = None,
    ) -> PortfolioCapitalDecision:
        cluster_mult = calculate_cluster_risk_multiplier(
            cluster_id=intent.cluster_id,
            active_cluster_counts=cluster_counts,
            max_strategies_per_cluster=self._max_strategies_per_cluster,
        )
        return PortfolioCapitalDecision(
            thesis_id=intent.thesis_id,
            symbol=intent.symbol,
            family=intent.family,
            overlap_group_id=intent.overlap_group_id,
            requested_notional=intent.requested_notional,
            allocated_notional=0.0,
            risk_multiplier=risk_multiplier,
            cluster_multiplier=cluster_mult,
            correlation_adjustment=1.0,
            edge_multiplier=0.0 if any(reason.startswith("edge:") for reason in reasons) else 1.0,
            incubation_state=intent.incubation_state,
            admission=admission or AdmissionResult(False, "; ".join(reasons)),
            decision_status="blocked",
            priority_score=self._priority_score(intent),
            reasons=tuple(reasons),
        )
