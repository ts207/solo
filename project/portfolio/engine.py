from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Set, Tuple

from project.portfolio.admission_policy import AdmissionResult, PortfolioAdmissionPolicy
from project.portfolio.risk_budget import (
    calculate_cluster_risk_multiplier,
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
            f"reasons={self.reasons}"
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
    ) -> None:
        self._admission = PortfolioAdmissionPolicy(family_budgets=family_budgets or {})
        self._symbol_caps: Dict[str, float] = dict(symbol_caps or {})
        self._max_gross_leverage = max_gross_leverage
        self._target_vol = target_vol
        self._current_vol = current_vol
        self._gross_exposure = gross_exposure
        self._correlation_limit = correlation_limit
        self._max_strategies_per_cluster = max_strategies_per_cluster

    def decide(
        self,
        intents: List[ThesisIntent],
        *,
        active_overlap_groups: Set[str] | None = None,
        family_exposures: Dict[str, float] | None = None,
        bucket_exposures: Dict[str, float] | None = None,
        active_cluster_counts: Dict[int, int] | None = None,
        symbol_exposures: Dict[str, float] | None = None,
    ) -> List[PortfolioCapitalDecision]:
        """Produce one PortfolioCapitalDecision per intent, in priority order.

        Intents are processed in descending support_score order. Overlap group
        state is updated incrementally so each decision is made against the
        current committed allocation state.
        """
        active_groups: Set[str] = set(active_overlap_groups or set())
        fam_exp: Dict[str, float] = dict(family_exposures or {})
        bucket_exp: Dict[str, float] = dict(bucket_exposures or {})
        cluster_counts: Dict[int, int] = dict(active_cluster_counts or {})
        sym_exp: Dict[str, float] = dict(symbol_exposures or {})

        portfolio_risk_mult = calculate_portfolio_risk_multiplier(
            gross_exposure=self._gross_exposure,
            max_gross_leverage=self._max_gross_leverage,
            target_vol=self._target_vol,
            current_vol=self._current_vol,
        )

        sorted_intents = sorted(intents, key=lambda i: i.support_score, reverse=True)
        decisions: List[PortfolioCapitalDecision] = []

        for intent in sorted_intents:
            reasons: List[str] = []

            # --- incubation gate ---
            if intent.incubation_state == "incubating":
                reasons.append("incubating:paper_only")
                decisions.append(self._blocked(intent, reasons, portfolio_risk_mult, cluster_counts))
                continue

            # --- overlap group gate ---
            admission = self._admission.is_thesis_admissible(
                intent.thesis_id, intent.overlap_group_id, active_groups
            )
            if not admission.admissible:
                reasons.append(f"overlap:{admission.reason}")
                decisions.append(self._blocked(intent, reasons, portfolio_risk_mult, cluster_counts, admission=admission))
                continue

            # --- family budget gate ---
            fam_admission = self._admission.is_family_admissible(intent.family, fam_exp)
            if not fam_admission.admissible:
                reasons.append(f"family:{fam_admission.reason}")
                decisions.append(self._blocked(intent, reasons, portfolio_risk_mult, cluster_counts, admission=fam_admission))
                continue

            # --- symbol cap gate ---
            sym_cap = self._symbol_caps.get(intent.symbol, 0.0)
            if sym_cap > 0.0 and sym_exp.get(intent.symbol, 0.0) >= sym_cap:
                reasons.append(f"symbol_cap_exhausted:{intent.symbol}")
                decisions.append(self._blocked(intent, reasons, portfolio_risk_mult, cluster_counts))
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
            combined_mult = portfolio_risk_mult * cluster_mult * corr_adj * intent.evidence_multiplier
            allocated = max(0.0, intent.requested_notional * combined_mult)

            reasons.append(f"risk_mult={combined_mult:.3f}")
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
                incubation_state=intent.incubation_state,
                admission=admission,
                reasons=tuple(reasons),
            )
            decisions.append(decision)

            # commit allocation state for subsequent intents
            if intent.overlap_group_id:
                active_groups.add(intent.overlap_group_id)
            fam_exp[intent.family] = fam_exp.get(intent.family, 0.0) + allocated
            bucket_exp[intent.asset_bucket] = bucket_exp.get(intent.asset_bucket, 0.0) + allocated
            sym_exp[intent.symbol] = sym_exp.get(intent.symbol, 0.0) + allocated
            cluster_counts[intent.cluster_id] = cluster_counts.get(intent.cluster_id, 0) + 1

        return decisions

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
            incubation_state=intent.incubation_state,
            admission=admission or AdmissionResult(False, "; ".join(reasons)),
            reasons=tuple(reasons),
        )
