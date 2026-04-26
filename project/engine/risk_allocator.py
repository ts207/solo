from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

import numpy as np
import pandas as pd

from project.core.constants import BARS_PER_YEAR_BY_TIMEFRAME
from project.engine.risk_allocator_support import (
    _clamp_positions_py,
    _equity_curve_from_pnl,
)
from project.portfolio import AllocationSpec
from project.portfolio.marginal_risk import estimate_marginal_risk, marginal_risk_multiplier
from project.portfolio.risk_budget import (
    calculate_edge_risk_multiplier,
    calculate_execution_quality_multiplier,
)

ALLOCATION_CONTRACT_SCHEMA_VERSION = "allocation_contract_v1"
ALLOCATION_DIAGNOSTICS_SCHEMA_VERSION = "allocation_diagnostics_v1"

try:
    from numba import njit  # type: ignore

    _clamp_positions = njit(cache=True)(_clamp_positions_py)
except Exception:
    _clamp_positions = _clamp_positions_py

_LOG = logging.getLogger(__name__)


def _drawdown_from_pnl(pnl: pd.Series, *, pnl_mode: Literal["dollar", "return"]) -> pd.Series:
    equity = _equity_curve_from_pnl(pnl, pnl_mode=pnl_mode)
    if equity.empty:
        return pd.Series(dtype=float)
    equity_path = pd.concat(
        [pd.Series([1.0], dtype=float), equity.reset_index(drop=True)],
        ignore_index=True,
    )
    peak = equity_path.cummax().replace(0.0, np.nan)
    drawdown_path = ((peak - equity_path) / peak).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return pd.Series(drawdown_path.iloc[1:].to_numpy(dtype=float), index=pnl.index)


@dataclass(frozen=True)
class RiskLimits:
    max_portfolio_gross: float = 1.0
    max_symbol_gross: float = 1.0
    max_strategy_gross: float = 1.0
    # Permit a full 1x short-to-long (or long-to-short) reversal by default.
    max_new_exposure_per_bar: float = 2.0
    target_annual_vol: float | None = None
    max_drawdown_limit: float | None = None
    max_correlated_gross: float | None = None
    max_pairwise_correlation: float | None = None
    # NEW: stress-conditional correlation limit — tighter limit applied when
    # regime_series contains a value in stressed_regime_values
    stressed_max_pairwise_correlation: float | None = None
    stressed_regime_values: frozenset[str] = frozenset(
        {
            # Canonical labels
            "stress",
            "crisis",
            "high_vol",
            # Uppercase registry variants
            "STRESS",
            "CRISIS",
            "HIGH_VOL",
            # Additional regime-registry naming conventions
            "SHOCK",
            "HIGH_VOL_REGIME",
            "high_vol_shock",
            "vol_shock",
            "crisis_regime",
            "CRISIS_REGIME",
        }
    )
    portfolio_max_drawdown: float | None = None
    symbol_max_exposure: float | None = None
    portfolio_max_exposure: float | None = None
    enable_correlation_allocation: bool = False
    pnl_mode: Literal["dollar", "return"] = "dollar"

    # Safety: don't increase leverage by more than 1x based on vol alone unless opted in.
    allow_lever_up: bool = False

    # Vol estimator configuration
    vol_estimator_mode: str = "rolling"  # "rolling" | "ewma"
    vol_window_bars: int = 5760  # rolling window (bars); only used when mode="rolling"
    vol_ewma_halflife_bars: int = 1440  # EWMA halflife (bars); only used when mode="ewma"
    bars_per_year: float = float(BARS_PER_YEAR_BY_TIMEFRAME["5m"])

    def __post_init__(self) -> None:
        if self.vol_estimator_mode not in ("rolling", "ewma"):
            raise ValueError(
                f"vol_estimator_mode must be 'rolling' or 'ewma', got {self.vol_estimator_mode!r}"
            )


@dataclass(frozen=True)
class AllocationPolicy:
    mode: str = "deterministic_optimizer"
    deterministic: bool = True
    turnover_penalty: float = 0.0
    strategy_risk_budgets: dict[str, float] = field(default_factory=dict)
    family_risk_budgets: dict[str, float] = field(default_factory=dict)
    strategy_family_map: dict[str, str] = field(default_factory=dict)
    strategy_thesis_map: dict[str, str] = field(default_factory=dict)
    thesis_overlap_group_map: dict[str, str] = field(default_factory=dict)
    overlap_group_risk_budgets: dict[str, float] = field(default_factory=dict)
    thesis_evidence_multipliers: dict[str, float] = field(default_factory=dict)
    thesis_execution_quality_multipliers: dict[str, float] = field(default_factory=dict)
    # NEW: Parity with live runtime
    overlap_mode: str = "budgeted"  # "budgeted" | "exclusive"
    thesis_ranking_data: dict[str, dict[str, float]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.mode not in {"heuristic", "deterministic_optimizer"}:
            raise ValueError(
                "allocator mode must be 'heuristic' or 'deterministic_optimizer', "
                f"got {self.mode!r}"
            )
        if self.overlap_mode not in {"budgeted", "exclusive"}:
            raise ValueError(
                f"overlap_mode must be 'budgeted' or 'exclusive', got {self.overlap_mode!r}"
            )
        if self.turnover_penalty < 0.0:
            raise ValueError("turnover_penalty must be non-negative")


@dataclass(frozen=True)
class AllocationContract:
    limits: RiskLimits
    policy: AllocationPolicy = field(default_factory=AllocationPolicy)
    schema_version: str = ALLOCATION_CONTRACT_SCHEMA_VERSION
    diagnostics_schema_version: str = ALLOCATION_DIAGNOSTICS_SCHEMA_VERSION

    def to_manifest_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "diagnostics_schema_version": self.diagnostics_schema_version,
            "policy": {
                "mode": self.policy.mode,
                "deterministic": self.policy.deterministic,
                "turnover_penalty": float(self.policy.turnover_penalty),
                "strategy_risk_budgets": {
                    str(key): float(value)
                    for key, value in sorted(self.policy.strategy_risk_budgets.items())
                },
                "family_risk_budgets": {
                    str(key): float(value)
                    for key, value in sorted(self.policy.family_risk_budgets.items())
                },
                "strategy_family_map": {
                    str(key): str(value)
                    for key, value in sorted(self.policy.strategy_family_map.items())
                },
                "strategy_thesis_map": {
                    str(key): str(value)
                    for key, value in sorted(self.policy.strategy_thesis_map.items())
                },
                "thesis_overlap_group_map": {
                    str(key): str(value)
                    for key, value in sorted(self.policy.thesis_overlap_group_map.items())
                },
                "overlap_group_risk_budgets": {
                    str(key): float(value)
                    for key, value in sorted(self.policy.overlap_group_risk_budgets.items())
                },
                "overlap_mode": self.policy.overlap_mode,
                "thesis_ranking_data": self.policy.thesis_ranking_data,
                "thesis_evidence_multipliers": {
                    str(key): float(value)
                    for key, value in sorted(self.policy.thesis_evidence_multipliers.items())
                },
                "thesis_execution_quality_multipliers": {
                    str(key): float(value)
                    for key, value in sorted(
                        self.policy.thesis_execution_quality_multipliers.items()
                    )
                },
            },
            "limits": {
                field_name: (
                    sorted(getattr(self.limits, field_name))
                    if isinstance(getattr(self.limits, field_name), (set, frozenset))
                    else getattr(self.limits, field_name)
                )
                for field_name in self.limits.__dataclass_fields__
            },
        }


@dataclass(frozen=True)
class AllocationDetails:
    allocated_positions_by_strategy: dict[str, pd.Series]
    scale_by_strategy: dict[str, pd.Series]
    diagnostics: pd.DataFrame
    summary: dict[str, object]
    contract: AllocationContract
    policy_weights: dict[str, float]


def _as_float_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)


def _coerce_budget_mapping(raw: object) -> dict[str, float]:
    if raw is None:
        return {}
    parsed = raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        parsed = json.loads(text)
    if not isinstance(parsed, Mapping):
        raise ValueError("risk budget mappings must be a mapping or JSON object string")
    out: dict[str, float] = {}
    for key, value in parsed.items():
        numeric = float(value)
        if numeric < 0.0:
            raise ValueError(f"risk budget for {key!r} must be non-negative")
        out[str(key)] = numeric
    return out


def _coerce_string_mapping(raw: object) -> dict[str, str]:
    if raw is None:
        return {}
    parsed = raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        parsed = json.loads(text)
    if not isinstance(parsed, Mapping):
        raise ValueError("string mappings must be a mapping or JSON object string")
    return {str(key): str(value).strip() for key, value in parsed.items() if str(value).strip()}


def _optional_float(raw: object) -> float | None:
    if raw is None:
        return None
    return float(raw)


def _required_float(params: Mapping[str, object], key: str) -> float:
    if key not in params or params.get(key) is None:
        raise ValueError(f"build_allocation_contract requires explicit {key}")
    return float(params[key])


def _lagged_for_vol_estimation(series: pd.Series, *, limits: RiskLimits) -> pd.Series:
    lag_bars = (
        max(1, int(limits.vol_window_bars // 2))
        if limits.vol_estimator_mode == "rolling"
        else max(1, int(limits.vol_ewma_halflife_bars // 2))
    )
    return _as_float_series(series).shift(lag_bars)


def build_allocation_contract(params: Mapping[str, object]) -> AllocationContract:
    raw_allocation_spec = params.get("allocation_spec")
    if raw_allocation_spec is not None:
        allocation_spec = (
            raw_allocation_spec
            if isinstance(raw_allocation_spec, AllocationSpec)
            else AllocationSpec.model_validate(dict(raw_allocation_spec))
        )
        params = {
            **allocation_spec.to_allocator_params(),
            **{k: v for k, v in params.items() if k != "allocation_spec"},
        }
    limits = RiskLimits(
        portfolio_max_exposure=float(params.get("portfolio_max_exposure", 10.0)),
        max_portfolio_gross=float(params.get("max_portfolio_gross", 1.0)),
        max_strategy_gross=float(params.get("max_strategy_gross", 1.0)),
        max_symbol_gross=float(params.get("max_symbol_gross", 1.0)),
        max_new_exposure_per_bar=float(params.get("max_new_exposure_per_bar", 2.0)),
        target_annual_vol=_optional_float(params.get("target_annual_volatility")),
        max_pairwise_correlation=_optional_float(params.get("max_pairwise_correlation")),
        max_drawdown_limit=_optional_float(params.get("drawdown_limit")),
        portfolio_max_drawdown=_optional_float(params.get("portfolio_max_drawdown")),
        symbol_max_exposure=_optional_float(params.get("max_symbol_exposure")),
        enable_correlation_allocation=bool(params.get("enable_correlation_allocation", False)),
        pnl_mode=params.get("pnl_mode", "dollar"),  # type: ignore
        allow_lever_up=bool(params.get("allow_lever_up", False)),
    )
    policy = AllocationPolicy(
        mode=str(params.get("allocator_mode", "heuristic")).strip().lower(),
        deterministic=bool(params.get("allocator_deterministic", True)),
        turnover_penalty=float(params.get("allocator_turnover_penalty", 0.0)),
        strategy_risk_budgets=_coerce_budget_mapping(params.get("strategy_risk_budgets")),
        family_risk_budgets=_coerce_budget_mapping(params.get("family_risk_budgets")),
        strategy_family_map=_coerce_string_mapping(params.get("strategy_family_map")),
        strategy_thesis_map=_coerce_string_mapping(params.get("strategy_thesis_map")),
        thesis_overlap_group_map=_coerce_string_mapping(params.get("thesis_overlap_group_map")),
        overlap_group_risk_budgets=_coerce_budget_mapping(params.get("overlap_group_risk_budgets")),
        thesis_evidence_multipliers=_coerce_budget_mapping(
            params.get("thesis_evidence_multipliers")
        ),
        thesis_execution_quality_multipliers=_coerce_budget_mapping(
            params.get("thesis_execution_quality_multipliers")
        ),
    )
    return AllocationContract(limits=limits, policy=policy)


def _normalize_policy_weights(scores: pd.Series) -> pd.Series:
    positive = pd.to_numeric(scores, errors="coerce").fillna(0.0).clip(lower=0.0)
    total = float(positive.sum())
    if total <= 0.0:
        if len(positive.index) == 0:
            return positive.astype(float)
        return pd.Series(
            1.0 / float(len(positive.index)),
            index=positive.index,
            dtype=float,
        )
    return (positive / total).astype(float)


def _resolve_policy_weights(
    requested: dict[str, pd.Series],
    ordered: list[str],
    contract: AllocationContract,
) -> dict[str, float]:
    if not requested:
        return {}
    if contract.policy.mode == "heuristic":
        return {key: 1.0 for key in ordered}
    if not contract.policy.strategy_risk_budgets and float(contract.policy.turnover_penalty) <= 0.0:
        return {key: 1.0 for key in ordered}

    requested_frame = pd.DataFrame(requested).fillna(0.0)
    gross_by_strategy = requested_frame.abs().sum(axis=0).reindex(ordered).fillna(0.0)
    turnover_by_strategy = requested_frame.diff().abs().sum(axis=0).reindex(ordered).fillna(0.0)
    scores = pd.Series(index=ordered, dtype=float)
    for key in ordered:
        budget = float(contract.policy.strategy_risk_budgets.get(key, 1.0))
        gross = float(gross_by_strategy.get(key, 0.0))
        turnover = float(turnover_by_strategy.get(key, 0.0))
        denom = max(gross + (float(contract.policy.turnover_penalty) * turnover), 1e-12)
        scores.loc[key] = 0.0 if budget <= 0.0 else budget / denom
    weights = _normalize_policy_weights(scores)
    return {str(key): float(weights.loc[key]) for key in ordered}


def _apply_thesis_evidence_scaling(
    requested: dict[str, pd.Series],
    *,
    ordered: list[str],
    contract: AllocationContract,
) -> None:
    for key in ordered:
        thesis_id = str(contract.policy.strategy_thesis_map.get(key, "")).strip()
        if not thesis_id:
            continue
        multiplier = float(contract.policy.thesis_evidence_multipliers.get(thesis_id, 1.0))
        if abs(multiplier - 1.0) <= 1e-12:
            continue
        requested[key] = requested[key] * max(0.0, multiplier)


def _apply_thesis_optimizer_scaling(
    requested: dict[str, pd.Series],
    *,
    ordered: list[str],
    contract: AllocationContract,
) -> dict[str, float]:
    multipliers: dict[str, float] = {}
    for key in ordered:
        thesis_id = str(contract.policy.strategy_thesis_map.get(key, "")).strip()
        if not thesis_id:
            continue
        rank_info = contract.policy.thesis_ranking_data.get(thesis_id, {})
        if not rank_info and thesis_id not in contract.policy.thesis_execution_quality_multipliers:
            continue
        edge_multiplier = 1.0
        if "expected_net_edge_bps" in rank_info:
            edge_multiplier = calculate_edge_risk_multiplier(
                expected_net_edge_bps=float(rank_info.get("expected_net_edge_bps", 0.0)),
                expected_downside_bps=float(rank_info.get("expected_downside_bps", 100.0)),
                fill_probability=float(rank_info.get("fill_probability", 1.0)),
                edge_confidence=float(rank_info.get("edge_confidence", 1.0)),
            )
        risk_multiplier = marginal_risk_multiplier(
            estimate_marginal_risk(
                downside_bps=rank_info.get("expected_downside_bps"),
                marginal_volatility=rank_info.get("marginal_volatility"),
                marginal_drawdown_contribution=rank_info.get("marginal_drawdown_contribution"),
            )
        )
        explicit_quality = contract.policy.thesis_execution_quality_multipliers.get(
            thesis_id,
            rank_info.get("execution_quality"),
        )
        execution_multiplier = calculate_execution_quality_multiplier(
            explicit_quality=None if explicit_quality is None else float(explicit_quality)
        )
        multiplier = edge_multiplier * risk_multiplier * execution_multiplier
        requested[key] = requested[key] * max(0.0, multiplier)
        multipliers[thesis_id] = float(multiplier)
    return multipliers


def _apply_family_budget_caps(
    allocated: dict[str, pd.Series],
    *,
    ordered: list[str],
    aligned_index: pd.Index,
    contract: AllocationContract,
    flag: callable,
) -> dict[str, int]:
    family_budget_hits: dict[str, int] = {}
    family_members: dict[str, list[str]] = {}
    for key in ordered:
        family = str(contract.policy.strategy_family_map.get(key, key)).strip() or key
        family_members.setdefault(family, []).append(key)

    for family, members in family_members.items():
        budget = contract.policy.family_risk_budgets.get(family)
        if budget is None:
            continue
        family_frame = pd.DataFrame(
            {member: allocated[member] for member in members}, index=aligned_index
        )
        family_gross = family_frame.abs().sum(axis=1)
        safe_family_gross = family_gross.replace(0.0, np.nan)
        family_ratio = (float(max(0.0, budget)) / safe_family_gross).where(
            family_gross > float(budget), 1.0
        )
        family_ratio = (
            family_ratio.replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
        )
        family_mask = family_ratio < 0.999999
        if bool(family_mask.any()):
            family_budget_hits[family] = int(family_mask.sum())
            flag(
                f"family_risk_budget:{family}",
                family_mask,
            )
            flag("family_risk_budget", family_mask)
            for member in members:
                allocated[member] = allocated[member] * family_ratio
    return family_budget_hits


def _apply_thesis_overlap_budget_caps(
    allocated: dict[str, pd.Series],
    *,
    ordered: list[str],
    aligned_index: pd.Index,
    contract: AllocationContract,
    flag: callable,
) -> dict[str, int]:
    overlap_hits: dict[str, int] = {}
    members_by_group: dict[str, list[str]] = {}
    strategy_to_thesis: dict[str, str] = {}

    for key in ordered:
        thesis_id = str(contract.policy.strategy_thesis_map.get(key, "")).strip()
        if not thesis_id:
            continue
        strategy_to_thesis[key] = thesis_id
        group = str(contract.policy.thesis_overlap_group_map.get(thesis_id, "")).strip()
        if not group:
            continue
        members_by_group.setdefault(group, []).append(key)

    if contract.policy.overlap_mode == "exclusive":
        # Parity with live: pick exactly one winner per group per timestamp
        from project.portfolio.admission_policy import PortfolioAdmissionPolicy

        policy = PortfolioAdmissionPolicy()

        for group, members in members_by_group.items():
            group_frame = pd.DataFrame({m: allocated[m] for m in members}, index=aligned_index)
            # Find active members at each timestamp
            active_mask = group_frame.abs() > 1e-12

            # If multiple members are active, we must pick one based on policy ranking
            multi_active = active_mask.sum(axis=1) > 1
            if not multi_active.any():
                continue

            # For each timestamp with a conflict, pick the winner
            for ts in aligned_index[multi_active]:
                candidates = []
                for m in members:
                    if abs(allocated[m].loc[ts]) <= 1e-12:
                        continue
                    thesis_id = strategy_to_thesis[m]
                    rank_info = contract.policy.thesis_ranking_data.get(thesis_id, {})
                    candidates.append(
                        {
                            "thesis_id": thesis_id,
                            "strategy_key": m,
                            "support_score": float(rank_info.get("support_score", 0.0)),
                            "contradiction_penalty": float(
                                rank_info.get("contradiction_penalty", 0.0)
                            ),
                            "sample_size": int(rank_info.get("sample_size", 0)),
                            "overlap_group_id": group,
                        }
                    )

                # Policy selects the winners (in our case, just one winner for the group)
                winners = policy.resolve_overlap_winners(candidates, active_groups=set())
                winner_keys = {w["strategy_key"] for w in winners}

                # Suppress losers
                for m in members:
                    if m not in winner_keys:
                        allocated[m].loc[ts] = 0.0

            overlap_hits[group] = int(multi_active.sum())
            flag(f"overlap_exclusive_suppression:{group}", multi_active)
            flag("overlap_exclusive_suppression", multi_active)

        return overlap_hits

    # Original "budgeted" mode (scaling)
    for group, members in members_by_group.items():
        budget = contract.policy.overlap_group_risk_budgets.get(group)
        if budget is None:
            continue
        group_frame = pd.DataFrame(
            {member: allocated[member] for member in members}, index=aligned_index
        )
        group_gross = group_frame.abs().sum(axis=1)
        safe_group_gross = group_gross.replace(0.0, np.nan)
        group_ratio = (float(max(0.0, budget)) / safe_group_gross).where(
            group_gross > float(budget), 1.0
        )
        group_ratio = (
            group_ratio.replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
        )
        group_mask = group_ratio < 0.999999
        if bool(group_mask.any()):
            overlap_hits[group] = int(group_mask.sum())
            flag(f"thesis_overlap_budget:{group}", group_mask)
            flag("thesis_overlap_budget", group_mask)
            for member in members:
                allocated[member] = allocated[member] * group_ratio
    return overlap_hits


def allocate_position_scales(
    raw_positions_by_strategy: dict[str, pd.Series],
    requested_scale_by_strategy: dict[str, pd.Series],
    limits: RiskLimits,
    contract: AllocationContract | None = None,
    portfolio_pnl_series: pd.Series | None = None,
    regime_series: pd.Series | None = None,
    regime_scale_map: dict[str, float] | None = None,
) -> tuple[dict[str, pd.Series], dict[str, object]]:
    details = allocate_position_details(
        raw_positions_by_strategy,
        requested_scale_by_strategy,
        limits,
        contract=contract,
        portfolio_pnl_series=portfolio_pnl_series,
        regime_series=regime_series,
        regime_scale_map=regime_scale_map,
    )
    return details.scale_by_strategy, details.summary


def allocate_position_details(
    raw_positions_by_strategy: dict[str, pd.Series],
    requested_scale_by_strategy: dict[str, pd.Series],
    limits: RiskLimits,
    contract: AllocationContract | None = None,
    portfolio_pnl_series: pd.Series | None = None,
    regime_series: pd.Series | None = None,
    regime_scale_map: dict[str, float] | None = None,
    strategy_returns: dict[str, pd.Series] | None = None,
) -> AllocationDetails:
    resolved_contract = contract or AllocationContract(limits=limits)
    if not raw_positions_by_strategy:
        empty_diag = pd.DataFrame(
            columns=[
                "requested_gross",
                "allocated_gross",
                "clip_fraction",
                "clip_reason",
                "allocator_mode",
            ]
        )
        return AllocationDetails(
            {},
            {},
            empty_diag,
            {
                "requested_gross": 0.0,
                "allocated_gross": 0.0,
                "clipped_fraction": 0.0,
                "allocator_mode": resolved_contract.policy.mode,
            },
            resolved_contract,
            {},
        )

    ordered = sorted(raw_positions_by_strategy.keys())
    aligned_index = None
    for key in ordered:
        idx = raw_positions_by_strategy[key].index
        aligned_index = idx if aligned_index is None else aligned_index.union(idx)
    if aligned_index is None:
        raise ValueError("aligned_index must not be None")

    requested: dict[str, pd.Series] = {}
    for key in ordered:
        pos = _as_float_series(raw_positions_by_strategy[key]).reindex(aligned_index).fillna(0.0)
        scale = (
            _as_float_series(
                requested_scale_by_strategy.get(key, pd.Series(1.0, index=aligned_index))
            )
            .reindex(aligned_index)
            .fillna(1.0)
        )
        requested[key] = (pos * scale.clip(lower=0.0)).astype(float)
    # L-3: Policy weights are applied to pre-cap exposures ('requested').
    # Subsequent strategy, family, and portfolio caps are applied to 'allocated'
    # which may further scale back these positions. The final risk budget split
    # may deviate from the policy if some strategies hit their caps.
    policy_weights = _resolve_policy_weights(requested, ordered, resolved_contract)
    if resolved_contract.policy.mode == "deterministic_optimizer" and policy_weights:
        for key in ordered:
            requested[key] = requested[key] * float(policy_weights.get(key, 0.0))
    _apply_thesis_evidence_scaling(requested, ordered=ordered, contract=resolved_contract)
    optimizer_thesis_multipliers = _apply_thesis_optimizer_scaling(
        requested,
        ordered=ordered,
        contract=resolved_contract,
    )

    # NEW: Apply exclusive overlap suppression before correlation/scaling
    # so winners get their intended full weight.
    reason_flags: dict[str, pd.Series] = {}
    aligned_index_non_null = aligned_index if aligned_index is not None else pd.Index([])

    def _flag(name: str, mask: pd.Series) -> None:
        nonlocal reason_flags
        reason_flags[name] = reason_flags.get(
            name, pd.Series(False, index=aligned_index_non_null)
        ) | mask.reindex(aligned_index_non_null).fillna(False)

    if resolved_contract.policy.overlap_mode == "exclusive":
        _apply_thesis_overlap_budget_caps(
            requested,
            ordered=ordered,
            aligned_index=aligned_index_non_null,
            contract=resolved_contract,
            flag=_flag,
        )

    scale_by_strategy: dict[str, pd.Series] = {}
    requested_gross = (
        pd.DataFrame(requested).abs().sum(axis=1)
        if requested
        else pd.Series(0.0, index=aligned_index)
    )

    # ----- Original allocator logic (preserved) -----
    if limits.enable_correlation_allocation and len(requested) > 1:
        try:
            if not strategy_returns:
                raise ValueError(
                    "correlation allocation requires strategy_returns; "
                    "position-change covariance is disabled"
                )
            df_req = pd.DataFrame(requested).fillna(0.0)
            df_ret = pd.DataFrame(strategy_returns).reindex(df_req.index).fillna(0.0)
            cov = df_ret.cov()

            if (cov.isnull().any().any()) or len(cov) != len(requested):
                raise ValueError("invalid covariance for allocation")

            # Audit 3.2: Add regularization (Shrinkage) to ensure numerical stability
            # Simple constant shrinkage towards identity to prevent singular matrix
            cov_vals = cov.values
            n_assets = len(cov_vals)
            shrinkage = 0.1
            shrunk_cov = (1 - shrinkage) * cov_vals + shrinkage * np.eye(n_assets) * np.trace(
                cov_vals
            ) / n_assets

            inv_cov = np.linalg.inv(shrunk_cov)
            ones = np.ones(len(inv_cov))
            weights = inv_cov @ ones
            weights = np.clip(weights, 0.0, None)
            total = weights.sum()
            weights = weights / total if total > 0 else np.full_like(weights, 1.0 / len(weights))
            for key, w in zip(ordered, weights):
                requested[key] = requested[key] * float(w)
        except Exception:
            _LOG.warning(
                "Correlation allocation failed, falling back to equal-weight", exc_info=True
            )

    allocated = {key: s.copy() for key, s in requested.items()}

    for key in ordered:
        max_s = float(max(0.0, limits.max_strategy_gross))
        gross = allocated[key].abs()
        safe_gross = gross.replace(0.0, np.nan)
        ratio_series = (max_s / safe_gross).where(gross > max_s, 1.0)
        ratio_series = (
            ratio_series.replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
        )
        _flag("max_strategy_gross", ratio_series < 0.999999)
        allocated[key] = allocated[key] * ratio_series

    symbol_cap = float(max(0.0, limits.max_symbol_gross))
    symbol_gross = (
        pd.DataFrame(allocated).abs().sum(axis=1)
        if allocated
        else pd.Series(0.0, index=aligned_index)
    )
    safe_symbol_gross = symbol_gross.replace(0.0, np.nan)
    symbol_ratio_series = (symbol_cap / safe_symbol_gross).where(symbol_gross > symbol_cap, 1.0)
    symbol_ratio_series = (
        symbol_ratio_series.replace([np.inf, -np.inf], np.nan)
        .fillna(1.0)
        .clip(lower=0.0, upper=1.0)
    )
    _flag("max_symbol_gross", symbol_ratio_series < 0.999999)
    for key in ordered:
        allocated[key] = allocated[key] * symbol_ratio_series

    family_budget_hits = _apply_family_budget_caps(
        allocated,
        ordered=ordered,
        aligned_index=aligned_index,
        contract=resolved_contract,
        flag=_flag,
    )
    # Only apply budgeted mode here if not already handled in exclusive mode
    overlap_group_budget_hits = {}
    if resolved_contract.policy.overlap_mode != "exclusive":
        overlap_group_budget_hits = _apply_thesis_overlap_budget_caps(
            allocated,
            ordered=ordered,
            aligned_index=aligned_index,
            contract=resolved_contract,
            flag=_flag,
        )
    else:
        # In exclusive mode, we already flagged but didn't return hits count
        # We can re-check requested vs allocated if needed, but for stats:
        overlap_group_budget_hits = {
            group: 1
            for group in set(resolved_contract.policy.thesis_overlap_group_map.values())
            if reason_flags.get(f"overlap_exclusive_suppression:{group}") is not None
            and reason_flags[f"overlap_exclusive_suppression:{group}"].any()
        }

    if limits.max_correlated_gross is not None:
        corr_cap = float(max(0.0, limits.max_correlated_gross))
        df_alloc = pd.DataFrame(allocated) if allocated else pd.DataFrame(index=aligned_index)
        net_direction = (
            df_alloc.sum(axis=1) if not df_alloc.empty else pd.Series(0.0, index=aligned_index)
        )
        same_dir_gross = (
            df_alloc.abs().sum(axis=1)
            if not df_alloc.empty
            else pd.Series(0.0, index=aligned_index)
        )
        fully_concordant = (net_direction.abs() - same_dir_gross).abs() < 1e-9
        needs_clip = fully_concordant & (same_dir_gross > corr_cap)
        safe_gross = same_dir_gross.replace(0.0, np.nan)
        corr_ratio_series = (corr_cap / safe_gross).where(needs_clip, 1.0)
        corr_ratio_series = (
            corr_ratio_series.replace([np.inf, -np.inf], np.nan)
            .fillna(1.0)
            .clip(lower=0.0, upper=1.0)
        )
        _flag("max_correlated_gross", corr_ratio_series < 0.999999)
        for key in ordered:
            allocated[key] = allocated[key] * corr_ratio_series

    if limits.max_pairwise_correlation is not None and len(allocated) > 1:
        try:
            if strategy_returns:
                # Use returns (PnL) for correlation instead of positions
                # We need to scale the raw returns by the allocation scale factor
                # to account for any strategy-level caps applied so far.
                allocated_returns = {}
                for key in ordered:
                    raw_pos = (
                        _as_float_series(raw_positions_by_strategy[key])
                        .reindex(aligned_index)
                        .fillna(0.0)
                    )
                    # Avoid division by zero: where raw_pos is zero, scale is 1.0 (no change)
                    # but return would be zero anyway if pos is zero.
                    # We can use the ratio of allocated/raw_pos as the scale factor.
                    scale_factor = (
                        (allocated[key] / raw_pos.replace(0.0, np.nan))
                        .fillna(1.0)
                        .clip(lower=0.0, upper=1.0)
                    )
                    allocated_returns[key] = (
                        strategy_returns[key].reindex(aligned_index).fillna(0.0) * scale_factor
                    )
                df_alloc = pd.DataFrame(allocated_returns)
            else:
                df_alloc = pd.DataFrame({k: v for k, v in allocated.items()})

            if df_alloc.empty or len(df_alloc) < 2:
                raise ValueError("insufficient history for correlation estimate")

            # Use a shorter rolling window so the clamp reacts to current
            # co-movement instead of anchoring on stale historical episodes.
            corr_window = min(len(df_alloc), 60)

            # Calculate rolling pairwise correlation matrices
            # To avoid huge memory/compute for very long series, we only scale where needed
            # For backtest efficiency, we compute the max rolling correlation bar-by-bar
            # This is still O(N * K^2) but is necessary for time-varying protection
            rolling_corr = (
                df_alloc.rolling(window=corr_window, min_periods=max(20, corr_window // 4))
                .corr()
                .abs()
            )

            # Extract max pairwise correlation at each bar (excluding self-correlation)
            # rolling_corr has a MultiIndex (timestamp, strategy)
            max_corr_series = pd.Series(0.0, index=aligned_index)

            # Iterate through timestamps to find the max off-diagonal correlation
            # We use a vectorized approach by unstacking the second level
            unstacked = rolling_corr.unstack(level=1)
            # Remove identity correlations (strategy vs itself)
            for strategy in ordered:
                if (strategy, strategy) in unstacked.columns:
                    unstacked[(strategy, strategy)] = np.nan

            max_corr_series = unstacked.max(axis=1).fillna(0.0)

            # Determine effective limit — use stressed limit on stressed bars if provided
            if (
                limits.stressed_max_pairwise_correlation is not None
                and regime_series is not None
                and limits.stressed_regime_values
            ):
                regime_aligned = regime_series.reindex(aligned_index).astype(str)
                is_stressed = regime_aligned.isin(limits.stressed_regime_values)

                stressed_limit = float(limits.stressed_max_pairwise_correlation)
                normal_limit = float(limits.max_pairwise_correlation)

                # Bar-by-bar scale factor
                scale_series = pd.Series(1.0, index=aligned_index)

                # Stressed bars
                stressed_mask = (
                    is_stressed & (max_corr_series > stressed_limit) & (max_corr_series > 0)
                )
                scale_series[stressed_mask] = stressed_limit / max_corr_series[stressed_mask]

                # Normal bars
                normal_mask = (
                    (~is_stressed) & (max_corr_series > normal_limit) & (max_corr_series > 0)
                )
                scale_series[normal_mask] = normal_limit / max_corr_series[normal_mask]

                scale_series = scale_series.clip(lower=0.0, upper=1.0)
                _flag("max_pairwise_correlation", scale_series < 0.999999)
                for key in ordered:
                    allocated[key] = allocated[key] * scale_series
            else:
                limit = float(limits.max_pairwise_correlation)
                if limit > 0:
                    scale_series = (
                        (limit / max_corr_series.replace(0.0, np.nan))
                        .fillna(1.0)
                        .clip(lower=0.0, upper=1.0)
                    )
                    _flag("max_pairwise_correlation", scale_series < 0.999999)
                    for key in ordered:
                        allocated[key] = allocated[key] * scale_series
        except Exception:
            _LOG.warning("Rolling correlation scaling failed", exc_info=True)

    if regime_series is not None and regime_scale_map:
        regime_aligned = regime_series.reindex(aligned_index).astype(str)
        regime_scale_vals = (
            regime_aligned.map(regime_scale_map).fillna(1.0).clip(lower=0.0, upper=1.0)
        )
        _flag("regime_scale", regime_scale_vals < 0.999999)
        for key in ordered:
            allocated[key] = allocated[key] * regime_scale_vals

    vol_scale_series = pd.Series(1.0, index=aligned_index)
    if limits.target_annual_vol is not None:
        target_vol = float(limits.target_annual_vol)
        bars_per_year = float(max(1.0, limits.bars_per_year))

        if strategy_returns and len(ordered) > 0:
            # Per-strategy vol scaling before portfolio sum
            per_strategy_vol_scales = {}
            for key in ordered:
                s_pnl = _lagged_for_vol_estimation(
                    strategy_returns[key].reindex(aligned_index).fillna(0.0), limits=limits
                )
                if limits.vol_estimator_mode == "ewma":
                    s_std = s_pnl.ewm(halflife=limits.vol_ewma_halflife_bars, adjust=False).std()
                else:
                    s_std = s_pnl.rolling(
                        window=limits.vol_window_bars,
                        min_periods=min(288, limits.vol_window_bars),
                    ).std()

                s_ann_vol = s_std * np.sqrt(bars_per_year)
                # Scale each strategy to its share of the target portfolio vol.
                # If K strategies were independent, each could use target_vol / sqrt(K).
                # However, the requirement is to use per-strategy information.
                # Let's scale each to the target_vol as if it were the only strategy,
                # then apply a portfolio correction.
                s_scale = (
                    (target_vol / s_ann_vol.replace(0.0, np.nan))
                    .replace([np.inf, -np.inf], np.nan)
                    .fillna(1.0)
                )
                per_strategy_vol_scales[key] = s_scale.clip(
                    lower=0.0, upper=2.0 if limits.allow_lever_up else 1.0
                )

            # Portfolio correction pass: estimate portfolio vol from the post-cap
            # allocation that will actually be traded.
            scaled_pnl_sum = pd.Series(0.0, index=aligned_index)
            for key in ordered:
                pos_weighted_ret = allocated[key] * _lagged_for_vol_estimation(
                    strategy_returns[key].reindex(aligned_index).fillna(0.0), limits=limits
                )
                scaled_pnl_sum += pos_weighted_ret

            if limits.vol_estimator_mode == "ewma":
                p_std = scaled_pnl_sum.ewm(
                    halflife=limits.vol_ewma_halflife_bars, adjust=False
                ).std()
            else:
                p_std = scaled_pnl_sum.rolling(
                    window=limits.vol_window_bars, min_periods=min(288, limits.vol_window_bars)
                ).std()

            p_ann_vol = p_std * np.sqrt(bars_per_year)
            portfolio_correction = (
                (target_vol / p_ann_vol.replace(0.0, np.nan))
                .replace([np.inf, -np.inf], np.nan)
                .fillna(1.0)
            )
            portfolio_correction = portfolio_correction.clip(
                lower=0.0, upper=1.0
            )  # only scale down in correction

            for key in ordered:
                total_s_scale = (
                    (per_strategy_vol_scales[key] * portfolio_correction)
                    .fillna(1.0)
                    .clip(lower=0.0)
                )
                allocated[key] = allocated[key] * total_s_scale
                _flag("target_annual_vol", total_s_scale < 0.999999)

        elif portfolio_pnl_series is not None:
            # Fallback to portfolio-level uniform scaling
            pnl = _lagged_for_vol_estimation(
                portfolio_pnl_series.reindex(aligned_index).fillna(0.0), limits=limits
            )
            if limits.vol_estimator_mode == "ewma":
                roll_std = pnl.ewm(halflife=limits.vol_ewma_halflife_bars, adjust=False).std()
            else:  # "rolling"
                roll_std = pnl.rolling(
                    window=limits.vol_window_bars,
                    min_periods=min(288, limits.vol_window_bars),
                ).std()
            ann_vol = roll_std * np.sqrt(bars_per_year)
            vol_scale = (
                (target_vol / ann_vol.replace(0.0, np.nan))
                .replace([np.inf, -np.inf], np.nan)
                .fillna(1.0)
            )
            # Apply safety cap on vol scaling
            upper_bound = 2.0 if limits.allow_lever_up else 1.0
            vol_scale_series = vol_scale.clip(lower=0.0, upper=upper_bound)
            _flag("target_annual_vol", vol_scale_series < 0.999999)
            for key in ordered:
                allocated[key] = allocated[key] * vol_scale_series

    dd_scale_series = pd.Series(1.0, index=aligned_index)
    if limits.max_drawdown_limit is not None and portfolio_pnl_series is not None:
        pnl = portfolio_pnl_series.reindex(aligned_index).fillna(0.0)
        drawdown = _drawdown_from_pnl(pnl, pnl_mode=limits.pnl_mode)
        dd_factor = (limits.max_drawdown_limit - drawdown) / limits.max_drawdown_limit
        dd_scale_series = dd_factor.clip(lower=0.0, upper=1.0)
        _flag("max_drawdown_limit", dd_scale_series < 0.999999)

    dynamic_overlay_series = dd_scale_series.fillna(1.0)
    for key in ordered:
        allocated[key] = allocated[key] * dynamic_overlay_series

    if limits.portfolio_max_drawdown is not None and portfolio_pnl_series is not None:
        pnl = portfolio_pnl_series.reindex(aligned_index).fillna(0.0)
        drawdown = _drawdown_from_pnl(pnl, pnl_mode=limits.pnl_mode)
        reject_mask = drawdown > limits.portfolio_max_drawdown
        _flag("portfolio_max_drawdown", reject_mask)
        for key in ordered:
            allocated[key] = allocated[key].mask(reject_mask, 0.0)

    if limits.symbol_max_exposure is not None:
        symbol_cap_exp = float(max(0.0, limits.symbol_max_exposure))
        symbol_gross = (
            pd.DataFrame(allocated).abs().sum(axis=1)
            if allocated
            else pd.Series(0.0, index=aligned_index)
        )
        reject_mask = symbol_gross > symbol_cap_exp
        _flag("symbol_max_exposure", reject_mask)
        for key in ordered:
            allocated[key] = allocated[key].mask(reject_mask, 0.0)

    if limits.portfolio_max_exposure is not None:
        portfolio_cap_exp = float(max(0.0, limits.portfolio_max_exposure))
        portfolio_gross_exp = (
            pd.DataFrame(allocated).abs().sum(axis=1)
            if allocated
            else pd.Series(0.0, index=aligned_index)
        )
        reject_mask = portfolio_gross_exp > portfolio_cap_exp
        _flag("portfolio_max_exposure", reject_mask)
        for key in ordered:
            allocated[key] = allocated[key].mask(reject_mask, 0.0)

    portfolio_cap = float(max(0.0, limits.max_portfolio_gross))
    portfolio_gross = (
        pd.DataFrame(allocated).abs().sum(axis=1)
        if allocated
        else pd.Series(0.0, index=aligned_index)
    )
    safe_portfolio_gross = portfolio_gross.replace(0.0, np.nan)
    portfolio_ratio_series = (portfolio_cap / safe_portfolio_gross).where(
        portfolio_gross > portfolio_cap, 1.0
    )
    portfolio_ratio_series = (
        portfolio_ratio_series.replace([np.inf, -np.inf], np.nan)
        .fillna(1.0)
        .clip(lower=0.0, upper=1.0)
    )
    _flag("max_portfolio_gross", portfolio_ratio_series < 0.999999)
    for key in ordered:
        allocated[key] = allocated[key] * portfolio_ratio_series

    max_new = float(max(0.0, limits.max_new_exposure_per_bar))
    for key in ordered:
        raw_alloc = allocated[key].values.astype(float, copy=False)
        clamped = _clamp_positions(raw_alloc, max_new)
        clamped_series = pd.Series(clamped, index=aligned_index)
        _flag("max_new_exposure_per_bar", (clamped_series - allocated[key]).abs() > 1e-12)
        allocated[key] = clamped_series

    allocated_gross = (
        pd.DataFrame(allocated).abs().sum(axis=1)
        if allocated
        else pd.Series(0.0, index=aligned_index)
    )
    if not requested_gross.empty:
        # Final allocation must not exceed the requested gross exposure once caps
        # and overlays have been applied.
        assert bool((allocated_gross <= requested_gross + 1e-9).all())
    req_total = float(requested_gross.sum())
    alloc_total = float(allocated_gross.sum())
    clipped_fraction = (
        0.0 if req_total <= 0 else float(max(0.0, (req_total - alloc_total) / req_total))
    )

    for key in ordered:
        pos = _as_float_series(raw_positions_by_strategy[key]).reindex(aligned_index).fillna(0.0)
        denom = pos.replace(0.0, np.nan).abs()
        scale = (allocated[key].abs() / denom).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        scale_by_strategy[key] = scale.astype(float)

    diagnostics = pd.DataFrame(index=aligned_index)
    diagnostics["requested_gross"] = requested_gross.astype(float)
    diagnostics["allocated_gross"] = allocated_gross.astype(float)
    diagnostics["clip_fraction"] = np.where(
        diagnostics["requested_gross"] > 0,
        (diagnostics["requested_gross"] - diagnostics["allocated_gross"]).clip(lower=0.0)
        / diagnostics["requested_gross"],
        0.0,
    )
    for reason_name, mask in reason_flags.items():
        diagnostics[reason_name] = mask.astype(bool)
    if reason_flags:
        reason_cols = list(reason_flags.keys())
        diagnostics["clip_reason"] = diagnostics[reason_cols].apply(
            lambda row: ",".join(sorted([col for col in reason_cols if bool(row[col])])), axis=1
        )
    else:
        diagnostics["clip_reason"] = ""
    diagnostics["allocator_mode"] = resolved_contract.policy.mode
    diagnostics = diagnostics.reset_index().rename(columns={"index": "timestamp"})

    return AllocationDetails(
        allocated_positions_by_strategy=allocated,
        scale_by_strategy=scale_by_strategy,
        diagnostics=diagnostics,
        summary={
            "requested_gross": req_total,
            "allocated_gross": alloc_total,
            "clipped_fraction": clipped_fraction,
            "allocator_mode": resolved_contract.policy.mode,
            "policy_weights": policy_weights,
            "optimizer_thesis_multipliers": optimizer_thesis_multipliers,
            "family_budget_hits": family_budget_hits,
            "overlap_group_budget_hits": overlap_group_budget_hits,
            "overlap_exclusive_suppression": int(
                reason_flags["overlap_exclusive_suppression"].sum()
                if "overlap_exclusive_suppression" in reason_flags
                else 0
            ),
        },
        contract=resolved_contract,
        policy_weights=policy_weights,
    )
