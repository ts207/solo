"""
Multiplicity Controls: BH/BY FDR adjustments and Family/Cluster logic.
Extracted from pipeline scripts to improve testability and separate concerns.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

import numpy as np
import pandas as pd

from project.core.stats import canonical_bh_group_key
from project.research.gating import bh_adjust
from project.specs.ontology import state_id_to_context_column

log = logging.getLogger(__name__)


def _finite_q_value(value: Any, default: float = 1.0) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric) or not np.isfinite(numeric):
        return float(default)
    return float(np.clip(numeric, 0.0, 1.0))


def _resolve_multiplicity_p_value_column(frame: pd.DataFrame) -> str:
    """Return the operative p-value column for multiplicity controls.

    `p_value_for_fdr` is the canonical input column for BH/BY. In raw evaluator
    output it is just the unadjusted p-value; later stages may replace it with a
    shrunk or otherwise transformed value intended for multiplicity control.
    """
    for candidate in ("p_value_for_fdr", "p_value_raw", "p_value"):
        if candidate in frame.columns:
            return candidate
    raise KeyError("raw_df must contain one of p_value_for_fdr, p_value_raw, or p_value")


def make_family_id(
    symbol: str,
    event_type: str,
    rule: str,
    horizon: str,
    cond_label: str,
    *,
    research_family: Optional[str] = None,
    canonical_family: Optional[str] = None,
    state_id: Optional[str] = None,
) -> str:
    """BH family key based on ontology axes, stratified by symbol."""
    base = canonical_bh_group_key(
        canonical_family=str(research_family or canonical_family or event_type),
        canonical_event_type=str(event_type),
        template_verb=str(rule),
        horizon=str(horizon),
        state_id=(str(state_id).strip() if state_id else None),
        symbol=None,
        include_symbol=False,
        direction_bucket=None,
    )
    return f"{str(symbol).strip().upper()}_{base}"


def resolved_sample_size(joined_event_count: int, symbol_event_count: int) -> int:
    try:
        joined = int(joined_event_count)
        symbol_total = int(symbol_event_count)
    except (TypeError, ValueError):
        return 0
    return max(0, min(joined, symbol_total if symbol_total > 0 else joined))


def resolve_state_context_column(columns: pd.Index, state_id: Optional[str]) -> Optional[str]:
    state = str(state_id or "").strip()
    if not state:
        return None
    by_id = state_id_to_context_column(state)
    for candidate in [by_id, state, state.upper(), state.lower()]:
        if candidate and candidate in columns:
            return str(candidate)
    return None


def simes_p_value(p_vals: pd.Series, n_tests: int | None = None) -> float:
    p = p_vals.dropna().sort_values()
    m = len(p)
    n = n_tests if n_tests is not None else m
    if n == 0 or m == 0:
        return 1.0
    return float((p * n / np.arange(1, m + 1)).min())


def by_adjust(p_values: np.ndarray, n_tests: int | None = None) -> np.ndarray:
    """Benjamini-Yekutieli FDR adjustment."""
    if len(p_values) == 0:
        return p_values
    m = len(p_values)
    n = n_tests if n_tests is not None else m
    idx = np.argsort(p_values)
    sorted_p = np.asarray(p_values, dtype=float)[idx]
    harmonic = float(np.sum(1.0 / np.arange(1, n + 1)))
    adj = np.zeros(m, dtype=float)
    min_p = 1.0
    for i in range(m - 1, -1, -1):
        q = sorted_p[i] * n * harmonic / float(i + 1)
        min_p = min(min_p, q)
        adj[i] = min_p
    rev_idx = np.zeros(m, dtype=int)
    rev_idx[idx] = np.arange(m)
    return np.clip(adj[rev_idx], 0.0, 1.0)


def apply_multiplicity_controls(
    raw_df: pd.DataFrame,
    max_q: float,
    *,
    mode: str = "production",
    min_sample_size: int = 0,
    enable_cluster_adjusted: bool = True,
    cluster_threshold: float = 0.85,
    enable_by_diagnostic: bool = True,
) -> pd.DataFrame:
    """Apply BH correction per-family, then a global BH over family-adjusted q-values."""
    if raw_df.empty:
        return raw_df.copy()
    out = raw_df.copy()

    # Init columns
    for col in [
        "p_value_family",
        "q_value_family",
        "q_value",
        "q_value_by",
        "p_value_cluster",
        "q_value_cluster",
    ]:
        out[col] = np.nan
    for col in [
        "is_discovery_family",
        "is_discovery",
        "is_discovery_by",
        "is_discovery_cluster",
        "multiplicity_pool_eligible",
    ]:
        out[col] = False
    out["family_cluster_id"] = ""
    out["num_tests_family"] = 0
    out["num_tests_campaign"] = 0
    out["num_tests_effective"] = 0

    eligible_mask = pd.Series(True, index=out.index)
    if mode == "research" and min_sample_size > 0:
        eligible_mask = out.get("sample_size", pd.Series(0, index=out.index)) >= min_sample_size
    out.loc[eligible_mask, "multiplicity_pool_eligible"] = True

    eligible = out[eligible_mask].copy()
    if eligible.empty:
        return out

    p_col = _resolve_multiplicity_p_value_column(eligible)

    test_weights = pd.Series(1, index=out.index)
    if "side_policy" in out.columns:
        test_weights[out["side_policy"].astype(str).str.lower() == "both"] = 2

    # 1. Family Simes p-values
    family_simes_list = []
    for fid, group in eligible.groupby("family_id"):
        n_family_tests = test_weights.loc[group.index].sum()
        p_val = simes_p_value(group[p_col], n_tests=int(n_family_tests))
        family_simes_list.append({"family_id": fid, "p_value_family": p_val})

    if family_simes_list:
        family_simes = pd.DataFrame(family_simes_list)
        family_simes["q_value_family"] = bh_adjust(family_simes["p_value_family"].values)
        family_simes["is_discovery_family"] = family_simes["q_value_family"] <= float(max_q)

        # Map family metrics back to out
        for col in ["p_value_family", "q_value_family", "is_discovery_family"]:
            mapping = dict(zip(family_simes["family_id"], family_simes[col]))
            out[col] = out["family_id"].map(mapping)

    # 2. Within-family BH for rows in discovered families
    out["q_value"] = 1.0
    for fid, group in out[out["multiplicity_pool_eligible"]].groupby("family_id"):
        if group["is_discovery_family"].any():
            n_family_tests = int(test_weights.loc[group.index].sum())
            qvals = bh_adjust(group[p_col].fillna(1.0).to_numpy(), n_tests=n_family_tests)
            out.loc[group.index, "q_value"] = qvals

    out["is_discovery"] = out["is_discovery_family"].astype("boolean").fillna(False).astype(
        bool
    ) & (out["q_value"] <= float(max_q))

    # 3. BY Adjustment (Optional Diagnostic)
    if enable_by_diagnostic:
        eligible_idx = out["multiplicity_pool_eligible"]
        p_vals_all = out.loc[eligible_idx, p_col].fillna(1.0).to_numpy()
        n_total_tests = int(test_weights.loc[eligible_idx].sum())
        if len(p_vals_all) > 0:
            q_by = by_adjust(p_vals_all, n_tests=n_total_tests)
            out.loc[eligible_idx, "q_value_by"] = q_by
            out["is_discovery_by"] = out["q_value_by"] <= float(max_q)

    # 4. Cluster Logic
    def _cluster_key(row):
        symbol = str(row.get("symbol", "")).strip().upper()
        event = str(
            row.get("canonical_regime", "")
            or row.get("research_family", "")
            or row.get("canonical_family", "")
            or row.get("event_type", "")
        ).strip()
        horizon = str(row.get("horizon", "")).strip()
        state = str(row.get("state_id", "")).strip()
        return f"{symbol}_{event}_{horizon}_{state}"

    out["family_cluster_id"] = out.apply(_cluster_key, axis=1)

    if enable_cluster_adjusted and not out[out["multiplicity_pool_eligible"]].empty:
        cluster_simes_list = []
        for cid, group in out[out["multiplicity_pool_eligible"]].groupby("family_cluster_id"):
            n_cluster_tests = test_weights.loc[group.index].sum()
            p_val = simes_p_value(group[p_col], n_tests=int(n_cluster_tests))
            cluster_simes_list.append({"family_cluster_id": cid, "p_value_cluster": p_val})

        if cluster_simes_list:
            cluster_simes = pd.DataFrame(cluster_simes_list)
            cluster_simes["q_value_cluster"] = bh_adjust(cluster_simes["p_value_cluster"].values)

            for col in ["p_value_cluster", "q_value_cluster"]:
                mapping = dict(zip(cluster_simes["family_cluster_id"], cluster_simes[col]))
                out[col] = out["family_cluster_id"].map(mapping)

            out["is_discovery_cluster"] = out["q_value_cluster"] <= float(max_q)

    # 5. Metadata
    family_row_counts = out.groupby("family_id").size().astype(int).to_dict()
    family_effective_counts = (
        out.assign(_test_weight=test_weights)
        .groupby("family_id")["_test_weight"]
        .sum()
        .astype(int)
        .to_dict()
    )

    out["num_tests_family"] = out["family_id"].map(family_row_counts).fillna(0).astype(int)
    out["num_tests_effective"] = out["family_id"].map(family_effective_counts).fillna(0).astype(int)

    # Campaign-level search burden should not understate either layer of the
    # hierarchical procedure:
    #   1) outer-family discovery burden (number of tested families), or
    #   2) inner-family effective burden for the largest family (including
    #      side_policy='both' counting as two tests).
    #
    # This preserves the expected hierarchical semantics in which campaign-level
    # BH runs over discovered families, while still surfacing a conservative
    # fallback trial count for DSR when only campaign-level metadata is available.
    n_tested_families = int(out.loc[out["multiplicity_pool_eligible"], "family_id"].nunique())
    max_family_effective = max(family_effective_counts.values(), default=0)
    out["num_tests_campaign"] = max(n_tested_families, int(max_family_effective))

    # Backward compatibility aliases for legacy column names
    out["num_tests_primary_event_id"] = out["num_tests_family"]
    out["num_tests_event_family"] = out["num_tests_family"]

    out["gate_multiplicity"] = out["is_discovery"].astype(bool)
    out["gate_multiplicity_strict"] = out["is_discovery"].astype(bool) & out[
        "is_discovery_by"
    ].astype("boolean").fillna(False).astype(bool)
    return out


def apply_cross_campaign_fdr(
    dataframes: List[pd.DataFrame], max_q: float, *, p_col_candidate: str = "p_value_for_fdr"
) -> pd.DataFrame:
    """
    [DEPRECATED] Use apply_canonical_cross_campaign_multiplicity instead.

    Legacy batch cross-campaign FDR helper. Kept for backward compatibility.
    Prefer apply_canonical_cross_campaign_multiplicity for new code.

    NOTE: This function applies a simple BH across concatenated dataframes.
    For campaign-lineage scope control, use the canonical function.
    """
    if not dataframes:
        return pd.DataFrame()

    combined = pd.concat(dataframes, ignore_index=True)
    if combined.empty:
        return combined

    p_col = _resolve_multiplicity_p_value_column(combined)
    test_weights = pd.Series(1, index=combined.index)
    if "side_policy" in combined.columns:
        test_weights[combined["side_policy"].astype(str).str.lower() == "both"] = 2

    eligible_idx = combined.get("multiplicity_pool_eligible", pd.Series(True, index=combined.index))
    n_total_tests = int(test_weights.loc[eligible_idx].sum())

    combined["q_value_global"] = 1.0
    combined["is_discovery_global"] = False

    if n_total_tests > 0 and not combined[eligible_idx].empty:
        p_vals = combined.loc[eligible_idx, p_col].fillna(1.0).to_numpy()
        q_vals = bh_adjust(p_vals, n_tests=n_total_tests)
        combined.loc[eligible_idx, "q_value_global"] = q_vals
        combined["is_discovery_global"] = combined["q_value_global"] <= float(max_q)

    return combined


def apply_canonical_cross_campaign_multiplicity(
    frame: pd.DataFrame,
    max_q: float,
    *,
    scope_mode: str = "campaign_lineage",
    eligible_col: str = "multiplicity_pool_eligible",
    p_col_candidate: str = "p_value_for_fdr",
    scope_version: str = "phase1_v1",
) -> pd.DataFrame:
    """
    Canonical cross-campaign / campaign-lineage multiplicity adjustment.

    This is THE standard API for scope-level FDR correction in promotions.
    It adds scope-aware multiplicity fields alongside existing family-level fields.

    This function MUST be called before promotion gates. All promoted candidates
    will have effective_q_value computed as max(q_value, q_value_scope, q_value_program).

    Phase 1 invariant:
        No promoted candidate may lack effective_q_value.

    Inputs:
        - frame: DataFrame with at least run_id, p_value columns
        - max_q: FDR threshold
        - scope_mode: "run", "campaign", "program", "campaign_lineage"
        - eligible_col: column indicating multiplicity eligibility
        - p_col_candidate: column name for p-value
        - scope_version: version string for this contract

    Outputs (added to frame):
        - num_tests_scope
        - q_value_scope
        - is_discovery_scope
        - effective_q_value (canonical q-value for promotion decisions)
        - multiplicity_scope_mode
        - multiplicity_scope_key
        - multiplicity_scope_version
        - multiplicity_scope_degraded (if historical data missing)

    See:
        - docs/92_assurance_and_benchmarks.md for status
        - project/research/contracts/multiplicity_scope.py for contract
    """
    from project.research.contracts.multiplicity_scope import resolve_effective_scope_key

    if frame.empty:
        return frame.copy()

    out = frame.copy()

    p_col = (
        _resolve_multiplicity_p_value_column(out)
        if p_col_candidate not in out.columns
        else p_col_candidate
    )

    test_weights = pd.Series(1, index=out.index)
    if "side_policy" in out.columns:
        test_weights[out["side_policy"].astype(str).str.lower() == "both"] = 2

    eligible_mask = out.get(eligible_col, pd.Series(True, index=out.index))

    out["multiplicity_scope_mode"] = scope_mode
    out["multiplicity_scope_version"] = scope_version

    out["multiplicity_scope_key"] = out.apply(
        lambda r: resolve_effective_scope_key(r.to_dict(), mode=scope_mode), axis=1
    )

    scope_groups = out.groupby("multiplicity_scope_key")

    out["num_tests_scope"] = 0
    out["q_value_scope"] = 1.0
    out["is_discovery_scope"] = False

    for scope_key, group_idx in scope_groups.groups.items():
        group_eligible = eligible_mask.loc[group_idx]
        if not group_eligible.any():
            continue
        n_tests = int(test_weights.loc[group_idx][group_eligible].sum())
        out.loc[group_idx, "num_tests_scope"] = n_tests

        if n_tests > 0:
            p_vals = out.loc[group_idx[group_eligible], p_col].fillna(1.0).to_numpy()
            q_vals = bh_adjust(p_vals, n_tests=n_tests)
            out.loc[group_idx[group_eligible], "q_value_scope"] = q_vals

    eligible_out = out[eligible_mask]
    if not eligible_out.empty:
        out.loc[eligible_mask, "is_discovery_scope"] = out.loc[
            eligible_mask, "q_value_scope"
        ] <= float(max_q)

    q_value_col = "q_value" if "q_value" in out.columns else "q_value_family"
    if q_value_col not in out.columns:
        out[q_value_col] = 1.0
    else:
        local_q = out[q_value_col].apply(_finite_q_value)
        out["q_value_scope"] = np.maximum(out["q_value_scope"].astype(float), local_q.astype(float))

    # max() is intentional: each scope (family, campaign, program) is an independent
    # FDR constraint. A candidate must satisfy all of them — scope-shopping is not
    # allowed. Taking the worst-case (max) q-value across scopes means the candidate
    # fails if any scope-level correction rejects it. This is conservative by design.
    out["effective_q_value"] = out.apply(
        lambda r: max(
            _finite_q_value(r.get(q_value_col, 1.0)),
            _finite_q_value(r.get("q_value_scope", 1.0)),
            _finite_q_value(r.get("q_value_program", 1.0)),
        ),
        axis=1,
    )

    out["is_discovery_effective"] = out["effective_q_value"] <= float(max_q)

    return out


def merge_historical_candidates(
    current: pd.DataFrame,
    historical: pd.DataFrame | None,
    *,
    scope_mode: str = "campaign_lineage",
    lineage_key_col: str = "concept_lineage_key",
) -> pd.DataFrame:
    """
    Merge current candidates with historical tested universe for cross-campaign scope.

    If historical is None or empty, returns current with degraded scope status.

    Outputs:
        - multiplicity_scope_degraded: bool (True if historical missing)
        - multiplicity_scope_reason: str ("missing_history" or "ok")
        - Historical rows are added with a flag `multiplicity_context="historical"`
        - Current rows have `multiplicity_context="current"`
    """
    from project.research.contracts.multiplicity_scope import resolve_effective_scope_key

    if current.empty:
        return current.copy()

    out = current.copy()
    out["multiplicity_context"] = "current"

    if historical is None or historical.empty:
        out["multiplicity_scope_degraded"] = pd.Series(
            [True] * len(out), index=out.index, dtype=object
        )
        out["multiplicity_scope_reason"] = "missing_history"
        return out

    historical_copy = historical.copy()
    historical_copy["multiplicity_context"] = "historical"

    for col in ["multiplicity_scope_key", "multiplicity_context"]:
        if col not in historical_copy.columns:
            historical_copy[col] = ""

    combined = pd.concat([out, historical_copy], ignore_index=True)
    combined["multiplicity_scope_key"] = combined.apply(
        lambda r: resolve_effective_scope_key(r.to_dict(), mode=scope_mode), axis=1
    )
    combined["multiplicity_scope_degraded"] = pd.Series(
        [False] * len(combined), index=combined.index, dtype=object
    )
    combined["multiplicity_scope_reason"] = "ok"

    return combined


def build_multiplicity_diagnostics(
    scored: pd.DataFrame,
    *,
    max_q: float,
    mode: str = "production",
    min_sample_size: int = 0,
) -> dict:
    """Compute summary diagnostics for multiplicity results."""
    if scored.empty:
        return {"global": {"discovery_count": 0}, "families": {}}

    discovery_mask = scored.get("is_discovery", pd.Series(False, index=scored.index))
    family_discovery_mask = scored.get("is_discovery_family", pd.Series(False, index=scored.index))

    return {
        "global": {
            "run_id": str(scored["run_id"].iloc[0]) if "run_id" in scored.columns else "unknown",
            "max_q_threshold": float(max_q),
            "candidates_total": len(scored),
            "families_total": int(scored["family_id"].nunique())
            if "family_id" in scored.columns
            else 0,
            "eligible_candidates": int(
                scored.get("multiplicity_pool_eligible", pd.Series(False)).sum()
            ),
            "families_pool_eligible": int(
                scored[scored.get("multiplicity_pool_eligible", pd.Series(False))][
                    "family_id"
                ].nunique()
            )
            if "family_id" in scored.columns
            else 0,
            "discoveries_total": int(discovery_mask.sum()),
            "discovered_families_count": int(scored[family_discovery_mask]["family_id"].nunique())
            if "family_id" in scored.columns
            else 0,
            "discoveries_by_total": int(scored.get("is_discovery_by", pd.Series(False)).sum()),
            "discoveries_cluster_total": int(
                scored.get("is_discovery_cluster", pd.Series(False)).sum()
            ),
        },
        "by_family": scored.groupby("family_id").size().to_dict()
        if "family_id" in scored.columns
        else {},
        "by_cluster": scored.groupby("family_cluster_id").size().to_dict()
        if "family_cluster_id" in scored.columns
        else {},
    }
