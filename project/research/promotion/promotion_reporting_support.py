from __future__ import annotations

import json
import re
from typing import Any

import numpy as np
import pandas as pd

from project.core.coercion import as_bool, safe_float, safe_int
from project.research.utils.decision_safety import bool_gate

_OVERLAP_TOKEN_SPLIT_RE = re.compile(r"[^a-z0-9]+")


def _quiet_float(value: Any, default: float) -> float:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return float(default)
    coerced = safe_float(value, default)
    return float(default if coerced is None else coerced)


def _quiet_int(value: Any, default: int) -> int:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return int(default)
    coerced = safe_int(value, default)
    return int(default if coerced is None else coerced)


def resolve_promotion_tier(
    row: dict[str, Any],
    *,
    require_retail_viability: bool = True,
    promotion_confirmatory_gates: dict[str, Any] | None = None,
) -> str:
    decision = str(row.get("promotion_decision", "")).strip().lower()
    if decision != "promoted":
        return "research_promoted"

    conf_gates = promotion_confirmatory_gates or {}
    dep_gates = conf_gates.get("deployable", {})

    stat_ok = True
    max_q = float(dep_gates.get("max_q_value", 0.025))
    q_value = _quiet_float(row.get("effective_q_value", row.get("q_value", 1.0)), 1.0)
    test_q_value = _quiet_float(row.get("test_q_value", q_value), q_value)
    stat_ok = (q_value <= max_q) and (test_q_value <= max_q)

    samples_ok = True
    min_oos_events = int(dep_gates.get("min_oos_event_count", 75))
    validation_samples = _quiet_int(
        row.get("validation_samples", row.get("validation_samples_raw")),
        0,
    )
    test_samples = _quiet_int(row.get("test_samples", row.get("test_samples_raw")), 0)
    samples_ok = (validation_samples >= min_oos_events) and (test_samples >= min_oos_events)

    bridge_viability_ok = bool(
        row.get("gate_bridge_tradable") == "pass" or row.get("gate_bridge_tradable") is True
    )
    track = str(row.get("promotion_track", "")).strip().lower()
    retail_viable = bool(bool_gate(row.get("gate_promo_retail_viability")))
    redundancy_ok = bool(bool_gate(row.get("gate_promo_redundancy")))
    retail_gate_ok = retail_viable if bool(require_retail_viability) else True
    dsr_ok = bool(bool_gate(row.get("gate_promo_dsr"))) and _quiet_float(
        row.get("dsr_value"), 0.0
    ) >= float(dep_gates.get("min_dsr", 0.5))
    cost_ok = _quiet_float(row.get("cost_survival_ratio"), 0.0) >= float(
        dep_gates.get("min_cost_survival_ratio", 1.0)
    )
    regimes_ok = bool(bool_gate(row.get("gate_regime_stability"))) and _quiet_int(
        row.get("num_regimes_supported"), 0
    ) >= int(dep_gates.get("min_regimes_supported", 2))
    robustness_ok = bool(bool_gate(row.get("gate_promo_robustness"))) or bool(
        as_bool(row.get("robustness_panel_complete", False))
    )
    multiplicity_ok = bool(bool_gate(row.get("gate_promo_multiplicity_confirmatory"))) and bool(
        bool_gate(row.get("gate_promo_multiplicity_diagnostics"))
    )
    n_events_ok = _quiet_int(row.get("n_events"), 0) >= int(dep_gates.get("min_events", 150))

    if (
        track == "standard"
        and retail_gate_ok
        and redundancy_ok
        and stat_ok
        and samples_ok
        and bridge_viability_ok
        and dsr_ok
        and cost_ok
        and regimes_ok
        and robustness_ok
        and multiplicity_ok
        and n_events_ok
    ):
        return "live_eligible"
    return "paper_eligible"


def build_promotion_capital_footprint(
    *,
    promoted_df: pd.DataFrame,
    contract: Any,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    cols = [
        "candidate_id",
        "event_type",
        "promotion_track",
        "usage_signal",
        "usage_signal_source",
        "target_account_size_usd",
        "capital_budget_usd",
        "per_position_notional_cap_usd",
        "max_concurrent_positions",
        "turnover_proxy_mean",
        "capacity_proxy",
        "estimated_position_notional_usd",
        "slot_pressure_fraction",
        "leverage_usage_fraction",
        "gate_capital_slot_within_limit",
        "gate_capital_leverage_within_budget",
    ]
    if promoted_df.empty:
        return pd.DataFrame(columns=cols), {
            "promoted_count": 0,
            "estimated_notional_count": 0,
            "slot_pressure_over_limit_count": 0,
            "leverage_over_budget_count": 0,
            "mean_estimated_position_notional_usd": 0.0,
        }

    target_account_size_usd = _quiet_float(
        getattr(contract, "target_account_size_usd", None), np.nan
    )
    capital_budget_usd = _quiet_float(getattr(contract, "capital_budget_usd", None), np.nan)
    per_position_cap_usd = _quiet_float(
        getattr(contract, "effective_per_position_notional_cap_usd", None), np.nan
    )
    max_concurrent_positions = _quiet_int(getattr(contract, "max_concurrent_positions", None), 0)
    turnover_cap = _quiet_float(getattr(contract, "max_daily_turnover_multiple", None), np.nan)

    rows: list[dict[str, Any]] = []
    for row in promoted_df.to_dict(orient="records"):
        turnover_proxy_mean = _quiet_float(row.get("turnover_proxy_mean"), np.nan)
        capacity_proxy = _quiet_float(row.get("capacity_proxy"), np.nan)
        usage_signal, usage_signal_source = np.nan, "none"
        if np.isfinite(capacity_proxy) and capacity_proxy > 0.0:
            usage_signal, usage_signal_source = (
                float(np.clip(capacity_proxy, 0.0, 1.0)),
                "capacity_proxy",
            )
        elif np.isfinite(turnover_proxy_mean) and np.isfinite(turnover_cap) and turnover_cap > 0.0:
            usage_signal, usage_signal_source = (
                float(np.clip(turnover_proxy_mean / turnover_cap, 0.0, 1.0)),
                "turnover_ratio",
            )
        elif np.isfinite(turnover_proxy_mean):
            usage_signal, usage_signal_source = (
                float(np.clip(turnover_proxy_mean / 10.0, 0.0, 1.0)),
                "turnover_proxy_fallback",
            )

        est_notional = (
            capital_budget_usd * usage_signal
            if np.isfinite(capital_budget_usd) and np.isfinite(usage_signal)
            else np.nan
        )
        slot_pressure = (
            est_notional / per_position_cap_usd
            if np.isfinite(est_notional)
            and np.isfinite(per_position_cap_usd)
            and per_position_cap_usd > 0.0
            else np.nan
        )
        leverage_usage = (
            est_notional / target_account_size_usd
            if np.isfinite(est_notional)
            and np.isfinite(target_account_size_usd)
            and target_account_size_usd > 0.0
            else np.nan
        )

        rows.append(
            {
                "candidate_id": str(row.get("candidate_id", "")).strip(),
                "event_type": str(row.get("event_type", row.get("event", ""))).strip(),
                "promotion_track": str(row.get("promotion_track", "")).strip(),
                "fallback_used": bool(as_bool(row.get("fallback_used", False))),
                "fallback_reason": str(row.get("fallback_reason", "")).strip(),
                "usage_signal": None if not np.isfinite(usage_signal) else float(usage_signal),
                "usage_signal_source": usage_signal_source,
                "target_account_size_usd": None
                if not np.isfinite(target_account_size_usd)
                else float(target_account_size_usd),
                "capital_budget_usd": None
                if not np.isfinite(capital_budget_usd)
                else float(capital_budget_usd),
                "per_position_notional_cap_usd": None
                if not np.isfinite(per_position_cap_usd)
                else float(per_position_cap_usd),
                "max_concurrent_positions": int(max_concurrent_positions),
                "turnover_proxy_mean": None
                if not np.isfinite(turnover_proxy_mean)
                else float(turnover_proxy_mean),
                "capacity_proxy": None
                if not np.isfinite(capacity_proxy)
                else float(capacity_proxy),
                "estimated_position_notional_usd": None
                if not np.isfinite(est_notional)
                else float(est_notional),
                "slot_pressure_fraction": None
                if not np.isfinite(slot_pressure)
                else float(slot_pressure),
                "leverage_usage_fraction": None
                if not np.isfinite(leverage_usage)
                else float(leverage_usage),
                "gate_capital_slot_within_limit": bool(slot_pressure <= 1.0)
                if np.isfinite(slot_pressure)
                else False,
                "gate_capital_leverage_within_budget": bool(usage_signal <= 1.0)
                if np.isfinite(usage_signal)
                else False,
            }
        )

    out_df = pd.DataFrame(rows, columns=cols)
    est_notional_s = pd.to_numeric(
        out_df.get("estimated_position_notional_usd", pd.Series(dtype=float)), errors="coerce"
    )
    summary = {
        "promoted_count": len(out_df),
        "estimated_notional_count": int(est_notional_s.notna().sum()),
        "slot_pressure_over_limit_count": int(
            (pd.to_numeric(out_df["slot_pressure_fraction"], errors="coerce") > 1.0).sum()
        ),
        "leverage_over_budget_count": int(
            (pd.to_numeric(out_df["leverage_usage_fraction"], errors="coerce") > 1.0).sum()
        ),
        "mean_estimated_position_notional_usd": 0.0
        if est_notional_s.dropna().empty
        else float(est_notional_s.dropna().mean()),
    }
    return out_df, summary


def behavior_key(row: dict[str, Any]) -> tuple[str, ...]:
    fields = (
        "event_type",
        "event",
        "symbol",
        "condition",
        "action",
        "direction_rule",
        "horizon",
        "template_verb",
        "state_id",
        "family_id",
    )
    return tuple(str(row.get(f, "")).strip().lower() for f in fields)


def behavior_token_set(row: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    fields = (
        "event_type",
        "event",
        "symbol",
        "condition",
        "action",
        "direction_rule",
        "horizon",
        "template_verb",
        "state_id",
        "family_id",
        "condition_label",
    )
    for field in fields:
        raw = str(row.get(field, "")).strip().lower()
        if not raw or raw in {"none", "nan"}:
            continue
        for token in _OVERLAP_TOKEN_SPLIT_RE.split(raw):
            if token:
                tokens.add(token)
    signature_hash = str(row.get("behavior_signature_hash", "")).strip().lower()
    if signature_hash:
        tokens.add(f"sig:{signature_hash}")
    return tokens


def behavior_overlap_score(left: dict[str, Any], right: dict[str, Any]) -> float:
    if behavior_key(left) == behavior_key(right):
        return 1.0
    lt, rt = behavior_token_set(left), behavior_token_set(right)
    if not lt or not rt:
        return float(np.nan)
    union = lt | rt
    if not union:
        return float(np.nan)
    return float(len(lt & rt) / len(union))


def delay_profile_map(row: dict[str, Any]) -> dict[int, float]:
    payload = row.get("delay_expectancy_map", {})
    if isinstance(payload, str):
        text = payload.strip()
        try:
            payload = json.loads(text) if text else {}
        except Exception:
            payload = {}
    if not isinstance(payload, dict):
        return {}
    out: dict[int, float] = {}
    for k, v in payload.items():
        try:
            d = int(float(k))
            e = float(v)
            if np.isfinite(e):
                out[d] = e
        except (TypeError, ValueError):
            continue
    return out


def delay_profile_correlation(left: dict[str, Any], right: dict[str, Any]) -> float:
    lm, rm = delay_profile_map(left), delay_profile_map(right)
    shared = sorted(set(lm.keys()) & set(rm.keys()))
    if len(shared) < 2:
        return float(np.nan)
    lv = np.asarray([lm[d] for d in shared], dtype=float)
    rv = np.asarray([rm[d] for d in shared], dtype=float)
    if lv.size < 2 or rv.size < 2 or float(np.std(lv)) <= 1e-12 or float(np.std(rv)) <= 1e-12:
        return float(np.nan)
    corr = np.corrcoef(lv, rv)[0, 1]
    return float(abs(corr)) if np.isfinite(corr) else float(np.nan)


def apply_portfolio_overlap_gate(
    *,
    promoted_df: pd.DataFrame,
    max_overlap_ratio: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if promoted_df.empty:
        return promoted_df.copy(), pd.DataFrame(
            columns=[
                "candidate_id",
                "event_type",
                "overlap_with_candidate_id",
                "overlap_score",
                "overlap_reason",
            ]
        )
    sort_cols, ascending = [], []
    for col, asc in (
        ("selection_score", False),
        ("promotion_score", False),
        ("robustness_score", False),
        ("n_events", False),
        ("candidate_id", True),
    ):
        if col in promoted_df.columns:
            sort_cols.append(col)
            ascending.append(asc)
    ranked = (
        promoted_df.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)
        if sort_cols
        else promoted_df.reset_index(drop=True)
    )
    selected_rows, dropped_rows = [], []
    for row in ranked.to_dict(orient="records"):
        cid = str(row.get("candidate_id", "")).strip()
        etype = str(row.get("event_type", row.get("event", ""))).strip()
        worst_overlap, worst_with = float(np.nan), ""
        for selected in selected_rows:
            score = behavior_overlap_score(row, selected)
            if np.isfinite(score) and (not np.isfinite(worst_overlap) or score > worst_overlap):
                worst_overlap, worst_with = (
                    float(score),
                    str(selected.get("candidate_id", "")).strip(),
                )
        if np.isfinite(worst_overlap) and worst_overlap >= float(max_overlap_ratio):
            dropped_rows.append(
                {
                    "candidate_id": cid,
                    "event_type": etype,
                    "overlap_with_candidate_id": worst_with,
                    "overlap_score": float(worst_overlap),
                    "overlap_reason": "orthogonality_overlap_gate",
                }
            )
            continue
        selected_rows.append(row)
    return pd.DataFrame(selected_rows, columns=ranked.columns), pd.DataFrame(
        dropped_rows,
        columns=[
            "candidate_id",
            "event_type",
            "overlap_with_candidate_id",
            "overlap_score",
            "overlap_reason",
        ],
    )


def portfolio_diversification_violations(
    *,
    promoted_df: pd.DataFrame,
    max_profile_correlation: float,
    max_overlap_ratio: float,
    max_examples: int = 25,
) -> dict[str, Any]:
    rows = promoted_df.to_dict(orient="records")
    pair_total, corr_eval, overlap_eval = 0, 0, 0
    corr_violations, overlap_violations = [], []
    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            left, right = rows[i], rows[j]
            lid, rid = (
                str(left.get("candidate_id", "")).strip(),
                str(right.get("candidate_id", "")).strip(),
            )
            pair_total += 1
            corr = delay_profile_correlation(left, right)
            if np.isfinite(corr):
                corr_eval += 1
                if corr >= float(max_profile_correlation):
                    corr_violations.append(
                        {
                            "left_candidate_id": lid,
                            "right_candidate_id": rid,
                            "profile_correlation": float(corr),
                        }
                    )
            overlap = behavior_overlap_score(left, right)
            if np.isfinite(overlap):
                overlap_eval += 1
                if overlap >= float(max_overlap_ratio):
                    overlap_violations.append(
                        {
                            "left_candidate_id": lid,
                            "right_candidate_id": rid,
                            "overlap_ratio": float(overlap),
                        }
                    )
    return {
        "pair_count_total": int(pair_total),
        "correlation_pairs_evaluated": int(corr_eval),
        "overlap_pairs_evaluated": int(overlap_eval),
        "correlation_violation_count": len(corr_violations),
        "overlap_violation_count": len(overlap_violations),
        "correlation_violations": corr_violations[:max_examples],
        "overlap_violations": overlap_violations[:max_examples],
    }


def assign_and_validate_promotion_tiers(
    *,
    audit_df: pd.DataFrame,
    promoted_df: pd.DataFrame,
    require_retail_viability: bool,
    promotion_confirmatory_gates: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    def _get_tier(row: dict[str, Any]) -> str:
        return resolve_promotion_tier(
            row,
            require_retail_viability=require_retail_viability,
            promotion_confirmatory_gates=promotion_confirmatory_gates,
        )

    if not audit_df.empty:
        audit_df = audit_df.copy()
        audit_df["promotion_tier"] = [_get_tier(r) for r in audit_df.to_dict(orient="records")]
    else:
        audit_df = audit_df.copy()
        audit_df["promotion_tier"] = pd.Series(dtype="object")
    if not promoted_df.empty:
        promoted_df = promoted_df.copy()
        promoted_df["promotion_tier"] = [
            _get_tier(r) for r in promoted_df.to_dict(orient="records")
        ]
        if (promoted_df["promotion_tier"] == "research_promoted").any():
            raise ValueError("promoted output cannot contain tier=research_promoted")
    else:
        promoted_df = promoted_df.copy()
        promoted_df["promotion_tier"] = pd.Series(dtype="object")

    tier_counts = (
        audit_df.get("promotion_tier", pd.Series(dtype="object"))
        .astype(str)
        .value_counts()
        .to_dict()
    )
    return audit_df, promoted_df, {str(k): int(v) for k, v in tier_counts.items()}


def stabilize_promoted_output_schema(
    *,
    promoted_df: pd.DataFrame,
    audit_df: pd.DataFrame,
) -> pd.DataFrame:
    out = promoted_df.copy()
    if "status" not in out.columns:
        if out.empty:
            out["status"] = pd.Series(dtype="object")
        else:
            out["status"] = pd.Series("PROMOTED", index=out.index, dtype="object")

    preferred_order = [
        "candidate_id",
        "run_id",
        "symbol",
        "event",
        "event_type",
        "status",
        "promotion_decision",
        "promotion_tier",
        "promotion_track",
        "is_reduced_evidence",
        "condition",
        "action",
        "direction_rule",
        "horizon",
        "effective_lag_bars",
        "selection_score",
        "n_events",
        "gate_bridge_tradable",
        "gate_promo_retail_viability",
        "gate_promo_low_capital_viability",
    ]

    if out.empty:
        merged_cols = []
        for c in preferred_order + audit_df.columns.tolist() + out.columns.tolist():
            if c not in merged_cols:
                merged_cols.append(c)
        source = audit_df.copy()
        if "status" not in source.columns:
            source["status"] = pd.Series(dtype="object")
        dtype_by_col = {
            c: source[c].dtype
            if c in source.columns
            else (out[c].dtype if c in out.columns else "object")
            for c in merged_cols
        }
        return pd.DataFrame(
            {c: pd.Series(dtype=dtype_by_col[c]) for c in merged_cols}, columns=merged_cols
        )

    for column in preferred_order:
        if column in out.columns:
            continue
        if column == "status":
            out[column] = pd.Series("PROMOTED", index=out.index, dtype="object")
        elif column in {"selection_score"}:
            out[column] = np.nan
        elif column in {"n_events", "effective_lag_bars"}:
            out[column] = 0
        elif column.startswith("gate_"):
            out[column] = False
        else:
            out[column] = ""
    ordered_cols = []
    for c in preferred_order + out.columns.tolist():
        if c not in ordered_cols:
            ordered_cols.append(c)
    return out.reindex(columns=ordered_cols)
