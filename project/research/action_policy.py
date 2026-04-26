
from __future__ import annotations

import math
from typing import Any

import pandas as pd

from project.research.knowledge.memory import compute_event_statistics

_POLICY_SCOPE_FIELDS: tuple[str, ...] = (
    "event_type",
    "trigger_type",
    "trigger_key",
    "trigger_payload_json",
    "template_id",
    "direction",
    "horizon",
    "entry_lag",
    "context_json",
    "state_id",
    "region_key",
    "feature",
    "operator",
    "threshold",
    "from_state",
    "to_state",
    "canonical_regimes",
    "subtypes",
    "phases",
    "evidence_modes",
)


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        numeric = float(value)
        if not math.isfinite(numeric):
            return float(default)
        return float(numeric)
    except Exception:
        return float(default)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _bounded_rate(value: Any, default: float = 0.0) -> float:
    numeric = _coerce_float(value, default)
    return float(min(max(numeric, 0.0), 1.0))


def is_cost_fail_gate(value: Any) -> bool:
    token = str(value or "").strip().lower()
    if not token:
        return False
    return ("cost" in token) or ("after_cost" in token) or token in {
        "gate_after_cost_positive",
        "gate_after_cost_stressed_positive",
    }


def _gate_rank(val: Any) -> int:
    text = str(val).strip().lower()
    if text in ("pass", "true", "1", "1.0"):
        return 2
    if text in ("fail", "false", "0", "0.0"):
        return 1
    return 0


def event_economics_signals(event_statistics: pd.DataFrame) -> dict[str, dict[str, Any]]:
    signals: dict[str, dict[str, Any]] = {}
    if event_statistics.empty or "event_type" not in event_statistics.columns:
        return signals
    for row in event_statistics.to_dict(orient="records"):
        event_type = str(row.get("event_type", "")).strip()
        if not event_type:
            continue
        times_evaluated = max(_coerce_int(row.get("times_evaluated"), 0), 0)
        times_promoted = max(_coerce_int(row.get("times_promoted"), 0), 0)
        avg_after = _coerce_float(row.get("avg_after_cost_expectancy"), 0.0)
        median_after = _coerce_float(row.get("median_after_cost_expectancy"), avg_after)
        recent_after = _coerce_float(row.get("recent_after_cost_expectancy"), median_after)
        avg_stressed = _coerce_float(row.get("avg_stressed_after_cost_expectancy"), avg_after)
        median_stressed = _coerce_float(row.get("median_stressed_after_cost_expectancy"), avg_stressed)
        recent_stressed = _coerce_float(row.get("recent_stressed_after_cost_expectancy"), median_stressed)
        pos_rate = _bounded_rate(row.get("positive_after_cost_rate"), 1.0 if avg_after > 0.0 else 0.0)
        pos_stressed_rate = _bounded_rate(
            row.get("positive_stressed_after_cost_rate"),
            1.0 if median_stressed > 0.0 else 0.0,
        )
        tradable_rate = _bounded_rate(row.get("tradable_rate"), 0.0)
        stat_rate = _bounded_rate(row.get("statistical_pass_rate"), 0.0)
        avg_q = _coerce_float(row.get("avg_q_value"), 0.5)
        dominant_fail_gate = str(row.get("dominant_fail_gate", "") or "").strip()

        evidence_weight = min(times_evaluated / 5.0, 1.0) if times_evaluated > 0 else 0.0
        promotion_rate = min(times_promoted / max(times_evaluated, 1), 1.0)
        blended_after = (0.35 * avg_after) + (0.30 * median_after) + (0.35 * recent_after)
        blended_stressed = (0.25 * avg_stressed) + (0.35 * median_stressed) + (0.40 * recent_stressed)
        expectancy_component = 0.95 * math.tanh(blended_after / 4.0) * evidence_weight
        stressed_component = 0.90 * math.tanh(blended_stressed / 4.0) * evidence_weight
        positive_component = 0.35 * ((pos_rate - 0.5) * 2.0) * evidence_weight
        stressed_positive_component = 0.55 * ((pos_stressed_rate - 0.5) * 2.0) * evidence_weight
        tradability_component = 0.25 * ((tradable_rate - 0.5) * 2.0) * evidence_weight
        stat_component = 0.25 * ((stat_rate - 0.5) * 2.0) * evidence_weight
        promotion_component = 0.55 * promotion_rate * evidence_weight
        quality_edge = max(-1.0, min((0.35 - avg_q) / 0.35, 1.0))
        quality_component = 0.30 * quality_edge * evidence_weight
        cost_drag = 0.45 * evidence_weight if is_cost_fail_gate(dominant_fail_gate) else 0.0
        if blended_stressed <= 0.0:
            cost_drag += 0.20 * evidence_weight
        score = (
            expectancy_component
            + stressed_component
            + positive_component
            + stressed_positive_component
            + tradability_component
            + stat_component
            + promotion_component
            + quality_component
            - cost_drag
        )
        signals[event_type] = {
            "score": float(score),
            "status": "positive" if score > 0.15 else "negative" if score < -0.15 else "neutral",
            "evidence_weight": float(evidence_weight),
            "times_evaluated": int(times_evaluated),
            "times_promoted": int(times_promoted),
            "promotion_rate": float(promotion_rate),
            "avg_after_cost_expectancy": float(avg_after),
            "recent_after_cost_expectancy": float(recent_after),
            "avg_stressed_after_cost_expectancy": float(avg_stressed),
            "recent_stressed_after_cost_expectancy": float(recent_stressed),
            "positive_after_cost_rate": float(pos_rate),
            "positive_stressed_after_cost_rate": float(pos_stressed_rate),
            "tradable_rate": float(tradable_rate),
            "statistical_pass_rate": float(stat_rate),
            "avg_q_value": float(avg_q),
            "dominant_fail_gate": dominant_fail_gate,
            "cost_drag": float(cost_drag),
        }
    return signals


def _scope_from_row(row: dict[str, Any]) -> dict[str, Any]:
    scope: dict[str, Any] = {}
    for key in _POLICY_SCOPE_FIELDS:
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        scope[key] = value
    if "trigger_type" not in scope:
        scope["trigger_type"] = "EVENT"
    return scope


def _policy_evidence(event_signal: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_score": float(event_signal.get("score", 0.0) or 0.0),
        "event_status": str(event_signal.get("status", "unknown") or "unknown"),
        "evidence_weight": float(event_signal.get("evidence_weight", 0.0) or 0.0),
        "recent_after_cost_expectancy": float(event_signal.get("recent_after_cost_expectancy", 0.0) or 0.0),
        "recent_stressed_after_cost_expectancy": float(event_signal.get("recent_stressed_after_cost_expectancy", 0.0) or 0.0),
        "positive_after_cost_rate": float(event_signal.get("positive_after_cost_rate", 0.0) or 0.0),
        "positive_stressed_after_cost_rate": float(event_signal.get("positive_stressed_after_cost_rate", 0.0) or 0.0),
        "tradable_rate": float(event_signal.get("tradable_rate", 0.0) or 0.0),
        "statistical_pass_rate": float(event_signal.get("statistical_pass_rate", 0.0) or 0.0),
        "dominant_fail_gate": str(event_signal.get("dominant_fail_gate", "") or ""),
        "row_after_cost_expectancy": _coerce_float(row.get("after_cost_expectancy"), 0.0),
        "row_stressed_after_cost_expectancy": _coerce_float(row.get("stressed_after_cost_expectancy"), 0.0),
        "row_q_value": _coerce_float(row.get("q_value"), 1.0),
    }


def _ranked_rows(tested_regions: pd.DataFrame) -> pd.DataFrame:
    ranked = tested_regions.copy()
    if "gate_promo_statistical" in ranked.columns:
        ranked["__gate_rank"] = ranked["gate_promo_statistical"].apply(_gate_rank)
    else:
        ranked["__gate_rank"] = 0
    ranked["__after_cost"] = pd.to_numeric(ranked.get("after_cost_expectancy", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    ranked["__q"] = pd.to_numeric(ranked.get("q_value", pd.Series(dtype=float)), errors="coerce").fillna(1.0)
    return ranked.sort_values(["__gate_rank", "__after_cost", "__q"], ascending=[False, False, True])


def build_action_policy_queues(
    tested_regions: pd.DataFrame,
    *,
    exploit_top_k: int = 3,
    retest_top_k: int | None = None,
    hold_top_k: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    if tested_regions.empty:
        return {"exploit": [], "retest": [], "hold": []}

    retest_top_k = int(retest_top_k if retest_top_k is not None else exploit_top_k)
    hold_top_k = int(hold_top_k if hold_top_k is not None else exploit_top_k)
    signals = event_economics_signals(compute_event_statistics(tested_regions))
    ranked = _ranked_rows(tested_regions)
    exploit: list[dict[str, Any]] = []
    retest: list[dict[str, Any]] = []
    hold: list[dict[str, Any]] = []
    seen_exploit: set[str] = set()
    seen_retest: set[str] = set()
    seen_hold: set[str] = set()

    for row in ranked.to_dict(orient="records"):
        event_type = str(row.get("event_type", "") or "").strip()
        signal = signals.get(event_type, {})
        if not signal:
            continue
        region_key = str(row.get("region_key", "") or "")
        dedupe_key = region_key or f"{event_type}|{row.get('template_id', '')}|{row.get('direction', '')}|{row.get('horizon', '')}|{row.get('entry_lag', '')}"
        evidence = _policy_evidence(signal, row)
        score = float(signal.get("score", 0.0) or 0.0)
        ev_weight = float(signal.get("evidence_weight", 0.0) or 0.0)
        recent_after = float(signal.get("recent_after_cost_expectancy", 0.0) or 0.0)
        recent_stressed = float(signal.get("recent_stressed_after_cost_expectancy", 0.0) or 0.0)
        tradable_rate = float(signal.get("tradable_rate", 0.0) or 0.0)
        stat_rate = float(signal.get("statistical_pass_rate", 0.0) or 0.0)
        pos_rate = float(signal.get("positive_after_cost_rate", 0.0) or 0.0)
        pos_stressed_rate = float(signal.get("positive_stressed_after_cost_rate", 0.0) or 0.0)
        dominant_fail_gate = str(signal.get("dominant_fail_gate", "") or "")
        cost_fail = is_cost_fail_gate(dominant_fail_gate)
        structurally_viable = (tradable_rate >= 0.45) or (stat_rate >= 0.45) or (_gate_rank(row.get("gate_promo_statistical")) >= 1)
        unstable_but_live = (
            structurally_viable
            and (recent_after > 0.0 or pos_rate >= 0.4)
            and (recent_stressed <= 0.0 or pos_stressed_rate < 0.55 or 0.25 <= stat_rate <= 0.75)
        )
        repeated_cost_drag = cost_fail and ev_weight >= 0.4 and recent_after <= 0.0 and recent_stressed <= 0.0
        exploit_ready = (
            score >= 0.20
            and recent_after > 0.0
            and recent_stressed > 0.0
            and tradable_rate >= 0.5
            and stat_rate >= 0.4
            and pos_rate >= 0.5
            and pos_stressed_rate >= 0.5
            and not cost_fail
        )
        hold_ready = repeated_cost_drag or (score <= -0.20 and ev_weight >= 0.4 and recent_after <= 0.0 and recent_stressed <= 0.0)
        retest_ready = (not exploit_ready) and (not hold_ready) and (unstable_but_live or (score >= -0.05 and structurally_viable and recent_after > 0.0))

        if exploit_ready and dedupe_key not in seen_exploit and len(exploit) < max(exploit_top_k, 0):
            seen_exploit.add(dedupe_key)
            exploit.append({
                "reason": "economics policy: recent and stressed post-cost economics are strengthening",
                "priority": "high",
                "policy_action": "exploit",
                "policy_evidence": evidence,
                "proposed_scope": _scope_from_row(row),
            })
            continue
        if retest_ready and dedupe_key not in seen_retest and len(retest) < max(retest_top_k, 0):
            seen_retest.add(dedupe_key)
            retest.append({
                "reason": "economics policy: structurally viable but unstable after-cost region needs retest",
                "priority": "medium",
                "policy_action": "retest",
                "policy_evidence": evidence,
                "proposed_scope": _scope_from_row(row),
            })
            continue
        if hold_ready and dedupe_key not in seen_hold and len(hold) < max(hold_top_k, 0):
            seen_hold.add(dedupe_key)
            hold.append({
                "reason": "economics policy: repeated cost drag dominates this region",
                "priority": "medium",
                "policy_action": "hold",
                "policy_evidence": evidence,
                "proposed_scope": _scope_from_row(row),
            })
    return {"exploit": exploit, "retest": retest, "hold": hold}
