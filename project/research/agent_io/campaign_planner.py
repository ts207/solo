from __future__ import annotations

import argparse
import json
import logging
import math
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.events.governance import event_matches_filters, get_event_governance_metadata
from project.io.utils import atomic_write_text
from project.research.agent_io.generated_proposal_policy import (
    resolve_generated_proposal_controls,
    summarize_viability_for_event,
)
from project.research.agent_io.issue_proposal import generate_run_id, issue_proposal
from project.research.agent_io.proposal_schema import load_operator_proposal
from project.research.feature_surface_viability import analyze_feature_surface_viability
from project.research.knowledge.memory import ensure_memory_store, read_memory_table
from project.research.knowledge.schemas import canonical_json, region_key
from project.research.semantic_registry_views import build_canonical_semantic_registry_views
from project.spec_registry.search_space import (
    DEFAULT_EVENT_PRIORITY_WEIGHT,
    load_event_priority_weights,
)

_LOG = logging.getLogger(__name__)
_DEFAULT_TARGET_CONTEXT_LABELS: dict[str, tuple[str, ...]] = {
    "vol_regime": ("low", "high"),
}


def _normalize_key(value: Any) -> str:
    return str(value or "").strip()


def _load_json_object(value: Any, *, source: str = "context_json") -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    try:
        if pd.isna(value):
            return {}
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        raw = value.strip()
        if not raw or raw == "{}":
            return {}
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DataIntegrityError(
                f"Malformed planner memory {source}: expected JSON object"
            ) from exc
        if not isinstance(payload, dict):
            raise DataIntegrityError(
                f"Malformed planner memory {source}: expected JSON object, got {type(payload).__name__}"
            )
        return payload
    raise DataIntegrityError(
        f"Malformed planner memory {source}: expected JSON object, got {type(value).__name__}"
    )


def _load_contexts(value: Any, *, source: str = "context_json") -> dict[str, list[str]]:
    payload = _load_json_object(value, source=source)
    out: dict[str, list[str]] = {}
    for key, raw in payload.items():
        family = _normalize_key(key)
        if not family:
            continue
        if isinstance(raw, (list, tuple, set)):
            labels = [str(item).strip() for item in raw if str(item).strip()]
        else:
            labels = [str(raw).strip()] if str(raw).strip() else []
        if labels:
            out[family] = labels
    return out


def _family_from_event_type(event_type: str, registry_events: dict[str, Any]) -> str:
    meta = registry_events.get(event_type, {})
    family = str(meta.get("family", "")).strip()
    if family:
        return family
    spec = EVENT_REGISTRY_SPECS.get(event_type)
    if spec is not None:
        return str(getattr(spec, "family", "") or "")
    return ""


def _allowed_templates_for_family(family: str, registry_templates: dict[str, Any]) -> list[str]:
    families = (
        registry_templates.get("families", {}) if isinstance(registry_templates, dict) else {}
    )
    meta = families.get(family, {}) if isinstance(families, dict) else {}
    allowed = meta.get("allowed_templates", []) if isinstance(meta, dict) else []
    if isinstance(allowed, str):
        allowed = [allowed]
    out = [str(value).strip() for value in allowed if str(value).strip()]
    return out or ["mean_reversion", "continuation"]


def _search_space_path(registry_root: Path, override: Path | None = None) -> Path:
    if override is not None:
        return override
    candidates = [
        Path("spec/search_space.yaml"),
        registry_root.parent.parent / "spec" / "search_space.yaml",
    ]
    return next((candidate for candidate in candidates if candidate.exists()), candidates[0])


def _tested_region_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    for column in ["event_type", "template_id", "direction", "horizon", "entry_lag", "region_key"]:
        if column not in out.columns:
            out[column] = ""
    for column, default in [
        ("failure_cause_class", ""),
        ("failure_confidence", 0.0),
        ("failure_sample_size", 0),
    ]:
        if column not in out.columns:
            out[column] = default
    return out


def _count_contexts_by_event(tested_regions: pd.DataFrame) -> dict[str, dict[str, Counter]]:
    counts: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
    if (
        tested_regions.empty
        or "event_type" not in tested_regions.columns
        or "context_json" not in tested_regions.columns
    ):
        return counts
    for row_number, row in enumerate(
        tested_regions[["event_type", "context_json"]].to_dict(orient="records")
    ):
        event_type = str(row.get("event_type", "")).strip()
        if not event_type:
            continue
        contexts = _load_contexts(
            row.get("context_json"),
            source=f"tested_regions.context_json[row={row_number},event_type={event_type}]",
        )
        for family, labels in contexts.items():
            for label in labels:
                counts[event_type][family][label] += 1
    return counts


def _regime_gap_for_event(
    *,
    event_type: str,
    event_count: int,
    event_context_counts: dict[str, dict[str, Counter]],
    target_contexts: Sequence[str],
    threshold: int,
) -> tuple[float, dict[str, list[str]], dict[str, Any]]:
    if event_count <= 0:
        return 0.0, {}, {"event_type": event_type, "undercovered_contexts": {}}

    score = 0.0
    context_payload: dict[str, list[str]] = {}
    undercovered: dict[str, Any] = {}
    per_event_counts = event_context_counts.get(event_type, {})
    resolved_threshold = max(int(threshold), 0)

    for context_name in target_contexts:
        counts = Counter(per_event_counts.get(context_name, Counter()))
        expected_labels = list(_DEFAULT_TARGET_CONTEXT_LABELS.get(context_name, ()))
        for label in counts:
            if label not in expected_labels:
                expected_labels.append(label)
        if not expected_labels:
            continue
        for label in expected_labels:
            counts.setdefault(label, 0)
        min_count = min(int(value) for value in counts.values())
        context_score = (
            1.0
            if resolved_threshold == 0 and min_count == 0
            else max(0.0, 1.0 - (min_count / max(resolved_threshold, 1)))
        )
        if context_score <= 0.0:
            continue
        ordered_labels = expected_labels + [
            label for label in sorted(counts) if label not in set(expected_labels)
        ]
        low_labels = [label for label in ordered_labels if int(counts[label]) == min_count]
        if low_labels:
            context_payload[context_name] = low_labels[:2]
            score = max(score, context_score)
            undercovered[context_name] = {
                "labels": low_labels[:2],
                "counts": {label: int(counts[label]) for label in sorted(counts)},
                "min_count": int(min_count),
                "threshold": int(resolved_threshold),
                "score": float(context_score),
            }

    return (
        score,
        context_payload,
        {
            "event_type": event_type,
            "undercovered_contexts": undercovered,
        },
    )


def _family_counts(tested_regions: pd.DataFrame, event_to_family: dict[str, str]) -> Counter:
    counts: Counter = Counter()
    if tested_regions.empty or "event_type" not in tested_regions.columns:
        return counts
    for event_type in tested_regions["event_type"].astype(str).tolist():
        family = event_to_family.get(event_type, "")
        if family:
            counts[family] += 1
    return counts


def _event_counts(tested_regions: pd.DataFrame) -> Counter:
    counts: Counter = Counter()
    if tested_regions.empty or "event_type" not in tested_regions.columns:
        return counts
    counts.update(tested_regions["event_type"].astype(str).tolist())
    return counts


def _mechanical_exclusions(tested_regions: pd.DataFrame) -> set[str]:
    if tested_regions.empty or "failure_cause_class" not in tested_regions.columns:
        return set()
    mechanical = tested_regions[
        tested_regions["failure_cause_class"].astype(str).str.strip().str.lower() == "mechanical"
    ]
    if "region_key" not in mechanical.columns:
        return set()
    return set(mechanical["region_key"].astype(str).tolist())


_FAILURE_CLASS_PENALTY_WEIGHTS = {
    "mechanical": 3.0,
    "market": 0.95,
    "cost": 0.8,
    "overfitting": 1.15,
    "trust_degraded": 1.6,
    "requires_revalidation": 2.2,
    "exhausted": 1.4,
    "insufficient_sample": 0.25,
    "regime_shift": 0.1,
    "decay": 0.1,
}

_RETEST_WORTHY_FAILURE_CLASSES = frozenset(
    {
        "insufficient_sample",
        "regime_shift",
        "decay",
        "retest_worthy",
    }
)

_TRUST_DEGRADED_FAILURE_CLASSES = frozenset(
    {
        "trust_degraded",
        "degraded_artifact",
        "requires_revalidation",
    }
)


def _empty_failure_penalty() -> dict[str, float]:
    return {
        "total_penalty": 0.0,
        "mechanical": 0.0,
        "market": 0.0,
        "insufficient_sample": 0.0,
        "cost": 0.0,
        "overfitting": 0.0,
        "trust_degraded": 0.0,
        "requires_revalidation": 0.0,
        "exhausted": 0.0,
        "retest_bonus": 0.0,
    }


def _failure_penalty_components(tested_regions: pd.DataFrame) -> dict[str, dict[str, float]]:
    penalties: dict[str, dict[str, float]] = {}
    if tested_regions.empty or "failure_cause_class" not in tested_regions.columns:
        return penalties
    for event_type, frame in tested_regions.groupby("event_type", dropna=False):
        event_key = str(event_type)
        if not event_key:
            continue
        classes = frame["failure_cause_class"].astype(str).str.strip().str.lower()
        if classes.empty:
            continue
        components = _empty_failure_penalty()
        for class_name in (
            "mechanical",
            "market",
            "insufficient_sample",
            "cost",
            "overfitting",
            "requires_revalidation",
            "exhausted",
        ):
            share = float((classes == class_name).mean())
            components[class_name] = share * _FAILURE_CLASS_PENALTY_WEIGHTS[class_name]
        trust_share = float(classes.isin(_TRUST_DEGRADED_FAILURE_CLASSES).mean())
        components["trust_degraded"] = (
            trust_share * _FAILURE_CLASS_PENALTY_WEIGHTS["trust_degraded"]
        )
        retest_share = float(classes.isin(_RETEST_WORTHY_FAILURE_CLASSES).mean())
        components["retest_bonus"] = 0.15 * retest_share
        components["total_penalty"] = max(
            0.0,
            sum(
                value
                for key, value in components.items()
                if key not in {"total_penalty", "retest_bonus"}
            )
            - components["retest_bonus"],
        )
        penalties[event_key] = components
    return penalties


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        numeric = float(value)
        if math.isnan(numeric) or math.isinf(numeric):
            return float(default)
        return numeric
    except Exception:
        return float(default)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _empty_economics_signal() -> dict[str, Any]:
    return {
        "score": 0.0,
        "evidence_weight": 0.0,
        "times_evaluated": 0,
        "times_promoted": 0,
        "promotion_rate": 0.0,
        "avg_after_cost_expectancy": 0.0,
        "median_after_cost_expectancy": 0.0,
        "recent_after_cost_expectancy": 0.0,
        "avg_stressed_after_cost_expectancy": 0.0,
        "median_stressed_after_cost_expectancy": 0.0,
        "recent_stressed_after_cost_expectancy": 0.0,
        "positive_after_cost_rate": 0.0,
        "positive_stressed_after_cost_rate": 0.0,
        "tradable_rate": 0.0,
        "statistical_pass_rate": 0.0,
        "avg_q_value": 0.0,
        "dominant_fail_gate": "",
        "expectancy_component": 0.0,
        "stressed_component": 0.0,
        "positive_rate_component": 0.0,
        "stressed_positive_rate_component": 0.0,
        "tradability_component": 0.0,
        "statistical_component": 0.0,
        "promotion_component": 0.0,
        "quality_component": 0.0,
        "cost_drag": 0.0,
        "status": "unknown",
    }


def _is_cost_fail_gate(value: Any) -> bool:
    token = str(value or "").strip().lower()
    if not token:
        return False
    return ("cost" in token) or ("after_cost" in token) or token in {
        "gate_after_cost_positive",
        "gate_after_cost_stressed_positive",
    }


def _bounded_rate(value: Any, default: float = 0.0) -> float:
    numeric = _coerce_float(value, default)
    if not math.isfinite(numeric):
        return float(default)
    return float(min(max(numeric, 0.0), 1.0))


def _event_economics_signals(event_statistics: pd.DataFrame) -> dict[str, dict[str, Any]]:
    signals: dict[str, dict[str, Any]] = {}
    if event_statistics.empty or "event_type" not in event_statistics.columns:
        return signals
    for row in event_statistics.to_dict(orient="records"):
        event_type = str(row.get("event_type", "")).strip()
        if not event_type:
            continue
        times_evaluated = max(_coerce_int(row.get("times_evaluated"), 0), 0)
        times_promoted = max(_coerce_int(row.get("times_promoted"), 0), 0)
        avg_after_cost = _coerce_float(row.get("avg_after_cost_expectancy"), 0.0)
        median_after_cost = _coerce_float(row.get("median_after_cost_expectancy"), avg_after_cost)
        recent_after_cost = _coerce_float(row.get("recent_after_cost_expectancy"), median_after_cost)
        avg_stressed_after_cost = _coerce_float(
            row.get("avg_stressed_after_cost_expectancy"),
            avg_after_cost,
        )
        median_stressed_after_cost = _coerce_float(
            row.get("median_stressed_after_cost_expectancy"),
            avg_stressed_after_cost,
        )
        recent_stressed_after_cost = _coerce_float(
            row.get("recent_stressed_after_cost_expectancy"),
            median_stressed_after_cost,
        )
        positive_after_cost_rate = _bounded_rate(
            row.get("positive_after_cost_rate"),
            1.0 if avg_after_cost > 0.0 else 0.0,
        )
        positive_stressed_after_cost_rate = _bounded_rate(
            row.get("positive_stressed_after_cost_rate"),
            1.0 if median_stressed_after_cost > 0.0 else 0.0,
        )
        tradable_rate = _bounded_rate(row.get("tradable_rate"), 0.0)
        statistical_pass_rate = _bounded_rate(row.get("statistical_pass_rate"), 0.0)
        avg_q = _coerce_float(row.get("avg_q_value"), 0.5)
        dominant_fail_gate = str(row.get("dominant_fail_gate", "") or "").strip()

        evidence_weight = min(times_evaluated / 5.0, 1.0) if times_evaluated > 0 else 0.0
        promotion_rate = min(times_promoted / max(times_evaluated, 1), 1.0)
        blended_expectancy = (0.35 * avg_after_cost) + (0.30 * median_after_cost) + (0.35 * recent_after_cost)
        blended_stressed = (0.25 * avg_stressed_after_cost) + (0.35 * median_stressed_after_cost) + (0.40 * recent_stressed_after_cost)
        expectancy_component = 0.95 * math.tanh(blended_expectancy / 4.0) * evidence_weight
        stressed_component = 0.90 * math.tanh(blended_stressed / 4.0) * evidence_weight
        positive_rate_component = 0.35 * ((positive_after_cost_rate - 0.5) * 2.0) * evidence_weight
        stressed_positive_rate_component = 0.55 * ((positive_stressed_after_cost_rate - 0.5) * 2.0) * evidence_weight
        tradability_component = 0.25 * ((tradable_rate - 0.5) * 2.0) * evidence_weight
        statistical_component = 0.25 * ((statistical_pass_rate - 0.5) * 2.0) * evidence_weight
        promotion_component = 0.55 * promotion_rate * evidence_weight
        quality_edge = max(-1.0, min((0.35 - avg_q) / 0.35, 1.0))
        quality_component = 0.30 * quality_edge * evidence_weight
        cost_drag = 0.45 * evidence_weight if _is_cost_fail_gate(dominant_fail_gate) else 0.0
        if blended_stressed <= 0.0:
            cost_drag += 0.20 * evidence_weight
        score = (
            expectancy_component
            + stressed_component
            + positive_rate_component
            + stressed_positive_rate_component
            + tradability_component
            + statistical_component
            + promotion_component
            + quality_component
            - cost_drag
        )
        status = "positive" if score > 0.15 else "negative" if score < -0.15 else "neutral"
        signals[event_type] = {
            "score": float(score),
            "evidence_weight": float(evidence_weight),
            "times_evaluated": int(times_evaluated),
            "times_promoted": int(times_promoted),
            "promotion_rate": float(promotion_rate),
            "avg_after_cost_expectancy": float(avg_after_cost),
            "median_after_cost_expectancy": float(median_after_cost),
            "recent_after_cost_expectancy": float(recent_after_cost),
            "avg_stressed_after_cost_expectancy": float(avg_stressed_after_cost),
            "median_stressed_after_cost_expectancy": float(median_stressed_after_cost),
            "recent_stressed_after_cost_expectancy": float(recent_stressed_after_cost),
            "positive_after_cost_rate": float(positive_after_cost_rate),
            "positive_stressed_after_cost_rate": float(positive_stressed_after_cost_rate),
            "tradable_rate": float(tradable_rate),
            "statistical_pass_rate": float(statistical_pass_rate),
            "avg_q_value": float(avg_q),
            "dominant_fail_gate": dominant_fail_gate,
            "expectancy_component": float(expectancy_component),
            "stressed_component": float(stressed_component),
            "positive_rate_component": float(positive_rate_component),
            "stressed_positive_rate_component": float(stressed_positive_rate_component),
            "tradability_component": float(tradability_component),
            "statistical_component": float(statistical_component),
            "promotion_component": float(promotion_component),
            "quality_component": float(quality_component),
            "cost_drag": float(cost_drag),
            "status": status,
        }
    return signals


def _normalize_horizon(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token[:-1] if token.endswith("b") and token[:-1].isdigit() else token


def _normalize_entry_lag(value: Any) -> str:
    try:
        return str(int(float(str(value).strip())))
    except Exception:
        return str(value or "").strip()


def _tested_scope_keys(
    tested_regions: pd.DataFrame,
) -> set[tuple[str, str, str, str, str, str]]:
    if tested_regions.empty:
        return set()
    required = {
        "event_type",
        "template_id",
        "direction",
        "horizon",
        "entry_lag",
        "context_json",
    }
    if not required.issubset(set(tested_regions.columns)):
        return set()
    keys: set[tuple[str, str, str, str, str, str]] = set()
    for row_number, row in enumerate(tested_regions.to_dict(orient="records")):
        event_type = str(row.get("event_type", "")).strip()
        template_id = str(row.get("template_id", "")).strip()
        direction = str(row.get("direction", "")).strip()
        horizon = _normalize_horizon(row.get("horizon"))
        entry_lag = _normalize_entry_lag(row.get("entry_lag"))
        contexts = _load_contexts(
            row.get("context_json"),
            source=f"tested_regions.context_json[row={row_number},event_type={event_type}]",
        )
        context_json = canonical_json(contexts)
        if event_type and template_id and direction and horizon and entry_lag:
            keys.add((event_type, template_id, direction, horizon, entry_lag, context_json))
    return keys


def _proposal_scope_keys(
    *,
    event_type: str,
    templates: Sequence[str],
    directions: Sequence[str],
    horizons: Sequence[int],
    entry_lags: Sequence[int],
    contexts: dict[str, list[str]],
) -> set[tuple[str, str, str, str, str, str]]:
    context_json = canonical_json(contexts)
    return {
        (
            str(event_type).strip(),
            str(template).strip(),
            str(direction).strip(),
            _normalize_horizon(horizon),
            _normalize_entry_lag(entry_lag),
            context_json,
        )
        for template in templates
        for direction in directions
        for horizon in horizons
        for entry_lag in entry_lags
        if str(template).strip() and str(direction).strip()
    }


def _region_keys_for_scope_keys(
    *,
    program_id: str,
    symbols: Sequence[str],
    scope_keys: Sequence[tuple[str, str, str, str, str, str]],
) -> list[str]:
    symbol_scope = ",".join(
        str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
    )
    return [
        region_key(
            {
                "program_id": program_id,
                "symbol_scope": symbol_scope,
                "event_type": event_type,
                "trigger_type": "EVENT",
                "template_id": template_id,
                "direction": direction,
                "horizon": horizon,
                "entry_lag": entry_lag,
                "context_hash": context_json,
            }
        )
        for event_type, template_id, direction, horizon, entry_lag, context_json in scope_keys
    ]


def _dominant_score_factors(
    components: dict[str, float],
    *,
    positive: bool,
    limit: int = 3,
) -> list[dict[str, float]]:
    rows: list[tuple[str, float]] = []
    for key, raw_value in components.items():
        value = float(raw_value or 0.0)
        if (positive and value > 0) or (not positive and value < 0):
            rows.append((key, value))
    rows.sort(key=lambda item: abs(item[1]), reverse=True)
    return [{"factor": key, "contribution": value} for key, value in rows[:limit]]


def _build_selection_rationale(
    ranked: Sequence[PlannedCampaignProposal],
) -> dict[str, Any]:
    if not ranked:
        return {}
    selected = ranked[0]
    selected_components = dict(selected.rationale.get("score_components", {}))
    runner_up = ranked[1] if len(ranked) > 1 else None
    score_margin = float(selected.score - runner_up.score) if runner_up is not None else None
    payload: dict[str, Any] = {
        "selected_event_type": selected.event_type,
        "selected_score": float(selected.score),
        "dominant_positive_factors": _dominant_score_factors(
            selected_components,
            positive=True,
        ),
        "dominant_penalties": _dominant_score_factors(
            selected_components,
            positive=False,
        ),
        "runner_up_event_type": runner_up.event_type if runner_up is not None else "",
        "runner_up_score": float(runner_up.score) if runner_up is not None else None,
        "score_margin": score_margin,
    }
    if runner_up is not None:
        runner_components = dict(runner_up.rationale.get("score_components", {}))
        payload["runner_up_dominant_penalties"] = _dominant_score_factors(
            runner_components,
            positive=False,
        )
    return payload


def _default_date_scope(lookback_days: int) -> tuple[str, str]:
    end = datetime.now(UTC).date()
    start = end - timedelta(days=int(lookback_days))
    return start.isoformat(), end.isoformat()


@dataclass(frozen=True)
class CampaignPlannerConfig:
    program_id: str
    registry_root: Path
    data_root: Path | None = None
    search_space_path: Path | None = None
    symbols: tuple[str, ...] = ("BTCUSDT",)
    instrument_classes: tuple[str, ...] = ("crypto",)
    timeframe: str = "5m"
    lookback_days: int = 90
    horizon_bars: tuple[int, ...] = (12, 24)
    entry_lags: tuple[int, ...] = (1,)
    directions: tuple[str, ...] = ("long", "short")
    templates: tuple[str, ...] = ()
    max_proposals: int = 10
    regime_gap_threshold: int = 5
    min_region_test_count: int = 0
    objective_name: str = "retail_profitability"
    promotion_profile: str = "research"
    run_mode: str = "research"
    target_contexts: tuple[str, ...] = ("vol_regime",)
    enabled_trigger_types: tuple[str, ...] = ("EVENT",)
    event_tiers: tuple[str, ...] = ("A", "B")
    operational_roles: tuple[str, ...] = ("trigger", "confirm")


@dataclass
class PlannedCampaignProposal:
    score: float
    event_type: str
    family: str
    rationale: dict[str, Any]
    proposal: dict[str, Any]


@dataclass
class CampaignPlanResult:
    program_id: str
    ranked_proposals: list[PlannedCampaignProposal] = field(default_factory=list)
    excluded_region_keys: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "program_id": self.program_id,
            "ranked_proposals": [
                {
                    "score": item.score,
                    "event_type": item.event_type,
                    "family": item.family,
                    "rationale": item.rationale,
                    "proposal": item.proposal,
                }
                for item in self.ranked_proposals
            ],
            "excluded_region_keys": list(self.excluded_region_keys),
            "summary": dict(self.summary),
        }


class CampaignPlanner:
    def __init__(self, config: CampaignPlannerConfig):
        self.config = config
        self.data_root = Path(config.data_root) if config.data_root is not None else get_data_root()
        self.registry_root = Path(config.registry_root)
        self.paths = ensure_memory_store(config.program_id, data_root=self.data_root)
        self.search_space_path = _search_space_path(self.registry_root, config.search_space_path)
        semantic_registry = build_canonical_semantic_registry_views()
        self.registry = {
            "events": semantic_registry["events"],
            "templates": semantic_registry["templates"],
            "search_limits": self._load_yaml(self.registry_root / "search_limits.yaml"),
        }
        self.event_weights = self._event_priority_weights(self.search_space_path)
        self._last_duplicate_excluded_region_keys: set[str] = set()
        self._last_duplicate_exclusion_details: list[dict[str, Any]] = []
        self._last_surface_viability_summary: dict[str, Any] = {}

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            _LOG.warning("Failed to load YAML from %s", path, exc_info=True)
            return {}

    def _event_priority_weights(self, search_space_path: Path | None) -> dict[str, float]:
        try:
            return load_event_priority_weights(search_space_path)
        except Exception:
            return {}

    def _feature_surface_viability(self, event_types: Sequence[str]) -> dict[str, Any]:
        requested = sorted({str(event).strip().upper() for event in event_types if str(event).strip()})
        if not requested:
            return {"status": "unknown", "detectors": {}}
        start, end = _default_date_scope(self.config.lookback_days)
        try:
            return analyze_feature_surface_viability(
                data_root=self.data_root,
                run_id="campaign_planner_preflight",
                symbols=self.config.symbols,
                timeframe=self.config.timeframe,
                start=start,
                end=end,
                event_types=requested,
            )
        except Exception:
            _LOG.warning("Campaign planner feature-surface viability analysis failed", exc_info=True)
            return {
                "status": "unknown",
                "event_types": requested,
                "detectors": {},
                "issues": ["feature_surface_viability_failed"],
            }

    def _memory(self) -> dict[str, pd.DataFrame]:
        return {
            "tested_regions": _tested_region_columns(
                read_memory_table(
                    self.config.program_id, "tested_regions", data_root=self.data_root
                )
            ),
            "failures": read_memory_table(
                self.config.program_id, "failures", data_root=self.data_root
            ),
            "region_statistics": read_memory_table(
                self.config.program_id, "region_statistics", data_root=self.data_root
            ),
            "event_statistics": read_memory_table(
                self.config.program_id, "event_statistics", data_root=self.data_root
            ),
            "template_statistics": read_memory_table(
                self.config.program_id, "template_statistics", data_root=self.data_root
            ),
            "context_statistics": read_memory_table(
                self.config.program_id, "context_statistics", data_root=self.data_root
            ),
            "reflections": read_memory_table(
                self.config.program_id, "reflections", data_root=self.data_root
            ),
        }

    def _candidate_events(
        self,
        tested_regions: pd.DataFrame,
        *,
        event_statistics: pd.DataFrame | None = None,
    ) -> list[dict[str, Any]]:
        events_registry = self.registry.get("events", {}).get("events", {})
        event_to_family = {
            event_type: _family_from_event_type(event_type, events_registry)
            for event_type in events_registry
        }
        event_counts = _event_counts(tested_regions)
        family_counts = _family_counts(tested_regions, event_to_family)
        event_context_counts = _count_contexts_by_event(tested_regions)
        mechanical_region_keys = _mechanical_exclusions(tested_regions)
        penalties = _failure_penalty_components(tested_regions)
        tested_scope_keys = _tested_scope_keys(tested_regions)
        economics = _event_economics_signals(event_statistics if event_statistics is not None else pd.DataFrame())
        weights = self.event_weights
        max_weight = max(weights.values(), default=DEFAULT_EVENT_PRIORITY_WEIGHT)
        candidates: list[dict[str, Any]] = []
        self._last_duplicate_excluded_region_keys = set()
        self._last_duplicate_exclusion_details = []

        eligible_events: list[tuple[str, dict[str, Any], dict[str, Any], str, int, int]] = []
        for event_type, meta in events_registry.items():
            if not bool(meta.get("enabled", True)):
                continue
            if not event_matches_filters(
                event_type,
                tiers=self.config.event_tiers,
                roles=self.config.operational_roles,
                trade_trigger_eligible=True,
            ):
                continue
            governance = get_event_governance_metadata(event_type)
            family = event_to_family.get(event_type, "")
            event_count = int(event_counts.get(event_type, 0))
            family_count = int(family_counts.get(family, 0))
            if (
                self.config.min_region_test_count > 0
                and event_count >= self.config.min_region_test_count
            ):
                continue
            eligible_events.append((event_type, meta, governance, family, event_count, family_count))

        viability_report = self._feature_surface_viability([row[0] for row in eligible_events])
        blocked_events: list[dict[str, Any]] = []
        warn_events: list[dict[str, Any]] = []

        for event_type, _meta, governance, family, event_count, family_count in eligible_events:
            viability = summarize_viability_for_event(viability_report, event_type)
            if viability["status"] == "block":
                blocked_events.append({"event_type": event_type, **viability})
                continue
            if viability["status"] == "warn":
                warn_events.append({"event_type": event_type, **viability})

            weight = float(weights.get(event_type, DEFAULT_EVENT_PRIORITY_WEIGHT))
            priority_score = weight / max_weight if max_weight > 0 else 0.5
            family_gap_score = 1.0 / (1.0 + family_count)
            event_gap_score = 1.0 / (1.0 + event_count)

            regime_score, context_payload, regime_gap = _regime_gap_for_event(
                event_type=event_type,
                event_count=event_count,
                event_context_counts=event_context_counts,
                target_contexts=self.config.target_contexts,
                threshold=self.config.regime_gap_threshold,
            )

            if not context_payload and "vol_regime" in self.config.target_contexts:
                context_payload = {"vol_regime": ["low", "high"]}

            failure_penalty = penalties.get(event_type, _empty_failure_penalty())
            history_penalty = float(failure_penalty.get("total_penalty", 0.0))
            economics_signal = economics.get(event_type, _empty_economics_signal())
            maturity_bonus = 0.4 if governance["tier"] == "A" else 0.15
            governance_penalty = float(governance.get("rank_penalty", 0.0)) * 0.35
            score_components = {
                "priority_score": 1.8 * priority_score,
                "family_gap_score": 1.2 * family_gap_score,
                "event_gap_score": 0.9 * event_gap_score,
                "regime_score": 0.8 * regime_score,
                "maturity_bonus": maturity_bonus,
                "economics_score": 1.1 * float(economics_signal.get("score", 0.0)),
                "history_penalty": -history_penalty,
                "governance_penalty": -governance_penalty,
            }
            score = sum(score_components.values())

            templates = _allowed_templates_for_family(family, self.registry.get("templates", {}))
            proposal_scope_keys = _proposal_scope_keys(
                event_type=event_type,
                templates=templates,
                directions=self.config.directions,
                horizons=self.config.horizon_bars,
                entry_lags=self.config.entry_lags,
                contexts=context_payload,
            )
            if proposal_scope_keys and proposal_scope_keys.issubset(tested_scope_keys):
                excluded_keys = _region_keys_for_scope_keys(
                    program_id=self.config.program_id,
                    symbols=self.config.symbols,
                    scope_keys=sorted(proposal_scope_keys),
                )
                self._last_duplicate_excluded_region_keys.update(excluded_keys)
                self._last_duplicate_exclusion_details.append(
                    {
                        "event_type": event_type,
                        "reason": "all_proposed_scope_combinations_already_tested",
                        "tested_scope_count": len(proposal_scope_keys),
                        "excluded_region_keys": excluded_keys[:10],
                    }
                )
                continue
            proposal = self._build_proposal_payload(
                event_type=event_type,
                family=family,
                templates=templates,
                contexts=context_payload,
                score=score,
                weight=weight,
                event_count=event_count,
                family_count=family_count,
                regime_score=regime_score,
                priority_score=priority_score,
                family_gap_score=family_gap_score,
                event_gap_score=event_gap_score,
                history_penalty=history_penalty,
                failure_penalty=failure_penalty,
                economics_signal=economics_signal,
                score_components=score_components,
                regime_gap=regime_gap,
                governance=governance,
                excluded_region_keys=mechanical_region_keys,
                viability=viability,
            )
            if proposal is not None:
                candidates.append(proposal)

        self._last_surface_viability_summary = {
            "status": str(viability_report.get("status", "unknown") or "unknown"),
            "blocked_events": blocked_events,
            "warn_events": warn_events,
        }
        candidates.sort(key=lambda item: item["score"], reverse=True)
        return candidates

    def _build_proposal_payload(
        self,
        *,
        event_type: str,
        family: str,
        templates: Sequence[str],
        contexts: dict[str, list[str]],
        score: float,
        weight: float,
        event_count: int,
        family_count: int,
        regime_score: float,
        priority_score: float,
        family_gap_score: float,
        event_gap_score: float,
        history_penalty: float,
        failure_penalty: dict[str, float],
        economics_signal: dict[str, Any],
        score_components: dict[str, float],
        regime_gap: dict[str, Any],
        governance: dict[str, Any],
        excluded_region_keys: set[str] | None = None,
        viability: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if score <= -10.0:
            return None
        start, end = _default_date_scope(self.config.lookback_days)
        resolved_controls = resolve_generated_proposal_controls(
            templates=templates,
            horizons_bars=self.config.horizon_bars,
            directions=self.config.directions,
            entry_lags=self.config.entry_lags,
            promotion_profile=self.config.promotion_profile,
            run_mode=self.config.run_mode,
        )
        proposal = {
            "program_id": self.config.program_id,
            "start": start,
            "end": end,
            "symbols": list(self.config.symbols),
            "trigger_space": {
                "allowed_trigger_types": list(self.config.enabled_trigger_types),
                "events": {"include": [event_type]},
                "tiers": list(self.config.event_tiers),
                "operational_roles": list(self.config.operational_roles),
                "deployment_dispositions": [
                    str(governance.get("deployment_disposition", "")).strip()
                ]
                if str(governance.get("deployment_disposition", "")).strip()
                else [],
            },
            "templates": list(dict.fromkeys(templates))[:4],
            "description": f"Autonomous campaign proposal for {event_type}"
            + (f" (family={family})" if family else ""),
            "run_mode": self.config.run_mode,
            "objective_name": self.config.objective_name,
            "promotion_profile": self.config.promotion_profile,
            "timeframe": self.config.timeframe,
            "instrument_classes": list(self.config.instrument_classes),
            "horizons_bars": list(self.config.horizon_bars),
            "directions": list(self.config.directions),
            "entry_lags": list(self.config.entry_lags),
            "contexts": contexts,
            "discovery_profile": resolved_controls["discovery_profile"],
            "phase2_gate_profile": resolved_controls["phase2_gate_profile"],
            "search_spec": resolved_controls["search_spec"],
            "search_control": {
                "max_hypotheses_total": 1000,
                "max_hypotheses_per_template": 500,
                "max_hypotheses_per_event_family": 500,
                "random_seed": 42,
            },
            "artifacts": {
                "campaign_memory": True,
                "proposal_audit": True,
                "search_frontier": True,
            },
            "knobs": {},
        }
        rationale = {
            "surface_viability": dict(viability or {}),
            "event_weight": weight,
            "priority_score": priority_score,
            "family_gap_score": family_gap_score,
            "event_gap_score": event_gap_score,
            "regime_score": regime_score,
            "regime_gap": dict(regime_gap),
            "history_penalty": history_penalty,
            "failure_penalty": dict(failure_penalty),
            "economics_signal": dict(economics_signal),
            "score_components": dict(score_components),
            "event_count": event_count,
            "family_count": family_count,
            "governance": {
                "tier": governance.get("tier", ""),
                "operational_role": governance.get("operational_role", ""),
                "deployment_disposition": governance.get("deployment_disposition", ""),
                "evidence_mode": governance.get("evidence_mode", ""),
                "trade_trigger_eligible": bool(governance.get("trade_trigger_eligible", False)),
            },
        }
        proposal_key = region_key(
            {
                "program_id": proposal["program_id"],
                "symbol_scope": ",".join(proposal["symbols"]),
                "event_type": event_type,
                "trigger_type": "EVENT",
                "template_id": ",".join(proposal["templates"]),
                "direction": ",".join(proposal["directions"]),
                "horizon": ",".join(str(value) for value in proposal["horizons_bars"]),
                "entry_lag": ",".join(str(value) for value in proposal["entry_lags"]),
                "context_hash": canonical_json(contexts),
            }
        )
        rationale["proposal_region_key"] = proposal_key
        if excluded_region_keys and proposal_key in excluded_region_keys:
            return None
        return {
            "score": float(score),
            "event_type": event_type,
            "family": family,
            "rationale": rationale,
            "proposal": proposal,
        }

    def plan(self) -> CampaignPlanResult:
        memory = self._memory()
        tested_regions = memory["tested_regions"]
        candidate_rows = self._candidate_events(
            tested_regions,
            event_statistics=memory.get("event_statistics"),
        )
        ranked = [
            PlannedCampaignProposal(
                score=float(row["score"]),
                event_type=str(row["event_type"]),
                family=str(row["family"]),
                rationale=dict(row["rationale"]),
                proposal=dict(row["proposal"]),
            )
            for row in candidate_rows[: self.config.max_proposals]
        ]
        summary = {
            "tested_regions": len(tested_regions),
            "candidate_pool": len(candidate_rows),
            "surface_blocked_events": list(self._last_surface_viability_summary.get("blocked_events", [])),
            "surface_warn_events": list(self._last_surface_viability_summary.get("warn_events", [])),
            "top_event_type": ranked[0].event_type if ranked else "",
            "top_family": ranked[0].family if ranked else "",
            "search_space_path": str(self.search_space_path),
            "duplicate_region_exclusions": len(self._last_duplicate_exclusion_details),
            "duplicate_exclusion_reasons": list(self._last_duplicate_exclusion_details[:5]),
            "selection_rationale": _build_selection_rationale(ranked),
        }
        return CampaignPlanResult(
            program_id=self.config.program_id,
            ranked_proposals=ranked,
            excluded_region_keys=sorted(
                self._excluded_region_keys(tested_regions)
                | self._last_duplicate_excluded_region_keys
            ),
            summary=summary,
        )

    def _excluded_region_keys(self, tested_regions: pd.DataFrame) -> set[str]:
        if tested_regions.empty or "failure_cause_class" not in tested_regions.columns:
            return set()
        mask = (
            tested_regions["failure_cause_class"].astype(str).str.strip().str.lower()
            == "mechanical"
        )
        if not mask.any() or "region_key" not in tested_regions.columns:
            return set()
        return set(tested_regions.loc[mask, "region_key"].astype(str).tolist())

    def top_proposal(self) -> dict[str, Any] | None:
        plan = self.plan()
        if not plan.ranked_proposals:
            return None
        return plan.ranked_proposals[0].proposal


def run_campaign_planner_cycle(
    *,
    program_id: str,
    registry_root: Path,
    data_root: Path | None = None,
    search_space_path: Path | None = None,
    symbols: Sequence[str] = ("BTCUSDT",),
    plan_only: bool = False,
    dry_run: bool = False,
    check: bool = False,
    lookback_days: int = 90,
    max_proposals: int = 10,
) -> dict[str, Any]:
    planner = CampaignPlanner(
        CampaignPlannerConfig(
            program_id=program_id,
            registry_root=Path(registry_root),
            data_root=Path(data_root) if data_root is not None else None,
            search_space_path=Path(search_space_path) if search_space_path is not None else None,
            symbols=tuple(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()),
            lookback_days=int(lookback_days),
            max_proposals=int(max_proposals),
        )
    )
    plan = planner.plan()
    top = planner.top_proposal()
    if top is None:
        return {"plan": plan.to_dict(), "execution": None, "run_id": "", "proposal": None}

    proposal_path = planner.paths.proposals_dir / "planned_proposal.yaml"
    proposal_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(proposal_path, yaml.safe_dump(top, sort_keys=False))
    proposal = load_operator_proposal(proposal_path)
    run_id = generate_run_id(program_id, proposal.to_dict())
    execution = issue_proposal(
        proposal_path,
        registry_root=Path(registry_root),
        data_root=Path(data_root) if data_root is not None else None,
        run_id=run_id,
        plan_only=plan_only,
        dry_run=dry_run,
        check=check,
    )
    return {
        "plan": plan.to_dict(),
        "execution": execution,
        "run_id": run_id,
        "proposal": proposal.to_dict(),
        "proposal_path": str(proposal_path),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Score campaign memory and emit ranked proposals.")
    parser.add_argument("--program_id", required=True)
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--search_space_path", default=None)
    parser.add_argument("--symbols", default="BTCUSDT")
    parser.add_argument("--lookback_days", type=int, default=90)
    parser.add_argument("--max_proposals", type=int, default=10)
    parser.add_argument("--plan_only", type=int, default=0)
    parser.add_argument("--dry_run", type=int, default=0)
    parser.add_argument("--check", type=int, default=0)
    parser.add_argument("--execute", type=int, default=1)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    plan_only = bool(args.plan_only)
    execute = bool(args.execute)
    result = run_campaign_planner_cycle(
        program_id=args.program_id,
        registry_root=Path(args.registry_root),
        data_root=Path(args.data_root) if args.data_root else None,
        search_space_path=Path(args.search_space_path) if args.search_space_path else None,
        symbols=tuple(sym.strip().upper() for sym in str(args.symbols).split(",") if sym.strip()),
        lookback_days=int(args.lookback_days),
        max_proposals=int(args.max_proposals),
        plan_only=plan_only or not execute,
        dry_run=bool(args.dry_run),
        check=bool(args.check),
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    execution = result.get("execution") or {}
    if isinstance(execution, dict):
        nested = execution.get("execution")
        if isinstance(nested, dict):
            return int(nested.get("returncode", 0))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
