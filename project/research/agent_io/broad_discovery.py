"""
BroadDiscovery: Family-wide hypothesis testing with per-event attribution.

Enables running all events in a family simultaneously while maintaining
proper statistical interpretation:
- Within-family FDR correction via Benjamini-Hochberg
- Per-event attribution reports
- Family-level vs. event-specific effect detection
- Preserves "narrow interpretation" discipline without run-time constraints

The "narrow before broad" rule is enforced in interpretation, not execution.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.research.agent_io.campaign_planner import CampaignPlanner, CampaignPlannerConfig
from project.research.agent_io.issue_proposal import generate_run_id, issue_proposal
from project.research.knowledge.memory import (
    ensure_memory_store,
    read_memory_table,
    write_memory_table,
)
from project.research.semantic_registry_views import build_canonical_semantic_registry_views

_LOG = logging.getLogger(__name__)


@dataclass
class FamilyEventConfig:
    event_type: str
    weight: float
    priority_score: float
    tested_count: int


@dataclass
class EventAttribution:
    event_type: str
    family: str
    q_value: float
    raw_p_value: float
    is_significant: bool
    after_cost_expectancy: float
    robustness_score: float
    sample_size: int
    candidate_count: int
    promoted_count: int
    attribution_confidence: float
    effect_type: str
    effect_detail: str


@dataclass
class FamilyDiscoveryResult:
    family: str
    run_id: str
    timestamp: str
    events_tested: int
    events_significant: int
    events_with_family_effect: int
    events_with_specific_effect: int
    attributions: List[EventAttribution]
    family_level_q_value: float
    is_family_level_effect: bool
    summary: Dict[str, Any]
    errors: List[str] = field(default_factory=list)


def _load_family_events(
    family: str,
    registry_root: Path,
    event_weights: Dict[str, float],
    tested_counts: Dict[str, int],
) -> List[FamilyEventConfig]:
    """Load all events in a family with their configurations."""
    from project.events.governance import get_event_governance_metadata

    normalized = str(family).strip().upper()
    configs = []
    del registry_root
    payload = build_canonical_semantic_registry_views()["events"]
    events = payload.get("events", {}) if isinstance(payload, dict) else {}
    for event_type, spec in events.items():
        if not isinstance(spec, dict):
            continue
        if not bool(spec.get("enabled", False)):
            continue
        family_name = str(spec.get("family", "") or "").strip().upper()
        if family_name != normalized:
            continue
        governance = get_event_governance_metadata(str(event_type).strip().upper())
        if not bool(governance.get("trade_trigger_eligible", False)):
            continue
        weight = event_weights.get(event_type, 1.5)
        tested = tested_counts.get(event_type, 0)
        configs.append(
            FamilyEventConfig(
                event_type=str(event_type).strip().upper(),
                weight=weight,
                priority_score=weight,
                tested_count=tested,
            )
        )

    configs.sort(key=lambda x: x.priority_score, reverse=True)
    return configs


def _load_event_weights(registry_root: Path) -> Dict[str, float]:
    """Load event priority weights from search space."""
    try:
        from project.spec_registry.search_space import load_event_priority_weights

        search_space_path = registry_root.parent.parent / "spec" / "search_space.yaml"
        if search_space_path.exists():
            return load_event_priority_weights(search_space_path)
    except Exception as e:
        _LOG.warning("Failed to load event weights: %s", e)
    return {}


def _load_tested_counts(program_id: str, data_root: Path) -> Dict[str, int]:
    """Load tested counts per event from memory."""
    try:
        tested = read_memory_table(program_id, "tested_regions", data_root=data_root)
        if tested.empty:
            return {}
        counts: Dict[str, int] = {}
        for event_type, count in tested["event_type"].value_counts().items():
            counts[str(event_type)] = int(count)
        return counts
    except Exception:
        return {}


def _load_avoid_region_keys(
    *,
    program_id: str,
    data_root: Path,
    symbols: Sequence[str],
    event_types: Sequence[str],
    templates: Sequence[str],
    directions: Sequence[str],
    horizon_bars: Sequence[int],
    entry_lags: Sequence[int],
) -> List[str]:
    try:
        tested = read_memory_table(program_id, "tested_regions", data_root=data_root)
    except Exception:
        return []
    if tested.empty or "region_key" not in tested.columns:
        return []

    mask = pd.Series(True, index=tested.index)
    if "symbol_scope" in tested.columns:
        mask &= (
            tested["symbol_scope"]
            .astype(str)
            .str.upper()
            .isin({str(symbol).strip().upper() for symbol in symbols})
        )
    if "event_type" in tested.columns:
        mask &= (
            tested["event_type"]
            .astype(str)
            .str.upper()
            .isin({str(event_type).strip().upper() for event_type in event_types})
        )
    if "template_id" in tested.columns:
        mask &= (
            tested["template_id"]
            .astype(str)
            .isin({str(template).strip() for template in templates})
        )
    if "direction" in tested.columns:
        mask &= (
            tested["direction"]
            .astype(str)
            .isin({str(direction).strip() for direction in directions})
        )
    if "horizon" in tested.columns:
        requested_horizons = {
            token
            for horizon in horizon_bars
            for token in (str(horizon).strip(), f"{int(horizon)}b")
            if str(token).strip()
        }
        mask &= tested["horizon"].astype(str).isin(requested_horizons)
    if "entry_lag" in tested.columns:
        mask &= (
            pd.to_numeric(tested["entry_lag"], errors="coerce")
            .fillna(-1)
            .astype(int)
            .isin({int(entry_lag) for entry_lag in entry_lags})
        )

    return sorted(
        {
            str(value).strip()
            for value in tested.loc[mask, "region_key"].tolist()
            if str(value).strip()
        }
    )


def _benjamini_hochberg(p_values: List[float]) -> List[float]:
    """
    Apply Benjamini-Hochberg FDR correction.

    Returns adjusted q-values that control the false discovery rate.
    """
    n = len(p_values)
    if n == 0:
        return []
    if n == 1:
        return p_values[:]

    sorted_indices = np.argsort(p_values)
    sorted_p = np.array(p_values)[sorted_indices]

    adjusted = np.zeros(n)
    for i in range(n):
        rank = i + 1
        adjusted[sorted_indices[i]] = sorted_p[i] * n / rank

    adjusted = np.minimum(adjusted, 1.0)
    for i in range(n - 2, -1, -1):
        adjusted[sorted_indices[i]] = min(
            adjusted[sorted_indices[i]], adjusted[sorted_indices[i + 1]]
        )

    return adjusted.tolist()


def _detect_effect_type(
    event_q_values: Dict[str, float],
    event_expectancies: Dict[str, float],
    threshold: float = 0.10,
) -> tuple[bool, float]:
    """
    Detect if there's a family-level effect (all events show similar signal).

    A family-level effect is indicated when:
    1. Most events have low q-values (significant)
    2. The direction of effect is consistent across events

    Returns (is_family_level, family_level_q_value)
    """
    if not event_q_values:
        return False, 1.0

    q_values = list(event_q_values.values())
    expectancies = list(event_expectancies.values())

    n_significant = sum(1 for q in q_values if q < threshold)
    significant_ratio = n_significant / len(q_values) if q_values else 0

    if significant_ratio < 0.5:
        return False, min(q_values) if q_values else 1.0

    consistency_score = 0.0
    if len(expectancies) > 1:
        pos = sum(1 for e in expectancies if e > 0)
        consistency_score = max(pos, len(expectancies) - pos) / len(expectancies)

    if significant_ratio > 0.7 and consistency_score > 0.8:
        return True, np.median(q_values)

    return False, min(q_values) if q_values else 1.0


def _compute_attribution_confidence(
    q_value: float,
    sample_size: int,
    candidate_count: int,
    is_family_level: bool,
) -> float:
    """
    Compute confidence in event-specific attribution.

    Higher confidence when:
    - Lower q-value (stronger signal)
    - Larger sample size
    - More candidates tested
    - Not a family-level effect
    """
    q_conf = 1.0 - min(q_value, 1.0)
    sample_conf = min(sample_size / 1000.0, 1.0)
    candidate_conf = min(candidate_count / 100.0, 1.0)
    specificity_conf = 0.8 if not is_family_level else 0.3

    confidence = 0.4 * q_conf + 0.2 * sample_conf + 0.2 * candidate_conf + 0.2 * specificity_conf
    return round(confidence, 3)


def _build_broad_proposal(
    family: str,
    events: List[FamilyEventConfig],
    config: "BroadDiscoveryConfig",
) -> Dict[str, Any]:
    """Build a proposal that tests all events in a family."""
    start, end = _default_date_scope(config.lookback_days)
    event_types = [e.event_type for e in events[: config.max_events_per_family]]
    templates = ["mean_reversion", "continuation", "trend_continuation"]
    avoid_region_keys = _load_avoid_region_keys(
        program_id=config.program_id,
        data_root=Path(config.data_root) if config.data_root else get_data_root(),
        symbols=config.symbols,
        event_types=event_types,
        templates=templates,
        directions=config.directions,
        horizon_bars=config.horizon_bars,
        entry_lags=config.entry_lags,
    )

    return {
        "program_id": config.program_id,
        "start": start,
        "end": end,
        "symbols": list(config.symbols),
        "trigger_space": {
            "allowed_trigger_types": ["EVENT"],
            "events": {"include": event_types},
        },
        "templates": templates,
        "description": f"BroadDiscovery: {family} family ({len(event_types)} events)",
        "run_mode": "research",
        "objective_name": config.objective_name,
        "promotion_profile": config.promotion_profile,
        "timeframe": config.timeframe,
        "instrument_classes": list(config.instrument_classes),
        "horizons_bars": list(config.horizon_bars),
        "directions": list(config.directions),
        "entry_lags": list(config.entry_lags),
        "contexts": {},
        "search_control": {
            "max_hypotheses_total": config.max_hypotheses_total,
            "max_hypotheses_per_template": config.max_hypotheses_per_event,
            "max_hypotheses_per_event_family": config.max_hypotheses_per_event * len(event_types),
            "random_seed": 42,
        },
        "avoid_region_keys": avoid_region_keys,
        "artifacts": {
            "campaign_memory": True,
            "proposal_audit": True,
            "search_frontier": True,
        },
        "knobs": {},
        "broad_discovery": {
            "family": family,
            "events_included": event_types,
            "event_count": len(event_types),
        },
    }


def _default_date_scope(lookback_days: int) -> tuple[str, str]:
    from datetime import timedelta

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=int(lookback_days))
    return start.isoformat(), end.isoformat()


def _parse_results_for_attribution(
    run_id: str,
    program_id: str,
    data_root: Path,
    family: str,
    events_tested: List[str],
) -> List[EventAttribution]:
    """Parse run results and build per-event attribution."""
    from project.research.knowledge.memory import _read_best_available

    results: List[EventAttribution] = []
    data_root = Path(data_root)

    promotion_path = (
        data_root / "reports" / "promotions" / run_id / "promotion_statistical_audit.parquet"
    )

    if not promotion_path.exists():
        promotion_path = promotion_path.with_suffix(".csv")

    df = _read_best_available(promotion_path)
    if df.empty:
        return results

    event_q_values: Dict[str, List[float]] = {}
    event_stats: Dict[str, Dict[str, Any]] = {}

    for _, row in df.iterrows():
        event_type = str(row.get("event_type", "")).strip()
        if event_type not in events_tested:
            continue

        q_val = float(row.get("q_value", 1.0))
        if pd.isna(q_val):
            q_val = 1.0

        event_q_values.setdefault(event_type, []).append(q_val)

        if event_type not in event_stats:
            event_stats[event_type] = {
                "expectancies": [],
                "robustness": [],
                "sample_sizes": [],
                "candidates": 0,
                "promoted": 0,
            }

        event_stats[event_type]["expectancies"].append(
            float(row.get("after_cost_expectancy", 0.0) or 0)
        )
        event_stats[event_type]["robustness"].append(float(row.get("robustness_score", 0.0) or 0))
        event_stats[event_type]["sample_sizes"].append(
            int(row.get("train_n_obs", row.get("sample_size", 0)) or 0)
        )
        event_stats[event_type]["candidates"] += 1

        decision = str(row.get("promotion_decision", "")).strip().lower()
        if decision == "promoted":
            event_stats[event_type]["promoted"] += 1

    q_values_for_fdr = []
    for event_type in events_tested:
        if event_type in event_q_values:
            q_values_for_fdr.append(min(event_q_values[event_type]))
        else:
            q_values_for_fdr.append(1.0)

    adjusted_q_values = _benjamini_hochberg(q_values_for_fdr)

    event_q_dict: Dict[str, float] = {}
    event_exp_dict: Dict[str, float] = {}
    for i, event_type in enumerate(events_tested):
        event_q_dict[event_type] = adjusted_q_values[i]
        if event_type in event_stats:
            event_exp_dict[event_type] = np.mean(event_stats[event_type]["expectancies"])
        else:
            event_exp_dict[event_type] = 0.0

    is_family_level, family_q = _detect_effect_type(event_q_dict, event_exp_dict)

    for i, event_type in enumerate(events_tested):
        q_value = adjusted_q_values[i]
        is_significant = q_value < 0.10

        stats = event_stats.get(event_type, {})
        avg_expectancy = np.mean(stats.get("expectancies", [0.0])) if stats else 0.0
        avg_robustness = np.mean(stats.get("robustness", [0.0])) if stats else 0.0
        max_sample = max(stats.get("sample_sizes", [0])) if stats else 0
        candidate_count = stats.get("candidates", 0)
        promoted_count = stats.get("promoted", 0)

        if is_family_level:
            effect_type = "family_level"
            effect_detail = f"Consistent effect across family (median q={family_q:.3f})"
        elif is_significant:
            effect_type = "event_specific"
            effect_detail = f"Significant event-specific signal (q={q_value:.3f})"
        else:
            effect_type = "no_signal"
            effect_detail = f"No significant signal (q={q_value:.3f})"

        attribution_confidence = _compute_attribution_confidence(
            q_value, max_sample, candidate_count, is_family_level
        )

        results.append(
            EventAttribution(
                event_type=event_type,
                family=family,
                q_value=q_value,
                raw_p_value=1.0,
                is_significant=is_significant,
                after_cost_expectancy=round(avg_expectancy, 6),
                robustness_score=round(avg_robustness, 4),
                sample_size=max_sample,
                candidate_count=candidate_count,
                promoted_count=promoted_count,
                attribution_confidence=attribution_confidence,
                effect_type=effect_type,
                effect_detail=effect_detail,
            )
        )

    return results


def _save_attribution_report(
    result: FamilyDiscoveryResult,
    paths: Any,
) -> Path:
    """Save attribution report to memory."""
    report = {
        "family": result.family,
        "run_id": result.run_id,
        "timestamp": result.timestamp,
        "events_tested": result.events_tested,
        "events_significant": result.events_significant,
        "events_with_family_effect": result.events_with_family_effect,
        "events_with_specific_effect": result.events_with_specific_effect,
        "family_level_q_value": result.family_level_q_value,
        "is_family_level_effect": result.is_family_level_effect,
        "summary": result.summary,
        "attributions": [
            {
                "event_type": a.event_type,
                "q_value": a.q_value,
                "is_significant": a.is_significant,
                "after_cost_expectancy": a.after_cost_expectancy,
                "robustness_score": a.robustness_score,
                "sample_size": a.sample_size,
                "candidate_count": a.candidate_count,
                "promoted_count": a.promoted_count,
                "attribution_confidence": a.attribution_confidence,
                "effect_type": a.effect_type,
                "effect_detail": a.effect_detail,
            }
            for a in result.attributions
        ],
        "errors": result.errors,
    }

    report_path = paths.root / f"broad_discovery_{result.family}_{result.run_id}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return report_path


@dataclass
class BroadDiscoveryConfig:
    program_id: str
    family: str
    registry_root: Path
    data_root: Path | None = None
    symbols: tuple[str, ...] = ("BTCUSDT",)
    instrument_classes: tuple[str, ...] = ("crypto",)
    timeframe: str = "5m"
    lookback_days: int = 90
    horizon_bars: tuple[int, ...] = (12, 24)
    entry_lags: tuple[int, ...] = (1,)
    directions: tuple[str, ...] = ("long", "short")
    objective_name: str = "retail_profitability"
    promotion_profile: str = "research"
    max_events_per_family: int = 10
    max_hypotheses_total: int = 5000
    max_hypotheses_per_event: int = 500
    execute: bool = True
    dry_run: bool = False
    plan_only: bool = False


class BroadDiscoveryRunner:
    """Run broad discovery for a family of events."""

    def __init__(self, config: BroadDiscoveryConfig):
        self.config = config
        self.data_root = Path(config.data_root) if config.data_root else get_data_root()
        self.registry_root = Path(config.registry_root)
        self.paths = ensure_memory_store(config.program_id, data_root=self.data_root)
        self.event_weights = _load_event_weights(self.registry_root)
        self.tested_counts = _load_tested_counts(config.program_id, self.data_root)

    def discover(self) -> FamilyDiscoveryResult:
        """Execute a broad discovery run for the family."""
        errors: List[str] = []

        events = _load_family_events(
            self.config.family,
            self.registry_root,
            self.event_weights,
            self.tested_counts,
        )

        if not events:
            return FamilyDiscoveryResult(
                family=self.config.family,
                run_id="",
                timestamp=datetime.now(timezone.utc).isoformat(),
                events_tested=0,
                events_significant=0,
                events_with_family_effect=0,
                events_with_specific_effect=0,
                attributions=[],
                family_level_q_value=1.0,
                is_family_level_effect=False,
                summary={"error": f"No events found for family {self.config.family}"},
                errors=[f"No events found for family {self.config.family}"],
            )

        events = events[: self.config.max_events_per_family]
        event_types = [e.event_type for e in events]

        proposal = _build_broad_proposal(self.config.family, events, self.config)

        if self.config.plan_only:
            return FamilyDiscoveryResult(
                family=self.config.family,
                run_id="",
                timestamp=datetime.now(timezone.utc).isoformat(),
                events_tested=len(events),
                events_significant=0,
                events_with_family_effect=0,
                events_with_specific_effect=0,
                attributions=[],
                family_level_q_value=1.0,
                is_family_level_effect=False,
                summary={
                    "events_planned": len(events),
                    "event_types": event_types,
                    "status": "planned_only",
                },
            )

        if not self.config.execute:
            return FamilyDiscoveryResult(
                family=self.config.family,
                run_id="",
                timestamp=datetime.now(timezone.utc).isoformat(),
                events_tested=len(events),
                events_significant=0,
                events_with_family_effect=0,
                events_with_specific_effect=0,
                attributions=[],
                family_level_q_value=1.0,
                is_family_level_effect=False,
                summary={
                    "events_planned": len(events),
                    "event_types": event_types,
                    "status": "skipped",
                },
            )

        run_id = generate_run_id(self.config.program_id, proposal)
        proposal_path = (
            self.paths.proposals_dir / f"broad_{self.config.family}_{run_id}" / "proposal.yaml"
        )
        proposal_path.parent.mkdir(parents=True, exist_ok=True)

        import yaml

        proposal_path.write_text(yaml.safe_dump(proposal, sort_keys=False), encoding="utf-8")

        try:
            issue_proposal(
                proposal_path,
                registry_root=self.registry_root,
                data_root=self.data_root,
                run_id=run_id,
                plan_only=False,
                dry_run=self.config.dry_run,
            )
        except Exception as e:
            _LOG.error("Failed to execute broad discovery: %s", e)
            errors.append(str(e))
            return FamilyDiscoveryResult(
                family=self.config.family,
                run_id=run_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                events_tested=len(events),
                events_significant=0,
                events_with_family_effect=0,
                events_with_specific_effect=0,
                attributions=[],
                family_level_q_value=1.0,
                is_family_level_effect=False,
                summary={"status": "execution_failed", "error": str(e)},
                errors=errors,
            )

        attributions = _parse_results_for_attribution(
            run_id, self.config.program_id, self.data_root, self.config.family, event_types
        )

        events_significant = sum(1 for a in attributions if a.is_significant)
        events_family_effect = sum(1 for a in attributions if a.effect_type == "family_level")
        events_specific_effect = sum(1 for a in attributions if a.effect_type == "event_specific")

        q_values = [a.q_value for a in attributions]
        family_q = min(q_values) if q_values else 1.0

        is_family_level = events_family_effect > events_specific_effect

        result_obj = FamilyDiscoveryResult(
            family=self.config.family,
            run_id=run_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            events_tested=len(events),
            events_significant=events_significant,
            events_with_family_effect=events_family_effect,
            events_with_specific_effect=events_specific_effect,
            attributions=attributions,
            family_level_q_value=family_q,
            is_family_level_effect=is_family_level,
            summary={
                "events_tested": len(events),
                "events_significant": events_significant,
                "events_family_effect": events_family_effect,
                "events_specific_effect": events_specific_effect,
                "family_q_value": family_q,
                "is_family_level_effect": is_family_level,
                "event_types": event_types,
            },
            errors=errors,
        )

        _save_attribution_report(result_obj, self.paths)

        return result_obj


def run_broad_discovery(config: BroadDiscoveryConfig) -> FamilyDiscoveryResult:
    """Convenience function to run broad discovery."""
    runner = BroadDiscoveryRunner(config)
    return runner.discover()


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Run BroadDiscovery for a family of events.")
    parser.add_argument("--program_id", required=True)
    parser.add_argument("--family", required=True)
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--symbols", default="BTCUSDT")
    parser.add_argument("--lookback_days", type=int, default=90)
    parser.add_argument("--max_events", type=int, default=10)
    parser.add_argument("--execute", type=int, default=0)
    parser.add_argument("--plan_only", type=int, default=1)
    parser.add_argument("--dry_run", type=int, default=0)

    args = parser.parse_args()

    config = BroadDiscoveryConfig(
        program_id=args.program_id,
        family=args.family,
        registry_root=Path(args.registry_root),
        data_root=Path(args.data_root) if args.data_root else None,
        symbols=tuple(s.strip().upper() for s in str(args.symbols).split(",") if s.strip()),
        lookback_days=args.lookback_days,
        max_events_per_family=args.max_events,
        execute=bool(args.execute),
        plan_only=bool(args.plan_only),
        dry_run=bool(args.dry_run),
    )

    runner = BroadDiscoveryRunner(config)
    result = runner.discover()

    print(
        json.dumps(
            {
                "family": result.family,
                "run_id": result.run_id,
                "timestamp": result.timestamp,
                "events_tested": result.events_tested,
                "events_significant": result.events_significant,
                "is_family_level_effect": result.is_family_level_effect,
                "summary": result.summary,
                "attributions": [
                    {
                        "event_type": a.event_type,
                        "q_value": a.q_value,
                        "is_significant": a.is_significant,
                        "effect_type": a.effect_type,
                        "attribution_confidence": a.attribution_confidence,
                    }
                    for a in result.attributions
                ],
                "errors": result.errors,
            },
            indent=2,
            default=str,
        )
    )
