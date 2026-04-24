from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError
from project.domain.compiled_registry import get_domain_registry
from project.events.governance import event_matches_filters, governed_default_planning_event_ids
from project.io.utils import read_parquet
from project.research.experiment_engine import RegistryBundle
from project.research.knowledge.memory import (
    ensure_memory_store,
    read_memory_table,
)
from project.spec_registry.search_space import (
    DEFAULT_EVENT_PRIORITY_WEIGHT,
    load_event_priority_weights,
)

_LOG = logging.getLogger(__name__)


def _safe_read_legacy_ledger(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return read_parquet(path)
    except Exception as exc:
        _LOG.warning("Failed to read legacy campaign ledger from %s", path, exc_info=True)
        raise DataIntegrityError(f"Failed to read legacy campaign ledger from {path}: {exc}") from exc


def _event_series_from_legacy_ledger(legacy_ledger: pd.DataFrame) -> pd.Series:
    if legacy_ledger.empty:
        return pd.Series(dtype="object")
    if "event_type" in legacy_ledger.columns:
        return legacy_ledger["event_type"].astype(str)

    if "trigger_payload" not in legacy_ledger.columns:
        return pd.Series("", index=legacy_ledger.index, dtype="object")

    def _extract_event_id(payload: Any) -> str:
        if payload is None or (isinstance(payload, float) and pd.isna(payload)):
            return ""
        if isinstance(payload, dict):
            return str(payload.get("event_id", "")).strip()
        try:
            parsed = json.loads(str(payload))
        except Exception:
            return ""
        if isinstance(parsed, dict):
            return str(parsed.get("event_id", "")).strip()
        return ""

    return legacy_ledger["trigger_payload"].map(_extract_event_id).astype(str)


def _build_dynamic_quality_weights(
    tested_regions: pd.DataFrame,
    static_weights: Dict[str, float],
    *,
    min_evaluations: int = 5,
    alpha: float = 0.4,  # blend factor: 0 = fully static, 1 = fully empirical
) -> Dict[str, float]:
    """Blend static YAML quality weights with empirical promotion rates.

    Only events with at least `min_evaluations` evaluations contribute an
    empirical signal; below that floor the static weight is used unchanged.
    """
    if tested_regions.empty or "event_type" not in tested_regions.columns:
        return static_weights

    grouped = (
        tested_regions.groupby("event_type")
        .agg(
            n_eval=("candidate_id", "count"),
            n_promoted=("eval_status", lambda s: (s.astype(str) == "promoted").sum()),
        )
        .reset_index()
    )

    dynamic: Dict[str, float] = dict(static_weights)
    for row in grouped.to_dict(orient="records"):
        event = str(row["event_type"])
        n_eval = int(row["n_eval"])
        n_promoted = int(row["n_promoted"])
        if n_eval < min_evaluations:
            continue

        empirical = float(n_promoted) / max(n_eval, 1)
        # Normalise empirical rate to same scale as static weights (0–3)
        empirical_scaled = empirical * 3.0
        static = static_weights.get(event, DEFAULT_EVENT_PRIORITY_WEIGHT)
        dynamic[event] = (1 - alpha) * static + alpha * empirical_scaled
    return dynamic


def _build_summary(program_id: str, tested_regions: pd.DataFrame, *, top_k: int) -> Dict[str, Any]:
    if tested_regions.empty:
        return {"program_id": program_id, "status": "no_data"}

    evaluated = tested_regions.copy()
    promoted = evaluated[evaluated["eval_status"].astype(str) == "promoted"]
    summary: Dict[str, Any] = {
        "program_id": program_id,
        "metrics": {
            "total_runs": int(evaluated["run_id"].nunique()),
            "total_regions": int(len(evaluated)),
            "status_counts": evaluated["eval_status"].astype(str).value_counts().to_dict(),
            "evaluated_share": 1.0,
            "promotion_rate": float(len(promoted) / max(len(evaluated), 1)),
        },
    }

    def _group_stats(column: str) -> Dict[str, Any]:
        if column not in evaluated.columns:
            return {}
        grouped = evaluated.groupby(column)
        out: Dict[str, Any] = {}
        for key, sub in grouped:
            promoted_share = float((sub["eval_status"].astype(str) == "promoted").mean())
            out[str(key)] = {
                "sample": int(len(sub)),
                "promotion_rate": promoted_share,
                "avg_after_cost_expectancy": float(
                    pd.to_numeric(sub["after_cost_expectancy"], errors="coerce").mean()
                ),
            }
        return out

    summary["win_rates"] = {
        "by_trigger_type": _group_stats("trigger_type"),
        "by_template": _group_stats("template_id"),
        "by_direction": _group_stats("direction"),
        "by_horizon": _group_stats("horizon"),
        "by_event_type": _group_stats("event_type"),
        "by_canonical_regime": _group_stats("canonical_regime"),
    }

    def _gate_rank(val) -> int:
        val = str(val).strip().lower()
        if val in ("pass", "true", "1", "1.0"):
            return 2
        if val in ("fail", "false", "0", "0.0"):
            return 1
        return 0

    ranked = evaluated.copy()
    if "gate_promo_statistical" in ranked.columns:
        ranked["_gate_rank"] = ranked["gate_promo_statistical"].apply(_gate_rank)
    else:
        ranked["_gate_rank"] = 0
    ranked["_after_cost_expectancy"] = pd.to_numeric(
        ranked.get("after_cost_expectancy", pd.Series(dtype=float)),
        errors="coerce",
    )
    ranked["_q_value"] = pd.to_numeric(
        ranked.get("q_value", pd.Series(dtype=float)),
        errors="coerce",
    )
    statistically_supported = ranked["_gate_rank"] >= 2
    if "_q_value" in ranked.columns:
        statistically_supported = statistically_supported | (
            ranked["_q_value"].notna() & (ranked["_q_value"] <= 0.10)
        )
    ranked = ranked[
        statistically_supported
        & ranked["_after_cost_expectancy"].notna()
        & (ranked["_after_cost_expectancy"] > 0)
    ].copy()

    summary["top_performing_regions"] = (
        ranked.sort_values(
            ["_gate_rank", "_after_cost_expectancy", "_q_value"],
            ascending=[False, False, True],
        )
        .head(int(top_k))[
            [
                c
                for c in [
                    "run_id",
                    "candidate_id",
                    "event_type",
                    "canonical_regime",
                    "template_id",
                    "direction",
                    "horizon",
                    "q_value",
                    "after_cost_expectancy",
                    "primary_fail_gate",
                ]
                if c in evaluated.columns
            ]
        ]
        .to_dict(orient="records")
    )
    return summary


def _build_frontier(
    registries: RegistryBundle,
    tested_regions: pd.DataFrame,
    failures: pd.DataFrame,
    *,
    untested_top_k: int,
    repair_top_k: int,
    exhausted_failure_threshold: int,
    quality_weights: Dict[str, float] | None = None,
) -> Dict[str, Any]:
    """Build the search frontier artefact for a campaign program.

    Phase 2.2: ``untested_registry_events`` is now sorted by descending
    quality weight (sourced from ``spec_registry.search_space``) rather than
    alphabetically.  Events with an explicit ``[QUALITY: HIGH/MODERATE/LOW]``
    annotation — plus an optional raw IG bonus — are surfaced first.
    Unannotated events receive ``DEFAULT_EVENT_PRIORITY_WEIGHT`` (1.5) and
    are ordered deterministically after annotated ones.
    """
    if quality_weights is None:
        quality_weights = {}

    domain_registry = get_domain_registry()
    events = registries.events.get("events", {})
    if isinstance(events, dict) and events:
        enabled_events = [
            str(event_id).strip()
            for event_id, cfg in events.items()
            if str(event_id).strip()
            and not (
                isinstance(cfg, dict)
                and str(cfg.get("enabled", True)).strip().lower() in {"0", "false", "no"}
            )
            and (
                domain_registry.get_event(str(event_id).strip()) is None
                or event_matches_filters(
                    str(event_id).strip(),
                    tiers=("A", "B"),
                    roles=("trigger", "confirm"),
                    trade_trigger_eligible=True,
                )
            )
        ]
    else:
        enabled_events = list(governed_default_planning_event_ids())
    tested_events = {
        str(event_id).strip()
        for event_id in tested_regions.get("event_type", pd.Series(dtype="object")).astype(str).unique()
        if str(event_id).strip()
    }
    untested_events_raw = list(set(enabled_events) - tested_events)

    # Phase 2.2: sort by descending quality weight; stable within a tier
    # (preserves registry insertion order for equal weights → deterministic).
    untested_events_raw.sort(
        key=lambda e: quality_weights.get(e, DEFAULT_EVENT_PRIORITY_WEIGHT),
        reverse=True,
    )

    family_to_events: Dict[str, list[str]] = {}
    if isinstance(events, dict) and events:
        for event_id, cfg in events.items():
            event_name = str(event_id).strip()
            if not event_name:
                continue
            family = ""
            if isinstance(cfg, dict):
                family = str(cfg.get("family", "")).strip()
            if family:
                family_to_events.setdefault(family, []).append(event_name)

    partial_families: Dict[str, str] = {}
    if family_to_events:
        for family, family_events in family_to_events.items():
            tested_count = sum(1 for event_id in family_events if event_id in tested_events)
            if 0 < tested_count < len(family_events):
                partial_families[family] = f"{tested_count}/{len(family_events)}"

    exhausted_events: list[str] = []
    if (
        not tested_regions.empty
        and "event_type" in tested_regions.columns
        and "eval_status" in tested_regions.columns
    ):
        fail_counts = (
            tested_regions[tested_regions["eval_status"].astype(str) != "promoted"]
            .groupby("event_type")
            .size()
        )
        exhausted_events = sorted(
            list(fail_counts[fail_counts >= int(exhausted_failure_threshold)].index.astype(str))
        )

    tested_regimes = set(
        tested_regions.get("canonical_regime", pd.Series(dtype="object"))
        .astype(str)
        .str.strip()
        .str.upper()
    ) - {""}
    if not tested_regimes:
        tested_regimes = {
            domain_registry.get_event(event_id).canonical_regime
            for event_id in tested_events
            if domain_registry.get_event(event_id) is not None
        }

    regime_fanout = {
        regime: list(domain_registry.get_event_ids_for_regime(regime, executable_only=True))
        for regime in domain_registry.canonical_regime_rows()
    }
    untested_regimes = [
        regime
        for regime, event_ids in regime_fanout.items()
        if event_ids and regime not in tested_regimes
    ]
    untested_regimes.sort(
        key=lambda regime: max(
            (
                quality_weights.get(event_id, DEFAULT_EVENT_PRIORITY_WEIGHT)
                for event_id in regime_fanout.get(regime, [])
            ),
            default=DEFAULT_EVENT_PRIORITY_WEIGHT,
        ),
        reverse=True,
    )

    partial_regimes: Dict[str, str] = {}
    for regime, event_ids in regime_fanout.items():
        if not event_ids:
            continue
        tested_count = sum(1 for event_id in event_ids if event_id in tested_events)
        if 0 < tested_count < len(event_ids):
            partial_regimes[regime] = f"{tested_count}/{len(event_ids)}"

    repair_candidates = []
    if not failures.empty:
        for stage, count in (
            failures["stage"].astype(str).value_counts().head(int(repair_top_k)).items()
        ):
            repair_candidates.append(f"repair repeated failure in stage: {stage} ({int(count)})")

    next_moves = []
    if untested_regimes:
        next_moves.append(
            f"explore untested canonical regimes: {untested_regimes[: int(untested_top_k)]}"
        )
    if partial_regimes:
        next_moves.append(f"complete coverage for regime: {next(iter(partial_regimes))}")
    next_moves.extend(repair_candidates[: int(repair_top_k)])

    frontier_payload = {
        "untested_canonical_regimes": untested_regimes[: int(untested_top_k)],
        "canonical_regime_event_fanout": {
            regime: regime_fanout[regime] for regime in untested_regimes[: int(untested_top_k)]
        },
        "untested_registry_events": untested_events_raw[: int(untested_top_k)],
        "untested_events": untested_events_raw,
        "exhausted_events_to_avoid": exhausted_events,
        "partially_explored_regimes": partial_regimes,
        "partially_explored_families": partial_families or partial_regimes,
        "candidate_next_moves": next_moves,
    }
    return frontier_payload


def update_search_intelligence(
    data_root: Path,
    registry_root: Path,
    program_id: str,
    *,
    summary_top_k: int = 10,
    frontier_untested_top_k: int = 3,
    frontier_repair_top_k: int = 2,
    exhausted_failure_threshold: int = 3,
    search_space_path: Path | None = None,
) -> Dict[str, Any]:
    """Update campaign summary and search frontier artefacts.

    Phase 2.2: accepts an optional *search_space_path* so callers can pin a
    specific YAML file (useful in tests).  When *None*, resolves the
    canonical ``spec/search_space.yaml`` via ``load_event_priority_weights``.
    Quality weights are passed into ``_build_frontier`` so that
    ``untested_registry_events`` is ordered by descending quality rather than
    alphabetically.
    """
    campaign_dir = data_root / "artifacts" / "experiments" / program_id
    campaign_dir.mkdir(parents=True, exist_ok=True)
    summary_path = campaign_dir / "campaign_summary.json"
    frontier_path = campaign_dir / "search_frontier.json"

    registries = RegistryBundle(registry_root)
    ensure_memory_store(program_id, data_root=data_root)
    tested_regions = read_memory_table(program_id, "tested_regions", data_root=data_root)
    failures = read_memory_table(program_id, "failures", data_root=data_root)

    if tested_regions.empty:
        legacy_ledger = _safe_read_legacy_ledger(campaign_dir / "tested_ledger.parquet")
        if not legacy_ledger.empty:
            tested_regions = pd.DataFrame(
                {
                    "run_id": legacy_ledger.get("run_id", pd.Series(dtype="object")),
                    "event_type": _event_series_from_legacy_ledger(legacy_ledger),
                    "template_id": legacy_ledger.get("template_id", pd.Series(dtype="object")),
                    "direction": legacy_ledger.get("direction", pd.Series(dtype="object")),
                    "horizon": legacy_ledger.get("horizon", pd.Series(dtype="object")),
                    "trigger_type": legacy_ledger.get("trigger_type", pd.Series(dtype="object")),
                    "after_cost_expectancy": legacy_ledger.get(
                        "expectancy", pd.Series(dtype=float)
                    ),
                    "q_value": legacy_ledger.get("q_value", pd.Series(dtype=float)),
                    "eval_status": legacy_ledger.get("eval_status", pd.Series(dtype="object")),
                    "candidate_id": legacy_ledger.get("candidate_id", pd.Series(dtype="object")),
                    "gate_promo_statistical": False,
                    "primary_fail_gate": "",
                }
            )

    # Phase 2.2: load quality weights from the centralised spec_registry loader
    static_weights = load_event_priority_weights(search_space_path)

    # Phase 2-A: blend with empirical win rates if available
    quality_weights = _build_dynamic_quality_weights(tested_regions, static_weights)

    summary = _build_summary(program_id, tested_regions, top_k=summary_top_k)
    frontier = _build_frontier(
        registries,
        tested_regions,
        failures,
        untested_top_k=frontier_untested_top_k,
        repair_top_k=frontier_repair_top_k,
        exhausted_failure_threshold=exhausted_failure_threshold,
        quality_weights=quality_weights,
    )
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    frontier_path.write_text(
        json.dumps(frontier, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _LOG.info("Updated intelligence for %s from campaign memory.", program_id)
    return {"summary": summary, "frontier": frontier}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--program_id", required=True)
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument("--summary_top_k", type=int, default=10)
    parser.add_argument("--frontier_untested_top_k", type=int, default=3)
    parser.add_argument("--frontier_repair_top_k", type=int, default=2)
    parser.add_argument("--exhausted_failure_threshold", type=int, default=3)
    args = parser.parse_args()

    data_root = get_data_root()
    update_search_intelligence(
        data_root,
        Path(args.registry_root),
        args.program_id,
        summary_top_k=int(args.summary_top_k),
        frontier_untested_top_k=int(args.frontier_untested_top_k),
        frontier_repair_top_k=int(args.frontier_repair_top_k),
        exhausted_failure_threshold=int(args.exhausted_failure_threshold),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
