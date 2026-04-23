from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from project.io.utils import atomic_write_json, atomic_write_text, read_parquet
from project.research.agent_io.proposal_schema import load_operator_proposal
from project.research.cell_discovery.paths import paths_for_run
from project.research.condition_routing import condition_routing


def _slug(value: Any) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9]+", "_", raw)
    return raw.strip("_") or "cell"


def _horizon_bars(value: Any) -> int:
    raw = str(value or "").lower().replace("bars", "").replace("b", "").strip()
    try:
        return max(1, int(float(raw)))
    except (TypeError, ValueError):
        return 1


def _context_filter_from_json(row: pd.Series) -> dict[str, list[str]]:
    context_json = str(row.get("context_json", "") or "")
    if not context_json:
        return {}
    try:
        parsed = json.loads(context_json)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {
        str(key): [str(value)]
        for key, value in parsed.items()
        if str(key).strip() and str(value).strip()
    }


def _condition_name(dimension: str, value: str) -> str:
    dim = str(dimension or "").strip()
    val = str(value or "").strip()
    if not dim or not val:
        return ""
    if val.startswith(f"{dim}_"):
        return val
    return f"{dim}_{val}"


def _supportive_context(row: pd.Series) -> dict[str, Any]:
    raw = str(row.get("supportive_context_json", "") or "")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {
        str(key): value
        for key, value in parsed.items()
        if str(key).strip() and value not in (None, "", [], {})
    }


def _load_source_scope(paths) -> dict[str, Any]:
    if not paths.experiment_path.exists():
        return {}
    try:
        payload = json.loads(paths.experiment_path.read_text())
    except json.JSONDecodeError:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _runtime_context_translation(row: pd.Series, *, symbol: str) -> dict[str, Any]:
    contexts = _context_filter_from_json(row)
    routing = []
    for dimension, values in contexts.items():
        for value in values:
            condition_name = _condition_name(dimension, value)
            routed_condition, source = condition_routing(
                condition_name,
                run_symbols=[symbol],
                strict=True,
            )
            routing.append(
                {
                    "dimension": dimension,
                    "value": value,
                    "condition_name": condition_name,
                    "routed_condition": routed_condition,
                    "routing_source": source,
                }
            )
            if source != "runtime" or routed_condition == "__BLOCKED__":
                return {
                    "allowed": False,
                    "disposition": "runtime_context_not_executable",
                    "contexts": {},
                    "routing": routing,
                }
    return {
        "allowed": True,
        "disposition": "runtime_executable",
        "contexts": contexts,
        "routing": routing,
    }


def _assembly_translation(row: pd.Series, *, symbol: str) -> dict[str, Any]:
    if bool(row.get("runtime_executable", False)):
        return _runtime_context_translation(row, symbol=symbol)
    executability = str(row.get("executability_class", "") or "").strip()
    if executability == "supportive_only":
        supportive_context = _supportive_context(row)
        contexts = _context_filter_from_json(row)
        routing = []
        for dimension, values in contexts.items():
            for value in values:
                condition_name = _condition_name(dimension, value)
                routed_condition, source = condition_routing(
                    condition_name,
                    run_symbols=[symbol],
                    strict=True,
                )
                routing.append(
                    {
                        "dimension": dimension,
                        "value": value,
                        "condition_name": condition_name,
                        "routed_condition": routed_condition,
                        "routing_source": source,
                    }
                )
        if supportive_context:
            return {
                "allowed": True,
                "disposition": "supportive_only_context_downgraded",
                "contexts": {},
                "supportive_context": supportive_context,
                "routing": routing,
            }
        return {
            "allowed": False,
            "disposition": "supportive_only_context_unmapped",
            "contexts": {},
            "routing": routing,
        }
    return {
        "allowed": False,
        "disposition": "research_only_or_non_executable_context",
        "contexts": {},
        "routing": [],
    }


def _proposal_payload(
    row: pd.Series,
    *,
    run_id: str,
    source_scope: dict[str, Any],
    translation: dict[str, Any],
) -> dict[str, Any]:
    symbol = str(row.get("symbol", "BTCUSDT") or "BTCUSDT")
    horizon = _horizon_bars(row.get("horizon"))
    event_atom = str(row.get("event_atom", "") or row.get("source_event_atom", ""))
    program_id = (
        f"edge_cell_{_slug(run_id)}_{_slug(row.get('cell_id'))}_{_slug(symbol)}_v1"
    )[:120]
    start = str(source_scope.get("start", "") or "").strip()
    end = str(source_scope.get("end", "") or "").strip()
    search_spec = str(source_scope.get("search_spec_path", "") or "").strip()
    if not start or not end or not search_spec:
        raise ValueError("missing_source_scope")
    supportive_context = dict(translation.get("supportive_context", {}) or {})
    payload = {
        "program_id": program_id,
        "description": (
            f"Cell-origin thesis from {run_id}: {event_atom} "
            f"{row.get('direction', '')} over {horizon} bars."
        ),
        "start": start,
        "end": end,
        "symbols": [symbol],
        "timeframe": str(source_scope.get("timeframe", row.get("timeframe", "5m")) or "5m"),
        "instrument_classes": ["crypto"],
        "objective_name": "retail_profitability",
        "promotion_profile": "research",
        "search_spec": {"path": search_spec},
        "avoid_region_keys": [],
        "hypothesis": {
            "anchor": {
                "type": "event",
                "event_id": event_atom,
            },
            "filters": {
                "contexts": dict(translation.get("contexts", {}) or {}),
            },
            "sampling_policy": {
                "mode": "episodic",
                "entry_lag_bars": 1,
                "overlap_policy": "suppress",
            },
            "template": {
                "id": str(row.get("template", "") or "continuation"),
            },
            "direction": str(row.get("direction", "long") or "long"),
            "horizon_bars": horizon,
        },
        "artifacts": {
            "source_discovery_mode": "edge_cells",
            "source_cell_id": str(row.get("cell_id", "")),
            "source_scoreboard_run_id": run_id,
            "source_event_atom": str(row.get("source_event_atom", row.get("event_atom", ""))),
            "source_context_cell": str(row.get("context_cell", "")),
            "source_contrast_lift_bps": float(row.get("contrast_lift_bps", 0.0) or 0.0),
            "source_rank_score": float(row.get("rank_score", 0.0) or 0.0),
            "source_redundancy_cluster_id": str(row.get("redundancy_cluster_id", "")),
            "source_context_executability_class": str(row.get("executability_class", "") or ""),
            "source_experiment_path": str(source_scope.get("experiment_path", "") or ""),
            "source_search_spec_path": search_spec,
            "source_lineage_path": str(source_scope.get("lineage_path", "") or ""),
            "context_translation": str(translation.get("disposition", "") or ""),
            "context_routing": list(translation.get("routing", []) or []),
        },
        "version": 1,
    }
    if supportive_context:
        payload["artifacts"]["supportive_context"] = supportive_context
    return payload


def assemble_theses(
    *,
    run_id: str,
    data_root: Path,
    limit: int = 20,
) -> dict[str, Any]:
    paths = paths_for_run(data_root=data_root, run_id=run_id)
    if not paths.cluster_representatives_path.exists():
        raise FileNotFoundError(
            f"edge cluster representatives not found: {paths.cluster_representatives_path}"
        )
    representatives = read_parquet([paths.cluster_representatives_path])
    source_scope = _load_source_scope(paths)
    if source_scope:
        source_scope.setdefault("experiment_path", str(paths.experiment_path))
    paths.generated_proposals_dir.mkdir(parents=True, exist_ok=True)
    generated: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    if not representatives.empty:
        representatives = representatives.sort_values(
            ["rank_score", "net_mean_bps"],
            ascending=False,
        ).head(limit)

    for _, row in representatives.iterrows():
        cell_id = str(row.get("cell_id", ""))
        if not bool(row.get("is_representative", False)):
            rejected.append({"cell_id": cell_id, "reason": "not_cluster_representative"})
            continue
        if str(row.get("blocked_reason", "") or "").strip():
            rejected.append({"cell_id": cell_id, "reason": str(row.get("blocked_reason"))})
            continue
        symbol = str(row.get("symbol", "BTCUSDT") or "BTCUSDT")
        translation = _assembly_translation(row, symbol=symbol)
        if not bool(translation.get("allowed", False)):
            rejected.append({"cell_id": cell_id, "reason": str(translation.get("disposition"))})
            continue
        try:
            payload = _proposal_payload(
                row,
                run_id=run_id,
                source_scope=source_scope,
                translation=translation,
            )
        except Exception as exc:
            rejected.append({"cell_id": cell_id, "reason": f"invalid_proposal: {exc}"})
            continue
        try:
            load_operator_proposal(payload)
        except Exception as exc:
            rejected.append({"cell_id": cell_id, "reason": f"invalid_proposal: {exc}"})
            continue
        out_path = paths.generated_proposals_dir / f"{payload['program_id']}.yaml"
        atomic_write_text(out_path, yaml.safe_dump(payload, sort_keys=False))
        generated.append(
            {
                "cell_id": cell_id,
                "proposal_path": str(out_path),
                "program_id": payload["program_id"],
                "disposition": str(translation.get("disposition")),
            }
        )

    report = {
        "schema_version": "edge_cell_thesis_assembly_v1",
        "run_id": run_id,
        "generated_count": len(generated),
        "rejected_count": len(rejected),
        "generated": generated,
        "rejected": rejected,
    }
    atomic_write_json(paths.thesis_assembly_report_path, report)
    return report
