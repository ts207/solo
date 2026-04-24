from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import read_parquet
from project.research.knowledge.memory import ensure_memory_store, read_memory_table


def _read_optional_parquet(path: Path) -> pd.DataFrame:
    if path.exists():
        return read_parquet(path)
    return pd.DataFrame()


def _has_columns(df: pd.DataFrame, columns: Iterable[str]) -> bool:
    required = {str(column) for column in columns}
    return required.issubset(set(df.columns))


def query_static_rows(
    *,
    data_root: Path | None = None,
    event: str = "",
    template: str = "",
    state: str = "",
    detector: str = "",
    feature: str = "",
    limit: int = 20,
) -> Dict[str, Any]:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    static_root = resolved_data_root / "knowledge" / "static"
    entities = _read_optional_parquet(static_root / "entities.parquet")
    relations = _read_optional_parquet(static_root / "relations.parquet")

    filtered = entities.copy()
    if event:
        if not _has_columns(filtered, ("entity_type", "name")):
            filtered = filtered.head(0)
        else:
            filtered = filtered[
                (filtered["entity_type"] == "event") & (filtered["name"].astype(str) == str(event))
            ]
    elif template:
        if not _has_columns(filtered, ("entity_type", "name")):
            filtered = filtered.head(0)
        else:
            filtered = filtered[
                (filtered["entity_type"] == "template")
                & (filtered["name"].astype(str) == str(template))
            ]
    elif state:
        if not _has_columns(filtered, ("entity_type", "name")):
            filtered = filtered.head(0)
        else:
            filtered = filtered[
                (filtered["entity_type"] == "state") & (filtered["name"].astype(str) == str(state))
            ]
    elif detector:
        if not _has_columns(filtered, ("entity_type", "name")):
            filtered = filtered.head(0)
        else:
            filtered = filtered[
                (filtered["entity_type"] == "detector")
                & (filtered["name"].astype(str) == str(detector))
            ]
    elif feature:
        if not _has_columns(filtered, ("entity_type", "name")):
            filtered = filtered.head(0)
        else:
            filtered = filtered[
                (filtered["entity_type"] == "feature")
                & (filtered["name"].astype(str) == str(feature))
            ]
    else:
        filtered = filtered.head(limit)

    entity_ids = filtered.get("entity_id", pd.Series(dtype="object")).astype(str).tolist()
    if _has_columns(relations, ("from_entity_id", "to_entity_id")):
        rel = relations[
            relations.get("from_entity_id", pd.Series(dtype="object")).astype(str).isin(entity_ids)
            | relations.get("to_entity_id", pd.Series(dtype="object")).astype(str).isin(entity_ids)
        ].head(limit)
    else:
        rel = relations.head(0)
    return {
        "entities": filtered.head(limit).to_dict(orient="records"),
        "relations": rel.to_dict(orient="records"),
    }


def query_agent_knobs(
    *,
    data_root: Path | None = None,
    group: str = "",
    name_prefix: str = "",
    include_advanced: bool = False,
    include_internal: bool = False,
    mutability: str = "proposal_settable",
    limit: int = 50,
) -> Dict[str, Any]:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    static_root = resolved_data_root / "knowledge" / "static"
    knobs = _read_optional_parquet(static_root / "agent_knobs.parquet")
    if knobs.empty:
        return {"knobs": []}
    if group and "group" in knobs.columns:
        knobs = knobs[knobs["group"].astype(str) == str(group)]
    if name_prefix and "name" in knobs.columns:
        knobs = knobs[knobs["name"].astype(str).str.startswith(str(name_prefix))]
    if "agent_level" in knobs.columns:
        allowed_levels = {"core"}
        if include_advanced:
            allowed_levels.add("advanced")
        if include_internal:
            allowed_levels.add("internal")
        knobs = knobs[knobs["agent_level"].astype(str).isin(sorted(allowed_levels))]
    if mutability and "mutability" in knobs.columns:
        if str(mutability).strip().lower() != "any":
            knobs = knobs[knobs["mutability"].astype(str) == str(mutability)]
    knobs = knobs.sort_values(["group", "name"], kind="stable")
    return {"knobs": knobs.head(limit).to_dict(orient="records")}


def query_memory_rows(
    *,
    program_id: str,
    data_root: Path | None = None,
    event_type: str = "",
    template_id: str = "",
    failure_class: str = "",
    limit: int = 20,
) -> Dict[str, Any]:
    ensure_memory_store(program_id, data_root=data_root)
    tested_regions = read_memory_table(program_id, "tested_regions", data_root=data_root)
    failures = read_memory_table(program_id, "failures", data_root=data_root)
    reflections = read_memory_table(program_id, "reflections", data_root=data_root)
    proposals = read_memory_table(program_id, "proposals", data_root=data_root)

    if event_type and "event_type" in tested_regions.columns:
        tested_regions = tested_regions[tested_regions["event_type"].astype(str) == str(event_type)]
    if template_id and "template_id" in tested_regions.columns:
        tested_regions = tested_regions[
            tested_regions["template_id"].astype(str) == str(template_id)
        ]
    if failure_class and "failure_class" in failures.columns:
        failures = failures[failures["failure_class"].astype(str) == str(failure_class)]

    return {
        "tested_regions": tested_regions.head(limit).to_dict(orient="records"),
        "failures": failures.head(limit).to_dict(orient="records"),
        "reflections": reflections.head(limit).to_dict(orient="records"),
        "proposals": proposals.head(limit).to_dict(orient="records"),
    }


def query_adjacent_regions(
    *,
    program_id: str,
    data_root: Path | None = None,
    event_type: str,
    template: str = "",
    limit: int = 20,
) -> Dict[str, Any]:
    ensure_memory_store(program_id, data_root=data_root)
    tested_regions = read_memory_table(program_id, "tested_regions", data_root=data_root)
    if tested_regions.empty:
        return {"adjacent_regions": []}
    filtered = tested_regions[tested_regions["event_type"].astype(str) == str(event_type)]
    if template:
        filtered = filtered[filtered["template_id"].astype(str) == str(template)]
    sort_cols = [
        column
        for column in ["gate_promo_statistical", "q_value", "after_cost_expectancy"]
        if column in filtered.columns
    ]
    ascending = [False, True, False][: len(sort_cols)]
    if sort_cols:
        if "gate_promo_statistical" in sort_cols:

            def _gate_rank(val) -> int:
                val = str(val).strip().lower()
                if val in ("pass", "true", "1", "1.0"):
                    return 2
                if val in ("fail", "false", "0", "0.0"):
                    return 1
                return 0

            filtered["_gate_rank"] = filtered["gate_promo_statistical"].apply(_gate_rank)
            sort_cols[sort_cols.index("gate_promo_statistical")] = "_gate_rank"

        filtered = filtered.sort_values(sort_cols, ascending=ascending)
        if "_gate_rank" in filtered.columns:
            filtered = filtered.drop(columns=["_gate_rank"])

    return {"adjacent_regions": filtered.head(limit).to_dict(orient="records")}


def query_dynamic_weights(
    *,
    program_id: str,
    data_root: Path | None = None,
) -> Dict[str, Any]:
    """Query dynamic quality weights computed from campaign promotion history."""
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()

    tested_regions = read_memory_table(
        program_id,
        "tested_regions",
        data_root=resolved_data_root,
    )

    if tested_regions.empty:
        return {"error": "No tested regions in campaign memory", "program_id": program_id}

    from project.research.search_intelligence import _build_dynamic_quality_weights

    static_weights = {
        "HIGH": 3.0,
        "MODERATE": 2.0,
        "LOW": 1.0,
        "DEFAULT": 1.0,
    }

    dynamic = _build_dynamic_quality_weights(tested_regions, static_weights, alpha=0.4)

    return {
        "program_id": program_id,
        "static_weights": static_weights,
        "dynamic_weights": dynamic,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Query static and campaign memory artifacts.")
    parser.add_argument("--data_root", default=None)
    subparsers = parser.add_subparsers(dest="command", required=True)

    static_parser = subparsers.add_parser("static")
    static_parser.add_argument("--event", default="")
    static_parser.add_argument("--template", default="")
    static_parser.add_argument("--state", default="")
    static_parser.add_argument("--detector", default="")
    static_parser.add_argument("--feature", default="")
    static_parser.add_argument("--limit", type=int, default=20)

    knobs_parser = subparsers.add_parser("knobs")
    knobs_parser.add_argument("--group", default="")
    knobs_parser.add_argument("--name_prefix", default="")
    knobs_parser.add_argument("--include_advanced", type=int, default=0)
    knobs_parser.add_argument("--include_internal", type=int, default=0)
    knobs_parser.add_argument("--mutability", default="proposal_settable")
    knobs_parser.add_argument("--limit", type=int, default=50)

    memory_parser = subparsers.add_parser("memory")
    memory_parser.add_argument("--program_id", required=True)
    memory_parser.add_argument("--event_type", default="")
    memory_parser.add_argument("--template_id", default="")
    memory_parser.add_argument("--failure_class", default="")
    memory_parser.add_argument("--limit", type=int, default=20)

    adjacent_parser = subparsers.add_parser("adjacent")
    adjacent_parser.add_argument("--program_id", required=True)
    adjacent_parser.add_argument("--event_type", required=True)
    adjacent_parser.add_argument("--template", default="")
    adjacent_parser.add_argument("--limit", type=int, default=20)

    dynamic_parser = subparsers.add_parser("dynamic_weights")
    dynamic_parser.add_argument("--program_id", required=True)

    args = parser.parse_args(list(argv) if argv is not None else None)
    data_root = Path(args.data_root) if args.data_root else None

    if args.command == "static":
        payload = query_static_rows(
            data_root=data_root,
            event=args.event,
            template=args.template,
            state=args.state,
            detector=args.detector,
            feature=args.feature,
            limit=args.limit,
        )
    elif args.command == "knobs":
        payload = query_agent_knobs(
            data_root=data_root,
            group=args.group,
            name_prefix=args.name_prefix,
            include_advanced=bool(args.include_advanced),
            include_internal=bool(args.include_internal),
            mutability=args.mutability,
            limit=args.limit,
        )
    elif args.command == "memory":
        payload = query_memory_rows(
            program_id=args.program_id,
            data_root=data_root,
            event_type=args.event_type,
            template_id=args.template_id,
            failure_class=args.failure_class,
            limit=args.limit,
        )
    elif args.command == "dynamic_weights":
        payload = query_dynamic_weights(
            program_id=args.program_id,
            data_root=data_root,
        )
    else:
        payload = query_adjacent_regions(
            program_id=args.program_id,
            data_root=data_root,
            event_type=args.event_type,
            template=args.template,
            limit=args.limit,
        )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
