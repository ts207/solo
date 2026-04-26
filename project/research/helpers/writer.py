from __future__ import annotations

import dataclasses
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import yaml

from project.strategy.dsl import Blueprint


def _replace_model(instance: Any, **updates: object) -> Any:
    model_copy = getattr(instance, "model_copy", None)
    if callable(model_copy):
        return model_copy(update=updates)
    return dataclasses.replace(instance, **updates)


def sort_blueprints_for_write(
    blueprints: Sequence[Blueprint],
    selection_records: Sequence[dict[str, object]],
) -> list[Blueprint]:
    by_candidate: dict[str, dict[str, object]] = {}
    for row in selection_records:
        cid = str(row.get("candidate_id", "")).strip()
        if cid and cid not in by_candidate:
            by_candidate[cid] = row

    def _sort_key(bp: Blueprint):
        match = by_candidate.get(str(bp.candidate_id), {})
        return (
            -float(match.get("after_cost_expectancy", 0.0)),
            -float(match.get("robustness_score", 0.0)),
            -int(match.get("n_events", 0)),
            str(bp.candidate_id),
        )

    return sorted(list(blueprints), key=_sort_key)


def apply_portfolio_cap(
    blueprints: Sequence[Blueprint],
    max_concurrent_positions: int | None,
) -> tuple[list[Blueprint], list[str]]:
    out = list(blueprints)
    if max_concurrent_positions is None or int(max_concurrent_positions) <= 0:
        return out, []
    cap = int(max_concurrent_positions)
    if len(out) <= cap:
        return out, []
    dropped_ids = [bp.id for bp in out[cap:]]
    return out[:cap], dropped_ids


def apply_retail_constraints(
    blueprints: Sequence[Blueprint],
    retail_constraints: dict[str, object],
) -> list[Blueprint]:
    if not blueprints:
        return []
    updated: list[Blueprint] = []
    for bp in blueprints:
        constraints = dict(bp.lineage.constraints)
        constraints.update(retail_constraints)
        new_lineage = _replace_model(bp.lineage, constraints=constraints)
        updated.append(_replace_model(bp, lineage=new_lineage))
    return updated


def write_blueprint_artifacts(
    *,
    blueprints: Sequence[Blueprint],
    out_jsonl: Path,
    out_yaml: Path,
) -> None:
    tmp_jsonl = out_jsonl.with_suffix(".jsonl.tmp")
    lines = [json.dumps(bp.to_dict(), sort_keys=True) for bp in blueprints]
    tmp_jsonl.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    tmp_jsonl.replace(out_jsonl)

    tmp_yaml = out_yaml.with_suffix(".yaml.tmp")
    yaml_payload = [bp.to_dict() for bp in blueprints]
    with open(tmp_yaml, "w", encoding="utf-8") as handle:
        yaml.dump(yaml_payload, handle, sort_keys=True)
    tmp_yaml.replace(out_yaml)
