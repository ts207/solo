from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.io.utils import atomic_write_json, atomic_write_text, write_parquet
from project.research.cell_discovery.models import (
    CompileResult,
    ContextCell,
    DiscoveryRegistry,
    EventAtom,
)
from project.research.cell_discovery.paths import paths_for_run
from project.spec_validation import validate_search_spec_doc


def _context_label(context: dict[str, str] | None) -> str:
    if not context:
        return "all"
    return "__".join(f"{key}-{value}" for key, value in sorted(context.items()))


def _source_cell_id(
    *,
    atom: EventAtom,
    source_context_cell: str,
    hyp: HypothesisSpec,
) -> str:
    return "::".join(
        [
            atom.atom_id,
            source_context_cell,
            _context_label(dict(hyp.context or {})),
            hyp.direction,
            str(hyp.horizon),
            str(hyp.template_id),
        ]
    )


def _hypotheses_for_atom(atom: EventAtom, cell: ContextCell | None) -> list[HypothesisSpec]:
    contexts: list[dict[str, str] | None]
    if cell is None:
        contexts = [None]
    else:
        contexts = [{cell.dimension: value} for value in cell.values]
    out: list[HypothesisSpec] = []
    for template in atom.templates:
        for horizon in atom.horizons:
            for direction in atom.directions:
                for context in contexts:
                    out.append(
                        HypothesisSpec(
                            trigger=TriggerSpec.event(atom.event_type),
                            direction=direction,
                            horizon=horizon,
                            template_id=template,
                            context=context,
                            entry_lag=1,
                        )
                    )
    return out


def _lineage_rows(registry: DiscoveryRegistry) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cells: list[ContextCell | None] = [None, *registry.context_cells]
    for atom in registry.event_atoms:
        for cell in cells:
            source_context_cell = cell.cell_id if cell is not None else "unconditional"
            runtime_executable = cell is not None and cell.executability_class == "runtime"
            if cell is not None and cell.executability_class == "research_only":
                thesis_eligible = False
            for hyp in _hypotheses_for_atom(atom, cell):
                context = dict(hyp.context or {})
                source_cell_id = _source_cell_id(
                    atom=atom,
                    source_context_cell=source_context_cell,
                    hyp=hyp,
                )
                rows.append(
                    {
                        "hypothesis_id": hyp.hypothesis_id(),
                        "source_discovery_mode": "edge_cells",
                        "source_event_atom": atom.atom_id,
                        "source_context_cell": source_context_cell,
                        "source_context_value": _context_label(context),
                        "source_cell_id": source_cell_id,
                        "source_discovery_spec_version": registry.spec_version,
                        "event_family": atom.event_family,
                        "event_atom": atom.event_type,
                        "direction": hyp.direction,
                        "horizon": hyp.horizon,
                        "template": hyp.template_id,
                        "context_cell": cell.cell_id if cell is not None else "unconditional",
                        "context_json": json.dumps(context, sort_keys=True),
                        "context_dimension_count": len(context),
                        "runtime_executable": bool(runtime_executable),
                        "thesis_eligible": bool(atom.promotion_role == "eligible"),
                        "executability_class": (
                            "unconditional" if cell is None else cell.executability_class
                        ),
                        "supportive_context_json": json.dumps(
                            dict(cell.supportive_context) if cell is not None else {},
                            sort_keys=True,
                        ),
                    }
                )
    return rows


def _compile_search_spec(registry: DiscoveryRegistry, lineage: pd.DataFrame) -> dict[str, Any]:
    contexts: dict[str, list[str]] = {}
    if not lineage.empty and "context_json" in lineage.columns:
        for raw in lineage["context_json"].dropna().astype(str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = {}
            if not isinstance(parsed, dict):
                continue
            for key, value in parsed.items():
                dimension = str(key).strip()
                label = str(value).strip()
                if not dimension or not label:
                    continue
                contexts.setdefault(dimension, [])
                if label not in contexts[dimension]:
                    contexts[dimension].append(label)

    events = sorted(set(lineage["event_atom"].astype(str))) if not lineage.empty else []
    horizons = sorted(set(lineage["horizon"].astype(str))) if not lineage.empty else []
    directions = sorted(set(lineage["direction"].astype(str))) if not lineage.empty else []
    templates = sorted(set(lineage["template"].astype(str))) if not lineage.empty else []
    doc: dict[str, Any] = {
        "version": 1,
        "kind": "search_space",
        "metadata": {
            "phase": "edge_cells_v1",
            "description": "Generated bounded search space for cell-first discovery.",
            "source_discovery_mode": "edge_cells",
        },
        "template_policy": {
            "generic_templates_allowed": True,
            "reason": "compiled_from_discovery"
        },
        "triggers": {"events": events},
        "horizons": horizons,
        "directions": directions,
        "entry_lag": 1,
        "cost_profiles": ["standard"],
        "expression_templates": templates,
        "filter_templates": [],
        "execution_templates": [],
        "contexts": contexts,
        "include_sequences": False,
        "include_interactions": False,
        "discovery_search": {"mode": "flat"},
        "discovery_selection": {"mode": "off"},
    }
    validate_search_spec_doc(doc, source="edge_cells_generated")
    return doc


def _normalize_symbols(symbols: list[str]) -> list[str]:
    return [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]


def _feasible_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("event_atom_id", "")).strip(),
        str(row.get("context_cell", "")).strip(),
        str(row.get("symbol", "")).strip().upper(),
    )


def _lineage_key(row: pd.Series, *, symbol: str) -> tuple[str, str, str]:
    return (
        str(row.get("source_event_atom", "")).strip(),
        str(row.get("source_context_cell", "")).strip(),
        str(symbol).strip().upper(),
    )


def _filter_lineage_for_feasibility(
    lineage: pd.DataFrame,
    cell_feasibility: list[dict[str, Any]] | None,
    *,
    symbols: list[str],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    normalized_symbols = _normalize_symbols(symbols)
    if not cell_feasibility:
        frames = []
        for symbol in normalized_symbols:
            scoped = lineage.copy()
            scoped["symbol"] = symbol
            frames.append(scoped)
        if not frames:
            out = lineage.iloc[0:0].copy()
            out["symbol"] = pd.Series(dtype="object")
            return out, []
        return pd.concat(frames, ignore_index=True), []
    feasible_statuses = {"pass", "warn"}
    allowed_symbol = {
        _feasible_key(row)
        for row in cell_feasibility
        if str(row.get("status", "")).strip() in feasible_statuses
        and str(row.get("symbol", "")).strip()
    }
    allowed_global = {
        (event_atom, context_cell)
        for event_atom, context_cell, symbol in (
            _feasible_key(row)
            for row in cell_feasibility
            if str(row.get("status", "")).strip() in feasible_statuses
        )
        if not symbol
    }
    skipped = [
        dict(row)
        for row in cell_feasibility
        if str(row.get("status", "")).strip() not in feasible_statuses
    ]
    if not allowed_symbol and not allowed_global:
        out = lineage.iloc[0:0].copy()
        out["symbol"] = pd.Series(dtype="object")
        return out, skipped
    frames = []
    for symbol in normalized_symbols:
        mask = lineage.apply(
            lambda row: (
                _lineage_key(row, symbol=symbol) in allowed_symbol
                or _lineage_key(row, symbol=symbol)[:2] in allowed_global
            ),
            axis=1,
        )
        scoped = lineage[mask].copy()
        if scoped.empty:
            continue
        scoped["symbol"] = symbol
        frames.append(scoped)
    if not frames:
        out = lineage.iloc[0:0].copy()
        out["symbol"] = pd.Series(dtype="object")
        return out, skipped
    return pd.concat(frames, ignore_index=True), skipped


def compile_cells(
    *,
    registry: DiscoveryRegistry,
    run_id: str,
    data_root: Path,
    symbols: list[str],
    timeframe: str,
    start: str = "",
    end: str = "",
    cell_feasibility: list[dict[str, Any]] | None = None,
) -> CompileResult:
    paths = paths_for_run(data_root=data_root, run_id=run_id)
    paths.generated_dir.mkdir(parents=True, exist_ok=True)

    full_lineage = pd.DataFrame(_lineage_rows(registry))
    lineage, skipped_cells = _filter_lineage_for_feasibility(
        full_lineage,
        cell_feasibility,
        symbols=symbols,
    )
    estimated_count = len(lineage)
    if estimated_count > registry.ranking_policy.max_search_hypotheses:
        raise ValueError(
            "Generated edge-cell search surface exceeds max_search_hypotheses: "
            f"{estimated_count} > {registry.ranking_policy.max_search_hypotheses}"
        )

    search_spec = _compile_search_spec(registry, lineage)
    atomic_write_text(
        paths.search_spec_path,
        yaml.safe_dump(search_spec, sort_keys=False),
    )
    experiment_payload = {
        "schema_version": "edge_cell_experiment_v1",
        "run_id": run_id,
        "symbols": symbols,
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "search_spec_path": str(paths.search_spec_path),
        "lineage_path": str(paths.lineage_path),
        "skipped_cells_path": str(paths.skipped_cells_path),
        "estimated_hypothesis_count": estimated_count,
        "skipped_cell_count": len(skipped_cells),
    }
    atomic_write_json(paths.experiment_path, experiment_payload)
    atomic_write_json(
        paths.skipped_cells_path,
        {
            "schema_version": "edge_cell_skipped_cells_v1",
            "run_id": run_id,
            "skipped_cell_count": len(skipped_cells),
            "skipped_cells": skipped_cells,
        },
    )
    write_parquet(lineage, paths.lineage_path)

    if lineage.empty:
        compiled_cells = pd.DataFrame()
        family_counts: dict[str, int] = {}
    else:
        compiled_cells = lineage.drop_duplicates(["source_event_atom", "source_context_cell"])
        family_counts = dict(Counter(str(value) for value in compiled_cells["event_family"]))
    return CompileResult(
        run_id=run_id,
        search_spec_path=paths.search_spec_path,
        experiment_path=paths.experiment_path,
        lineage_path=paths.lineage_path,
        skipped_cells_path=paths.skipped_cells_path,
        estimated_hypothesis_count=estimated_count,
        cell_count=len(compiled_cells),
        family_counts=family_counts,
        skipped_cell_count=len(skipped_cells),
    )
