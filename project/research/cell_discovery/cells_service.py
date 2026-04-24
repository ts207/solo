from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from project.core.config import get_data_root
from project.research.cell_discovery.compiler import compile_cells
from project.research.cell_discovery.coverage import build_cell_coverage_audit
from project.research.cell_discovery.data_feasibility import verify_data_contract
from project.research.cell_discovery.paths import paths_for_run
from project.research.cell_discovery.redundancy import build_redundancy_clusters
from project.research.cell_discovery.registry import load_registry
from project.research.cell_discovery.scoreboard import build_scoreboard
from project.research.cell_discovery.spec_audit import build_spec_audit
from project.research.cell_discovery.thesis_assembly import assemble_theses


def _symbols(value: str | list[str] | tuple[str, ...]) -> list[str]:
    if isinstance(value, str):
        raw = value.split(",")
    else:
        raw = list(value)
    return [str(item).strip().upper() for item in raw if str(item).strip()]


def _data_root(value: str | Path | None) -> Path:
    return Path(value) if value else get_data_root()


def _skipped_cell_summary(paths) -> dict[str, Any]:
    if not paths.skipped_cells_path.exists():
        return {
            "skipped_cells_path": str(paths.skipped_cells_path),
            "skipped_cell_count": 0,
            "skipped_by_reason": {},
            "skipped_by_event_atom": {},
            "skipped_by_context_cell": {},
            "skipped_cells": [],
        }
    payload = json.loads(paths.skipped_cells_path.read_text())
    skipped_cells = list(payload.get("skipped_cells", []) or [])
    by_reason: dict[str, int] = {}
    by_event_atom: dict[str, int] = {}
    by_context_cell: dict[str, int] = {}
    for row in skipped_cells:
        if not isinstance(row, dict):
            continue
        event_atom = str(row.get("event_atom_id", "")).strip() or "unknown"
        context_cell = str(row.get("context_cell", "")).strip() or "unknown"
        by_event_atom[event_atom] = by_event_atom.get(event_atom, 0) + 1
        by_context_cell[context_cell] = by_context_cell.get(context_cell, 0) + 1
        reasons = row.get("blocked_reasons", []) or []
        if isinstance(reasons, str):
            reasons = [reasons]
        for reason in reasons:
            label = str(reason).strip() or "unknown"
            by_reason[label] = by_reason.get(label, 0) + 1
    return {
        "skipped_cells_path": str(paths.skipped_cells_path),
        "skipped_cell_count": int(payload.get("skipped_cell_count", len(skipped_cells)) or 0),
        "skipped_by_reason": dict(sorted(by_reason.items())),
        "skipped_by_event_atom": dict(sorted(by_event_atom.items())),
        "skipped_by_context_cell": dict(sorted(by_context_cell.items())),
        "skipped_cells": skipped_cells,
    }


def coverage_audit(
    *,
    spec_root: str | Path = "spec/discovery",
    search_spec: str | Path = "spec/search_space.yaml",
    event_registry: str | Path = "spec/events/event_registry_unified.yaml",
) -> dict[str, Any]:
    return build_cell_coverage_audit(
        spec_root=spec_root,
        search_spec=search_spec,
        event_registry=event_registry,
    )


def spec_audit(
    *,
    spec_dir: str | Path,
    template_registry: str | Path = "spec/templates/event_template_registry.yaml",
    verify_report: str | Path | None = None,
) -> dict[str, Any]:
    return build_spec_audit(
        spec_dir=spec_dir,
        template_registry=template_registry,
        verify_report=verify_report,
    )


def verify_data(
    *,
    run_id: str,
    symbols: str | list[str],
    timeframe: str = "5m",
    start: str = "",
    end: str = "",
    data_root: str | Path | None = None,
    spec_dir: str | Path = "spec/discovery",
) -> dict[str, Any]:
    resolved_root = _data_root(data_root)
    registry = load_registry(spec_dir)
    result = verify_data_contract(
        registry=registry,
        run_id=run_id,
        data_root=resolved_root,
        symbols=_symbols(symbols),
        timeframe=timeframe,
        start=start,
        end=end,
    )
    return {
        "exit_code": 1 if result.status == "block" else 0,
        "status": result.status,
        "report_path": str(result.report_path),
        "blocked_reasons": result.payload.get("blocked_reasons", []),
        "cell_status_counts": result.payload.get("cell_status_counts", {}),
        "support_status_counts": result.payload.get("support_status_counts", {}),
    }


def plan_cells(
    *,
    run_id: str,
    symbols: str | list[str],
    timeframe: str = "5m",
    start: str = "",
    end: str = "",
    data_root: str | Path | None = None,
    spec_dir: str | Path = "spec/discovery",
) -> dict[str, Any]:
    resolved_root = _data_root(data_root)
    registry = load_registry(spec_dir)
    feasibility = verify_data_contract(
        registry=registry,
        run_id=run_id,
        data_root=resolved_root,
        symbols=_symbols(symbols),
        timeframe=timeframe,
        start=start,
        end=end,
    )
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=resolved_root,
        symbols=_symbols(symbols),
        timeframe=timeframe,
        start=start,
        end=end,
        cell_feasibility=list(feasibility.payload.get("cell_feasibility", []) or []),
    )
    return {
        "exit_code": 1 if compiled.estimated_hypothesis_count == 0 else 0,
        "status": "planned" if compiled.estimated_hypothesis_count > 0 else "blocked_by_data",
        "data_status": feasibility.status,
        "search_spec_path": str(compiled.search_spec_path),
        "experiment_path": str(compiled.experiment_path),
        "lineage_path": str(compiled.lineage_path),
        "skipped_cells_path": str(compiled.skipped_cells_path),
        "estimated_hypothesis_count": compiled.estimated_hypothesis_count,
        "skipped_cell_count": compiled.skipped_cell_count,
        "cell_count": compiled.cell_count,
        "family_counts": compiled.family_counts,
        "cell_status_counts": feasibility.payload.get("cell_status_counts", {}),
        "support_status_counts": feasibility.payload.get("support_status_counts", {}),
        "blocked_reasons": feasibility.payload.get("blocked_reasons", []),
    }


def run_cells(
    *,
    run_id: str,
    symbols: str | list[str],
    timeframe: str = "5m",
    start: str = "",
    end: str = "",
    data_root: str | Path | None = None,
    spec_dir: str | Path = "spec/discovery",
    registry_root: str | Path = "project/configs/registries",
    search_budget: int | None = None,
) -> dict[str, Any]:
    resolved_root = _data_root(data_root)
    registry = load_registry(spec_dir)
    feasibility = verify_data_contract(
        registry=registry,
        run_id=run_id,
        data_root=resolved_root,
        symbols=_symbols(symbols),
        timeframe=timeframe,
        start=start,
        end=end,
    )
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=resolved_root,
        symbols=_symbols(symbols),
        timeframe=timeframe,
        start=start,
        end=end,
        cell_feasibility=list(feasibility.payload.get("cell_feasibility", []) or []),
    )
    if compiled.estimated_hypothesis_count <= 0:
        return {
            "exit_code": 1,
            "status": "blocked_by_data",
            "report_path": str(feasibility.report_path),
            "search_spec_path": str(compiled.search_spec_path),
            "experiment_path": str(compiled.experiment_path),
            "lineage_path": str(compiled.lineage_path),
            "skipped_cells_path": str(compiled.skipped_cells_path),
            "estimated_hypothesis_count": compiled.estimated_hypothesis_count,
            "skipped_cell_count": compiled.skipped_cell_count,
            "support_status_counts": feasibility.payload.get("support_status_counts", {}),
            "blocked_reasons": feasibility.payload.get("blocked_reasons", []),
        }
    from project.research.phase2_search_engine import run as run_phase2

    paths = paths_for_run(data_root=resolved_root, run_id=run_id)
    rc = run_phase2(
        run_id=run_id,
        symbols=",".join(_symbols(symbols)),
        data_root=resolved_root,
        out_dir=paths.run_dir,
        timeframe=timeframe,
        start=start,
        end=end,
        discovery_profile="standard",
        gate_profile="auto",
        search_spec=str(compiled.search_spec_path),
        search_budget=search_budget,
        registry_root=registry_root,
        discovery_mode="edge_cells",
        lineage_path=str(compiled.lineage_path),
    )
    if int(rc) != 0:
        return {
            "exit_code": int(rc),
            "status": "failed",
            "phase2_returncode": int(rc),
            "run_dir": str(paths.run_dir),
        }

    # Canonical contract: do not emit the main scoreboard unless fold-forward
    # evidence exists (phase2_candidate_fold_metrics.parquet).
    contract_error_path = paths.run_dir / "edge_scoreboard_contract_error.json"
    try:
        summary = build_scoreboard(
            registry=registry,
            run_id=run_id,
            data_root=resolved_root,
            timeframe=timeframe,
        )
    except Exception as e:
        payload = {
            "schema_version": "edge_scoreboard_contract_error_v1",
            "run_id": run_id,
            "status": "contract_error",
            "error": str(e),
        }
        contract_error_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
        return {
            "exit_code": 1,
            "status": "contract_error",
            "phase2_returncode": int(rc),
            "run_dir": str(paths.run_dir),
            "contract_error_path": str(contract_error_path),
            "error": str(e),
        }

    clusters = build_redundancy_clusters(run_id=run_id, data_root=resolved_root)
    return {
        "exit_code": int(rc),
        "status": "executed",
        "phase2_returncode": int(rc),
        "scoreboard_summary": summary,
        "cluster_summary": clusters,
        "run_dir": str(paths.run_dir),
    }


def summarize_cells(
    *,
    run_id: str,
    data_root: str | Path | None = None,
) -> dict[str, Any]:
    resolved_root = _data_root(data_root)
    paths = paths_for_run(data_root=resolved_root, run_id=run_id)
    skipped_summary = _skipped_cell_summary(paths)
    if not paths.summary_path.exists() and not paths.skipped_cells_path.exists():
        return {
            "exit_code": 1,
            "status": "missing",
            "reason": f"scoreboard summary not found: {paths.summary_path}",
        }
    summary = json.loads(paths.summary_path.read_text()) if paths.summary_path.exists() else {}
    return {
        "exit_code": 0,
        "status": "ok",
        **summary,
        **skipped_summary,
    }


def assemble_cell_theses(
    *,
    run_id: str,
    data_root: str | Path | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    resolved_root = _data_root(data_root)
    paths = paths_for_run(data_root=resolved_root, run_id=run_id)
    if not paths.cluster_representatives_path.exists():
        build_redundancy_clusters(run_id=run_id, data_root=resolved_root, shortlist_size=limit)
    report = assemble_theses(run_id=run_id, data_root=resolved_root, limit=limit)
    return {"exit_code": 0, "status": "ok", **report}
