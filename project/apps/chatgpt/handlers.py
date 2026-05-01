from __future__ import annotations

import datetime as dt
import json
import logging
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

import anyio
import pandas as pd

from project.apps.chatgpt import handler_utils as _handler_utils
from project.artifacts import run_manifest_path
from project.operator.preflight import run_preflight
from project.operator.proposal_tools import explain_proposal, lint_proposal
from project.operator.stability import (
    build_negative_result_diagnostics,
    build_regime_split_report,
    build_time_slice_report,
)
from project.research.agent_io.issue_proposal import issue_proposal
from project.research.agent_io.proposal_to_experiment import translate_and_validate_proposal

_LOG = logging.getLogger(__name__)


_path_or_none = _handler_utils._path_or_none
_scratch_dir = _handler_utils._scratch_dir
_resolve_data_root = _handler_utils._resolve_data_root
_read_json_dict = _handler_utils._read_json_dict
_invalid_run_summary = _handler_utils._invalid_run_summary
_read_table = _handler_utils._read_table
_clean_value = _handler_utils._clean_value
_sort_records = _handler_utils._sort_records
_safe_int = _handler_utils._safe_int
_safe_float = _handler_utils._safe_float
_first_present = _handler_utils._first_present
_per_trade_to_bps = _handler_utils._per_trade_to_bps
_repo_root = _handler_utils._repo_root
_coerce_text = _handler_utils._coerce_text
_normalize_timeout_sec = _handler_utils._normalize_timeout_sec
_normalize_limit = _handler_utils._normalize_limit
_parse_json_like = _handler_utils._parse_json_like
_normalize_summary = _handler_utils._normalize_summary
_normalize_sections = _handler_utils._normalize_sections


def _memory_root(program_id: str, data_root: Path) -> Path:
    return data_root / "artifacts" / "experiments" / str(program_id) / "memory"


def _dashboard_status(run_summary: dict[str, Any]) -> str | None:
    status = str(run_summary.get("status") or "").strip().lower()
    if status in {"success", "executed", "completed"}:
        return "pass"
    if status in {"warning", "warn"} or str(run_summary.get("mechanical_outcome") or "").strip().lower() == "warning_only":
        return "warn"
    if status in {"failed", "error", "aborted_stale_run"}:
        return "fail"
    return None


def _limit_frame(df: pd.DataFrame, *, sort_by: str, limit: int) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if sort_by in out.columns:
        out = out.sort_values(by=sort_by, ascending=False, na_position="last")
    return out.head(limit)


def _project_program_ids(data_root: Path, run_summaries: list[dict[str, Any]]) -> list[str]:
    program_ids = {
        str(run.get("program_id") or "").strip()
        for run in run_summaries
        if str(run.get("program_id") or "").strip()
    }
    experiments_root = data_root / "artifacts" / "experiments"
    if experiments_root.exists():
        for path in sorted(experiments_root.iterdir()):
            if path.is_dir() and (path / "memory").exists():
                program_ids.add(path.name)
    return sorted(program_ids)


def _proposal_records(program_id: str, data_root: Path, *, limit: int) -> tuple[int, list[dict[str, Any]]]:
    proposals_path = _memory_root(program_id, data_root) / "proposals.parquet"
    proposals = _read_table(proposals_path)
    if proposals.empty:
        return 0, []
    for column in (
        "run_id",
        "status",
        "issued_at",
        "objective_name",
        "promotion_profile",
        "symbols",
        "experiment_type",
        "allowed_change_field",
        "baseline_run_id",
        "decision",
        "mutation_type",
        "plan_only",
        "dry_run",
        "returncode",
        "proposal_path",
    ):
        if column not in proposals.columns:
            proposals[column] = None
    rows = _limit_frame(proposals, sort_by="issued_at", limit=limit)[
        [
            "run_id",
            "status",
            "issued_at",
            "objective_name",
            "promotion_profile",
            "symbols",
            "experiment_type",
            "allowed_change_field",
            "baseline_run_id",
            "decision",
            "mutation_type",
            "plan_only",
            "dry_run",
            "returncode",
            "proposal_path",
        ]
    ].to_dict("records")
    return len(proposals.index), [_clean_value(row) for row in rows]


def _memory_snapshot(program_id: str, data_root: Path, *, limit: int) -> dict[str, Any]:
    root = _memory_root(program_id, data_root)
    belief_state = _read_json_dict(root / "belief_state.json")
    next_actions = _read_json_dict(root / "next_actions.json")
    reflections = _read_table(root / "reflections.parquet")
    evidence = _read_table(root / "evidence_ledger.parquet")

    if not reflections.empty:
        for column in (
            "created_at",
            "run_id",
            "run_status",
            "market_findings",
            "system_findings",
            "recommended_next_action",
            "recommended_next_experiment",
            "confidence",
        ):
            if column not in reflections.columns:
                reflections[column] = None
        reflection_rows = _limit_frame(reflections, sort_by="created_at", limit=limit)[
            [
                "created_at",
                "run_id",
                "run_status",
                "market_findings",
                "system_findings",
                "recommended_next_action",
                "recommended_next_experiment",
                "confidence",
            ]
        ].to_dict("records")
    else:
        reflection_rows = []

    if not evidence.empty:
        for column in (
            "updated_at",
            "run_id",
            "verdict",
            "recommended_next_action",
            "recommended_next_experiment",
            "terminal_status",
            "promoted_count",
            "candidate_count",
            "negative_diagnosis",
        ):
            if column not in evidence.columns:
                evidence[column] = None
        evidence_rows = _limit_frame(evidence, sort_by="updated_at", limit=limit)[
            [
                "updated_at",
                "run_id",
                "verdict",
                "recommended_next_action",
                "recommended_next_experiment",
                "terminal_status",
                "promoted_count",
                "candidate_count",
                "negative_diagnosis",
            ]
        ].to_dict("records")
    else:
        evidence_rows = []

    return {
        "available": root.exists(),
        "belief_state": _clean_value(belief_state),
        "next_actions": _clean_value(next_actions),
        "recent_reflections": [_clean_value(row) for row in reflection_rows],
        "recent_evidence": [_clean_value(row) for row in evidence_rows],
        "paths": {
            "root": str(root),
            "belief_state": str(root / "belief_state.json"),
            "next_actions": str(root / "next_actions.json"),
            "proposals": str(root / "proposals.parquet"),
        },
    }


def _run_summary(manifest: dict[str, Any], run_id_hint: str | None = None) -> dict[str, Any]:
    run_id = str(manifest.get("run_id") or run_id_hint or "").strip()
    symbols = manifest.get("normalized_symbols") or []
    if not isinstance(symbols, list):
        symbols = []
    return _clean_value(
        {
            "run_id": run_id,
            "program_id": manifest.get("program_id"),
            "status": manifest.get("status") or manifest.get("run_status"),
            "mechanical_outcome": manifest.get("mechanical_outcome"),
            "checklist_decision": manifest.get("checklist_decision"),
            "failed_stage": manifest.get("failed_stage"),
            "objective_name": manifest.get("objective_name"),
            "objective_id": manifest.get("objective_id"),
            "promotion_profile": manifest.get("promotion_profile"),
            "experiment_type": manifest.get("experiment_type"),
            "start": manifest.get("start"),
            "end": manifest.get("end"),
            "finished_at": manifest.get("finished_at"),
            "started_at": manifest.get("started_at"),
            "planned_stage_count": manifest.get("planned_stage_count"),
            "completed_stage_count": manifest.get("completed_stage_count"),
            "artifact_count": manifest.get("artifact_count"),
            "candidate_count": manifest.get("candidate_count") or manifest.get("exported_candidate_count"),
            "promoted_count": manifest.get("promoted_count"),
            "normalized_symbols": symbols,
            "normalized_timeframes": manifest.get("normalized_timeframes") or [],
            "symbols_label": ", ".join(str(symbol) for symbol in symbols[:4]),
        }
    )


def _recent_run_summaries(data_root: Path, *, program_id: str | None = None, limit: int) -> list[dict[str, Any]]:
    runs_root = data_root / "runs"
    if not runs_root.exists():
        return []

    # Fast glob + limit early if we don't have a program filter
    # If we HAVE a program filter, we must scan more to find matching runs
    records: list[dict[str, Any]] = []

    # Sort manifest paths by mtime to get recent ones first without reading all
    try:
        manifest_paths = sorted(
            runs_root.glob("*/run_manifest.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        manifest_paths = list(runs_root.glob("*/run_manifest.json"))

    scan_cap = None if program_id else 100
    manifest_iterable = manifest_paths if scan_cap is None else manifest_paths[:scan_cap]

    for manifest_path in manifest_iterable:
        manifest = _read_json_dict(manifest_path, include_errors=True)
        if not manifest:
            continue
        if manifest.get("__invalid_json__"):
            summary = _invalid_run_summary(manifest_path, manifest)
        else:
            summary = _run_summary(manifest, manifest_path.parent.name)
        if program_id and str(summary.get("program_id") or "").strip() != str(program_id).strip():
            continue
        records.append(summary)
        if len(records) >= limit:
            break

    return _sort_records(records, "finished_at", "started_at", "run_id")


def _selected_run_snapshot(run_id: str | None, data_root: Path) -> dict[str, Any]:
    resolved_run_id = str(run_id or "").strip()
    if not resolved_run_id:
        return {}
    manifest_path = run_manifest_path(resolved_run_id, data_root)
    manifest = _read_json_dict(manifest_path, include_errors=True)
    if not manifest:
        return {}
    if manifest.get("__invalid_json__"):
        return _invalid_run_summary(manifest_path, manifest)
    summary = _run_summary(manifest, resolved_run_id)
    summary["effective_behavior"] = _clean_value(manifest.get("effective_behavior") or {})
    summary["objective_hard_gates"] = _clean_value(manifest.get("objective_hard_gates") or {})
    summary["paths"] = _clean_value(
        {
            "manifest": str(run_manifest_path(resolved_run_id, data_root)),
            "effective_config": manifest.get("effective_config_path"),
            "objective_spec": manifest.get("objective_spec_path"),
            "experiment_config": (
                (manifest.get("config_resolution") or {}).get("experiment_config_path")
                if isinstance(manifest.get("config_resolution"), dict)
                else None
            ),
        }
    )
    summary["planned_stage_instances"] = _clean_value(list(manifest.get("planned_stage_instances") or [])[:12])
    return summary


def _candidate_paths(run_id: str, data_root: Path) -> dict[str, Path]:
    return {
        "phase2": data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet",
        "edge": data_root / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet",
        "promotion_summary": data_root / "reports" / "promotions" / run_id / "promotion_summary.csv",
        "phase2_diagnostics": data_root / "reports" / "phase2" / run_id / "phase2_diagnostics.json",
    }


def _candidate_label(row: dict[str, Any]) -> str:
    event_type = str(_first_present(row, "event_type", "event", "trigger_type") or "unknown_event")
    template = str(_first_present(row, "template_verb", "rule_template", "template_id", "template") or "unknown_template")
    direction = str(_first_present(row, "direction") or "?")
    horizon = str(_first_present(row, "horizon", "horizon_bars", "horizon_label") or "?")
    return " / ".join([event_type, template, direction, horizon])


def _candidate_metric_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    raw_bps = _safe_float(_first_present(row, "mean_return_bps", "bridge_gross_edge_bps_per_trade"))
    if raw_bps is None:
        raw_bps = _per_trade_to_bps(_first_present(row, "expectancy", "expectancy_per_trade", "expected_return_proxy"))
    after_cost_bps = _safe_float(_first_present(row, "cost_adjusted_return_bps", "bridge_train_after_cost_bps"))
    if after_cost_bps is None:
        after_cost_bps = _per_trade_to_bps(_first_present(row, "after_cost_expectancy_per_trade"))
    return {
        "raw_bps": raw_bps,
        "after_cost_bps": after_cost_bps,
        "t_stat": _safe_float(_first_present(row, "t_stat", "t_value")),
        "q_value": _safe_float(_first_present(row, "q_value", "q_value_family", "q_value_cluster")),
        "n_events": _safe_int(_first_present(row, "n_events", "sample_size", "n")),
    }


def _candidate_row_score(row: dict[str, Any]) -> tuple[int, int, float, float, float]:
    tradable = bool(
        _first_present(row, "gate_bridge_tradable") is True
        or str(_first_present(row, "bridge_eval_status", "status") or "").strip().lower() == "tradable"
    )
    promoted = str(_first_present(row, "status") or "").strip().lower() == "promoted"
    metrics = _candidate_metric_snapshot(row)
    t_stat = metrics["t_stat"] if metrics["t_stat"] is not None else float("-inf")
    after_cost_bps = metrics["after_cost_bps"] if metrics["after_cost_bps"] is not None else float("-inf")
    raw_bps = metrics["raw_bps"] if metrics["raw_bps"] is not None else float("-inf")
    return (1 if promoted else 0, 1 if tradable else 0, t_stat, after_cost_bps, raw_bps)


def _best_candidate_row(df: pd.DataFrame, *, source: str) -> dict[str, Any]:
    if df.empty:
        return {}
    records = [_clean_value(record) for record in df.to_dict("records")]
    best_row = max(records, key=_candidate_row_score)
    metrics = _candidate_metric_snapshot(best_row)
    return {
        "source": source,
        "candidate_id": _first_present(best_row, "candidate_id"),
        "symbol": _first_present(best_row, "symbol", "candidate_symbol"),
        "label": _candidate_label(best_row),
        "event_type": _first_present(best_row, "event_type", "event", "trigger_type"),
        "template": _first_present(best_row, "template_verb", "rule_template", "template_id", "template"),
        "direction": _first_present(best_row, "direction"),
        "horizon": _first_present(best_row, "horizon", "horizon_bars", "horizon_label"),
        "bridge_eval_status": _first_present(best_row, "bridge_eval_status", "status"),
        "gate_bridge_tradable": bool(_first_present(best_row, "gate_bridge_tradable") is True),
        "primary_fail_gate": _first_present(best_row, "promotion_fail_gate_primary", "primary_fail_gate"),
        **metrics,
    }


def _dominant_rejection_reason(diagnostics: dict[str, Any]) -> str | None:
    counts = diagnostics.get("rejection_reason_counts")
    if not isinstance(counts, dict) or not counts:
        return None
    sorted_counts = sorted(
        ((str(key), _safe_int(value)) for key, value in counts.items()),
        key=lambda item: item[1],
        reverse=True,
    )
    return sorted_counts[0][0] if sorted_counts and sorted_counts[0][1] > 0 else None


def _candidate_pipeline_status(
    *,
    promoted_count: int,
    tradable_count: int,
    phase2_count: int,
    edge_count: int,
    bridge_candidates_rows: int,
    dominant_reject_reason: str | None,
) -> str:
    if promoted_count > 0:
        return "promoted"
    if tradable_count > 0:
        return "tradable"
    if phase2_count > 0 and bridge_candidates_rows > 0:
        return "bridge_reject"
    if phase2_count > 0:
        return "phase2_survivor"
    if edge_count > 0:
        return "edge_candidate"
    if dominant_reject_reason:
        return "search_reject"
    return "no_signal"


def _candidate_pipeline_note(
    *,
    pipeline_status: str,
    promoted_count: int,
    tradable_count: int,
    dominant_reject_reason: str | None,
    best_candidate: dict[str, Any],
) -> str:
    if pipeline_status == "promoted":
        return f"{promoted_count} promoted candidate(s)"
    if pipeline_status == "tradable":
        return f"{tradable_count} bridge-tradable candidate(s)"
    if pipeline_status == "bridge_reject":
        fail_gate = str(best_candidate.get("primary_fail_gate") or "").strip()
        return f"Search pass, bridge reject{': ' + fail_gate if fail_gate else ''}"
    if pipeline_status == "phase2_survivor":
        return "Phase-2 survivor awaiting bridge confirmation"
    if pipeline_status == "edge_candidate":
        return "Exported edge candidate without promotion"
    if pipeline_status == "search_reject" and dominant_reject_reason:
        return f"Rejected at {dominant_reject_reason}"
    return "No live candidate artifacts"


def _run_candidate_snapshot(run_summary: dict[str, Any], data_root: Path) -> dict[str, Any]:
    run_id = str(run_summary.get("run_id") or "").strip()
    if not run_id:
        return {}
    paths = _candidate_paths(run_id, data_root)

    # Performance: only attempt to read parquets if at least one exists
    # Checking file existence is much faster than failing parquet open/reads
    if not any(p.exists() for p in paths.values()):
        return _clean_value(
            {
                "run_id": run_id,
                "program_id": run_summary.get("program_id"),
                "run_status": run_summary.get("status"),
                "pipeline_status": "no_artifacts",
                "pipeline_note": "No candidate artifacts found for this run.",
            }
        )

    phase2 = _read_table(paths["phase2"])
    edge = _read_table(paths["edge"])
    promotion_summary = _read_table(paths["promotion_summary"])
    diagnostics = _read_json_dict(paths["phase2_diagnostics"])

    phase2_count = len(phase2.index)
    edge_count = len(edge.index)
    promotion_summary_rows = len(promotion_summary.index)
    promoted_count = _safe_int(run_summary.get("promoted_count"))
    phase2_best = _best_candidate_row(phase2, source="phase2_candidates")
    edge_best = _best_candidate_row(edge, source="edge_candidates")
    best_candidate = edge_best or phase2_best

    tradable_count = 0
    if phase2_count:
        phase2_tradable = phase2.copy()
        if "gate_bridge_tradable" in phase2_tradable.columns:
            tradable_count = int(phase2_tradable["gate_bridge_tradable"].fillna(False).astype(bool).sum())
        elif "bridge_eval_status" in phase2_tradable.columns:
            tradable_count = int(
                phase2_tradable["bridge_eval_status"].astype(str).str.lower().eq("tradable").sum()
            )

    bridge_candidates_rows = _safe_int(diagnostics.get("bridge_candidates_rows"))
    dominant_reject_reason = _dominant_rejection_reason(diagnostics)
    pipeline_status = _candidate_pipeline_status(
        promoted_count=promoted_count,
        tradable_count=tradable_count,
        phase2_count=phase2_count,
        edge_count=edge_count,
        bridge_candidates_rows=bridge_candidates_rows,
        dominant_reject_reason=dominant_reject_reason,
    )
    return _clean_value(
        {
            "run_id": run_id,
            "program_id": run_summary.get("program_id"),
            "finished_at": run_summary.get("finished_at") or run_summary.get("started_at"),
            "run_status": run_summary.get("status"),
            "pipeline_status": pipeline_status,
            "pipeline_note": _candidate_pipeline_note(
                pipeline_status=pipeline_status,
                promoted_count=promoted_count,
                tradable_count=tradable_count,
                dominant_reject_reason=dominant_reject_reason,
                best_candidate=best_candidate,
            ),
            "phase2_candidate_count": phase2_count,
            "edge_candidate_count": edge_count,
            "bridge_candidate_count": bridge_candidates_rows,
            "promotion_summary_rows": promotion_summary_rows,
            "tradable_count": tradable_count,
            "promoted_count": promoted_count,
            "dominant_reject_reason": dominant_reject_reason,
            "best_candidate": best_candidate,
            "gate_funnel": _clean_value(diagnostics.get("gate_funnel") or {}),
        }
    )


def _candidate_board(
    run_summaries: list[dict[str, Any]],
    *,
    data_root: Path,
    limit: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    # Only compute snapshots for the subset of runs actually shown in the board
    board = [_run_candidate_snapshot(run_summary, data_root) for run_summary in run_summaries[:limit]]
    status_counts = Counter(str(row.get("pipeline_status") or "") for row in board)
    return (
        {
            "runs_tracked": len(board),
            "phase2_survivors": sum(_safe_int(row.get("phase2_candidate_count")) for row in board),
            "bridge_tradable": sum(_safe_int(row.get("tradable_count")) for row in board),
            "promoted": sum(_safe_int(row.get("promoted_count")) for row in board),
            "bridge_reject_runs": status_counts.get("bridge_reject", 0),
            "search_reject_runs": status_counts.get("search_reject", 0),
        },
        board,
    )


def _import_codex_mcp_runtime() -> tuple[Any, Any, Any]:
    try:
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client
    except ImportError as exc:  # pragma: no cover - runtime dependency path
        raise RuntimeError(
            "The Python MCP SDK is required to proxy Codex through ChatGPT Apps. Install it with `pip install \"mcp[cli]\"`."
        ) from exc
    return ClientSession, StdioServerParameters, stdio_client


def _codex_text_content(items: list[Any] | None) -> str:
    messages: list[str] = []
    for item in list(items or []):
        if str(getattr(item, "type", "") or "") != "text":
            continue
        text = str(getattr(item, "text", "") or "").strip()
        if text:
            messages.append(text)
    return "\n\n".join(messages)


async def _run_codex_mcp_tool(
    codex_path: str,
    tool_name: str,
    arguments: dict[str, Any],
    timeout_sec: int,
) -> dict[str, Any]:
    ClientSession, StdioServerParameters, stdio_client = _import_codex_mcp_runtime()
    server = StdioServerParameters(
        command=str(codex_path),
        args=["mcp-server"],
        cwd=str(_repo_root()),
    )

    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            with anyio.fail_after(timeout_sec):
                await session.initialize()
                result = await session.call_tool(
                    tool_name,
                    arguments=arguments,
                    read_timeout_seconds=dt.timedelta(seconds=timeout_sec),
                )

    structured_content = _clean_value(result.structuredContent or {})
    thread_id = str(structured_content.get("threadId") or "").strip() or None
    final_message = str(structured_content.get("content") or "").strip() or _codex_text_content(result.content)
    return {
        "tool_name": tool_name,
        "thread_id": thread_id,
        "final_message": final_message,
        "structured_content": structured_content,
        "content": _clean_value([item.model_dump(mode="json") for item in list(result.content or [])]),
        "is_error": bool(result.isError),
    }


def _snapshot_operator_state(data_root: Path) -> dict[str, Any]:
    # Faster snapshot: only look at the most recent 20 runs
    recent_runs = _recent_run_summaries(data_root, limit=20)
    proposal_counts: dict[str, int] = {}
    for program_id in list(_project_program_ids(data_root, recent_runs))[:15]:
        proposal_count, _ = _proposal_records(program_id, data_root, limit=1)
        proposal_counts[str(program_id)] = int(proposal_count)
    return {
        "data_root": str(data_root),
        "recent_run_ids": [
            str(run.get("run_id"))
            for run in recent_runs
            if str(run.get("run_id") or "").strip()
        ],
        "proposal_counts": proposal_counts,
    }


def _diff_operator_state(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_run_ids = {
        str(run_id)
        for run_id in list(before.get("recent_run_ids") or [])
        if str(run_id).strip()
    }
    after_run_ids = [
        str(run_id)
        for run_id in list(after.get("recent_run_ids") or [])
        if str(run_id).strip()
    ]
    new_run_ids = [run_id for run_id in after_run_ids if run_id not in before_run_ids]

    before_counts = {
        str(program_id): int(count)
        for program_id, count in dict(before.get("proposal_counts") or {}).items()
    }
    after_counts = {
        str(program_id): int(count)
        for program_id, count in dict(after.get("proposal_counts") or {}).items()
    }
    proposal_memory_changes: list[dict[str, Any]] = []
    for program_id in sorted(set(before_counts) | set(after_counts)):
        before_count = int(before_counts.get(program_id, 0))
        after_count = int(after_counts.get(program_id, 0))
        if after_count > before_count:
            proposal_memory_changes.append(
                {
                    "program_id": program_id,
                    "before_count": before_count,
                    "after_count": after_count,
                    "delta": after_count - before_count,
                }
            )

    return {
        "data_root": str(after.get("data_root") or before.get("data_root") or ""),
        "new_run_ids": new_run_ids,
        "proposal_memory_changes": proposal_memory_changes,
        "dashboard_changed": bool(new_run_ids or proposal_memory_changes),
    }


def invoke_codex_operator(
    *,
    task: str,
    thread_id: str | None = None,
    sandbox: str = "workspace-write",
    model: str | None = None,
    profile: str | None = None,
    timeout_sec: int = 300,
) -> dict[str, Any]:
    codex_path = shutil.which("codex")
    if codex_path is None:
        raise RuntimeError("The `codex` CLI is not installed or not on PATH.")
    normalized_timeout_sec = _normalize_timeout_sec(timeout_sec)
    normalized_thread_id = str(thread_id or "").strip() or None
    resolved_data_root = _resolve_data_root(None)
    before_state = _snapshot_operator_state(resolved_data_root)
    tool_name = "codex-reply" if normalized_thread_id else "codex"
    arguments: dict[str, Any] = {"prompt": str(task)}
    if normalized_thread_id:
        arguments["threadId"] = normalized_thread_id
    else:
        arguments["cwd"] = str(_repo_root())
        arguments["sandbox"] = str(sandbox)
        if model:
            arguments["model"] = str(model)
        if profile:
            arguments["profile"] = str(profile)

    result_payload: dict[str, Any] = {}
    timeout_hit = False
    stderr_text = ""
    try:
        result_payload = anyio.run(
            _run_codex_mcp_tool,
            str(codex_path),
            tool_name,
            arguments,
            normalized_timeout_sec,
        )
    except TimeoutError:
        timeout_hit = True
        result_payload = {
            "tool_name": tool_name,
            "thread_id": normalized_thread_id,
            "final_message": "",
            "structured_content": {},
            "content": [],
            "is_error": False,
        }
    except Exception as exc:
        stderr_text = _coerce_text(exc).strip()
        result_payload = {
            "tool_name": tool_name,
            "thread_id": normalized_thread_id,
            "final_message": "",
            "structured_content": {},
            "content": [],
            "is_error": True,
        }

    after_state = _snapshot_operator_state(resolved_data_root)
    post_run_probe = _diff_operator_state(before_state, after_state)
    success = bool(result_payload) and not bool(result_payload.get("is_error"))

    return {
        "status": "timeout" if timeout_hit else "success" if success else "failed",
        "exit_code": None,
        "task": str(task),
        "tool_name": tool_name,
        "sandbox": str(sandbox),
        "model": str(model) if model else None,
        "profile": str(profile) if profile else None,
        "timeout_sec": normalized_timeout_sec,
        "timed_out": timeout_hit,
        "thread_id": result_payload.get("thread_id"),
        "final_message": result_payload.get("final_message"),
        "structured_content": result_payload.get("structured_content") or {},
        "content": result_payload.get("content") or [],
        "is_error": bool(result_payload.get("is_error")),
        "stderr": stderr_text or None,
        "usage": {},
        "event_types": [],
        "post_run_probe": post_run_probe,
    }


def get_operator_dashboard(
    *,
    program_id: str | None = None,
    run_id: str | None = None,
    data_root: str | None = None,
    limit: int = 8,
) -> dict[str, Any]:
    resolved_data_root = _resolve_data_root(data_root)
    normalized_limit = _normalize_limit(limit)
    initial_runs = _recent_run_summaries(resolved_data_root, limit=40)
    selected_run = _selected_run_snapshot(run_id, resolved_data_root)

    requested_program = str(program_id or "").strip() or str(selected_run.get("program_id") or "").strip()
    program_ids = _project_program_ids(resolved_data_root, initial_runs)

    program_cards: list[dict[str, Any]] = []
    for candidate_program in program_ids:
        proposal_count, candidate_proposals = _proposal_records(candidate_program, resolved_data_root, limit=1)
        candidate_runs = [row for row in initial_runs if str(row.get("program_id") or "") == candidate_program]
        latest_run = candidate_runs[0] if candidate_runs else {}
        memory = _memory_snapshot(candidate_program, resolved_data_root, limit=min(normalized_limit, 3))
        next_actions = memory.get("next_actions") if isinstance(memory.get("next_actions"), dict) else {}
        belief_state = memory.get("belief_state") if isinstance(memory.get("belief_state"), dict) else {}
        program_cards.append(
            _clean_value(
                {
                    "program_id": candidate_program,
                    "proposal_count": proposal_count,
                    "recent_run_count": len(candidate_runs),
                    "latest_proposal_at": (candidate_proposals[0].get("issued_at") if candidate_proposals else None),
                    "latest_run_id": latest_run.get("run_id"),
                    "latest_run_status": latest_run.get("status"),
                    "latest_run_finished_at": latest_run.get("finished_at") or latest_run.get("started_at"),
                    "repair_count": len(list((next_actions or {}).get("repair") or [])),
                    "exploit_count": len(list((next_actions or {}).get("exploit") or [])),
                    "promising_region_count": len(list((belief_state or {}).get("promising_regions") or [])),
                }
            )
        )

    program_cards = _sort_records(program_cards, "latest_run_finished_at", "latest_proposal_at", "program_id")
    active_program_id = requested_program or (program_cards[0]["program_id"] if program_cards else "")
    recent_runs = _recent_run_summaries(
        resolved_data_root,
        program_id=active_program_id or None,
        limit=normalized_limit,
    )
    candidate_summary, candidate_board = _candidate_board(
        recent_runs,
        data_root=resolved_data_root,
        limit=normalized_limit,
    )
    proposal_count = 0
    recent_proposals: list[dict[str, Any]] = []
    memory: dict[str, Any] = {}
    if active_program_id:
        proposal_count, recent_proposals = _proposal_records(
            active_program_id,
            resolved_data_root,
            limit=normalized_limit,
        )
        memory = _memory_snapshot(active_program_id, resolved_data_root, limit=normalized_limit)

    if not selected_run and recent_runs:
        selected_run = _selected_run_snapshot(str(recent_runs[0].get("run_id") or ""), resolved_data_root)
    if selected_run:
        selected_run["candidate_snapshot"] = _run_candidate_snapshot(selected_run, resolved_data_root)

    current_status = _dashboard_status(selected_run or {})
    subtitle = (
        f"Memory and prior results at {resolved_data_root}"
        if active_program_id
        else f"No program selected. Showing available Edge history at {resolved_data_root}"
    )

    return {
        "layout": "dashboard",
        "title": active_program_id or "Edge Operator Dashboard",
        "status": current_status,
        "subtitle": subtitle,
        "summary": {
            "active_program": active_program_id or "none",
            "known_programs": len(program_cards),
            "recent_runs": len(recent_runs),
            "recent_proposals": proposal_count,
            "phase2_survivors": candidate_summary["phase2_survivors"],
            "bridge_tradable": candidate_summary["bridge_tradable"],
            "promoted": candidate_summary["promoted"],
            "selected_run": selected_run.get("run_id") or "none",
        },
        "data_root": str(resolved_data_root),
        "active_program_id": active_program_id or None,
        "programs": program_cards,
        "candidate_summary": candidate_summary,
        "candidate_board": candidate_board,
        "memory": memory,
        "recent_proposals": recent_proposals,
        "recent_runs": recent_runs,
        "selected_run": selected_run,
        "source_tool": "edge_get_operator_dashboard",
    }


def preflight_proposal(
    *,
    proposal: str,
    registry_root: str = "project/configs/registries",
    data_root: str | None = None,
    out_dir: str | None = None,
    json_output: str | None = None,
) -> dict[str, Any]:
    if out_dir:
        _handler_utils.guard_mutation_path(out_dir)
    if json_output:
        _handler_utils.guard_mutation_path(json_output)
    with _scratch_dir(out_dir) as scratch_dir:
        return run_preflight(
            proposal_path=proposal,
            registry_root=registry_root,
            data_root=data_root,
            out_dir=scratch_dir,
            json_output=json_output,
        )


def explain_proposal_summary(
    *,
    proposal: str,
    registry_root: str = "project/configs/registries",
    data_root: str | None = None,
    out_dir: str | None = None,
) -> dict[str, Any]:
    if out_dir:
        _handler_utils.guard_mutation_path(out_dir)
    with _scratch_dir(out_dir) as scratch_dir:
        return explain_proposal(
            proposal_path=proposal,
            registry_root=registry_root,
            data_root=data_root,
            out_dir=scratch_dir,
        )


def lint_proposal_summary(
    *,
    proposal: str,
    registry_root: str = "project/configs/registries",
    data_root: str | None = None,
    out_dir: str | None = None,
) -> dict[str, Any]:
    if out_dir:
        _handler_utils.guard_mutation_path(out_dir)
    with _scratch_dir(out_dir) as scratch_dir:
        return lint_proposal(
            proposal_path=proposal,
            registry_root=registry_root,
            data_root=data_root,
            out_dir=scratch_dir,
        )


def preview_plan(
    *,
    proposal: str,
    registry_root: str = "project/configs/registries",
    data_root: str | None = None,
    out_dir: str | None = None,
    include_experiment_config: bool = True,
) -> dict[str, Any]:
    del data_root
    if out_dir:
        _handler_utils.guard_mutation_path(out_dir)
    with _scratch_dir(out_dir) as scratch_dir:
        translated = translate_and_validate_proposal(
            proposal,
            registry_root=Path(registry_root),
            out_dir=scratch_dir,
            config_path=scratch_dir / "proposal_preview.yaml",
        )
    payload = {
        "proposal": translated["proposal"],
        "validated_plan": translated["validated_plan"],
        "run_all_overrides": translated["run_all_overrides"],
        "ephemeral": True,
    }
    if include_experiment_config:
        payload["experiment_config"] = translated["experiment_config"]
    return payload


def issue_plan(
    *,
    proposal: str,
    registry_root: str = "project/configs/registries",
    data_root: str | None = None,
    run_id: str | None = None,
    check: bool = False,
) -> dict[str, Any]:
    return issue_proposal(
        proposal,
        registry_root=Path(registry_root),
        data_root=_path_or_none(data_root),
        run_id=run_id,
        plan_only=True,
        dry_run=False,
        check=check,
    )


def issue_run(
    *,
    proposal: str,
    registry_root: str = "project/configs/registries",
    data_root: str | None = None,
    run_id: str | None = None,
    check: bool = False,
) -> dict[str, Any]:
    return issue_proposal(
        proposal,
        registry_root=Path(registry_root),
        data_root=_path_or_none(data_root),
        run_id=run_id,
        plan_only=False,
        dry_run=False,
        check=check,
    )


def get_negative_result_diagnostics(
    *,
    run_id: str,
    program_id: str | None = None,
    data_root: str | None = None,
) -> dict[str, Any]:
    return build_negative_result_diagnostics(
        run_id=run_id,
        program_id=program_id,
        data_root=_path_or_none(data_root),
    )


def get_regime_report(
    *,
    run_id: str,
    data_root: str | None = None,
) -> dict[str, Any]:
    return build_regime_split_report(
        run_id=run_id,
        data_root=_path_or_none(data_root),
    )


def compare_runs(
    *,
    run_ids: list[str],
    program_id: str | None = None,
    data_root: str | None = None,
) -> dict[str, Any]:
    # Constraint: max 6 runs to avoid massive parquet scan latencies
    clamped_run_ids = list(run_ids)[:6]
    return build_time_slice_report(
        run_ids=clamped_run_ids,
        program_id=program_id,
        data_root=_path_or_none(data_root),
    )


def get_memory_summary(
    *,
    program_id: str | None = None,
    data_root: str | None = None,
    limit: int = 8,
) -> dict[str, Any]:
    resolved_data_root = _resolve_data_root(data_root)
    normalized_limit = _normalize_limit(limit)

    # Use the same project resolution logic to find the active program if not given
    if not program_id:
        recent_runs = _recent_run_summaries(resolved_data_root, limit=1)
        if recent_runs:
            program_id = str(recent_runs[0].get("program_id") or "").strip()
        if not program_id:
            program_ids = _project_program_ids(resolved_data_root, [])
            if program_ids:
                program_id = program_ids[0]

    if not program_id:
        return {
            "available": False,
            "program_id": None,
            "message": "No active program found to summarize.",
        }

    memory = _memory_snapshot(program_id, resolved_data_root, limit=normalized_limit)
    proposal_count, recent_proposals = _proposal_records(program_id, resolved_data_root, limit=normalized_limit)

    return {
        "title": f"{program_id} Memory Summary",
        "subtitle": f"Belief state and recent evidence from {resolved_data_root}",
        "available": memory.get("available", False),
        "program_id": program_id,
        "memory": memory,
        "proposal_count": proposal_count,
        "recent_proposals": recent_proposals,
        "widget": "operator_dashboard",
    }


def discover_run(
    *,
    proposal: str,
    registry_root: str = "project/configs/registries",
    run_id: str | None = None,
    data_root: str | None = None,
    check: bool = False,
    confirmations: Any | None = None,
) -> dict[str, Any]:
    """Execute Stage 1 discovery for a proposal YAML file."""
    from project import discover  # lazy import — heavy pipeline deps

    # Enforce confirmations
    conf = confirmations or {}
    if hasattr(conf, "model_dump"):
        conf = conf.model_dump()

    required = [
        "understands_writes_artifacts",
        "no_live_trading",
        "no_threshold_relaxation",
        "no_posthoc_rescue",
    ]
    missing = [req for req in required if not conf.get(req)]
    if missing:
        return {
            "status": "blocked",
            "message": f"Discovery blocked. Missing required operator confirmations: {', '.join(missing)}",
            "next_safe_command": "Please provide all required confirmations in the tool input.",
        }

    _handler_utils.check_app_mode({"proposal": proposal, "run_id": run_id})
    resolved_data_root = _resolve_data_root(data_root)

    # Use a generic run_id if not provided so we can lock it
    resolved_run_id = run_id or f"disc_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"

    with _handler_utils.RunLock(resolved_run_id, resolved_data_root):
        result = discover.run(
            proposal,
            registry_root=Path(registry_root),
            data_root=resolved_data_root,
            run_id=resolved_run_id,
            plan_only=False,
            dry_run=False,
            check=check,
        )
    return _clean_value(result)


def validate_run(
    *,
    run_id: str,
    data_root: str | None = None,
    timeout_sec: int = 600,
) -> dict[str, Any]:
    """Execute Stage 2 validation for an existing discovery run."""
    import concurrent.futures

    from project import validate  # lazy import

    _handler_utils.check_app_mode({"run_id": run_id})
    resolved_data_root = _resolve_data_root(data_root)
    normalized_timeout = min(max(int(timeout_sec), 30), 3600)

    def _run() -> dict[str, Any]:
        with _handler_utils.RunLock(run_id, resolved_data_root):
            return validate.run(run_id, data_root=resolved_data_root)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_run)
        try:
            result = future.result(timeout=normalized_timeout)
            return _clean_value(result) if isinstance(result, dict) else {"status": "complete", "raw": str(result)}
        except concurrent.futures.TimeoutError:
            return {
                "status": "timeout",
                "run_id": run_id,
                "timeout_sec": normalized_timeout,
                "message": f"Validation did not complete within {normalized_timeout}s. The run may still be in progress.",
            }
        except Exception as exc:
            return {
                "status": "error",
                "run_id": run_id,
                "error": str(exc),
                "message": "Validation raised an exception. Check run artifacts for details.",
            }


def promote_run(
    *,
    run_id: str,
    symbols: str,
    retail_profile: str = "capital_constrained",
    data_root: str | None = None,
    timeout_sec: int = 300,
    confirmations: Any | None = None,
) -> dict[str, Any]:
    """Execute Stage 3 promotion for a validated discovery run."""
    import concurrent.futures

    from project import promote  # lazy import

    # Enforce confirmations
    conf = confirmations or {}
    if hasattr(conf, "model_dump"):
        conf = conf.model_dump()

    required = [
        "canonical_validation_passed",
        "promotion_gates_must_hold",
        "do_not_export_rejected_candidates",
        "confirm_governed_write",
    ]
    missing = [req for req in required if not conf.get(req)]
    if missing:
        return {
            "status": "blocked",
            "run_id": run_id,
            "message": f"Promotion blocked. Missing required operator confirmations: {', '.join(missing)}",
            "next_safe_command": "Please provide all required confirmations in the tool input.",
        }

    _handler_utils.check_app_mode({"run_id": run_id, "symbols": symbols})
    resolved_data_root = _resolve_data_root(data_root)
    normalized_timeout = min(max(int(timeout_sec), 30), 3600)

    def _run() -> dict[str, Any]:
        with _handler_utils.RunLock(run_id, resolved_data_root):
            result = promote.run(
                run_id=run_id,
                symbols=symbols,
                retail_profile=retail_profile,
                out_dir=None,
            )
        exit_code = getattr(result, "exit_code", 0) or 0
        thesis_count = getattr(result, "thesis_count", None)
        output_path = getattr(result, "output_path", None)
        diagnostics = _clean_value(getattr(result, "diagnostics", {}) or {})
        return {
            "status": "promoted" if exit_code == 0 else "failed",
            "run_id": run_id,
            "exit_code": exit_code,
            "thesis_count": thesis_count,
            "output_path": str(output_path) if output_path else None,
            "diagnostics": diagnostics,
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_run)
        try:
            return future.result(timeout=normalized_timeout)
        except concurrent.futures.TimeoutError:
            return {
                "status": "timeout",
                "run_id": run_id,
                "timeout_sec": normalized_timeout,
                "message": f"Promotion did not complete within {normalized_timeout}s.",
            }
        except Exception as exc:
            return {
                "status": "error",
                "run_id": run_id,
                "error": str(exc),
                "message": "Promotion raised an exception. Check run artifacts for details.",
            }


def list_theses(
    *,
    data_root: str | None = None,
) -> dict[str, Any]:
    """List available promoted thesis batches from Stage 3/4 output."""
    resolved_data_root = _resolve_data_root(data_root)
    theses_dir = resolved_data_root / "live" / "theses"
    batches: list[dict[str, Any]] = []

    if theses_dir.exists():
        for batch_dir in sorted(theses_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
            if not batch_dir.is_dir():
                continue
            thesis_file = batch_dir / "promoted_theses.json"
            manifest_file = batch_dir / "promotion_manifest.json"
            thesis_count: int | None = None
            run_id: str | None = batch_dir.name
            promoted_at: str | None = None
            if thesis_file.exists():
                try:
                    raw = json.loads(thesis_file.read_text(encoding="utf-8"))
                    if isinstance(raw, list):
                        thesis_count = len(raw)
                    elif isinstance(raw, dict):
                        thesis_count = len(raw.get("theses", raw.get("items", [])))
                except Exception:
                    pass
            if manifest_file.exists():
                manifest_data = _read_json_dict(manifest_file)
                promoted_at = str(manifest_data.get("promoted_at") or "")
                run_id = str(manifest_data.get("run_id") or run_id)
            batches.append(
                _clean_value(
                    {
                        "run_id": run_id,
                        "thesis_count": thesis_count,
                        "promoted_at": promoted_at,
                        "path": str(batch_dir.relative_to(resolved_data_root)),
                    }
                )
            )

    return {
        "title": "Promoted Thesis Inventory",
        "data_root": str(resolved_data_root),
        "total_batches": len(batches),
        "batches": batches,
    }


def catalog_list_runs(
    *,
    stage: str | None = None,
    data_root: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """List runs with manifests from the artifact catalog."""
    resolved_data_root = _resolve_data_root(data_root)
    normalized_limit = min(max(int(limit), 1), 100)
    normalized_stage = str(stage or "").strip().lower() or None

    # Discover all run manifests sorted by recency
    try:
        manifest_paths = sorted(
            (resolved_data_root / "runs").glob("*/run_manifest.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        manifest_paths = []

    records: list[dict[str, Any]] = []
    for manifest_path in manifest_paths[:normalized_limit * 3]:
        manifest = _read_json_dict(manifest_path, include_errors=True)
        if not manifest:
            continue
        if manifest.get("__invalid_json__"):
            summary = _invalid_run_summary(manifest_path, manifest)
            run_id = str(summary.get("run_id") or manifest_path.parent.name)
        else:
            run_id = str(manifest.get("run_id") or manifest_path.parent.name)
            summary = _run_summary(manifest, run_id)

        # Stage-filter: check presence of stage-specific artifact directories
        if normalized_stage:
            stage_dirs = {
                "discover": resolved_data_root / "reports" / "phase2" / run_id,
                "validate": resolved_data_root / "reports" / "validation" / run_id,
                "promote": resolved_data_root / "reports" / "promotions" / run_id,
                "deploy": resolved_data_root / "live" / "theses" / run_id,
            }
            check_path = stage_dirs.get(normalized_stage)
            if check_path is None or not check_path.exists():
                continue

        # Annotate which stages have artifacts present
        stages_present: list[str] = []
        for sname, sdir in [
            ("discover", resolved_data_root / "reports" / "phase2" / run_id),
            ("validate", resolved_data_root / "reports" / "validation" / run_id),
            ("promote", resolved_data_root / "reports" / "promotions" / run_id),
            ("deploy", resolved_data_root / "live" / "theses" / run_id),
        ]:
            if sdir.exists():
                stages_present.append(sname)

        summary["stages_present"] = stages_present
        records.append(_clean_value(summary))
        if len(records) >= normalized_limit:
            break

    return {
        "title": "Run Catalog",
        "data_root": str(resolved_data_root),
        "stage_filter": normalized_stage,
        "total_returned": len(records),
        "runs": records,
    }


def render_operator_summary(
    *,
    dashboard: dict[str, Any] | None = None,
    title: str | None = None,
    status: str | None = None,
    subtitle: str | None = None,
    summary: dict[str, Any] | None = None,
    sections: list[dict[str, str]] | None = None,
    source_tool: str | None = None,
) -> dict[str, Any]:
    normalized_dashboard = _parse_json_like(dashboard)
    if isinstance(normalized_dashboard, dict) and normalized_dashboard:
        payload = dict(normalized_dashboard)
        payload.setdefault("layout", "dashboard")
        payload.setdefault("widget", "operator_dashboard")
        payload.setdefault("source_tool", source_tool or payload.get("source_tool"))
        payload["summary"] = _normalize_summary(payload.get("summary"))
        payload["sections"] = _normalize_sections(payload.get("sections"))
        return payload

    return {
        "title": title,
        "status": status,
        "subtitle": subtitle,
        "summary": _normalize_summary(summary),
        "sections": _normalize_sections(sections),
        "source_tool": source_tool,
        "widget": "operator_dashboard",
    }
