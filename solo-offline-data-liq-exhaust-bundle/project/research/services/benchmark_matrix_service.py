from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import yaml


REQUIRED_RUN_KEYS = ("run_id", "symbols", "start", "end")
OPTIONAL_METADATA_KEYS = (
    "benchmark_id",
    "family",
    "event_type",
    "template",
    "context_label",
    "timeframe",
    "objective",
)


def load_benchmark_matrix(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Benchmark matrix not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Benchmark matrix must be a YAML mapping.")
    runs = payload.get("runs", [])
    if not isinstance(runs, list) or not runs:
        raise ValueError("Benchmark matrix must define a non-empty 'runs' list.")
    for idx, run in enumerate(runs):
        if not isinstance(run, dict):
            raise ValueError(f"Run entry at index {idx} must be a mapping.")
        for key in REQUIRED_RUN_KEYS:
            value = str(run.get(key, "")).strip()
            if not value:
                raise ValueError(f"Run entry at index {idx} is missing required key '{key}'.")
    return payload


def _slice_row(run: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "run_id": str(run.get("run_id", "")),
        "symbols": str(run.get("symbols", "")),
        "start": str(run.get("start", "")),
        "end": str(run.get("end", "")),
        "status": str(result.get("status", "")),
        "returncode": result.get("returncode"),
        "duration_sec": result.get("duration_sec"),
    }
    if isinstance(result.get("generated_reports"), dict):
        row["generated_reports"] = dict(result.get("generated_reports", {}))
    for key in OPTIONAL_METADATA_KEYS:
        row[key] = str(run.get(key, "")).strip()
    return row


def build_benchmark_summary(*, matrix: Dict[str, Any], manifest: Dict[str, Any]) -> Dict[str, Any]:
    runs = matrix.get("runs", [])
    results_by_run_id = {
        str(row.get("run_id", "")).strip(): dict(row)
        for row in manifest.get("results", [])
        if str(row.get("run_id", "")).strip()
    }
    slice_rows: List[Dict[str, Any]] = []
    status_counter: Counter[str] = Counter()
    family_counter: Counter[str] = Counter()
    template_counter: Counter[str] = Counter()
    context_counter: Counter[str] = Counter()

    for run in runs:
        run_id = str(run.get("run_id", "")).strip()
        result = results_by_run_id.get(run_id, {})
        row = _slice_row(run, result)
        slice_rows.append(row)
        status_counter[str(row.get("status", "")).strip() or "unknown"] += 1
        family = str(row.get("family", "")).strip()
        template = str(row.get("template", "")).strip()
        context_label = str(row.get("context_label", "")).strip()
        if family:
            family_counter[family] += 1
        if template:
            template_counter[template] += 1
        if context_label:
            context_counter[context_label] += 1

    return {
        "matrix_id": str(matrix.get("matrix_id", "matrix")).strip() or "matrix",
        "description": str(matrix.get("description", "")).strip(),
        "planned_runs": int(len(runs)),
        "completed_rows": int(len(manifest.get("results", []))),
        "status_counts": dict(sorted(status_counter.items())),
        "families": dict(sorted(family_counter.items())),
        "templates": dict(sorted(template_counter.items())),
        "contexts": dict(sorted(context_counter.items())),
        "slices": slice_rows,
    }


def render_benchmark_summary_markdown(summary: Dict[str, Any]) -> str:
    lines = [
        "# Benchmark Matrix Summary",
        "",
        f"- `matrix_id`: `{summary.get('matrix_id', '')}`",
        f"- `planned_runs`: `{summary.get('planned_runs', 0)}`",
        f"- `completed_rows`: `{summary.get('completed_rows', 0)}`",
        "",
        "## Status Counts",
        "",
    ]
    for key, value in dict(summary.get("status_counts", {})).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## Slices",
            "",
            "| Run | Family | Template | Context | Status |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for row in summary.get("slices", []):
        lines.append(
            f"| `{row.get('run_id', '')}` | `{row.get('family', '')}` | `{row.get('template', '')}` | "
            f"`{row.get('context_label', '')}` | `{row.get('status', '')}` |"
        )
    lines.append("")
    return "\n".join(lines)


def write_benchmark_summary(*, out_dir: Path, summary: Dict[str, Any]) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "benchmark_summary.json"
    md_path = out_dir / "benchmark_summary.md"
    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_benchmark_summary_markdown(summary), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
