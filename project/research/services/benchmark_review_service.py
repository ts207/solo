from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from project.core.exceptions import DataIntegrityError


def _load_json(path_like: str | Path | None) -> Dict[str, Any]:
    if not path_like:
        return {}
    path = Path(path_like)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise DataIntegrityError(f"Failed to read benchmark review json artifact {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DataIntegrityError(f"Benchmark review json artifact {path} did not contain an object payload")
    return payload if isinstance(payload, dict) else {}


def _selected_hypothesis_key(selected: Dict[str, Any]) -> str:
    if not isinstance(selected, dict):
        return ""
    return str(selected.get("hypothesis_id", "")).strip()


def classify_benchmark_slice(*, generated_reports: Dict[str, Any]) -> str:
    live = _load_json(generated_reports.get("live_foundation"))
    comparison = _load_json(generated_reports.get("context_mode_comparison"))

    readiness = str(live.get("readiness", "")).strip().lower()
    if comparison:
        if bool(comparison.get("selection_changed")):
            return "authoritative"
        if bool(comparison.get("selection_outcome_changed")):
            return "quality_boundary"
        hard_rows = int(comparison.get("hard_label", {}).get("evaluated_rows", 0) or 0)
        conf_rows = int(comparison.get("confidence_aware", {}).get("evaluated_rows", 0) or 0)
        if hard_rows > 0 and conf_rows > 0:
            return "informative"
        return "coverage_limited"
    if readiness in {"ready", "warn"}:
        return "foundation_only"
    return "coverage_limited"


def build_benchmark_review(*, summary: Dict[str, Any]) -> Dict[str, Any]:
    slices = []
    status_counts: Dict[str, int] = {}
    for row in summary.get("slices", []):
        generated_reports = (
            dict(row.get("generated_reports", {}))
            if isinstance(row.get("generated_reports"), dict)
            else {}
        )
        live = _load_json(generated_reports.get("live_foundation"))
        comparison = _load_json(generated_reports.get("context_mode_comparison"))
        benchmark_status = classify_benchmark_slice(generated_reports=generated_reports)
        status_counts[benchmark_status] = status_counts.get(benchmark_status, 0) + 1
        hard_selected = comparison.get("hard_label", {}).get("selected", {}) if comparison else {}
        conf_selected = (
            comparison.get("confidence_aware", {}).get("selected", {}) if comparison else {}
        )
        slices.append(
            {
                "benchmark_id": str(row.get("benchmark_id", "")).strip(),
                "run_id": str(row.get("run_id", "")).strip(),
                "family": str(row.get("family", "")).strip(),
                "event_type": str(row.get("event_type", "")).strip(),
                "template": str(row.get("template", "")).strip(),
                "context_label": str(row.get("context_label", "")).strip(),
                "status": str(row.get("status", "")).strip(),
                "benchmark_status": benchmark_status,
                "live_foundation_readiness": str(live.get("readiness", "")).strip(),
                "context_comparison_present": bool(comparison),
                "hard_evaluated_rows": int(
                    comparison.get("hard_label", {}).get("evaluated_rows", 0) or 0
                )
                if comparison
                else 0,
                "confidence_evaluated_rows": int(
                    comparison.get("confidence_aware", {}).get("evaluated_rows", 0) or 0
                )
                if comparison
                else 0,
                "selection_changed": bool(comparison.get("selection_changed"))
                if comparison
                else False,
                "selection_outcome_changed": bool(comparison.get("selection_outcome_changed"))
                if comparison
                else False,
                "selected_hypothesis_hard": _selected_hypothesis_key(hard_selected),
                "selected_hypothesis_confidence_aware": _selected_hypothesis_key(conf_selected),
                "selected_valid_hard": bool(hard_selected.get("valid"))
                if isinstance(hard_selected, dict)
                else False,
                "selected_valid_confidence_aware": bool(conf_selected.get("valid"))
                if isinstance(conf_selected, dict)
                else False,
                "generated_reports": generated_reports,
            }
        )

    return {
        "schema_version": "benchmark_review_v1",
        "matrix_id": str(summary.get("matrix_id", "")).strip(),
        "status_counts": status_counts,
        "slices": slices,
    }


def render_benchmark_review_markdown(review: Dict[str, Any]) -> str:
    lines = [
        "# Benchmark Review",
        "",
        f"- `matrix_id`: `{review.get('matrix_id', '')}`",
        "",
        "## Status Counts",
        "",
    ]
    for key, value in sorted(dict(review.get("status_counts", {})).items()):
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## Slices",
            "",
            "| Run | Family | Event | Context | Benchmark Status | Live Foundation | Hard Rows | Confidence Rows | Selection Changed | Outcome Changed |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in review.get("slices", []):
        lines.append(
            f"| `{row.get('run_id', '')}` | `{row.get('family', '')}` | `{row.get('event_type', '')}` | "
            f"`{row.get('context_label', '')}` | `{row.get('benchmark_status', '')}` | "
            f"`{row.get('live_foundation_readiness', '')}` | `{row.get('hard_evaluated_rows', 0)}` | "
            f"`{row.get('confidence_evaluated_rows', 0)}` | `{row.get('selection_changed', False)}` | "
            f"`{row.get('selection_outcome_changed', False)}` |"
        )
    lines.append("")
    return "\n".join(lines)


def write_benchmark_review(*, out_dir: Path, review: Dict[str, Any]) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "benchmark_review.json"
    md_path = out_dir / "benchmark_review.md"
    json_path.write_text(json.dumps(review, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_benchmark_review_markdown(review), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
