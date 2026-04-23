from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError
from project.research.knowledge.memory import read_memory_table
from project.research.validation.regime_tests import build_stability_result_from_row


@dataclass(frozen=True)
class OperatorReportPaths:
    root: Path
    json_path: Path
    markdown_path: Path


def _resolved_root(data_root: Path | None = None) -> Path:
    return Path(data_root) if data_root is not None else get_data_root()


def _report_paths(*, report_kind: str, name: str, data_root: Path | None = None) -> OperatorReportPaths:
    root = _resolved_root(data_root) / "reports" / "operator" / report_kind / name
    return OperatorReportPaths(root=root, json_path=root / f"{report_kind}.json", markdown_path=root / f"{report_kind}.md")


def _slug(parts: Iterable[str]) -> str:
    clean = []
    for part in parts:
        token = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(part))
        token = "_".join(piece for piece in token.split("_") if piece)
        if token:
            clean.append(token)
    return "__".join(clean) or "report"


def _normalize_run_ids(run_ids: Iterable[str]) -> list[str]:
    out: list[str] = []
    for run_id in run_ids:
        value = str(run_id or "").strip()
        if value and value not in out:
            out.append(value)
    return out


def _load_candidate_table_with_source(run_id: str, *, data_root: Path) -> tuple[pd.DataFrame, Path | None]:
    for relative in (
        data_root / "reports" / "promotions" / run_id / "promotion_statistical_audit.parquet",
        data_root / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet",
        data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet",
    ):
        if relative.exists():
            return pd.read_parquet(relative), relative
        csv = relative.with_suffix(".csv")
        if csv.exists():
            return pd.read_csv(csv), csv
    return pd.DataFrame(), None


def _load_candidate_table(run_id: str, *, data_root: Path) -> pd.DataFrame:
    candidates, _source = _load_candidate_table_with_source(run_id, data_root=data_root)
    return candidates


def _metric_sign(value: Any) -> int:
    try:
        numeric = float(value)
    except Exception:
        return 0
    if numeric > 0:
        return 1
    if numeric < 0:
        return -1
    return 0


def _time_slice_classification(rows: list[dict[str, Any]]) -> tuple[str, str]:
    non_empty = [row for row in rows if row.get("top_metric_value") is not None or int(row.get("candidate_count", 0)) > 0]
    if not non_empty:
        return "absent", "No slice produced a candidate or interpretable metric."

    signs = {_metric_sign(row.get("top_metric_value")) for row in non_empty if _metric_sign(row.get("top_metric_value")) != 0}
    strong = [row for row in non_empty if abs(float(row.get("top_metric_value") or 0.0)) >= 2.0]
    if len(signs) > 1:
        return "mixed", "Effect direction flips across slices, which points to unstable temporal behavior."
    if len(strong) >= 2:
        return "stable", "At least two slices show the same directional effect with non-trivial magnitude."
    if len(strong) == 1 and len(non_empty) > 1:
        return "concentrated", "One slice dominates the evidence while the others are weak or absent."
    return "weak", "There is some slice-level evidence, but not enough to call the effect stable."


def build_time_slice_report(*, run_ids: Iterable[str], program_id: str | None = None, data_root: Path | None = None) -> dict[str, Any]:
    resolved = _resolved_root(data_root)
    run_ids = _normalize_run_ids(run_ids)
    from project.research.reports.operator_reporting import build_operator_summary

    summaries = [build_operator_summary(run_id=run_id, program_id=program_id, data_root=resolved) for run_id in run_ids]
    rows: list[dict[str, Any]] = []
    for summary in summaries:
        top = summary.get("top_candidate", {}) or {}
        rows.append(
            {
                "run_id": summary.get("run_id", ""),
                "baseline_run_id": summary.get("baseline_run_id", ""),
                "date_range": summary.get("date_range", ""),
                "terminal_status": summary.get("terminal_status", ""),
                "candidate_count": int(summary.get("candidate_count", 0) or 0),
                "promoted_count": int(summary.get("promoted_count", 0) or 0),
                "top_metric_name": top.get("metric_name", ""),
                "top_metric_value": top.get("metric_value"),
                "top_candidate_label": top.get("label", ""),
                "verdict": summary.get("verdict", ""),
            }
        )
    classification, rationale = _time_slice_classification(rows)
    strongest = None
    if rows:
        strongest = max(rows, key=lambda row: abs(float(row.get("top_metric_value") or 0.0)))
    report = {
        "schema_version": "time_slice_report_v1",
        "program_id": program_id or summaries[0].get("program_id", "") if summaries else "",
        "run_ids": run_ids,
        "classification": classification,
        "rationale": rationale,
        "strongest_slice": strongest or {},
        "slices": rows,
        "recommended_next_action": {
            "stable": "run_regime_split_confirmation",
            "concentrated": "freeze_winner_and_test_adjacent_period",
            "mixed": "investigate_regime_instability",
            "weak": "test_adjacent_bounded_variant",
            "absent": "kill_or_reframe_hypothesis",
        }.get(classification, "test_adjacent_bounded_variant"),
    }
    return report


def _parse_mapping_from_row(row: dict[str, Any], key: str) -> dict[str, Any]:
    value = row.get(key)
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
    return {}


def _first_present(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        try:
            if pd.isna(value):
                continue
        except TypeError:
            pass
        if value not in (None, ""):
            return value
    return None


def _candidate_template_label(row: dict[str, Any]) -> str:
    return str(
        _first_present(
            row,
            "template_id",
            "rule_template",
            "template_verb",
            "template",
            "template_family",
        )
        or "unknown_template"
    )


def build_regime_split_report(*, run_id: str, data_root: Path | None = None) -> dict[str, Any]:
    resolved = _resolved_root(data_root)
    from project.research.reports.operator_reporting import build_operator_summary

    summary = build_operator_summary(run_id=run_id, data_root=resolved)
    candidates, candidate_source = _load_candidate_table_with_source(run_id, data_root=resolved)
    rows: list[dict[str, Any]] = []
    if not candidates.empty:
        for _, row in candidates.head(10).iterrows():
            stability = build_stability_result_from_row(
                dict(row),
                source_artifact=str(candidate_source) if candidate_source is not None else None,
            )
            rows.append(
                {
                    "label": " / ".join(
                        [
                            str(row.get("event_type", row.get("trigger_type", "unknown_event"))),
                            _candidate_template_label(dict(row)),
                            str(row.get("direction", "?")),
                            str(row.get("horizon", row.get("horizon_bars", row.get("horizon_label", "?")))),
                        ]
                    ),
                    "regime_flip_flag": bool(stability.regime_flip_flag),
                    "worst_regime_estimate": float(stability.worst_regime_estimate),
                    "cross_symbol_sign_consistency": float(stability.cross_symbol_sign_consistency),
                    "rolling_instability_score": float(stability.rolling_instability_score),
                    "by_regime": stability.details.get("by_regime", {}),
                }
            )
    has_flip = any(row.get("regime_flip_flag") for row in rows)
    report = {
        "schema_version": "regime_split_report_v1",
        "run_id": run_id,
        "program_id": summary.get("program_id", ""),
        "classification": "regime_instability" if has_flip else "regime_consistent",
        "rationale": "At least one leading candidate flips sign across regimes." if has_flip else "No regime sign flip was detected in the leading candidate rows.",
        "candidate_regime_diagnostics": rows,
        "recommended_next_action": "freeze_regime_filter_and_confirm" if has_flip else "compare_time_slices",
    }
    return report


def build_negative_result_diagnostics(*, run_id: str, program_id: str | None = None, data_root: Path | None = None) -> dict[str, Any]:
    resolved = _resolved_root(data_root)
    from project.research.reports.operator_reporting import build_operator_summary

    summary = build_operator_summary(run_id=run_id, program_id=program_id, data_root=resolved)
    candidates = _load_candidate_table(run_id, data_root=resolved)
    top = summary.get("top_candidate", {}) or {}
    primary_fail_gate = str(summary.get("primary_fail_gate", top.get("primary_fail_gate", "")) or "")
    terminal_status = str(summary.get("terminal_status", "") or "")
    mechanical_outcome = str(summary.get("mechanical_outcome", "") or "")
    diagnosis = "no_effect"
    rationale = "The run finished without enough evidence to support the hypothesis."
    sample_size = None
    if terminal_status in {"failed_mechanical", "failed_data_quality", "failed_runtime_invariants"} or mechanical_outcome in {
        "mechanical_failure",
        "artifact_contract_failure",
        "data_quality_failure",
    }:
        diagnosis = "mechanical_artifact_gap"
        rationale = "The main blocker is infrastructure or artifact integrity rather than the market claim itself."
    else:
        if not candidates.empty:
            row = dict(candidates.iloc[0])
            sample_size = row.get("train_n_obs", row.get("n_obs", None))
            try:
                if sample_size is not None and int(sample_size) < 30:
                    diagnosis = "low_sample_power"
                    rationale = "The strongest row exists, but the sample size is too small to treat the rejection as decisive."
                else:
                    stability = build_stability_result_from_row(row)
                    if stability.regime_flip_flag:
                        diagnosis = "regime_instability"
                        rationale = "The leading candidate changes sign across regimes, so the effect is not stable enough for promotion."
                    elif primary_fail_gate in {"gate_promo_timeframe_consensus", "gate_promo_stability_gate", "gate_promo_stability_score"}:
                        diagnosis = "regime_instability"
                        rationale = "The run failed a stability-style gate rather than a pure signal gate."
            except DataIntegrityError:
                raise
            except Exception:
                pass
        elif primary_fail_gate and "sample" in primary_fail_gate.lower():
            diagnosis = "low_sample_power"
            rationale = "The run failed on a sample-quality gate before generating stable candidate evidence."

    next_action = {
        "mechanical_artifact_gap": "repair_artifacts_or_resume",
        "low_sample_power": "increase_sample_or_coarsen_slice",
        "regime_instability": "freeze_regime_filter_and_confirm",
        "no_effect": "kill_or_reframe_hypothesis",
    }[diagnosis]
    payload = {
        "schema_version": "negative_result_diagnostics_v1",
        "run_id": run_id,
        "program_id": summary.get("program_id", program_id or ""),
        "diagnosis": diagnosis,
        "rationale": rationale,
        "primary_fail_gate": primary_fail_gate,
        "sample_size": None if sample_size is None else int(sample_size),
        "top_candidate_label": top.get("label", ""),
        "recommended_next_action": next_action,
    }
    return payload


def _markdown(title: str, payload: dict[str, Any]) -> str:
    lines = [f"# {title}", ""]
    for key, value in payload.items():
        if isinstance(value, (dict, list)):
            lines.append(f"## {key}")
            lines.append("")
            lines.append("```json")
            lines.append(json.dumps(value, indent=2, sort_keys=True))
            lines.append("```")
        else:
            lines.append(f"- {key}: `{value}`")
    return "\n".join(lines) + "\n"


def write_time_slice_report(*, run_ids: Iterable[str], program_id: str | None = None, data_root: Path | None = None) -> dict[str, Any]:
    report = build_time_slice_report(run_ids=run_ids, program_id=program_id, data_root=data_root)
    paths = _report_paths(report_kind="time_slice_report", name=_slug(report.get("run_ids", [])), data_root=data_root)
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths.markdown_path.write_text(_markdown("Time-slice report", report), encoding="utf-8")
    report["report_json_path"] = str(paths.json_path)
    report["report_markdown_path"] = str(paths.markdown_path)
    return report


def write_regime_split_report(*, run_id: str, data_root: Path | None = None) -> dict[str, Any]:
    report = build_regime_split_report(run_id=run_id, data_root=data_root)
    paths = _report_paths(report_kind="regime_split_report", name=run_id, data_root=data_root)
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths.markdown_path.write_text(_markdown("Regime split report", report), encoding="utf-8")
    report["report_json_path"] = str(paths.json_path)
    report["report_markdown_path"] = str(paths.markdown_path)
    return report


def write_negative_result_diagnostics(*, run_id: str, program_id: str | None = None, data_root: Path | None = None) -> dict[str, Any]:
    report = build_negative_result_diagnostics(run_id=run_id, program_id=program_id, data_root=data_root)
    paths = _report_paths(report_kind="negative_result_diagnostics", name=run_id, data_root=data_root)
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths.markdown_path.write_text(_markdown("Negative-result diagnostics", report), encoding="utf-8")
    report["report_json_path"] = str(paths.json_path)
    report["report_markdown_path"] = str(paths.markdown_path)
    return report


def write_sprint4_outputs_for_run(*, run_id: str, program_id: str | None = None, data_root: Path | None = None) -> dict[str, Any]:
    negative = write_negative_result_diagnostics(run_id=run_id, program_id=program_id, data_root=data_root)
    regime = write_regime_split_report(run_id=run_id, data_root=data_root)
    return {
        "negative_result_diagnostics": negative,
        "regime_split_report": regime,
    }
