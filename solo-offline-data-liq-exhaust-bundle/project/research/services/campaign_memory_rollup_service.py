from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from project.research.knowledge.memory import ensure_memory_store, read_memory_table


def _top_records(frame: pd.DataFrame, sort_col: str, *, limit: int = 5) -> list[dict[str, Any]]:
    if frame.empty or sort_col not in frame.columns:
        return []
    return frame.sort_values(sort_col, ascending=False).head(int(limit)).to_dict(orient="records")


def _active_failures(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    if "stage" in out.columns:
        out = out[
            ~out["stage"].astype(str).str.strip().str.lower().isin({"", "none", "null", "nan"})
        ]
    if "superseded_by_run_id" in out.columns:
        out = out[out["superseded_by_run_id"].astype(str).str.strip() == ""]
    return out.reset_index(drop=True)


def build_campaign_memory_rollup(
    *,
    program_id: str,
    data_root: Path,
    repeated_fail_threshold: int = 2,
    top_k: int = 5,
) -> Dict[str, Any]:
    ensure_memory_store(program_id, data_root=data_root)
    tested_regions = read_memory_table(program_id, "tested_regions", data_root=data_root)
    failures = read_memory_table(program_id, "failures", data_root=data_root)
    reflections = read_memory_table(program_id, "reflections", data_root=data_root)
    event_stats = read_memory_table(program_id, "event_statistics", data_root=data_root)
    template_stats = read_memory_table(program_id, "template_statistics", data_root=data_root)
    context_stats = read_memory_table(program_id, "context_statistics", data_root=data_root)

    superseded_rows = 0
    repeated_fail_regions: list[dict[str, Any]] = []
    latest_run_id = ""
    if not tested_regions.empty:
        latest_run_id = str(
            tested_regions.get("run_id", pd.Series(dtype="object")).astype(str).iloc[-1]
        )
        if "region_key" in tested_regions.columns and "run_id" in tested_regions.columns:
            latest_by_region = tested_regions.groupby("region_key", dropna=False)[
                "run_id"
            ].transform("max")
            superseded_rows = int(
                (tested_regions["run_id"].astype(str) != latest_by_region.astype(str)).sum()
            )
        fail_gate_col = (
            "primary_fail_gate" if "primary_fail_gate" in tested_regions.columns else None
        )
        if fail_gate_col:
            rejected = tested_regions[
                tested_regions[fail_gate_col].astype(str).str.strip() != ""
            ].copy()
            if not rejected.empty:
                grouped = (
                    rejected.groupby("region_key", dropna=False)
                    .agg(
                        fail_count=("region_key", "size"),
                        event_type=("event_type", "first"),
                        template_id=("template_id", "first"),
                        primary_fail_gate=(fail_gate_col, "last"),
                    )
                    .reset_index()
                )
                repeated = grouped[grouped["fail_count"] >= int(repeated_fail_threshold)]
                repeated_fail_regions = repeated.sort_values("fail_count", ascending=False).to_dict(
                    orient="records"
                )

    unresolved_repairs = []
    active_failures = _active_failures(failures)
    if not active_failures.empty:
        unresolved_repairs = active_failures.head(int(top_k)).to_dict(orient="records")

    return {
        "schema_version": "campaign_memory_rollup_v1",
        "program_id": program_id,
        "latest_run_id": latest_run_id,
        "totals": {
            "tested_region_rows": int(len(tested_regions)),
            "reflection_rows": int(len(reflections)),
            "failure_rows": int(len(failures)),
            "unique_region_count": int(tested_regions["region_key"].nunique())
            if "region_key" in tested_regions.columns and not tested_regions.empty
            else 0,
            "superseded_region_rows": superseded_rows,
        },
        "top_events": _top_records(event_stats, "avg_after_cost_expectancy", limit=top_k),
        "top_templates": _top_records(template_stats, "avg_after_cost_expectancy", limit=top_k),
        "top_contexts": _top_records(context_stats, "avg_after_cost_expectancy", limit=top_k),
        "repeated_fail_regions": repeated_fail_regions[: int(top_k)],
        "unresolved_repairs": unresolved_repairs,
    }


def write_campaign_memory_rollup(
    *,
    program_id: str,
    data_root: Path,
    out_dir: Path | None = None,
    repeated_fail_threshold: int = 2,
    top_k: int = 5,
) -> Path:
    payload = build_campaign_memory_rollup(
        program_id=program_id,
        data_root=data_root,
        repeated_fail_threshold=repeated_fail_threshold,
        top_k=top_k,
    )
    report_dir = (
        out_dir if out_dir is not None else data_root / "artifacts" / "experiments" / program_id
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    out_path = report_dir / "campaign_memory_rollup.json"
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return out_path
