from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from project.io.utils import read_parquet

GROUP_KEY = ["symbol", "event_type", "direction", "horizon"]


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _bool_col(frame: pd.DataFrame, col: str) -> pd.Series:
    if col not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[col].fillna(False).astype(bool)


def build_shadow_playbook_payload(
    *,
    data_root: Path,
    run_id: str,
    confirmatory_report_path: Path | None = None,
    adjacent_survivorship_report_path: Path | None = None,
) -> Dict[str, Any]:
    edge_path = (
        data_root / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet"
    )
    naive_path = data_root / "reports" / "phase2" / run_id / "naive_evaluation.parquet"
    edge = _read_parquet(edge_path)
    naive = _read_parquet(naive_path)
    confirmatory = (
        _read_json(confirmatory_report_path) if confirmatory_report_path is not None else {}
    )
    adjacent_survivorship = (
        _read_json(adjacent_survivorship_report_path)
        if adjacent_survivorship_report_path is not None
        else {}
    )

    if edge.empty:
        return {
            "run_id": run_id,
            "candidate_count": 0,
            "playbook_groups": [],
            "primary_playbook": None,
        }

    merged = edge.merge(
        naive[["candidate_id", "naive_expectancy", "event_count"]]
        if not naive.empty
        else pd.DataFrame(columns=["candidate_id", "naive_expectancy", "event_count"]),
        on="candidate_id",
        how="left",
    )
    merged["strict_pass"] = _bool_col(merged, "gate_multiplicity_strict")
    merged["bridge_pass"] = (
        merged.get("gate_bridge_tradable", pd.Series("fail", index=merged.index))
        .astype(str)
        .str.lower()
        .eq("pass")
    )
    merged["naive_positive"] = (
        pd.to_numeric(merged.get("naive_expectancy", 0.0), errors="coerce").fillna(0.0) > 0.0
    )
    merged["q_value_sort"] = pd.to_numeric(merged.get("q_value", 1.0), errors="coerce").fillna(1.0)
    merged["expectancy_sort"] = pd.to_numeric(
        merged.get("after_cost_expectancy_per_trade", 0.0), errors="coerce"
    ).fillna(0.0)

    confirmatory_index: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    for row in confirmatory.get("matched_candidates", []) if isinstance(confirmatory, dict) else []:
        if not isinstance(row, dict):
            continue
        key = (
            str(row.get("symbol", "")),
            str(row.get("event_type", "")),
            str(row.get("direction", "")),
            str(row.get("horizon", "")),
        )
        confirmatory_index[key] = row

    adjacent_index: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    for row in (
        adjacent_survivorship.get("candidate_rows", [])
        if isinstance(adjacent_survivorship, dict)
        else []
    ):
        if not isinstance(row, dict):
            continue
        key = (
            str(row.get("symbol", "")),
            str(row.get("event_type", "")),
            str(row.get("direction", "")),
            str(row.get("horizon", "")),
        )
        existing = adjacent_index.get(key)
        if existing is None or bool(existing.get("survived_adjacent_window", False)) is False:
            adjacent_index[key] = row

    groups: List[Dict[str, Any]] = []
    for key, group in merged.groupby(GROUP_KEY, dropna=False):
        ranked = group.sort_values(
            by=["strict_pass", "q_value_sort", "expectancy_sort", "rule_template"],
            ascending=[False, True, False, True],
        ).reset_index(drop=True)
        rep = ranked.iloc[0]
        confirm_key = tuple(str(rep[col]) for col in GROUP_KEY)
        confirm_row = confirmatory_index.get(confirm_key, {})
        adjacent_row = adjacent_index.get(confirm_key, {})

        deploy_blockers: List[str] = []
        if not bool(rep.get("strict_pass", False)):
            deploy_blockers.append("multiplicity_not_strict")
        if not bool(rep.get("naive_positive", False)):
            deploy_blockers.append("naive_expectancy_non_positive")
        if adjacent_row and not bool(adjacent_row.get("survived_adjacent_window", False)):
            deploy_blockers.append("adjacent_window_fail")
        if confirm_row and not bool(confirm_row.get("target_gate_pass", False)):
            deploy_blockers.append("confirmatory_gate_fail")
        elif not confirm_row:
            deploy_blockers.append("no_confirmatory_match")

        status = "shadow_candidate" if not deploy_blockers else "research_only"
        groups.append(
            {
                "group_key": {
                    "symbol": str(rep["symbol"]),
                    "event_type": str(rep["event_type"]),
                    "direction": str(rep["direction"]),
                    "horizon": str(rep["horizon"]),
                },
                "template_count": int(len(ranked)),
                "representative": {
                    "candidate_id": rep.get("candidate_id"),
                    "rule_template": rep.get("rule_template"),
                    "q_value": float(rep.get("q_value_sort", 1.0)),
                    "after_cost_expectancy_per_trade": float(
                        rep.get("after_cost_expectancy_per_trade", 0.0) or 0.0
                    ),
                    "stressed_after_cost_expectancy_per_trade": float(
                        rep.get("stressed_after_cost_expectancy_per_trade", 0.0) or 0.0
                    ),
                    "naive_expectancy": float(rep.get("naive_expectancy", 0.0) or 0.0),
                    "event_count": int(rep.get("event_count", 0) or 0),
                    "strict_pass": bool(rep.get("strict_pass", False)),
                    "bridge_pass": bool(rep.get("bridge_pass", False)),
                },
                "alternate_templates": [
                    str(x) for x in ranked["rule_template"].astype(str).tolist()[1:]
                ],
                "confirmatory_match": {
                    "matched": bool(confirm_row),
                    "target_candidate_id": confirm_row.get("candidate_id_target"),
                    "target_gate_pass": bool(confirm_row.get("target_gate_pass", False)),
                    "target_bridge_pass": bool(confirm_row.get("target_bridge_pass", False)),
                    "target_q_value": confirm_row.get("target_q_value"),
                },
                "adjacent_survivorship": {
                    "matched": bool(adjacent_row),
                    "survived_adjacent_window": bool(
                        adjacent_row.get("survived_adjacent_window", False)
                    ),
                    "target_candidate_id": adjacent_row.get("target_candidate_id"),
                    "failure_reasons": list(adjacent_row.get("failure_reasons", []))
                    if adjacent_row
                    else [],
                    "target_after_cost_expectancy_per_trade": adjacent_row.get(
                        "target_after_cost_expectancy_per_trade"
                    ),
                },
                "status": status,
                "deploy_blockers": deploy_blockers,
            }
        )

    groups.sort(
        key=lambda row: (
            row["status"] != "shadow_candidate",
            not row["representative"]["strict_pass"],
            row["representative"]["q_value"],
            -row["representative"]["after_cost_expectancy_per_trade"],
        )
    )

    primary = groups[0] if groups else None
    return {
        "run_id": run_id,
        "candidate_count": int(len(merged)),
        "playbook_groups": groups,
        "primary_playbook": primary,
        "source_paths": {
            "edge_candidates": str(edge_path),
            "naive_evaluation": str(naive_path),
            "confirmatory_report": str(confirmatory_report_path)
            if confirmatory_report_path is not None
            else None,
            "adjacent_survivorship_report": (
                str(adjacent_survivorship_report_path)
                if adjacent_survivorship_report_path is not None
                else None
            ),
        },
    }


def render_shadow_playbook_summary(payload: Dict[str, Any]) -> str:
    lines = [f"# Shadow Playbook: {payload.get('run_id', 'unknown')}"]
    primary = payload.get("primary_playbook")
    if primary:
        rep = primary["representative"]
        key = primary["group_key"]
        lines.append("")
        lines.append("## Primary")
        lines.append(
            f"- {key['symbol']} {key['event_type']} {key['direction']} {key['horizon']} via `{rep['rule_template']}`"
        )
        lines.append(f"- status: `{primary['status']}`")
        lines.append(f"- q_value: `{rep['q_value']:.6f}`")
        lines.append(
            f"- after_cost_expectancy_per_trade: `{rep['after_cost_expectancy_per_trade']:.6f}`"
        )
        lines.append(f"- naive_expectancy: `{rep['naive_expectancy']:.6f}`")
        if primary.get("deploy_blockers"):
            lines.append(f"- blockers: `{', '.join(primary['deploy_blockers'])}`")

    lines.append("")
    lines.append("## Groups")
    for group in payload.get("playbook_groups", []):
        rep = group["representative"]
        key = group["group_key"]
        lines.append(
            f"- {key['event_type']} {key['direction']} {key['horizon']}: `{group['status']}`, representative=`{rep['rule_template']}`, templates={group['template_count']}, q=`{rep['q_value']:.6f}`"
        )
    return "\n".join(lines) + "\n"


def write_shadow_playbook_report(
    *,
    data_root: Path,
    run_id: str,
    confirmatory_report_path: Path | None = None,
    adjacent_survivorship_report_path: Path | None = None,
    out_dir: Path | None = None,
) -> Dict[str, Path]:
    payload = build_shadow_playbook_payload(
        data_root=data_root,
        run_id=run_id,
        confirmatory_report_path=confirmatory_report_path,
        adjacent_survivorship_report_path=adjacent_survivorship_report_path,
    )
    report_dir = (
        out_dir if out_dir is not None else data_root / "reports" / "shadow_playbook" / run_id
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / "shadow_playbook.json"
    md_path = report_dir / "shadow_playbook_summary.md"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_path.write_text(render_shadow_playbook_summary(payload), encoding="utf-8")
    return {"json": json_path, "summary": md_path}
