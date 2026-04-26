from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd


def summarize_phase2_event(event_path: Path) -> dict[str, Any]:
    if not event_path.exists():
        return {
            "rows": 0,
            "templates": [],
            "actions": [],
            "direction_rules": [],
            "by_template": {},
            "missing": True,
        }
    df = pd.read_parquet(event_path)
    if df.empty:
        return {
            "rows": 0,
            "templates": [],
            "actions": [],
            "direction_rules": [],
            "by_template": {},
            "missing": False,
        }
    template_col = "template_verb" if "template_verb" in df.columns else "rule_template"
    action_col = "action" if "action" in df.columns else ""
    direction_col = "direction_rule" if "direction_rule" in df.columns else ""

    templates = sorted(
        {
            str(x).strip()
            for x in df.get(template_col, pd.Series(dtype=str)).dropna()
            if str(x).strip()
        }
    )
    actions = (
        sorted(
            {
                str(x).strip()
                for x in df.get(action_col, pd.Series(dtype=str)).dropna()
                if str(x).strip()
            }
        )
        if action_col
        else []
    )
    direction_rules = (
        sorted(
            {
                str(x).strip()
                for x in df.get(direction_col, pd.Series(dtype=str)).dropna()
                if str(x).strip()
            }
        )
        if direction_col
        else []
    )
    by_template = (
        df.groupby(template_col).size().sort_index().astype(int).to_dict()
        if template_col in df.columns
        else {}
    )
    return {
        "rows": len(df),
        "templates": templates,
        "actions": actions,
        "direction_rules": direction_rules,
        "by_template": {str(k): int(v) for k, v in by_template.items()},
        "missing": False,
    }


def build_run_summary(*, data_root: Path, run_id: str, events: Iterable[str]) -> dict[str, Any]:
    out: dict[str, Any] = {"run_id": str(run_id), "events": {}}
    for event in events:
        et = str(event).strip().upper()
        if not et:
            continue
        event_path = (
            Path(data_root) / "reports" / "phase2" / str(run_id) / et / "phase2_candidates.parquet"
        )
        out["events"][et] = summarize_phase2_event(event_path)
    return out


def compare_summaries(
    *,
    baseline: dict[str, Any],
    current: dict[str, Any],
    keys: Iterable[str] = ("rows", "templates", "actions", "direction_rules"),
) -> list[str]:
    failures: list[str] = []
    baseline_events = baseline.get("events", {})
    current_events = current.get("events", {})
    all_events = sorted(set(baseline_events) | set(current_events))
    for event in all_events:
        b = baseline_events.get(event, {})
        c = current_events.get(event, {})
        for key in keys:
            if b.get(key) != c.get(key):
                failures.append(f"{event}:{key} baseline={b.get(key)!r} current={c.get(key)!r}")
    return failures
