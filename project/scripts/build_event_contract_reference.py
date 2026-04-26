#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections.abc import Iterable, Mapping
from io import StringIO
from pathlib import Path
from typing import Any

from project.domain.compiled_registry import get_domain_registry
from project.events.config import compose_event_config


def _json_text(payload: object) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _csv_text(rows: Iterable[Mapping[str, Any]]) -> str:
    fieldnames = [
        "event_type",
        "canonical_regime",
        "canonical_family",
        "default_executable",
        "detector_band",
        "planning_eligible",
        "runtime_eligible",
        "promotion_eligible",
        "primary_anchor_eligible",
        "detector",
        "enabled",
        "tags",
        "instrument_classes",
        "sequence_eligible",
        "subtype",
        "phase",
        "evidence_mode",
        "layer",
        "disposition",
        "asset_scope",
        "venue_scope",
        "research_only",
        "strategy_only",
        "is_composite",
        "is_context_tag",
        "is_strategy_construct",
        "deconflict_priority",
        "reports_dir",
        "events_file",
        "signal_column",
        "templates",
        "horizons",
        "conditioning_cols",
        "max_candidates_per_run",
        "threshold_parameters",
    ]
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                **{name: row.get(name) for name in fieldnames},
                "tags": "|".join(str(item) for item in row.get("tags", []) or []),
                "instrument_classes": "|".join(str(item) for item in row.get("instrument_classes", []) or []),
                "templates": "|".join(str(item) for item in row.get("templates", []) or []),
                "horizons": "|".join(str(item) for item in row.get("horizons", []) or []),
                "conditioning_cols": "|".join(str(item) for item in row.get("conditioning_cols", []) or []),
                "threshold_parameters": json.dumps(row.get("threshold_parameters", {}), sort_keys=True),
            }
        )
    return buffer.getvalue()


def _csv_wide_text(rows: Iterable[Mapping[str, Any]]) -> str:
    normalized_rows = [dict(row) for row in rows]
    threshold_columns = sorted(
        {
            str(key)
            for row in normalized_rows
            for key in (row.get("threshold_parameters", {}) or {}).keys()
        }
    )
    base_fieldnames = [
        "event_type",
        "canonical_regime",
        "canonical_family",
        "default_executable",
        "detector_band",
        "planning_eligible",
        "runtime_eligible",
        "promotion_eligible",
        "primary_anchor_eligible",
        "detector",
        "enabled",
        "tags",
        "instrument_classes",
        "sequence_eligible",
        "subtype",
        "phase",
        "evidence_mode",
        "layer",
        "disposition",
        "asset_scope",
        "venue_scope",
        "research_only",
        "strategy_only",
        "is_composite",
        "is_context_tag",
        "is_strategy_construct",
        "deconflict_priority",
        "reports_dir",
        "events_file",
        "signal_column",
        "templates",
        "horizons",
        "conditioning_cols",
        "max_candidates_per_run",
    ]
    fieldnames = [*base_fieldnames, *threshold_columns]
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in normalized_rows:
        thresholds = dict(row.get("threshold_parameters", {}) or {})
        payload = {
            **{name: row.get(name) for name in base_fieldnames},
            "tags": "|".join(str(item) for item in row.get("tags", []) or []),
            "instrument_classes": "|".join(str(item) for item in row.get("instrument_classes", []) or []),
            "templates": "|".join(str(item) for item in row.get("templates", []) or []),
            "horizons": "|".join(str(item) for item in row.get("horizons", []) or []),
            "conditioning_cols": "|".join(str(item) for item in row.get("conditioning_cols", []) or []),
        }
        for key in threshold_columns:
            value = thresholds.get(key, "")
            if isinstance(value, list):
                payload[key] = "|".join(str(item) for item in value)
            else:
                payload[key] = value
        writer.writerow(payload)
    return buffer.getvalue()


def _write_or_check(path: Path, content: str, *, check: bool) -> bool:
    if check:
        current = path.read_text(encoding="utf-8") if path.exists() else None
        return current == content
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def _is_simple_value(value: Any) -> bool:
    return isinstance(value, (str, int, float, bool)) or value is None


def _threshold_parameters(parameters: Mapping[str, Any]) -> dict[str, Any]:
    include_tokens = (
        "threshold",
        "quantile",
        "lookback",
        "window",
        "cooldown",
        "spacing",
        "multiplier",
        "ratio",
        "z_",
        "_z",
        "zscore",
        "_min",
        "_max",
        "min_",
        "max_",
        "_bps",
        "bars",
        "_lag",
        "lag_",
    )
    exclude_keys = {
        "canonical_family",
        "synthetic_coverage",
        "maturity",
        "cluster_id",
        "trigger",
        "confirmation",
        "expected_behavior",
        "notes",
    }
    out: dict[str, Any] = {}
    for key, value in parameters.items():
        if key in exclude_keys:
            continue
        normalized = str(key).strip().lower()
        if not any(token in normalized for token in include_tokens):
            continue
        if _is_simple_value(value):
            out[key] = value
            continue
        if isinstance(value, list) and all(_is_simple_value(item) for item in value):
            out[key] = list(value)
    return out
def _rows() -> list[dict[str, Any]]:
    registry = get_domain_registry()

    rows: list[dict[str, Any]] = []
    for event_type in registry.event_ids:
        event = registry.event_definitions[event_type]
        cfg = compose_event_config(event_type)
        rows.append(
            {
                "event_type": event_type,
                "canonical_regime": event.canonical_regime,
                "canonical_family": event.canonical_family,
                "default_executable": event.default_executable,
                "detector_band": event.detector_band,
                "planning_eligible": event.planning_eligible,
                "runtime_eligible": event.runtime_eligible,
                "promotion_eligible": event.promotion_eligible,
                "primary_anchor_eligible": event.primary_anchor_eligible,
                "detector": event.detector_name,
                "enabled": event.enabled,
                "tags": list(event.runtime_tags),
                "instrument_classes": list(event.instrument_classes),
                "sequence_eligible": event.sequence_eligible,
                "subtype": event.subtype,
                "phase": event.phase,
                "evidence_mode": event.evidence_mode,
                "layer": event.layer,
                "disposition": event.disposition,
                "asset_scope": event.asset_scope,
                "venue_scope": event.venue_scope,
                "research_only": event.research_only,
                "strategy_only": event.strategy_only,
                "is_composite": event.is_composite,
                "is_context_tag": event.is_context_tag,
                "is_strategy_construct": event.is_strategy_construct,
                "deconflict_priority": event.deconflict_priority,
                "reports_dir": cfg.reports_dir,
                "events_file": cfg.events_file,
                "signal_column": cfg.signal_column,
                "templates": list(cfg.templates),
                "horizons": list(cfg.horizons),
                "conditioning_cols": list(cfg.conditioning_cols),
                "max_candidates_per_run": cfg.max_candidates_per_run,
                "threshold_parameters": _threshold_parameters(cfg.parameters),
                "parameters": dict(cfg.parameters),
                "notes": event.notes,
            }
        )
    return rows


def _grouped_rows(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["canonical_regime"]), []).append(dict(row))
    return {regime: sorted(items, key=lambda item: str(item["event_type"])) for regime, items in sorted(grouped.items())}


def _render_markdown(rows: list[Mapping[str, Any]]) -> str:
    grouped = _grouped_rows(rows)
    lines = [
        "# Event Contract Reference",
        "",
        f"- Active events: `{len(rows)}`",
        "",
    ]
    for regime, items in grouped.items():
        lines.append(f"## {regime}")
        lines.append("")
        for row in items:
            lines.append(f"### {row['event_type']}")
            lines.append("")
            lines.append(
                f"- Detector: `{row['detector'] or 'unwired'}` | enabled=`{row['enabled']}` | band=`{row['detector_band']}`"
            )
            lines.append(
                f"- Eligibility: planning=`{row['planning_eligible']}` | runtime=`{row['runtime_eligible']}` | promotion=`{row['promotion_eligible']}` | primary_anchor=`{row['primary_anchor_eligible']}` | legacy_default_executable=`{row['default_executable']}`"
            )
            lines.append(
                f"- Family: canonical=`{row['canonical_family']}`"
            )
            lines.append(
                f"- Shape: subtype=`{row['subtype']}` | phase=`{row['phase']}` | evidence=`{row['evidence_mode']}` | layer=`{row['layer']}` | disposition=`{row['disposition']}`"
            )
            lines.append(
                f"- Scope: asset=`{row['asset_scope']}` | venue=`{row['venue_scope']}` | research_only=`{row['research_only']}` | composite=`{row['is_composite']}` | context_tag=`{row['is_context_tag']}`"
            )
            lines.append(
                f"- Runtime config: signal=`{row['signal_column']}` | file=`{row['events_file']}` | templates=`{row['templates']}` | horizons=`{row['horizons']}` | max_candidates=`{row['max_candidates_per_run']}`"
            )
            lines.append(f"- Thresholds: `{row['threshold_parameters']}`")
            if row.get("tags"):
                lines.append(f"- Tags: `{row['tags']}`")
            if row.get("notes"):
                lines.append(f"- Notes: {row['notes']}")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_outputs() -> dict[Path, str]:
    rows = _rows()
    grouped = _grouped_rows(rows)
    out_root = Path("docs/generated")
    return {
        out_root / "event_contract_reference.json": _json_text({"rows": rows, "by_regime": grouped}),
        out_root / "event_contract_reference.csv": _csv_text(rows),
        out_root / "event_contract_reference_wide.csv": _csv_wide_text(rows),
        out_root / "event_contract_reference.md": _render_markdown(rows),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate event contract reference artifacts.")
    parser.add_argument("--check", action="store_true", help="Fail if generated artifacts drift.")
    args = parser.parse_args(argv)

    drift: list[str] = []
    for path, content in build_outputs().items():
        if not _write_or_check(path, content, check=args.check):
            drift.append(str(path))
    if drift:
        for path in drift:
            print(f"event contract reference drift: {path}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
