from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from project.events.calibration.registry import latest_calibration_artifact
from project.events.event_aliases import event_alias_policy_rows
from project.events.registry import build_detector_version_inventory_rows, list_governed_detectors


def _md_table(headers: list[str], rows: list[list[object]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        out.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(out)


def build_governance_artifacts(output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    contracts = list_governed_detectors()
    inventory_rows = build_detector_version_inventory_rows()
    inventory_rows.sort(key=lambda row: str(row["event_name"]))

    governed_count = len(inventory_rows)
    legacy_rows = [r for r in inventory_rows if r["event_version"] != "v2"]
    v2_rows = [r for r in inventory_rows if r["event_version"] == "v2"]
    runtime_non_v2 = [r for r in inventory_rows if r["event_version"] != "v2" and r["runtime_default"]]
    runtime_v2 = [r for r in inventory_rows if r["event_version"] == "v2" and r["runtime_default"]]
    legacy_retired_safe = [r for r in legacy_rows if r["legacy_retired_safe"]]

    summary = {
        "governed_detectors": governed_count,
        "legacy_detectors": len(legacy_rows),
        "v2_detectors": len(v2_rows),
        "legacy_retired_safe": len(legacy_retired_safe),
        "runtime_non_v2": len(runtime_non_v2),
        "runtime_v2": len(runtime_v2),
        "alias_count": len(event_alias_policy_rows()),
        "role_counts": dict(Counter(str(r["role"]) for r in inventory_rows)),
        "band_counts": dict(Counter(str(r["detector_band"]) for r in inventory_rows)),
        "version_counts": dict(Counter(str(r["event_version"]) for r in inventory_rows)),
    }

    runtime_rows = [[r["event_name"], r["event_version"], r["detector_band"], r["role"], r["maturity"], r["runtime_default"], r["promotion_eligible"], r["supports_confidence"], r["emits_quality_flag"]] for r in inventory_rows]
    (output_dir / 'detector_runtime_matrix.md').write_text('# Detector Runtime Matrix\n\n' + _md_table(['Event','Version','Band','Role','Maturity','Runtime Eligible','Promotion Eligible','Confidence','Quality Flag'], runtime_rows) + '\n', encoding='utf-8')

    promotion_rows = [[r["event_name"], r["event_version"], r["detector_band"], r["maturity"], r["primary_anchor_eligible"], r["runtime_default"], r["promotion_eligible"]] for r in inventory_rows]
    (output_dir / 'detector_promotion_matrix.md').write_text('# Detector Promotion Matrix\n\n' + _md_table(['Event','Version','Band','Maturity','Primary Anchor','Runtime Eligible','Promotion Eligible'], promotion_rows) + '\n', encoding='utf-8')

    calibration_rows = []
    for c in contracts:
        if c.event_version != 'v2':
            continue
        artifact = latest_calibration_artifact(c.event_name, preferred_version=c.event_version)
        symbol_group = artifact.symbol_group if artifact else ''
        timeframe_group = artifact.timeframe_group if artifact else ''
        lineage = artifact.dataset_lineage.get("calibration_input_dataset", "") if artifact else ""
        robustness = artifact.robustness.get("status", "") if artifact else ""
        calibration_rows.append([c.event_name, c.event_version, c.threshold_schema_version, c.calibration_mode, symbol_group, timeframe_group, lineage, robustness])
    (output_dir / 'detector_calibration_matrix.md').write_text('# Detector Calibration Matrix\n\n' + _md_table(['Event','Version','Threshold Version','Calibration Mode','Symbol Group','Timeframe Group','Calibration Dataset','Robustness Status'], calibration_rows) + '\n', encoding='utf-8')

    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in inventory_rows:
        grouped[str(row['role'])].append(row)
    role_parts = [f'# Detector Role Inventory\n\nTotal governed detectors: **{governed_count}**\n']
    for role in sorted(grouped):
        rows = grouped[role]
        role_parts.append(f'## {role}\n\nCount: **{len(rows)}**\n')
        for row in rows:
            role_parts.append(f"- `{row['event_name']}` — band `{row['detector_band']}`, version `{row['event_version']}`, maturity `{row['maturity']}`, runtime `{row['runtime_default']}`, promotion `{row['promotion_eligible']}`")
        role_parts.append('')
    (output_dir / 'detector_role_inventory.md').write_text('\n'.join(role_parts).strip() + '\n', encoding='utf-8')

    version_rows = [[version, sum(1 for r in inventory_rows if r['event_version']==version), sum(1 for r in inventory_rows if r['event_version']==version and r['runtime_default']), sum(1 for r in inventory_rows if r['event_version']==version and r['promotion_eligible'])] for version in sorted(summary['version_counts'])]
    version_md = ['# Detector Version Coverage\n', _md_table(['Version','Count','Runtime Eligible','Promotion Eligible'], version_rows), '', f"Runtime eligible non-v2 detectors: **{len(runtime_non_v2)}**", f"Runtime eligible v2 detectors: **{len(runtime_v2)}**"]
    (output_dir / 'detector_version_coverage.md').write_text('\n'.join(version_md) + '\n', encoding='utf-8')

    legacy_md = ['# Legacy Detector Retirement\n', f"Legacy detectors: **{len(legacy_rows)}**", f"Legacy detectors retired-safe: **{len(legacy_retired_safe)}**", '', _md_table(['Event','Role','Runtime Eligible','Promotion Eligible','Primary Anchor','Retired Safe'], [[r['event_name'], r['role'], r['runtime_default'], r['promotion_eligible'], r['primary_anchor_eligible'], r['legacy_retired_safe']] for r in legacy_rows])]
    (output_dir / 'legacy_detector_retirement.md').write_text('\n'.join(legacy_md) + '\n', encoding='utf-8')

    alias_rows = event_alias_policy_rows()
    alias_table = [
        [
            row["alias"],
            row["canonical_event_type"],
            row["scope"],
            row["planning_identity"],
            row["runtime_identity"],
            row["promotion_identity"],
            row["reason"],
        ]
        for row in alias_rows
    ]
    (output_dir / 'detector_alias_policy.md').write_text('# Detector Alias Policy\n\n' + _md_table(['Alias','Canonical Event','Scope','Planning Identity','Runtime Identity','Promotion Identity','Reason'], alias_table) + '\n', encoding='utf-8')
    (output_dir / 'detector_alias_policy.json').write_text(json.dumps(list(alias_rows), indent=2, sort_keys=True), encoding='utf-8')

    (output_dir / 'detector_governance_summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-dir', default='docs/generated')
    args = parser.parse_args()
    build_governance_artifacts(Path(args.output_dir))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
