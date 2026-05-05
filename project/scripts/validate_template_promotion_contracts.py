#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import yaml

ABSTRACT_STATUS = "abstract_template_family"
FILTER_TYPES = {"filter_template", "execution_policy_template"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate template promotion contract invariants.")
    parser.add_argument("--registry", default="spec/templates/registry.yaml")
    args = parser.parse_args()
    path = Path(args.registry)
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    templates = {}
    if isinstance(payload, dict):
        if isinstance(payload.get("operators"), dict):
            templates = payload.get("operators", {})
        elif isinstance(payload.get("templates"), dict):
            templates = payload.get("templates", {})
    failures: list[str] = []
    for template_id, row in templates.items():
        if not isinstance(row, dict):
            continue
        status = str(row.get("contract_status", "")).strip().lower()
        typ = str(row.get("template_type", row.get("type", ""))).strip().lower()
        promo = row.get("promotion_eligible", row.get("promotion_allowed", None))
        if status == ABSTRACT_STATUS and promo is True:
            failures.append(f"{template_id}: abstract template must not be promotion_eligible")
        if typ in FILTER_TYPES and promo is True:
            failures.append(f"{template_id}: filter/execution template must not be standalone promotion_eligible")
    if failures:
        for f in failures:
            print(f)
        return 1
    print(f"template promotion contracts OK: templates={len(templates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
