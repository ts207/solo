import json
from pathlib import Path

import yaml


def find_missing_matrix_entries():
    repo_root = Path("/home/irene/Edge")
    matrix_path = repo_root / "spec" / "compatibility" / "event_template_matrix.yaml"
    with open(matrix_path) as f:
        matrix_doc = yaml.safe_load(f)
    matrix = matrix_doc.get("event_template_matrix", {})

    eligibility_path = repo_root / "docs/generated/detector_eligibility_matrix.json"
    with open(eligibility_path) as f:
        eligibility_rows = json.load(f)

    # Note: I'll check maturity: production or tier: A as well
    # because some might be missing the runtime: true flag due to the registry generator issue.

    # Let's also check event_registry_unified.yaml to see tier
    unified_path = repo_root / "spec/events/event_registry_unified.yaml"
    with open(unified_path) as f:
        unified_doc = yaml.safe_load(f)
    events = unified_doc.get("events", {})

    missing_runtime = []
    missing_production = []

    for event_name, row in events.items():
        if event_name not in matrix:
            if row.get("runtime_eligible"):
                missing_runtime.append(event_name)
            elif row.get("tier") == "A" or row.get("maturity") == "production":
                missing_production.append(event_name)

    print("Missing runtime-eligible events in matrix:")
    for e in sorted(missing_runtime):
        print(f"  - {e}")

    print("\nMissing Tier-A/Production events in matrix (potentially should be runtime-eligible):")
    for e in sorted(missing_production):
        print(f"  - {e}")

if __name__ == "__main__":
    find_missing_matrix_entries()
