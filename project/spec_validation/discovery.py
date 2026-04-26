import json
import sys
from collections import Counter
from pathlib import Path

import yaml

from project.domain.compiled_registry import get_domain_registry
from project.research.cell_discovery.registry import load_registry


def validate_all_discovery_specs(root: Path = Path("spec/discovery")) -> list[tuple[str, str]]:
    errors: list[tuple[str, str]] = []

    # Check the root directory itself if it has the required files
    if not root.exists():
        return []

    dirs_to_check = [root] + [p for p in root.iterdir() if p.is_dir()]

    audit_data = []
    domain_registry = get_domain_registry()

    for d in dirs_to_check:
        # Check if it has event_atoms.yaml
        event_atoms_path = d / "event_atoms.yaml"
        if not event_atoms_path.exists():
            continue

        try:
            load_registry(d)

            # Additional role validation
            with open(event_atoms_path, encoding="utf-8") as f:
                doc = yaml.safe_load(f)

            for atom in doc.get("event_atoms", []):
                event_type = atom.get("event_type", "").upper()
                row = domain_registry.event_row(event_type) or {}

                promotion_eligible = bool(row.get("promotion_eligible", False))
                runtime_eligible = bool(row.get("runtime_eligible", False))
                detector_band = str(row.get("detector_band", ""))

                search_role = atom.get("search_role")
                promotion_role = atom.get("promotion_role")
                runtime_role = atom.get("runtime_role")

                # Impossible combinations
                if promotion_role == "eligible" and not promotion_eligible:
                    errors.append((str(event_atoms_path), f"Atom {atom.get('id')} has promotion_role=eligible but generated promotion_eligible=False"))

                if runtime_role == "trade_trigger" and not runtime_eligible:
                    errors.append((str(event_atoms_path), f"Atom {atom.get('id')} has runtime_role=trade_trigger but generated runtime_eligible=False"))

                audit_data.append({
                    "id": atom.get("id"),
                    "event_type": event_type,
                    "detector_band": detector_band,
                    "search_role": search_role,
                    "promotion_role": promotion_role,
                    "runtime_role": runtime_role
                })

        except Exception as e:
            errors.append((str(d), str(e)))

    # Generate audit report
    if audit_data:
        # Resolve repo root relative to discovery root
        # If root is /home/irene/Edge/spec/discovery, repo_root is /home/irene/Edge
        repo_root = root.parents[1]
        audit_path = repo_root / "docs" / "generated" / "event_atom_role_audit.json"

        summary = {
            "total_atoms": len(audit_data),
            "by_detector_band": dict(Counter(a["detector_band"] for a in audit_data)),
            "by_search_role": dict(Counter(a["search_role"] for a in audit_data)),
            "by_promotion_role": dict(Counter(a["promotion_role"] for a in audit_data)),
            "by_runtime_role": dict(Counter(a["runtime_role"] for a in audit_data)),
            "atoms": audit_data
        }

        try:
            audit_path.parent.mkdir(parents=True, exist_ok=True)
            with open(audit_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
        except Exception:
            pass

    return errors

if __name__ == "__main__":
    validation_errors = validate_all_discovery_specs()
    if validation_errors:
        print(f"FAILURE: {len(validation_errors)} discovery spec directories failed validation:")
        for loc, msg in validation_errors:
            print(f"  [{loc}] {msg}")
        sys.exit(1)
    print("SUCCESS: All discovery specs passed validation.")
    sys.exit(0)
