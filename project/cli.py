import sys
import argparse
from typing import List, Tuple

from project.spec_validation.ontology import validate_ontology
from project.spec_validation.grammar import validate_grammar
from project.spec_validation.loaders import load_search_spec, SEARCH_DIR
from project.spec_validation.search import validate_search_spec_doc


def run_all_validations() -> int:
    all_errors: List[Tuple[str, str]] = []

    print("Running Ontology validation...")
    all_errors.extend(validate_ontology())

    print("Running Grammar validation...")
    all_errors.extend(validate_grammar())

    # Validate each search spec
    print("Running Search spec validation...")
    for p in SEARCH_DIR.glob("*.yaml"):
        print(f"  Checking {p.name}...")
        try:
            doc = load_search_spec(str(p))
            validate_search_spec_doc(doc, source=str(p))
        except Exception as exc:
            all_errors.append((str(p), str(exc)))

    if not all_errors:
        print("\nSUCCESS: All specs are consistent.")
        return 0
    else:
        print(f"\nFAILURE: Found {len(all_errors)} errors:")
        for loc, msg in all_errors:
            print(f"  [{loc}] {msg}")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_validations())
