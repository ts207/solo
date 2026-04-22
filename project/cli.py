from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Tuple

from project.spec_validation.grammar import validate_grammar
from project.spec_validation.loaders import SEARCH_DIR as DEFAULT_SEARCH_DIR, load_search_spec

SEARCH_DIR = DEFAULT_SEARCH_DIR
from project.spec_validation.ontology import validate_ontology


def run_all_validations(*, root: Path | str = ".", verbose: bool = False) -> int:
    repo_root = Path(root).resolve()
    search_dir = Path(SEARCH_DIR)
    if search_dir == Path(DEFAULT_SEARCH_DIR):
        search_dir = repo_root / "spec" / "search"
    all_errors: List[Tuple[str, str]] = []

    if verbose:
        print(f"Spec validation root: {repo_root}")

    print("Running Ontology validation...")
    all_errors.extend(validate_ontology())

    print("Running Grammar validation...")
    all_errors.extend(validate_grammar())

    print("Running Search spec validation...")
    for p in sorted(search_dir.glob("*.yaml")):
        print(f"  Checking {p.name}...")
        try:
            doc = load_search_spec(str(p))
            if not doc:
                all_errors.append((str(p), "empty or unreadable search spec"))
        except Exception as exc:
            all_errors.append((str(p), str(exc)))

    if not all_errors:
        print("\nSUCCESS: All specs are consistent.")
        return 0

    print(f"\nFAILURE: Found {len(all_errors)} errors:")
    for loc, msg in all_errors:
        print(f"  [{loc}] {msg}")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate ontology, grammar, and search specs.")
    parser.add_argument("--root", default=".")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    return run_all_validations(root=Path(args.root), verbose=args.verbose)


if __name__ == "__main__":
    sys.exit(main())
