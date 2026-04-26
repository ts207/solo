import argparse
import sys
from pathlib import Path

import yaml

KNOWN_DATASETS = {
    "perp_ohlcv_1m",
    "spot_ohlcv_1m",
    "perp_ohlcv_15m",
    "tob_1s",
    "tob_1m_agg",
    "basis_1m",
    "um_funding_rates",
    "um_open_interest_hist",
    "events.parquet",
    "forward_labels",
    "run_manifest.json",
    "validation_results",
    "equity_curves.parquet",
    "universe_snapshots.parquet",
    "feature_vectors",
    "event_flags",
    "event_registry",
    "sources",
    "fragments.jsonl",
    "blueprints.jsonl",
    "cleaned_1m",
    "cleaned_bars",
}


def load_specs(spec_dir: Path) -> dict[str, dict]:
    specs = {}
    for yaml_file in spec_dir.glob("*.yaml"):
        with open(yaml_file) as f:
            spec = yaml.safe_load(f)
            specs[spec["concept_id"]] = spec
    return specs


def check_cycles(specs: dict[str, dict]):
    graph = {cid: spec.get("dependencies", []) for cid, spec in specs.items()}
    visited = set()
    path = []

    def visit(node):
        if node in path:
            print(f"ERROR: Cycle detected: {' -> '.join(path + [node])}")
            return False
        if node in visited:
            return True

        visited.add(node)
        path.append(node)
        for neighbor in graph.get(node, []):
            if not visit(neighbor):
                return False
        path.pop()
        return True

    for node in graph:
        if node not in visited:
            if not visit(node):
                sys.exit(1)
    print("SUCCESS: No cycles in dependency DAG.")


def check_datasets(specs: dict[str, dict]):
    errors = 0
    for cid, spec in specs.items():
        for req in spec.get("data_requirements", []):
            ds = req.get("dataset")
            if ds not in KNOWN_DATASETS:
                print(f"ERROR: Unknown dataset '{ds}' in concept {cid}")
                errors += 1
    if errors == 0:
        print("SUCCESS: All dataset contracts valid.")
    else:
        sys.exit(1)


def check_tests(specs: dict[str, dict]):
    test_ids = set()
    errors = 0
    for cid, spec in specs.items():
        for test in spec.get("tests", []):
            tid = test.get("id")
            if tid in test_ids:
                print(f"ERROR: Duplicate test ID '{tid}' in concept {cid}")
                errors += 1
            test_ids.add(tid)
    if errors == 0:
        print(f"SUCCESS: Test catalog unique. Total tests: {len(test_ids)}")
    else:
        sys.exit(1)


def _is_runtime_artifact_path(path_str: str) -> bool:
    normalized = path_str.strip()
    return normalized.startswith("data/") or "<" in normalized or ">" in normalized


def _resolve_artifact_path(
    path_str: str,
    *,
    project_root: Path,
    placeholder_values: dict[str, str] | None = None,
) -> Path:
    resolved = path_str.replace("{symbol}", "BTCUSDT")
    for key, value in (placeholder_values or {}).items():
        resolved = resolved.replace(f"<{key}>", value)
        resolved = resolved.replace(f"{{{key}}}", value)
    return project_root / resolved


def _parse_placeholder_args(raw_values: list[str] | None) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in raw_values or []:
        if "=" not in item:
            raise ValueError(f"Invalid runtime placeholder override {item!r}; expected KEY=VALUE")
        key, value = item.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def check_artifacts(
    specs: dict[str, dict],
    project_root: Path,
    *,
    strict_runtime_artifacts: bool = False,
    runtime_placeholder_values: dict[str, str] | None = None,
):
    missing = []
    runtime_artifacts = []
    missing_runtime = []
    for cid, spec in specs.items():
        for art in spec.get("artifacts", []):
            path_str = art.get("path")
            if not path_str:
                continue
            if _is_runtime_artifact_path(path_str):
                runtime_artifacts.append(f"{cid}: {path_str}")
                resolved_runtime_path = _resolve_artifact_path(
                    path_str,
                    project_root=project_root,
                    placeholder_values=runtime_placeholder_values,
                )
                if strict_runtime_artifacts and not resolved_runtime_path.exists():
                    missing_runtime.append(f"{cid}: {path_str} -> {resolved_runtime_path}")
                continue

            path = _resolve_artifact_path(
                path_str,
                project_root=project_root,
                placeholder_values=runtime_placeholder_values,
            )
            if not path.exists():
                missing.append(f"{cid}: {path_str}")

    if missing:
        print("REPORT: Missing authored artifacts:")
        for m in missing:
            print(f"  - {m}")
    else:
        print("SUCCESS: All authored artifacts exist.")

    if runtime_artifacts:
        if strict_runtime_artifacts:
            if missing_runtime:
                print("ERROR: Missing runtime artifacts under strict enforcement:")
                for artifact in missing_runtime:
                    print(f"  - {artifact}")
                sys.exit(1)
            print("SUCCESS: All runtime artifacts resolved under strict enforcement.")
        else:
            print("REPORT: Runtime artifacts not statically checked:")
            for artifact in runtime_artifacts:
                print(f"  - {artifact}")


def _check_detector_contract_completeness(data: dict, fname: str, errors: list) -> None:
    """If detector_contract is declared, enforce required sections are present."""
    if not data.get("detector_contract"):
        return
    for section in ("detector", "calibration", "expected_behavior"):
        if section not in data:
            errors.append(
                f"{fname}: declares detector_contract=true but missing '{section}' section"
            )


def check_detector_contracts(specs: dict[str, dict]):
    errors = []
    for fname, data in specs.items():
        _check_detector_contract_completeness(data, fname, errors)
    if errors:
        for e in errors:
            print(f"ERROR: {e}")
        sys.exit(1)
    else:
        print("SUCCESS: All detector_contract specs have required sections.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Lint concept/spec governance surfaces.")
    parser.add_argument("--spec-dir", default="spec/concepts")
    parser.add_argument("--strict-runtime-artifacts", action="store_true")
    parser.add_argument(
        "--runtime-placeholder",
        action="append",
        default=[],
        help="Placeholder override for runtime artifacts, e.g. run_id=my_run",
    )
    args = parser.parse_args(argv)

    project_root = Path(".").resolve()
    spec_dir = project_root / str(args.spec_dir)

    if not spec_dir.exists():
        print(f"ERROR: Spec directory {spec_dir} not found.")
        sys.exit(1)

    placeholder_values = _parse_placeholder_args(list(args.runtime_placeholder or []))
    specs = load_specs(spec_dir)
    print(f"Loaded {len(specs)} concepts.")

    check_cycles(specs)
    check_datasets(specs)
    check_tests(specs)
    check_artifacts(
        specs,
        project_root,
        strict_runtime_artifacts=bool(args.strict_runtime_artifacts),
        runtime_placeholder_values=placeholder_values,
    )
    check_detector_contracts(specs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
