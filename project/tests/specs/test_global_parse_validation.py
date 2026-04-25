"""Global YAML/JSON parse validation for all repo specs, configs, and generated artifacts."""
from __future__ import annotations

import json
from pathlib import Path

import yaml

from project.spec_registry.loaders import iter_spec_yaml_files

REPO_ROOT = Path(__file__).resolve().parents[4]


def test_all_spec_yaml_files_parse_without_error():
    errors: list[tuple[str, str]] = []
    files = iter_spec_yaml_files(repo_root=REPO_ROOT)
    for path in files:
        try:
            yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append((str(path.relative_to(REPO_ROOT)), str(exc)))
    assert not errors, (
        f"YAML parse errors in {len(errors)} spec files:\n"
        + "\n".join(f"  {f}: {e}" for f, e in errors)
    )


def test_all_config_yaml_files_parse_without_error():
    config_dir = REPO_ROOT / "project" / "configs"
    errors: list[tuple[str, str]] = []
    for path in sorted(config_dir.rglob("*.yaml")):
        try:
            yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append((str(path.relative_to(REPO_ROOT)), str(exc)))
    assert not errors, (
        f"YAML parse errors in {len(errors)} config files:\n"
        + "\n".join(f"  {f}: {e}" for f, e in errors)
    )


def test_all_generated_json_artifacts_parse_without_error():
    generated_dir = REPO_ROOT / "docs" / "generated"
    errors: list[tuple[str, str]] = []
    for path in sorted(generated_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append((str(path.relative_to(REPO_ROOT)), str(exc)))
    assert not errors, (
        f"JSON parse errors in {len(errors)} generated artifacts:\n"
        + "\n".join(f"  {f}: {e}" for f, e in errors)
    )


def test_detector_eligibility_matrix_consistent_with_event_contract_reference():
    elig_path = REPO_ROOT / "docs" / "generated" / "detector_eligibility_matrix.json"
    contract_path = REPO_ROOT / "docs" / "generated" / "event_contract_reference.json"
    if not elig_path.exists() or not contract_path.exists():
        return
    elig = json.loads(elig_path.read_text(encoding="utf-8"))
    elig_map = {r["event_name"]: r for r in elig if isinstance(r, dict)}
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        return

    inconsistencies: list[str] = []
    for event_id, contract_data in contract.items():
        if event_id not in elig_map:
            continue
        elig_row = elig_map[event_id]
        contract_band = str(contract_data.get("detector_band", "") or "")
        elig_band = str(elig_row.get("detector_band", "") or "")
        if contract_band and elig_band and contract_band != elig_band:
            inconsistencies.append(
                f"{event_id}: detector_band mismatch: contract={contract_band!r} vs eligibility={elig_band!r}"
            )
        contract_promo = bool(contract_data.get("promotion_eligible", False))
        elig_promo = bool(elig_row.get("promotion", False))
        if contract_promo != elig_promo:
            inconsistencies.append(
                f"{event_id}: promotion_eligible mismatch: contract={contract_promo} vs eligibility={elig_promo}"
            )

    assert not inconsistencies, (
        f"Artifact consistency failures ({len(inconsistencies)}):\n"
        + "\n".join(f"  {i}" for i in inconsistencies)
    )
