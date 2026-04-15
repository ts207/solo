# tests/specs/test_detector_spec_schema.py
import pytest
import yaml
from pathlib import Path

from project.events.detector_contract import DetectorContract

SPEC_DIR = Path(__file__).resolve().parents[2] / "spec" / "events"

REQUIRED_DETECTOR_KEYS = {
    "signal_definition",
    "formula",
    "required_columns",
    "lookback_bars",
    "warmup_bars",
    "bar_type",
}
REQUIRED_CALIBRATION_KEYS = {
    "default_threshold",
    "search_range",
    "calibration_target",
    "stability_requirement",
}
REQUIRED_BEHAVIOR_KEYS = {
    "false_positive_scenarios",
    "disabled_regimes",
    "expected_overlap",
}


def load_detector_specs():
    specs = []
    for p in sorted(SPEC_DIR.glob("*.yaml")):
        if p.name.startswith("_") or p.name in (
            "canonical_event_registry.yaml",
            "event_registry_unified.yaml",
            "DESIGN.yaml",
        ):
            continue
        try:
            data = yaml.safe_load(p.read_text())
        except yaml.YAMLError as exc:
            raise RuntimeError(f"Failed to parse {p.name}: {exc}") from exc
        if not data or not data.get("detector_contract"):
            continue
        specs.append((p.name, data))
    return specs


DETECTOR_SPECS = load_detector_specs()


@pytest.mark.parametrize("fname, data", DETECTOR_SPECS)
def test_detector_section_present(fname, data):
    assert "detector" in data, f"{fname} missing 'detector' section"
    missing = REQUIRED_DETECTOR_KEYS - set(data["detector"].keys())
    assert not missing, f"{fname} detector missing keys: {missing}"
    bar_type = data["detector"].get("bar_type")
    assert bar_type in DetectorContract._VALID_BAR_TYPES, (
        f"{fname} detector.bar_type={bar_type!r} not in {sorted(DetectorContract._VALID_BAR_TYPES)}"
    )


@pytest.mark.parametrize("fname, data", DETECTOR_SPECS)
def test_calibration_section_present(fname, data):
    assert "calibration" in data, f"{fname} missing 'calibration' section"
    missing = REQUIRED_CALIBRATION_KEYS - set(data["calibration"].keys())
    assert not missing, f"{fname} calibration missing keys: {missing}"


@pytest.mark.parametrize("fname, data", DETECTOR_SPECS)
def test_expected_behavior_section_present(fname, data):
    assert "expected_behavior" in data, f"{fname} missing 'expected_behavior' section"
    missing = REQUIRED_BEHAVIOR_KEYS - set(data["expected_behavior"].keys())
    assert not missing, f"{fname} expected_behavior missing keys: {missing}"
