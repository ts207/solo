import pytest
import yaml

from project.spec_registry import SPEC_ROOT

SPEC_DIR = SPEC_ROOT / "events"
COMPAT_FILE = SPEC_DIR / "compatibility.yaml"
PREC_FILE = SPEC_DIR / "precedence.yaml"


def test_compatibility_yaml_exists():
    assert COMPAT_FILE.exists()


def test_precedence_yaml_exists():
    assert PREC_FILE.exists()


def test_compatibility_yaml_valid():
    data = yaml.safe_load(COMPAT_FILE.read_text())
    assert "suppression_rules" in data
    assert "composite_events" in data
    for rule in data["suppression_rules"]:
        assert "when_active" in rule
        assert "suppress" in rule
        assert "reason" in rule
    for rule in data["suppression_rules"]:
        assert "penalty_factor" in rule, (
            f"Missing penalty_factor in rule for {rule.get('when_active')}"
        )
        assert isinstance(rule["penalty_factor"], float), (
            f"penalty_factor must be float in rule for {rule.get('when_active')}"
        )
        assert 0.0 <= rule["penalty_factor"] <= 1.0, (
            f"penalty_factor out of range in rule for {rule.get('when_active')}"
        )
        assert "block" in rule, f"Missing block in rule for {rule.get('when_active')}"
        assert isinstance(rule["block"], bool), (
            f"block must be bool in rule for {rule.get('when_active')}"
        )


def test_precedence_yaml_valid():
    data = yaml.safe_load(PREC_FILE.read_text())
    assert "family_precedence" in data
    assert "event_overrides" in data
    for entry in data["family_precedence"]:
        assert "family" in entry
        assert "priority" in entry
    for entry in data["family_precedence"]:
        assert isinstance(entry["priority"], int), (
            f"priority must be int for family {entry.get('family')}"
        )
    for override in data.get("event_overrides", []):
        assert "override_priority" in override
        assert isinstance(override["override_priority"], int), (
            f"override_priority must be int for {override.get('event_type')}"
        )


def _load_known_event_types() -> set:
    known = set()
    for p in SPEC_DIR.glob("*.yaml"):
        if p.name.startswith("_") or p.name in (
            "compatibility.yaml",
            "precedence.yaml",
            "canonical_event_registry.yaml",
            "event_registry_unified.yaml",
            "DESIGN.yaml",
        ):
            continue
        try:
            d = yaml.safe_load(p.read_text())
            if d and "event_type" in d:
                known.add(d["event_type"])
        except Exception:
            pass
    return known


def test_suppression_rules_reference_known_event_types():
    known = _load_known_event_types()
    data = yaml.safe_load(COMPAT_FILE.read_text())
    for rule in data["suppression_rules"]:
        for et in [rule["when_active"]] + rule["suppress"]:
            assert et in known, f"Unknown event type in compatibility.yaml: {et}"


def test_composite_events_reference_known_event_types():
    known = _load_known_event_types()
    data = yaml.safe_load(COMPAT_FILE.read_text())
    for comp in data.get("composite_events", []):
        for et in comp.get("required", []):
            assert et in known, f"Unknown event type in composite_events.required: {et}"


def test_precedence_overrides_reference_known_event_types():
    known = _load_known_event_types()
    data = yaml.safe_load(PREC_FILE.read_text())
    for override in data.get("event_overrides", []):
        et = override["event_type"]
        assert et in known, f"Unknown event type in event_overrides: {et}"
