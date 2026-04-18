from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES
from project.events.event_aliases import EVENT_ALIASES
from project.events.governance import default_planning_event_ids, get_event_governance_metadata
from project.events.registry import (
    build_detector_version_inventory_rows,
    list_governed_detectors,
    list_legacy_detectors,
    list_v2_detectors,
    load_milestone_event_registry,
)


VALID_DETECTOR_BANDS = {"deployable_core", "research_trigger", "context_only", "composite_or_fragile"}
REQUIRED_SPEC_GOVERNANCE_FIELDS = {
    "detector_band",
    "planning_eligible",
    "runtime_eligible",
    "promotion_eligible",
    "primary_anchor_eligible",
}


def test_legacy_detectors_are_retired_safe():
    rows = build_detector_version_inventory_rows()
    legacy = [row for row in rows if row["event_version"] != "v2"]
    assert legacy
    assert all(not row["runtime_default"] for row in legacy)
    assert all(not row["promotion_eligible"] for row in legacy)
    assert all(not row["primary_anchor_eligible"] for row in legacy)
    assert all(row["legacy_retired_safe"] for row in legacy)


def test_runtime_surface_is_v2_only():
    rows = build_detector_version_inventory_rows()
    runtime_rows = [row for row in rows if row["runtime_default"]]
    assert runtime_rows
    assert all(row["event_version"] == "v2" for row in runtime_rows)
    assert {row["event_name"] for row in runtime_rows} == DEPLOYABLE_CORE_EVENT_TYPES
    assert all(row["detector_band"] == "deployable_core" for row in runtime_rows)


def test_detector_band_is_first_class_for_every_detector():
    rows = build_detector_version_inventory_rows()
    assert all(row["detector_band"] in VALID_DETECTOR_BANDS for row in rows)

    registry = load_milestone_event_registry()
    missing = {
        event_name: sorted(REQUIRED_SPEC_GOVERNANCE_FIELDS - set(row))
        for event_name, row in registry.items()
        if REQUIRED_SPEC_GOVERNANCE_FIELDS - set(row)
    }
    assert not missing


def test_aliases_do_not_create_detector_governance_identities():
    rows = build_detector_version_inventory_rows()
    detector_ids = {str(row["event_name"]) for row in rows}
    runtime_ids = {str(row["event_name"]) for row in rows if row["runtime_default"]}

    assert set(EVENT_ALIASES).isdisjoint(detector_ids)
    assert set(EVENT_ALIASES).isdisjoint(runtime_ids)

    planning_ids = set(default_planning_event_ids([*EVENT_ALIASES, *detector_ids]))
    assert set(EVENT_ALIASES).isdisjoint(planning_ids)
    assert all(canonical in detector_ids for canonical in EVENT_ALIASES.values())

    for alias, canonical in EVENT_ALIASES.items():
        meta = get_event_governance_metadata(alias)
        assert meta["event_type"] == canonical


def test_detector_version_inventory_helpers_cover_registry():
    governed = list_governed_detectors()
    legacy = list_legacy_detectors()
    v2 = list_v2_detectors()
    assert len(governed) == len(legacy) + len(v2)
    assert len(governed) == 71
