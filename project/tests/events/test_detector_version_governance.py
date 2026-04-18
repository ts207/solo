from project.events.registry import build_detector_version_inventory_rows, list_governed_detectors, list_legacy_detectors, list_v2_detectors


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


def test_detector_version_inventory_helpers_cover_registry():
    governed = list_governed_detectors()
    legacy = list_legacy_detectors()
    v2 = list_v2_detectors()
    assert len(governed) == len(legacy) + len(v2)
    assert len(governed) == 71
