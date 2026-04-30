from __future__ import annotations

from project.research.regime_event_inventory import (
    EVENT_INVENTORY_COLUMNS,
    build_event_inventory,
    build_regime_event_inventory,
    load_context_registry,
    load_template_ids,
)


def _rows_by_id(rows):
    return {row["id"]: row for row in rows}


def test_event_inventory_classifies_funding_events():
    rows = _rows_by_id(build_event_inventory())

    assert rows["FUNDING_EXTREME"]["registered_unified"] is False
    assert rows["FUNDING_EXTREME"]["classification"] == "invalid_unregistered"
    assert rows["FUNDING_EXTREME"]["recommended_action"] == "replace_with_registered_event"
    assert rows["FUNDING_EXTREME"]["active_candidate_event"] is False
    assert rows["FUNDING_EXTREME"]["draft_event"] is True

    assert rows["FUNDING_EXTREME_ONSET"]["registered_unified"] is True
    assert rows["FUNDING_EXTREME_ONSET"]["classification"] == "registered_executable"
    assert rows["FUNDING_EXTREME_ONSET"]["active_candidate_event"] is True
    assert (
        rows["FUNDING_EXTREME_ONSET"]["recommended_action"]
        == "eligible_for_baseline_or_event_lift"
    )

    assert rows["FUNDING_NEG_EXTREME_ONSET"]["registered_unified"] is True
    assert rows["FUNDING_NEG_EXTREME_ONSET"]["classification"] == "registered_maybe_not_materialized"
    assert rows["FUNDING_POS_EXTREME_ONSET"]["classification"] == "registered_maybe_not_materialized"


def test_context_and_template_registry_helpers_include_legacy_regime_dimensions():
    context_registry = load_context_registry()

    assert context_registry.has_dimension("vol_regime")
    assert context_registry.is_value_allowed("VOL_REGIME", "HIGH")
    assert context_registry.is_materializable("carry_state")
    assert "exhaustion_reversal" in load_template_ids()


def test_combined_inventory_has_stable_columns():
    df = build_regime_event_inventory()

    assert list(df.columns) == EVENT_INVENTORY_COLUMNS
    assert {"event", "context_dimension", "state", "mechanism"}.issubset(set(df["kind"]))


def test_funding_squeeze_inventory_has_no_active_invalid_events():
    df = build_regime_event_inventory()
    row = df[(df["kind"] == "mechanism") & (df["id"] == "funding_squeeze")].iloc[0]

    assert row["active_invalid_event_count"] == 0
    assert row["conditional_maybe_not_materialized_event_count"] > 0
    assert row["recommended_action"] == "baseline_and_event_lift_before_proposal"
