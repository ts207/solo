from __future__ import annotations

import pytest

from project.spec_validation.search import validate_search_spec_doc


def test_validate_search_spec_accepts_supported_optional_fields() -> None:
    validate_search_spec_doc(
        {
            "kind": "search_spec",
            "triggers": {"events": ["VOL_SHOCK"]},
            "expression_templates": ["continuation"],
            "template_policy": {"generic_templates_allowed": True, "reason": "test"},
            "horizons": ["15m"],
            "directions": ["long"],
            "entry_lag": 1,
            "cost_profiles": ["standard"],
            "conditioning_intersections": ["CROWDING_STATE + HIGH_VOL_REGIME"],
        },
        source="inline_search_spec",
    )


def test_validate_search_spec_rejects_unsupported_cost_profiles() -> None:
    with pytest.raises(ValueError, match="Unsupported cost_profiles entries: premium"):
        validate_search_spec_doc(
            {
                "kind": "search_spec",
                "triggers": {"events": ["VOL_SHOCK"]},
                "expression_templates": ["breakout_followthrough"],
                "horizons": ["15m"],
                "directions": ["long"],
                "entry_lag": 1,
                "cost_profiles": ["premium"],
            },
            source="inline_search_spec",
        )


def test_validate_search_spec_accepts_valid_entry_lag() -> None:
    validate_search_spec_doc(
        {
            "kind": "search_spec",
            "triggers": {"events": ["VOL_SHOCK"]},
            "expression_templates": ["breakout_followthrough"],
            "horizons": ["15m"],
            "directions": ["long"],
            "entry_lag": 1,
        },
        source="inline_search_spec",
    )


def test_validate_search_spec_rejects_zero_entry_lag() -> None:
    with pytest.raises(ValueError, match="entry_lag must be >= 1"):
        validate_search_spec_doc(
            {
                "kind": "search_spec",
                "triggers": {"events": ["VOL_SHOCK"]},
                "expression_templates": ["breakout_followthrough"],
                "horizons": ["15m"],
                "directions": ["long"],
                "entry_lag": 0,
            },
            source="inline_search_spec",
        )


def test_validate_search_spec_accepts_valid_entry_lags() -> None:
    validate_search_spec_doc(
        {
            "kind": "search_spec",
            "triggers": {"events": ["VOL_SHOCK"]},
            "expression_templates": ["breakout_followthrough"],
            "horizons": ["15m"],
            "directions": ["long"],
            "entry_lags": [1, 2],
        },
        source="inline_search_spec",
    )


def test_validate_search_spec_rejects_zero_entry_lags() -> None:
    with pytest.raises(ValueError, match="entry_lags must be >= 1"):
        validate_search_spec_doc(
            {
                "kind": "search_spec",
                "triggers": {"events": ["VOL_SHOCK"]},
                "expression_templates": ["breakout_followthrough"],
                "horizons": ["15m"],
                "directions": ["long"],
                "entry_lags": [0, 1],
            },
            source="inline_search_spec",
        )


def test_validate_search_spec_rejects_abstract_templates_without_policy() -> None:
    with pytest.raises(ValueError, match="generic abstract templates are forbidden"):
        validate_search_spec_doc(
            {
                "kind": "search_spec",
                "triggers": {"events": ["VOL_SHOCK"]},
                "expression_templates": ["continuation"],
                "horizons": ["15m"],
                "directions": ["long"],
                "entry_lag": 1,
            },
            source="inline_search_spec",
        )


def test_validate_search_spec_accepts_optional_filter_templates() -> None:
    validate_search_spec_doc(
        {
            "kind": "search_spec",
            "triggers": {"events": ["VOL_SHOCK"]},
            "expression_templates": ["continuation"],
            "template_policy": {"generic_templates_allowed": True, "reason": "test"},
            "filter_templates": ["only_if_regime"],
            "horizons": ["15m"],
            "directions": ["long"],
            "entry_lag": 1,
        },
        source="inline_search_spec",
    )
