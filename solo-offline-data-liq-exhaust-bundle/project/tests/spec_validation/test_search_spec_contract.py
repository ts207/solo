from __future__ import annotations

from pathlib import Path

import pytest

from project.spec_validation.cli import run_all_validations
from project.spec_validation.search import validate_search_spec_doc


def test_validate_search_spec_accepts_supported_optional_fields() -> None:
    validate_search_spec_doc(
        {
            "kind": "search_spec",
            "triggers": {"events": ["VOL_SHOCK"]},
            "expression_templates": ["continuation"],
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
                "expression_templates": ["continuation"],
                "horizons": ["15m"],
                "directions": ["long"],
                "entry_lag": 1,
                "cost_profiles": ["premium"],
            },
            source="inline_search_spec",
        )


def test_validate_search_spec_rejects_zero_entry_lag() -> None:
    with pytest.raises(ValueError, match="must be >= 1"):
        validate_search_spec_doc(
            {
                "kind": "search_spec",
                "triggers": {"events": ["VOL_SHOCK"]},
                "expression_templates": ["continuation"],
                "horizons": ["15m"],
                "directions": ["long"],
                "entry_lag": 0,
            },
            source="inline_search_spec",
        )


def test_validate_search_spec_rejects_filter_templates_as_top_level_hypothesis_templates() -> None:
    with pytest.raises(ValueError, match="filter templates belong in .*top-level templates"):
        validate_search_spec_doc(
            {
                "kind": "search_spec",
                "triggers": {"events": ["VOL_SHOCK"]},
                "expression_templates": ["only_if_regime"],
                "horizons": ["15m"],
                "directions": ["long"],
                "entry_lag": 1,
            },
            source="inline_search_spec",
        )


def test_spec_validation_cli_checks_real_search_specs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    search_dir = tmp_path / "search"
    search_dir.mkdir(parents=True, exist_ok=True)
    (search_dir / "search_valid.yaml").write_text(
        "\n".join(
            [
                "kind: search_spec",
                "triggers:",
                "  events:",
                "    - VOL_SHOCK",
                "expression_templates: [continuation]",
                "horizons: [15m]",
                "directions: [long]",
                "entry_lag: 1",
                "cost_profiles: [standard]",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (search_dir / "search_invalid.yaml").write_text(
        "\n".join(
            [
                "kind: search_spec",
                "triggers:",
                "  events:",
                "    - VOL_SHOCK",
                "expression_templates: [continuation]",
                "horizons: [15m]",
                "directions: [long]",
                "entry_lag: 0",
                "",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("project.spec_validation.cli.SEARCH_DIR", search_dir)

    assert run_all_validations() == 1



def test_validate_search_spec_accepts_optional_filter_templates() -> None:
    validate_search_spec_doc(
        {
            "kind": "search_spec",
            "triggers": {"events": ["VOL_SHOCK"]},
            "expression_templates": ["continuation"],
            "filter_templates": ["only_if_regime"],
            "horizons": ["15m"],
            "directions": ["long"],
            "entry_lag": 1,
        },
        source="inline_search_spec",
    )
