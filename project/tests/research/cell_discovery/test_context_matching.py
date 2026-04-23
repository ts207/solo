from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml

from project.research.cell_discovery.data_feasibility import _context_state_mask
from project.research.cell_discovery.registry import load_registry


def _write_minimal_discovery_spec(base: Path, *, context_values: list[str]) -> None:
    (base / "event_atoms.yaml").write_text(
        yaml.safe_dump(
            {
                "event_atoms": [
                    {
                        "id": "vol_shock_core",
                        "event_family": "volatility",
                        "event_type": "VOL_SHOCK",
                        "directions": ["long"],
                        "templates": ["continuation"],
                        "horizons": ["12b"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (base / "context_cells.yaml").write_text(
        yaml.safe_dump(
            {
                "context_cells": [
                    {
                        "id": "trend_probe",
                        "dimension": "ms_trend_state",
                        "values": context_values,
                        "required_feature_key": "ms_trend_state",
                        "executability_class": "runtime",
                        "max_conjunction_depth": 1,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (base / "horizons.yaml").write_text(yaml.safe_dump({"horizons": ["12b"]}), encoding="utf-8")
    (base / "contrast_rules.yaml").write_text(
        yaml.safe_dump(
            {
                "contrast_rules": [
                    {
                        "id": "default",
                        "type": "in_bucket_vs_unconditional",
                        "required": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (base / "ranking_policy.yaml").write_text(
        yaml.safe_dump({"ranking_policy": {"max_search_hypotheses": 10}}),
        encoding="utf-8",
    )


def test_context_state_mask_matches_dimension_encoded_values() -> None:
    frame = pd.DataFrame({"ms_trend_state": [0.0, 1.0, 2.0]})
    context = SimpleNamespace(
        dimension="ms_trend_state",
        values=("bullish",),
        required_feature_key="",
    )

    mask = _context_state_mask(frame, context)

    assert mask.tolist() == [False, True, False]


def test_context_state_mask_uses_required_feature_key_when_dimension_column_missing() -> None:
    frame = pd.DataFrame({"crowding_state": [0.0, 1.0]})
    context = SimpleNamespace(
        dimension="funding_regime",
        values=("crowded",),
        required_feature_key="crowding_state",
    )

    mask = _context_state_mask(frame, context)

    assert mask.tolist() == [False, True]


def test_load_registry_rejects_context_values_outside_authoritative_registry(tmp_path: Path) -> None:
    _write_minimal_discovery_spec(tmp_path, context_values=["trend"])

    with pytest.raises(ValueError, match="invalid values for ms_trend_state: trend"):
        load_registry(tmp_path)
