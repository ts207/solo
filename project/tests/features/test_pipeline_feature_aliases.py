from __future__ import annotations

from project.features.liquidity_vacuum import (
    detect_liquidity_vacuum_events as canonical_liquidity_vacuum,
)
from project.features.vol_shock_relaxation import (
    detect_vol_shock_relaxation_events as canonical_vol_shock,
)
from project.features.liquidity_vacuum import (
    detect_liquidity_vacuum_events as pipeline_liquidity_vacuum,
)
from project.features.vol_shock_relaxation import (
    detect_vol_shock_relaxation_events as pipeline_vol_shock,
)


def test_pipeline_vol_shock_detector_is_canonical_alias():
    assert pipeline_vol_shock is canonical_vol_shock


def test_pipeline_liquidity_vacuum_detector_is_canonical_alias():
    assert pipeline_liquidity_vacuum is canonical_liquidity_vacuum
