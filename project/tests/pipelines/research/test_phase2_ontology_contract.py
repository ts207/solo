from __future__ import annotations

import pandas as pd

from project.research.multiplicity import resolve_state_context_column
from project.research.services.phase2_support import bool_mask_from_series, optional_token


def test_phase2_state_context_column_resolution_uses_canonical_mapping():
    cols = pd.Index(["timestamp", "low_liquidity_state", "vol_regime"])
    resolved = resolve_state_context_column(cols, "LOW_LIQUIDITY_STATE")
    assert resolved == "low_liquidity_state"


def test_phase2_bool_mask_from_series_accepts_numeric_and_text_flags():
    numeric = bool_mask_from_series(pd.Series([1, 0, 2, None]))
    text = bool_mask_from_series(pd.Series(["true", "false", "yes", "off"]))
    assert numeric.tolist() == [True, False, True, False]
    assert text.tolist() == [True, False, True, False]


def test_optional_token_normalizes_null_markers():
    assert optional_token(None) is None
    assert optional_token(float("nan")) is None
    assert optional_token("None") is None
    assert optional_token(" null ") is None
    assert optional_token("NaN") is None
    assert optional_token("LOW_LIQUIDITY_STATE") == "LOW_LIQUIDITY_STATE"
