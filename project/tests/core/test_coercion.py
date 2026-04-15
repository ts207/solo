from __future__ import annotations

import logging

import numpy as np

from project.core.coercion import safe_float, safe_int


def test_safe_float_suppresses_missing_value_warning_for_nan_default(caplog):
    with caplog.at_level(logging.WARNING):
        value = safe_float(None, np.nan)

    assert np.isnan(value)
    assert "safe_float: failed to convert" not in caplog.text


def test_safe_float_still_warns_for_invalid_non_missing_value(caplog):
    with caplog.at_level(logging.WARNING):
        value = safe_float("bad", 0.0)

    assert value == 0.0
    assert "safe_float: failed to convert" in caplog.text


def test_safe_int_suppresses_missing_value_warning_for_none_default(caplog):
    with caplog.at_level(logging.WARNING):
        value = safe_int(None, None)

    assert value is None
    assert "safe_int: failed to convert" not in caplog.text


def test_safe_int_still_warns_when_context_present(caplog):
    with caplog.at_level(logging.WARNING):
        value = safe_int(None, 0, context="promotion")

    assert value == 0
    assert "safe_int: failed to convert" in caplog.text
