from __future__ import annotations

import pytest

from project.reliability.regression_checks import validate_timeframe_contracts


def test_timeframe_contracts_reject_unknown_values():
    with pytest.raises(AssertionError):
        validate_timeframe_contracts(["5m", "7m"])
