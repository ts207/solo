from __future__ import annotations

import pandas as pd
import pytest

from project.reliability.regression_checks import require_full_market_bars


def test_event_analyzer_requires_full_market_bars():
    events = pd.DataFrame(
        {"timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"])}
    )
    market = events.copy()
    with pytest.raises(AssertionError):
        require_full_market_bars(events, market)
