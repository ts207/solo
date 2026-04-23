from __future__ import annotations

import pandas as pd
import pytest

from project.core.context_quality import summarize_context_quality


def test_summarize_context_quality_reports_occupancy_transitions_and_distributions() -> None:
    frame = pd.DataFrame(
        {
            "ms_vol_state": [0.0, 0.0, 2.0, 2.0],
            "ms_vol_confidence": [0.9, 0.8, 0.7, 0.6],
            "ms_vol_entropy": [0.1, 0.2, 0.3, 0.4],
            "ms_liq_state": [1.0, 1.0, 1.0, 2.0],
            "ms_liq_confidence": [0.6, 0.6, 0.5, 0.9],
            "ms_liq_entropy": [0.4, 0.4, 0.5, 0.1],
        }
    )

    summary = summarize_context_quality(frame)

    assert summary["dimension_count"] == 6
    vol = summary["dimensions"]["vol"]
    assert vol["valid_rows"] == 4
    assert vol["occupancy"] == {"0": 0.5, "2": 0.5}
    assert vol["transition_count"] == 1
    assert vol["transition_rate"] == 1.0 / 3.0
    assert vol["confidence"]["mean"] == pytest.approx(0.75)
    assert vol["entropy"]["max"] == 0.4

    liq = summary["dimensions"]["liq"]
    assert liq["occupancy"] == {"1": 0.75, "2": 0.25}
    assert liq["transition_count"] == 1

    trend = summary["dimensions"]["trend"]
    assert trend["valid_rows"] == 0
    assert trend["occupancy"] == {}
    assert trend["confidence"]["count"] == 0
