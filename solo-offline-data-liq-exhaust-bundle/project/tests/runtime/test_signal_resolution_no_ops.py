"""
Tests that confirm the explicit no-op semantics of oos_validation_pass
and that the legacy name raises an error so production blueprints cannot
silently rely on a phantom safety surface.
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock


def _make_frame(n: int = 5) -> pd.DataFrame:
    return pd.DataFrame({"spread_abs": [0.5] * n, "funding_bps_abs": [1.0] * n})


def _make_blueprint() -> MagicMock:
    bp = MagicMock()
    bp.id = "test_bp"
    bp.overlays = []
    return bp


def test_oos_validation_pass_raises_unknown_signal():
    """The legacy oos_validation_pass signal must not silently pass.
    Production blueprints referencing it should fail at evaluation time.
    """
    from project.strategy.runtime.dsl_runtime.signal_resolution import signal_mask

    frame = _make_frame()
    bp = _make_blueprint()
    with pytest.raises(ValueError, match="unknown trigger signals"):
        signal_mask(signal="oos_validation_pass", frame=frame, blueprint=bp)


def test_event_detected_without_column_is_all_false():
    """A missing event column must not become an unconditional pass."""
    from project.strategy.runtime.dsl_runtime.signal_resolution import signal_mask

    frame = _make_frame()
    bp = _make_blueprint()
    mask = signal_mask(signal="event_detected", frame=frame, blueprint=bp)
    assert mask.dtype == bool
    assert mask.tolist() == [False] * len(frame)


def test_funding_normalization_pass_requires_canonical_funding_source():
    from project.strategy.runtime.dsl_runtime.signal_resolution import signal_mask

    frame = pd.DataFrame(
        {
            "spread_abs": [0.5],
            "funding_bps_abs": [0.0],
            "funding_rate_scaled_available": [False],
        }
    )
    bp = _make_blueprint()
    with pytest.raises(ValueError, match="requires canonical funding_rate_scaled"):
        signal_mask(signal="funding_normalization_pass", frame=frame, blueprint=bp)
