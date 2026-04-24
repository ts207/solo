import numpy as np
import pandas as pd

from project.events.event_specs import EventRegistrySpec
from project.research.baselines import basic_breakout_rule, basic_zscore_reversion


def test_baselines():
    bars = pd.DataFrame(
        {
            "close": [100, 101, 102, 103, 102, 101, 100, 99, 98, 97, 98, 99, 100],
            "high": [100.1] * 13,
            "low": [99.9] * 13,
        }
    )

    # Test Z-score
    pos_z = basic_zscore_reversion(bars, window=5, z_threshold=1.0)
    assert len(pos_z) == 13
    assert pos_z.dtype == np.int64

    # Test Breakout
    pos_b = basic_breakout_rule(bars, window=5)
    assert len(pos_b) == 13


def test_event_registry_spec_metadata():
    spec = EventRegistrySpec(
        event_type="test",
        reports_dir="dir",
        events_file="file",
        signal_column="col",
        is_descriptive=True,
        is_trade_trigger=False,
        requires_confirmation=True,
        allowed_templates=["VOL_REVERSION"],
        disallowed_states=["LOW_LIQUIDITY"],
    )
    assert spec.is_descriptive is True
    assert spec.is_trade_trigger is False
    assert spec.requires_confirmation is True
    assert "VOL_REVERSION" in spec.allowed_templates
    assert "LOW_LIQUIDITY" in spec.disallowed_states


if __name__ == "__main__":
    test_baselines()
    test_event_registry_spec_metadata()
    print("All Phase 8 & 9 component tests passed.")
