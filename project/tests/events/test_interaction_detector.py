"""
Test interaction detector CONFIRM/EXCLUDE semantics.

Verifies that EventInteractionDetector correctly implements:
- CONFIRM: Event B confirms Event A within N bars (intersection)
- EXCLUDE: Event A occurs without Event B within N bars
- AND: Both events occur within N bars (union)
- OR: Either event occurs (union)
"""

import pandas as pd
import pytest

from project.events.interaction_analyzer import (
    InteractionOp,
    detect_interactions,
)


class TestInteractionDetector:
    @pytest.fixture
    def sample_events(self):
        base_time = pd.Timestamp("2026-01-01T12:00:00", tz="UTC")
        return pd.DataFrame({
            "symbol": ["BTCUSDT"] * 6,
            "event_type": [
                "LIQUIDATION_CASCADE",
                "OI_FLUSH",
                "FUNDING_EXTREME_ONSET",
                "LIQUIDATION_CASCADE",
                "VOL_SHOCK",
                "RANGE_COMPRESSION_END",
            ],
            "signal_ts": [
                base_time,
                base_time + pd.Timedelta(minutes=5),
                base_time + pd.Timedelta(minutes=10),
                base_time + pd.Timedelta(minutes=30),
                base_time + pd.Timedelta(hours=1),
                base_time + pd.Timedelta(hours=1, minutes=30),
            ],
        })

    def test_confirm_operation_fires_on_matching_pair(self, sample_events):
        """CONFIRM should fire when right event confirms left within lag."""
        result = detect_interactions(
            df=sample_events,
            left_id="LIQUIDATION_CASCADE",
            right_id="OI_FLUSH",
            op=InteractionOp.CONFIRM,
            lag=pd.Timedelta(minutes=12),
            interaction_name="TEST_CONFIRM",
        )
        assert len(result) >= 1, "CONFIRM should detect LIQ -> OI sequence"

    def test_confirm_operation_ignores_outside_lag(self, sample_events):
        """CONFIRM should NOT fire when right event is outside lag window."""
        base_time = pd.Timestamp("2026-01-01T12:00:00", tz="UTC")
        outside_df = pd.DataFrame({
            "symbol": ["BTCUSDT"] * 2,
            "event_type": ["LIQUIDATION_CASCADE", "OI_FLUSH"],
            "signal_ts": [base_time, base_time + pd.Timedelta(minutes=10)],
        })
        result = detect_interactions(
            df=outside_df,
            left_id="LIQUIDATION_CASCADE",
            right_id="OI_FLUSH",
            op=InteractionOp.CONFIRM,
            lag=pd.Timedelta(minutes=5),
            interaction_name="TEST_CONFIRM",
        )
        assert len(result) == 0, "CONFIRM should not fire when right is outside lag"

    def test_exclude_operation_fires_when_right_missing(self, sample_events):
        """EXCLUDE should fire when left event occurs without right within lag."""
        result = detect_interactions(
            df=sample_events,
            left_id="VOL_SHOCK",
            right_id="RANGE_COMPRESSION_END",
            op=InteractionOp.EXCLUDE,
            lag=pd.Timedelta(minutes=5),
            interaction_name="TEST_EXCLUDE",
        )
        assert len(result) >= 1, "EXCLUDE should fire when right is missing"

    def test_and_operation_requires_both(self, sample_events):
        """AND should only fire when both events within lag."""
        result = detect_interactions(
            df=sample_events,
            left_id="LIQUIDATION_CASCADE",
            right_id="OI_FLUSH",
            op=InteractionOp.AND,
            lag=pd.Timedelta(minutes=12),
            interaction_name="TEST_AND",
        )
        assert len(result) >= 1, "AND should detect both events"

    def test_or_operation_fires_on_either(self, sample_events):
        """OR should fire when either event occurs."""
        result = detect_interactions(
            df=sample_events,
            left_id="LIQUIDATION_CASCADE",
            right_id="OI_FLUSH",
            op=InteractionOp.OR,
            lag=pd.Timedelta(hours=24),
            interaction_name="TEST_OR",
        )
        assert len(result) >= 1, "OR should detect either event"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
