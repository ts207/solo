import pandas as pd
from typing import Dict, Any


class DataBundle:
    def __init__(self, prices: pd.DataFrame, features: pd.DataFrame, events: pd.DataFrame):
        self._prices = prices
        self._features = features
        self._events = (
            events.set_index("eval_bar_ts") if "eval_bar_ts" in events.columns else events
        )
        self._execution_constraints = pd.Series(1.0, index=prices.index, name="liquidity")

    @property
    def prices(self) -> pd.DataFrame:
        return self._prices

    @property
    def features(self) -> pd.DataFrame:
        return self._features

    @property
    def execution_constraints(self) -> pd.Series:
        return self._execution_constraints

    def get_event_signal(self, event_type: str, kind: str) -> pd.Series:
        # Construct boolean mask matching original index
        mask = pd.Series(False, index=self._prices.index)
        if "event_type" not in self._events.columns:
            return mask

        event_subset = self._events[self._events["event_type"] == event_type]

        # PIT safety alignment: map signal_ts onto evaluations bar timeline.
        for signal_ts in event_subset["signal_ts"]:
            ts = pd.to_datetime(signal_ts)
            mask[mask.index >= ts] = True

        return mask
