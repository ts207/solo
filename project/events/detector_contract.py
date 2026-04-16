from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar, List

import pandas as pd


class DetectorContractError(Exception):
    pass


class DetectorContract(ABC):
    """
    Protocol that every event detector must satisfy.

    Subclass this and implement the three abstract methods.
    Class attributes define the contract metadata used for YAML validation
    and pipeline warm-up calculations.
    """

    # Columns that must be present in the input DataFrame
    required_columns: ClassVar[List[str]] = []

    # Bars of history consumed to produce a valid signal (lookback window)
    lookback_bars: ClassVar[int] = 0

    # Bars to skip at the start before trusting signal output (warm-up)
    warmup_bars: ClassVar[int] = 0

    # Timestamp alignment: "bar_close", "close_to_close", or "intrabar"
    bar_type: ClassVar[str] = "bar_close"

    _VALID_BAR_TYPES = frozenset({"bar_close", "close_to_close", "intrabar"})

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if cls.bar_type not in cls._VALID_BAR_TYPES:
            raise DetectorContractError(
                f"{cls.__name__}.bar_type={cls.bar_type!r} is not one of {sorted(cls._VALID_BAR_TYPES)}"
            )

    def check_required_columns(self, df: pd.DataFrame) -> None:
        missing = [c for c in self.required_columns if c not in df.columns]
        if missing:
            raise DetectorContractError(
                f"{self.__class__.__name__} missing required columns: {missing}"
            )

    @abstractmethod
    def compute_signal(self, df: pd.DataFrame) -> pd.Series:
        """
        Return a continuous signal Series (same length as df, index-aligned).
        Values should be non-negative floats; 0 means no signal.
        """

    @abstractmethod
    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """
        Return a DataFrame of detected events (may be empty).
        Must not use any future data beyond each row's bar close.
        """

    @abstractmethod
    def validate_no_lookahead(self, df: pd.DataFrame, event_frame: pd.DataFrame) -> None:
        """
        Raise DetectorContractError if event timestamps indicate lookahead.
        Implementations should verify that every event's detected_ts <=
        its source bar's timestamp.
        """
