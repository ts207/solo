from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar, List

import pandas as pd


class DetectorContractError(Exception):
    pass


@dataclass(frozen=True)
class DetectorContract:
    event_name: str
    event_version: str
    detector_class: str

    canonical_family: str
    subtype: str
    phase: str

    evidence_mode: str
    role: str
    maturity: str

    planning_default: bool
    runtime_default: bool
    promotion_eligible: bool
    primary_anchor_eligible: bool

    research_only: bool
    context_only: bool
    composite: bool

    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...]
    source_dependencies: tuple[str, ...]

    allowed_templates: tuple[str, ...]
    allowed_horizons: tuple[str, ...]

    calibration_mode: str
    threshold_schema_version: str
    merge_gap_bars: int
    cooldown_bars: int

    supports_confidence: bool
    supports_severity: bool
    emits_quality_flag: bool

    aliases: tuple[str, ...] = ()
    notes: str = ""


class DetectorLogicContract(ABC):
    """
    Protocol that every event detector must satisfy.
    """

    required_columns: ClassVar[List[str]] = []
    lookback_bars: ClassVar[int] = 0
    warmup_bars: ClassVar[int] = 0
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
        pass

    @abstractmethod
    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        pass

    @abstractmethod
    def validate_no_lookahead(self, df: pd.DataFrame, event_frame: pd.DataFrame) -> None:
        pass
