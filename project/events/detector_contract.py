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

    _VALID_EVIDENCE = frozenset(
        {
            "direct",
            "hybrid",
            "statistical",
            "proxy",
            "inferred_cross_asset",
            "contextual",
            "sequence_confirmed",
        }
    )
    _VALID_ROLES = frozenset({"trigger", "context", "composite", "research_only", "filter", "sequence_component"})
    _VALID_MATURITY = frozenset({"production", "specialized", "standard", "deprecated"})

    def __post_init__(self) -> None:
        if not self.event_name.strip():
            raise DetectorContractError("event_name must be non-empty")
        if not self.detector_class.strip():
            raise DetectorContractError(f"{self.event_name}: detector_class must be non-empty")
        if self.evidence_mode not in self._VALID_EVIDENCE:
            raise DetectorContractError(
                f"{self.event_name}: invalid evidence_mode {self.evidence_mode!r}"
            )
        if self.role not in self._VALID_ROLES:
            raise DetectorContractError(f"{self.event_name}: invalid role {self.role!r}")
        if self.maturity not in self._VALID_MATURITY:
            raise DetectorContractError(f"{self.event_name}: invalid maturity {self.maturity!r}")
        if self.context_only and self.primary_anchor_eligible:
            raise DetectorContractError(
                f"{self.event_name}: context_only detectors cannot be primary anchors"
            )
        if self.role in {"context", "filter"} and self.primary_anchor_eligible:
            raise DetectorContractError(
                f"{self.event_name}: context/filter detectors cannot be primary anchors"
            )
        if self.research_only and self.runtime_default:
            raise DetectorContractError(
                f"{self.event_name}: research_only detectors cannot default to runtime execution"
            )
        if self.role in {"composite", "sequence_component"} and self.runtime_default:
            raise DetectorContractError(
                f"{self.event_name}: composite/sequence detectors cannot default to runtime execution"
            )
        if self.runtime_default and not self.emits_quality_flag:
            raise DetectorContractError(
                f"{self.event_name}: runtime detectors must emit a data quality flag"
            )
        if self.runtime_default and not self.supports_confidence:
            raise DetectorContractError(
                f"{self.event_name}: runtime detectors must expose confidence"
            )
        if self.role == "trigger" and not self.allowed_templates:
            raise DetectorContractError(
                f"{self.event_name}: trigger detectors must define allowed_templates"
            )
        if self.merge_gap_bars < 0 or self.cooldown_bars < 0:
            raise DetectorContractError(
                f"{self.event_name}: merge_gap_bars and cooldown_bars must be >= 0"
            )


class DetectorLogicContract(ABC):
    """Protocol that every event detector must satisfy."""

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
        raise NotImplementedError

    @abstractmethod
    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def validate_no_lookahead(self, df: pd.DataFrame, event_frame: pd.DataFrame) -> None:
        raise NotImplementedError
