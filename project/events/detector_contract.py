from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar

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
    detector_band: str

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
    cooldown_semantics: str
    merge_key_strategy: str

    supports_confidence: bool
    supports_severity: bool
    supports_quality_flag: bool

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
    _VALID_MATURITY = frozenset(
        {"production", "specialized", "standard", "experimental", "deprecated"}
    )
    _VALID_BANDS = frozenset({"deployable_core", "research_trigger", "context_only", "composite_or_fragile"})

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
        if self.detector_band not in self._VALID_BANDS:
            raise DetectorContractError(f"{self.event_name}: invalid detector_band {self.detector_band!r}")
        if self.detector_band == "deployable_core" and not self.runtime_default:
            raise DetectorContractError(
                f"{self.event_name}: deployable_core detectors must default to runtime execution"
            )
        if self.detector_band == "context_only" and not self.context_only and self.role != "context":
            raise DetectorContractError(
                f"{self.event_name}: context_only band must use a context role"
            )
        if self.detector_band == "context_only" and self.primary_anchor_eligible:
            raise DetectorContractError(
                f"{self.event_name}: context_only detectors cannot be primary anchors"
            )
        if self.detector_band == "composite_or_fragile" and self.runtime_default:
            raise DetectorContractError(
                f"{self.event_name}: composite_or_fragile detectors cannot default to runtime execution"
            )
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
        if self.runtime_default and not self.supports_quality_flag:
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
        if not self.cooldown_semantics.strip():
            raise DetectorContractError(
                f"{self.event_name}: cooldown_semantics must be non-empty"
            )
        if not self.merge_key_strategy.strip():
            raise DetectorContractError(
                f"{self.event_name}: merge_key_strategy must be non-empty"
            )

    @property
    def emits_quality_flag(self) -> bool:
        return self.supports_quality_flag


@dataclass(frozen=True)
class NormalizedDetectorMetadata:
    event_name: str
    event_version: str
    detector_class: str
    required_columns: tuple[str, ...]
    supports_confidence: bool
    supports_severity: bool
    supports_quality_flag: bool
    cooldown_semantics: str
    merge_key_strategy: str
    role: str = "trigger"
    evidence_mode: str = "direct"
    maturity: str = "standard"
    detector_band: str = "research_trigger"
    planning_default: bool = False
    promotion_eligible: bool = False
    runtime_default: bool = False
    primary_anchor_eligible: bool = False

    _VALID_EVIDENCE = DetectorContract._VALID_EVIDENCE
    _VALID_ROLES = DetectorContract._VALID_ROLES
    _VALID_MATURITY = DetectorContract._VALID_MATURITY
    _VALID_BANDS = DetectorContract._VALID_BANDS

    def __post_init__(self) -> None:
        if not self.event_name.strip():
            raise DetectorContractError("normalized detector metadata requires event_name")
        if not self.detector_class.strip():
            raise DetectorContractError(
                f"{self.event_name}: normalized detector metadata requires detector_class"
            )
        if self.evidence_mode not in self._VALID_EVIDENCE:
            raise DetectorContractError(
                f"{self.event_name}: invalid normalized evidence_mode {self.evidence_mode!r}"
            )
        if self.role not in self._VALID_ROLES:
            raise DetectorContractError(
                f"{self.event_name}: invalid normalized role {self.role!r}"
            )
        if self.maturity not in self._VALID_MATURITY:
            raise DetectorContractError(
                f"{self.event_name}: invalid normalized maturity {self.maturity!r}"
            )
        if self.detector_band not in self._VALID_BANDS:
            raise DetectorContractError(
                f"{self.event_name}: invalid normalized detector_band {self.detector_band!r}"
            )
        if not self.required_columns:
            raise DetectorContractError(
                f"{self.event_name}: normalized detector metadata requires required_columns"
            )
        if not self.cooldown_semantics.strip():
            raise DetectorContractError(
                f"{self.event_name}: normalized detector metadata requires cooldown_semantics"
            )
        if not self.merge_key_strategy.strip():
            raise DetectorContractError(
                f"{self.event_name}: normalized detector metadata requires merge_key_strategy"
            )


def detector_metadata_from_class(
    detector_cls: type[Any], *, event_name: str | None = None
) -> NormalizedDetectorMetadata:
    token = str(
        event_name
        or getattr(detector_cls, "event_name", "")
        or getattr(detector_cls, "event_type", "")
        or detector_cls.__name__
    ).strip().upper()
    required_columns = tuple(
        str(column).strip()
        for column in getattr(detector_cls, "required_columns", ()) or ()
        if str(column).strip()
    ) or ("timestamp",)
    return NormalizedDetectorMetadata(
        event_name=token,
        event_version=str(getattr(detector_cls, "event_version", "v1")).strip().lower() or "v1",
        detector_class=str(detector_cls.__name__).strip(),
        required_columns=required_columns,
        supports_confidence=bool(getattr(detector_cls, "supports_confidence", False)),
        supports_severity=bool(getattr(detector_cls, "supports_severity", False)),
        supports_quality_flag=bool(getattr(detector_cls, "supports_quality_flag", False)),
        cooldown_semantics=str(getattr(detector_cls, "cooldown_semantics", "none")).strip()
        or "none",
        merge_key_strategy=str(getattr(detector_cls, "merge_key_strategy", "none")).strip()
        or "none",
        role=str(getattr(detector_cls, "role", "trigger")).strip().lower() or "trigger",
        evidence_mode=str(getattr(detector_cls, "evidence_mode", "direct")).strip().lower()
        or "direct",
        maturity=str(getattr(detector_cls, "maturity", "standard")).strip().lower()
        or "standard",
        detector_band=str(getattr(detector_cls, "detector_band", "research_trigger"))
        .strip()
        .lower()
        or "research_trigger",
        planning_default=bool(getattr(detector_cls, "planning_default", False)),
        promotion_eligible=bool(getattr(detector_cls, "promotion_eligible", False)),
        runtime_default=bool(getattr(detector_cls, "runtime_default", False)),
        primary_anchor_eligible=bool(
            getattr(detector_cls, "primary_anchor_eligible", False)
        ),
    )


class DetectorLogicContract(ABC):
    """Protocol that every event detector must satisfy."""

    event_name: ClassVar[str] = ""
    event_type: ClassVar[str] = ""
    event_version: ClassVar[str] = "v1"
    required_columns: ClassVar[list[str]] = []
    lookback_bars: ClassVar[int] = 0
    warmup_bars: ClassVar[int] = 0
    bar_type: ClassVar[str] = "bar_close"
    supports_confidence: ClassVar[bool] = False
    supports_severity: ClassVar[bool] = False
    supports_quality_flag: ClassVar[bool] = False
    cooldown_semantics: ClassVar[str] = "none"
    merge_key_strategy: ClassVar[str] = "none"
    role: ClassVar[str] = "trigger"
    evidence_mode: ClassVar[str] = "direct"
    maturity: ClassVar[str] = "standard"
    detector_band: ClassVar[str] = "research_trigger"
    planning_default: ClassVar[bool] = False
    promotion_eligible: ClassVar[bool] = False
    runtime_default: ClassVar[bool] = False
    primary_anchor_eligible: ClassVar[bool] = False

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

    @classmethod
    def detector_metadata(cls, *, event_name: str | None = None) -> NormalizedDetectorMetadata:
        return detector_metadata_from_class(cls, event_name=event_name)

    @abstractmethod
    def compute_signal(self, df: pd.DataFrame) -> pd.Series:
        raise NotImplementedError

    @abstractmethod
    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def validate_no_lookahead(self, df: pd.DataFrame, event_frame: pd.DataFrame) -> None:
        raise NotImplementedError
