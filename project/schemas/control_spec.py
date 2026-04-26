from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class EventDetectionLogic(BaseModel):
    model_config = ConfigDict(frozen=True)
    type: str = Field(
        description="Type of detection logic, e.g., 'ewma_zscore', 'static_threshold'"
    )
    target_feature: str
    lookback: int
    threshold: float
    dynamic_regime_adjustment: bool = False
    extra_params: dict[str, Any] = Field(default_factory=dict)


class EventDefinitionSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    event_type: str
    canonical_family: str
    detection_logic: EventDetectionLogic


class CustomFilterSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    feature: str
    operator: Literal["==", "!=", ">", "<", ">=", "<="]
    value: float


class MarketStateSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    required_regimes: list[str] = Field(default_factory=list)
    disallowed_states: list[str] = Field(default_factory=list)
    custom_filters: list[CustomFilterSpec] = Field(default_factory=list)


class TemplateConfigSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    base: str
    overlays: list[str] = Field(default_factory=list)


class RiskParamsSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    stop_loss_bps: list[float] | None = None
    take_profit_bps: list[float] | None = None
    stop_loss_atr_multipliers: list[float] | None = None
    take_profit_atr_multipliers: list[float] | None = None


class ExecutionParamsSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    style: str = "market"
    post_only_preference: bool = False


class ParameterGridSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    horizons_bars: list[int]
    risk: RiskParamsSpec
    execution: ExecutionParamsSpec = Field(default_factory=ExecutionParamsSpec)
    extra_grid: dict[str, list[Any]] = Field(default_factory=dict)


class ControlSpec(BaseModel):
    """
    Unified Control Interface for Strategy Definitions.
    """

    model_config = ConfigDict(frozen=True)
    concept_id: str
    description: str = ""
    event_definition: EventDefinitionSpec
    market_state: MarketStateSpec = Field(default_factory=MarketStateSpec)
    templates: TemplateConfigSpec
    parameters: ParameterGridSpec
