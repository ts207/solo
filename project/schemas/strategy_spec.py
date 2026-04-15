from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, ConfigDict, Field


class DataRequirements(BaseModel):
    model_config = ConfigDict(frozen=True)
    bars: List[str] = Field(default_factory=lambda: ["1m"])
    book: bool = False
    trades: bool = False
    latency_class: Literal["low", "medium", "high"] = "medium"
    depth_fidelity: Literal["tob", "top_5", "full"] = "tob"


class EntryCondition(BaseModel):
    model_config = ConfigDict(frozen=True)
    feature: str
    operator: Literal["==", "!=", ">", "<", ">=", "<="]
    value: float


class EntrySpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    event_family: str
    conditions: List[EntryCondition]
    direction: Literal["LONG", "SHORT", "FLAT"]


class ExitSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    time_stop_bars: Optional[int] = None
    take_profit_bps: Optional[float] = None
    stop_loss_bps: Optional[float] = None


class RiskSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    max_position_notional_usd: float
    max_concurrent_positions: int = 1


class ExecutionSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    style: Literal["market", "passive", "passive_then_cross", "limit"] = "market"
    post_only_preference: bool = False
    slippage_assumption_bps: float = 1.0
    cost_assumption_bps: float = 1.0


class StrategySpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    strategy_id: str = Field(min_length=1)
    thesis: str
    venue: str
    instrument: str
    data_requirements: DataRequirements
    entry: EntrySpec
    exit: ExitSpec
    risk: RiskSpec
    execution: ExecutionSpec

    def validate_spec(self) -> None:
        """Additional validation logic for execution realism."""
        if self.execution.style == "passive" and self.data_requirements.depth_fidelity == "tob":
            raise ValueError("Passive execution requires depth_fidelity > tob for realism")
        if self.execution.style == "passive" and not self.data_requirements.book:
            raise ValueError("Passive execution requires book=True for realism")
