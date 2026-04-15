from __future__ import annotations

from typing import Any, Dict, List, Set

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator


class LiveTradeContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    timeframe: str = Field(min_length=1)
    primary_event_id: str = Field(min_length=1)
    # Legacy compatibility metadata only. Runtime matching should prefer
    # primary_event_id, active_event_ids, and contradiction_event_ids.
    event_family: str = ""
    canonical_regime: str = ""
    event_side: str = Field(min_length=1)
    live_features: Dict[str, Any] = Field(default_factory=dict)
    regime_snapshot: Dict[str, Any] = Field(default_factory=dict)
    execution_env: Dict[str, Any] = Field(default_factory=dict)
    portfolio_state: Dict[str, Any] = Field(default_factory=dict)
    active_event_families: List[str] = Field(default_factory=list)
    active_event_ids: List[str] = Field(default_factory=list)
    active_episode_ids: List[str] = Field(default_factory=list)
    active_groups: Set[str] = Field(default_factory=set)
    family_budgets: Dict[str, float] = Field(default_factory=dict)
    contradiction_event_families: List[str] = Field(default_factory=list)
    contradiction_event_ids: List[str] = Field(default_factory=list)
    episode_snapshot: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _populate_compat_event_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        primary_event_id = str(data.get("primary_event_id", "")).strip()
        event_family = str(data.get("event_family", "")).strip()
        if not primary_event_id and event_family:
            primary_event_id = event_family
        if primary_event_id:
            data["primary_event_id"] = primary_event_id
        if event_family:
            data["event_family"] = event_family
        return data

    @field_validator("timestamp", "symbol", "timeframe", "primary_event_id", "event_side")
    @classmethod
    def _validate_non_empty(cls, value: str) -> str:
        token = str(value).strip()
        if not token:
            raise ValueError("field must be non-empty")
        return token

    @field_validator("event_family", "primary_event_id", "canonical_regime")
    @classmethod
    def _normalize_optional_tokens(cls, value: str) -> str:
        return str(value).strip().upper()

    @computed_field(return_type=dict)
    @property
    def context_clause(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "primary_event_id": self.primary_event_id,
            "canonical_regime": self.canonical_regime,
            "event_side": self.event_side,
            "active_event_ids": list(self.active_event_ids),
            "active_episode_ids": list(self.active_episode_ids),
            "active_groups": list(self.active_groups),
        }
