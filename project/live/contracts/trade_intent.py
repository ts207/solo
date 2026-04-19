from __future__ import annotations

from typing import Any, Dict, List, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TradeIntent(BaseModel):
    model_config = ConfigDict(frozen=True)

    action: Literal["reject", "watch", "probe", "trade_small", "trade_normal"]
    symbol: str = Field(min_length=1)
    side: Literal["buy", "sell", "flat"]
    thesis_id: str = ""
    support_score: float = 0.0
    contradiction_penalty: float = 0.0
    probability_positive_post_cost: float = 0.0
    expected_gross_edge_bps: float = 0.0
    expected_net_edge_bps: float = 0.0
    expected_downside_bps: float = 0.0
    expected_net_pnl_bps: float = 0.0
    fill_probability: float = 0.0
    edge_confidence: float = 0.0
    utility_score: float = 0.0
    confidence_band: Literal["none", "low", "medium", "high"] = "none"
    size_fraction: float = 0.0
    invalidation: Dict[str, Any] = Field(default_factory=dict)
    reasons_for: List[str] = Field(default_factory=list)
    reasons_against: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def _validate_symbol(cls, value: str) -> str:
        token = str(value).strip().upper()
        if not token:
            raise ValueError("symbol must be non-empty")
        return token
