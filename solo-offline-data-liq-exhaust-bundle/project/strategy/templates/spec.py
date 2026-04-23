import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional


@dataclass
class StrategySpec:
    event_family: str
    entry_signal: str
    exit_signal: str
    position_cap: float = 1.0
    cooldown_bars: int = 12
    stop_loss_bps: Optional[float] = None
    take_profit_bps: Optional[float] = None
    stop_loss_atr_multipliers: Optional[float] = None
    take_profit_atr_multipliers: Optional[float] = None
    params: Dict[str, float] = field(default_factory=dict)

    @property
    def primary_event_id(self) -> str:
        return str(self.event_family).strip().upper()

    @property
    def compat_event_family(self) -> str:
        return self.primary_event_id

    @property
    def strategy_id(self) -> str:
        return hashlib.sha256(json.dumps(self.normalize(), sort_keys=True).encode()).hexdigest()

    def normalize(self) -> Dict[str, object]:
        return {
            "primary_event_id": self.primary_event_id,
            "compat_event_family": self.compat_event_family,
            "event_family": str(self.event_family).strip().upper(),
            "entry_signal": str(self.entry_signal).strip().lower(),
            "exit_signal": str(self.exit_signal).strip().lower(),
            "position_cap": float(self.position_cap),
            "cooldown_bars": int(self.cooldown_bars),
            "stop_loss_bps": float(self.stop_loss_bps) if self.stop_loss_bps is not None else None,
            "take_profit_bps": float(self.take_profit_bps)
            if self.take_profit_bps is not None
            else None,
            "stop_loss_atr_multipliers": float(self.stop_loss_atr_multipliers)
            if self.stop_loss_atr_multipliers is not None
            else None,
            "take_profit_atr_multipliers": float(self.take_profit_atr_multipliers)
            if self.take_profit_atr_multipliers is not None
            else None,
            "params": {str(k): float(v) for k, v in sorted((self.params or {}).items())},
        }
