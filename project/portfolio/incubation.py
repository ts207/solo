import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

class IncubationLedger:
    def __init__(self, ledger_path: Path):
        self.path = ledger_path
        self._data = self._load()

    def _load(self) -> Dict[str, Any]:
        if self.path.exists():
            with open(self.path, "r") as f:
                return json.load(f)
        return {"strategies": {}}

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(self._data, f, indent=2)

    def start_incubation(self, strategy_id: str, blueprint_hash: str):
        if strategy_id not in self._data["strategies"]:
            self._data["strategies"][strategy_id] = {
                "blueprint_hash": blueprint_hash,
                "start_time": datetime.now(timezone.utc).isoformat(),
                "status": "incubating",
                "days_required": 30,
            }
            self.save()

    def get_status(self, strategy_id: str) -> Dict[str, Any]:
        return self._data["strategies"].get(strategy_id, {"status": "not_found"})

    def is_graduated(self, strategy_id: str) -> bool:
        strat = self._data["strategies"].get(strategy_id)
        if not strat:
            return False

        if str(strat.get("status", "")).strip().lower() == "live":
            return True
        if str(strat.get("status", "")).strip().lower() != "incubating":
            return False

        start_time = datetime.fromisoformat(strat["start_time"])
        required_days = strat.get("days_required", 30)
        return (datetime.now(timezone.utc) - start_time) >= timedelta(days=required_days)

    def graduate(self, strategy_id: str):
        if strategy_id in self._data["strategies"]:
            self._data["strategies"][strategy_id]["status"] = "live"
            self._data["strategies"][strategy_id]["graduation_time"] = datetime.now(timezone.utc).isoformat()
            self.save()


from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class IncubationEvidence:
    """Evidence inputs used to determine whether a strategy is ready to graduate.

    Combines the time dimension (already tracked by IncubationLedger) with
    live performance evidence so graduation is not purely calendar-driven.
    """
    strategy_id: str
    days_elapsed: float
    days_required: float
    realized_pnl_usd: float = 0.0
    n_trades: int = 0
    hit_rate: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    drawdown_limit_pct: float = 0.15

    @property
    def time_complete(self) -> bool:
        return self.days_elapsed >= self.days_required

    @property
    def drawdown_breach(self) -> bool:
        if self.max_drawdown_pct is None:
            return False
        return abs(self.max_drawdown_pct) > self.drawdown_limit_pct

    @property
    def has_trade_sample(self) -> bool:
        return self.n_trades >= 5

    def evaluate_graduation(self) -> tuple[bool, str]:
        """Return (should_graduate, reason).

        Graduation requires:
          - time complete
          - no drawdown breach
          - at least a minimal trade sample (5 trades)
        """
        if not self.time_complete:
            remaining = self.days_required - self.days_elapsed
            return False, f"time_incomplete:{remaining:.1f}d_remaining"
        if self.drawdown_breach:
            return False, f"drawdown_breach:{abs(self.max_drawdown_pct or 0):.1%}>{self.drawdown_limit_pct:.1%}"
        if not self.has_trade_sample:
            return False, f"insufficient_trade_sample:{self.n_trades}<5"
        return True, "evidence_and_time_complete"
