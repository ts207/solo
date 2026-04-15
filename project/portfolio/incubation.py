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
