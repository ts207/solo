from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

from project.io.utils import ensure_dir

_LOG = logging.getLogger(__name__)


@dataclass
class PaperPosition:
    thesis_id: str
    symbol: str
    side: Literal["long", "short"]
    entry_price: float
    entry_ts: str
    quantity: float = 1.0  # Unit quantity for bps calculation
    highest_price: float = 0.0
    lowest_price: float = 0.0


@dataclass
class PaperTradeRecord:
    thesis_id: str
    symbol: str
    side: Literal["long", "short"]
    entry_ts: str
    entry_price: float
    exit_ts: str
    exit_price: float
    gross_bps: float
    fee_bps: float
    slippage_bps: float
    spread_bps: float = 0.0
    funding_bps: float = 0.0
    net_bps: float = 0.0
    mae_bps: float = 0.0
    mfe_bps: float = 0.0
    cost_model_version: str = "v1_basic"


class PaperExecutionLedger:
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir
        self.active_positions: dict[str, PaperPosition] = {}  # "thesis_id:symbol" -> PaperPosition
        self.taker_fee_bps = 2.0  # Default fallback
        self.slippage_bps = 1.0  # Default fallback
        self.cost_model_version = "v1_basic"

    def update(
        self,
        thesis_id: str,
        symbol: str,
        action: str,
        side: str,
        price: float,
        timestamp: str,
    ):
        if not thesis_id or not symbol:
            return

        pos_key = f"{thesis_id}:{symbol}"

        # 1. Update MAE/MFE for active position
        if pos_key in self.active_positions:
            pos = self.active_positions[pos_key]
            pos.highest_price = max(pos.highest_price, price)
            pos.lowest_price = min(pos.lowest_price, price)

        # 2. Handle Entry
        if action in {"probe", "trade_small", "trade_normal"}:
            if pos_key not in self.active_positions:
                if side in {"buy", "sell"}:
                    self.active_positions[pos_key] = PaperPosition(
                        thesis_id=thesis_id,
                        symbol=symbol,
                        side="long" if side == "buy" else "short",
                        entry_price=price,
                        entry_ts=timestamp,
                        highest_price=price,
                        lowest_price=price,
                    )
                    _LOG.info(f"PAPER ENTRY: {pos_key} {side} @ {price}")

        # 3. Handle Exit
        elif action == "reject" or (action == "watch" and side == "flat"):
            if pos_key in self.active_positions:
                pos = self.active_positions.pop(pos_key)
                self._record_exit(pos, price, timestamp)

    def _record_exit(self, pos: PaperPosition, exit_price: float, exit_ts: str):
        if pos.entry_price <= 0:
            return

        # Simple bps return: (exit/entry - 1) * 10000
        raw_return = (exit_price / pos.entry_price - 1.0) * 10000.0
        gross_bps = raw_return if pos.side == "long" else -raw_return

        # MAE/MFE
        mae_raw = (
            (pos.lowest_price / pos.entry_price - 1.0) * 10000.0
            if pos.side == "long"
            else (pos.highest_price / pos.entry_price - 1.0) * -10000.0
        )
        mfe_raw = (
            (pos.highest_price / pos.entry_price - 1.0) * 10000.0
            if pos.side == "long"
            else (pos.lowest_price / pos.entry_price - 1.0) * -10000.0
        )

        # Net BPS (2x fees for entry+exit, 2x slippage)
        # TODO: Add real funding and spread attribution in next phase
        fee_total = self.taker_fee_bps * 2
        slippage_total = self.slippage_bps * 2
        funding_total = 0.0 # Placeholder
        spread_total = 0.0  # Placeholder
        
        total_costs = fee_total + slippage_total + funding_total + spread_total
        net_bps = gross_bps - total_costs

        record = PaperTradeRecord(
            thesis_id=pos.thesis_id,
            symbol=pos.symbol,
            side=pos.side,
            entry_ts=pos.entry_ts,
            entry_price=pos.entry_price,
            exit_ts=exit_ts,
            exit_price=exit_price,
            gross_bps=gross_bps,
            fee_bps=fee_total,
            slippage_bps=slippage_total,
            funding_bps=funding_total,
            spread_bps=spread_total,
            net_bps=net_bps,
            mae_bps=min(0.0, mae_raw),
            mfe_bps=max(0.0, mfe_raw),
            cost_model_version=self.cost_model_version
        )

        self._persist_record(record)
        self._update_summary(pos.thesis_id)
        _LOG.info(f"PAPER EXIT: {pos.thesis_id} net_bps={net_bps:.2f}")

    def _persist_record(self, record: PaperTradeRecord):
        path = self.root_dir / record.thesis_id / "trades.jsonl"
        ensure_dir(path.parent)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(record), sort_keys=True) + "\n")

    def _update_summary(self, thesis_id: str):
        trades_path = self.root_dir / thesis_id / "trades.jsonl"
        if not trades_path.exists():
            return
            
        trades = []
        with open(trades_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    trades.append(json.loads(line))
        
        if not trades:
            return
            
        net_bps = [t["net_bps"] for t in trades]
        mae_bps = [t["mae_bps"] for t in trades]
        mfe_bps = [t["mfe_bps"] for t in trades]
        
        summary = {
            "thesis_id": thesis_id,
            "trade_count": len(trades),
            "hit_rate": sum(1 for b in net_bps if b > 0) / len(trades),
            "mean_net_bps": sum(net_bps) / len(trades),
            "cumulative_net_bps": sum(net_bps),
            "mean_mae_bps": sum(mae_bps) / len(trades),
            "mean_mfe_bps": sum(mfe_bps) / len(trades),
            "fee_bps_total": sum(t["fee_bps"] for t in trades),
            "slippage_bps_total": sum(t["slippage_bps"] for t in trades),
            "funding_bps_total": sum(t["funding_bps"] for t in trades),
            "last_updated_at": datetime.now().isoformat()
        }
        
        summary_path = self.root_dir / thesis_id / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
