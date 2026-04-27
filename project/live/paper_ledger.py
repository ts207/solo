from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
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
    net_bps: float
    mae_bps: float
    mfe_bps: float


class PaperExecutionLedger:
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir
        self.active_positions: dict[str, PaperPosition] = {}  # thesis_id -> PaperPosition
        self.taker_fee_bps = 2.0  # Default fallback
        self.slippage_bps = 1.0  # Default fallback

    def update(
        self,
        thesis_id: str,
        symbol: str,
        action: str,
        side: str,
        price: float,
        timestamp: str,
    ):
        if not thesis_id:
            return

        # 1. Update MAE/MFE for active position
        if thesis_id in self.active_positions:
            pos = self.active_positions[thesis_id]
            pos.highest_price = max(pos.highest_price, price)
            pos.lowest_price = min(pos.lowest_price, price)

        # 2. Handle Entry
        if action in {"probe", "trade_small", "trade_normal"}:
            if thesis_id not in self.active_positions:
                if side in {"buy", "sell"}:
                    self.active_positions[thesis_id] = PaperPosition(
                        thesis_id=thesis_id,
                        symbol=symbol,
                        side="long" if side == "buy" else "short",
                        entry_price=price,
                        entry_ts=timestamp,
                        highest_price=price,
                        lowest_price=price,
                    )
                    _LOG.info(f"PAPER ENTRY: {thesis_id} {side} {symbol} @ {price}")

        # 3. Handle Exit
        elif action == "reject" or (action == "watch" and side == "flat"):
            if thesis_id in self.active_positions:
                pos = self.active_positions.pop(thesis_id)
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
        total_costs = (self.taker_fee_bps * 2) + (self.slippage_bps * 2)
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
            fee_bps=self.taker_fee_bps * 2,
            slippage_bps=self.slippage_bps * 2,
            net_bps=net_bps,
            mae_bps=min(0.0, mae_raw),
            mfe_bps=max(0.0, mfe_raw),
        )

        self._persist_record(record)
        _LOG.info(f"PAPER EXIT: {pos.thesis_id} net_bps={net_bps:.2f}")

    def _persist_record(self, record: PaperTradeRecord):
        path = self.root_dir / record.thesis_id / "trades.jsonl"
        ensure_dir(path.parent)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(record), sort_keys=True) + "\n")
