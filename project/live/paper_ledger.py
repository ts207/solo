from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Any

from project.io.utils import ensure_dir, atomic_write_json
from project.live.execution_costs import estimate_execution_cost_bps

_LOG = logging.getLogger(__name__)


@dataclass
class PaperPosition:
    thesis_id: str
    symbol: str
    side: Literal["long", "short"]
    entry_price: float
    entry_ts: str
    entry_idx: int = 0
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
    cost_model_version: str = "paper_cost_v1"
    degraded_cost: bool = False
    degraded_reason: str | None = None


class PaperExecutionLedger:
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir
        self.active_positions: dict[str, PaperPosition] = {}  # "thesis_id:symbol" -> PaperPosition
        self.taker_fee_bps = 2.0  # Default fallback
        self.slippage_bps = 1.0  # Default fallback
        self._bar_counter = 0

    def update(
        self,
        thesis_id: str,
        symbol: str,
        action: str,
        side: str,
        price: float,
        timestamp: str,
        best_bid: float | None = None,
        best_ask: float | None = None,
        funding_rate: float | None = None,
    ):
        if not thesis_id or not symbol:
            return

        self._bar_counter += 1
        pos_key = f"{thesis_id}:{symbol}"

        # 1. Update MAE/MFE for active position
        is_exit = action == "reject" or (action == "watch" and side == "flat")
        if pos_key in self.active_positions and not is_exit:
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
                        entry_idx=self._bar_counter,
                        highest_price=price,
                        lowest_price=price,
                    )
                    _LOG.info(f"PAPER ENTRY: {pos_key} {side} @ {price}")

        # 3. Handle Exit
        elif action == "reject" or (action == "watch" and side == "flat"):
            if pos_key in self.active_positions:
                pos = self.active_positions.pop(pos_key)
                self._record_exit(pos, price, timestamp, best_bid, best_ask, funding_rate)

    def _record_exit(
        self, 
        pos: PaperPosition, 
        exit_price: float, 
        exit_ts: str,
        best_bid: float | None = None,
        best_ask: float | None = None,
        funding_rate: float | None = None,
    ):
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

        # Real Paper Costs
        horizon_bars = max(1, self._bar_counter - pos.entry_idx)
        costs = estimate_execution_cost_bps(
            side=pos.side,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            best_bid=best_bid,
            best_ask=best_ask,
            funding_rate=funding_rate,
            horizon_bars=horizon_bars,
            fee_bps_per_side=self.taker_fee_bps,
            fallback_slippage_bps=self.slippage_bps,
        )
        
        net_bps = gross_bps - costs.total_bps

        record = PaperTradeRecord(
            thesis_id=pos.thesis_id,
            symbol=pos.symbol,
            side=pos.side,
            entry_ts=pos.entry_ts,
            entry_price=pos.entry_price,
            exit_ts=exit_ts,
            exit_price=exit_price,
            gross_bps=gross_bps,
            fee_bps=costs.fee_bps,
            slippage_bps=costs.slippage_bps,
            funding_bps=costs.funding_bps,
            spread_bps=costs.spread_bps,
            net_bps=net_bps,
            mae_bps=min(0.0, mae_raw),
            mfe_bps=max(0.0, mfe_raw),
            cost_model_version=costs.cost_model_version,
            degraded_cost=costs.degraded,
            degraded_reason=costs.degraded_reason,
        )

        self._persist_record(record)
        self._write_quality_summary(pos.thesis_id)
        _LOG.info(f"PAPER EXIT: {pos.thesis_id} net_bps={net_bps:.2f}")

    def _persist_record(self, record: PaperTradeRecord):
        path = self.root_dir / record.thesis_id / "trades.jsonl"
        ensure_dir(path.parent)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(record), sort_keys=True) + "\n")

    def _write_quality_summary(self, thesis_id: str):
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
        degraded = [t.get("degraded_cost", False) for t in trades]
        
        trade_count = len(trades)
        mean_net_bps = sum(net_bps) / trade_count
        cumulative_net_bps = sum(net_bps)
        hit_rate = sum(1 for b in net_bps if b > 0) / trade_count
        degraded_cost_fraction = sum(1 for d in degraded if d) / trade_count
        
        # Calculate max drawdown in BPS from cumulative net BPS
        cum_bps = []
        curr = 0.0
        for b in net_bps:
            curr += b
            cum_bps.append(curr)
        
        running_max = 0.0
        max_drawdown = 0.0
        for val in cum_bps:
            running_max = max(running_max, val)
            drawdown = running_max - val
            max_drawdown = max(max_drawdown, drawdown)

        # gate-ready logic
        # conservative defaults: drawdown > 500 bps is bad
        gate_ready = (
            trade_count >= 30 and
            mean_net_bps > 0 and
            cumulative_net_bps > 0 and
            hit_rate > 0.50 and
            degraded_cost_fraction <= 0.20 and
            max_drawdown < 500.0
        )

        summary = {
            "thesis_id": thesis_id,
            "trade_count": trade_count,
            "mean_net_bps": round(mean_net_bps, 4),
            "cumulative_net_bps": round(cumulative_net_bps, 4),
            "hit_rate": round(hit_rate, 4),
            "max_drawdown_bps": round(max_drawdown, 4),
            "mean_mae_bps": round(sum(mae_bps) / trade_count, 4),
            "mean_mfe_bps": round(sum(mfe_bps) / trade_count, 4),
            "fee_bps_total": round(sum(t["fee_bps"] for t in trades), 4),
            "spread_bps_total": round(sum(t["spread_bps"] for t in trades), 4),
            "slippage_bps_total": round(sum(t["slippage_bps"] for t in trades), 4),
            "funding_bps_total": round(sum(t["funding_bps"] for t in trades), 4),
            "degraded_cost_fraction": round(degraded_cost_fraction, 4),
            "paper_gate_ready": gate_ready,
            "last_updated_at": datetime.now(UTC).isoformat()
        }
        
        # Legacy summary.json in thesis dir
        legacy_path = self.root_dir / thesis_id / "summary.json"
        atomic_write_json(legacy_path, summary)
        
        # New quality summary in canonical location
        quality_path = self.root_dir / thesis_id / "paper_quality_summary.json"
        atomic_write_json(quality_path, summary)
