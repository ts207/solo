# Sprint 4 — Paper Execution Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record would-have-traded entries/exits in simulation mode with realistic cost attribution (fees, spread, slippage, funding).

**Architecture:**
- **PaperExecutionLedger:** A new component `project/live/paper_ledger.py` that tracks active paper positions per thesis and records closed trades to a JSONL file.
- **Cost Model:** Basic realistic cost attribution in the ledger (taker fees, constant slippage/spread proxy).
- **Runner Integration:** `LiveEngineRunner` initializes and calls `PaperExecutionLedger` during its decision cycle when in `simulation` or `monitor_only` mode.
- **Persistence:** Closed paper trades are saved to `data/reports/paper/<thesis_id>/trades.jsonl`.

**Tech Stack:** Python, Pydantic, JSONL.

---

### Task 1: Implement `PaperExecutionLedger`

**Files:**
- Create: `project/live/paper_ledger.py`

- [ ] **Step 1: Define `PaperTrade` and `PaperExecutionLedger`**

```python
from __future__ import annotations
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Literal
from project.io.utils import ensure_dir

_LOG = logging.getLogger(__name__)

@dataclass
class PaperPosition:
    thesis_id: str
    symbol: str
    side: Literal["long", "short"]
    entry_price: float
    entry_ts: str
    quantity: float = 1.0 # Unit quantity for bps calculation
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
        self.active_positions: dict[str, PaperPosition] = {} # thesis_id -> PaperPosition
        self.taker_fee_bps = 2.0 # Default fallback
        self.slippage_bps = 1.0  # Default fallback

    def update(self, thesis_id: str, symbol: str, action: str, side: str, price: float, timestamp: str):
        # 1. Update MAE/MFE for active position
        if thesis_id in self.active_positions:
            pos = self.active_positions[thesis_id]
            pos.highest_price = max(pos.highest_price, price)
            pos.lowest_price = min(pos.lowest_price, price)

        # 2. Handle Entry
        if action in {"probe", "trade_small", "trade_normal"} and thesis_id not in self.active_positions:
            if side in {"buy", "sell"}:
                self.active_positions[thesis_id] = PaperPosition(
                    thesis_id=thesis_id,
                    symbol=symbol,
                    side="long" if side == "buy" else "short",
                    entry_price=price,
                    entry_ts=timestamp,
                    highest_price=price,
                    lowest_price=price
                )
                _LOG.info(f"PAPER ENTRY: {thesis_id} {side} {symbol} @ {price}")

        # 3. Handle Exit
        elif action == "reject" or (action == "watch" and side == "flat"):
            if thesis_id in self.active_positions:
                pos = self.active_positions.pop(thesis_id)
                self._record_exit(pos, price, timestamp)

    def _record_exit(self, pos: PaperPosition, exit_price: float, exit_ts: str):
        # Simple bps return: (exit/entry - 1) * 10000
        raw_return = (exit_price / pos.entry_price - 1.0) * 10000.0
        gross_bps = raw_return if pos.side == "long" else -raw_return
        
        # MAE/MFE
        mae_raw = (pos.lowest_price / pos.entry_price - 1.0) * 10000.0 if pos.side == "long" else (pos.highest_price / pos.entry_price - 1.0) * -10000.0
        mfe_raw = (pos.highest_price / pos.entry_price - 1.0) * 10000.0 if pos.side == "long" else (pos.lowest_price / pos.entry_price - 1.0) * -10000.0
        
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
            mfe_bps=max(0.0, mfe_raw)
        )
        
        self._persist_record(record)
        _LOG.info(f"PAPER EXIT: {pos.thesis_id} net_bps={net_bps:.2f}")

    def _persist_record(self, record: PaperTradeRecord):
        path = self.root_dir / record.thesis_id / "trades.jsonl"
        ensure_dir(path.parent)
        with open(path, "a") as f:
            f.write(json.dumps(asdict(record)) + "\n")
```

---

### Task 2: Integrate into `LiveEngineRunner`

**Files:**
- Modify: `project/live/runner.py`

- [ ] **Step 1: Initialize `PaperExecutionLedger` in `__init__`**

```python
        # Sprint 4: Paper Ledger
        self.paper_ledger = PaperExecutionLedger(data_root / "reports" / "paper")
```

- [ ] **Step 2: Update ledger in `_process_kline_for_thesis_runtime`**

```python
        # ... after candidates are processed ...
        for candidate in candidate_outcomes:
            thesis_id = str(candidate.trade_intent.thesis_id)
            if self.runtime_mode != "trading":
                self.paper_ledger.update(
                    thesis_id=thesis_id,
                    symbol=candidate.trade_intent.symbol,
                    action=candidate.trade_intent.action,
                    side=candidate.trade_intent.side,
                    price=close,
                    timestamp=str(timestamp)
                )
```

---

### Task 3: Verification

- [ ] **Step 1: Write a test for `PaperExecutionLedger`**
- [ ] **Step 2: Run a dry-run simulation to see if `trades.jsonl` is created**
