import json
import pytest
from pathlib import Path
from project.live.paper_ledger import PaperExecutionLedger

def test_paper_ledger_lifecycle(tmp_path):
    ledger = PaperExecutionLedger(tmp_path)
    thesis_id = "test_thesis"
    symbol = "BTCUSDT"
    
    # 1. Entry
    ledger.update(
        thesis_id=thesis_id,
        symbol=symbol,
        action="trade_normal",
        side="buy",
        price=50000.0,
        timestamp="2026-04-27T00:00:00Z"
    )
    
    pos_key = f"{thesis_id}:{symbol}"
    assert pos_key in ledger.active_positions
    pos = ledger.active_positions[pos_key]
    assert pos.side == "long"
    assert pos.entry_price == 50000.0
    
    # 2. Update MAE/MFE
    ledger.update(
        thesis_id=thesis_id,
        symbol=symbol,
        action="watch",
        side="buy",
        price=49000.0, # Adverse
        timestamp="2026-04-27T00:05:00Z"
    )
    assert pos.lowest_price == 49000.0
    
    ledger.update(
        thesis_id=thesis_id,
        symbol=symbol,
        action="watch",
        side="buy",
        price=51000.0, # Favorable
        timestamp="2026-04-27T00:10:00Z"
    )
    assert pos.highest_price == 51000.0
    
    # 3. Exit
    ledger.update(
        thesis_id=thesis_id,
        symbol=symbol,
        action="reject",
        side="flat",
        price=52000.0,
        timestamp="2026-04-27T00:15:00Z"
    )
    
    assert pos_key not in ledger.active_positions
    
    # 4. Verify persisted record
    record_file = tmp_path / thesis_id / "trades.jsonl"
    assert record_file.exists()
    
    lines = record_file.read_text().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    
    assert record["thesis_id"] == thesis_id
    assert record["gross_bps"] == pytest.approx(400.0) # (52000/50000 - 1) * 10000
    
    # Costs: 2x2.0 fee + 2x1.0 slippage = 6.0 bps
    assert record["fee_bps"] == 4.0
    assert record["slippage_bps"] == 2.0
    assert record["net_bps"] == pytest.approx(394.0)
    
    # 5. Verify Summary
    summary_file = tmp_path / thesis_id / "summary.json"
    assert summary_file.exists()
    summary = json.loads(summary_file.read_text())
    assert summary["trade_count"] == 1
    assert summary["cumulative_net_bps"] == pytest.approx(394.0)
    assert summary["hit_rate"] == 1.0

def test_paper_ledger_simultaneous_positions(tmp_path):
    ledger = PaperExecutionLedger(tmp_path)
    thesis_id = "test_thesis"
    
    # Same thesis, different symbols
    ledger.update(thesis_id, "BTCUSDT", "trade_normal", "buy", 50000.0, "ts1")
    ledger.update(thesis_id, "ETHUSDT", "trade_normal", "buy", 3000.0, "ts1")
    
    assert len(ledger.active_positions) == 2
    assert f"{thesis_id}:BTCUSDT" in ledger.active_positions
    assert f"{thesis_id}:ETHUSDT" in ledger.active_positions
