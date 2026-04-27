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
    
    assert thesis_id in ledger.active_positions
    pos = ledger.active_positions[thesis_id]
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
    
    assert thesis_id not in ledger.active_positions
    
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
    
    # MAE: (49000/50000 - 1) * 10000 = -200 bps
    assert record["mae_bps"] == pytest.approx(-200.0)
    # MFE: (51000/50000 - 1) * 10000 = 200 bps (Wait, highest was 51000 BEFORE exit at 52000?)
    # Actually, highest_price is updated before exit check?
    # No, in my implementation:
    # if thesis_id in active: update high/low
    # elif action == reject: record_exit
    # Let's check the code.
    
    # Ah, at 52000, update is called with action="reject".
    # Since thesis_id IS in active, it updates highest_price to 52000.
    # THEN it hits the 'elif action == "reject"' block? NO, it's 'elif', so it only hits ONE.
    # So 52000 is NOT used for high/low update if it triggers exit.
    # Actually, 52000 IS the exit price, so MFE would be at least up to 51000 in this test.
    # Let's re-verify my 'update' logic.
