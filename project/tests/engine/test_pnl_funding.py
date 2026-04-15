import pandas as pd
import numpy as np
from project.engine.pnl import compute_pnl_ledger

def test_funding_overcounting_default():
    # 5-minute bars for 1 day = 288 bars
    idx = pd.date_range("2025-01-01", periods=288, freq="5min", tz="UTC")
    pos = pd.Series(1.0, index=idx)
    close = pd.Series(100.0, index=idx)
    # Funding rate of 0.01% (0.0001) every 8 hours
    # In event-aligned mode, it should only be applied 3 times (00:00, 08:00, 16:00)
    funding_rate = pd.Series(0.0001, index=idx)
    
    # Current behavior: False by default, overcounts
    # 0.0001 * 288 = 0.0288
    
    # We want to change the default to True or normalize.
    # The plan says to change default to True.
    
    ledger_default = compute_pnl_ledger(pos, close, funding_rate=funding_rate)
    # If it's event-aligned, sum should be 0.0003 (3 events * 0.0001)
    # Actually compute_funding_pnl_event_aligned uses prior_pos.
    # At 00:00, prior_pos is from 23:55 (which is 1.0).
    # There are 3 such events in 288 bars if we start at 00:00.
    # 00:00, 08:00, 16:00.
    
    funding_sum = ledger_default["funding_pnl"].sum()
    print(f"Funding PnL Sum: {funding_sum}")
    assert abs(funding_sum) < 0.001
