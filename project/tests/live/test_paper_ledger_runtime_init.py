import pytest
from pathlib import Path
from unittest.mock import MagicMock
from project.live.runner import LiveEngineRunner

class FakeDataManager:
    def __init__(self):
        self.kline_queue = MagicMock()
        self.ticker_queue = MagicMock()

def test_live_runner_initializes_paper_ledger_without_crash(tmp_path):
    # This test verifies that data_root NameError is fixed and configurable
    paper_root = tmp_path / "custom_paper_reports"
    
    runner = LiveEngineRunner(
        ["BTCUSDT"],
        runtime_mode="monitor_only",
        strategy_runtime={
            "paper_ledger_root": str(paper_root),
            "implemented": True,
        },
        data_manager=FakeDataManager(),
    )
    
    assert runner.paper_ledger.root_dir == paper_root
    assert not paper_root.exists() # Should not create until first write
