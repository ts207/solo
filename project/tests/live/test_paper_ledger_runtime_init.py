import json
from unittest.mock import MagicMock

import pytest

from project.live.runner import LiveEngineRunner


class FakeDataManager:
    def __init__(self):
        self.kline_queue = MagicMock()
        self.ticker_queue = MagicMock()


def test_live_runner_initializes_paper_ledger_without_crash(tmp_path):
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
    assert not paper_root.exists()


def test_live_runner_owned_paper_ledger_records_lifecycle(tmp_path):
    paper_root = tmp_path / "paper_reports"
    runner = LiveEngineRunner(
        ["BTCUSDT"],
        runtime_mode="monitor_only",
        strategy_runtime={
            "paper_ledger_root": str(paper_root),
            "implemented": True,
        },
        data_manager=FakeDataManager(),
    )

    thesis_id = "runner_thesis"
    symbol = "BTCUSDT"

    runner.paper_ledger.update(
        thesis_id=thesis_id,
        symbol=symbol,
        action="trade_normal",
        side="buy",
        price=50_000.0,
        timestamp="2026-04-27T00:00:00Z",
    )
    runner.paper_ledger.update(
        thesis_id=thesis_id,
        symbol=symbol,
        action="watch",
        side="buy",
        price=49_000.0,
        timestamp="2026-04-27T00:05:00Z",
    )
    runner.paper_ledger.update(
        thesis_id=thesis_id,
        symbol=symbol,
        action="watch",
        side="buy",
        price=51_000.0,
        timestamp="2026-04-27T00:10:00Z",
    )
    runner.paper_ledger.update(
        thesis_id=thesis_id,
        symbol=symbol,
        action="watch",
        side="flat",
        price=52_000.0,
        timestamp="2026-04-27T00:15:00Z",
    )

    record_file = paper_root / thesis_id / "trades.jsonl"
    summary_file = paper_root / thesis_id / "summary.json"

    assert record_file.exists()
    assert summary_file.exists()
    assert f"{thesis_id}:{symbol}" not in runner.paper_ledger.active_positions

    record = json.loads(record_file.read_text().splitlines()[0])
    assert record["thesis_id"] == thesis_id
    assert record["symbol"] == symbol
    assert record["side"] == "long"
    assert record["gross_bps"] == pytest.approx(400.0)
    assert record["net_bps"] == pytest.approx(394.0)
    assert record["mae_bps"] == pytest.approx(-200.0)
    assert record["mfe_bps"] == pytest.approx(200.0)

    summary = json.loads(summary_file.read_text())
    assert summary["trade_count"] == 1
    assert summary["cumulative_net_bps"] == pytest.approx(394.0)
    assert summary["hit_rate"] == 1.0
    assert summary["last_updated_at"].endswith("+00:00")
