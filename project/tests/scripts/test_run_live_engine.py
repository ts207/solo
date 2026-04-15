from __future__ import annotations

import asyncio
import json
from pathlib import Path

import project.live.runner as live_runner
from project.scripts import run_live_engine


def test_build_live_runner_uses_snapshot_path_and_config_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "live_config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime_mode: monitor_only",
                "freshness_streams:",
                "  - symbol: BTCUSDT",
                "    stream: kline_5m",
                "  - symbol: ETHUSDT",
                "    stream: kline_5m",
                "live_state_snapshot_path: state/live_state.json",
                "microstructure_recovery_streak: 4",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class _DummyRunner:
        def __init__(
            self,
            symbols,
            *,
            snapshot_path,
            microstructure_recovery_streak,
            account_sync_interval_seconds,
            account_sync_failure_threshold,
            execution_degradation_min_samples,
            execution_degradation_warn_edge_bps,
            execution_degradation_block_edge_bps,
            execution_degradation_throttle_scale,
            stale_threshold_sec,
            order_manager,
            runtime_mode,
            strategy_runtime,
        ):
            self.session_metadata = {
                "symbols": list(symbols),
                "live_state_snapshot_path": str(snapshot_path),
                "kill_switch_recovery_streak": int(microstructure_recovery_streak),
                "account_sync_interval_seconds": float(account_sync_interval_seconds),
                "account_sync_failure_threshold": int(account_sync_failure_threshold),
                "execution_degradation_min_samples": int(execution_degradation_min_samples),
                "execution_degradation_warn_edge_bps": float(execution_degradation_warn_edge_bps),
                "execution_degradation_block_edge_bps": float(execution_degradation_block_edge_bps),
                "execution_degradation_throttle_scale": float(execution_degradation_throttle_scale),
                "stale_threshold_sec": float(stale_threshold_sec),
                "runtime_mode": str(runtime_mode),
                "strategy_runtime_implemented": bool(
                    isinstance(strategy_runtime, dict) and strategy_runtime.get("implemented", False)
                ),
            }
            self.order_manager = order_manager

    original = live_runner.LiveEngineRunner
    live_runner.LiveEngineRunner = _DummyRunner
    try:
        runner = run_live_engine.build_live_runner(config_path=config_path)
    finally:
        live_runner.LiveEngineRunner = original

    assert runner.session_metadata["symbols"] == ["btcusdt", "ethusdt"]
    assert runner.session_metadata["live_state_snapshot_path"].endswith("state/live_state.json")
    assert runner.session_metadata["kill_switch_recovery_streak"] == 4
    assert runner.session_metadata["account_sync_interval_seconds"] == 30.0
    assert runner.session_metadata["account_sync_failure_threshold"] == 3
    assert runner.session_metadata["execution_degradation_min_samples"] == 3
    assert runner.session_metadata["execution_degradation_warn_edge_bps"] == 0.0
    assert runner.session_metadata["execution_degradation_block_edge_bps"] == -5.0
    assert runner.session_metadata["execution_degradation_throttle_scale"] == 0.5
    assert runner.session_metadata["stale_threshold_sec"] == 60.0
    assert runner.session_metadata["runtime_mode"] == "monitor_only"
    assert runner.session_metadata["strategy_runtime_implemented"] is False


def test_run_live_engine_print_session_metadata(capsys, tmp_path: Path) -> None:
    config_path = tmp_path / "live_config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime_mode: monitor_only",
                "freshness_streams:",
                "  - symbol: BTCUSDT",
                "    stream: kline_5m",
                "live_state_snapshot_path: state/live_state.json",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    assert run_live_engine.main(["--config", str(config_path), "--print_session_metadata"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["symbols"] == ["btcusdt"]
    assert out["live_state_snapshot_path"].endswith("state/live_state.json")
    assert out["live_state_auto_persist_enabled"] is True
    assert out["kill_switch_recovery_streak"] == 3
    assert out["account_sync_interval_seconds"] == 30.0
    assert out["account_sync_failure_threshold"] == 3
    assert out["execution_degradation_min_samples"] == 3
    assert out["execution_degradation_warn_edge_bps"] == 0.0
    assert out["execution_degradation_block_edge_bps"] == -5.0
    assert out["execution_degradation_throttle_scale"] == 0.5
    assert out["stale_threshold_sec"] == 60.0
    assert out["runtime_mode"] == "monitor_only"
    assert out["strategy_runtime_implemented"] is False


def test_validate_live_runtime_environment_accepts_paper_contract() -> None:
    out = run_live_engine.validate_live_runtime_environment(
        config_path=Path("project/configs/live_paper.yaml"),
        snapshot_path="/var/lib/edge/live_state_paper.json",
        environ={
            "EDGE_ENVIRONMENT": "paper",
            "EDGE_VENUE": "binance",
            "EDGE_LIVE_CONFIG": "/opt/edge/project/configs/live_paper.yaml",
            "EDGE_LIVE_SNAPSHOT_PATH": "/var/lib/edge/live_state_paper.json",
            "EDGE_BINANCE_PAPER_API_KEY": "paper-key",
            "EDGE_BINANCE_PAPER_API_SECRET": "paper-secret",
        },
    )

    assert out["environment"] == "paper"
    assert out["venue"] == "binance"


def test_validate_live_runtime_environment_accepts_monitor_only_without_trading_credentials() -> None:
    out = run_live_engine.validate_live_runtime_environment(
        config_path=Path("project/configs/live_paper.yaml"),
        environ={
            "EDGE_ENVIRONMENT": "",
            "EDGE_VENUE": "",
            "EDGE_LIVE_CONFIG": "",
            "EDGE_LIVE_SNAPSHOT_PATH": "",
        },
    )

    assert out["environment"] == "paper"
    assert out["venue"] == ""


def test_validate_live_runtime_environment_accepts_paper_thesis_trading_contract() -> None:
    out = run_live_engine.validate_live_runtime_environment(
        config_path=Path("project/configs/live_paper_btc_thesis_v1.yaml"),
        snapshot_path="/var/lib/edge/live_state_paper_btc_thesis.json",
        environ={
            "EDGE_ENVIRONMENT": "paper",
            "EDGE_VENUE": "binance",
            "EDGE_LIVE_CONFIG": "/opt/edge/project/configs/live_paper_btc_thesis_v1.yaml",
            "EDGE_LIVE_SNAPSHOT_PATH": "/var/lib/edge/live_state_paper_btc_thesis.json",
            "EDGE_BINANCE_PAPER_API_KEY": "paper-key",
            "EDGE_BINANCE_PAPER_API_SECRET": "paper-secret",
        },
    )

    assert out["environment"] == "paper"
    assert out["venue"] == "binance"


def test_validate_live_runtime_environment_rejects_missing_production_credentials(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "live_trading_production.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow_id: live_production_v1",
                "runtime_mode: trading",
                "freshness_streams:",
                "  - symbol: BTCUSDT",
                "    stream: kline_5m",
                "oms_lineage:",
                "  order_source: production_oms",
                "live_state_snapshot_path: state/live_state.json",
                "strategy_runtime:",
                "  implemented: true",
                "  thesis_run_id: run_prod_001",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    try:
        run_live_engine.validate_live_runtime_environment(
            config_path=config_path,
            environ={
                "EDGE_ENVIRONMENT": "production",
                "EDGE_VENUE": "binance",
                "EDGE_LIVE_CONFIG": str(config_path),
                "EDGE_LIVE_SNAPSHOT_PATH": "/var/lib/edge/live_state_production.json",
            },
        )
    except run_live_engine.LiveRuntimeConfigError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected LiveRuntimeConfigError")

    assert "EDGE_BINANCE_API_KEY must be set" in message
    assert "EDGE_BINANCE_API_SECRET must be set" in message


def test_validate_live_runtime_environment_rejects_trading_mode_without_strategy_runtime(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "live_trading.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime_mode: trading",
                "freshness_streams:",
                "  - symbol: BTCUSDT",
                "    stream: kline_5m",
                "live_state_snapshot_path: state/live_state.json",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    try:
        run_live_engine.validate_live_runtime_environment(
            config_path=config_path,
            environ={
                "EDGE_ENVIRONMENT": "paper",
                "EDGE_VENUE": "binance",
                "EDGE_LIVE_CONFIG": str(config_path),
                "EDGE_LIVE_SNAPSHOT_PATH": "state/live_state.json",
                "EDGE_BINANCE_PAPER_API_KEY": "paper-key",
                "EDGE_BINANCE_PAPER_API_SECRET": "paper-secret",
            },
        )
    except run_live_engine.LiveRuntimeConfigError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected LiveRuntimeConfigError")

    assert "strategy_runtime.implemented=true" in message


def test_load_live_engine_config_rejects_latest_thesis_fallback(tmp_path: Path) -> None:
    config_path = tmp_path / "live_config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime_mode: monitor_only",
                "strategy_runtime:",
                "  implemented: true",
                "  load_latest_theses: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    try:
        run_live_engine.load_live_engine_config(config_path)
    except run_live_engine.LiveRuntimeConfigError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected LiveRuntimeConfigError")

    assert "load_latest_theses is no longer supported" in message


def test_load_live_engine_config_requires_explicit_thesis_source_when_enabled(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "live_config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime_mode: monitor_only",
                "strategy_runtime:",
                "  implemented: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    try:
        run_live_engine.load_live_engine_config(config_path)
    except run_live_engine.LiveRuntimeConfigError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected LiveRuntimeConfigError")

    assert "requires explicit thesis input" in message


def test_run_live_engine_print_session_metadata_skips_runtime_env_validation(
    monkeypatch, capsys
) -> None:
    called = {"count": 0}

    def _fail_validation(**kwargs):
        called["count"] += 1
        raise AssertionError("validation should not run for metadata-only output")

    monkeypatch.setattr(run_live_engine, "validate_live_runtime_environment", _fail_validation)

    assert (
        run_live_engine.main(
            ["--config", "project/configs/live_production.yaml", "--print_session_metadata"]
        )
        == 0
    )
    out = json.loads(capsys.readouterr().out)
    assert out["live_state_snapshot_path"] == "artifacts/live_state_production.json"
    assert called["count"] == 0


def test_run_live_engine_missing_config_fails_fast() -> None:
    try:
        run_live_engine.main([])
    except SystemExit as exc:
        assert exc.code != 0
    else:
        raise AssertionError("expected SystemExit for missing --config")


def test_run_live_engine_start_validates_runtime_environment_before_start(
    monkeypatch, tmp_path: Path
) -> None:
    class _DummyStateStore:
        def update_from_exchange_snapshot(self, snapshot) -> None:
            self.snapshot = snapshot

    class _DummyRunner:
        def __init__(self) -> None:
            self.started = False
            self.state_store = _DummyStateStore()

        async def start(self) -> None:
            self.started = True

    dummy_runner = _DummyRunner()
    called = {"count": 0}

    monkeypatch.setattr(run_live_engine, "build_live_runner", lambda **kwargs: dummy_runner)

    config_path = tmp_path / "live_trading.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow_id: live_paper_v1",
                "runtime_mode: trading",
                "freshness_streams:",
                "  - symbol: BTCUSDT",
                "    stream: kline_5m",
                "oms_lineage:",
                "  order_source: paper_oms",
                "live_state_snapshot_path: state/live_state.json",
                "strategy_runtime:",
                "  implemented: true",
                "  thesis_run_id: run_paper_001",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    def _validate(**kwargs):
        called["count"] += 1
        return {"environment": "paper", "venue": "binance"}

    monkeypatch.setattr(run_live_engine, "validate_live_runtime_environment", _validate)
    monkeypatch.setattr(
        run_live_engine,
        "preflight_binance_venue_connectivity",
        lambda **kwargs: _async_return(
            {
                "environment": "paper",
                "venue": "binance",
                "account_can_trade": True,
                "account_type": "USDT_FUTURE",
            }
        ),
    )
    monkeypatch.setattr(run_live_engine, "validate_binance_account_preflight", lambda payload: payload)
    monkeypatch.setattr(
        run_live_engine,
        "fetch_binance_futures_account_snapshot",
        lambda **kwargs: _async_return(
            {
                "wallet_balance": 0.0,
                "margin_balance": 0.0,
                "available_balance": 0.0,
                "exchange_status": "NORMAL",
                "positions": [],
            }
        ),
    )

    assert run_live_engine.main(["--config", str(config_path)]) == 0
    assert called["count"] == 1
    assert dummy_runner.started is True


async def _async_return(value):
    return value


class _FakeResponse:
    def __init__(self, status: int, payload=None, text: str = "") -> None:
        self.status = status
        self._payload = payload if payload is not None else {}
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def json(self):
        return self._payload

    async def text(self):
        return self._text


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def get(self, url, headers=None):
        self.calls.append((url, headers or {}))
        return self._responses.pop(0)


def test_preflight_binance_venue_connectivity_accepts_paper_endpoint(monkeypatch) -> None:
    session = _FakeSession(
        [
            _FakeResponse(200, payload={}),
            _FakeResponse(200, payload={"canTrade": True, "accountType": "USDT_FUTURE"}),
        ]
    )
    monkeypatch.setenv("EDGE_BINANCE_PAPER_API_BASE", "https://testnet.binancefuture.com")
    monkeypatch.setenv("EDGE_BINANCE_PAPER_API_KEY", "paper-key")
    monkeypatch.setenv("EDGE_BINANCE_PAPER_API_SECRET", "paper-secret")

    out = asyncio.run(
        run_live_engine.preflight_binance_venue_connectivity(
            environment={"environment": "paper", "venue": "binance"},
            session_factory=lambda **kwargs: session,
        )
    )

    assert out["environment"] == "paper"
    assert out["account_can_trade"] is True
    assert out["account_type"] == "USDT_FUTURE"
    assert session.calls[0][0] == "https://testnet.binancefuture.com/fapi/v1/ping"
    assert session.calls[1][1]["X-MBX-APIKEY"] == "paper-key"


def test_validate_binance_account_preflight_rejects_non_tradable_account() -> None:
    try:
        run_live_engine.validate_binance_account_preflight(
            {"account_can_trade": False, "account_type": "USDT_FUTURE"}
        )
    except run_live_engine.VenueConnectivityError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected VenueConnectivityError")

    assert "cannot trade" in message


def test_validate_binance_account_preflight_rejects_wrong_account_type() -> None:
    try:
        run_live_engine.validate_binance_account_preflight(
            {"account_can_trade": True, "account_type": "SPOT"}
        )
    except run_live_engine.VenueConnectivityError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected VenueConnectivityError")

    assert "unexpected account type" in message


def test_preflight_binance_venue_connectivity_rejects_wrong_host(monkeypatch) -> None:
    monkeypatch.setenv("EDGE_BINANCE_PAPER_API_BASE", "https://fapi.binance.com")
    monkeypatch.setenv("EDGE_BINANCE_PAPER_API_KEY", "paper-key")
    monkeypatch.setenv("EDGE_BINANCE_PAPER_API_SECRET", "paper-secret")

    try:
        asyncio.run(
            run_live_engine.preflight_binance_venue_connectivity(
                environment={"environment": "paper", "venue": "binance"},
                session_factory=lambda **kwargs: _FakeSession([]),
            )
        )
    except run_live_engine.VenueConnectivityError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected VenueConnectivityError")

    assert "testnet.binancefuture.com" in message


def test_normalize_binance_futures_account_snapshot_maps_balances_and_positions() -> None:
    payload = {
        "totalWalletBalance": "1200.5",
        "totalMarginBalance": "1250.0",
        "availableBalance": "900.25",
        "positions": [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0.02",
                "entryPrice": "62000",
                "markPrice": "62500",
                "unrealizedProfit": "10.5",
                "liquidationPrice": "50000",
                "leverage": "5",
                "marginType": "isolated",
            },
            {
                "symbol": "ETHUSDT",
                "positionAmt": "0",
            },
        ],
    }

    out = run_live_engine.normalize_binance_futures_account_snapshot(payload)

    assert out["wallet_balance"] == 1200.5
    assert out["margin_balance"] == 1250.0
    assert out["available_balance"] == 900.25
    assert out["exchange_status"] == "NORMAL"
    assert out["positions"] == [
        {
            "symbol": "BTCUSDT",
            "quantity": 0.02,
            "entry_price": 62000.0,
            "mark_price": 62500.0,
            "unrealized_pnl": 10.5,
            "liquidation_price": 50000.0,
            "leverage": 5.0,
            "margin_type": "ISOLATED",
        }
    ]


def test_fetch_binance_futures_account_snapshot_normalizes_payload(monkeypatch) -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                200,
                payload={
                    "totalWalletBalance": "1500",
                    "totalMarginBalance": "1520",
                    "availableBalance": "1400",
                    "positions": [
                        {
                            "symbol": "BTCUSDT",
                            "positionAmt": "-0.01",
                            "entryPrice": "63000",
                            "unrealizedProfit": "-3.5",
                            "leverage": "3",
                            "marginType": "cross",
                        }
                    ],
                },
            )
        ]
    )
    monkeypatch.setenv("EDGE_BINANCE_PAPER_API_BASE", "https://testnet.binancefuture.com")
    monkeypatch.setenv("EDGE_BINANCE_PAPER_API_KEY", "paper-key")
    monkeypatch.setenv("EDGE_BINANCE_PAPER_API_SECRET", "paper-secret")

    out = asyncio.run(
        run_live_engine.fetch_binance_futures_account_snapshot(
            environment={"environment": "paper", "venue": "binance"},
            session_factory=lambda **kwargs: session,
        )
    )

    assert out["wallet_balance"] == 1500.0
    assert out["positions"][0]["quantity"] == -0.01
    assert out["positions"][0]["margin_type"] == "CROSS"


def test_run_live_engine_start_blocks_when_venue_preflight_fails(
    monkeypatch, tmp_path: Path
) -> None:
    class _DummyRunner:
        def __init__(self) -> None:
            self.started = False

        async def start(self) -> None:
            self.started = True

    dummy_runner = _DummyRunner()
    monkeypatch.setattr(run_live_engine, "build_live_runner", lambda **kwargs: dummy_runner)
    config_path = tmp_path / "live_trading.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow_id: live_production_v1",
                "runtime_mode: trading",
                "freshness_streams:",
                "  - symbol: BTCUSDT",
                "    stream: kline_5m",
                "oms_lineage:",
                "  order_source: production_oms",
                "live_state_snapshot_path: state/live_state.json",
                "strategy_runtime:",
                "  implemented: true",
                "  thesis_run_id: run_prod_001",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        run_live_engine,
        "validate_live_runtime_environment",
        lambda **kwargs: {"environment": "production", "venue": "binance"},
    )

    async def _fail_preflight(**kwargs):
        raise run_live_engine.VenueConnectivityError("boom")

    monkeypatch.setattr(run_live_engine, "preflight_binance_venue_connectivity", _fail_preflight)

    try:
        run_live_engine.main(["--config", str(config_path)])
    except run_live_engine.VenueConnectivityError as exc:
        assert str(exc) == "boom"
    else:
        raise AssertionError("expected VenueConnectivityError")

    assert dummy_runner.started is False


def test_run_live_engine_start_hydrates_initial_account_snapshot_before_start(
    monkeypatch, tmp_path: Path
) -> None:
    class _DummyStateStore:
        def __init__(self) -> None:
            self.snapshots = []

        def update_from_exchange_snapshot(self, snapshot) -> None:
            self.snapshots.append(snapshot)

    class _DummyRunner:
        def __init__(self) -> None:
            self.started = False
            self.state_store = _DummyStateStore()

        async def start(self) -> None:
            self.started = True

    dummy_runner = _DummyRunner()
    monkeypatch.setattr(run_live_engine, "build_live_runner", lambda **kwargs: dummy_runner)
    config_path = tmp_path / "live_trading.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow_id: live_paper_v1",
                "runtime_mode: trading",
                "freshness_streams:",
                "  - symbol: BTCUSDT",
                "    stream: kline_5m",
                "oms_lineage:",
                "  order_source: paper_oms",
                "live_state_snapshot_path: state/live_state.json",
                "strategy_runtime:",
                "  implemented: true",
                "  thesis_run_id: run_paper_001",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        run_live_engine,
        "validate_live_runtime_environment",
        lambda **kwargs: {"environment": "paper", "venue": "binance"},
    )
    monkeypatch.setattr(
        run_live_engine,
        "preflight_binance_venue_connectivity",
        lambda **kwargs: _async_return(
            {
                "environment": "paper",
                "venue": "binance",
                "account_can_trade": True,
                "account_type": "USDT_FUTURE",
            }
        ),
    )
    monkeypatch.setattr(run_live_engine, "validate_binance_account_preflight", lambda payload: payload)
    monkeypatch.setattr(
        run_live_engine,
        "fetch_binance_futures_account_snapshot",
        lambda **kwargs: _async_return(
            {
                "wallet_balance": 111.0,
                "margin_balance": 112.0,
                "available_balance": 90.0,
                "exchange_status": "NORMAL",
                "positions": [{"symbol": "BTCUSDT", "quantity": 0.1, "unrealized_pnl": 1.5}],
            }
        ),
    )

    assert run_live_engine.main(["--config", str(config_path)]) == 0
    assert dummy_runner.state_store.snapshots == [
        {
            "wallet_balance": 111.0,
            "margin_balance": 112.0,
            "available_balance": 90.0,
            "exchange_status": "NORMAL",
            "positions": [{"symbol": "BTCUSDT", "quantity": 0.1, "unrealized_pnl": 1.5}],
        }
    ]
    assert dummy_runner.started is True
