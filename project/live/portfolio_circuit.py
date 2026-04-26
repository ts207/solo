"""Portfolio-level live circuit breaker.

This module evaluates account-level loss/volatility/concentration tripwires.
It is intentionally independent of the runner so it can be unit-tested with
synthetic account snapshots and then wired into the live account-sync loop.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

try:  # pragma: no cover - dependency availability varies by install profile
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore[assignment]


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class PortfolioCircuitConfig:
    enabled: bool = True
    lookback_samples: int = 288
    min_samples: int = 2
    max_portfolio_dd_pct: float = 0.05
    target_vol_pct: float = 0.10
    max_realized_vol_multiple: float = 2.0
    realized_vol_window_samples: int = 48
    concentration_cap_pct: float = 0.05
    concentration_breach_multiplier: float = 1.5

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> "PortfolioCircuitConfig":
        values = dict(raw or {})
        return cls(
            enabled=bool(values.get("enabled", True)),
            lookback_samples=max(2, int(values.get("lookback_samples", 288) or 288)),
            min_samples=max(2, int(values.get("min_samples", 2) or 2)),
            max_portfolio_dd_pct=max(
                0.0, float(values.get("max_portfolio_dd_pct", 0.05) or 0.05)
            ),
            target_vol_pct=max(0.0, float(values.get("target_vol_pct", 0.10) or 0.10)),
            max_realized_vol_multiple=max(
                0.0, float(values.get("max_realized_vol_multiple", 2.0) or 2.0)
            ),
            realized_vol_window_samples=max(
                2, int(values.get("realized_vol_window_samples", 48) or 48)
            ),
            concentration_cap_pct=max(
                0.0, float(values.get("concentration_cap_pct", 0.05) or 0.05)
            ),
            concentration_breach_multiplier=max(
                1.0, float(values.get("concentration_breach_multiplier", 1.5) or 1.5)
            ),
        )


@dataclass(frozen=True)
class PortfolioCircuitSnapshot:
    timestamp: str
    equity: float
    gross_exposure: float
    symbol_exposures: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class PortfolioCircuitVerdict:
    triggered: bool
    reason: str = ""
    message: str = ""
    metrics: dict[str, float | str] = field(default_factory=dict)


def load_portfolio_circuit_config(path: str | Path) -> PortfolioCircuitConfig:
    target = Path(path)
    if not target.exists():
        return PortfolioCircuitConfig()
    if yaml is None:
        raise RuntimeError("PyYAML is required to load portfolio circuit YAML config")
    payload = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, Mapping):
        raise ValueError(f"Portfolio circuit config {target} must be a mapping")
    return PortfolioCircuitConfig.from_mapping(payload)


class PortfolioCircuitBreaker:
    def __init__(self, config: PortfolioCircuitConfig | None = None):
        self.config = config or PortfolioCircuitConfig()
        self.history: list[PortfolioCircuitSnapshot] = []
        self.last_verdict = PortfolioCircuitVerdict(triggered=False)

    @staticmethod
    def _position_symbol(position: Any) -> str:
        if isinstance(position, Mapping):
            return str(position.get("symbol", "")).upper()
        return str(getattr(position, "symbol", "")).upper()

    @staticmethod
    def _position_notional(position: Any) -> float:
        if isinstance(position, Mapping):
            quantity = abs(float(position.get("quantity", 0.0) or 0.0))
            price = float(position.get("mark_price", position.get("price", 0.0)) or 0.0)
            return quantity * abs(price)
        quantity = abs(float(getattr(position, "quantity", 0.0) or 0.0))
        price = float(getattr(position, "mark_price", getattr(position, "price", 0.0)) or 0.0)
        return quantity * abs(price)

    def _snapshot_from_account(self, account: Any) -> PortfolioCircuitSnapshot:
        wallet = float(getattr(account, "wallet_balance", 0.0) or 0.0)
        unrealized = float(getattr(account, "total_unrealized_pnl", 0.0) or 0.0)
        positions_obj = getattr(account, "positions", {}) or {}
        positions: Sequence[Any]
        if isinstance(positions_obj, Mapping):
            positions = list(positions_obj.values())
        else:
            positions = list(positions_obj)

        symbol_exposures: dict[str, float] = {}
        for position in positions:
            symbol = self._position_symbol(position)
            if not symbol:
                continue
            symbol_exposures[symbol] = symbol_exposures.get(symbol, 0.0) + self._position_notional(
                position
            )
        gross = sum(abs(v) for v in symbol_exposures.values())
        return PortfolioCircuitSnapshot(
            timestamp=_utcnow(),
            equity=wallet + unrealized,
            gross_exposure=float(gross),
            symbol_exposures=symbol_exposures,
        )

    def record_snapshot(self, snapshot: PortfolioCircuitSnapshot) -> None:
        self.history.append(snapshot)
        max_len = max(self.config.lookback_samples, self.config.realized_vol_window_samples, 2)
        self.history = self.history[-max_len:]

    def evaluate_account(self, account: Any) -> PortfolioCircuitVerdict:
        snapshot = self._snapshot_from_account(account)
        self.record_snapshot(snapshot)
        verdict = self.evaluate_current()
        self.last_verdict = verdict
        return verdict

    def evaluate_current(self) -> PortfolioCircuitVerdict:
        cfg = self.config
        if not cfg.enabled or len(self.history) < cfg.min_samples:
            return PortfolioCircuitVerdict(
                False, metrics={"sample_count": float(len(self.history))}
            )

        window = self.history[-cfg.lookback_samples :]
        current = window[-1]
        peak_equity = max(item.equity for item in window)
        safe_peak = max(abs(peak_equity), 1e-9)
        drawdown = max(0.0, (peak_equity - current.equity) / safe_peak)
        metrics: dict[str, float | str] = {
            "sample_count": float(len(window)),
            "equity": float(current.equity),
            "peak_equity": float(peak_equity),
            "portfolio_drawdown_pct": float(drawdown),
            "gross_exposure": float(current.gross_exposure),
        }

        if drawdown >= cfg.max_portfolio_dd_pct:
            return PortfolioCircuitVerdict(
                True,
                reason="portfolio_drawdown",
                message=(
                    f"Portfolio drawdown {drawdown:.2%} exceeded "
                    f"limit {cfg.max_portfolio_dd_pct:.2%}"
                ),
                metrics=metrics,
            )

        vol_window = self.history[-cfg.realized_vol_window_samples :]
        returns: list[float] = []
        for prev, curr in zip(vol_window, vol_window[1:]):
            denom = max(abs(prev.equity), 1e-9)
            returns.append((curr.equity - prev.equity) / denom)
        realized_vol = 0.0
        if len(returns) >= 2:
            mean_return = sum(returns) / len(returns)
            variance = sum((item - mean_return) ** 2 for item in returns) / (len(returns) - 1)
            realized_vol = math.sqrt(max(0.0, variance))
        metrics["realized_vol_pct"] = float(realized_vol)
        vol_limit = cfg.target_vol_pct * cfg.max_realized_vol_multiple
        metrics["realized_vol_limit_pct"] = float(vol_limit)
        if vol_limit > 0.0 and realized_vol >= vol_limit:
            return PortfolioCircuitVerdict(
                True,
                reason="portfolio_realized_vol",
                message=(
                    f"Portfolio realized volatility {realized_vol:.2%} exceeded "
                    f"limit {vol_limit:.2%}"
                ),
                metrics=metrics,
            )

        equity = max(abs(current.equity), 1e-9)
        max_symbol = ""
        max_concentration = 0.0
        for symbol, exposure in current.symbol_exposures.items():
            concentration = abs(float(exposure)) / equity
            if concentration > max_concentration:
                max_concentration = concentration
                max_symbol = symbol
        concentration_limit = cfg.concentration_cap_pct * cfg.concentration_breach_multiplier
        metrics["max_symbol_concentration_pct"] = float(max_concentration)
        metrics["concentration_limit_pct"] = float(concentration_limit)
        metrics["max_symbol"] = max_symbol
        if concentration_limit > 0.0 and max_concentration >= concentration_limit:
            return PortfolioCircuitVerdict(
                True,
                reason="portfolio_concentration",
                message=(
                    f"Symbol concentration {max_concentration:.2%} in {max_symbol} exceeded "
                    f"limit {concentration_limit:.2%}"
                ),
                metrics=metrics,
            )

        return PortfolioCircuitVerdict(False, metrics=metrics)
