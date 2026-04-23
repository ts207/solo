"""
E1-T1: Research → engine reconciliation integration test.

Verifies that research-style metrics (mean next-bar return when signal fires)
and engine-style metrics (Sharpe from compute_pnl_ledger) agree on sign and
rank-order for two synthetic strategy variants.

All data is synthetic and deterministic (seed=42). No file I/O required.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.engine.pnl import compute_pnl_ledger

# ── constants ────────────────────────────────────────────────────────────────
_N_BARS = 500
_SEED = 42
_COST_BPS = 3.0
_BARS_PER_YEAR = 252 * 24 * 4  # 15-min bars
_EMA_WINDOW = 20


# ── helpers ───────────────────────────────────────────────────────────────────


def _make_bars(n: int = _N_BARS, seed: int = _SEED) -> pd.DataFrame:
    """
    Synthetic OHLCV bars with a slight upward drift so momentum has a real edge.
    close = cumsum of N(+0.0003, 0.001) shifted to start near 100.
    """
    rng = np.random.default_rng(seed)
    log_returns = rng.normal(0.0003, 0.001, n)
    close = 100.0 * np.exp(np.cumsum(log_returns))
    ts = pd.date_range("2024-01-01", periods=n, freq="15min", tz="UTC")
    spread = 0.001
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": close * (1 - spread / 2),
            "high": close * 1.002,
            "low": close * 0.998,
            "close": close,
            "volume": 1_000.0,
        },
        index=ts,
    )


def _compute_research_metric(positions: pd.Series, bars: pd.DataFrame) -> float:
    """
    Mean of next-bar close-to-close returns on bars where the signal fires.

    Positions are shifted by 1 bar to prevent lookahead bias:
    the position known at bar T is applied to the return from T to T+1.
    """
    close = bars["close"]
    next_bar_return = close.shift(-1) / close - 1.0  # return earned by holding through bar T

    # shift(1): the position decided at bar T-1 is the one active at bar T
    active_signal = positions.shift(1).fillna(0.0) == 1.0

    eligible = next_bar_return[active_signal].dropna()
    if len(eligible) == 0:
        return 0.0
    return float(eligible.mean())


def _compute_engine_metric(
    positions: pd.Series,
    bars: pd.DataFrame,
    cost_bps: float = _COST_BPS,
) -> float:
    """
    Annualised Sharpe ratio derived from compute_pnl_ledger's equity_return column.

    Returns 0.0 when there are fewer than 10 non-NaN returns or std == 0.
    """
    close = bars["close"]
    ledger = compute_pnl_ledger(
        target_position=positions,
        close=close,
        cost_bps=cost_bps,
    )
    eq_ret = ledger["equity_return"].dropna()
    if len(eq_ret) < 10 or eq_ret.std() == 0.0:
        return 0.0
    return float(eq_ret.mean() / eq_ret.std() * np.sqrt(_BARS_PER_YEAR))


def _make_positive_edge_positions(bars: pd.DataFrame) -> pd.Series:
    """
    Raw signal: 1.0 when close > 20-bar EMA at bar T, 0.0 otherwise.

    No shift is applied here.  Each consumer is responsible for its own
    one-bar lookahead guard:
    - _compute_research_metric applies shift(1) explicitly.
    - compute_pnl_ledger / build_execution_state applies shift(1) internally
      (executed_position[t] = target_position[t-1]).
    """
    ema = bars["close"].ewm(span=_EMA_WINDOW, adjust=False).mean()
    return (bars["close"] > ema).astype(float)


def _make_zero_edge_positions(bars: pd.DataFrame) -> pd.Series:
    """Flat (0.0) at all times — no positions taken."""
    return pd.Series(0.0, index=bars.index)


# ── test class ────────────────────────────────────────────────────────────────


class TestResearchEngineReconciliation:
    """Integration tests verifying sign and rank agreement between research and engine metrics."""

    @pytest.fixture(scope="class")
    def bars(self):
        return _make_bars(n=_N_BARS, seed=_SEED)

    @pytest.fixture(scope="class")
    def pos_a(self, bars):
        """Variant A: momentum long (positive edge expected)."""
        return _make_positive_edge_positions(bars)

    @pytest.fixture(scope="class")
    def pos_b(self, bars):
        """Variant B: always flat (zero edge)."""
        return _make_zero_edge_positions(bars)

    # ── test 1 ────────────────────────────────────────────────────────────────

    def test_sign_agreement_positive_variant(self, bars, pos_a):
        """
        Variant A (momentum long) must produce a positive mean return in both
        the research metric and the engine metric.
        """
        research = _compute_research_metric(pos_a, bars)
        engine = _compute_engine_metric(pos_a, bars)

        assert research > 0.0, (
            f"Research metric for variant A should be positive, got {research:.6f}"
        )
        assert engine > 0.0, f"Engine Sharpe for variant A should be positive, got {engine:.4f}"

    # ── test 2 ────────────────────────────────────────────────────────────────

    def test_rank_order_preserved(self, bars, pos_a, pos_b):
        """
        Variant A must outrank variant B in BOTH the research metric and the
        engine metric.  Variant B is flat so both metrics should be zero (or
        undefined); variant A with positive drift should be strictly higher.
        """
        research_a = _compute_research_metric(pos_a, bars)
        research_b = _compute_research_metric(pos_b, bars)
        engine_a = _compute_engine_metric(pos_a, bars)
        engine_b = _compute_engine_metric(pos_b, bars)

        assert research_a > research_b, (
            f"Research rank wrong: A={research_a:.6f}, B={research_b:.6f}"
        )
        assert engine_a > engine_b, f"Engine rank wrong: A={engine_a:.4f}, B={engine_b:.4f}"

    # ── test 3 ────────────────────────────────────────────────────────────────

    def test_sign_agrees_between_research_and_engine(self, bars, pos_a):
        """
        The sign of the research metric and the sign of the engine Sharpe must
        agree for variant A.
        """
        research = _compute_research_metric(pos_a, bars)
        engine = _compute_engine_metric(pos_a, bars)

        assert np.sign(research) == np.sign(engine), (
            f"Sign disagreement: research={research:.6f}, engine={engine:.4f}"
        )

    # ── test 4 ────────────────────────────────────────────────────────────────

    def test_pnl_ledger_returns_dataframe_with_return_column(self, bars, pos_a):
        """
        Smoke test: compute_pnl_ledger returns a DataFrame that contains the
        equity_return column used by _compute_engine_metric.
        """
        close = bars["close"]
        ledger = compute_pnl_ledger(
            target_position=pos_a,
            close=close,
            cost_bps=_COST_BPS,
        )

        assert isinstance(ledger, pd.DataFrame), f"Expected DataFrame, got {type(ledger)}"
        assert "equity_return" in ledger.columns, (
            f"'equity_return' column missing; available: {ledger.columns.tolist()}"
        )
        assert len(ledger) == len(bars), f"Ledger length {len(ledger)} != bars length {len(bars)}"
