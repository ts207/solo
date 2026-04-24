import numpy as np
import pandas as pd
import pytest

from project.features.liquidity_vacuum import LiquidityVacuumConfig, detect_liquidity_vacuum_events


@pytest.mark.audit
def test_liquidity_vacuum_lookahead_bias():
    """
    Red Team Test: Changing future prices should not affect past event detection
    if the system is Point-In-Time (PIT) compliant.
    """
    n = 2000
    dates = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")

    # 1. Base data: stationary with small random returns
    np.random.seed(42)
    prices = 100.0 * np.exp(np.random.normal(0, 0.001, n).cumsum())
    volumes = np.random.uniform(100, 200, n)

    df_base = pd.DataFrame(
        {
            "timestamp": dates,
            "close": prices,
            "high": prices * 1.001,
            "low": prices * 0.999,
            "volume": volumes,
        }
    )

    # Use a small window for medians so we don't have too much warmup
    # Note: shock_threshold_mode defaults to 'rolling' in the new implementation
    cfg = LiquidityVacuumConfig(volume_window=100, range_window=100, shock_quantile=0.99)

    # 2. Run detection on base data
    # The new implementation computes t_shock_dynamic inside _compute_core_series
    events_base = detect_liquidity_vacuum_events(df_base, "TEST", cfg=cfg)

    # 3. Modify FUTURE data: Insert a massive shock at the very end of the series
    df_shocked = df_base.copy()
    df_shocked.loc[n - 1, "close"] = df_shocked.loc[n - 2, "close"] * 1.50  # 50% shock

    # 4. Run detection on shocked data
    events_shocked = detect_liquidity_vacuum_events(df_shocked, "TEST", cfg=cfg)

    # 5. Check for leaks
    # We compare up to n-2 to avoid the very last bar which we changed
    cutoff = dates[n - 10]

    print(f"DEBUG: events_base columns: {events_base.columns.tolist() or 'EMPTY'}")
    print(f"DEBUG: events_base len: {len(events_base)}")

    if "eval_bar_ts" not in events_base.columns:
        print("DEBUG: eval_bar_ts MISSING from events_base")
        # Ensure we have the right columns even if empty
        if len(events_base) == 0:
            from project.events.shared import EVENT_COLUMNS

            events_base = pd.DataFrame(columns=EVENT_COLUMNS)
            events_shocked = pd.DataFrame(columns=EVENT_COLUMNS)

    past_events_base = events_base[events_base["eval_bar_ts"] < cutoff].copy()
    past_events_shocked = events_shocked[events_shocked["eval_bar_ts"] < cutoff].copy()

    if not past_events_base.empty:
        pd.testing.assert_frame_equal(
            past_events_base.reset_index(drop=True), past_events_shocked.reset_index(drop=True)
        )
    else:
        assert past_events_shocked.empty


def test_vol_regime_lookahead_bias():
    """
    Check if rv_percentile_24h is truly PIT-safe.
    """
    from project.features.vol_regime import calculate_rv_percentile_24h

    n = 2000
    np.random.seed(42)
    prices = 100.0 * np.exp(np.random.normal(0, 0.01, n).cumsum())

    # 1. Base run
    res_base = calculate_rv_percentile_24h(pd.Series(prices))

    # 2. Change future: insert high vol at the end
    prices_shocked = prices.copy()
    prices_shocked[n - 10 :] = prices_shocked[n - 11] * 1.2  # Huge spike

    # 3. Shocked run
    res_shocked = calculate_rv_percentile_24h(pd.Series(prices_shocked))

    # 4. Assert past values (up to n-20) are identical
    cutoff = n - 20
    np.testing.assert_array_almost_equal(
        res_base[:cutoff].values,
        res_shocked[:cutoff].values,
        err_msg="Look-ahead detected in vol_regime calculation!",
    )
