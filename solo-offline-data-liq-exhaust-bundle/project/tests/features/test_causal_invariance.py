from __future__ import annotations

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import importlib

# Feature builders to test
# Format: (module_path, function_name, mock_input_generator)
BUILDERS_TO_TEST = [
    ("project.features.carry_state", "calculate_funding_rate_bps", "gen_funding_data"),
    ("project.features.vol_regime", "calculate_rv_percentile_24h", "gen_price_data"),
    ("project.features.microstructure", "calculate_roll", "gen_price_data"),
    ("project.features.microstructure", "calculate_roll_spread_bps", "gen_price_data"),
    ("project.features.microstructure", "calculate_amihud_illiquidity", "gen_price_vol_data"),
    ("project.features.microstructure", "calculate_kyle_lambda", "gen_price_buy_sell_vol_data"),
    ("project.features.microstructure", "calculate_vpin_score", "gen_vol_buy_vol_data"),
    ("project.features.context_states", "calculate_ms_vol_state", "gen_rv_pct_data"),
    ("project.features.context_states", "calculate_ms_liq_state", "gen_quote_vol_data"),
    ("project.features.context_states", "calculate_ms_oi_state", "gen_oi_delta_data"),
    ("project.features.context_states", "calculate_ms_funding_state", "gen_funding_bps_data"),
    ("project.features.context_states", "calculate_ms_trend_state", "gen_trend_return_data"),
    ("project.features.context_states", "calculate_ms_spread_state", "gen_spread_z_data"),
]

# --- Mock Data Generators ---


def gen_price_data(n=500):
    return pd.Series(100.0 * np.exp(np.cumsum(np.random.normal(0, 0.001, n))), name="close")


def gen_funding_data(n=500):
    return pd.Series(np.random.normal(0, 0.0001, n), name="funding_rate")


def gen_price_vol_data(n=500):
    close = gen_price_data(n)
    volume = pd.Series(np.random.gamma(2, 100, n), name="volume")
    return close, volume


def gen_price_buy_sell_vol_data(n=500):
    close = gen_price_data(n)
    buy_vol = pd.Series(np.random.gamma(2, 50, n), name="buy_vol")
    sell_vol = pd.Series(np.random.gamma(2, 50, n), name="sell_vol")
    return close, buy_vol, sell_vol


def gen_vol_buy_vol_data(n=500):
    volume = pd.Series(np.random.gamma(2, 100, n), name="volume")
    buy_volume = volume * np.random.uniform(0.4, 0.6, n)
    return volume, buy_volume


def gen_rv_pct_data(n=500):
    return pd.Series(np.random.uniform(0, 100, n), name="rv_pct")


def gen_quote_vol_data(n=500):
    return pd.Series(np.random.gamma(2, 1000, n), name="quote_volume")


def gen_oi_delta_data(n=500):
    return pd.Series(np.random.normal(0, 100000, n), name="oi_delta_1h")


def gen_funding_bps_data(n=500):
    return pd.Series(np.random.normal(0, 2, n), name="funding_rate_bps")


def gen_trend_return_data(n=500):
    return pd.Series(np.random.normal(0, 0.01, n), name="trend_return")


def gen_spread_z_data(n=500):
    return pd.Series(np.random.normal(0, 1, n), name="spread_z")


@pytest.mark.parametrize("module_path, func_name, gen_name", BUILDERS_TO_TEST)
def test_feature_prefix_invariance(module_path, func_name, gen_name):
    """
    F3: Expand prefix-invariance coverage.
    A causal feature must only depend on past data.
    """
    # 1. Dynamically import the function
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)

    # 2. Generate inputs
    gen_func = globals()[gen_name]
    inputs = gen_func(n=500)

    # 3. Compute on full dataset
    if isinstance(inputs, tuple):
        full_result = func(*inputs)
    else:
        full_result = func(inputs)

    # 4. Check multiple cutoffs
    for cutoff in [100, 250, 400]:
        if isinstance(inputs, tuple):
            trunc_inputs = tuple(arg.iloc[:cutoff] for arg in inputs)
            trunc_result = func(*trunc_inputs)
        else:
            trunc_inputs = inputs.iloc[:cutoff]
            trunc_result = func(trunc_inputs)

        # 5. Compare
        # We use fillna(0) or similar because some builders might have different
        # NaN behavior at the start depending on windowing, but once they have
        # enough data, they must be identical.

        # Take the slice from full_result up to cutoff
        expected = full_result.iloc[:cutoff]
        actual = trunc_result

        # Assert equality for non-NaN values
        # (Using a small epsilon for float comparison)
        pd.testing.assert_series_equal(
            expected, actual, check_dtype=False, atol=1e-10, obj=f"{func_name} at cutoff {cutoff}"
        )


def test_vol_shock_relaxation_causality():
    """VSR has a different signature (detect_vol_shock_relaxation_events)."""
    from project.features.vol_shock_relaxation import (
        detect_vol_shock_relaxation_events,
        VolShockRelaxationConfig,
    )

    n = 1000
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": 100.0 * np.exp(np.cumsum(np.random.normal(0, 0.001, n))),
            "high": 101.0,
            "low": 99.0,
        }
    )
    df["high"] = df["close"] * 1.001
    df["low"] = df["close"] * 0.999

    config = VolShockRelaxationConfig(baseline_window=100, rv_window=5)

    # Full run
    full_events, full_df, _ = detect_vol_shock_relaxation_events(df, "TEST", config)

    # Cutoff check: The core series (full_df) must be invariant
    for cutoff in [300, 600]:
        trunc_df = df.iloc[:cutoff].copy()
        _, trunc_core, _ = detect_vol_shock_relaxation_events(trunc_df, "TEST", config)

        # Check core columns like shock_ratio, shock_z
        for col in ["rv", "rv_base", "shock_ratio", "shock_z"]:
            pd.testing.assert_series_equal(
                full_df[col].iloc[:cutoff],
                trunc_core[col],
                check_dtype=False,
                atol=1e-10,
                obj=f"VSR {col} at cutoff {cutoff}",
            )

        # Events check: any event that STARTED before cutoff should have identical metadata
        # (though exit_idx might change if it was truncated)
        full_events_started = full_events[full_events["enter_idx"] < cutoff - 200]  # Safe margin
        if not full_events_started.empty:
            # This is a bit more complex since events are discrete objects
            pass


def test_liquidity_vacuum_causality():
    """LiquidityVacuumDetector must not use future bars (prefix invariance).

    Calls the public ``detect_liquidity_vacuum_events`` with a pinned
    ``t_shock`` threshold so the threshold is not recomputed from truncated
    data (which would cause spurious prefix differences).  Events whose
    ``event_idx`` falls before the safe zone must be identical between the
    full and truncated runs.
    """
    from project.features.liquidity_vacuum import (
        detect_liquidity_vacuum_events,
        LiquidityVacuumConfig,
        _compute_core_series,
    )

    rng = np.random.default_rng(42)
    n = 800
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, n)))
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "open": close,
            "high": close * (1 + rng.uniform(0, 0.002, n)),
            "low": close * (1 - rng.uniform(0, 0.002, n)),
            "close": close,
            "volume": rng.gamma(2, 1000, n),
        }
    )

    cfg = LiquidityVacuumConfig(volume_window=100, range_window=100)

    # Compute a fixed shock threshold from the FULL dataset to avoid
    # threshold recomputation causing spurious prefix differences.
    full_core = _compute_core_series(df, cfg)
    t_shock_fixed = float(full_core["abs_return"].quantile(cfg.shock_quantile))

    # Full run using the public event detection function
    full_events = detect_liquidity_vacuum_events(df, "TEST", cfg, t_shock=t_shock_fixed)

    warmup = cfg.volume_window + 20  # safe margin past the rolling window warmup

    for cutoff in [400, 650]:
        trunc_events = detect_liquidity_vacuum_events(
            df.iloc[:cutoff].copy(), "TEST", cfg, t_shock=t_shock_fixed
        )

        safe_limit = cutoff - warmup

        full_safe = set(
            full_events.loc[full_events["event_idx"] < safe_limit, "event_idx"].tolist()
        )
        trunc_safe = set(
            trunc_events.loc[trunc_events["event_idx"] < safe_limit, "event_idx"].tolist()
        )

        assert full_safe == trunc_safe, (
            f"Liquidity vacuum events differ at cutoff={cutoff}: "
            f"full={sorted(full_safe)}, trunc={sorted(trunc_safe)}"
        )


def test_funding_persistence_causality():
    """
    Causality test for build_funding_persistence_state.

    The rolling percentile window is 96 bars (hardcoded in _rolling_percentile).
    Values at indices strictly inside the warmup region will be NaN / 0.0, but
    values at safe indices (warmup + margin before cutoff) must be identical
    whether computed on the full dataset or a truncated dataset.
    """
    from project.features.funding_persistence import (
        build_funding_persistence_state,
        FundingPersistenceConfig,
    )

    rng = np.random.default_rng(seed=99)
    n = 2000
    rolling_window = 96  # matches hardcoded window in _rolling_percentile

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "funding_rate_scaled": rng.normal(0, 2.0, n),
        }
    )

    full_out = build_funding_persistence_state(df, "TEST")

    for cutoff in [500, 1000, 1500]:
        trunc_df = df.iloc[:cutoff].copy()
        trunc_out = build_funding_persistence_state(trunc_df, "TEST")

        # Safe region: well past the rolling window, with a margin before the cutoff
        safe_start = rolling_window
        safe_end = cutoff - rolling_window  # leave rolling_window bars as margin
        if safe_end <= safe_start:
            continue

        for col in ["fp_active", "fp_age_bars", "fp_severity"]:
            pd.testing.assert_series_equal(
                full_out[col].iloc[safe_start:safe_end].reset_index(drop=True),
                trunc_out[col].iloc[safe_start:safe_end].reset_index(drop=True),
                check_dtype=False,
                atol=1e-10,
                obj=f"FP {col} at cutoff {cutoff}",
            )


def test_rolling_center_not_used_in_feature_modules():
    """
    Rolling windows with center=True introduce lookahead bias by symmetrically
    weighting future bars. Scan all project/features/*.py source files and assert
    none use rolling(center=True).
    """
    import re
    from project.tests.conftest import PROJECT_ROOT

    features_dir = PROJECT_ROOT / "features"
    assert features_dir.is_dir(), f"Features directory not found: {features_dir}"

    violations: list[str] = []
    center_pattern = re.compile(r"\.rolling\s*\([^)]*\bcenter\s*=\s*True", re.DOTALL)

    for py_file in sorted(features_dir.rglob("*.py")):
        source = py_file.read_text(encoding="utf-8", errors="replace")
        for match in center_pattern.finditer(source):
            line_num = source[: match.start()].count("\n") + 1
            violations.append(f"{py_file.relative_to(features_dir.parent.parent)}:{line_num}")

    assert not violations, (
        "Found rolling(center=True) in feature modules — this introduces lookahead bias.\n"
        "Use rolling(window=N, min_periods=N) instead.\n"
        "Violations:\n" + "\n".join(violations)
    )
