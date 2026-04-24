from __future__ import annotations

import logging
from typing import Literal

import pandas as pd

from project.contracts.temporal_contracts import TemporalContract
from project.core.validation import assert_monotonic_utc_timestamp, ts_ns_utc

LOGGER = logging.getLogger(__name__)

# --- Temporal Contract ---

TEMPORAL_CONTRACT = TemporalContract(
    name="alignment",
    output_mode="alignment",
    observation_clock="event_timestamp",
    decision_lag_bars=0,
    lookback_bars=None,
    uses_current_observation=True,  # Aligning to current bar's timestamp
    calibration_mode="none",
    fit_scope="none",
    approved_primitives=("backward_asof_align"),
    notes="Aligns external data (e.g. funding) using backward merge_asof.",
)


def align_funding_to_bars(
    bars: pd.DataFrame, funding: pd.DataFrame, max_staleness: pd.Timedelta = pd.Timedelta("8h")
) -> pd.DataFrame:
    """
    Align funding data to bar timestamps using backward merge_asof.
    Ensures both inputs are properly formatted and monotonic.
    """
    if bars.empty:
        return pd.DataFrame()
    if funding.empty:
        # Return bars with null funding columns if columns are known, or just bars
        return bars.copy()

    bars_sorted = bars.copy()
    bars_sorted["timestamp"] = ts_ns_utc(bars_sorted["timestamp"])
    bars_sorted = bars_sorted.sort_values("timestamp").reset_index(drop=True)
    assert_monotonic_utc_timestamp(bars_sorted, "timestamp")

    funding_rates = funding.copy()
    funding_rates["timestamp"] = ts_ns_utc(funding_rates["timestamp"])
    if "funding_rate_scaled" in funding_rates.columns:
        funding_rates["funding_rate_scaled"] = pd.to_numeric(
            funding_rates["funding_rate_scaled"], errors="coerce"
        )
    funding_rates = funding_rates.sort_values("timestamp").reset_index(drop=True)
    assert_monotonic_utc_timestamp(funding_rates, "timestamp")

    expected_rows = len(bars_sorted)

    # Track the source timestamp to verify PIT and staleness
    funding_rates["_source_ts"] = funding_rates["timestamp"]

    aligned = pd.merge_asof(
        bars_sorted,
        funding_rates,
        on="timestamp",
        direction="backward",
        tolerance=max_staleness,
    )

    if len(aligned) != expected_rows:
        raise ValueError(
            f"Cardinality mismatch after funding alignment: "
            f"expected {expected_rows}, got {len(aligned)}"
        )

    # Calculate staleness
    aligned["funding_staleness"] = aligned["timestamp"] - aligned["_source_ts"]

    return aligned


def assert_complete_funding_series(
    df: pd.DataFrame,
    symbol: str = "unknown",
    on_missing: Literal["raise", "warn"] = "raise",
    fill_value: float = 0.0
) -> pd.Series:
    """
    Validate that funding alignment has no major gaps and return the funding series.
    """
    if df.empty:
        return pd.Series(dtype=float)

    if "funding_rate_scaled" not in df.columns:
        raise ValueError(f"Required funding_rate_scaled column missing for {symbol}")

    series = pd.to_numeric(df["funding_rate_scaled"], errors="coerce").astype(float)
    has_gaps = series.isna().any()

    # Also check explicit "funding_missing" flag if present
    if "funding_missing" in df.columns:
        if df["funding_missing"].astype(bool).any():
            has_gaps = True

    if has_gaps:
        msg = f"Funding alignment gaps found for {symbol}"
        if on_missing == "raise":
            raise ValueError(msg)
        elif on_missing == "warn":
            LOGGER.warning(f"{msg} - filling with {fill_value}")
            series = series.fillna(fill_value)

    return series
