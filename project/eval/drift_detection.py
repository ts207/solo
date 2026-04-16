import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

LOGGER = logging.getLogger(__name__)


def detect_parameter_drift(
    old_manifest: Dict[str, Any], new_manifest: Dict[str, Any], threshold: float = 0.1
) -> List[Dict[str, Any]]:
    """
    Detects parameters that have changed by more than `threshold` (10% by default)
    between `old_manifest` and `new_manifest`.
    """
    drift_flags = []

    old_blueprints = old_manifest.get("blueprints", {})
    new_blueprints = new_manifest.get("blueprints", {})

    for bp_id, new_params in new_blueprints.items():
        if bp_id in old_blueprints:
            old_params = old_blueprints[bp_id]
            for param_name, new_val in new_params.items():
                if param_name in old_params:
                    old_val = old_params[param_name]

                    if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
                        if old_val != 0:
                            pct_change = abs(new_val - old_val) / abs(old_val)
                            if pct_change > threshold:
                                drift_flags.append(
                                    {
                                        "blueprint_id": bp_id,
                                        "parameter": param_name,
                                        "old_value": old_val,
                                        "new_value": new_val,
                                        "pct_change": pct_change,
                                    }
                                )
                        elif new_val != 0:
                            drift_flags.append(
                                {
                                    "blueprint_id": bp_id,
                                    "parameter": param_name,
                                    "old_value": old_val,
                                    "new_value": new_val,
                                    "pct_change": float("inf"),
                                }
                            )

    return drift_flags


def detect_feature_drift(
    current_features_df: pd.DataFrame,
    reference_distributions_path: str = "",
    p_value_threshold: float = 0.05,
    ks_threshold: float = 0.15,
    reference_fraction: float = 0.5,
) -> List[Dict[str, Any]]:
    """
    Compares the latter portion of the features dataframe against the first portion using the Kolmogorov-Smirnov test.
    Emits a warning/flag if the KS p-value < 0.05 AND the KS statistic > ks_threshold.
    This rolling window approach prevents the need for static reference JSON files and adapts to slow regime drift.
    """
    drift_flags = []

    if current_features_df.empty or len(current_features_df) < 100:
        return drift_flags

    n_rows = len(current_features_df)
    split_idx = int(n_rows * reference_fraction)

    reference_df = current_features_df.iloc[:split_idx]
    recent_df = current_features_df.iloc[split_idx:]

    SKIP_COLS = {
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "enter_idx",
        "exit_idx",
        "quote_volume",
        "taker_base_volume",
        "taker_buy_quote_volume",
        "trade_count",
        "depth_usd",
        "spot_close",
        "spot_open",
        "spot_high",
        "spot_low",
        "liquidation_notional",
        "liquidation_count",
        "oi_notional",
        "funding_abs",
    }

    for col in current_features_df.select_dtypes(include=[np.number]).columns:
        if col in SKIP_COLS or col.endswith("_raw") or col.endswith("_count"):
            continue

        recent_data = recent_df[col].dropna().values
        ref_data = reference_df[col].dropna().values

        if len(recent_data) < 10 or len(ref_data) < 10:
            continue

        # Sample to improve performance on very large dataframes
        max_samples = 1000
        if len(recent_data) > max_samples:
            rng = np.random.RandomState(42)
            recent_data = rng.choice(recent_data, max_samples, replace=False)
        if len(ref_data) > max_samples:
            rng = np.random.RandomState(42)
            ref_data = rng.choice(ref_data, max_samples, replace=False)

        # Add a tiny noise to prevent exact ties triggering KS test warnings for discrete features
        if len(np.unique(recent_data)) < 10:
            rng = np.random.RandomState(42)
            recent_data = recent_data + rng.normal(0, 1e-8, len(recent_data))
            ref_data = ref_data + rng.normal(0, 1e-8, len(ref_data))

        stat, p_value = ks_2samp(recent_data, ref_data)

        if p_value < p_value_threshold and stat > ks_threshold:
            flag = {"feature": col, "ks_statistic": float(stat), "p_value": float(p_value)}
            drift_flags.append(flag)

    return drift_flags
