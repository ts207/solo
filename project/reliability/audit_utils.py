from typing import Any, Callable, Dict

import numpy as np
import pandas as pd


def verify_pit_compliance(
    feature_fn: Callable[[pd.DataFrame, Any], pd.DataFrame | pd.Series],
    df: pd.DataFrame,
    *args,
    **kwargs,
) -> Dict[str, Any]:
    """
    Audit a feature generation function for look-ahead bias.

    It compares the output of the function on the full dataframe
    vs the output on truncated versions of the dataframe.
    """
    n = len(df)
    full_output = feature_fn(df, *args, **kwargs)

    if isinstance(full_output, pd.Series):
        full_output = full_output.to_frame(name="feature")

    mismatches = []

    # Check at several points (e.g. 25%, 50%, 75%)
    for pct in [0.25, 0.5, 0.75]:
        cutoff = int(n * pct)
        df_truncated = df.iloc[:cutoff].copy()

        truncated_output = feature_fn(df_truncated, *args, **kwargs)
        if isinstance(truncated_output, pd.Series):
            truncated_output = truncated_output.to_frame(name="feature")

        # Compare overlapping part
        common_idx = truncated_output.index.intersection(full_output.index)

        for col in truncated_output.columns:
            if col not in full_output.columns:
                continue

            s_full = full_output.loc[common_idx, col]
            s_trunc = truncated_output.loc[common_idx, col]

            # Mask NaNs as they compare as unequal
            mask = s_full.notna() & s_trunc.notna()
            if not s_full[mask].equals(s_trunc[mask]):
                # Find the first index where they differ
                diff_idx = np.where(s_full[mask].values != s_trunc[mask].values)[0]
                if len(diff_idx) > 0:
                    first_diff = common_idx[mask][diff_idx[0]]
                    mismatches.append(
                        {
                            "cutoff_idx": cutoff,
                            "column": col,
                            "first_mismatch_at": first_diff,
                            "max_diff": (s_full[mask] - s_trunc[mask]).abs().max(),
                        }
                    )

    is_compliant = len(mismatches) == 0
    return {"is_compliant": is_compliant, "mismatches": mismatches}
