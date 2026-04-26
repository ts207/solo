from typing import Any

import pandas as pd

_CANONICAL_ORDERS = {
    "vol_regime": ["LOW", "MID", "HIGH", "SHOCK"],
    "liquidity_state": ["LOW", "NORMAL", "RECOVERY"],
    "funding_regime": ["NEGATIVE", "NEUTRAL", "POSITIVE"],
}


def run_state_acceptance(
    df: pd.DataFrame, state_col: str, target_metric_col: str
) -> dict[str, Any]:
    """
    Verify monotonicity and separation of a state.
    Example: High vol state should have higher realized volatility than low vol state.
    """
    if state_col not in df.columns or target_metric_col not in df.columns:
        return {"ok": False, "error": f"Columns {state_col} or {target_metric_col} missing"}

    # Calculate group means
    group_means = df.groupby(state_col)[target_metric_col].mean()

    # Resolve order
    order = _CANONICAL_ORDERS.get(state_col.lower())
    if not order:
        # Fallback to numeric or lexical sort of values if no canonical order defined
        is_monotonic = group_means.is_monotonic_increasing
    else:
        # Filter only states present in means and check order
        present_order = [s for s in order if s in group_means.index]
        if len(present_order) < 2:
            is_monotonic = True
        else:
            is_monotonic = True
            for i in range(len(present_order) - 1):
                if group_means[present_order[i]] > group_means[present_order[i + 1]]:
                    is_monotonic = False
                    break

    # Calculate join rate
    join_rate = 1.0 - df[state_col].isna().mean()

    report = {
        "ok": True,
        "state_col": state_col,
        "target_metric_col": target_metric_col,
        "group_means": group_means.to_dict(),
        "is_monotonic": is_monotonic,
        "join_rate": join_rate,
        "passed": is_monotonic and join_rate > 0.99,
    }

    return report


if __name__ == "__main__":
    # Shell implementation for CLI
    print("State acceptance module ready.")
