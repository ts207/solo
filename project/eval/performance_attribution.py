import numpy as np
import pandas as pd


def calculate_regime_metrics(
    df: pd.DataFrame, regime_col: str = "vol_regime", pnl_col: str = "pnl"
) -> pd.DataFrame:
    """
    Calculate performance metrics grouped by market regime.

    Args:
        df: DataFrame containing the regime column and pnl column
        regime_col: Column name for the regime grouping
        pnl_col: Column name for the P&L values

    Returns:
        DataFrame with metrics per regime as index
    """
    if regime_col not in df.columns:
        raise ValueError(f"Column '{regime_col}' not found in DataFrame")
    if pnl_col not in df.columns:
        raise ValueError(f"Column '{pnl_col}' not found in DataFrame")

    def aggregate_metrics(group):
        pnl = group[pnl_col]
        total_pnl = pnl.sum()
        count = len(pnl)

        # Simple Sharpe: mean/std.
        # In a real system, we'd annualize based on frequency.
        mean_pnl = pnl.mean()
        std_pnl = pnl.std()
        sharpe = (mean_pnl / std_pnl) if std_pnl > 0 and not np.isnan(std_pnl) else 0.0

        # Calculate Max Drawdown
        cum_pnl = pd.concat(
            [pd.Series([0.0], dtype=float), pnl.cumsum().reset_index(drop=True)],
            ignore_index=True,
        )
        running_max = cum_pnl.cummax()
        drawdown = running_max - cum_pnl
        max_drawdown = drawdown.max()

        return pd.Series(
            {
                "total_pnl": total_pnl,
                "mean_pnl": mean_pnl,
                "std_pnl": std_pnl,
                "sharpe_ratio": sharpe,
                "max_drawdown": max_drawdown,
                "count": count,
            }
        )

    metrics = df.groupby(regime_col).apply(aggregate_metrics, include_groups=False)
    return metrics
