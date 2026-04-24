
import numpy as np
import pandas as pd


def inject_return_noise(df: pd.DataFrame, noise_scale: float = 1.0) -> pd.DataFrame:
    """
    Injects Gaussian noise into the returns while preserving the volatility structure.
    If noise_scale is high, the expectancy should converge to zero.
    """
    df = df.copy()
    if "close" not in df.columns:
        return df

    returns = df["close"].pct_change().fillna(0)
    std = returns.std()

    # Generate noise with same standard deviation but zero mean
    noise = np.random.normal(0, std * noise_scale, len(returns))

    # Reconstruct prices from noised returns (cumulative product)
    noised_returns = returns + noise
    # We use a simple additive noise to returns, then reconstruct price
    # This is a 'negative control' - if the strategy still works, it's overfitting to features
    # that are not correlated with price movements in this synthetic set.

    initial_price = df["close"].iloc[0]
    df["close"] = initial_price * (1 + noised_returns).cumprod()

    # Update high/low/open to be consistent with noised close
    # (Simple approximation: maintain same relative range)
    ratio = df["close"] / df["close"].shift(1).fillna(initial_price)
    df["open"] = df["open"] * ratio
    df["high"] = df["high"] * ratio
    df["low"] = df["low"] * ratio

    return df

def shuffle_features(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """
    Shuffles feature columns independently to break any temporal or cross-correlation
    with the target returns.
    """
    df = df.copy()
    for col in feature_cols:
        if col in df.columns:
            df[col] = np.random.permutation(df[col].values)
    return df

def generate_negative_control(df: pd.DataFrame, mode: str = "shuffle_features") -> pd.DataFrame:
    """
    Harness for generating negative control datasets.
    """
    if mode == "shuffle_features":
        # Assume features are everything except timestamp and OHLCV
        basic_cols = {"timestamp", "open", "high", "low", "close", "volume", "symbol"}
        feature_cols = [c for c in df.columns if c not in basic_cols]
        return shuffle_features(df, feature_cols)
    elif mode == "noise_returns":
        return inject_return_noise(df)
    return df
