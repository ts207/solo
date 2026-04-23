import numpy as np
import pandas as pd


def ensure_timestamp_index(df: pd.DataFrame, freq: str = "5min") -> pd.DataFrame:
    if "timestamp" not in df.columns and df.index.name == "timestamp":
        df = df.reset_index()
    if "timestamp" not in df.columns:
        n = len(df)
        df = df.copy()
        df.insert(0, "timestamp", pd.date_range("2024-01-01", periods=n, freq=freq, tz="UTC"))
    return df


def smooth_transition(
    arr: np.ndarray,
    injection_point: int,
    duration: int,
    target_value: float,
    ramp_bars: int = 5,
) -> np.ndarray:
    result = arr.copy()
    pre_value = arr[max(0, injection_point - 1)]
    ramp_start = max(0, injection_point - ramp_bars)
    ramp_end = min(len(arr), injection_point + duration + ramp_bars)
    for i in range(ramp_start, injection_point):
        alpha = (i - ramp_start) / max(1, injection_point - ramp_start)
        result[i] = pre_value + alpha * (target_value - pre_value)
    for i in range(injection_point, min(len(arr), injection_point + duration)):
        result[i] = target_value
    for i in range(injection_point + duration, ramp_end):
        alpha = (ramp_end - i) / max(1, ramp_end - injection_point - duration)
        result[i] = target_value + alpha * (pre_value - target_value)
    return result


__all__ = [
    "ensure_timestamp_index",
    "smooth_transition",
]
