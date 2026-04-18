from __future__ import annotations

"""Event quality and confidence scoring."""

from typing import TYPE_CHECKING, List

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from project.events.arbitration import (
        ArbitrationResult,
        arbitrate_events,
        load_compatibility_spec,
        load_precedence_spec,
    )
    from project.events.scoring.confidence import (
        EventConfidenceModel,
        load_event_confidence_model,
        score_detected_events,
        train_event_confidence_model,
    )

EventScoreColumns: List[str] = [
    "severity_score",
    "cleanliness_score",
    "crowding_score",
    "execution_score",
    "novelty_score",
    "microstructure_score",
    "event_tradeability_score",
]

_NOVELTY_LOOKBACK = 20


def _minmax_normalize(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series(0.5, index=series.index)
    return ((series - lo) / (hi - lo)).clip(0.0, 1.0)


def _severity_score(df: pd.DataFrame) -> pd.Series:
    intensity = df["evt_signal_intensity"].astype(float).fillna(0.0)
    out = pd.Series(0.5, index=df.index, dtype=float)
    seen: list[float] = []
    for idx, val in intensity.items():
        seen.append(float(val))
        hist = np.asarray(seen, dtype=float)
        rank = float((hist <= val).mean())
        out.loc[idx] = np.power(np.clip(rank, 0.0, 1.0), 1.5)
    return out


def _cleanliness_score(df: pd.DataFrame) -> pd.Series:
    result = pd.Series(1.0, index=df.index, dtype=float)
    if "enter_ts" not in df.columns:
        return result

    for _, grp in df.groupby("symbol", sort=False):
        times = pd.to_datetime(grp["enter_ts"], utc=True)
        if times.empty:
            continue
        order = np.argsort(times.values)
        sorted_times = times.iloc[order]
        values = sorted_times.values
        window = np.timedelta64(25, "m")
        counts = np.zeros(len(values), dtype=float)
        for i, t in enumerate(values):
            left = np.searchsorted(values, t - window, side="left")
            right = np.searchsorted(values, t, side="right")
            counts[i] = max(0.0, float(right - left - 1))
        max_neighbors = max(float(np.max(counts)), 1.0)
        score = 1.0 - (counts / max_neighbors)
        result.loc[sorted_times.index] = score

    return result.clip(0.0, 1.0)


def _crowding_score(df: pd.DataFrame) -> pd.Series:
    if "basis_z" not in df.columns:
        return pd.Series(0.5, index=df.index)
    abs_basis = df["basis_z"].abs().astype(float).fillna(0.0)
    crowdedness = _minmax_normalize(abs_basis)
    return (1.0 - np.power(crowdedness, 2.0)).clip(0.0, 1.0)


def _execution_score(df: pd.DataFrame) -> pd.Series:
    if "spread_z" not in df.columns:
        return pd.Series(0.5, index=df.index)
    spread = df["spread_z"].astype(float).fillna(0.0).clip(lower=0.0)
    normalized_spread = _minmax_normalize(spread)
    return (1.0 - np.power(normalized_spread, 1.5)).clip(0.0, 1.0)


def _microstructure_score(df: pd.DataFrame) -> pd.Series:
    score = pd.Series(0.5, index=df.index)
    has_depth = "depth_usd" in df.columns
    has_vol = "quote_volume" in df.columns

    if has_depth and has_vol:
        depth = df["depth_usd"].astype(float).fillna(0.0)
        vol = df["quote_volume"].astype(float).fillna(0.0)
        combined = (depth * vol).apply(np.sqrt)
        score = _minmax_normalize(combined)
    elif has_vol:
        vol = df["quote_volume"].astype(float).fillna(0.0)
        score = _minmax_normalize(vol)

    return score.clip(0.0, 1.0)


def _novelty_score(df: pd.DataFrame) -> pd.Series:
    result = pd.Series(0.5, index=df.index)
    intensity = df["evt_signal_intensity"].astype(float).fillna(0.0)
    group_keys = [k for k in ("event_type", "symbol") if k in df.columns]
    if not group_keys:
        return result
    for _, grp in df.groupby(group_keys, sort=False):
        idx = grp.index
        vals = intensity.loc[idx].values
        n = len(vals)
        scores = np.full(n, 0.5)
        alpha = 2.0 / (_NOVELTY_LOOKBACK + 1.0)
        mu = vals[0] if n > 0 else 0.0
        var = 0.0

        for k in range(n):
            val = vals[k]
            mu_prev = mu
            diff = val - mu_prev
            inc = alpha * diff
            mu = mu_prev + inc
            var = (1 - alpha) * (var + diff * inc)
            sigma = np.sqrt(max(var, 0.0))

            if sigma < 1e-10:
                scores[k] = 0.5
            else:
                z = (val - mu_prev) / sigma
                scores[k] = float(1.0 / (1.0 + np.exp(-z)))
        result.loc[idx] = scores
    return result.clip(0.0, 1.0)


def score_event_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        for col in EventScoreColumns:
            out[col] = pd.Series(dtype=float)
        return out

    out["severity_score"] = _severity_score(out).values
    out["cleanliness_score"] = _cleanliness_score(out).values
    out["crowding_score"] = _crowding_score(out).values
    out["execution_score"] = _execution_score(out).values
    out["novelty_score"] = _novelty_score(out).values
    out["microstructure_score"] = _microstructure_score(out).values

    base_viability = (
        out["cleanliness_score"]
        * out["crowding_score"]
        * out["execution_score"]
        * out["microstructure_score"]
    ).apply(lambda x: np.power(x, 0.25))

    out["event_tradeability_score"] = (
        (base_viability * out["severity_score"] * out["novelty_score"])
        .apply(np.sqrt)
        .clip(0.0, 1.0)
    )

    return out


_LAZY_CONFIDENCE_EXPORTS = {
    "EventConfidenceModel",
    "load_event_confidence_model",
    "score_detected_events",
    "train_event_confidence_model",
}

_LAZY_ARBITRATION_EXPORTS = {
    "ArbitrationResult",
    "arbitrate_events",
    "load_compatibility_spec",
    "load_precedence_spec",
}


def __getattr__(name: str):
    if name in _LAZY_CONFIDENCE_EXPORTS:
        from project.events.scoring import confidence as _confidence

        value = getattr(_confidence, name)
        globals()[name] = value
        return value
    if name in _LAZY_ARBITRATION_EXPORTS:
        from project.events import arbitration as _arbitration

        value = getattr(_arbitration, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ArbitrationResult",
    "EventConfidenceModel",
    "EventScoreColumns",
    "arbitrate_events",
    "load_event_confidence_model",
    "load_compatibility_spec",
    "load_precedence_spec",
    "score_detected_events",
    "score_event_frame",
    "train_event_confidence_model",
]
