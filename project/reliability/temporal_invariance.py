from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Callable, Sequence, Any


@dataclass(frozen=True)
class PerturbationSpec:
    name: str
    mutate: Callable[[pd.DataFrame | pd.Series, int], pd.DataFrame | pd.Series]


@dataclass(frozen=True)
class InvarianceCheckSpec:
    name: str
    build_output: Callable[[pd.DataFrame | pd.Series], Any]
    extract_comparable_prefix: Callable[[Any, int, int], Any]
    cutoffs: Sequence[int]
    warmup: int
    perturbations: Sequence[PerturbationSpec]


def assert_future_invariance(
    data: pd.DataFrame | pd.Series,
    spec: InvarianceCheckSpec,
):
    """
    Assert that changes in data after a cutoff 'T' do not affect results up to 'T'.
    """
    base = spec.build_output(data)

    for cutoff in spec.cutoffs:
        for perturb in spec.perturbations:
            changed = perturb.mutate(data.copy(), cutoff)
            alt = spec.build_output(changed)

            expected = spec.extract_comparable_prefix(base, cutoff, spec.warmup)
            actual = spec.extract_comparable_prefix(alt, cutoff, spec.warmup)

            try:
                if isinstance(expected, (pd.Series, pd.DataFrame)):
                    pd.testing.assert_frame_equal(expected, actual) if isinstance(
                        expected, pd.DataFrame
                    ) else pd.testing.assert_series_equal(expected, actual)
                else:
                    assert expected == actual
            except AssertionError as e:
                raise AssertionError(
                    f"Temporal Invariance Failure [{spec.name}] at cutoff {cutoff} "
                    f"with perturbation '{perturb.name}':\n{e}"
                ) from e


# --- Common Perturbations ---


def future_price_spike(df: pd.DataFrame, cutoff_idx: int) -> pd.DataFrame:
    if "close" in df.columns:
        df.loc[cutoff_idx + 1 :, "close"] *= 10.0
    return df


def future_missing_data(df: pd.DataFrame, cutoff_idx: int) -> pd.DataFrame:
    df.iloc[cutoff_idx + 1 :] = np.nan
    return df


def future_noise(df: pd.DataFrame, cutoff_idx: int) -> pd.DataFrame:
    noise = np.random.normal(0, 1, len(df) - (cutoff_idx + 1))
    if isinstance(df, pd.DataFrame):
        for col in df.select_dtypes(include=[np.number]).columns:
            df.loc[cutoff_idx + 1 :, col] += noise
    else:
        df.iloc[cutoff_idx + 1 :] += noise
    return df


STANDARD_PERTURBATIONS = [
    PerturbationSpec("price_spike", future_price_spike),
    PerturbationSpec("missing_data", future_missing_data),
    PerturbationSpec("noise", future_noise),
]
