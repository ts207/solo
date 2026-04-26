from __future__ import annotations

import logging
from typing import Any, Literal

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)
from project.core.constants import BARS_PER_YEAR_BY_TIMEFRAME

_DEFAULT_RANDOM_SEED = 0


def _max_drawdown(equity: np.ndarray, *, fallback_absolute: bool = False) -> float:
    path = np.asarray(equity, dtype=float)
    path = path[np.isfinite(path)]
    if path.size == 0:
        return 0.0
    peak = np.maximum.accumulate(path)
    drawdown = peak - path
    if fallback_absolute:
        dd = drawdown.copy()
        positive_peak = peak > 0.0
        dd[positive_peak] = drawdown[positive_peak] / peak[positive_peak]
    else:
        safe_peak = np.where(peak > 0.0, peak, 1.0)
        dd = drawdown / safe_peak
    return float(np.max(dd))


def block_bootstrap_pnl(
    pnl_series: pd.Series,
    block_size_bars: int = 576,
    n_iterations: int = 1000,
    random_seed: int = _DEFAULT_RANDOM_SEED,
    periods_per_year: int | None = None,
    pnl_mode: Literal["dollar", "return"] = "dollar",
    capital_base: float = 1.0,
) -> dict[str, float]:
    pnl = pd.to_numeric(pnl_series, errors="coerce").dropna().values
    n = len(pnl)
    if n < block_size_bars or n == 0:
        return {}

    if periods_per_year is None:
        periods_per_year = BARS_PER_YEAR_BY_TIMEFRAME["5m"]

    capital_base_value = float(capital_base)
    if pnl_mode == "dollar" and (not np.isfinite(capital_base_value) or capital_base_value <= 0.0):
        raise ValueError("capital_base must be finite and > 0 for dollar-mode bootstrap drawdown")

    rng = np.random.default_rng(random_seed)
    n_blocks = int(np.ceil(n / block_size_bars))
    annualized_returns = []
    max_drawdowns = []

    for _ in range(n_iterations):
        # Circular block bootstrap
        start_indices = rng.integers(0, n, size=n_blocks)
        blocks = []
        for start in start_indices:
            if start + block_size_bars <= n:
                blocks.append(pnl[start : start + block_size_bars])
            else:
                # Wrap around
                first_part = pnl[start:]
                second_part = pnl[:block_size_bars - len(first_part)]
                blocks.append(np.concatenate([first_part, second_part]))

        bootstrapped_pnl = np.concatenate(blocks)[:n]
        mean_ret = np.mean(bootstrapped_pnl)
        ann_ret = mean_ret * periods_per_year
        annualized_returns.append(ann_ret)

        if pnl_mode == "return":
            equity = np.concatenate([[1.0], np.cumprod(1.0 + bootstrapped_pnl)])
            max_drawdowns.append(_max_drawdown(equity))
        else:
            # Dollar PnL is additive around a strictly positive capital base.
            # Starting the path at zero mixes absolute and percentage drawdown
            # units whenever the running peak is non-positive.
            equity = np.concatenate(
                [[capital_base_value], capital_base_value + np.cumsum(bootstrapped_pnl)]
            )
            max_drawdowns.append(_max_drawdown(equity))

    return {
        "bootstrap_return_p05": float(np.percentile(annualized_returns, 5)),
        "bootstrap_return_p50": float(np.percentile(annualized_returns, 50)),
        "bootstrap_return_p95": float(np.percentile(annualized_returns, 95)),
        "bootstrap_drawdown_p05": float(np.percentile(max_drawdowns, 5)),
        "bootstrap_drawdown_p50": float(np.percentile(max_drawdowns, 50)),
        "bootstrap_drawdown_p95": float(np.percentile(max_drawdowns, 95)),
        "bootstrap_random_seed": int(random_seed),
        "bootstrap_pnl_mode": str(pnl_mode),
        "bootstrap_capital_base": float(capital_base_value if pnl_mode == "dollar" else 1.0),
    }


def simulate_parameter_perturbation(
    pnl_series: pd.Series,
    noise_std_dev: float = 0.05,
    n_iterations: int = 100,
    random_seed: int = _DEFAULT_RANDOM_SEED,
    periods_per_year: int | None = None,
) -> dict[str, float]:
    pnl = pd.to_numeric(pnl_series, errors="coerce").dropna().values
    if len(pnl) == 0:
        return {}

    if periods_per_year is None:
        periods_per_year = BARS_PER_YEAR_BY_TIMEFRAME["5m"]

    base_std = np.std(pnl)
    if base_std == 0:
        return {}

    rng = np.random.default_rng(random_seed)
    annualized_returns = []

    # Improved perturbation:
    # 1. Measurement noise (white noise)
    # 2. Parameter sensitivity proxy (trend/vol drift)
    # We simulate "what if" the realized pnl had slightly worse fill or slightly different regime.

    for _ in range(n_iterations):
        # White noise component (execution noise)
        noise = rng.normal(0, base_std * noise_std_dev, size=len(pnl))

        # Drift component (regime shift / parameter decay)
        # Random walk drift scaled to be subtle
        drift = np.cumsum(rng.normal(0, base_std * noise_std_dev * 0.1, size=len(pnl)))

        perturbed_pnl = pnl + noise + drift
        mean_ret = np.mean(perturbed_pnl)
        annualized_returns.append(mean_ret * periods_per_year)

    baseline_ann = float(np.mean(pnl) * periods_per_year)
    p05 = float(np.percentile(annualized_returns, 5))
    return {
        "perturbation_return_p05": p05,
        "perturbation_return_p50": float(np.percentile(annualized_returns, 50)),
        "perturbation_return_p95": float(np.percentile(annualized_returns, 95)),
        "perturbation_degradation_pct": float(p05 / baseline_ann - 1.0)
        if baseline_ann > 0
        else 0.0,
        "fraction_positive": float(np.mean([r > 0.0 for r in annualized_returns])),
        "perturbation_random_seed": int(random_seed),
    }


def analyze_regime_segmentation(
    pnl_series: pd.Series,
    volatility_series: pd.Series,
    q_high: float = 0.75,
    q_low: float = 0.25,
    periods_per_year: int | None = None,
) -> dict[str, float]:
    pnl = pd.to_numeric(pnl_series, errors="coerce")
    vol = pd.to_numeric(volatility_series, errors="coerce")
    df = pd.DataFrame({"pnl": pnl, "vol": vol}).dropna()
    if df.empty:
        return {}

    if periods_per_year is None:
        periods_per_year = BARS_PER_YEAR_BY_TIMEFRAME["5m"]

    high_thresh = df["vol"].quantile(q_high)
    low_thresh = df["vol"].quantile(q_low)
    high_vol_pnl = df.loc[df["vol"] >= high_thresh, "pnl"]
    low_vol_pnl = df.loc[df["vol"] <= low_thresh, "pnl"]
    mid_vol_pnl = df.loc[(df["vol"] > low_thresh) & (df["vol"] < high_thresh), "pnl"]

    def _ann_ret(subset: pd.Series) -> float:
        return 0.0 if subset.empty else float(subset.mean() * periods_per_year)

    return {
        "high_vol_regime_annualized": _ann_ret(high_vol_pnl),
        "low_vol_regime_annualized": _ann_ret(low_vol_pnl),
        "mid_vol_regime_annualized": _ann_ret(mid_vol_pnl),
        "high_vol_exposure_fraction": float(len(high_vol_pnl) / len(df)),
    }


_DEFAULT_COST_STRESS_MULTIPLIERS: tuple[float, ...] = (1.0, 2.0, 5.0, 10.0)


def evaluate_structural_robustness(
    base_pnl: pd.Series,
    *,
    returns_raw: pd.Series | None = None,
    costs_bps: pd.Series | None = None,
    entry_delay_pnl: pd.Series | None = None,
    spread_widening_factor: float = 2.0,
    cost_multiplier: float = 2.0,
    cost_stress_multipliers: tuple[float, ...] = _DEFAULT_COST_STRESS_MULTIPLIERS,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    base_pnl = pd.to_numeric(base_pnl, errors="coerce").dropna()
    if base_pnl.empty:
        return {
            "structural_robustness_score": 0.0,
            "sign_retention_rate": 0.0,
            "robustness_panel_complete": False,
        }

    base_mean = float(base_pnl.mean())
    base_sign = np.sign(base_mean)

    if returns_raw is not None and costs_bps is not None:
        raw = pd.to_numeric(returns_raw, errors="coerce")
        cst = pd.to_numeric(costs_bps, errors="coerce").fillna(0.0).clip(lower=0.0)

        for mult in cost_stress_multipliers:
            stressed_pnl = raw - cst * float(mult) / 10000.0
            stressed_mean = float(stressed_pnl.mean())
            # Key like "cost_stress_1x_pass", "cost_stress_2x_pass", etc.
            mult_label = int(mult) if float(mult) == int(mult) else str(mult).replace(".", "p")
            results[f"cost_stress_{mult_label}x_pass"] = bool(stressed_mean > 0)
            results[f"cost_stress_{mult_label}x_retention"] = (
                float(stressed_mean / base_mean) if base_mean != 0 else 0.0
            )

        # Backward compat: cost_stress_pass = 2× result (or use the explicit cost_multiplier if provided)
        # If the user passed cost_multiplier but not cost_stress_multipliers, we use it for cost_stress_pass
        legacy_stressed_pnl = raw - cst * cost_multiplier / 10000.0
        legacy_stressed_mean = float(legacy_stressed_pnl.mean())
        results["cost_stress_retention"] = (
            float(legacy_stressed_mean / base_mean) if base_mean != 0 else 0.0
        )
        results["cost_stress_sign_match"] = bool(np.sign(legacy_stressed_mean) == base_sign)
        results["cost_stress_pass"] = bool(legacy_stressed_mean > 0)

    if entry_delay_pnl is not None:
        entry_delay_pnl = pd.to_numeric(entry_delay_pnl, errors="coerce").dropna()
        if not entry_delay_pnl.empty:
            delay_mean = float(entry_delay_pnl.mean())
            results["delay_stress_retention"] = (
                float(delay_mean / base_mean) if base_mean != 0 else 0.0
            )
            results["delay_stress_sign_match"] = bool(np.sign(delay_mean) == base_sign)
            results["delay_stress_pass"] = bool(delay_mean > 0)

    means = [base_mean]
    if "cost_stress_pass" in results:
        means.append(legacy_stressed_mean)
    if "delay_stress_pass" in results:
        delay_mean = float(entry_delay_pnl.mean()) if entry_delay_pnl is not None else 0.0
        means.append(delay_mean)

    results["sign_retention_rate"] = float(np.mean([np.sign(m) == base_sign for m in means]))
    score = 0.0
    if results.get("cost_stress_pass"):
        score += 0.4
    if results.get("delay_stress_pass"):
        score += 0.4
    if results.get("sign_retention_rate", 0) > 0.9:
        score += 0.2
    results["structural_robustness_score"] = float(score)
    results["robustness_panel_complete"] = bool(
        "cost_stress_pass" in results or "delay_stress_pass" in results
    )
    return results


def compute_rank_retention(original_ranks: pd.Series, perturbed_ranks: pd.Series) -> float:
    if len(original_ranks) < 2:
        return 1.0
    try:
        from scipy.stats import kendalltau

        tau, _ = kendalltau(original_ranks, perturbed_ranks)
    except ImportError:
        from project.core.stats import _StatsCompat

        tau, _ = _StatsCompat.kendalltau(original_ranks, perturbed_ranks)
    return float(tau) if np.isfinite(tau) else 0.0


def evaluate_structural_breaks(
    pnl_series: pd.Series,
    timestamps: pd.Series,
    *,
    min_samples: int = 50,
) -> dict[str, Any]:
    if len(pnl_series) < min_samples * 2:
        return {"status": "insufficient_data", "pass": True}

    df = pd.DataFrame({"pnl": pnl_series, "ts": timestamps}).dropna().sort_values("ts")
    if len(df) < min_samples * 2:
        return {"status": "insufficient_data", "pass": True}
    mid_idx = len(df) // 2
    pre = df.iloc[:mid_idx]["pnl"]
    post = df.iloc[mid_idx:]["pnl"]
    pre_mean = pre.mean()
    post_mean = post.mean()
    mean_ratio = post_mean / pre_mean if abs(pre_mean) > 1e-9 else 1.0
    direction_flip = bool(
        np.sign(pre_mean) != np.sign(post_mean) and abs(pre_mean) > 1e-6 and abs(post_mean) > 1e-6
    )
    window = max(20, len(df) // 3)
    rolling_mean = df["pnl"].rolling(window).mean()
    stability = (
        float(rolling_mean.std() / abs(df["pnl"].mean())) if abs(df["pnl"].mean()) > 1e-9 else 1.0
    )
    return {
        "pre_mean": float(pre_mean),
        "post_mean": float(post_mean),
        "pre_post_ratio": float(mean_ratio),
        "structural_break_detected": bool(direction_flip or stability > 2.0),
        "rolling_stability_z": float(stability),
        "pass": not (direction_flip or (stability > 3.0)),
    }
