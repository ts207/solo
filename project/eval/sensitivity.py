import copy
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd


def perturb_exit_parameters(blueprint: dict[str, Any], pct: float = 0.1) -> list[dict[str, Any]]:
    """
    Perturb exit parameters (t_stop/stop_value and t_target/target_value) by +/- pct.
    Returns a list of perturbed blueprints.
    """
    perturbed = []

    base_stop = 1.0
    base_target = 1.0
    is_dsl = False

    if "exit" in blueprint and "stop_value" in blueprint["exit"]:
        is_dsl = True
        base_stop = float(blueprint["exit"]["stop_value"])
        base_target = float(blueprint["exit"]["target_value"])
    elif "params" in blueprint and "t_stop" in blueprint["params"]:
        base_stop = float(blueprint["params"]["t_stop"])
        base_target = float(blueprint["params"]["t_target"])
    else:
        # Default behavior assuming DSL
        is_dsl = True
        if "exit" in blueprint:
            base_stop = float(blueprint["exit"].get("stop_value", 1.0))
            base_target = float(blueprint["exit"].get("target_value", 1.0))

    multipliers = [
        (1.0 - pct, 1.0 - pct),
        (1.0 - pct, 1.0 + pct),
        (1.0 + pct, 1.0 - pct),
        (1.0 + pct, 1.0 + pct),
    ]

    for m_stop, m_target in multipliers:
        bp_copy = copy.deepcopy(blueprint)
        if is_dsl:
            if "exit" not in bp_copy:
                bp_copy["exit"] = {}
            bp_copy["exit"]["stop_value"] = base_stop * m_stop
            bp_copy["exit"]["target_value"] = base_target * m_target
        else:
            if "params" not in bp_copy:
                bp_copy["params"] = {}
            bp_copy["params"]["t_stop"] = base_stop * m_stop
            bp_copy["params"]["t_target"] = base_target * m_target

        perturbed.append(bp_copy)

    return perturbed


def perturb_delay_bars(blueprint: dict[str, Any], delays: list[int] = None) -> list[dict[str, Any]]:
    """
    Perturb delay_bars on a blueprint to evaluate delay sensitivity.
    """
    if delays is None:
        delays = [0, 1, 2, 3]

    perturbed = []

    is_dsl = ("entry" in blueprint and "delay_bars" in blueprint["entry"]) or ("entry" in blueprint)
    if not is_dsl and "params" in blueprint and "delay_bars" in blueprint["params"]:
        pass  # Not DSL, but uses params
    elif not is_dsl and "entry" not in blueprint and "params" not in blueprint:
        is_dsl = True  # Default

    for delay in delays:
        bp_copy = copy.deepcopy(blueprint)
        if is_dsl:
            if "entry" not in bp_copy:
                bp_copy["entry"] = {}
            bp_copy["entry"]["delay_bars"] = delay
        else:
            if "params" not in bp_copy:
                bp_copy["params"] = {}
            bp_copy["params"]["delay_bars"] = delay
        perturbed.append(bp_copy)

    return perturbed


def run_lightweight_eval(
    blueprints: list[dict[str, Any]], evaluator_fn: Callable[[dict[str, Any]], dict[str, float]]
) -> dict[str, float]:
    """
    Run lightweight evaluation with perturbed parameters and return the variance
    in performance metrics (e.g., win_rate, pnl).
    """
    results = []
    for bp in blueprints:
        metrics = evaluator_fn(bp)
        results.append(metrics)

    if not results:
        return {}

    df = pd.DataFrame(results)

    variances = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        # Use ddof=1 for sample variance. If length is 1, it will be NaN.
        val = df[col].var(ddof=1)
        variances[f"{col}_variance"] = float(val) if not pd.isna(val) else 0.0

    return variances


def append_sensitivity_to_report(
    blueprint: dict[str, Any],
    evaluator_fn: Callable[[dict[str, Any]], dict[str, float]],
    pct: float = 0.1,
    delays: list[int] = None,
) -> pd.DataFrame:
    """
    Perform both sweeps, run evaluations, and return a DataFrame that can be
    appended to an evaluation report.
    """
    if delays is None:
        delays = [0, 1, 2, 3]

    report_rows = []

    # Baseline
    base_metrics = evaluator_fn(blueprint)
    base_row = {"perturbation": "baseline", "type": "baseline"}
    base_row.update(base_metrics)
    report_rows.append(base_row)

    # Exits
    exit_bps = perturb_exit_parameters(blueprint, pct=pct)
    for i, bp in enumerate(exit_bps):
        metrics = evaluator_fn(bp)
        row = {"perturbation": f"exit_sweep_{i}", "type": "exit"}
        row.update(metrics)
        report_rows.append(row)

    # Delays
    delay_bps = perturb_delay_bars(blueprint, delays=delays)
    for i, bp in enumerate(delay_bps):
        metrics = evaluator_fn(bp)
        row = {"perturbation": f"delay_{delays[i]}", "type": "delay"}
        row.update(metrics)
        report_rows.append(row)

    return pd.DataFrame(report_rows)
