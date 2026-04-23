import pytest
import pandas as pd
from project.eval.sensitivity import (
    perturb_exit_parameters,
    perturb_delay_bars,
    run_lightweight_eval,
    append_sensitivity_to_report,
)


def test_perturb_exit_parameters_dsl():
    blueprint = {"exit": {"stop_value": 2.0, "target_value": 4.0}}
    pct = 0.1
    perturbed = perturb_exit_parameters(blueprint, pct=pct)
    assert len(perturbed) == 4

    # Check bounds
    stops = [b["exit"]["stop_value"] for b in perturbed]
    targets = [b["exit"]["target_value"] for b in perturbed]

    assert min(stops) == 1.8
    assert max(stops) == 2.2
    assert min(targets) == 3.6
    assert max(targets) == 4.4


def test_perturb_exit_parameters_params():
    blueprint = {"params": {"t_stop": 10.0, "t_target": 20.0}}
    pct = 0.1
    perturbed = perturb_exit_parameters(blueprint, pct=pct)
    assert len(perturbed) == 4

    stops = [b["params"]["t_stop"] for b in perturbed]
    targets = [b["params"]["t_target"] for b in perturbed]

    assert min(stops) == 9.0
    assert max(stops) == 11.0
    assert min(targets) == 18.0
    assert max(targets) == 22.0


def test_perturb_delay_bars_dsl():
    blueprint = {"entry": {"delay_bars": 1}}
    delays = [0, 1, 2, 3]
    perturbed = perturb_delay_bars(blueprint, delays=delays)
    assert len(perturbed) == 4
    for i, delay in enumerate(delays):
        assert perturbed[i]["entry"]["delay_bars"] == delay


def test_run_lightweight_eval():
    blueprints = [
        {"id": 1, "win_rate": 0.5, "pnl": 100},
        {"id": 2, "win_rate": 0.6, "pnl": 150},
        {"id": 3, "win_rate": 0.4, "pnl": 50},
    ]

    def dummy_evaluator(bp):
        return {"win_rate": bp["win_rate"], "pnl": bp["pnl"]}

    variances = run_lightweight_eval(blueprints, dummy_evaluator)

    assert "win_rate_variance" in variances
    assert "pnl_variance" in variances

    # Variance of [0.5, 0.6, 0.4] is 0.01
    assert abs(variances["win_rate_variance"] - 0.01) < 1e-5
    # Variance of [100, 150, 50] is 2500
    assert abs(variances["pnl_variance"] - 2500) < 1e-5


def test_append_sensitivity_to_report():
    blueprint = {"exit": {"stop_value": 1.0, "target_value": 2.0}, "entry": {"delay_bars": 1}}

    def dummy_evaluator(bp):
        # Dummy logic depending on delay and stops
        delay = bp.get("entry", {}).get("delay_bars", 1)
        stop = bp.get("exit", {}).get("stop_value", 1.0)
        return {"win_rate": 0.5 - (delay * 0.05), "pnl": 100 * stop - (delay * 10)}

    df = append_sensitivity_to_report(blueprint, dummy_evaluator, pct=0.1, delays=[0, 1, 2])

    # 1 baseline + 4 exit + 3 delay = 8 rows
    assert len(df) == 8
    assert "perturbation" in df.columns
    assert "win_rate" in df.columns
    assert "pnl" in df.columns
    assert list(df["type"]) == [
        "baseline",
        "exit",
        "exit",
        "exit",
        "exit",
        "delay",
        "delay",
        "delay",
    ]
