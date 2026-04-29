import pandas as pd

from project.scripts.update_results_index import prepare_results, select_best_row


def _base_row(**overrides):
    row = {
        "source_file": "event_stats",
        "run_id": "run",
        "program_id": "program",
        "event_type": "TEST_EVENT",
        "direction": "",
        "horizon": "24b",
        "template_id": "mean_reversion",
        "t_stat": None,
        "robustness_score": None,
        "n_events": None,
        "n": None,
        "q_value": None,
        "after_cost_expectancy_per_trade": 1.23,
        "mean_return_bps": None,
        "is_discovery": False,
    }
    row.update(overrides)
    return row


def test_prepare_results_hides_expectancy_without_evaluable_metrics():
    df = pd.DataFrame(
        [
            _base_row(program_id="summary_only"),
            _base_row(event_type="EMPTY_EVENT", program_id="explicit_no_events", n_events=0),
            _base_row(
                source_file="eval_results",
                program_id="evaluated",
                direction="long",
                t_stat=0.5,
                robustness_score=0.4,
                n_events=20,
                q_value=1.0,
                after_cost_expectancy_per_trade=0.002,
            ),
        ]
    )

    out = prepare_results(df)

    summary = out[out["program_id"] == "summary_only"].iloc[0]
    assert summary["status"] == "not evaluated"
    assert pd.isna(summary["exp_bps"])

    no_events = out[out["program_id"] == "explicit_no_events"].iloc[0]
    assert no_events["status"] == "no events"
    assert pd.isna(no_events["exp_bps"])

    evaluated = out[out["program_id"] == "evaluated"].iloc[0]
    assert evaluated["status"] == "below gate"
    assert evaluated["exp_bps"] == 20.0


def test_select_best_row_prefers_promoted_evaluated_metrics_over_summary():
    df = pd.DataFrame(
        [
            _base_row(program_id="campaign_pe_oi-spike-negative"),
            _base_row(
                source_file="eval_results",
                program_id="campaign_pe_oi-spike-negative",
                direction="long",
                t_stat=2.28,
                robustness_score=0.84,
                n_events=53,
                q_value=0.01,
                after_cost_expectancy_per_trade=0.005,
            ),
        ]
    )

    out = prepare_results(df)
    best = select_best_row(out)

    assert best["direction"] == "long"
    assert best["status"] == "**PROMOTED**"
    assert best["exp_bps"] == 50.0


def test_prepare_results_applies_current_status_override():
    df = pd.DataFrame(
        [
            _base_row(
                event_type="CLIMAX_VOLUME_BAR",
                program_id="single_event_climax_volu_20260428T212745Z_386e107171",
                direction="long",
                horizon="24b",
                template_id="exhaustion_reversal",
                t_stat=2.2495,
                robustness_score=0.7041,
                n_events=309,
                q_value=0.0122,
                after_cost_expectancy_per_trade=0.0026,
            ),
        ]
    )

    out = prepare_results(df)

    assert out.iloc[0]["status"] == "parked: forward failed"
