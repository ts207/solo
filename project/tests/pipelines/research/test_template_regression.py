from __future__ import annotations

import pandas as pd

from project.research.template_regression import (
    build_run_summary,
    compare_summaries,
    summarize_phase2_event,
)


def test_summarize_phase2_event_extracts_template_action_direction(tmp_path):
    event_path = tmp_path / "phase2_candidates.parquet"
    pd.DataFrame(
        [
            {
                "template_verb": "mean_reversion",
                "action": "enter_short_market",
                "direction_rule": "contrarian",
            },
            {
                "template_verb": "continuation",
                "action": "enter_long_market",
                "direction_rule": "directional",
            },
        ]
    ).to_parquet(event_path, index=False)

    summary = summarize_phase2_event(event_path)
    assert summary["rows"] == 2
    assert summary["templates"] == ["continuation", "mean_reversion"]
    assert summary["actions"] == ["enter_long_market", "enter_short_market"]
    assert summary["direction_rules"] == ["contrarian", "directional"]
    assert summary["by_template"]["mean_reversion"] == 1


def test_compare_summaries_reports_differences():
    baseline = {
        "events": {
            "VOL_SHOCK": {"rows": 2, "templates": ["a"], "actions": ["x"], "direction_rules": ["d"]}
        }
    }
    current = {
        "events": {
            "VOL_SHOCK": {"rows": 3, "templates": ["a"], "actions": ["y"], "direction_rules": ["d"]}
        }
    }
    failures = compare_summaries(baseline=baseline, current=current)
    assert any("VOL_SHOCK:rows" in row for row in failures)
    assert any("VOL_SHOCK:actions" in row for row in failures)


def test_build_run_summary_reads_event_parquet(tmp_path):
    run_id = "r1"
    event = "VOL_SHOCK"
    path = tmp_path / "reports" / "phase2" / run_id / event
    path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "template_verb": "continuation",
                "action": "enter_long_market",
                "direction_rule": "directional",
            }
        ]
    ).to_parquet(
        path / "phase2_candidates.parquet",
        index=False,
    )

    out = build_run_summary(data_root=tmp_path, run_id=run_id, events=[event])
    assert out["events"][event]["rows"] == 1
