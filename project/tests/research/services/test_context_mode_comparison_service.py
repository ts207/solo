from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from project.research.services import context_mode_comparison_service as svc


def test_compare_context_modes_reports_selected_rows_and_delta(monkeypatch) -> None:
    def fake_evaluate(hypotheses, features, *, min_sample_size, use_context_quality):
        assert min_sample_size == 10
        assert len(hypotheses) == 1
        assert not features.empty
        if use_context_quality:
            return pd.DataFrame(
                [
                    {
                        "valid": True,
                        "n": 12,
                        "validation_n_obs": 4,
                        "test_n_obs": 3,
                        "t_stat": 1.5,
                        "robustness_score": 0.7,
                        "stress_score": 0.4,
                    }
                ]
            )
        return pd.DataFrame(
            [
                {
                    "valid": True,
                    "n": 20,
                    "validation_n_obs": 7,
                    "test_n_obs": 5,
                    "t_stat": 1.2,
                    "robustness_score": 0.6,
                    "stress_score": 0.5,
                }
            ]
        )

    monkeypatch.setattr(svc, "evaluate_hypothesis_batch", fake_evaluate)

    comparison = svc.compare_context_modes(
        hypotheses=["dummy_hypothesis"],
        features=pd.DataFrame({"close": [100.0, 101.0]}),
        min_sample_size=10,
    )

    assert comparison["schema_version"] == "context_mode_comparison_v1"
    assert comparison["hard_label"]["selected"]["n"] == 20
    assert comparison["confidence_aware"]["selected"]["n"] == 12
    assert comparison["delta"]["n"] == -8.0
    assert comparison["delta"]["robustness_score"] == pytest.approx(0.1)
    assert comparison["selection_changed"] is False
    assert comparison["selection_outcome_changed"] is False


def test_compare_context_modes_reports_selection_outcome_change(monkeypatch) -> None:
    def fake_evaluate(hypotheses, features, *, min_sample_size, use_context_quality):
        if use_context_quality:
            return pd.DataFrame(
                [
                    {
                        "valid": False,
                        "hypothesis_id": "hyp_1",
                        "n": 12,
                        "invalid_reason": "min_sample_size",
                    }
                ]
            )
        return pd.DataFrame(
            [
                {
                    "valid": True,
                    "hypothesis_id": "hyp_1",
                    "n": 20,
                    "invalid_reason": None,
                }
            ]
        )

    monkeypatch.setattr(svc, "evaluate_hypothesis_batch", fake_evaluate)

    comparison = svc.compare_context_modes(
        hypotheses=["dummy_hypothesis"],
        features=pd.DataFrame({"close": [100.0, 101.0]}),
        min_sample_size=10,
    )

    assert comparison["selection_changed"] is False
    assert comparison["selection_outcome_changed"] is True


def test_write_context_mode_comparison_report(tmp_path: Path) -> None:
    out_path = svc.write_context_mode_comparison_report(
        out_path=tmp_path / "out" / "context_mode_comparison.json",
        comparison={
            "schema_version": "context_mode_comparison_v1",
            "hard_label": {"evaluated_rows": 1, "selected": {"n": 20}},
            "confidence_aware": {"evaluated_rows": 1, "selected": {"n": 12}},
            "delta": {"n": -8.0},
        },
    )

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["delta"]["n"] == -8.0


def test_build_context_mode_comparison_payload_uses_prepared_search_frame(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_load_search_feature_frame(*, run_id, symbols, timeframe, data_root):
        captured["run_id"] = run_id
        captured["symbols"] = list(symbols)
        captured["timeframe"] = timeframe
        captured["data_root"] = data_root
        return pd.DataFrame({"timestamp": [1], "symbol": ["BTCUSDT"], "event_vol_shock": [True]})

    def fake_generate(*, search_space_path, features):
        assert features is not None
        assert "event_vol_shock" in features.columns
        return ["dummy_hypothesis"], {"counts": {"generated": 1, "feasible": 1, "rejected": 0}}

    def fake_compare(*, hypotheses, features, min_sample_size):
        assert hypotheses == ["dummy_hypothesis"]
        assert "event_vol_shock" in features.columns
        assert min_sample_size == 30
        return {
            "schema_version": "context_mode_comparison_v1",
            "hard_label": {},
            "confidence_aware": {},
            "delta": {},
        }

    monkeypatch.setattr(svc, "load_search_feature_frame", fake_load_search_feature_frame)
    monkeypatch.setattr(svc, "generate_hypotheses_with_audit", fake_generate)
    monkeypatch.setattr(svc, "compare_context_modes", fake_compare)

    payload = svc.build_context_mode_comparison_payload(
        data_root=Path("/tmp/data"),
        run_id="bench_vol_shock_btc_2024q1",
        symbols=["BTCUSDT"],
        timeframe="5m",
        search_space_path=Path("spec/search/search_benchmark_vol_shock.yaml"),
    )

    assert payload["run_id"] == "bench_vol_shock_btc_2024q1"
    assert captured["symbols"] == ["BTCUSDT"]
