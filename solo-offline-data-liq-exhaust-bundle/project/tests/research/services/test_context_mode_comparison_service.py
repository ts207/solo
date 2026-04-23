from __future__ import annotations

import pandas as pd

from project.research.services import context_mode_comparison_service
from project.research.services.context_mode_comparison_service import (
    _expected_event_ids_from_search_space_doc,
    compare_context_modes,
)


def test_expected_event_ids_from_search_space_doc_expands_trigger_events() -> None:
    search_space_doc = {
        "triggers": {
            "events": ["VOL_SPIKE", "VOL_SHOCK"],
        }
    }

    observed = _expected_event_ids_from_search_space_doc(search_space_doc)

    assert set(observed) >= {"VOL_SPIKE", "VOL_SHOCK"}


def test_compare_context_modes_returns_empty_payload_for_missing_features() -> None:
    observed = compare_context_modes(hypotheses=[], features=pd.DataFrame())

    assert observed["hard_label"]["evaluated_rows"] == 0
    assert observed["confidence_aware"]["evaluated_rows"] == 0
    assert observed["selection_changed"] is False


def test_build_context_mode_payload_threads_fixture_override(tmp_path, monkeypatch) -> None:
    search_space_path = tmp_path / "search.yaml"
    search_space_path.write_text(
        "triggers:\n"
        "  events:\n"
        "    - VOL_SPIKE\n",
        encoding="utf-8",
    )

    observed = {}

    def fake_load_search_feature_frame(**kwargs):
        observed["event_registry_override"] = kwargs.get("event_registry_override")
        observed["expected_event_ids"] = kwargs.get("expected_event_ids")
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z"]),
                "symbol": ["BTCUSDT"],
                "close": [100.0],
                "evt_VOL_SPIKE": [True],
            }
        )

    monkeypatch.setattr(
        context_mode_comparison_service,
        "load_search_feature_frame",
        fake_load_search_feature_frame,
    )
    monkeypatch.setattr(
        context_mode_comparison_service,
        "generate_hypotheses_with_audit",
        lambda **_: ([{"hypothesis_id": "h1"}], {}),
    )
    monkeypatch.setattr(
        context_mode_comparison_service,
        "compare_context_modes",
        lambda **_: {
            "schema_version": "context_mode_comparison_v1",
            "hard_label": {"evaluated_rows": 1, "selected": {}},
            "confidence_aware": {"evaluated_rows": 1, "selected": {}},
            "delta": {},
            "selection_changed": False,
            "selection_outcome_changed": False,
        },
    )

    payload = context_mode_comparison_service.build_context_mode_comparison_payload(
        data_root=tmp_path,
        run_id="bench_run",
        symbols=["BTCUSDT"],
        timeframe="5m",
        search_space_path=search_space_path,
        event_registry_override="tmp/fixture.parquet",
    )

    assert observed["event_registry_override"] == "tmp/fixture.parquet"
    assert observed["expected_event_ids"] == ["VOL_SPIKE"]
    assert payload["event_registry_override"] == "tmp/fixture.parquet"
