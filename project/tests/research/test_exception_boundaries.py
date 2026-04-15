from __future__ import annotations

import json

import pandas as pd
import pytest

from project.research.bridge_evaluation import _series_from_row_value
from project.research.candidates import filtering
from project.research.helpers.viability import _delay_expectancy_map


def test_series_from_row_value_returns_empty_series_on_invalid_json() -> None:
    out = _series_from_row_value("{bad json")
    assert isinstance(out, pd.Series)
    assert out.empty


def test_delay_expectancy_map_returns_empty_dict_on_invalid_json() -> None:
    out = _delay_expectancy_map({"delay_expectancy_map": "{bad json"})
    assert out == {}


def test_load_candidate_detail_returns_empty_dict_on_invalid_json(tmp_path) -> None:
    source_path = tmp_path / "candidate.json"
    source_path.write_text("{bad json", encoding="utf-8")

    assert filtering.load_candidate_detail(source_path, "cand_1") == {}


def test_load_candidate_detail_does_not_swallow_unexpected_runtime_errors(
    monkeypatch, tmp_path
) -> None:
    source_path = tmp_path / "candidate.json"
    source_path.write_text(json.dumps({"candidate_id": "cand_1"}), encoding="utf-8")

    def _boom(*args, **kwargs):
        raise RuntimeError("unexpected read failure")

    monkeypatch.setattr(filtering.Path, "read_text", _boom)

    with pytest.raises(RuntimeError, match="unexpected read failure"):
        filtering.load_candidate_detail(source_path, "cand_1")
