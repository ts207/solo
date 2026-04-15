from __future__ import annotations

import pandas as pd
import pytest

from project.research.cli.candidate_discovery_cli import build_candidate_discovery_parser
from project.research.services import phase2_support


def test_phase2_parser_rejects_removed_candidate_plan_flag():
    parser = build_candidate_discovery_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(
            [
                "--run_id",
                "r1",
                "--event_type",
                "VOL_SHOCK",
                "--symbols",
                "BTCUSDT",
                "--candidate_plan",
                "/tmp/plan.jsonl",
            ]
        )
    assert int(exc.value.code) == 2


def test_phase2_parser_rejects_removed_atlas_mode_flag():
    parser = build_candidate_discovery_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(
            [
                "--run_id",
                "r1",
                "--event_type",
                "VOL_SHOCK",
                "--symbols",
                "BTCUSDT",
                "--atlas_mode",
                "1",
            ]
        )
    assert int(exc.value.code) == 2


def test_phase2_load_features_resolves_data_root_at_call_time(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    captured: dict[str, object] = {}

    def _fake_load_features_impl(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame()

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(phase2_support, "_load_features_impl", _fake_load_features_impl)

    phase2_support.load_phase2_features(run_id="r1", symbol="BTCUSDT")

    assert captured["data_root"] == tmp_path
