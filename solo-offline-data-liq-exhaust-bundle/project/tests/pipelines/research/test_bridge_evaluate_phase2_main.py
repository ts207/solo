from __future__ import annotations

import json
from pathlib import Path

from project.research import bridge_evaluate_phase2 as module


def test_bridge_main_returns_warning_exit_when_no_candidates(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    out_dir = tmp_path / "out"
    monkeypatch.setattr(module, "get_data_root", lambda: data_root)
    monkeypatch.setattr(
        module,
        "resolve_objective_profile_contract",
        lambda **_: type(
            "Contract",
            (),
            {
                "min_net_expectancy_bps": 0.0,
                "max_fee_plus_slippage_bps": None,
                "max_daily_turnover_multiple": None,
                "require_retail_viability": False,
                "low_capital_contract": {},
                "require_low_capital_contract": False,
            },
        )(),
    )
    monkeypatch.setattr(module, "bridge_event_out_dir", lambda **_: out_dir)
    monkeypatch.setattr(module, "phase2_event_out_dir", lambda **_: tmp_path / "missing_phase2_dir")
    monkeypatch.setattr(
        module.sys,
        "argv",
        [
            "bridge_evaluate_phase2.py",
            "--run_id",
            "run_1",
            "--event_type",
            "VOL_SHOCK",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "5m",
            "--out_dir",
            str(out_dir),
        ],
    )

    rc = module.main()

    assert rc == 1
    manifest_path = out_dir / "run_1" / "bridge_evaluate_phase2__VOL_SHOCK_5m.json"
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert payload["status"] == "warning"
