from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import ClassVar

import pytest
import yaml

import project.discover as discover_module
import project.promote as promote_module
from project.research.cell_discovery import cells_cli as cells_cli_module
from project.scripts.run_live_engine import load_live_engine_config
from project.tests.conftest import PROJECT_ROOT

CLI_PATH = PROJECT_ROOT / "cli.py"


def _load_cli_module():
    spec = importlib.util.spec_from_file_location("project_cli", CLI_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_cli_rejects_removed_strategy_subcommand(monkeypatch, capsys):
    cli = _load_cli_module()
    monkeypatch.setattr(sys, "argv", ["backtest", "strategy", "eval"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert int(exc.value.code) == 2
    err = capsys.readouterr().err.lower()
    assert "invalid choice: 'strategy'" in err


def test_cli_help_hides_operator_compatibility_surface(monkeypatch, capsys):
    cli = _load_cli_module()
    monkeypatch.setattr(sys, "argv", ["backtest", "--help"])

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert int(exc.value.code) == 0
    out = capsys.readouterr().out
    assert "discover" in out
    assert "validate" in out
    assert "promote" in out
    assert "deploy" in out
    assert "operator" not in out


def test_cli_rejects_removed_pipeline_run_all_alias(monkeypatch, capsys):
    cli = _load_cli_module()
    monkeypatch.setattr(sys, "argv", ["backtest", "pipeline", "run-all", "--run_id", "unit"])

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert int(exc.value.code) == 2
    err = capsys.readouterr().err.lower()
    assert "invalid choice: 'pipeline'" in err


def test_cli_discover_plan_delegates_without_legacy_compatibility(monkeypatch):
    cli = _load_cli_module()
    captured = {}

    def _fake_run(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return {"execution": {"returncode": 0}}

    monkeypatch.setattr(discover_module, "run", _fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        ["backtest", "discover", "plan", "--proposal", "spec/proposals/unit.yaml"],
    )

    assert cli.main() == 0
    assert captured["args"] == ("spec/proposals/unit.yaml",)
    assert captured["kwargs"] == {
        "registry_root": Path("project/configs/registries"),
        "data_root": None,
        "run_id": None,
        "plan_only": True,
        "dry_run": False,
        "check": False,
    }


def test_cli_discover_plan_rejects_legacy_compatibility_flag(monkeypatch, capsys):
    cli = _load_cli_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest",
            "discover",
            "plan",
            "--proposal",
            "spec/proposals/unit.yaml",
            "--legacy_compatibility",
            "1",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert int(exc.value.code) == 2
    err = capsys.readouterr().err
    assert "unrecognized arguments: --legacy_compatibility 1" in err


def test_cli_discover_cells_plan_delegates_to_cell_lane(monkeypatch, tmp_path: Path):
    cli = _load_cli_module()
    captured = {}

    def _fake_run_from_namespace(args):
        captured["args"] = args
        return {
            "exit_code": 0,
            "status": "planned",
            "run_id": args.run_id,
        }

    monkeypatch.setattr(cells_cli_module, "run_from_namespace", _fake_run_from_namespace)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest",
            "discover",
            "cells",
            "plan",
            "--run_id",
            "unit_cells",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--data_root",
            str(tmp_path),
        ],
    )

    assert cli.main() == 0
    assert captured["args"].cells_action == "plan"
    assert captured["args"].run_id == "unit_cells"
    assert captured["args"].symbols == "BTCUSDT,ETHUSDT"
    assert captured["args"].data_root == str(tmp_path)


def test_cli_discover_cells_coverage_audit_delegates(monkeypatch):
    cli = _load_cli_module()
    captured = {}

    def _fake_run_from_namespace(args):
        captured["args"] = args
        return {"exit_code": 0, "status": "ok"}

    monkeypatch.setattr(cells_cli_module, "run_from_namespace", _fake_run_from_namespace)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest",
            "discover",
            "cells",
            "coverage-audit",
            "--spec_root",
            "spec/discovery",
        ],
    )

    assert cli.main() == 0
    assert captured["args"].cells_action == "coverage-audit"
    assert captured["args"].spec_root == "spec/discovery"


def test_cli_discover_cells_spec_audit_delegates(monkeypatch):
    cli = _load_cli_module()
    captured = {}

    def _fake_run_from_namespace(args):
        captured["args"] = args
        return {"exit_code": 0, "status": "ok"}

    monkeypatch.setattr(cells_cli_module, "run_from_namespace", _fake_run_from_namespace)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest",
            "discover",
            "cells",
            "spec-audit",
            "--spec_dir",
            "spec/discovery/tier2_trend_failure_runtime_v1",
        ],
    )

    assert cli.main() == 0
    assert captured["args"].cells_action == "spec-audit"
    assert captured["args"].spec_dir == "spec/discovery/tier2_trend_failure_runtime_v1"


def test_cli_discover_cells_rejects_compat_discovery_flag(monkeypatch, capsys):
    cli = _load_cli_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest",
            "discover",
            "cells",
            "plan",
            "--run_id",
            "unit_cells",
            "--use_candidate_discovery",
            "1",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert int(exc.value.code) == 2
    assert "unrecognized arguments: --use_candidate_discovery 1" in capsys.readouterr().err


def test_cli_discover_cells_assemble_theses_delegates(monkeypatch, tmp_path: Path):
    cli = _load_cli_module()
    captured = {}

    def _fake_run_from_namespace(args):
        captured["args"] = args
        return {"exit_code": 0, "status": "ok", "generated_count": 0}

    monkeypatch.setattr(cells_cli_module, "run_from_namespace", _fake_run_from_namespace)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest",
            "discover",
            "cells",
            "assemble-theses",
            "--run_id",
            "unit_cells",
            "--data_root",
            str(tmp_path),
            "--limit",
            "3",
            "--per-cell",
        ],
    )

    assert cli.main() == 0
    assert captured["args"].cells_action == "assemble-theses"
    assert captured["args"].limit == 3
    assert captured["args"].per_cell is True


def test_cli_promote_run_delegates_without_compatibility_bridge(monkeypatch):
    cli = _load_cli_module()
    captured = {}

    class _Result:
        exit_code = 0
        diagnostics: ClassVar[dict] = {}

    def _fake_run(**kwargs):
        captured["kwargs"] = kwargs
        return _Result()

    monkeypatch.setattr(
        promote_module,
        "run",
        _fake_run,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["backtest", "promote", "run", "--run_id", "unit", "--symbols", "BTCUSDT"],
    )

    assert cli.main() == 0
    assert captured["kwargs"] == {
        "run_id": "unit",
        "symbols": "BTCUSDT",
        "out_dir": None,
        "retail_profile": "capital_constrained",
        "promotion_profile": "auto",
        "require_forward_confirmation": None,
    }


def test_cli_promote_run_rejects_compatibility_bridge_flag(monkeypatch, capsys):
    cli = _load_cli_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest",
            "promote",
            "run",
            "--run_id",
            "unit",
            "--symbols",
            "BTCUSDT",
            "--use_compatibility_bridge",
            "1",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert int(exc.value.code) == 2
    err = capsys.readouterr().err
    assert "unrecognized arguments: --use_compatibility_bridge 1" in err


def test_cli_deploy_bind_config_defaults_to_project_configs_and_emits_single_thesis_source(
    monkeypatch, tmp_path: Path
):
    cli = _load_cli_module()
    run_id = "bind_contract_run"
    data_root = tmp_path / "data"
    thesis_dir = data_root / "live" / "theses" / run_id
    thesis_dir.mkdir(parents=True)
    (thesis_dir / "promoted_theses.json").write_text(
        json.dumps(
            {
                "schema_version": "promoted_theses_v1",
                "run_id": run_id,
                "generated_at_utc": "2026-04-27T00:00:00Z",
                "thesis_count": 1,
                "active_thesis_count": 0,
                "pending_thesis_count": 0,
                "theses": [
                    {
                        "primary_event_id": "VOL_SHOCK",
                        "thesis_id": f"thesis::{run_id}::BTCUSDT",
                        "deployment_state": "monitor_only",
                        "timeframe": "5m",
                        "evidence": {"sample_size": 100},
                        "lineage": {
                            "source_run_id": run_id,
                            "run_id": run_id,
                            "candidate_id": f"cand::{run_id}",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    default_config_dir = PROJECT_ROOT / "configs"
    config_path = default_config_dir / f"live_monitor_{run_id}.yaml"
    try:
        if config_path.exists():
            config_path.unlink()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "backtest",
                "deploy",
                "bind-config",
                "--run_id",
                run_id,
                "--data_root",
                str(data_root),
            ],
        )

        assert cli.main() == 0
        assert config_path.exists()

        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        strategy_runtime = payload["strategy_runtime"]
        assert strategy_runtime["thesis_run_id"] == run_id
        assert "thesis_path" not in strategy_runtime
        assert payload["freshness_streams"] == [
            {"symbol": "btcusdt", "stream": "kline_5m"},
            {"symbol": "ethusdt", "stream": "kline_5m"},
        ]

        loaded = load_live_engine_config(config_path)
        loaded_runtime = loaded["strategy_runtime"]
        assert loaded_runtime["thesis_run_id"] == run_id
        assert "thesis_path" not in loaded_runtime
    finally:
        if config_path.exists():
            config_path.unlink()


def test_cli_promote_export_delegates_to_current_export_api(monkeypatch):
    cli = _load_cli_module()
    captured = {}

    class _Result:
        exit_code = 0

        def to_dict(self):
            return {"run_id": "unit", "thesis_count": 0}

    def _fake_export(**kwargs):
        captured["kwargs"] = kwargs
        return _Result()

    monkeypatch.setattr(promote_module, "export", _fake_export)
    monkeypatch.setattr(
        sys,
        "argv",
        ["backtest", "promote", "export", "--run_id", "unit", "--data_root", "data"],
    )

    assert cli.main() == 0
    assert captured["kwargs"] == {
        "run_id": "unit",
        "data_root": Path("data"),
    }


def test_cli_operator_plan_alias_removed(monkeypatch, capsys):
    cli = _load_cli_module()

    monkeypatch.setattr(
        sys,
        "argv",
        ["backtest", "operator", "plan", "--proposal", "spec/proposals/unit.yaml"],
    )

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert int(exc.value.code) == 2
    assert "invalid choice: 'operator'" in capsys.readouterr().err


def test_cli_deploy_bind_config_does_not_inject_synthetic_microstructure_defaults(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cli = _load_cli_module()
    run_id = "unit_run"
    data_root = tmp_path / "data"
    thesis_dir = data_root / "live" / "theses" / run_id
    thesis_dir.mkdir(parents=True)
    (thesis_dir / "promoted_theses.json").write_text(
        json.dumps(
            {
                "schema_version": "promoted_theses_v1",
                "run_id": run_id,
                "generated_at_utc": "2026-04-27T00:00:00Z",
                "thesis_count": 1,
                "active_thesis_count": 0,
                "pending_thesis_count": 0,
                "theses": [
                    {
                        "primary_event_id": "VOL_SHOCK",
                        "thesis_id": f"thesis::{run_id}::BTCUSDT",
                        "deployment_state": "monitor_only",
                        "timeframe": "5m",
                        "evidence": {"sample_size": 100},
                        "lineage": {
                            "source_run_id": run_id,
                            "run_id": run_id,
                            "candidate_id": f"cand::{run_id}",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    out_dir = tmp_path / "configs"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest",
            "deploy",
            "bind-config",
            "--run_id",
            run_id,
            "--data_root",
            str(data_root),
            "--out_dir",
            str(out_dir),
        ],
    )

    assert cli.main() == 0
    payload = yaml.safe_load((out_dir / f"live_monitor_{run_id}.yaml").read_text())
    strategy_runtime = payload["strategy_runtime"]
    assert "default_depth_usd" not in strategy_runtime
    assert "default_tob_coverage" not in strategy_runtime
    assert "default_expected_cost_bps" not in strategy_runtime
