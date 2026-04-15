from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

import project.discover as discover_module
import project.promote as promote_module
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


def test_cli_promote_run_delegates_without_compatibility_bridge(monkeypatch):
    cli = _load_cli_module()
    captured = {}

    class _Result:
        exit_code = 0
        diagnostics = {}

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
