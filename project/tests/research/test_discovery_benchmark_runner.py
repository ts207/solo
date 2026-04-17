import pytest
from pathlib import Path
from project import PROJECT_ROOT
from project.research.benchmarks import discovery_benchmark
from project.research.benchmarks.benchmark_modes import get_mode


def test_benchmark_runner_immutability():
    ledger_config_path = PROJECT_ROOT.parent / "project/configs/discovery_ledger.yaml"
    scoring_config_path = PROJECT_ROOT.parent / "project/configs/discovery_scoring_v2.yaml"

    def get_bytes(p):
        return p.read_bytes() if p.exists() else b""

    before_ledger = get_bytes(ledger_config_path)
    before_scoring = get_bytes(scoring_config_path)

    base_search = {"symbol": "BTC", "cases": []}
    base_scoring = {"v2_scoring": {"enabled": True}}
    base_ledger = {"enabled": False}

    mode_e = get_mode("E")
    assert mode_e is not None
    resolved = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, mode_e
    )

    assert resolved["ledger"]["enabled"] is True
    assert base_ledger["enabled"] is False

    assert get_bytes(ledger_config_path) == before_ledger
    assert get_bytes(scoring_config_path) == before_scoring


def test_benchmark_output_persistence(tmp_path):
    from project.research.benchmarks import discovery_benchmark
    import json

    base_search = {"search": "flat"}
    base_scoring = {"v2": True}
    base_ledger = {"enabled": False}

    out_dir = tmp_path / "ledger"
    out_dir.mkdir()

    mode_e = get_mode("E")
    assert mode_e is not None
    resolved = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, mode_e
    )

    with open(out_dir / "resolved_mode_config.json", "w") as f:
        json.dump(resolved, f, indent=2)

    assert (out_dir / "resolved_mode_config.json").exists()
    saved = json.loads((out_dir / "resolved_mode_config.json").read_text())
    assert saved["mode_id"] == "E"
    assert saved["ledger"]["enabled"] is True


def test_benchmark_mode_isolation():
    from project.research.benchmarks import discovery_benchmark

    base_search = {"search": "flat"}
    base_scoring = {"v2": True}
    base_ledger = {"enabled": False}

    mode_a = get_mode("A")
    mode_b = get_mode("B")
    mode_e = get_mode("E")
    assert mode_a is not None and mode_b is not None and mode_e is not None

    res_legacy = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, mode_a
    )
    res_v2 = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, mode_b
    )
    res_ledger = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, mode_e
    )

    assert res_legacy["mode_id"] == "A"
    assert res_v2["mode_id"] == "B"
    assert res_ledger["mode_id"] == "E"

    assert res_legacy["scoring_v2"]["enable_discovery_v2_scoring"] is False
    assert res_legacy["ledger"]["enabled"] is False

    assert res_v2["scoring_v2"]["enable_discovery_v2_scoring"] is True
    assert res_v2["ledger"]["enabled"] is False

    assert res_ledger["scoring_v2"]["enable_discovery_v2_scoring"] is True
    assert res_ledger["ledger"]["enabled"] is True
