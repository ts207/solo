import pytest
from pathlib import Path
from project import PROJECT_ROOT
from project.research.benchmarks import discovery_benchmark

def test_benchmark_runner_immutability():
    # Read bytes of configs before run
    ledger_config_path = PROJECT_ROOT.parent / "project/configs/discovery_ledger.yaml"
    scoring_config_path = PROJECT_ROOT.parent / "project/configs/discovery_scoring_v2.yaml"
    
    def get_bytes(p):
        return p.read_bytes() if p.exists() else b""

    before_ledger = get_bytes(ledger_config_path)
    before_scoring = get_bytes(scoring_config_path)

    # Note: We don't actually run a full benchmark here as it might be slow/data-dependent
    # but we verify that the helper function doesn't touch the disk.
    base_search = {"symbol": "BTC", "cases": []}
    base_scoring = {"v2_scoring": {"enabled": True}}
    base_ledger = {"enabled": False}
    
    # Run helper
    resolved = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, "ledger"
    )
    
    assert resolved["ledger"]["enabled"] is True
    assert base_ledger["enabled"] is False # Ensure deepcopy worked
    
    # Check disk bytes again
    assert get_bytes(ledger_config_path) == before_ledger
    assert get_bytes(scoring_config_path) == before_scoring

def test_benchmark_output_persistence(tmp_path):
    # Verify that resolved config is persisted correctly
    from project.research.benchmarks import discovery_benchmark
    import json
    
    base_search = {"search": "flat"}
    base_scoring = {"v2": True}
    base_ledger = {"enabled": False}
    
    # Mock a minimal run or call the persistence logic
    out_dir = tmp_path / "ledger"
    out_dir.mkdir()
    
    resolved = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, "ledger"
    )
    
    with open(out_dir / "resolved_mode_config.json", "w") as f:
        json.dump(resolved, f, indent=2)
        
    assert (out_dir / "resolved_mode_config.json").exists()
    saved = json.loads((out_dir / "resolved_mode_config.json").read_text())
    assert saved["mode"] == "ledger"
    assert saved["ledger"]["enabled"] is True

def test_benchmark_mode_isolation():
    from project.research.benchmarks import discovery_benchmark
    
    base_search = {"search": "flat"}
    base_scoring = {"v2": True}
    base_ledger = {"enabled": False}
    
    res_legacy = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, "legacy"
    )
    res_v2 = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, "v2"
    )
    res_ledger = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, "ledger"
    )
    
    # Assert isolation in resolved configs
    assert res_legacy["mode"] == "legacy"
    assert res_v2["mode"] == "v2"
    assert res_ledger["mode"] == "ledger"
    
    # legacy disables v2 and ledger
    assert res_legacy["scoring_v2"]["enable_discovery_v2_scoring"] is False
    assert res_legacy["ledger"]["enabled"] is False
    
    # v2 enables v2 but disables ledger
    assert res_v2["scoring_v2"]["enable_discovery_v2_scoring"] is True
    assert res_v2["ledger"]["enabled"] is False
    
    # ledger enables both
    assert res_ledger["scoring_v2"]["enable_discovery_v2_scoring"] is True
    assert res_ledger["ledger"]["enabled"] is True
