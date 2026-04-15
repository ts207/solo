from __future__ import annotations

from pathlib import Path

import yaml

from project.domain.hypotheses import TriggerType
from project.research.search.generator import generate_hypotheses_with_audit


def test_search_space_includes_vol_shock_event_trigger():
    repo_root = Path(__file__).resolve().parents[3]
    search_space_path = repo_root / "spec" / "search_space.yaml"
    payload = yaml.safe_load(search_space_path.read_text(encoding="utf-8"))

    assert "VOL_SHOCK" in payload["triggers"]["events"]


def test_synthetic_truth_search_spec_expands_event_specific_templates():
    hypotheses, audit = generate_hypotheses_with_audit("synthetic_truth")

    assert hypotheses
    assert audit["counts"]["feasible"] > 0
    assert audit["counts"]["feasible"] >= len(hypotheses)


def test_default_search_space_is_tier1_event_expression_only():
    repo_root = Path(__file__).resolve().parents[3]
    search_space_path = repo_root / "spec" / "search_space.yaml"
    payload = yaml.safe_load(search_space_path.read_text(encoding="utf-8"))

    assert payload["metadata"]["search_tier"] == "tier1"
    assert payload["metadata"]["default_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert payload["horizons"] == ["12b", "24b", "48b"]
    assert payload["entry_lag"] == 1
    assert "states" not in payload["triggers"]
    assert "transitions" not in payload["triggers"]
    assert "feature_predicates" not in payload["triggers"]


def test_default_search_space_generates_event_led_expression_hypotheses_only():
    repo_root = Path(__file__).resolve().parents[3]
    search_space_path = repo_root / "spec" / "search_space.yaml"

    hypotheses, audit = generate_hypotheses_with_audit(search_space_path=search_space_path)

    assert hypotheses
    assert audit["counts"]["feasible"] > 0
    assert all(spec.trigger.trigger_type == TriggerType.EVENT for spec in hypotheses)
    assert {spec.horizon for spec in hypotheses} == {"12b", "24b", "48b"}
    assert {spec.entry_lag for spec in hypotheses} == {1}
    assert all(getattr(spec, "filter_template_id", None) in (None, "") for spec in hypotheses)
