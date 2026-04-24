"""Phase 2.2 — Tests for the centralised search_space quality-weight loader
and for the quality-ordered frontier in search_intelligence.

Three test classes:
  TestSearchSpaceLoader      — unit tests for spec_registry.search_space
  TestFrontierOrdering       — verifies search_intelligence sorts by weight
  TestControllerUsesRegistry — verifies campaign_controller delegates to loader
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from project.spec_registry.search_space import (
    IG_SCALE_FACTOR,
    QUALITY_SCORES,
    _parse_annotation_line,
    load_event_priority_weights,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_ss(tmp_path: Path, content: str) -> Path:
    """Write search_space YAML content and return the path."""
    p = tmp_path / "search_space.yaml"
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# TestSearchSpaceLoader
# ---------------------------------------------------------------------------


class TestSearchSpaceLoader:
    """Unit tests for load_event_priority_weights()."""

    def test_high_quality_label_parsed(self, tmp_path):
        p = _write_ss(tmp_path, "    - LIQUIDATION_CASCADE # [QUALITY: HIGH] - High IG (0.000467)\n")
        weights = load_event_priority_weights(p)
        assert "LIQUIDATION_CASCADE" in weights
        assert weights["LIQUIDATION_CASCADE"] == pytest.approx(
            QUALITY_SCORES["HIGH"] + 0.000467 * IG_SCALE_FACTOR, rel=1e-6
        )

    def test_moderate_quality_label_parsed(self, tmp_path):
        p = _write_ss(tmp_path, "    - OVERSHOOT_AFTER_SHOCK # [QUALITY: MODERATE] - Moderate IG (0.000226)\n")
        weights = load_event_priority_weights(p)
        assert weights["OVERSHOOT_AFTER_SHOCK"] == pytest.approx(
            QUALITY_SCORES["MODERATE"] + 0.000226 * IG_SCALE_FACTOR, rel=1e-6
        )

    def test_low_quality_label_parsed(self, tmp_path):
        p = _write_ss(tmp_path, "    - LIQUIDITY_VACUUM # [QUALITY: LOW] - Marginal IG (0.000134)\n")
        weights = load_event_priority_weights(p)
        assert weights["LIQUIDITY_VACUUM"] == pytest.approx(
            QUALITY_SCORES["LOW"] + 0.000134 * IG_SCALE_FACTOR, rel=1e-6
        )

    def test_quality_without_ig_value(self, tmp_path):
        """Label-only annotation → tier base weight, no IG bonus."""
        p = _write_ss(tmp_path, "    - BAND_BREAK # [QUALITY: MODERATE]\n")
        weights = load_event_priority_weights(p)
        assert weights["BAND_BREAK"] == pytest.approx(QUALITY_SCORES["MODERATE"], rel=1e-6)

    def test_unannotated_events_absent(self, tmp_path):
        """Events without a QUALITY annotation must not appear in the dict."""
        p = _write_ss(tmp_path, "    - VOL_SPIKE\n    - SPREAD_BLOWOUT\n")
        weights = load_event_priority_weights(p)
        assert "VOL_SPIKE" not in weights
        assert "SPREAD_BLOWOUT" not in weights

    def test_missing_file_returns_empty(self, tmp_path):
        weights = load_event_priority_weights(tmp_path / "nonexistent.yaml")
        assert weights == {}

    def test_structured_entries_skipped(self, tmp_path):
        """Dict-style transition entries must not be parsed as event IDs."""
        p = _write_ss(tmp_path, "    - { from: LOW_VOL_REGIME, to: HIGH_VOL_REGIME } # [QUALITY: HIGH]\n")
        weights = load_event_priority_weights(p)
        assert weights == {}

    def test_tier_ordering_preserved_with_ig_bonus(self, tmp_path):
        """HIGH + tiny IG must outweigh MODERATE + large IG."""
        content = (
            "    - A_HIGH # [QUALITY: HIGH] - High IG (0.000001)\n"
            "    - B_MODERATE # [QUALITY: MODERATE] - Moderate IG (0.000999)\n"
        )
        p = _write_ss(tmp_path, content)
        weights = load_event_priority_weights(p)
        # HIGH base (3.0) + IG 0.001 = 3.001
        # MODERATE base (2.0) + IG 0.999 = 2.999
        assert weights["A_HIGH"] > weights["B_MODERATE"]

    def test_multiple_events_all_parsed(self, tmp_path):
        content = (
            "    - LIQUIDATION_CASCADE # [QUALITY: HIGH] - High IG (0.000467)\n"
            "    - OVERSHOOT_AFTER_SHOCK # [QUALITY: MODERATE] - Moderate IG (0.000226)\n"
            "    - LIQUIDITY_VACUUM # [QUALITY: LOW] - Marginal IG (0.000134)\n"
        )
        p = _write_ss(tmp_path, content)
        weights = load_event_priority_weights(p)
        assert len(weights) == 3
        assert weights["LIQUIDATION_CASCADE"] > weights["OVERSHOOT_AFTER_SHOCK"] > weights["LIQUIDITY_VACUUM"]

    def test_case_insensitive_quality_label(self, tmp_path):
        p = _write_ss(tmp_path, "    - MY_EVENT # [QUALITY: high] - High IG (0.000200)\n")
        weights = load_event_priority_weights(p)
        assert "MY_EVENT" in weights
        assert weights["MY_EVENT"] == pytest.approx(
            QUALITY_SCORES["HIGH"] + 0.000200 * IG_SCALE_FACTOR, rel=1e-6
        )

    def test_real_search_space_yaml_parses_three_events(self):
        """Integration: the real spec/search_space.yaml has exactly 3 annotated events."""
        weights = load_event_priority_weights()
        # The three annotated events from spec/search_space.yaml
        annotated = {"LIQUIDATION_CASCADE", "OVERSHOOT_AFTER_SHOCK", "LIQUIDITY_VACUUM"}
        found = annotated & set(weights.keys())
        assert found == annotated, (
            f"Expected all three annotated events; found only: {found}"
        )

    def test_parse_annotation_line_returns_none_for_no_comment(self):
        assert _parse_annotation_line("    - VOL_SPIKE") is None

    def test_parse_annotation_line_returns_none_for_non_list_item(self):
        assert _parse_annotation_line("  vol_regime: high  # [QUALITY: HIGH]") is None

    def test_parse_annotation_line_returns_none_for_dict_entry(self):
        assert _parse_annotation_line("    - { from: A, to: B }  # [QUALITY: HIGH]") is None

    def test_ig_exponent_notation_parsed(self, tmp_path):
        """IG values in scientific notation (e.g. 4.67e-4) are handled."""
        p = _write_ss(tmp_path, "    - MY_EVENT # [QUALITY: HIGH] - High IG (4.67e-4)\n")
        weights = load_event_priority_weights(p)
        assert "MY_EVENT" in weights
        assert weights["MY_EVENT"] == pytest.approx(
            QUALITY_SCORES["HIGH"] + 4.67e-4 * IG_SCALE_FACTOR, rel=1e-6
        )


# ---------------------------------------------------------------------------
# TestFrontierOrdering
# ---------------------------------------------------------------------------


class TestFrontierOrdering:
    """Verifies that search_intelligence._build_frontier sorts by quality weight."""

    def _make_registries(self, events: Dict[str, Any]) -> MagicMock:
        reg = MagicMock()
        reg.events = {"events": events}
        return reg

    def test_high_quality_event_appears_first(self, tmp_path):
        from project.research.search_intelligence import _build_frontier

        registries = self._make_registries({
            "LOW_EVENT":  {"enabled": True, "family": "F"},
            "HIGH_EVENT": {"enabled": True, "family": "F"},
            "MID_EVENT":  {"enabled": True, "family": "F"},
        })
        quality_weights = {
            "HIGH_EVENT": QUALITY_SCORES["HIGH"] + 0.467,
            "MID_EVENT":  QUALITY_SCORES["MODERATE"] + 0.226,
            "LOW_EVENT":  QUALITY_SCORES["LOW"] + 0.134,
        }
        result = _build_frontier(
            registries,
            pd.DataFrame(),  # no tested regions
            pd.DataFrame(),  # no failures
            untested_top_k=3,
            repair_top_k=2,
            exhausted_failure_threshold=3,
            quality_weights=quality_weights,
        )
        untested = result["untested_registry_events"]
        assert len(untested) == 3
        assert untested[0] == "HIGH_EVENT"
        assert untested[1] == "MID_EVENT"
        assert untested[2] == "LOW_EVENT"

    def test_unannotated_events_use_default_weight(self, tmp_path):
        from project.research.search_intelligence import _build_frontier

        registries = self._make_registries({
            "ANNOTATED":   {"enabled": True, "family": "F"},
            "UNANNOTATED": {"enabled": True, "family": "F"},
        })
        # Only ANNOTATED has an explicit weight; UNANNOTATED falls back to DEFAULT
        quality_weights = {
            "ANNOTATED": QUALITY_SCORES["LOW"],  # 1.0 < DEFAULT 1.5
        }
        result = _build_frontier(
            registries,
            pd.DataFrame(),
            pd.DataFrame(),
            untested_top_k=2,
            repair_top_k=2,
            exhausted_failure_threshold=3,
            quality_weights=quality_weights,
        )
        untested = result["untested_registry_events"]
        # UNANNOTATED (default 1.5) > ANNOTATED (LOW = 1.0) → UNANNOTATED first
        assert untested[0] == "UNANNOTATED"
        assert untested[1] == "ANNOTATED"

    def test_no_quality_weights_falls_back_to_default(self, tmp_path):
        from project.research.search_intelligence import _build_frontier

        registries = self._make_registries({
            "EVT_A": {"enabled": True, "family": "F"},
            "EVT_B": {"enabled": True, "family": "F"},
        })
        # Empty quality_weights → all events get DEFAULT_EVENT_PRIORITY_WEIGHT
        result = _build_frontier(
            registries,
            pd.DataFrame(),
            pd.DataFrame(),
            untested_top_k=5,
            repair_top_k=2,
            exhausted_failure_threshold=3,
            quality_weights={},
        )
        assert set(result["untested_registry_events"]) == {"EVT_A", "EVT_B"}

    def test_frontier_top_k_respected(self, tmp_path):
        from project.research.search_intelligence import _build_frontier

        events = {f"EVT_{i}": {"enabled": True, "family": "F"} for i in range(10)}
        registries = self._make_registries(events)
        result = _build_frontier(
            registries,
            pd.DataFrame(),
            pd.DataFrame(),
            untested_top_k=3,
            repair_top_k=2,
            exhausted_failure_threshold=3,
            quality_weights={},
        )
        assert len(result["untested_registry_events"]) == 3


# ---------------------------------------------------------------------------
# TestControllerUsesRegistry
# ---------------------------------------------------------------------------


class TestControllerUsesRegistry:
    """Verifies that CampaignController delegates weight loading to spec_registry."""

    def test_quality_weights_loaded_on_init(self, tmp_path):
        """Controller's _quality_weights should contain HIGH-annotated events."""
        from project.research.campaign_controller import (
            CampaignConfig,
            CampaignController,
        )

        config = CampaignConfig(program_id="test_phase22", max_runs=1)
        registry_root = tmp_path / "reg"
        registry_root.mkdir()

        ctrl = CampaignController.__new__(CampaignController)
        ctrl.config = config
        ctrl.data_root = tmp_path
        ctrl.registry_root = registry_root
        ctrl.campaign_dir = tmp_path / "artifacts" / "experiments" / config.program_id
        ctrl.campaign_dir.mkdir(parents=True)
        ctrl.ledger_path = ctrl.campaign_dir / "tested_ledger.parquet"
        ctrl.summary_path = ctrl.campaign_dir / "campaign_summary.json"
        ctrl.registries = MagicMock()
        ctrl.registries.events = {"events": {}}
        ctrl.registries.templates = {"families": {}}

        # Point controller at the real spec/search_space.yaml
        ctrl._search_space_path = Path("spec/search_space.yaml")

        from project.spec_registry.search_space import load_event_priority_weights
        ctrl._quality_weights = load_event_priority_weights(ctrl._search_space_path)

        # The real file should surface LIQUIDATION_CASCADE as HIGH-tier
        assert "LIQUIDATION_CASCADE" in ctrl._quality_weights
        assert ctrl._quality_weights["LIQUIDATION_CASCADE"] > QUALITY_SCORES["MODERATE"]

    def test_shim_produces_same_output_as_loader(self, tmp_path):
        """_load_event_quality_weights shim must return identical results to the loader."""
        from project.research.campaign_controller import _load_event_quality_weights
        from project.spec_registry.search_space import load_event_priority_weights

        content = (
            "    - HIGH_EVT # [QUALITY: HIGH] - High IG (0.000467)\n"
            "    - MOD_EVT # [QUALITY: MODERATE] - Moderate IG (0.000226)\n"
            "    - LOW_EVT # [QUALITY: LOW] - Marginal IG (0.000134)\n"
        )
        p = tmp_path / "search_space.yaml"
        p.write_text(content, encoding="utf-8")

        shim_result = _load_event_quality_weights(p)
        direct_result = load_event_priority_weights(p)

        assert shim_result == direct_result

    def test_ig_bonus_increases_weight_above_tier_base(self, tmp_path):
        """The IG bonus must push an event's weight above its tier-base value."""
        from project.spec_registry.search_space import load_event_priority_weights

        p = tmp_path / "ss.yaml"
        p.write_text("    - MY_EVENT # [QUALITY: MODERATE] - Moderate IG (0.000500)\n")
        weights = load_event_priority_weights(p)

        assert weights["MY_EVENT"] > QUALITY_SCORES["MODERATE"]
        assert weights["MY_EVENT"] == pytest.approx(
            QUALITY_SCORES["MODERATE"] + 0.000500 * IG_SCALE_FACTOR, rel=1e-6
        )

    def test_frontier_ordering_in_update_search_intelligence(self, tmp_path):
        """End-to-end: update_search_intelligence writes quality-ordered frontier."""
        from project.research.experiment_engine import RegistryBundle
        from project.research.search_intelligence import update_search_intelligence

        # Write a minimal search_space.yaml with 2 annotated events
        ss_path = tmp_path / "spec" / "search_space.yaml"
        ss_path.parent.mkdir(parents=True)
        ss_path.write_text(
            "    - HIGH_EVT # [QUALITY: HIGH] - High IG (0.000467)\n"
            "    - LOW_EVT  # [QUALITY: LOW] - Marginal IG (0.000134)\n",
            encoding="utf-8",
        )

        registry_root = tmp_path / "registries"
        registry_root.mkdir()

        # Patch RegistryBundle to return a simple events dict
        mock_reg = MagicMock(spec=RegistryBundle)
        mock_reg.events = {
            "events": {
                "HIGH_EVT": {"enabled": True, "family": "F"},
                "LOW_EVT":  {"enabled": True, "family": "F"},
            }
        }

        with patch(
            "project.research.search_intelligence.RegistryBundle",
            return_value=mock_reg,
        ), patch(
            "project.research.search_intelligence.ensure_memory_store",
        ), patch(
            "project.research.search_intelligence.read_memory_table",
            return_value=pd.DataFrame(),
        ):
            result = update_search_intelligence(
                tmp_path,
                registry_root,
                "test_prog",
                frontier_untested_top_k=2,
                search_space_path=ss_path,
            )

        untested = result["frontier"]["untested_registry_events"]
        assert untested[0] == "HIGH_EVT", (
            f"HIGH_EVT should be first; got {untested}"
        )
        assert untested[1] == "LOW_EVT"
