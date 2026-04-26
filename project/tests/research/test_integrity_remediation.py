"""Tests for the integrity fixes from the Edge v11 definitive deep analysis.

Covers:
  - E1: Concept ledger symbol isolation (primary_symbol in lineage key)
  - E3: Diversified shortlist determinism (stable pre-sort before MMR)
  - E4: Stage score gate-relative t_norm normalisation
  - E5: build_repeated_walkforward_splits diagnostic WARNING on empty-fold return
  - B1: Benchmark spec references only canonical registry events (no phantom events)
  - Tier 6: TriggerFeatureColumns non-mutation contract
"""

import logging

import pandas as pd
import pytest
import yaml

from project import PROJECT_ROOT

# ─────────────────────────────────────────────────────────────────────────────
# E1: Concept ledger symbol isolation
# ─────────────────────────────────────────────────────────────────────────────

class TestConceptLineageKeySymbolIsolation:
    def _make_row(self, symbol: str) -> dict:
        return {
            "event_family": "VOLATILITY_SHOCK",
            "template_id": "continuation",
            "direction": "long",
            "timeframe": "5m",
            "horizon_bars": 12,
            "symbol": symbol,
        }

    def test_different_symbols_produce_different_keys(self):
        from project.research.knowledge.concept_ledger import build_concept_lineage_key

        btc_key = build_concept_lineage_key(self._make_row("BTCUSDT"))
        eth_key = build_concept_lineage_key(self._make_row("ETHUSDT"))
        sol_key = build_concept_lineage_key(self._make_row("SOLUSDT"))

        assert btc_key != eth_key, "BTC and ETH must have distinct lineage keys"
        assert btc_key != sol_key, "BTC and SOL must have distinct lineage keys"
        assert eth_key != sol_key, "ETH and SOL must have distinct lineage keys"

    def test_same_symbol_same_key(self):
        from project.research.knowledge.concept_ledger import build_concept_lineage_key

        key1 = build_concept_lineage_key(self._make_row("BTCUSDT"))
        key2 = build_concept_lineage_key(self._make_row("BTCUSDT"))
        assert key1 == key2, "Same row must produce the same key (deterministic)"

    def test_sym_id_segment_present_in_key(self):
        from project.research.knowledge.concept_ledger import build_concept_lineage_key

        key = build_concept_lineage_key(self._make_row("BTCUSDT"))
        assert "SYM_ID:" in key, "Key must contain SYM_ID segment"
        assert "BTCUSDT" in key, "Key must contain the normalised symbol"

    def test_missing_symbol_falls_back_to_any(self):
        from project.research.knowledge.concept_ledger import build_concept_lineage_key

        row = self._make_row("")
        row.pop("symbol")
        key = build_concept_lineage_key(row)
        assert "SYM_ID:any" in key, "Missing symbol must produce SYM_ID:any"

    def test_primary_symbol_in_ledger_columns(self):
        from project.research.knowledge.concept_ledger import CONCEPT_LEDGER_COLUMNS

        assert "primary_symbol" in CONCEPT_LEDGER_COLUMNS, (
            "primary_symbol must be a stored ledger column"
        )


# ─────────────────────────────────────────────────────────────────────────────
# E3: Diversified shortlist determinism
# ─────────────────────────────────────────────────────────────────────────────

class TestDiversifiedShortlistDeterminism:
    def _make_candidates(self, n: int = 20, seed: int = 42) -> pd.DataFrame:
        import numpy as np
        rng = np.random.default_rng(seed)
        ids = [f"cand_{i:04d}" for i in range(n)]
        scores = rng.uniform(0.1, 1.0, size=n)
        return pd.DataFrame({
            "candidate_id": ids,
            "discovery_quality_score": scores,
            "event_family": ["VOL_SPIKE"] * n,
            "concept_lineage_key": [f"key_{i}" for i in range(n)],
        })

    def test_shuffled_input_produces_same_shortlist(self):
        from project.research.services.candidate_diversification import select_diversified_shortlist

        base = self._make_candidates(30)
        shuffled = base.sample(frac=1, random_state=99).reset_index(drop=True)

        result_base = select_diversified_shortlist(base, size=5)
        result_shuffled = select_diversified_shortlist(shuffled, size=5)

        assert not result_base.empty, "Base shortlist must not be empty"
        assert not result_shuffled.empty, "Shuffled shortlist must not be empty"

        # The selected candidate_ids must be the same set regardless of input order
        ids_base = set(result_base["candidate_id"].tolist())
        ids_shuffled = set(result_shuffled["candidate_id"].tolist())
        assert ids_base == ids_shuffled, (
            f"Shortlist must be deterministic regardless of input order.\n"
            f"Base: {sorted(ids_base)}\nShuffled: {sorted(ids_shuffled)}"
        )

    def test_identical_quality_scores_still_deterministic(self):
        """Tie-breaking via candidate_id must be lexicographic and stable."""
        from project.research.services.candidate_diversification import select_diversified_shortlist

        # All candidates share the same quality score — pure tie-break scenario
        ids = [f"cand_{c}" for c in ["Z", "A", "M", "B", "Q"]]
        df = pd.DataFrame({
            "candidate_id": ids,
            "discovery_quality_score": [0.5] * 5,
            "event_family": ["EV"] * 5,
            "concept_lineage_key": [f"k{i}" for i in range(5)],
        })
        shuffled = df.sample(frac=1, random_state=7).reset_index(drop=True)

        r1 = select_diversified_shortlist(df, size=3)
        r2 = select_diversified_shortlist(shuffled, size=3)

        assert not r1.empty
        assert set(r1["candidate_id"]) == set(r2["candidate_id"]), (
            "Tie-breaking must produce the same candidates regardless of input row order"
        )


# ─────────────────────────────────────────────────────────────────────────────
# E4: Stage score gate-relative t_norm
# ─────────────────────────────────────────────────────────────────────────────

class TestStagePolicytNormRecalibration:
    def _score(self, t: float) -> float:
        from project.research.search.stage_policy import _T_MIN_GATE, _clamp
        return _clamp((abs(t) - _T_MIN_GATE) / (3.0 - _T_MIN_GATE), 0.0, 1.0)

    def test_t_norm_at_minimum_gate_is_zero(self):
        assert self._score(1.5) == pytest.approx(0.0), (
            "t=1.5 (minimum gate) must map to t_norm=0.0"
        )

    def test_t_norm_at_cap_is_one(self):
        assert self._score(3.0) == pytest.approx(1.0), (
            "t=3.0 (cap) must map to t_norm=1.0"
        )

    def test_t_norm_below_gate_clamps_to_zero(self):
        assert self._score(0.0) == pytest.approx(0.0), (
            "t<1.5 must clamp to t_norm=0.0"
        )
        assert self._score(1.4) == pytest.approx(0.0), (
            "t<1.5 must clamp to t_norm=0.0"
        )

    def test_t_norm_above_cap_clamps_to_one(self):
        assert self._score(5.0) == pytest.approx(1.0), (
            "t>3.0 must clamp to t_norm=1.0"
        )

    def test_t_norm_midpoint(self):
        # t=2.25 is midpoint of [1.5, 3.0]
        assert self._score(2.25) == pytest.approx(0.5, abs=1e-9), (
            "t=2.25 (midpoint of [1.5, 3.0]) must map to t_norm=0.5"
        )

    def test_t_norm_is_monotone(self):
        """t_norm must be non-decreasing in abs(t)."""
        prev = self._score(0.0)
        for t in [1.0, 1.5, 2.0, 2.5, 3.0, 4.0]:
            curr = self._score(t)
            assert curr >= prev - 1e-12, f"t_norm not monotone at t={t}"
            prev = curr

    def test_t_min_gate_constant_value(self):
        from project.research.search.stage_policy import _T_MIN_GATE
        assert pytest.approx(1.5) == _T_MIN_GATE, (
            "_T_MIN_GATE must match the discovery pipeline minimum t-stat gate"
        )


# ─────────────────────────────────────────────────────────────────────────────
# E5: build_repeated_walkforward_splits diagnostic WARNING
# ─────────────────────────────────────────────────────────────────────────────

class TestWalkForwardSplitsDiagnostics:
    def _timestamps(self, n_bars: int) -> pd.Series:
        """Build a minimal timestamp series of n_bars 5-minute bars."""
        return pd.Series(pd.date_range("2024-01-01", periods=n_bars, freq="5min"))

    def test_insufficient_data_returns_empty_list(self):
        from project.research.validation.splits import build_repeated_walkforward_splits

        ts = self._timestamps(100)  # Way too few for train_bars=2000
        result = build_repeated_walkforward_splits(
            ts,
            train_bars=2000,
            validation_bars=500,
            test_bars=500,
            step_bars=500,
            min_folds=3,
            max_folds=6,
            purge_bars=0,
            embargo_bars=0,
        )
        assert result == [], "Insufficient data must return empty list"

    def test_insufficient_data_emits_warning(self, caplog):
        from project.research.validation.splits import build_repeated_walkforward_splits

        ts = self._timestamps(100)
        with caplog.at_level(logging.WARNING, logger="project.research.validation.splits"):
            build_repeated_walkforward_splits(
                ts,
                train_bars=2000,
                validation_bars=500,
                test_bars=500,
                step_bars=500,
                min_folds=3,
                max_folds=6,
                purge_bars=0,
                embargo_bars=0,
            )

        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("min_folds" in m for m in warning_messages), (
            "A WARNING mentioning 'min_folds' must be logged when fold count is insufficient"
        )
        assert any("total_bars" in m or "Returning empty fold list" in m for m in warning_messages), (
            "WARNING must include diagnostic context (total_bars or fold-list info)"
        )

    def test_warning_includes_actionable_counts(self, caplog):
        from project.research.validation.splits import build_repeated_walkforward_splits

        ts = self._timestamps(50)
        with caplog.at_level(logging.WARNING, logger="project.research.validation.splits"):
            build_repeated_walkforward_splits(
                ts,
                train_bars=500,
                validation_bars=200,
                test_bars=200,
                step_bars=200,
                min_folds=5,
                max_folds=10,
                purge_bars=0,
                embargo_bars=0,
            )

        all_warnings = " ".join(r.message for r in caplog.records if r.levelno == logging.WARNING)
        # The warning should contain enough info for a developer to diagnose the problem
        assert "min_folds" in all_warnings
        assert "5" in all_warnings  # the actual min_folds value

    def test_max_folds_are_spread_across_available_window(self):
        from project.research.validation.splits import build_repeated_walkforward_splits

        ts = self._timestamps(20_000)

        folds = build_repeated_walkforward_splits(
            ts,
            train_bars=1000,
            validation_bars=100,
            test_bars=100,
            step_bars=100,
            min_folds=3,
            max_folds=3,
            purge_bars=0,
            embargo_bars=0,
        )

        assert [fold.fold_id for fold in folds] == [1, 2, 3]
        test_starts = [pd.Timestamp(fold.test_split.start) for fold in folds]
        assert test_starts[0] < test_starts[1] < test_starts[2]
        assert test_starts[2].tz_localize(None) > ts.iloc[-1000]

    def test_phase2_fold_builder_fails_closed_when_required_folds_are_empty(self, tmp_path):
        from project.research.phase2_search_engine import _build_required_walkforward_folds

        config_path = tmp_path / "discovery_validation.yaml"
        config_path.write_text(
            yaml.safe_dump(
                {
                    "discovery_validation": {
                        "repeated_walkforward": {
                            "enabled": True,
                            "train_bars": 2000,
                            "validation_bars": 500,
                            "test_bars": 500,
                            "step_bars": 500,
                            "min_folds": 3,
                            "max_folds": 6,
                            "purge_bars": 0,
                            "embargo_bars": 0,
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        features = pd.DataFrame({"timestamp": self._timestamps(100)})

        with pytest.raises(RuntimeError, match="produced 0 folds"):
            _build_required_walkforward_folds(features, config_path)


class TestPhase2DiversificationFallback:
    def test_fallback_columns_make_diversification_failure_observable(self):
        from project.research.phase2_search_engine import (
            _ensure_diversification_fallback_columns,
        )

        candidates = pd.DataFrame({"candidate_id": ["cand_1"], "t_stat": [2.1]})
        result = _ensure_diversification_fallback_columns(candidates, "boom")

        assert result.loc[0, "overlap_cluster_id"] is None
        assert result.loc[0, "cluster_size"] == 1
        assert result.loc[0, "novelty_score"] == pytest.approx(1.0)
        assert bool(result.loc[0, "selected_into_diversified_shortlist"]) is False
        assert bool(result.loc[0, "_diversification_error"]) is True
        assert result.loc[0, "_diversification_error_reason"] == "boom"


# ─────────────────────────────────────────────────────────────────────────────
# Tier 6: TriggerFeatureColumns non-mutation contract
# ─────────────────────────────────────────────────────────────────────────────

class TestTriggerFeatureColumnsContract:
    def _make_features(self) -> pd.DataFrame:
        import numpy as np
        ts = pd.date_range("2024-01-01", periods=200, freq="5min")
        rng = np.random.default_rng(0)
        return pd.DataFrame({
            "timestamp": ts,
            "realized_vol_30": rng.uniform(0.001, 0.05, 200),
            "close": rng.uniform(40000, 70000, 200),
        })

    def test_apply_to_features_does_not_mutate_original(self):
        from project.research.trigger_discovery.candidate_generation import TriggerFeatureColumns

        features = self._make_features()
        original_cols = set(features.columns)

        injected_series = (features["realized_vol_30"] > 0.03).astype(bool)
        tfc = TriggerFeatureColumns(columns={"PROPOSED_TEST_EVENT": injected_series})

        augmented = tfc.apply_to_features(features)

        # Original frame must be unchanged
        assert set(features.columns) == original_cols, (
            "apply_to_features must not mutate the original features DataFrame"
        )
        # Augmented frame must have the new column
        assert "PROPOSED_TEST_EVENT" in augmented.columns, (
            "Augmented frame must contain injected column"
        )

    def test_empty_trigger_cols_returns_copy(self):
        from project.research.trigger_discovery.candidate_generation import TriggerFeatureColumns

        features = self._make_features()
        tfc = TriggerFeatureColumns()
        result = tfc.apply_to_features(features)

        # Should be a copy, not the original object
        assert result is not features, "Empty TriggerFeatureColumns must still return a copy"
        assert list(result.columns) == list(features.columns)

    def test_column_names_method(self):
        import pandas as pd

        from project.research.trigger_discovery.candidate_generation import TriggerFeatureColumns

        s = pd.Series([True, False, True])
        tfc = TriggerFeatureColumns(columns={"COL_A": s, "COL_B": s})
        assert set(tfc.column_names()) == {"COL_A", "COL_B"}

    def test_package_exports(self):
        from project.research.trigger_discovery import TriggerFeatureColumns, TriggerProposal
        assert TriggerFeatureColumns is not None
        assert TriggerProposal is not None


# ─────────────────────────────────────────────────────────────────────────────
# B1: Benchmark spec integrity — no phantom events
# ─────────────────────────────────────────────────────────────────────────────

PHANTOM_EVENTS = [
    "LONG_WIKI_BREAKOUT",
    "RSI_OVERSOLD",
    "MACD_CROSSOVER",
    "EMA_CROSS_20_50",
    "RANDOM_NOISE_EVENT",
]

REGISTRY_PATH = PROJECT_ROOT.parent / "spec/events/event_registry_unified.yaml"
BENCHMARK_SPEC_PATH = PROJECT_ROOT.parent / "project/research/benchmarks/discovery_benchmark_spec.yaml"


class TestBenchmarkSpecIntegrity:
    def test_no_phantom_events_in_spec(self):
        if not BENCHMARK_SPEC_PATH.exists():
            pytest.skip("Benchmark spec not found")

        spec_text = BENCHMARK_SPEC_PATH.read_text()
        for phantom in PHANTOM_EVENTS:
            assert phantom not in spec_text, (
                f"Phantom event '{phantom}' found in benchmark spec. "
                f"All events must exist in {REGISTRY_PATH.name}."
            )

    def test_benchmark_events_exist_in_registry(self):
        if not BENCHMARK_SPEC_PATH.exists():
            pytest.skip("Benchmark spec not found")
        if not REGISTRY_PATH.exists():
            pytest.skip("Event registry not found")

        with open(BENCHMARK_SPEC_PATH) as f:
            spec = yaml.safe_load(f)
        registry_text = REGISTRY_PATH.read_text()

        for case in spec.get("cases", []):
            events = case.get("search_spec", {}).get("events", [])
            for event_id in events:
                assert event_id in registry_text, (
                    f"Benchmark case '{case['id']}' references event '{event_id}' "
                    f"which was not found in {REGISTRY_PATH.name}. "
                    "Add the event to the registry or update the benchmark spec."
                )

    def test_benchmark_symbols_use_perpetual_notation(self):
        if not BENCHMARK_SPEC_PATH.exists():
            pytest.skip("Benchmark spec not found")

        spec_text = BENCHMARK_SPEC_PATH.read_text()
        spot_notation_patterns = ["BTC-USD", "ETH-USD", "SOL-USD", "BTC/USD", "ETH/USD"]
        for pattern in spot_notation_patterns:
            assert pattern not in spec_text, (
                f"Spot notation '{pattern}' found in benchmark spec. "
                "Use perpetual format (BTCUSDT, ETHUSDT, SOLUSDT)."
            )
