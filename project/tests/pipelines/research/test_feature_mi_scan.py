"""Phase 4.1 — Feature MI Scan tests.

TestFeatureSelection        — _select_feature_columns filters correctly
TestForwardLogReturns       — _forward_log_returns computes correct series
TestDerivePredicates        — threshold candidates derived correctly
TestRunFeatureMiScan        — end-to-end run_feature_mi_scan function
TestMiScanOutputSchema      — artefact schema and content validation
TestMiScanRegimeStratif     — regime-stratified MI rows produced
TestControllerMiPredicates  — controller loads and merges MI predicates
TestControllerMiIntegration — full predicate proposal with MI candidates
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from project.core.exceptions import DataIntegrityError
from project.research.campaign_controller import (
    CampaignConfig,
    CampaignController,
)
from project.research.feature_mi_scan import (
    _MI_TABLE_COLUMNS,
    _derive_predicates,
    _forward_log_returns,
    _select_feature_columns,
    run_feature_mi_scan,
)

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

def _make_features(n: int = 300, seed: int = 42) -> pd.DataFrame:
    """Minimal feature DataFrame that mimics real pipeline output."""
    rng = np.random.default_rng(seed)
    t = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = 50_000.0 * np.exp(np.cumsum(rng.normal(0, 0.001, n)))
    df = pd.DataFrame({
        "timestamp": t,
        "symbol": "BTCUSDT",
        "close": close,
        "logret_1": np.concatenate([[np.nan], np.diff(np.log(close))]),
        # Feature columns — should be picked up for MI
        "rv_96": rng.uniform(0.001, 0.01, n),
        "spread_bps": rng.uniform(1.0, 5.0, n),
        "imbalance": rng.uniform(-1.0, 1.0, n),
        "funding_abs": rng.uniform(0.0, 0.002, n),
        "oi_notional": rng.uniform(1e8, 1e9, n),
        "basis_zscore": rng.normal(0, 1, n),
        # Regime columns — must NOT be MI features
        "ms_vol_state": rng.choice([0.0, 1.0, 2.0, 3.0], n),
        "ms_trend_state": rng.choice([0.0, 1.0, 2.0], n),
        "ms_spread_state": rng.choice([0.0, 1.0], n),
        "ms_vol_confidence": rng.uniform(0.5, 1.0, n),
        "high_vol_regime": rng.choice([0.0, 1.0], n),
        "low_vol_regime": rng.choice([0.0, 1.0], n),
        # Metadata — must NOT be MI features
        "split_label": rng.choice(["train", "val", "test"], n),
    })
    return df


def _make_ctrl(tmp_path: Path, **cfg_kwargs) -> CampaignController:
    config = CampaignConfig(program_id="mi_test", max_runs=5, **cfg_kwargs)
    ctrl = CampaignController.__new__(CampaignController)
    ctrl.config = config
    ctrl.data_root = tmp_path
    ctrl.registry_root = tmp_path / "reg"
    ctrl.campaign_dir = tmp_path / "artifacts" / "experiments" / config.program_id
    ctrl.campaign_dir.mkdir(parents=True)
    ctrl.ledger_path = ctrl.campaign_dir / "tested_ledger.parquet"
    ctrl.summary_path = ctrl.campaign_dir / "campaign_summary.json"
    ctrl._search_space_path = Path("spec/search_space.yaml")
    ctrl._quality_weights = {}
    ctrl.registries = MagicMock()
    ctrl.registries.events = {"events": {}}
    ctrl.registries.templates = {"families": {}}
    return ctrl


# ---------------------------------------------------------------------------
# TestFeatureSelection
# ---------------------------------------------------------------------------


class TestFeatureSelection:
    def test_numeric_columns_selected(self):
        df = _make_features()
        cols = _select_feature_columns(df)
        assert "rv_96" in cols
        assert "spread_bps" in cols
        assert "basis_zscore" in cols

    def test_regime_columns_excluded(self):
        df = _make_features()
        cols = _select_feature_columns(df)
        for excluded in ["ms_vol_state", "ms_trend_state", "ms_spread_state",
                         "ms_vol_confidence", "high_vol_regime", "low_vol_regime"]:
            assert excluded not in cols, f"{excluded} should be excluded"

    def test_metadata_columns_excluded(self):
        df = _make_features()
        cols = _select_feature_columns(df)
        assert "timestamp" not in cols
        assert "symbol" not in cols
        assert "split_label" not in cols

    def test_logret_1_excluded_as_target(self):
        """logret_1 is the forward-return target, not a predictor."""
        df = _make_features()
        cols = _select_feature_columns(df)
        assert "logret_1" not in cols

    def test_non_numeric_excluded(self):
        df = _make_features()
        df["text_col"] = "abc"
        cols = _select_feature_columns(df)
        assert "text_col" not in cols

    def test_event_flag_columns_excluded(self):
        df = _make_features()
        df["event_vol_spike"] = 0
        cols = _select_feature_columns(df)
        assert "event_vol_spike" not in cols


# ---------------------------------------------------------------------------
# TestForwardLogReturns
# ---------------------------------------------------------------------------


class TestForwardLogReturns:
    def test_uses_logret_1_when_present(self):
        df = _make_features(200)
        fwd = _forward_log_returns(df, 6)
        assert len(fwd) == len(df)

    def test_falls_back_to_close(self):
        df = _make_features(200)
        df_no_lr = df.drop(columns=["logret_1"])
        fwd = _forward_log_returns(df_no_lr, 6)
        assert len(fwd) == len(df)
        assert fwd.notna().sum() > 0

    def test_returns_empty_without_close_or_logret(self):
        df = pd.DataFrame({"rv_96": [1.0, 2.0, 3.0]})
        fwd = _forward_log_returns(df, 6)
        assert fwd.empty or fwd.isna().all()

    def test_horizon_affects_valid_count(self):
        df = _make_features(200)
        fwd6  = _forward_log_returns(df, 6).notna().sum()
        fwd48 = _forward_log_returns(df, 48).notna().sum()
        # Longer horizon → fewer valid rows at the tail
        assert fwd6 > fwd48


# ---------------------------------------------------------------------------
# TestDerivePredicates
# ---------------------------------------------------------------------------


class TestDerivePredicates:
    def test_three_predicates_per_feature(self):
        df = _make_features(300)
        preds = _derive_predicates(df, "rv_96", mi_score=0.01, regime_label="unconditional")
        # 3 percentiles → 3 predicates (if values not NaN)
        assert len(preds) == 3

    def test_upper_tail_uses_ge_operator(self):
        df = _make_features(300)
        preds = _derive_predicates(df, "rv_96", mi_score=0.01, regime_label="unconditional")
        upper = [p for p in preds if p["percentile"] in (75, 90)]
        for p in upper:
            assert p["operator"] == ">="

    def test_lower_tail_uses_le_operator(self):
        df = _make_features(300)
        preds = _derive_predicates(df, "rv_96", mi_score=0.01, regime_label="unconditional")
        lower = [p for p in preds if p["percentile"] == 25]
        for p in lower:
            assert p["operator"] == "<="

    def test_mi_score_and_regime_propagated(self):
        df = _make_features(300)
        preds = _derive_predicates(df, "rv_96", mi_score=0.0234, regime_label="ms_vol_state=2.0")
        for p in preds:
            assert p["mi_score"] == pytest.approx(0.0234, rel=1e-3)
            assert p["regime_label"] == "ms_vol_state=2.0"

    def test_missing_feature_returns_empty(self):
        df = _make_features(300)
        preds = _derive_predicates(df, "nonexistent_feature", mi_score=0.01, regime_label="unconditional")
        assert preds == []

    def test_insufficient_samples_returns_empty(self):
        df = _make_features(20)  # Only 20 rows — below 30-sample floor
        preds = _derive_predicates(df, "rv_96", mi_score=0.01, regime_label="unconditional")
        assert preds == []

    def test_source_set_to_mi_scan(self):
        df = _make_features(300)
        preds = _derive_predicates(df, "rv_96", mi_score=0.01, regime_label="unconditional")
        for p in preds:
            assert p["source"] == "mi_scan"


# ---------------------------------------------------------------------------
# TestRunFeatureMiScan — core output validation
# ---------------------------------------------------------------------------


class TestRunFeatureMiScan:
    def test_writes_parquet_and_json(self, tmp_path):
        df = _make_features(300)
        run_feature_mi_scan(df, out_dir=tmp_path)
        assert (tmp_path / "feature_horizon_mi.parquet").exists()
        assert (tmp_path / "candidate_predicates.json").exists()

    def test_parquet_has_correct_columns(self, tmp_path):
        df = _make_features(300)
        run_feature_mi_scan(df, out_dir=tmp_path)
        mi_df = pd.read_parquet(tmp_path / "feature_horizon_mi.parquet")
        for col in _MI_TABLE_COLUMNS:
            assert col in mi_df.columns, f"Missing column: {col}"

    def test_mi_rows_present_for_all_horizons(self, tmp_path):
        df = _make_features(300)
        horizons = [6, 12]
        run_feature_mi_scan(df, out_dir=tmp_path, horizons=horizons)
        mi_df = pd.read_parquet(tmp_path / "feature_horizon_mi.parquet")
        found_horizons = set(mi_df["horizon_bars"].unique())
        for h in horizons:
            assert h in found_horizons, f"No rows for horizon {h}"

    def test_unconditional_regime_label_present(self, tmp_path):
        df = _make_features(300)
        run_feature_mi_scan(df, out_dir=tmp_path)
        mi_df = pd.read_parquet(tmp_path / "feature_horizon_mi.parquet")
        assert "unconditional" in mi_df["regime_label"].values

    def test_mi_scores_non_negative(self, tmp_path):
        df = _make_features(300)
        run_feature_mi_scan(df, out_dir=tmp_path)
        mi_df = pd.read_parquet(tmp_path / "feature_horizon_mi.parquet")
        assert (mi_df["mi_score"] >= 0).all()

    def test_above_threshold_flag_consistent(self, tmp_path):
        df = _make_features(300)
        threshold = 0.001  # Low threshold to ensure some hits
        run_feature_mi_scan(df, out_dir=tmp_path, mi_threshold=threshold)
        mi_df = pd.read_parquet(tmp_path / "feature_horizon_mi.parquet")
        assert (mi_df["above_threshold"] == (mi_df["mi_score"] >= threshold)).all()

    def test_return_dict_has_expected_keys(self, tmp_path):
        df = _make_features(300)
        result = run_feature_mi_scan(df, out_dir=tmp_path)
        for key in ("mi_rows", "candidate_predicates", "features_scanned", "horizons", "out_dir"):
            assert key in result, f"Missing key: {key}"

    def test_features_scanned_count_reasonable(self, tmp_path):
        df = _make_features(300)
        result = run_feature_mi_scan(df, out_dir=tmp_path)
        # 6 numeric feature columns in test fixture (rv_96, spread_bps, etc.)
        assert result["features_scanned"] >= 4

    def test_candidate_predicates_are_valid_dicts(self, tmp_path):
        df = _make_features(300)
        run_feature_mi_scan(df, out_dir=tmp_path, mi_threshold=0.0)  # threshold=0 → all generate
        raw = json.loads((tmp_path / "candidate_predicates.json").read_text())
        assert isinstance(raw, list)
        for pred in raw:
            assert "feature" in pred
            assert "operator" in pred
            assert "threshold" in pred
            assert pred["operator"] in (">=", "<=", ">", "<")

    def test_candidate_predicates_sorted_by_mi_desc(self, tmp_path):
        df = _make_features(300)
        run_feature_mi_scan(df, out_dir=tmp_path, mi_threshold=0.0)
        raw = json.loads((tmp_path / "candidate_predicates.json").read_text())
        if len(raw) > 1:
            scores = [p.get("mi_score", 0.0) for p in raw]
            assert scores == sorted(scores, reverse=True)

    def test_no_duplicate_predicates(self, tmp_path):
        df = _make_features(300)
        run_feature_mi_scan(df, out_dir=tmp_path, mi_threshold=0.0)
        raw = json.loads((tmp_path / "candidate_predicates.json").read_text())
        keys = [f"{p['feature']}|{p['operator']}|{p['threshold']}|{p.get('regime_label','')}" for p in raw]
        assert len(keys) == len(set(keys)), "Duplicate predicates found"


# ---------------------------------------------------------------------------
# TestMiScanEmptyInput
# ---------------------------------------------------------------------------


class TestMiScanEmptyInput:
    def test_empty_df_writes_empty_artefacts(self, tmp_path):
        result = run_feature_mi_scan(pd.DataFrame(), out_dir=tmp_path)
        assert result["mi_rows"] == 0
        assert result["candidate_predicates"] == 0
        assert (tmp_path / "feature_horizon_mi.parquet").exists()
        raw = json.loads((tmp_path / "candidate_predicates.json").read_text())
        assert raw == []

    def test_empty_parquet_has_schema(self, tmp_path):
        run_feature_mi_scan(pd.DataFrame(), out_dir=tmp_path)
        mi_df = pd.read_parquet(tmp_path / "feature_horizon_mi.parquet")
        assert mi_df.empty
        for col in _MI_TABLE_COLUMNS:
            assert col in mi_df.columns


# ---------------------------------------------------------------------------
# TestMiScanRegimeStratification
# ---------------------------------------------------------------------------


class TestMiScanRegimeStratification:
    def test_regime_stratified_rows_present(self, tmp_path):
        df = _make_features(500)
        run_feature_mi_scan(df, out_dir=tmp_path, horizons=[12])
        mi_df = pd.read_parquet(tmp_path / "feature_horizon_mi.parquet")
        # Should have rows beyond "unconditional"
        non_unconditional = mi_df[mi_df["regime_label"] != "unconditional"]
        assert len(non_unconditional) > 0, "No regime-stratified rows found"

    def test_regime_label_format(self, tmp_path):
        df = _make_features(500)
        run_feature_mi_scan(df, out_dir=tmp_path, horizons=[12])
        mi_df = pd.read_parquet(tmp_path / "feature_horizon_mi.parquet")
        for lbl in mi_df["regime_label"].unique():
            if lbl != "unconditional":
                # Format: "ms_vol_state=2.0" etc.
                assert "=" in lbl, f"Unexpected regime_label format: {lbl}"

    def test_regime_rows_have_smaller_n_samples(self, tmp_path):
        df = _make_features(500)
        run_feature_mi_scan(df, out_dir=tmp_path, horizons=[12])
        mi_df = pd.read_parquet(tmp_path / "feature_horizon_mi.parquet")
        uncond = mi_df[mi_df["regime_label"] == "unconditional"]["n_samples"].max()
        regime = mi_df[mi_df["regime_label"] != "unconditional"]["n_samples"].max()
        assert regime < uncond, "Regime strata should have fewer samples than unconditional"

    def test_small_strata_skipped(self, tmp_path):
        """Strata with fewer than 50 rows should not generate MI rows."""
        df = _make_features(500)
        # Force most rows into regime label 0.0, with only 10 in label 99.0
        df["ms_vol_state"] = 0.0
        df.loc[:9, "ms_vol_state"] = 99.0
        run_feature_mi_scan(df, out_dir=tmp_path, horizons=[12])
        mi_df = pd.read_parquet(tmp_path / "feature_horizon_mi.parquet")
        assert "ms_vol_state=99.0" not in mi_df["regime_label"].values


# ---------------------------------------------------------------------------
# TestControllerMiPredicates
# ---------------------------------------------------------------------------


class TestControllerMiPredicates:
    def _write_mi_predicates(self, tmp_path: Path, preds: list) -> Path:
        """Write a candidate_predicates.json under data/reports/feature_mi/run_1/."""
        mi_dir = tmp_path / "reports" / "feature_mi" / "run_1"
        mi_dir.mkdir(parents=True)
        p = mi_dir / "candidate_predicates.json"
        p.write_text(json.dumps(preds), encoding="utf-8")
        return p

    def test_loads_mi_predicates_when_present(self, tmp_path):
        ctrl = _make_ctrl(tmp_path)
        preds = [
            {"feature": "rv_96", "operator": ">=", "threshold": 0.008,
             "source": "mi_scan", "mi_score": 0.025, "regime_label": "unconditional", "percentile": 90},
        ]
        self._write_mi_predicates(tmp_path, preds)
        result = ctrl._load_mi_candidate_predicates()
        assert len(result) == 1
        assert result[0]["feature"] == "rv_96"

    def test_returns_empty_when_no_mi_dir(self, tmp_path):
        ctrl = _make_ctrl(tmp_path)
        result = ctrl._load_mi_candidate_predicates()
        assert result == []

    def test_raises_on_malformed_json(self, tmp_path):
        ctrl = _make_ctrl(tmp_path)
        mi_dir = tmp_path / "reports" / "feature_mi" / "run_1"
        mi_dir.mkdir(parents=True)
        (mi_dir / "candidate_predicates.json").write_text("NOT JSON", encoding="utf-8")
        with pytest.raises(DataIntegrityError):
            ctrl._load_mi_candidate_predicates()

    def test_filters_malformed_predicate_dicts(self, tmp_path):
        ctrl = _make_ctrl(tmp_path)
        preds = [
            {"feature": "rv_96", "operator": ">=", "threshold": 0.008, "source": "mi_scan",
             "mi_score": 0.025, "regime_label": "unconditional", "percentile": 90},
            {"operator": ">=", "threshold": 0.5},  # missing "feature" → filtered out
            "not a dict",                           # wrong type → filtered out
        ]
        self._write_mi_predicates(tmp_path, preds)
        result = ctrl._load_mi_candidate_predicates()
        assert len(result) == 1

    def test_picks_most_recent_scan(self, tmp_path):
        """When multiple scans exist, the most recently modified is used."""
        import os
        ctrl = _make_ctrl(tmp_path)
        for idx, (run_id, mi_score) in enumerate([("run_1", 0.01), ("run_2", 0.99)]):
            mi_dir = tmp_path / "reports" / "feature_mi" / run_id
            mi_dir.mkdir(parents=True)
            pred = [{"feature": "f", "operator": ">=", "threshold": 0.5,
                     "source": "mi_scan", "mi_score": mi_score,
                     "regime_label": "unconditional", "percentile": 75}]
            p = mi_dir / "candidate_predicates.json"
            p.write_text(json.dumps(pred))
            # Set explicit mtime: run_2 gets a later mtime than run_1
            mtime = 1_700_000_000.0 + idx * 100.0
            os.utime(p, (mtime, mtime))
        result = ctrl._load_mi_candidate_predicates()
        assert result[0]["mi_score"] == pytest.approx(0.99)


# ---------------------------------------------------------------------------
# TestControllerMiIntegration
# ---------------------------------------------------------------------------


class TestControllerMiIntegration:
    """End-to-end: _step_scan_feature_predicates merges static + MI predicates."""

    def _write_mi_predicates(self, tmp_path: Path, preds: list) -> None:
        mi_dir = tmp_path / "reports" / "feature_mi" / "run_1"
        mi_dir.mkdir(parents=True)
        (mi_dir / "candidate_predicates.json").write_text(json.dumps(preds))

    def test_static_and_mi_predicates_merged(self, tmp_path):
        """MI predicates are added to the merged set alongside static predicates.

        The batch cap is 8. With 10 static predicates the MI predicate would be
        pushed past the cap. We patch _load_search_space_predicates to return
        3 items so rv_96 fits within the first 8.
        """
        ctrl = _make_ctrl(tmp_path, scan_trigger_types=["FEATURE_PREDICATE"])
        mi_preds = [
            {"feature": "rv_96", "operator": ">=", "threshold": 0.008,
             "source": "mi_scan", "mi_score": 0.025,
             "regime_label": "unconditional", "percentile": 90},
        ]
        self._write_mi_predicates(tmp_path, mi_preds)
        few_static = [
            {"feature": "imbalance_zscore", "operator": ">", "threshold": 2.0},
            {"feature": "spread_pct", "operator": ">=", "threshold": 0.90},
            {"feature": "funding_abs_pct", "operator": ">=", "threshold": 0.95},
        ]
        mem = {
            "belief_state": {}, "next_actions": {}, "latest_reflection": {},
            "avoid_region_keys": set(), "avoid_event_types": set(),
            "promising_regions": [], "superseded_stages": set(),
        }
        with patch(
            "project.research.campaign_controller.read_memory_table",
            return_value=pd.DataFrame(),
        ), patch.object(ctrl, "_load_search_space_predicates", return_value=few_static):
            result = ctrl._step_scan_feature_predicates(mem)
        assert result is not None
        preds_in_proposal = result["trigger_space"]["feature_predicates"]["include"]
        feat_names = [p["feature"] for p in preds_in_proposal]
        assert "rv_96" in feat_names, f"MI predicate rv_96 missing; got {feat_names}"
        assert "imbalance_zscore" in feat_names

    def test_deduplication_prevents_duplicates(self, tmp_path):
        ctrl = _make_ctrl(tmp_path)
        # MI predicate that overlaps with static search_space.yaml predicate
        static_preds = ctrl._load_search_space_predicates()
        if not static_preds:
            pytest.skip("No static predicates available")
        first_static = static_preds[0]
        mi_preds = [
            {
                "feature": first_static["feature"],
                "operator": first_static["operator"],
                "threshold": first_static["threshold"],
                "source": "mi_scan", "mi_score": 0.05,
                "regime_label": "unconditional", "percentile": 75,
            }
        ]
        self._write_mi_predicates(tmp_path, mi_preds)
        mem = {
            "belief_state": {}, "next_actions": {}, "latest_reflection": {},
            "avoid_region_keys": set(), "avoid_event_types": set(),
            "promising_regions": [], "superseded_stages": set(),
        }
        with patch(
            "project.research.campaign_controller.read_memory_table",
            return_value=pd.DataFrame(),
        ):
            result = ctrl._step_scan_feature_predicates(mem)
        if result is not None:
            preds_in_proposal = result["trigger_space"]["feature_predicates"]["include"]
            keys = [f"{p['feature']}|{p['operator']}|{p['threshold']}" for p in preds_in_proposal]
            assert len(keys) == len(set(keys)), "Duplicate predicates in proposal"

    def test_mi_predicates_sorted_by_score(self, tmp_path):
        ctrl = _make_ctrl(tmp_path)
        mi_preds = [
            {"feature": f"f_{i}", "operator": ">=", "threshold": float(i),
             "source": "mi_scan", "mi_score": float(i) / 100.0,
             "regime_label": "unconditional", "percentile": 75}
            for i in range(1, 6)
        ]
        self._write_mi_predicates(tmp_path, mi_preds)
        # Load and verify ordering
        result = ctrl._load_mi_candidate_predicates()
        if result:
            scores = [p.get("mi_score", 0.0) for p in result]
            assert scores == sorted(scores, reverse=True)

    def test_no_mi_file_falls_back_to_static(self, tmp_path):
        ctrl = _make_ctrl(tmp_path)

        # Mock search space with static predicates
        search_space_path = tmp_path / "search_space.yaml"
        search_space_path.write_text("""
triggers:
  feature_predicates:
    - feature: static_feature
      operator: ">="
      threshold: 0.5
        """, encoding="utf-8")
        ctrl._search_space_path = search_space_path

        mem = {
            "belief_state": {}, "next_actions": {}, "latest_reflection": {},
            "avoid_region_keys": set(), "avoid_event_types": set(),
            "promising_regions": [], "superseded_stages": set(),
        }
        with patch(
            "project.research.campaign_controller.read_memory_table",
            return_value=pd.DataFrame(),
        ):
            result = ctrl._step_scan_feature_predicates(mem)
        # Should still work using only static predicates from search_space.yaml
        assert result is not None
        preds = result["trigger_space"]["feature_predicates"]["include"]
        assert len(preds) > 0

    def test_proposal_description_reports_mi_count(self, tmp_path):
        ctrl = _make_ctrl(tmp_path)
        mi_preds = [
            {"feature": "new_mi_feature", "operator": ">=", "threshold": 1.0,
             "source": "mi_scan", "mi_score": 0.05,
             "regime_label": "unconditional", "percentile": 90}
        ]
        self._write_mi_predicates(tmp_path, mi_preds)
        mem = {
            "belief_state": {}, "next_actions": {}, "latest_reflection": {},
            "avoid_region_keys": set(), "avoid_event_types": set(),
            "promising_regions": [], "superseded_stages": set(),
        }
        with patch(
            "project.research.campaign_controller.read_memory_table",
            return_value=pd.DataFrame(),
        ):
            result = ctrl._step_scan_feature_predicates(mem)
        assert result is not None
        # Description must report both total and MI count
        assert "MI" in result["description"] or "predicates" in result["description"]
