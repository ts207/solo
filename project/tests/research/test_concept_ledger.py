"""
Tests for Phase 3 concept-ledger multiplicity correction.

Covers:
  - Lineage key determinism and bucketing
  - Ledger storage (load / append / dedup)
  - Lineage history summarization (full window and recent window)
  - Scoring: penalty model, v3 score, demotion reason codes
  - Regression: legacy columns unchanged when flag is off
  - Fault tolerance: ledger write failure does not raise
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from project.research.knowledge.concept_ledger import (
    CONCEPT_LEDGER_COLUMNS,
    _horizon_bucket,
    _normalize_direction,
    _template_family,
    append_concept_ledger,
    build_concept_lineage_key,
    build_ledger_records,
    default_ledger_path,
    load_concept_ledger,
    summarize_lineage_history,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_candidate(**kwargs) -> dict:
    defaults = {
        "event_type": "VOL_SHOCK",
        "event_family": "VOL_SHOCK",
        "rule_template": "continuation",
        "direction": "1.0",
        "timeframe": "5m",
        "horizon_bars": 24,
        "symbol": "BTCUSDT",
        "candidate_id": "cand_001",
        "is_discovery": True,
        "q_value": 0.04,
        "estimate_bps": 8.5,
        "discovery_quality_score": 2.1,
    }
    defaults.update(kwargs)
    return defaults


def _make_ledger_row(
    lineage_key: str,
    run_id: str = "run_001",
    is_discovery: bool = False,
    days_ago: int = 30,
) -> dict:
    ts = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    return {
        "ledger_id": f"{run_id}_{lineage_key}_{days_ago}",
        "run_id": run_id,
        "program_id": "prog_001",
        "candidate_id": "cand_001",
        "concept_lineage_key": lineage_key,
        "event_type": "VOL_SHOCK",
        "event_family": "VOL_SHOCK",
        "template_id": "continuation",
        "direction": "long",
        "timeframe": "5m",
        "horizon_bars": 24,
        "symbol_scope_type": "single",
        "context_dim_count": 0,
        "tested_at": ts,
        "is_discovery": is_discovery,
        "passed_sample_quality": True,
        "passed_promotion": False,
        "adjusted_q_value": 0.04,
        "after_cost_expectancy_bps": 8.5,
        "discovery_quality_score": 2.1,
    }


def _make_synthetic_ledger(
    lineage_key: str = "EVENT:VOL_SHOCK|TMPL:continuation|DIR:long|TF:5m|H:short|SYM:single|CTX:0",
    n_rows: int = 10,
    n_discoveries: int = 1,
    days_ago: int = 60,
) -> pd.DataFrame:
    rows = []
    for i in range(n_rows):
        rows.append(
            _make_ledger_row(
                lineage_key,
                run_id=f"run_{i:03d}",
                is_discovery=(i < n_discoveries),
                days_ago=days_ago,
            )
        )
    return pd.DataFrame(rows).reindex(columns=CONCEPT_LEDGER_COLUMNS)


# ---------------------------------------------------------------------------
# Lineage key tests
# ---------------------------------------------------------------------------

class TestLineageKey:
    def test_deterministic(self):
        row = _make_candidate()
        assert build_concept_lineage_key(row) == build_concept_lineage_key(row)

    def test_same_concept_same_key(self):
        row1 = _make_candidate(candidate_id="x")
        row2 = _make_candidate(candidate_id="y")
        assert build_concept_lineage_key(row1) == build_concept_lineage_key(row2)

    def test_different_template_different_key(self):
        row_cont = _make_candidate(rule_template="continuation")
        row_rev = _make_candidate(rule_template="mean_reversion")
        assert build_concept_lineage_key(row_cont) != build_concept_lineage_key(row_rev)

    def test_different_direction_different_key(self):
        row_long = _make_candidate(direction="1.0")
        row_short = _make_candidate(direction="-1.0")
        assert build_concept_lineage_key(row_long) != build_concept_lineage_key(row_short)

    def test_key_contains_event_family(self):
        row = _make_candidate(event_family="VOL_SHOCK")
        key = build_concept_lineage_key(row)
        assert "VOL_SHOCK" in key

    def test_horizon_bucketing_short(self):
        row_12 = _make_candidate(horizon_bars=12)
        row_24 = _make_candidate(horizon_bars=24)
        # Both in short bucket
        assert build_concept_lineage_key(row_12) == build_concept_lineage_key(row_24)

    def test_horizon_bucketing_medium_vs_short(self):
        row_short = _make_candidate(horizon_bars=24)
        row_medium = _make_candidate(horizon_bars=48)
        assert build_concept_lineage_key(row_short) != build_concept_lineage_key(row_medium)

    def test_horizon_bucketing_long(self):
        row = _make_candidate(horizon_bars=96)
        key = build_concept_lineage_key(row)
        assert "H:long" in key

    def test_normalize_direction_variants(self):
        assert _normalize_direction("1.0") == "long"
        assert _normalize_direction("-1.0") == "short"
        assert _normalize_direction("long") == "long"
        assert _normalize_direction("short") == "short"
        assert _normalize_direction("0") == "neutral"
        assert _normalize_direction("") == "neutral"

    def test_template_family_extraction(self):
        assert _template_family("continuation") == "continuation"
        assert _template_family("mean_reversion_v2") == "mean"
        assert _template_family("") == "unknown"

    def test_horizon_bucket_boundaries(self):
        assert _horizon_bucket(1) == "short"
        assert _horizon_bucket(24) == "short"
        assert _horizon_bucket(25) == "medium"
        assert _horizon_bucket(48) == "medium"
        assert _horizon_bucket(49) == "long"
        assert _horizon_bucket(0) == "unknown"

    def test_missing_fields_graceful(self):
        # Should not raise even with empty dict
        key = build_concept_lineage_key({})
        assert isinstance(key, str)
        assert len(key) > 0


# ---------------------------------------------------------------------------
# Ledger storage tests
# ---------------------------------------------------------------------------

class TestLedgerStorage:
    def test_load_nonexistent_returns_empty(self, tmp_path):
        ledger = load_concept_ledger(tmp_path / "nonexistent.parquet")
        assert isinstance(ledger, pd.DataFrame)
        assert ledger.empty
        for col in CONCEPT_LEDGER_COLUMNS:
            assert col in ledger.columns

    def test_append_creates_file(self, tmp_path):
        path = tmp_path / "ledger.parquet"
        assert not path.exists()
        row = pd.DataFrame([_make_ledger_row("KEY:A")])
        append_concept_ledger(row, path)
        assert path.exists()

    def test_append_accumulates(self, tmp_path):
        path = tmp_path / "ledger.parquet"
        row1 = pd.DataFrame([_make_ledger_row("KEY:A", run_id="run_001")])
        row2 = pd.DataFrame([_make_ledger_row("KEY:B", run_id="run_002")])
        append_concept_ledger(row1, path)
        append_concept_ledger(row2, path)
        ledger = load_concept_ledger(path)
        assert len(ledger) == 2

    def test_append_idempotent_by_ledger_id(self, tmp_path):
        path = tmp_path / "ledger.parquet"
        row = pd.DataFrame([_make_ledger_row("KEY:A", run_id="run_001")])
        # Same row twice — deduplication should keep only one
        append_concept_ledger(row, path)
        append_concept_ledger(row, path)
        ledger = load_concept_ledger(path)
        assert len(ledger) == 1

    def test_load_schema_complete(self, tmp_path):
        path = tmp_path / "ledger.parquet"
        row = pd.DataFrame([_make_ledger_row("KEY:A")])
        append_concept_ledger(row, path)
        ledger = load_concept_ledger(path)
        for col in CONCEPT_LEDGER_COLUMNS:
            assert col in ledger.columns

    def test_load_strict_mode_raises_on_read_failure(self, tmp_path, monkeypatch):
        import project.research.knowledge.concept_ledger as concept_ledger

        path = tmp_path / "ledger.parquet"
        path.write_bytes(b"not parquet")

        def _fail_read(*_args, **_kwargs):
            raise RuntimeError("corrupt ledger")

        monkeypatch.setattr(concept_ledger, "read_parquet", _fail_read)

        with pytest.raises(RuntimeError, match="corrupt ledger"):
            load_concept_ledger(path, raise_on_error=True)

    def test_append_exception_does_not_raise(self, tmp_path):
        """append_concept_ledger must never propagate exceptions (safe for runs)."""
        bad_path = tmp_path / "no_dir" / "no_subdir" / "ledger.parquet"
        row = pd.DataFrame([_make_ledger_row("KEY:A")])
        # Should not raise (directory creation is attempted internally)
        try:
            append_concept_ledger(row, bad_path)
        except Exception:
            pytest.fail("append_concept_ledger raised an exception")

    def test_append_strict_mode_raises_on_write_failure(self, tmp_path, monkeypatch):
        import project.research.knowledge.concept_ledger as concept_ledger

        def _fail_write(*_args, **_kwargs):
            raise RuntimeError("disk full")

        monkeypatch.setattr(concept_ledger, "write_parquet", _fail_write)
        row = pd.DataFrame([_make_ledger_row("KEY:A")])

        with pytest.raises(RuntimeError, match="disk full"):
            append_concept_ledger(row, tmp_path / "ledger.parquet", raise_on_error=True)

    def test_default_ledger_path(self, tmp_path):
        path = default_ledger_path(tmp_path)
        assert str(path).startswith(str(tmp_path))
        assert path.name == "concept_ledger.parquet"


# ---------------------------------------------------------------------------
# summarize_lineage_history tests
# ---------------------------------------------------------------------------

class TestSummarizeLineageHistory:
    def test_zero_history_returns_zeros(self):
        ledger = pd.DataFrame(columns=CONCEPT_LEDGER_COLUMNS)
        summary = summarize_lineage_history(ledger, ["KEY:NOVEL"])
        assert len(summary) == 1
        row = summary.iloc[0]
        assert row["ledger_prior_test_count"] == 0
        assert row["ledger_empirical_success_rate"] == 0.0
        assert not pd.isna(row["ledger_prior_test_count"])

    def test_prior_counts_correct(self):
        key = "EVENT:VOL_SHOCK|TMPL:cont|DIR:long|TF:5m|H:short|SYM:single|CTX:0"
        ledger = _make_synthetic_ledger(key, n_rows=10, n_discoveries=3)
        summary = summarize_lineage_history(ledger, [key], lookback_days=365)
        row = summary.iloc[0]
        assert row["ledger_prior_test_count"] == 10
        assert row["ledger_prior_discovery_count"] == 3

    def test_empirical_success_rate(self):
        key = "KEY:X"
        ledger = _make_synthetic_ledger(key, n_rows=10, n_discoveries=2)
        summary = summarize_lineage_history(ledger, [key])
        row = summary.iloc[0]
        assert abs(row["ledger_empirical_success_rate"] - 0.2) < 1e-6

    def test_recent_window_filters(self):
        key = "KEY:Z"
        # Old records (150 days ago) — beyond recent window
        old_rows = [_make_ledger_row(key, run_id=f"old_{i}", days_ago=150) for i in range(8)]
        # Recent records (30 days ago)
        new_rows = [_make_ledger_row(key, run_id=f"new_{i}", days_ago=30) for i in range(2)]
        ledger = pd.DataFrame(old_rows + new_rows).reindex(columns=CONCEPT_LEDGER_COLUMNS)
        summary = summarize_lineage_history(ledger, [key], recent_window_days=90)
        row = summary.iloc[0]
        assert row["ledger_prior_test_count"] == 10  # Full window
        assert row["ledger_recent_test_count"] == 2  # Recent only

    def test_multiple_lineages_separate(self):
        key_a = "KEY:A"
        key_b = "KEY:B"
        rows_a = [_make_ledger_row(key_a, run_id=f"run_a_{i}") for i in range(5)]
        rows_b = [_make_ledger_row(key_b, run_id=f"run_b_{i}") for i in range(3)]
        ledger = pd.DataFrame(rows_a + rows_b).reindex(columns=CONCEPT_LEDGER_COLUMNS)
        summary = summarize_lineage_history(ledger, [key_a, key_b])
        a = summary[summary["concept_lineage_key"] == key_a].iloc[0]
        b = summary[summary["concept_lineage_key"] == key_b].iloc[0]
        assert a["ledger_prior_test_count"] == 5
        assert b["ledger_prior_test_count"] == 3

    def test_family_density_counts_unique_runs(self):
        key = "KEY:D"
        rows = [_make_ledger_row(key, run_id="run_A"), _make_ledger_row(key, run_id="run_B")]
        ledger = pd.DataFrame(rows).reindex(columns=CONCEPT_LEDGER_COLUMNS)
        summary = summarize_lineage_history(ledger, [key])
        row = summary.iloc[0]
        assert row["ledger_family_density"] == 2


# ---------------------------------------------------------------------------
# Scoring tests
# ---------------------------------------------------------------------------

class TestLedgerScoring:
    """Test apply_ledger_multiplicity_correction in isolation."""

    def _make_candidates_df(self, n: int = 3, q_value: float = 0.04) -> pd.DataFrame:
        from project.research.knowledge.concept_ledger import build_concept_lineage_key
        rows = []
        for i in range(n):
            candidate_dict = {
                "candidate_id": f"cand_{i:03d}",
                "event_type": "VOL_SHOCK",
                "event_family": "VOL_SHOCK",
                "rule_template": "continuation",
                "direction": "1.0",
                "timeframe": "5m",
                "horizon_bars": 24,
                "symbol": "BTCUSDT",
                "q_value": q_value,
                "is_discovery": True,
                "discovery_quality_score": 2.0,
                "demotion_reason_codes": "",
                "run_id": "current_run",
            }
            rows.append(candidate_dict)
        df = pd.DataFrame(rows)
        df["concept_lineage_key"] = df.apply(lambda r: build_concept_lineage_key(r), axis=1)
        return df

    def test_flag_off_returns_unchanged_q_value(self, tmp_path):
        from project.research.services.candidate_discovery_scoring import (
            apply_ledger_multiplicity_correction,
        )
        candidates = self._make_candidates_df()
        result = apply_ledger_multiplicity_correction(
            candidates,
            data_root=tmp_path,
            current_run_id="current_run",
            config={"enabled": False},
        )
        assert "q_value" in result.columns
        # q_value should be untouched
        assert (result["q_value"] == 0.04).all()

    def test_flag_off_no_ledger_columns(self, tmp_path):
        from project.research.services.candidate_discovery_scoring import (
            apply_ledger_multiplicity_correction,
        )
        candidates = self._make_candidates_df()
        result = apply_ledger_multiplicity_correction(
            candidates,
            data_root=tmp_path,
            current_run_id="current_run",
            config={"enabled": False},
        )
        # Ledger scoring columns should not be present when disabled
        assert "ledger_multiplicity_penalty" not in result.columns
        assert "discovery_quality_score_v3" not in result.columns

    def test_flag_off_lineage_key_always_attached(self, tmp_path):
        from project.research.services.candidate_discovery_scoring import (
            apply_ledger_multiplicity_correction,
        )
        candidates = self._make_candidates_df()
        result = apply_ledger_multiplicity_correction(
            candidates,
            data_root=tmp_path,
            current_run_id="current_run",
            config={"enabled": False},
        )
        # concept_lineage_key is always added (needed for ledger writes)
        assert "concept_lineage_key" in result.columns
        assert result["concept_lineage_key"].notna().all()

    def test_zero_history_zero_penalty(self, tmp_path):
        from project.research.services.candidate_discovery_scoring import (
            apply_ledger_multiplicity_correction,
        )
        candidates = self._make_candidates_df()
        result = apply_ledger_multiplicity_correction(
            candidates,
            data_root=tmp_path,
            current_run_id="current_run",
            config={"enabled": True, "min_prior_tests_for_penalty": 3},
        )
        assert "ledger_multiplicity_penalty" in result.columns
        assert (result["ledger_multiplicity_penalty"] == 0.0).all()

    def test_high_history_positive_penalty(self, tmp_path):
        from project.research.knowledge.concept_ledger import build_concept_lineage_key
        from project.research.services.candidate_discovery_scoring import (
            apply_ledger_multiplicity_correction,
        )
        # Pre-populate ledger with many failures - use the key that build_concept_lineage_key produces
        ledger_path = default_ledger_path(tmp_path)
        ledger_path.parent.mkdir(parents=True, exist_ok=True)

        # Build the key the same way the correction function will
        candidate_row = {
            'event_type': 'VOL_SHOCK',
            'event_family': 'VOL_SHOCK',
            'rule_template': 'continuation',
            'direction': '1.0',
            'timeframe': '5m',
            'horizon_bars': 24,
            'symbol': 'BTCUSDT',
        }
        key = build_concept_lineage_key(candidate_row)

        rows = [_make_ledger_row(key, run_id=f"hist_{i}", is_discovery=False) for i in range(25)]
        pd.DataFrame(rows).reindex(columns=CONCEPT_LEDGER_COLUMNS).to_parquet(ledger_path, index=False)

        candidates = self._make_candidates_df()
        result = apply_ledger_multiplicity_correction(
            candidates,
            data_root=tmp_path,
            current_run_id="current_run",
            config={"enabled": True, "min_prior_tests_for_penalty": 3, "max_penalty": 3.0},
        )
        assert "ledger_multiplicity_penalty" in result.columns
        assert (result["ledger_multiplicity_penalty"] > 0).all()

    def test_current_run_excluded_from_ledger(self, tmp_path):
        from project.research.knowledge.concept_ledger import build_concept_lineage_key
        from project.research.services.candidate_discovery_scoring import (
            apply_ledger_multiplicity_correction,
        )
        # Populate ledger with records from the CURRENT run only
        ledger_path = default_ledger_path(tmp_path)
        ledger_path.parent.mkdir(parents=True, exist_ok=True)

        candidate_row = {
            'event_type': 'VOL_SHOCK',
            'event_family': 'VOL_SHOCK',
            'rule_template': 'continuation',
            'direction': '1.0',
            'timeframe': '5m',
            'horizon_bars': 24,
            'symbol': 'BTCUSDT',
        }
        key = build_concept_lineage_key(candidate_row)

        rows = [_make_ledger_row(key, run_id="current_run", is_discovery=False) for i in range(30)]
        pd.DataFrame(rows).reindex(columns=CONCEPT_LEDGER_COLUMNS).to_parquet(ledger_path, index=False)

        candidates = self._make_candidates_df()
        result = apply_ledger_multiplicity_correction(
            candidates,
            data_root=tmp_path,
            current_run_id="current_run",  # same as ledger run_id
            config={"enabled": True, "min_prior_tests_for_penalty": 3},
        )
        # All those records should be excluded → prior_test_count == 0 → 0 penalty
        assert (result["ledger_multiplicity_penalty"] == 0.0).all()

    def test_crowded_lineage_reason_code(self, tmp_path):
        from project.research.knowledge.concept_ledger import build_concept_lineage_key
        from project.research.services.candidate_discovery_scoring import (
            apply_ledger_multiplicity_correction,
        )
        ledger_path = default_ledger_path(tmp_path)
        ledger_path.parent.mkdir(parents=True, exist_ok=True)

        candidate_row = {
            'event_type': 'VOL_SHOCK',
            'event_family': 'VOL_SHOCK',
            'rule_template': 'continuation',
            'direction': '1.0',
            'timeframe': '5m',
            'horizon_bars': 24,
            'symbol': 'BTCUSDT',
        }
        key = build_concept_lineage_key(candidate_row)

        rows = [_make_ledger_row(key, run_id=f"r{i}", is_discovery=False) for i in range(25)]
        pd.DataFrame(rows).reindex(columns=CONCEPT_LEDGER_COLUMNS).to_parquet(ledger_path, index=False)

        candidates = self._make_candidates_df()
        result = apply_ledger_multiplicity_correction(
            candidates,
            data_root=tmp_path,
            current_run_id="new_run",
            config={
                "enabled": True,
                "min_prior_tests_for_penalty": 3,
                "crowded_lineage_threshold": 20,
            },
        )
        assert "demotion_reason_codes" in result.columns
        assert result["demotion_reason_codes"].str.contains("crowded_lineage").any()

    def test_v3_score_lower_than_v2_when_penalized(self, tmp_path):
        from project.research.services.candidate_discovery_scoring import (
            apply_ledger_multiplicity_correction,
        )
        ledger_path = default_ledger_path(tmp_path)
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        key = "EVENT:VOL_SHOCK|TMPL:continuation|DIR:long|TF:5m|H:short|SYM:single|CTX:0"
        rows = [_make_ledger_row(key, run_id=f"r{i}", is_discovery=False) for i in range(10)]
        pd.DataFrame(rows).reindex(columns=CONCEPT_LEDGER_COLUMNS).to_parquet(ledger_path, index=False)

        candidates = self._make_candidates_df()
        result = apply_ledger_multiplicity_correction(
            candidates,
            data_root=tmp_path,
            current_run_id="new_run",
            config={"enabled": True, "min_prior_tests_for_penalty": 3},
        )
        penalized = result[result["ledger_multiplicity_penalty"] > 0]
        if not penalized.empty:
            v2 = pd.to_numeric(penalized["discovery_quality_score"], errors="coerce")
            v3 = pd.to_numeric(penalized["discovery_quality_score_v3"], errors="coerce")
            assert (v3 < v2).all()


# ---------------------------------------------------------------------------
# build_ledger_records tests
# ---------------------------------------------------------------------------

class TestBuildLedgerRecords:
    def test_both_survivors_and_failures(self):
        rows = [
            _make_candidate(candidate_id="c1", is_discovery=True),
            _make_candidate(candidate_id="c2", is_discovery=False),
        ]
        df = pd.DataFrame(rows)
        records = build_ledger_records(df, run_id="run_001")
        assert len(records) == 2

    def test_empty_candidates_returns_empty(self):
        records = build_ledger_records(pd.DataFrame(), run_id="run_001")
        assert records.empty

    def test_schema_matches_columns(self):
        df = pd.DataFrame([_make_candidate()])
        records = build_ledger_records(df, run_id="run_001")
        for col in CONCEPT_LEDGER_COLUMNS:
            assert col in records.columns

    def test_ledger_id_deterministic(self):
        df = pd.DataFrame([_make_candidate()])
        r1 = build_ledger_records(df, run_id="run_001")
        r2 = build_ledger_records(df, run_id="run_001")
        assert r1.iloc[0]["ledger_id"] == r2.iloc[0]["ledger_id"]


# ---------------------------------------------------------------------------
# Diagnostics tests
# ---------------------------------------------------------------------------

class TestLedgerDiagnostics:
    def test_empty_df_returns_minimal(self):
        from project.research.services.candidate_discovery_diagnostics import (
            build_ledger_diagnostics,
        )
        result = build_ledger_diagnostics(pd.DataFrame())
        assert result["ledger_adjustment_enabled"] is False
        assert result["lineages_covered"] == 0

    def test_no_ledger_columns_returns_disabled(self):
        from project.research.services.candidate_discovery_diagnostics import (
            build_ledger_diagnostics,
        )
        df = pd.DataFrame([{"candidate_id": "c1", "is_discovery": True}])
        result = build_ledger_diagnostics(df)
        assert result["ledger_adjustment_enabled"] is False

    def test_with_ledger_columns_returns_enabled(self):
        from project.research.services.candidate_discovery_diagnostics import (
            build_ledger_diagnostics,
        )
        df = pd.DataFrame([{
            "candidate_id": "c1",
            "concept_lineage_key": "KEY:A",
            "ledger_multiplicity_penalty": 0.5,
            "ledger_prior_test_count": 5,
            "ledger_empirical_success_rate": 0.2,
            "discovery_quality_score": 2.0,
            "discovery_quality_score_v3": 1.5,
            "demotion_reason_codes": "ledger_penalty_applied",
        }])
        result = build_ledger_diagnostics(df)
        assert result["ledger_adjustment_enabled"] is True
        assert result["lineages_covered"] == 1
        assert result["ledger_coverage_rate"] == 1.0

    def test_crowded_lineage_detected(self):
        from project.research.services.candidate_discovery_diagnostics import (
            build_ledger_diagnostics,
        )
        df = pd.DataFrame([{
            "candidate_id": "c1",
            "concept_lineage_key": "KEY:CROWDED",
            "ledger_multiplicity_penalty": 2.0,
            "ledger_prior_test_count": 30,
            "ledger_empirical_success_rate": 0.05,
            "demotion_reason_codes": "crowded_lineage|ledger_penalty_applied",
        }])
        result = build_ledger_diagnostics(df)
        assert "KEY:CROWDED" in result["crowded_lineages"]
        assert result["demotion_reason_counts"].get("crowded_lineage", 0) >= 1


# ---------------------------------------------------------------------------
# Integration: ledger accumulates across runs
# ---------------------------------------------------------------------------

class TestIntegrationLedgerAccumulation:
    def test_second_run_sees_prior_burden(self, tmp_path):
        from project.research.knowledge.concept_ledger import build_concept_lineage_key
        from project.research.services.candidate_discovery_scoring import (
            apply_ledger_multiplicity_correction,
        )

        ledger_path = default_ledger_path(tmp_path)
        ledger_path.parent.mkdir(parents=True, exist_ok=True)

        candidate_row = {
            'event_type': 'VOL_SHOCK',
            'event_family': 'VOL_SHOCK',
            'rule_template': 'continuation',
            'direction': '1.0',
            'timeframe': '5m',
            'horizon_bars': 24,
            'symbol': 'BTCUSDT',
        }
        key = build_concept_lineage_key(candidate_row)

        rows_run1 = [_make_ledger_row(key, run_id="run_001", is_discovery=False) for _ in range(5)]
        pd.DataFrame(rows_run1).reindex(columns=CONCEPT_LEDGER_COLUMNS).to_parquet(
            ledger_path, index=False
        )

        candidates = pd.DataFrame([_make_candidate()])
        result = apply_ledger_multiplicity_correction(
            candidates,
            data_root=tmp_path,
            current_run_id="run_002",
            config={"enabled": True, "min_prior_tests_for_penalty": 3},
        )
        assert result.iloc[0]["ledger_prior_test_count"] == 5

    def test_unrelated_lineage_not_penalized(self, tmp_path):
        from project.research.knowledge.concept_ledger import build_concept_lineage_key
        from project.research.services.candidate_discovery_scoring import (
            apply_ledger_multiplicity_correction,
        )

        ledger_path = default_ledger_path(tmp_path)
        ledger_path.parent.mkdir(parents=True, exist_ok=True)

        candidate_row = {
            'event_type': 'VOL_SHOCK',
            'event_family': 'VOL_SHOCK',
            'rule_template': 'continuation',
            'direction': '1.0',
            'timeframe': '5m',
            'horizon_bars': 24,
            'symbol': 'BTCUSDT',
        }
        key = build_concept_lineage_key(candidate_row)

        rows = [_make_ledger_row(key, run_id=f"r{i}", is_discovery=False) for i in range(20)]
        pd.DataFrame(rows).reindex(columns=CONCEPT_LEDGER_COLUMNS).to_parquet(ledger_path, index=False)

        candidates = pd.DataFrame([_make_candidate(
            event_type='LIQUIDATION_CASCADE',
            event_family='LIQUIDATION_CASCADE',
            rule_template='mean_reversion',
        )])
        result = apply_ledger_multiplicity_correction(
            candidates,
            data_root=tmp_path,
            current_run_id="run_new",
            config={"enabled": True, "min_prior_tests_for_penalty": 3},
        )
        # Unrelated lineage → no prior burden
        assert result.iloc[0]["ledger_prior_test_count"] == 0
        assert result.iloc[0]["ledger_multiplicity_penalty"] == 0.0
