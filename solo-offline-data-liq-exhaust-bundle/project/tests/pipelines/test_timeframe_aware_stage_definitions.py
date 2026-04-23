"""Tests for timeframe-aware stage definitions and artifact contracts."""

import pytest

from project.pipelines.stage_definitions import (
    STAGE_ARTIFACT_REGISTRY,
    STAGE_FAMILY_REGISTRY,
    parse_timeframes,
    build_timeframe_artifact_contracts,
    resolve_stage_artifacts,
)
from project.pipelines.stage_dependencies import (
    resolve_stage_artifact_contract,
    _matching_artifact_contracts,
)


class TestTimeframeParsing:
    """Test timeframe string parsing."""

    def test_parse_single_timeframe(self):
        assert parse_timeframes("5m") == ["5m"]
        assert parse_timeframes("1m") == ["1m"]
        assert parse_timeframes("15m") == ["15m"]

    def test_parse_multiple_timeframes(self):
        assert parse_timeframes("1m,5m") == ["1m", "5m"]
        assert parse_timeframes("1m,5m,15m") == ["1m", "5m", "15m"]

    def test_parse_empty_string_defaults_to_5m(self):
        assert parse_timeframes("") == ["5m"]

    def test_parse_invalid_filters_to_5m(self):
        assert parse_timeframes("invalid") == ["5m"]
        assert parse_timeframes("1x,2y") == ["5m"]


class Test5mPlanArtifactResolution:
    """Build a plan for 5m, verify stage names and artifact tokens are correct."""

    def test_5m_ingest_artifact(self):
        """5m ingest stage produces raw.perp.ohlcv_5m."""
        contract, issues = resolve_stage_artifact_contract(
            "ingest_binance_um_ohlcv_5m", ["--timeframe", "5m"]
        )
        assert not issues
        assert contract.outputs == ("raw.perp.ohlcv_5m",)

    def test_5m_cleaned_artifact(self):
        """5m cleaned stage consumes raw.perp.ohlcv_5m."""
        contract, issues = resolve_stage_artifact_contract(
            "build_cleaned_5m", ["--timeframe", "5m"]
        )
        assert not issues
        assert contract.inputs == ("raw.perp.ohlcv_5m",)
        assert contract.outputs == ("clean.perp.*",)

    def test_5m_features_artifact(self):
        """5m features stage uses correct artifact chain."""
        contract, issues = resolve_stage_artifact_contract(
            "build_features_5m", ["--timeframe", "5m"]
        )
        assert not issues
        assert "clean.perp.*" in contract.inputs
        assert contract.outputs == ("features.perp.v2",)


class Test1mPlanArtifactResolution:
    """Build a plan for 1m, verify no 5m artifact token is injected."""

    def test_1m_ingest_artifact(self):
        """1m ingest stage produces raw.perp.ohlcv_1m, NOT 5m."""
        contract, issues = resolve_stage_artifact_contract(
            "ingest_binance_um_ohlcv_1m", ["--timeframe", "1m"]
        )
        assert not issues
        assert contract.outputs == ("raw.perp.ohlcv_1m",)
        assert "5m" not in str(contract.outputs)

    def test_1m_cleaned_artifact(self):
        """1m cleaned stage consumes raw.perp.ohlcv_1m, NOT 5m."""
        contract, issues = resolve_stage_artifact_contract(
            "build_cleaned_1m", ["--timeframe", "1m"]
        )
        assert not issues
        assert contract.inputs == ("raw.perp.ohlcv_1m",)
        assert "5m" not in str(contract.inputs)

    def test_1m_features_artifact(self):
        """1m features stage input does NOT contain 5m."""
        contract, issues = resolve_stage_artifact_contract(
            "build_features_1m", ["--timeframe", "1m"]
        )
        assert not issues
        # The clean.perp.* pattern is timeframe-agnostic, but raw inputs should be 1m
        assert "clean.perp.*" in contract.inputs


class TestMultiTimeframePlan:
    """Build a plan for 1m,5m,15m, verify correct multi-timeframe handling."""

    def test_multi_timeframe_contracts_generated(self):
        """Verify contracts are generated for all timeframes."""
        timeframes = parse_timeframes("1m,5m,15m")
        assert "1m" in timeframes
        assert "5m" in timeframes
        assert "15m" in timeframes

    def test_1m_contract_separate_from_5m(self):
        """1m and 5m stages have separate artifact contracts."""
        contract_1m, _ = resolve_stage_artifact_contract(
            "ingest_binance_um_ohlcv_1m", ["--timeframe", "1m"]
        )
        contract_5m, _ = resolve_stage_artifact_contract(
            "ingest_binance_um_ohlcv_5m", ["--timeframe", "5m"]
        )

        assert contract_1m.outputs != contract_5m.outputs
        assert "1m" in str(contract_1m.outputs)
        assert "5m" in str(contract_5m.outputs)

    def test_15m_contract(self):
        """15m timeframe is correctly handled."""
        contract, issues = resolve_stage_artifact_contract(
            "ingest_binance_um_ohlcv_15m", ["--timeframe", "15m"]
        )
        assert not issues
        assert contract.outputs == ("raw.perp.ohlcv_15m",)


class TestBackwardCompatibility:
    """Verify backward compatibility with existing 5m paths."""

    def test_legacy_5m_stage_still_works(self):
        """Legacy 5m stage names without explicit timeframe work."""
        contract, issues = resolve_stage_artifact_contract("build_cleaned_5m", [])
        assert not issues
        assert contract.inputs == ("raw.perp.ohlcv_5m",)

    def test_legacy_spot_5m_stage(self):
        """Legacy spot 5m stage still works."""
        contract, issues = resolve_stage_artifact_contract(
            "build_cleaned_5m_spot", ["--timeframe", "5m"]
        )
        assert not issues
        assert contract.inputs == ("raw.spot.ohlcv_5m",)
