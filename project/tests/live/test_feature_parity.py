from project.core.regime_classifier import ClassificationMode, RegimeName, classify_regime
from project.live.runner import _classify_canonical_regime


def test_regime_classifier_runtime_approximation():
    """
    Verify that the shared regime classifier correctly falls back to 
    bps-based approximation when research features are missing.
    
    This tests helper behavior, not full research/live parity.
    """
    # 1. High Vol (> 80 bps move)
    res = _classify_canonical_regime(move_bps=85.0)
    assert res["canonical_regime"] == RegimeName.HIGH_VOL.value
    assert res["regime_mode"] == ClassificationMode.RUNTIME_APPROX.value
    assert res["regime_metadata"]["move_bps"] == 85.0

    # 2. Low Vol (< 20 bps move)
    res = _classify_canonical_regime(move_bps=10.0)
    assert res["canonical_regime"] == RegimeName.LOW_VOL.value

    # 3. Bull Trend (moderate positive)
    res = _classify_canonical_regime(move_bps=30.0)
    assert res["canonical_regime"] == RegimeName.BULL_TREND.value

    # 4. Bear Trend (moderate negative)
    res = _classify_canonical_regime(move_bps=-30.0)
    assert res["canonical_regime"] == RegimeName.BEAR_TREND.value


def test_regime_classifier_research_exact():
    """
    Verify that the shared regime classifier uses exact research semantics
    when research features are provided.
    
    This tests helper behavior, not full research/live parity.
    """
    # High Vol (rv_pct >= 80)
    res = _classify_canonical_regime(move_bps=0.0, rv_pct=85.0, ms_trend_state=1.0)
    assert res["canonical_regime"] == RegimeName.HIGH_VOL.value
    assert res["regime_mode"] == ClassificationMode.RESEARCH_EXACT.value

    # Bull Trend (rv_pct normal, ms_trend_state=1)
    res = _classify_canonical_regime(move_bps=0.0, rv_pct=50.0, ms_trend_state=1.0)
    assert res["canonical_regime"] == RegimeName.BULL_TREND.value

    # Chop (rv_pct normal, ms_trend_state=0)
    res = _classify_canonical_regime(move_bps=0.0, rv_pct=50.0, ms_trend_state=0.0)
    assert res["canonical_regime"] == RegimeName.CHOP.value


def test_regime_classifier_output_contract():
    """
    Verify that _classify_canonical_regime returns all required fields.
    This is a contract test for the helper, not a parity test.
    """
    expected_fields = {
        "canonical_regime", "regime_mode",
        "regime_confidence", "regime_metadata"
    }

    # Test 1: Runtime approximation when research features unavailable
    result = _classify_canonical_regime(move_bps=30.0)
    assert set(result.keys()) >= expected_fields
    assert result["canonical_regime"] == RegimeName.BULL_TREND.value
    assert result["regime_mode"] == ClassificationMode.RUNTIME_APPROX.value
    assert 0.0 < result["regime_confidence"] <= 1.0

    # Test 2: Research exact when all features available
    result = _classify_canonical_regime(
        move_bps=0.0,
        rv_pct=50.0,
        ms_trend_state=0.0
    )
    assert result["canonical_regime"] == RegimeName.CHOP.value
    assert result["regime_mode"] == ClassificationMode.RESEARCH_EXACT.value

    # Test 3: CHOP is a valid canonical regime
    result = _classify_canonical_regime(move_bps=0.0, rv_pct=50.0, ms_trend_state=0.0)
    assert result["canonical_regime"] in [r.value for r in RegimeName]
    assert "move_bps" in result["regime_metadata"]


class TestRegimeResearchLiveParity:
    """
    Tests that compare research-built regime inputs with live-built regime inputs
    for matched fixtures. These tests verify actual parity, not just helper behavior.
    """

    def test_high_vol_parity(self):
        """Research and live should both classify HIGH_VOL correctly."""
        # Research: rv_pct >= 80, any ms_trend_state
        research_result = classify_regime(
            rv_pct=85.0,
            ms_trend_state=1.0,
            move_bps=85.0
        )
        # Live: move_bps >= 80
        live_result = classify_regime(
            move_bps=85.0,
            fallback_regime=RegimeName.LOW_VOL
        )

        assert research_result.regime == RegimeName.HIGH_VOL
        assert research_result.mode == ClassificationMode.RESEARCH_EXACT
        assert live_result.regime == RegimeName.HIGH_VOL
        assert live_result.mode == ClassificationMode.RUNTIME_APPROX

    def test_low_vol_parity(self):
        """Research and live should both classify LOW_VOL correctly."""
        # Research: rv_pct < 20
        research_result = classify_regime(
            rv_pct=15.0,
            ms_trend_state=0.0,
            move_bps=10.0
        )
        # Live: abs(move_bps) < 20
        live_result = classify_regime(
            move_bps=10.0,
            fallback_regime=RegimeName.LOW_VOL
        )

        assert research_result.regime == RegimeName.LOW_VOL
        assert live_result.regime == RegimeName.LOW_VOL

    def test_bull_trend_parity(self):
        """Research and live should both classify BULL_TREND correctly."""
        # Research: rv_pct in [20, 80), ms_trend_state=1
        research_result = classify_regime(
            rv_pct=50.0,
            ms_trend_state=1.0,
            move_bps=30.0
        )
        # Live: 20 <= move_bps < 80
        live_result = classify_regime(
            move_bps=30.0,
            fallback_regime=RegimeName.LOW_VOL
        )

        assert research_result.regime == RegimeName.BULL_TREND
        assert live_result.regime == RegimeName.BULL_TREND

    def test_bear_trend_parity(self):
        """Research and live should both classify BEAR_TREND correctly."""
        # Research: rv_pct in [20, 80), ms_trend_state=2
        research_result = classify_regime(
            rv_pct=50.0,
            ms_trend_state=2.0,
            move_bps=-30.0
        )
        # Live: -80 < move_bps <= -20
        live_result = classify_regime(
            move_bps=-30.0,
            fallback_regime=RegimeName.LOW_VOL
        )

        assert research_result.regime == RegimeName.BEAR_TREND
        assert live_result.regime == RegimeName.BEAR_TREND

    def test_chop_parity(self):
        """Research and live diverge on CHOP classification."""
        # Research: rv_pct in [20, 80), ms_trend_state=0
        research_result = classify_regime(
            rv_pct=50.0,
            ms_trend_state=0.0,
            move_bps=25.0
        )
        # Live: 20 <= move_bps < 80 -> BULL_TREND (cannot distinguish CHOP)
        live_result = classify_regime(
            move_bps=25.0,
            fallback_regime=RegimeName.LOW_VOL
        )

        assert research_result.regime == RegimeName.CHOP
        assert research_result.mode == ClassificationMode.RESEARCH_EXACT
        # Live approximation cannot detect CHOP; falls back to trend classification
        # based on sign of move_bps
        assert live_result.regime == RegimeName.BULL_TREND
        assert live_result.mode == ClassificationMode.RUNTIME_APPROX
        assert "missing_inputs" in live_result.metadata

    def test_approximation_mode_has_metadata(self):
        """When live uses approximation, it must document what's missing."""
        result = classify_regime(move_bps=30.0)

        assert result.mode == ClassificationMode.RUNTIME_APPROX
        assert "missing_inputs" in result.metadata
        assert "rv_pct" in result.metadata["missing_inputs"]
        assert "ms_trend_state" in result.metadata["missing_inputs"]
        assert result.confidence < 1.0

    def test_research_mode_high_confidence(self):
        """When research features are available, confidence should be high."""
        result = classify_regime(
            rv_pct=50.0,
            ms_trend_state=1.0,
            move_bps=30.0
        )

        assert result.mode == ClassificationMode.RESEARCH_EXACT
        assert result.confidence == 1.0

    def test_batch_fixture_parity(self):
        """
        Compare research and live classifications across a batch of fixtures.
        Both should agree when precondition matches, diverge onlywhen
        live approximation cannot distinguish (e.g., CHOP).
        """
        fixtures = [
            {"rv": 85.0, "ms": 1.0, "move": 85.0, "expected_research": RegimeName.HIGH_VOL, "expected_live": RegimeName.HIGH_VOL},
            {"rv": 15.0, "ms": 0.0, "move": 10.0, "expected_research": RegimeName.LOW_VOL, "expected_live": RegimeName.LOW_VOL},
            {"rv": 50.0, "ms": 1.0, "move": 30.0, "expected_research": RegimeName.BULL_TREND, "expected_live": RegimeName.BULL_TREND},
            {"rv": 50.0, "ms": 2.0, "move": -30.0, "expected_research": RegimeName.BEAR_TREND, "expected_live": RegimeName.BEAR_TREND},
            {"rv": 50.0, "ms": 0.0, "move": 25.0, "expected_research": RegimeName.CHOP, "expected_live": RegimeName.BULL_TREND},  # Divergence expected
        ]

        for i, fx in enumerate(fixtures):
            research = classify_regime(rv_pct=fx["rv"], ms_trend_state=fx["ms"], move_bps=fx["move"])
            live = classify_regime(move_bps=fx["move"])

            assert research.regime == fx["expected_research"], f"Fixture {i}: research mismatch"
            assert live.regime == fx["expected_live"], f"Fixture {i}: live mismatch"

            if fx["expected_research"] == fx["expected_live"]:
                # Agreement case: both should classify the same
                assert research.regime == live.regime, f"Fixture {i}: research/live parity violation"
