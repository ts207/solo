import pytest
from project.live.risk import RiskEnforcer, RuntimeRiskCaps, PerThesisCap


class TestRiskOverlapExclusivity:
    def _make_enforcer(self, **kwargs) -> RiskEnforcer:
        caps = RuntimeRiskCaps(**kwargs)
        return RiskEnforcer(caps)

    def test_no_overlap_group_passes(self):
        enforcer = self._make_enforcer()
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="T1",
            symbol="BTCUSDT",
            family="VOL",
            attempted_notional=1000.0,
            portfolio_state={},
            active_thesis_ids=[],
            timestamp="2026-04-12T10:00:00",
            thesis_overlap_group=None,
            active_overlap_groups={"G1"},
        )
        assert notional == 1000.0
        assert breach is None

    def test_overlap_group_not_active_passes(self):
        enforcer = self._make_enforcer()
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="T1",
            symbol="BTCUSDT",
            family="VOL",
            attempted_notional=1000.0,
            portfolio_state={},
            active_thesis_ids=[],
            timestamp="2026-04-12T10:00:00",
            thesis_overlap_group="G1",
            active_overlap_groups={"G2"},
        )
        assert notional == 1000.0
        assert breach is None

    def test_overlap_group_active_but_same_thesis_passes(self):
        # T1 is already active, so it's allowed to place more orders even if its group G1 is active
        enforcer = self._make_enforcer()
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="T1",
            symbol="BTCUSDT",
            family="VOL",
            attempted_notional=1000.0,
            portfolio_state={},
            active_thesis_ids=["T1"],
            timestamp="2026-04-12T10:00:00",
            thesis_overlap_group="G1",
            active_overlap_groups={"G1"},
        )
        assert notional == 1000.0
        assert breach is None

    def test_overlap_group_active_different_thesis_rejects(self):
        # T2 is trying to enter group G1, which is already occupied by someone else (e.g. T1)
        enforcer = self._make_enforcer()
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="T2",
            symbol="BTCUSDT",
            family="VOL",
            attempted_notional=1000.0,
            portfolio_state={},
            active_thesis_ids=["T1"],  # T1 is active
            timestamp="2026-04-12T10:00:00",
            thesis_overlap_group="G1",
            active_overlap_groups={"G1"},  # G1 is active
        )
        assert notional == 0.0
        assert breach is not None
        assert breach.cap_type == "overlap_group_exclusive"
        assert breach.thesis_id == "T2"

    def test_empty_overlap_group_string_passes(self):
        enforcer = self._make_enforcer()
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="T1",
            symbol="BTCUSDT",
            family="VOL",
            attempted_notional=1000.0,
            portfolio_state={},
            active_thesis_ids=[],
            timestamp="2026-04-12T10:00:00",
            thesis_overlap_group="  ",  # Empty after strip
            active_overlap_groups={"G1"},
        )
        assert notional == 1000.0
        assert breach is None

    def test_active_overlap_groups_none_passes(self):
        enforcer = self._make_enforcer()
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="T1",
            symbol="BTCUSDT",
            family="VOL",
            attempted_notional=1000.0,
            portfolio_state={},
            active_thesis_ids=[],
            timestamp="2026-04-12T10:00:00",
            thesis_overlap_group="G1",
            active_overlap_groups=None,
        )
        assert notional == 1000.0
        assert breach is None
