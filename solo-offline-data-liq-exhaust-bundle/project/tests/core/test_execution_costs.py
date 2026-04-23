from __future__ import annotations

from pathlib import Path

import pytest

from project.core.execution_costs import resolve_execution_costs


def test_resolve_execution_costs_exposes_round_trip_cost(tmp_path: Path) -> None:
    configs = tmp_path / "configs"
    configs.mkdir()
    (configs / "fees.yaml").write_text("fee_bps_per_side: 3\nslippage_bps_per_fill: 1\n", encoding="utf-8")
    (configs / "pipeline.yaml").write_text("{}\n", encoding="utf-8")

    costs = resolve_execution_costs(
        project_root=tmp_path,
        config_paths=(),
        fees_bps=None,
        slippage_bps=None,
        cost_bps=None,
    )

    assert costs.fee_bps_per_side == pytest.approx(3.0, abs=1e-9)
    assert costs.slippage_bps_per_fill == pytest.approx(1.0, abs=1e-9)
    assert costs.cost_bps == pytest.approx(4.0, abs=1e-9)
    assert costs.round_trip_cost_bps == pytest.approx(8.0, abs=1e-9)


def test_resolve_execution_costs_accepts_package_root(tmp_path: Path) -> None:
    package_root = tmp_path / "project"
    configs = package_root / "configs"
    configs.mkdir(parents=True)
    (configs / "fees.yaml").write_text("fee_bps_per_side: 5\nslippage_bps_per_fill: 2\n", encoding="utf-8")
    (configs / "pipeline.yaml").write_text("{}\n", encoding="utf-8")

    costs = resolve_execution_costs(
        project_root=package_root,
        config_paths=(),
        fees_bps=None,
        slippage_bps=None,
        cost_bps=None,
    )

    assert costs.fee_bps_per_side == pytest.approx(5.0, abs=1e-9)
    assert costs.slippage_bps_per_fill == pytest.approx(2.0, abs=1e-9)
