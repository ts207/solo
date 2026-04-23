from __future__ import annotations

import json

import pytest

from project.specs.objective import (
    assert_low_capital_contract,
    resolve_objective_profile_contract,
)


def test_objective_profile_contract_uses_run_manifest_paths(tmp_path):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    data_root = repo_root / "data"
    spec_root = repo_root / "spec" / "objectives"
    configs_root = project_root / "configs"
    run_dir = data_root / "runs" / "r1"
    spec_root.mkdir(parents=True)
    configs_root.mkdir(parents=True)
    run_dir.mkdir(parents=True)

    objective_path = spec_root / "custom_objective.yaml"
    objective_path.write_text(
        "objective:\n"
        "  id: custom_objective\n"
        "  hard_gates:\n"
        "    min_trade_count: 222\n"
        "    min_oos_sign_consistency: 0.75\n"
        "  constraints:\n"
        "    require_retail_viability: true\n",
        encoding="utf-8",
    )
    profiles_path = configs_root / "retail_profiles.yaml"
    profiles_path.write_text(
        "profiles:\n"
        "  constrained:\n"
        "    require_top_book_coverage: 0.82\n"
        "    min_net_expectancy_bps: 4.5\n"
        "    target_account_size_usd: 20000\n"
        "    max_initial_margin_pct: 0.5\n"
        "    max_leverage: 3\n"
        "    max_position_notional_usd: 8000\n"
        "    max_concurrent_positions: 2\n",
        encoding="utf-8",
    )
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "objective_name": "custom_objective",
                "objective_spec_path": str(objective_path),
                "retail_profile_name": "constrained",
                "retail_profile_spec_path": str(profiles_path),
            }
        ),
        encoding="utf-8",
    )

    contract = resolve_objective_profile_contract(
        project_root=project_root,
        data_root=data_root,
        run_id="r1",
        required=True,
    )
    assert contract.objective_id == "custom_objective"
    assert contract.min_trade_count == 222
    assert contract.min_oos_sign_consistency == 0.75
    assert contract.min_tob_coverage == 0.82
    assert contract.min_net_expectancy_bps == 4.5
    assert contract.require_retail_viability is True
    assert contract.capital_budget_usd == 30000.0
    assert contract.effective_per_position_notional_cap_usd == 8000.0


def test_objective_profile_contract_explicit_retail_profile_override(tmp_path):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    data_root = repo_root / "data"
    spec_root = repo_root / "spec" / "objectives"
    configs_root = project_root / "configs"
    run_dir = data_root / "runs" / "r2"
    spec_root.mkdir(parents=True)
    configs_root.mkdir(parents=True)
    run_dir.mkdir(parents=True)

    (spec_root / "retail_profitability.yaml").write_text(
        "objective:\n  id: retail_profitability\n",
        encoding="utf-8",
    )
    (configs_root / "retail_profiles.yaml").write_text(
        "profiles:\n"
        "  constrained:\n"
        "    require_top_book_coverage: 0.8\n"
        "  growth:\n"
        "    require_top_book_coverage: 0.7\n"
        "    max_concurrent_positions: 5\n",
        encoding="utf-8",
    )
    (run_dir / "run_manifest.json").write_text(
        json.dumps({"retail_profile_name": "constrained"}),
        encoding="utf-8",
    )

    contract = resolve_objective_profile_contract(
        project_root=project_root,
        data_root=data_root,
        run_id="r2",
        retail_profile_name="growth",
        required=True,
    )
    assert contract.retail_profile_name == "growth"
    assert contract.min_tob_coverage == 0.7
    assert contract.max_concurrent_positions == 5


def test_low_capital_contract_enforced_when_objective_requires_it(tmp_path):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    data_root = repo_root / "data"
    spec_root = repo_root / "spec" / "objectives"
    configs_root = project_root / "configs"
    run_dir = data_root / "runs" / "r3"
    spec_root.mkdir(parents=True)
    configs_root.mkdir(parents=True)
    run_dir.mkdir(parents=True)

    (spec_root / "retail_profitability.yaml").write_text(
        "objective:\n"
        "  id: retail_profitability\n"
        "  constraints:\n"
        "    require_low_capital_contract: true\n",
        encoding="utf-8",
    )
    (configs_root / "retail_profiles.yaml").write_text(
        "profiles:\n  constrained:\n    account_equity_usd: 10000\n",
        encoding="utf-8",
    )
    (run_dir / "run_manifest.json").write_text(
        json.dumps({"retail_profile_name": "constrained"}),
        encoding="utf-8",
    )

    contract = resolve_objective_profile_contract(
        project_root=project_root,
        data_root=data_root,
        run_id="r3",
        required=True,
    )
    assert contract.require_low_capital_contract is True
    with pytest.raises(ValueError, match="low-capital contract missing required fields"):
        assert_low_capital_contract(contract, stage_name="unit_test")


def test_objective_profile_contract_rejects_manifest_objective_path_outside_repo(tmp_path):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    data_root = repo_root / "data"
    spec_root = repo_root / "spec" / "objectives"
    configs_root = project_root / "configs"
    run_dir = data_root / "runs" / "r4"
    spec_root.mkdir(parents=True)
    configs_root.mkdir(parents=True)
    run_dir.mkdir(parents=True)

    (spec_root / "retail_profitability.yaml").write_text(
        "objective:\n  id: retail_profitability\n",
        encoding="utf-8",
    )
    (configs_root / "retail_profiles.yaml").write_text(
        "profiles:\n  capital_constrained:\n    require_top_book_coverage: 0.8\n",
        encoding="utf-8",
    )
    external_objective = tmp_path / "external_objective.yaml"
    external_objective.write_text("objective:\n  id: external\n", encoding="utf-8")
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "objective_name": "retail_profitability",
                "objective_spec_path": str(external_objective),
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="objective_spec_path must stay within active repo root"):
        resolve_objective_profile_contract(
            project_root=project_root,
            data_root=data_root,
            run_id="r4",
            required=True,
        )


def test_objective_profile_contract_rejects_missing_manifest_profile_path(tmp_path):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    data_root = repo_root / "data"
    spec_root = repo_root / "spec" / "objectives"
    configs_root = project_root / "configs"
    run_dir = data_root / "runs" / "r5"
    spec_root.mkdir(parents=True)
    configs_root.mkdir(parents=True)
    run_dir.mkdir(parents=True)

    (spec_root / "retail_profitability.yaml").write_text(
        "objective:\n  id: retail_profitability\n",
        encoding="utf-8",
    )
    (configs_root / "retail_profiles.yaml").write_text(
        "profiles:\n  capital_constrained:\n    require_top_book_coverage: 0.8\n",
        encoding="utf-8",
    )
    missing_profile = project_root / "configs" / "missing_profiles.yaml"
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "objective_name": "retail_profitability",
                "retail_profile_name": "capital_constrained",
                "retail_profile_spec_path": str(missing_profile),
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(FileNotFoundError, match="run manifest retail_profile_spec_path missing"):
        resolve_objective_profile_contract(
            project_root=project_root,
            data_root=data_root,
            run_id="r5",
            required=True,
        )
