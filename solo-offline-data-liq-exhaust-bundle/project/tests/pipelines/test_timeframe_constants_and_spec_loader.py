from __future__ import annotations

import pytest

from project.engine import runner
from project.specs.loader import (
    load_global_defaults,
    load_objective_spec,
    load_retail_profile,
)
from project.core.constants import (
    BARS_PER_YEAR_BY_TIMEFRAME,
    DEFAULT_EVENT_HORIZON_BARS,
    HORIZON_BARS_BY_TIMEFRAME,
    parse_horizon_bars,
)
from project.research.services.phase2_support import horizon_to_bars
from project.research import validate_event_quality


def test_runner_uses_canonical_bars_per_year_map():
    assert runner.BARS_PER_YEAR == BARS_PER_YEAR_BY_TIMEFRAME


def test_horizon_lookup_uses_canonical_mapping():
    assert horizon_to_bars("5m") == HORIZON_BARS_BY_TIMEFRAME["5m"]
    assert horizon_to_bars("60m") == HORIZON_BARS_BY_TIMEFRAME["60m"]
    assert horizon_to_bars("72b") == 72
    assert horizon_to_bars("unknown_horizon") == 12


def test_parse_horizon_bars_accepts_arbitrary_bar_count_labels():
    assert parse_horizon_bars("72b") == 72
    assert parse_horizon_bars("72") == 72


def test_default_event_horizon_grid_is_stable():
    assert DEFAULT_EVENT_HORIZON_BARS == [1, 3, 12]


def test_validate_event_quality_default_horizons_uses_canonical_constant():
    assert validate_event_quality._default_horizons_bars_csv() == ",".join(
        str(x) for x in DEFAULT_EVENT_HORIZON_BARS
    )


def test_spec_loader_precedence_explicit_over_env_over_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    spec_root = repo_root / "spec"
    project_root.mkdir(parents=True)
    spec_root.mkdir(parents=True)

    default_path = spec_root / "global_defaults.yaml"
    default_path.write_text("defaults:\n  source: default\n")

    env_path = tmp_path / "env_defaults.yaml"
    env_path.write_text("defaults:\n  source: env\n")
    monkeypatch.setenv("BACKTEST_GLOBAL_DEFAULTS_PATH", str(env_path))

    explicit_path = tmp_path / "explicit_defaults.yaml"
    explicit_path.write_text("defaults:\n  source: explicit\n")

    loaded = load_global_defaults(project_root=project_root, explicit_path=explicit_path)
    assert loaded["source"] == "explicit"


def test_spec_loader_env_over_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    spec_root = repo_root / "spec"
    project_root.mkdir(parents=True)
    spec_root.mkdir(parents=True)

    (spec_root / "global_defaults.yaml").write_text("defaults:\n  source: default\n")
    env_path = tmp_path / "env_defaults.yaml"
    env_path.write_text("defaults:\n  source: env\n")
    monkeypatch.setenv("BACKTEST_GLOBAL_DEFAULTS_PATH", str(env_path))

    loaded = load_global_defaults(project_root=project_root)
    assert loaded["source"] == "env"


def test_spec_loader_required_raises_when_missing(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    project_root.mkdir(parents=True)
    monkeypatch.delenv("BACKTEST_GLOBAL_DEFAULTS_PATH", raising=False)

    with pytest.raises(FileNotFoundError):
        load_global_defaults(project_root=project_root, required=True)


def test_spec_loader_returns_empty_when_optional_and_missing(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    project_root.mkdir(parents=True)
    monkeypatch.delenv("BACKTEST_GLOBAL_DEFAULTS_PATH", raising=False)

    assert load_global_defaults(project_root=project_root) == {}


def test_objective_spec_loader_precedence_explicit_over_env_over_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    objective_root = repo_root / "spec" / "objectives"
    project_root.mkdir(parents=True)
    objective_root.mkdir(parents=True)

    (objective_root / "retail_profitability.yaml").write_text(
        "objective:\n  id: default\n",
        encoding="utf-8",
    )
    env_path = tmp_path / "env_objective.yaml"
    env_path.write_text("objective:\n  id: env\n", encoding="utf-8")
    monkeypatch.setenv("BACKTEST_OBJECTIVE_SPEC_PATH", str(env_path))
    explicit_path = tmp_path / "explicit_objective.yaml"
    explicit_path.write_text("objective:\n  id: explicit\n", encoding="utf-8")

    loaded = load_objective_spec(
        project_root=project_root,
        objective_name="retail_profitability",
        explicit_path=explicit_path,
    )
    assert loaded["id"] == "explicit"


def test_objective_spec_loader_env_over_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    objective_root = repo_root / "spec" / "objectives"
    project_root.mkdir(parents=True)
    objective_root.mkdir(parents=True)

    (objective_root / "retail_profitability.yaml").write_text(
        "objective:\n  id: default\n",
        encoding="utf-8",
    )
    env_path = tmp_path / "env_objective.yaml"
    env_path.write_text("objective:\n  id: env\n", encoding="utf-8")
    monkeypatch.setenv("BACKTEST_OBJECTIVE_SPEC_PATH", str(env_path))

    loaded = load_objective_spec(
        project_root=project_root,
        objective_name="retail_profitability",
    )
    assert loaded["id"] == "env"


def test_objective_spec_loader_required_raises_when_missing(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    project_root.mkdir(parents=True)
    monkeypatch.delenv("BACKTEST_OBJECTIVE_SPEC_PATH", raising=False)

    with pytest.raises(FileNotFoundError):
        load_objective_spec(
            project_root=project_root,
            objective_name="retail_profitability",
            required=True,
        )


def test_retail_profile_loader_precedence_explicit_over_env_over_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    project_root.mkdir(parents=True)
    (project_root / "configs").mkdir(parents=True)

    (project_root / "configs" / "retail_profiles.yaml").write_text(
        "profiles:\n  capital_constrained:\n    max_leverage: 3\n",
        encoding="utf-8",
    )
    env_path = tmp_path / "env_profiles.yaml"
    env_path.write_text(
        "profiles:\n  capital_constrained:\n    max_leverage: 4\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKTEST_RETAIL_PROFILES_PATH", str(env_path))
    explicit_path = tmp_path / "explicit_profiles.yaml"
    explicit_path.write_text(
        "profiles:\n  capital_constrained:\n    max_leverage: 2\n",
        encoding="utf-8",
    )

    loaded = load_retail_profile(
        project_root=project_root,
        profile_name="capital_constrained",
        explicit_path=explicit_path,
    )
    assert int(loaded["max_leverage"]) == 2


def test_retail_profile_loader_env_over_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    project_root.mkdir(parents=True)
    (project_root / "configs").mkdir(parents=True)

    (project_root / "configs" / "retail_profiles.yaml").write_text(
        "profiles:\n  capital_constrained:\n    max_leverage: 3\n",
        encoding="utf-8",
    )
    env_path = tmp_path / "env_profiles.yaml"
    env_path.write_text(
        "profiles:\n  capital_constrained:\n    max_leverage: 5\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKTEST_RETAIL_PROFILES_PATH", str(env_path))

    loaded = load_retail_profile(
        project_root=project_root,
        profile_name="capital_constrained",
    )
    assert int(loaded["max_leverage"]) == 5


def test_retail_profile_loader_required_raises_when_missing(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    project_root.mkdir(parents=True)
    monkeypatch.delenv("BACKTEST_RETAIL_PROFILES_PATH", raising=False)

    with pytest.raises(FileNotFoundError):
        load_retail_profile(
            project_root=project_root,
            profile_name="capital_constrained",
            required=True,
        )


def test_retail_profile_loader_required_raises_when_profile_unknown(tmp_path):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    project_root.mkdir(parents=True)
    (project_root / "configs").mkdir(parents=True)
    (project_root / "configs" / "retail_profiles.yaml").write_text(
        "profiles:\n  capital_constrained:\n    max_leverage: 3\n",
        encoding="utf-8",
    )

    with pytest.raises(KeyError):
        load_retail_profile(
            project_root=project_root,
            profile_name="unknown_profile",
            required=True,
        )
