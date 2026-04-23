from __future__ import annotations

import json
from pathlib import Path

from project.pipelines.effective_config import (
    build_effective_config_payload,
    resolve_effective_args,
    write_effective_config,
)
from project.pipelines.pipeline_planning import build_parser
from project.pipelines.pipeline_provenance import data_fingerprint


def test_resolve_effective_args_respects_precedence(tmp_path: Path) -> None:
    parser = build_parser()
    base = tmp_path / "base.yaml"
    overlay = tmp_path / "overlay.yaml"
    base.write_text(
        "\n".join(
            [
                "mode: production",
                "run_phase2_conditional: 0",
                "phase2_max_conditions: 12",
                "",
            ]
        ),
        encoding="utf-8",
    )
    overlay.write_text(
        "\n".join(
            [
                "mode: certification",
                "run_phase2_conditional: 0",
                "phase2_max_conditions: 25",
                "",
            ]
        ),
        encoding="utf-8",
    )

    args, resolution = resolve_effective_args(
        parser,
        [
            "--experiment_config",
            str(base),
            "--config",
            str(overlay),
            "--mode",
            "research",
            "--run_phase2_conditional",
            "1",
            "--override",
            "phase2_max_conditions=77",
        ],
    )

    assert args.mode == "research"
    assert int(args.run_phase2_conditional) == 1
    assert int(args.phase2_max_conditions) == 77
    assert resolution["raw_inputs"]["experiment_config_path"] == str(base)
    assert resolution["raw_inputs"]["config_overlay_paths"] == [str(overlay)]
    assert "mode" in resolution["explicit_cli_destinations"]


def test_effective_config_write_is_deterministic(tmp_path: Path) -> None:
    parser = build_parser()
    args, resolution = resolve_effective_args(
        parser,
        [
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
        ],
    )
    payload = build_effective_config_payload(
        run_id="deterministic",
        resolution=resolution,
        preflight={
            "parsed_symbols": ["BTCUSDT", "ETHUSDT"],
            "normalized_timeframes_csv": "5m",
            "objective_name": "retail_profitability",
            "objective_spec_path": "spec/objectives/retail_profitability.yaml",
            "objective_spec_hash": "hash-1",
            "retail_profile_name": "capital_constrained",
            "retail_profile_spec_path": "spec/runtime/retail_profiles.yaml",
            "retail_profile_spec_hash": "hash-2",
            "runtime_invariants_mode": "audit",
            "search_spec": "spec/search_space.yaml",
        },
    )
    path_a, hash_a = write_effective_config(
        data_root=tmp_path, run_id="deterministic", payload=payload
    )
    path_b, hash_b = write_effective_config(
        data_root=tmp_path, run_id="deterministic", payload=payload
    )

    assert path_a == path_b
    assert hash_a == hash_b
    content = json.loads(path_a.read_text(encoding="utf-8"))
    assert content["config_resolution"]["normalized_symbols"] == ["BTCUSDT", "ETHUSDT"]


def test_data_fingerprint_changes_when_effective_config_hash_changes(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    project_root = tmp_path / "proj"
    project_root.mkdir(parents=True, exist_ok=True)
    spec_dir = project_root.parent / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)
    (spec_dir / "test_spec.yaml").write_text("key: value\n", encoding="utf-8")
    lake_dir = data_root / "lake" / "raw" / "binance" / "perp" / "BTCUSDT"
    lake_dir.mkdir(parents=True, exist_ok=True)
    (lake_dir / "ohlcv.csv").write_text("ts,price\n1,100\n", encoding="utf-8")

    hash_a, _ = data_fingerprint(
        ["BTCUSDT"],
        "run-1",
        project_root=project_root,
        data_root=data_root,
        effective_config_hash="sha256:a",
    )
    hash_b, _ = data_fingerprint(
        ["BTCUSDT"],
        "run-1",
        project_root=project_root,
        data_root=data_root,
        effective_config_hash="sha256:b",
    )

    assert hash_a != hash_b


def test_pipeline_parser_rejects_removed_blueprint_promotion_alias() -> None:
    parser = build_parser()
    option_strings = {
        option for action in parser._actions for option in getattr(action, "option_strings", [])
    }
    assert "--run_candidate_promotion" in option_strings
    assert "--run_blueprint_promotion" not in option_strings


def test_pipeline_parser_accepts_event_parameter_overrides_json() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--event_parameter_overrides",
            (
                '{"LIQUIDATION_EXHAUSTION_REVERSAL": '
                '{"liquidation_quantile": 0.9, "cooldown_bars": 36}}'
            ),
        ]
    )

    assert args.event_parameter_overrides == {
        "LIQUIDATION_EXHAUSTION_REVERSAL": {
            "liquidation_quantile": 0.9,
            "cooldown_bars": 36,
        }
    }
