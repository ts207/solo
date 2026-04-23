from pathlib import Path
from typing import List, Tuple

from project.core.timeframes import parse_timeframes


def build_core_stages(
    args,
    run_id: str,
    symbols: str,
    start: str,
    end: str,
    run_spot_pipeline: bool,
    project_root: Path,
) -> List[Tuple[str, Path, List[str]]]:
    stages: List[Tuple[str, Path, List[str]]] = []
    runtime_invariants_mode = (
        str(getattr(args, "runtime_invariants_mode", "off") or "off").strip().lower()
    )
    runtime_max_events = int(getattr(args, "runtime_max_events", 250000) or 250000)
    determinism_replay_checks = bool(int(getattr(args, "determinism_replay_checks", 0) or 0))
    oms_replay_checks = bool(int(getattr(args, "oms_replay_checks", 0) or 0))
    force_flag = str(int(getattr(args, "force", 0) or 0))

    timeframes = parse_timeframes(getattr(args, "timeframes", "5m"))
    universe_timeframe = timeframes[0]

    for tf in timeframes:
        stages.extend(
            [
                (
                    f"build_cleaned_{tf}",
                    project_root / "pipelines" / "clean" / "build_cleaned_bars.py",
                    [
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                        "--market",
                        "perp",
                        "--timeframe",
                        tf,
                        "--start",
                        start,
                        "--end",
                        end,
                        "--funding_scale",
                        str(getattr(args, "funding_scale", "auto")),
                    ],
                ),
                (
                    f"build_features_{tf}",
                    project_root / "pipelines" / "features" / "build_features.py",
                    [
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                        "--timeframe",
                        tf,
                        "--start",
                        start,
                        "--end",
                        end,
                        "--feature_schema_version",
                        str(getattr(args, "feature_schema_version", "v2")),
                    ],
                ),
            ]
        )

    stages.append(
        (
            "build_universe_snapshots",
            project_root / "pipelines" / "ingest" / "build_universe_snapshots.py",
            [
                "--run_id",
                run_id,
                "--symbols",
                symbols,
                "--market",
                "perp",
                "--timeframe",
                universe_timeframe,
                "--force",
                force_flag,
            ],
        )
    )

    for tf in timeframes:
        stages.extend(
            [
                (
                    f"build_market_context_{tf}",
                    project_root / "pipelines" / "features" / "build_market_context.py",
                    [
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                        "--timeframe",
                        tf,
                        "--start",
                        start,
                        "--end",
                        end,
                    ],
                ),
                (
                    f"build_microstructure_rollup_{tf}",
                    project_root / "pipelines" / "features" / "build_microstructure_rollup.py",
                    [
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                        "--timeframe",
                        tf,
                        "--start",
                        start,
                        "--end",
                        end,
                    ],
                ),
                (
                    f"validate_feature_integrity_{tf}",
                    project_root / "pipelines" / "clean" / "validate_feature_integrity.py",
                    [
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                        "--timeframe",
                        tf,
                        "--fail_on_issues",
                        "0",
                    ],
                ),
                (
                    f"validate_data_coverage_{tf}",
                    project_root / "pipelines" / "clean" / "validate_data_coverage.py",
                    [
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                        "--timeframe",
                        tf,
                        "--max_gap_pct",
                        "0.05",
                    ],
                ),
            ]
        )

    if run_spot_pipeline:
        for tf in timeframes:
            stages.extend(
                [
                    (
                        f"build_cleaned_{tf}_spot",
                        project_root / "pipelines" / "clean" / "build_cleaned_bars.py",
                        [
                            "--run_id",
                            run_id,
                            "--symbols",
                            symbols,
                            "--market",
                            "spot",
                            "--timeframe",
                            tf,
                            "--start",
                            start,
                            "--end",
                            end,
                            "--funding_scale",
                            str(getattr(args, "funding_scale", "auto")),
                        ],
                    ),
                    (
                        f"build_features_{tf}_spot",
                        project_root / "pipelines" / "features" / "build_features.py",
                        [
                            "--run_id",
                            run_id,
                            "--symbols",
                            symbols,
                            "--market",
                            "spot",
                            "--timeframe",
                            tf,
                            "--start",
                            start,
                            "--end",
                            end,
                            "--feature_schema_version",
                            str(getattr(args, "feature_schema_version", "v2")),
                        ],
                    ),
                ]
            )

    if runtime_invariants_mode != "off":
        stages.extend(
            [
                (
                    "build_normalized_replay_stream",
                    project_root / "pipelines" / "runtime" / "build_normalized_replay_stream.py",
                    ["--run_id", run_id, "--max_events", str(int(runtime_max_events))],
                ),
                (
                    "run_causal_lane_ticks",
                    project_root / "pipelines" / "runtime" / "run_causal_lane_ticks.py",
                    ["--run_id", run_id],
                ),
            ]
        )
        if determinism_replay_checks:
            stages.append(
                (
                    "run_determinism_replay_checks",
                    project_root / "pipelines" / "runtime" / "run_determinism_replay_checks.py",
                    ["--run_id", run_id],
                )
            )
        if oms_replay_checks:
            stages.append(
                (
                    "run_oms_replay_validation",
                    project_root / "pipelines" / "runtime" / "run_oms_replay_validation.py",
                    ["--run_id", run_id],
                )
            )

    return stages
