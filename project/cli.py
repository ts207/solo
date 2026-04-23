from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from project import PROJECT_ROOT

DEFAULT_REGISTRY_ROOT = Path("project/configs/registries")


def _path_or_none(value: str | None) -> Path | None:
    return Path(value) if value else None


def _result_exit_code(result: Any) -> int:
    if isinstance(result, dict):
        execution = result.get("execution")
        if isinstance(execution, dict) and "returncode" in execution:
            return int(execution["returncode"])
        if "exit_code" in result:
            return int(result["exit_code"])
        return 0
    return int(getattr(result, "exit_code", 0))


def _emit_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _run_discover(args: argparse.Namespace) -> int:
    from project import discover

    result = discover.run(
        args.proposal,
        registry_root=Path(args.registry_root),
        data_root=_path_or_none(args.data_root),
        run_id=args.run_id,
        plan_only=args.discover_action == "plan",
        dry_run=args.dry_run,
        check=args.check,
    )
    if isinstance(result, dict):
        _emit_json(result)
    return _result_exit_code(result)


def _run_discover_list_artifacts(args: argparse.Namespace) -> int:
    from project.artifacts.discovery import discover_run_artifacts

    _emit_json(discover_run_artifacts(run_id=args.run_id, data_root=_path_or_none(args.data_root)))
    return 0


def _run_discover_cells(args: argparse.Namespace) -> int:
    from project.research.cell_discovery.cells_cli import run_from_namespace

    result = run_from_namespace(args)
    _emit_json(result)
    return int(result.get("exit_code", 0))


def _run_trigger_parameter_sweep(args: argparse.Namespace) -> int:
    return _run_trigger_lane("parameter_sweep", args)


def _run_trigger_feature_cluster(args: argparse.Namespace) -> int:
    return _run_trigger_lane("feature_cluster", args)


def _run_trigger_lane(mode: str, args: argparse.Namespace) -> int:
    cmd = [
        sys.executable,
        "-m",
        "project.research.discover_triggers",
        "--mode",
        mode,
        "--symbol",
        args.symbol,
        "--out_dir",
        args.output_dir,
    ]
    if hasattr(args, "family"):
        cmd.extend(["--family", args.family])
    result = subprocess.run(cmd)
    return int(result.returncode)


def _run_emit_registry_payload(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    proposal = Path(args.proposal)
    out_path = output_dir / f"{proposal.stem}_registry_payload.json"
    payload = {
        "status": "experimental_adapter_only",
        "proposal_path": str(proposal),
        "registry_payload_path": str(out_path),
    }
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _emit_json(payload)
    return 0


def _run_validate(args: argparse.Namespace) -> int:
    from project import run as run_validation

    result = run_validation(args.run_id, data_root=_path_or_none(args.data_root))
    _emit_json(result)
    return _result_exit_code(result)


def _run_validate_specs(args: argparse.Namespace) -> int:
    from project.specs.validate_repo import run_all_validations

    return int(run_all_validations(root=Path(args.root), verbose=args.verbose))


def _run_promote(args: argparse.Namespace) -> int:
    from project import promote

    result = promote.run(
        run_id=args.run_id,
        symbols=args.symbols,
        out_dir=_path_or_none(args.out_dir),
        retail_profile=args.retail_profile,
    )
    if getattr(result, "diagnostics", None) is not None:
        _emit_json(result.diagnostics)
    return _result_exit_code(result)


def _run_promote_export(args: argparse.Namespace) -> int:
    from project import promote

    result = promote.export(
        run_id=args.run_id,
        data_root=_path_or_none(args.data_root),
    )
    if hasattr(result, "to_dict"):
        _emit_json(result.to_dict())
    else:
        _emit_json(result)
    return _result_exit_code(result)


def _run_deploy_export(args: argparse.Namespace) -> int:
    return _run_promote_export(args)


def _thesis_path_for_run(*, data_root: Path, run_id: str) -> Path:
    return data_root / "live" / "theses" / run_id / "promoted_theses.json"


def _run_deploy_list_theses(args: argparse.Namespace) -> int:
    data_root = _path_or_none(args.data_root) or PROJECT_ROOT.parent / "data"
    thesis_root = data_root / "live" / "theses"
    runs = (
        sorted(path.name for path in thesis_root.iterdir() if path.is_dir())
        if thesis_root.exists()
        else []
    )
    _emit_json({"data_root": str(data_root), "thesis_runs": runs})
    return 0


def _run_deploy_inspect(args: argparse.Namespace) -> int:
    data_root = _path_or_none(args.data_root) or PROJECT_ROOT.parent / "data"
    thesis_path = _path_or_none(args.thesis_path) or _thesis_path_for_run(
        data_root=data_root,
        run_id=args.run_id,
    )
    if not thesis_path.exists():
        raise FileNotFoundError(f"thesis artifact not found: {thesis_path}")
    _emit_json(json.loads(thesis_path.read_text(encoding="utf-8")))
    return 0


def _run_deploy_bind_config(args: argparse.Namespace) -> int:
    data_root = _path_or_none(args.data_root) or PROJECT_ROOT.parent / "data"
    thesis_path = _path_or_none(args.thesis_path) or _thesis_path_for_run(
        data_root=data_root,
        run_id=args.run_id,
    )
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"live_paper_{args.run_id}.yaml"
    payload = {
        "runtime_mode": args.runtime_mode,
        "strategy_runtime": {
            "implemented": True,
            "thesis_run_id": args.run_id,
            "thesis_path": str(thesis_path),
            "event_detector": {
                "adapter": "governed_runtime_core",
                "legacy_heuristic_enabled": False,
            },
        },
        "freshness_streams": [
            {"symbol": symbol.strip().lower()}
            for symbol in args.symbols.split(",")
            if symbol.strip()
        ],
    }
    out_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    _emit_json({"config_path": str(out_path), "thesis_path": str(thesis_path)})
    return 0


def _run_live_engine_entry(config: Path) -> int:
    script = PROJECT_ROOT / "scripts" / "run_live_engine.py"
    result = subprocess.run([sys.executable, str(script), "--config", str(config)])
    return int(result.returncode)


def _run_deploy_paper(args: argparse.Namespace) -> int:
    return _run_live_engine_entry(Path(args.config))


def _run_deploy_live(args: argparse.Namespace) -> int:
    return _run_live_engine_entry(Path(args.config))


def _run_deploy_status(args: argparse.Namespace) -> int:
    from project.live.deploy_status import build_deploy_status_report

    _emit_json(
        build_deploy_status_report(
            run_id=args.run_id,
            config_path=_path_or_none(args.config),
            data_root=_path_or_none(args.data_root),
        )
    )
    return 0


def _add_cell_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", default="BTCUSDT")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument("--data_root")
    parser.add_argument("--registry_root", default=str(DEFAULT_REGISTRY_ROOT))
    parser.add_argument("--spec_dir", default="spec/discovery")
    parser.add_argument("--search_budget", type=int, default=None)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="backtest")
    sub = parser.add_subparsers(dest="command")

    discover = sub.add_parser("discover", help="canonical discovery stage")
    discover_sub = discover.add_subparsers(dest="discover_action")
    for action in ("plan", "run"):
        stage = discover_sub.add_parser(action)
        stage.add_argument("--proposal", required=True)
        stage.add_argument("--registry_root", default=str(DEFAULT_REGISTRY_ROOT))
        stage.add_argument("--data_root")
        stage.add_argument("--run_id")
        stage.add_argument("--dry_run", action="store_true")
        stage.add_argument("--check", action="store_true")
        stage.set_defaults(func=_run_discover, discover_action=action)
    list_artifacts = discover_sub.add_parser("list-artifacts")
    list_artifacts.add_argument("--run_id", required=True)
    list_artifacts.add_argument("--data_root")
    list_artifacts.set_defaults(func=_run_discover_list_artifacts)

    cells = discover_sub.add_parser("cells", help="cell-first discovery lane")
    cells_sub = cells.add_subparsers(dest="cells_action")
    for action in ("verify-data", "plan", "run"):
        cell_parser = cells_sub.add_parser(action)
        _add_cell_common_args(cell_parser)
        cell_parser.set_defaults(func=_run_discover_cells, cells_action=action)
    summarize = cells_sub.add_parser("summarize")
    summarize.add_argument("--run_id", required=True)
    summarize.add_argument("--data_root")
    summarize.set_defaults(func=_run_discover_cells, cells_action="summarize")
    assemble = cells_sub.add_parser("assemble-theses")
    assemble.add_argument("--run_id", required=True)
    assemble.add_argument("--data_root")
    assemble.add_argument("--limit", type=int, default=20)
    assemble.set_defaults(func=_run_discover_cells, cells_action="assemble-theses")

    triggers = discover_sub.add_parser(
        "triggers",
        help="experimental proposal-generation lanes; adapter-only, not canonical discovery",
    )
    trigger_sub = triggers.add_subparsers(dest="trigger_action")
    parameter_sweep = trigger_sub.add_parser("parameter-sweep")
    parameter_sweep.add_argument("--family", required=True)
    parameter_sweep.add_argument("--symbol", default="BTCUSDT")
    parameter_sweep.add_argument("--output_dir", default="data/research/trigger_proposals")
    parameter_sweep.set_defaults(func=_run_trigger_parameter_sweep)
    feature_cluster = trigger_sub.add_parser("feature-cluster")
    feature_cluster.add_argument("--symbol", default="BTCUSDT")
    feature_cluster.add_argument("--output_dir", default="data/research/trigger_proposals")
    feature_cluster.set_defaults(func=_run_trigger_feature_cluster)
    registry_payload = trigger_sub.add_parser("emit-registry-payload")
    registry_payload.add_argument("--proposal", required=True)
    registry_payload.add_argument("--output_dir", default="data/research/trigger_registry_payloads")
    registry_payload.set_defaults(func=_run_emit_registry_payload)

    validate = sub.add_parser("validate", help="canonical validation stage")
    validate_sub = validate.add_subparsers(dest="validate_action")
    validate_run = validate_sub.add_parser("run")
    validate_run.add_argument("--run_id", required=True)
    validate_run.add_argument("--data_root")
    validate_run.set_defaults(func=_run_validate)
    validate_specs = validate_sub.add_parser("specs")
    validate_specs.add_argument("--root", default=".")
    validate_specs.add_argument("--verbose", action="store_true")
    validate_specs.set_defaults(func=_run_validate_specs)

    promote = sub.add_parser("promote", help="canonical promotion/export stage")
    promote_sub = promote.add_subparsers(dest="promote_action")
    promote_run = promote_sub.add_parser("run")
    promote_run.add_argument("--run_id", required=True)
    promote_run.add_argument("--symbols", required=True)
    promote_run.add_argument("--out_dir")
    promote_run.add_argument("--retail_profile", default="capital_constrained")
    promote_run.set_defaults(func=_run_promote)
    promote_export = promote_sub.add_parser("export")
    promote_export.add_argument("--run_id", required=True)
    promote_export.add_argument("--data_root")
    promote_export.set_defaults(func=_run_promote_export)

    deploy = sub.add_parser("deploy", help="canonical runtime deployment stage")
    deploy_sub = deploy.add_subparsers(dest="deploy_action")
    deploy_export = deploy_sub.add_parser("export")
    deploy_export.add_argument("--run_id", required=True)
    deploy_export.add_argument("--data_root")
    deploy_export.set_defaults(func=_run_deploy_export)
    bind_config = deploy_sub.add_parser("bind-config")
    bind_config.add_argument("--run_id", required=True)
    bind_config.add_argument("--data_root")
    bind_config.add_argument("--thesis_path")
    bind_config.add_argument("--out_dir", required=True)
    bind_config.add_argument("--runtime_mode", default="monitor_only")
    bind_config.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    bind_config.set_defaults(func=_run_deploy_bind_config)
    list_theses = deploy_sub.add_parser("list-theses")
    list_theses.add_argument("--data_root")
    list_theses.set_defaults(func=_run_deploy_list_theses)
    inspect = deploy_sub.add_parser("inspect")
    inspect.add_argument("--run_id", required=True)
    inspect.add_argument("--data_root")
    inspect.add_argument("--thesis_path")
    inspect.set_defaults(func=_run_deploy_inspect)
    inspect_thesis = deploy_sub.add_parser("inspect-thesis")
    inspect_thesis.add_argument("--run_id", required=True)
    inspect_thesis.add_argument("--data_root")
    inspect_thesis.add_argument("--thesis_path")
    inspect_thesis.set_defaults(func=_run_deploy_inspect)
    paper = deploy_sub.add_parser("paper-run")
    paper.add_argument("--config", required=True)
    paper.set_defaults(func=_run_deploy_paper)
    live = deploy_sub.add_parser("live-run")
    live.add_argument("--config", required=True)
    live.set_defaults(func=_run_deploy_live)
    status = deploy_sub.add_parser("status")
    status.add_argument("--run_id", required=True)
    status.add_argument("--config")
    status.add_argument("--data_root")
    status.set_defaults(func=_run_deploy_status)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 0
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
