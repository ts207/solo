from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from project import PROJECT_ROOT


def _path_or_none(value: str | None) -> Path | None:
    return Path(value) if value else None


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _emit_payload(payload: Any) -> None:
    if payload is None:
        return
    if isinstance(payload, (str, bytes)):
        text = payload.decode() if isinstance(payload, bytes) else payload
        if text.strip():
            print(text)
        return
    try:
        print(json.dumps(payload, indent=2, sort_keys=True, default=_json_default))
    except TypeError:
        print(str(payload))


def _result_exit_code(result: Any) -> int:
    if result is None:
        return 0
    if isinstance(result, int):
        return int(result)
    if isinstance(result, dict):
        if "execution" in result and isinstance(result["execution"], dict):
            return int(result["execution"].get("returncode", 0) or 0)
        if "returncode" in result:
            return int(result.get("returncode", 0) or 0)
        if "exit_code" in result:
            return int(result.get("exit_code", 0) or 0)
        return 0
    if hasattr(result, "exit_code"):
        return int(getattr(result, "exit_code") or 0)
    if hasattr(result, "returncode"):
        return int(getattr(result, "returncode") or 0)
    return 0


def _run_discover(args: argparse.Namespace) -> int:
    import project.discover as discover_module

    result = discover_module.run(
        args.proposal,
        registry_root=Path(args.registry_root),
        data_root=_path_or_none(args.data_root),
        run_id=args.run_id,
        plan_only=bool(args.discover_action == "plan"),
        dry_run=bool(args.dry_run),
        check=bool(args.check),
    )
    _emit_payload(result)
    return _result_exit_code(result)


def _run_trigger_discovery(args: argparse.Namespace) -> int:
    from project.core.config import get_data_root

    if args.trigger_command == "emit-registry-payload":
        payload = {
            "family": args.family,
            "symbol": args.symbol,
            "timeframe": args.timeframe,
            "lane": "advanced_internal_trigger_discovery",
            "status": "proposal_only",
        }
        _emit_payload(payload)
        return 0

    mode = {
        "parameter-sweep": "parameter_sweep",
        "feature-cluster": "feature_cluster",
    }[args.trigger_command]
    cmd = [
        sys.executable,
        "-m",
        "project.research.discover_triggers",
        "--mode",
        mode,
        "--symbol",
        args.symbol,
        "--timeframe",
        args.timeframe,
        "--data_root",
        str(_path_or_none(args.data_root) or get_data_root()),
        "--out_dir",
        args.out_dir,
    ]
    if args.trigger_command == "parameter-sweep":
        cmd.extend(["--family", args.family])
    completed = subprocess.run(cmd, check=False)
    return int(completed.returncode)


def _run_validate(args: argparse.Namespace) -> int:
    import project.validate as validate_module
    from project.spec_validation.cli import run_all_validations

    if args.validate_command == "specs":
        return int(run_all_validations())
    result = validate_module.run(
        run_id=args.run_id,
        data_root=_path_or_none(args.data_root),
    )
    _emit_payload(result)
    return _result_exit_code(result)


def _run_promote(args: argparse.Namespace) -> int:
    import project.promote as promote_module

    if args.promote_command == "run":
        result = promote_module.run(
            run_id=args.run_id,
            symbols=args.symbols,
            out_dir=_path_or_none(args.out_dir),
            retail_profile=args.retail_profile,
        )
        _emit_payload(getattr(result, "diagnostics", None) or result)
        return _result_exit_code(result)

    result = promote_module.export(
        args.run_id,
        data_root=_path_or_none(args.data_root),
    )
    _emit_payload(
        {
            "run_id": result.run_id,
            "output_path": result.output_path,
            "index_path": result.index_path,
            "thesis_count": result.thesis_count,
            "active_count": result.active_count,
            "pending_count": result.pending_count,
        }
    )
    return 0


def _deploy_paper(args: argparse.Namespace) -> int:
    import project.promote as promote_module
    from project.core.config import get_data_root

    data_root = _path_or_none(args.data_root) or get_data_root()
    promotion_root = data_root / "reports" / "promotions" / str(args.run_id)
    promoted_candidates_exist = any(
        path.exists()
        for path in (
            promotion_root / "promoted_candidates.parquet",
            promotion_root / "promoted_candidates.csv",
        )
    )
    if not promoted_candidates_exist:
        print(f"Error: No promoted thesis found for run {args.run_id}")
        print("Deploy stage requires a completed 'promote' stage before deploy.")
        return 1
    try:
        result = promote_module.export(args.run_id, data_root=data_root)
    except Exception as exc:
        print(f"Error: {exc}")
        return 1
    _emit_payload(
        {
            "run_id": result.run_id,
            "deployment": "paper",
            "output_path": result.output_path,
            "index_path": result.index_path,
            "thesis_count": result.thesis_count,
            "active_count": result.active_count,
            "pending_count": result.pending_count,
        }
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backtest",
        description="Canonical Edge command surface for discover, validate, promote, and deploy.",
    )
    subparsers = parser.add_subparsers(dest="command")

    discover = subparsers.add_parser(
        "discover",
        help="Canonical bounded research entry lane.",
        description=(
            "Canonical bounded research entry lane. Use plan/run for proposal-driven "
            "discovery. The triggers subgroup is an advanced internal research lane."
        ),
    )
    discover_subparsers = discover.add_subparsers(dest="discover_action")

    for action in ("plan", "run"):
        action_parser = discover_subparsers.add_parser(
            action,
            help=f"{action.title()} a proposal through canonical discovery.",
        )
        action_parser.add_argument("--proposal", required=True)
        action_parser.add_argument("--registry_root", default="project/configs/registries")
        action_parser.add_argument("--data_root", default=None)
        action_parser.add_argument("--run_id", default=None)
        action_parser.add_argument("--dry_run", type=int, default=0)
        action_parser.add_argument("--check", type=int, default=0)
        action_parser.set_defaults(func=_run_discover)

    triggers = discover_subparsers.add_parser(
        "triggers",
        help="Advanced internal trigger discovery lane.",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Advanced internal trigger discovery lane.\n"
            "Proposal-generating only.\n"
            "No runtime effect.\n"
            "Manual review required before registry adoption."
        ),
    )
    trigger_subparsers = triggers.add_subparsers(dest="trigger_command")
    for name, help_text in (
        ("parameter-sweep", "Advanced internal trigger discovery via parameter sweep."),
        ("feature-cluster", "Advanced internal trigger discovery via feature excursion mining."),
        ("emit-registry-payload", "Emit a proposal-only registry payload preview."),
    ):
        trigger_parser = trigger_subparsers.add_parser(name, help=help_text)
        trigger_parser.add_argument("--family", default="vol_shock")
        trigger_parser.add_argument("--symbol", default="BTCUSDT")
        trigger_parser.add_argument("--timeframe", default="5m")
        trigger_parser.add_argument("--data_root", default=None)
        trigger_parser.add_argument(
            "--out_dir",
            default=str(PROJECT_ROOT.parent / "data" / "trigger_proposals"),
        )
        trigger_parser.set_defaults(func=_run_trigger_discovery)

    validate = subparsers.add_parser(
        "validate",
        help="Canonical validation surface.",
        description="Canonical validation surface for run validation and explicit spec checks.",
    )
    validate_subparsers = validate.add_subparsers(dest="validate_command")
    validate_run = validate_subparsers.add_parser("run", help="Validate a completed run.")
    validate_run.add_argument("--run_id", required=True)
    validate_run.add_argument("--data_root", default=None)
    validate_run.set_defaults(func=_run_validate)
    validate_specs = validate_subparsers.add_parser(
        "specs", help="Run ontology, grammar, and search-spec validation explicitly."
    )
    validate_specs.set_defaults(func=_run_validate)

    promote = subparsers.add_parser(
        "promote",
        help="Canonical promotion and thesis export surface.",
        description="Canonical promotion surface for candidate promotion and thesis export.",
    )
    promote_subparsers = promote.add_subparsers(dest="promote_command")
    promote_run = promote_subparsers.add_parser("run", help="Promote validated candidates.")
    promote_run.add_argument("--run_id", required=True)
    promote_run.add_argument("--symbols", required=True)
    promote_run.add_argument("--out_dir", default=None)
    promote_run.add_argument("--retail_profile", default="capital_constrained")
    promote_run.set_defaults(func=_run_promote)
    promote_export = promote_subparsers.add_parser("export", help="Export promoted theses.")
    promote_export.add_argument("--run_id", required=True)
    promote_export.add_argument("--data_root", default=None)
    promote_export.set_defaults(func=_run_promote)

    deploy = subparsers.add_parser(
        "deploy",
        help="Canonical deployment surface.",
        description="Canonical deployment surface. Only promoted theses may be deployed.",
    )
    deploy_subparsers = deploy.add_subparsers(dest="deploy_command")
    deploy_paper = deploy_subparsers.add_parser(
        "paper",
        help="Export promoted theses for paper deployment.",
    )
    deploy_paper.add_argument("--run_id", required=True)
    deploy_paper.add_argument("--data_root", default=None)
    deploy_paper.set_defaults(func=_deploy_paper)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 0
    return int(func(args))


if __name__ == "__main__":
    raise SystemExit(main())
