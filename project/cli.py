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


def _run_discover_list_artifacts(args: argparse.Namespace) -> int:
    data_root = _path_or_none(args.data_root) or PROJECT_ROOT.parent / "data"
    run_id = str(args.run_id)
    phase2_root = data_root / "reports" / "phase2" / run_id
    artifact_names = {
        "phase2_candidates.parquet",
        "phase2_candidates.csv",
        "phase2_diagnostics.json",
        "hypothesis_registry.parquet",
        "search_burden_summary.json",
    }
    artifacts = (
        sorted(
            path
            for path in phase2_root.rglob("*")
            if path.is_file() and path.name in artifact_names
        )
        if phase2_root.exists()
        else []
    )
    if not artifacts:
        print(f"No discovery artifacts found for run {run_id}")
        return 0

    print(f"Artifacts for discovery run {run_id}:")
    for path in artifacts:
        print(path.relative_to(data_root).as_posix())
    return 0


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


def _deploy_export(args: argparse.Namespace) -> int:
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
            "deployment": "export",
            "output_path": result.output_path,
            "index_path": result.index_path,
            "thesis_count": result.thesis_count,
            "active_count": result.active_count,
            "pending_count": result.pending_count,
        }
    )
    return 0


def _deploy_bind_config(args: argparse.Namespace) -> int:
    """Generate a runtime config YAML from a promoted thesis run."""
    import yaml  # type: ignore[import-untyped]

    from project.core.config import get_data_root

    data_root = _path_or_none(args.data_root) or get_data_root()
    run_id = str(args.run_id)
    thesis_path = data_root / "live" / "theses" / run_id / "promoted_theses.json"
    if not thesis_path.exists():
        print(f"Error: No promoted theses found at {thesis_path}")
        print("Run 'promote export --run_id <run_id>' first.")
        return 1

    import json as _json

    bundle = _json.loads(thesis_path.read_text())
    theses = bundle.get("theses", [])
    if not theses:
        print(f"Error: Promoted thesis bundle for {run_id} contains no theses.")
        return 1

    event_ids: list[str] = []
    symbols: list[str] = []
    for t in theses:
        ev = t.get("primary_event_id") or ""
        if ev and ev not in event_ids:
            event_ids.append(ev)
        sym_raw = t.get("thesis_id", "")
        parts = sym_raw.split("::")
        if len(parts) >= 3:
            sym = parts[2]
            if sym and sym not in symbols:
                symbols.append(sym)

    if not event_ids:
        print("Error: Could not extract event IDs from theses — check thesis schema.")
        return 1

    out_dir = _path_or_none(getattr(args, "out_dir", None)) or (PROJECT_ROOT / "configs")
    out_path = out_dir / f"live_paper_{run_id}.yaml"

    primary_symbol = (symbols[0] if symbols else "BTCUSDT").upper()

    config: dict = {
        "workflow_id": f"live_paper_{run_id}",
        "golden_workflow_config": "project/configs/golden_workflow.yaml",
        "runtime_run_id": f"live_paper_{run_id}",
        "runtime_mode": "trading",
        "execution_mode": "measured",
        "stale_threshold_sec": 60.0,
        "freshness_streams": [{"symbol": primary_symbol, "stream": "kline_5m"}],
        "oms_lineage": {
            "order_source": "paper_oms",
            "session_id": f"live-paper-{run_id[:40]}",
        },
        "live_state_snapshot_path": f"artifacts/live_state_{run_id[:40]}.json",
        "microstructure_recovery_streak": 2,
        "account_sync_interval_seconds": 15.0,
        "account_sync_failure_threshold": 4,
        "execution_degradation_min_samples": 4,
        "execution_degradation_warn_edge_bps": 0.0,
        "execution_degradation_block_edge_bps": -8.0,
        "execution_degradation_throttle_scale": 0.5,
        "runtime_metrics_snapshot_path": f"artifacts/live_runtime_metrics_{run_id[:40]}.json",
        "strategy_runtime": {
            "implemented": True,
            "thesis_run_id": run_id,
            "include_pending_theses": False,
            "auto_submit": True,
            "supported_event_families": event_ids,
            "allowed_actions": ["probe", "trade_small"],
            "max_notional_fraction": 0.03,
            "max_spread_bps": 5.0,
            "min_depth_usd": 50000.0,
            "min_tob_coverage": 0.9,
            "memory_root": f"artifacts/live_memory/{run_id[:40]}",
            "event_detector": {
                "vol_shock_min_abs_move_bps": 35.0,
                "liquidity_vacuum_min_spread_bps": 5.0,
                "liquidity_vacuum_max_depth_usd": 25000.0,
                "liquidation_cascade_min_abs_move_bps": 80.0,
                "liquidation_cascade_min_abs_oi_drop_fraction": 0.03,
                "liquidation_cascade_min_abs_funding_rate": 0.0005,
            },
            "decision_policy": {
                "watch_min": 0.25,
                "probe_min": 0.4,
                "small_min": 0.6,
                "normal_min": 0.8,
                "max_contradiction_penalty": 0.4,
            },
        },
        "runtime_alerts": {
            "metrics_path": f"artifacts/live_runtime_metrics_{run_id[:40]}.json",
            "alert_log_path": f"artifacts/live_runtime_alerts_{run_id[:40]}.jsonl",
            "poll_interval_seconds": 15.0,
            "snapshot_max_age_seconds": 180.0,
            "decision_drought_seconds": 3600.0,
            "funding_elevated_abs": 0.0003,
            "funding_stretched_abs": 0.0005,
            "oi_stable_abs": 0.01,
            "oi_flush_abs": 0.03,
            "ratio_min_total": 8,
            "trade_small_probe_ratio_baseline": 1.0,
            "trade_small_probe_ratio_tolerance_fraction": 0.5,
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(yaml.dump(config, default_flow_style=False, sort_keys=False))
    _emit_payload(
        {
            "run_id": run_id,
            "config_path": str(out_path),
            "event_ids": event_ids,
            "symbols": symbols,
            "thesis_count": len(theses),
        }
    )
    print(f"Bound config written to: {out_path}")
    return 0


def _deploy_inspect(args: argparse.Namespace) -> int:
    from project.core.config import get_data_root
    from project.live.deploy_status import inspect_deployment

    payload = inspect_deployment(
        str(args.run_id),
        data_root=_path_or_none(getattr(args, "data_root", None)) or get_data_root(),
        config_path=_path_or_none(getattr(args, "config", None)),
    )
    _emit_payload(payload)
    return 0


def _run_live_engine_entry(config: str) -> int:
    config_path = Path(config)
    if not config_path.exists():
        print(f"Error: Config not found: {config_path}")
        return 1

    engine_script = PROJECT_ROOT / "scripts" / "run_live_engine.py"
    cmd = [sys.executable, str(engine_script), "--config", str(config_path)]
    result = subprocess.run(cmd)
    return result.returncode


def _deploy_paper_run(args: argparse.Namespace) -> int:
    return _run_live_engine_entry(args.config)


def _deploy_live_run(args: argparse.Namespace) -> int:
    print("WARNING: Launching LIVE execution engine!")
    return _run_live_engine_entry(args.config)


def _deploy_status(args: argparse.Namespace) -> int:
    from project.core.config import get_data_root
    from project.live.deploy_status import deployment_status

    payload = deployment_status(
        str(args.run_id),
        data_root=_path_or_none(getattr(args, "data_root", None)) or get_data_root(),
        config_path=_path_or_none(getattr(args, "config", None)),
        snapshot_path=_path_or_none(getattr(args, "snapshot_path", None)),
        metrics_path=_path_or_none(getattr(args, "metrics_path", None)),
    )
    _emit_payload(payload)
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

    discover_list = discover_subparsers.add_parser(
        "list-artifacts",
        help="List canonical discovery artifacts for a completed run.",
    )
    discover_list.add_argument("--run_id", required=True)
    discover_list.add_argument("--data_root", default=None)
    discover_list.set_defaults(func=_run_discover_list_artifacts)

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

    deploy_export = deploy_subparsers.add_parser(
        "export",
        help="Export promoted theses to the live thesis store (does not launch runtime).",
    )
    deploy_export.add_argument("--run_id", required=True)
    deploy_export.add_argument("--data_root", default=None)
    deploy_export.set_defaults(func=_deploy_export)

    deploy_bind = deploy_subparsers.add_parser(
        "bind-config",
        help="Generate a runtime config YAML from a promoted thesis run.",
    )
    deploy_bind.add_argument("--run_id", required=True)
    deploy_bind.add_argument("--data_root", default=None)
    deploy_bind.add_argument(
        "--out_dir", default=None, help="Output directory for the config YAML."
    )
    deploy_bind.set_defaults(func=_deploy_bind_config)

    deploy_inspect = deploy_subparsers.add_parser(
        "inspect",
        help="Inspect deployment status for a run ID.",
    )
    deploy_inspect.add_argument("--run_id", required=True)
    deploy_inspect.add_argument("--data_root", default=None)
    deploy_inspect.add_argument("--config", default=None)
    deploy_inspect.set_defaults(func=_deploy_inspect)

    deploy_paper_run = deploy_subparsers.add_parser(
        "paper-run",
        help="Launch the live engine in paper trading mode.",
    )
    deploy_paper_run.add_argument(
        "--config", required=True, help="Path to the bound runtime config YAML."
    )
    deploy_paper_run.set_defaults(func=_deploy_paper_run)

    deploy_live_run = deploy_subparsers.add_parser(
        "live-run",
        help="Launch the live engine in live trading mode.",
    )
    deploy_live_run.add_argument(
        "--config", required=True, help="Path to the bound runtime config YAML."
    )
    deploy_live_run.set_defaults(func=_deploy_live_run)

    deploy_status = deploy_subparsers.add_parser(
        "status",
        help="Check the runtime status of a deployment.",
    )
    deploy_status.add_argument("--run_id", required=True)
    deploy_status.add_argument("--data_root", default=None)
    deploy_status.add_argument("--config", default=None)
    deploy_status.add_argument("--snapshot_path", default=None)
    deploy_status.add_argument("--metrics_path", default=None)
    deploy_status.set_defaults(func=_deploy_status)

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
