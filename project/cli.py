from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from project import PROJECT_ROOT


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="edge",
        description="Canonical CLI for the Edge 4-stage model: discover -> validate -> promote -> deploy.",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        metavar="{discover,validate,promote,deploy,ingest,catalog}",
    )

    # --- CANONICAL STAGES ---

    # 1. DISCOVER
    discover_parser = subparsers.add_parser("discover", help="Stage 1: Broad candidate generation.")
    discover_sub = discover_parser.add_subparsers(dest="subcommand")

    discover_run = discover_sub.add_parser("run", help="Execute discovery for a proposal.")
    discover_run.add_argument("--proposal", required=True)
    discover_run.add_argument("--registry_root", default="project/configs/registries")
    discover_run.add_argument("--data_root", default=None)
    discover_run.add_argument("--run_id", default=None)
    discover_run.add_argument("--out_dir", default=None)
    discover_run.add_argument("--check", type=int, default=0)

    discover_plan = discover_sub.add_parser("plan", help="Plan discovery without executing.")
    discover_plan.add_argument("--proposal", required=True)
    discover_plan.add_argument("--registry_root", default="project/configs/registries")
    discover_plan.add_argument("--data_root", default=None)
    discover_plan.add_argument("--run_id", default=None)
    discover_plan.add_argument("--out_dir", default=None)

    discover_artifacts = discover_sub.add_parser("list-artifacts", help="List discovery artifacts.")
    discover_artifacts.add_argument("--run_id", required=True)
    discover_artifacts.add_argument("--data_root", default=None)

    # --- ADVANCED/INTERNAL TRIGGER DISCOVERY ---
    # Proposal-generating only. No runtime effect. Manual review required.
    triggers_parser = discover_sub.add_parser(
        "triggers",
        help="Advanced: Mining and proposing new trigger candidate definitions (internal research lane).",
        description="Advanced/Internal trigger discovery lane.\nProposal-generating only. No runtime effect. Manual review required before registry adoption.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    triggers_sub = triggers_parser.add_subparsers(dest="trigger_command")

    sweep_parser = triggers_sub.add_parser(
        "parameter-sweep",
        help="Run parameter sweep over a detector family (e.g. vol_shock) to propose candidate triggers."
    )
    sweep_parser.add_argument("--family", default="vol_shock")
    sweep_parser.add_argument("--symbol", default="BTCUSDT")
    sweep_parser.add_argument("--timeframe", default="5m")
    sweep_parser.add_argument("--data_root", default=None)
    sweep_parser.add_argument("--out_dir", default=None)

    cluster_parser = triggers_sub.add_parser(
        "feature-cluster",
        help="Mine recurring feature excursions to propose new trigger interaction families."
    )
    cluster_parser.add_argument("--symbol", default="BTCUSDT")
    cluster_parser.add_argument("--timeframe", default="5m")
    cluster_parser.add_argument("--data_root", default=None)
    cluster_parser.add_argument("--out_dir", default=None)

    report_parser = triggers_sub.add_parser(
        "report",
        help="Inspect generated candidate trigger proposals and registry novelty scores."
    )
    report_parser.add_argument("--proposal_dir", required=True)

    payload_parser = triggers_sub.add_parser(
        "emit-registry-payload",
        help="Generate a registry YAML snippet for a given candidate trigger ID."
    )
    payload_parser.add_argument("--candidate_id", required=True)
    payload_parser.add_argument("--proposal_dir", required=True)

    # Governance Control Plane
    list_parser = triggers_sub.add_parser("list", help="List all generated trigger proposals and their adoption states.")
    list_parser.add_argument("--proposal_dir", default="data/trigger_proposals")

    inspect_parser = triggers_sub.add_parser("inspect", help="Inspect a specific trigger proposal's details.")
    inspect_parser.add_argument("--candidate_id", required=True)
    inspect_parser.add_argument("--proposal_dir", default="data/trigger_proposals")

    review_parser = triggers_sub.add_parser("review", help="Mark a trigger proposal as under_review.")
    review_parser.add_argument("--candidate_id", required=True)
    review_parser.add_argument("--proposal_dir", default="data/trigger_proposals")

    approve_parser = triggers_sub.add_parser("approve", help="Approve a trigger proposal.")
    approve_parser.add_argument("--candidate_id", required=True)
    approve_parser.add_argument("--proposal_dir", default="data/trigger_proposals")

    reject_parser = triggers_sub.add_parser("reject", help="Reject a trigger proposal.")
    reject_parser.add_argument("--candidate_id", required=True)
    reject_parser.add_argument("--reason", required=True)
    reject_parser.add_argument("--proposal_dir", default="data/trigger_proposals")

    adopt_parser = triggers_sub.add_parser("mark-adopted", help="Mark an approved trigger proposal as formally adopted.")
    adopt_parser.add_argument("--candidate_id", required=True)
    adopt_parser.add_argument("--proposal_dir", default="data/trigger_proposals")

    # 2. VALIDATE
    validate_parser = subparsers.add_parser("validate", help="Stage 2: Truth-testing and robustness.")
    validate_sub = validate_parser.add_subparsers(dest="subcommand")

    validate_run = validate_sub.add_parser("run", help="Run formal validation on a discovery run.")
    validate_run.add_argument("--run_id", required=True)
    validate_run.add_argument("--data_root", default=None)

    validate_report = validate_sub.add_parser("report", help="Build regime/stability reports.")
    validate_report.add_argument("--run_id", required=True)
    validate_report.add_argument("--data_root", default=None)

    validate_diagnose = validate_sub.add_parser("diagnose", help="Write negative-result diagnostics.")
    validate_diagnose.add_argument("--run_id", required=True)
    validate_diagnose.add_argument("--program_id", default=None)
    validate_diagnose.add_argument("--data_root", default=None)

    validate_artifacts = validate_sub.add_parser("list-artifacts", help="List validation artifacts.")
    validate_artifacts.add_argument("--run_id", required=True)
    validate_artifacts.add_argument("--data_root", default=None)

    # 3. PROMOTE
    promote_parser = subparsers.add_parser("promote", help="Stage 3: Packaging and governance.")
    promote_sub = promote_parser.add_subparsers(dest="subcommand")

    promote_run = promote_sub.add_parser("run", help="Promote validated candidates to theses.")
    promote_run.add_argument("--run_id", required=True)
    promote_run.add_argument("--symbols", required=True)
    promote_run.add_argument("--out_dir", default=None)
    promote_run.add_argument("--retail_profile", default="capital_constrained")

    promote_export = promote_sub.add_parser("export", help="Export promoted theses for live use.")
    promote_export.add_argument("--run_id", required=True)
    promote_export.add_argument("--data_root", default=None)

    promote_artifacts = promote_sub.add_parser("list-artifacts", help="List promotion artifacts.")
    promote_artifacts.add_argument("--run_id", required=True)
    promote_artifacts.add_argument("--data_root", default=None)

    # 4. DEPLOY
    deploy_parser = subparsers.add_parser(
        "deploy",
        help="Stage 4: Runtime execution.",
        description=(
            "Deploy stage routes through the live-engine launcher.\n"
            "Requires a completed 'promote' stage with promoted_theses.json.\n"
            "Valid runtime modes: paper (dry-run), live (gated production).\n"
            "The --config flag accepts a live engine config YAML path.\n"
            "Bundle-only export is compatibility behavior, not the default path."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    deploy_sub = deploy_parser.add_subparsers(dest="subcommand")

    deploy_list = deploy_sub.add_parser("list-theses", help="List available promoted theses.")
    deploy_list.add_argument("--data_root", default=None)

    deploy_inspect = deploy_sub.add_parser("inspect-thesis", help="Inspect a specific promoted thesis.")
    deploy_inspect.add_argument("--run_id", required=True)
    deploy_inspect.add_argument("--data_root", default=None)

    deploy_paper = deploy_sub.add_parser("paper", help="[DRY-RUN] Plan paper trading session.")
    deploy_paper.add_argument("--run_id", required=True)
    deploy_paper.add_argument("--data_root", default=None)
    deploy_paper.add_argument("--config", default=None, help="Path to live engine config YAML.")

    deploy_live = deploy_sub.add_parser("live", help="[GATED] Live deployment (Sprint 6).")
    deploy_live.add_argument("--run_id", required=True)
    deploy_live.add_argument("--data_root", default=None)
    deploy_live.add_argument("--config", default=None, help="Path to live engine config YAML.")

    deploy_status = deploy_sub.add_parser("status", help="Show status of deployed theses.")
    deploy_status.add_argument("--data_root", default=None)

    ingest_parser = subparsers.add_parser("ingest", help="Ingest raw data from external exchanges")
    ingest_parser.add_argument("--run_id", required=True, help="Unique identifier for this run")
    ingest_parser.add_argument(
        "--symbols", required=True, help="Comma-separated list of symbols to ingest"
    )
    ingest_parser.add_argument("--start", required=True, help="Inclusive start date (YYYY-MM-DD)")
    ingest_parser.add_argument("--end", required=True, help="Inclusive end date (YYYY-MM-DD)")
    ingest_parser.add_argument(
        "--timeframe",
        default="1m",
        help="Timeframe to ingest, e.g. 1m, 5m. Only minute-based intervals are valid.",
    )
    ingest_parser.add_argument(
        "--exchange",
        default="binance",
        choices=["binance", "bybit"],
        help="Exchange to ingest from (binance or bybit)",
    )
    ingest_parser.add_argument(
        "--data_type",
        default="ohlcv",
        choices=["ohlcv", "funding", "oi", "mark_price", "index_price"],
        help="Type of data to ingest",
    )
    ingest_parser.add_argument(
        "--out_root",
        default=None,
        help="Output root directory for ingested Parquet files. Defaults to data root",
    )
    ingest_parser.add_argument(
        "--concurrency", type=int, default=5, help="Number of concurrent downloads"
    )
    ingest_parser.add_argument("--max_retries", type=int, default=5)
    ingest_parser.add_argument("--retry_backoff_sec", type=float, default=2.0)
    ingest_parser.add_argument("--force", type=int, default=0)
    ingest_parser.add_argument("--log_path", default=None)

    # 5. CATALOG (Sprint 7)
    catalog_parser = subparsers.add_parser("catalog", help="Research operations and artifact intelligence.")
    catalog_sub = catalog_parser.add_subparsers(dest="subcommand")

    catalog_list = catalog_sub.add_parser("list", help="List runs with artifact manifests.")
    catalog_list.add_argument("--stage", choices=["discover", "validate", "promote", "deploy"])
    catalog_list.add_argument("--data_root", default=None)

    catalog_compare = catalog_sub.add_parser("compare", help="Compare two runs at a specific stage.")
    catalog_compare.add_argument("--run_id_a", required=True)
    catalog_compare.add_argument("--run_id_b", required=True)
    catalog_compare.add_argument("--stage", required=True, choices=["discover", "validate", "promote"])
    catalog_compare.add_argument("--data_root", default=None)

    catalog_audit = catalog_sub.add_parser("audit-artifacts", help="Scan historical artifacts for audit inventory.")
    catalog_audit.add_argument("--run_id", default=None, help="Filter to specific run ID")
    catalog_audit.add_argument("--since", default=None, help="ISO timestamp to filter artifacts since")
    catalog_audit.add_argument("--data_root", default=None)
    catalog_audit.add_argument("--emit_inventory", type=int, default=1, help="Write inventory outputs (parquet/json/md)")
    catalog_audit.add_argument("--rewrite_stamps", type=int, default=0, help="Write sidecar audit stamps (non-destructive)")

    subparsers.required = True
    return parser


def _default_out_dir(proposal_path: str | Path, run_id: str | None) -> Path:
    proposal_name = Path(proposal_path).stem or "proposal"
    suffix = run_id or proposal_name
    return Path("data") / "artifacts" / "operator" / suffix


def main() -> int:
    parser = _build_parser()
    args, unknown = parser.parse_known_args()

    if unknown and args.command in {"discover", "validate", "promote", "deploy"}:
        parser.error(f"unrecognized arguments: {' '.join(unknown)}")

    # --- CANONICAL COMMAND DISPATCH ---

    if args.command == "discover":
        if args.subcommand in {"plan", "run"}:
            from project import discover
            result = discover.run(
                args.proposal,
                registry_root=Path(args.registry_root),
                data_root=Path(args.data_root) if args.data_root else None,
                run_id=getattr(args, "run_id", None),
                plan_only=(args.subcommand == "plan"),
                dry_run=False,
                check=bool(getattr(args, "check", False)),
            )
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0 if int(result.get("execution", {}).get("returncode", 0)) == 0 else int(result["execution"]["returncode"])

        if args.subcommand == "list-artifacts":
            from project.core.config import get_data_root
            data_root = Path(args.data_root) if args.data_root else get_data_root()
            paths = [data_root / "reports" / "phase2" / args.run_id]
            print(f"Artifacts for discovery run {args.run_id}:")
            found = False
            for p in paths:
                if p.exists():
                    found = True
                    for f in p.glob("*"):
                        print(f"  - {f.relative_to(data_root)}")
            if not found:
                print(f"No discovery artifacts found for run {args.run_id}")
            return 0

        if args.subcommand == "triggers":
            # Direct dispatch to trigger discovery orchestrator
            from project.research.discover_triggers import main as trigger_main
            # We must map the CLI args back to the orchestrator's expectations
            # discover_triggers.py expects --mode choices: [parameter_sweep, feature_cluster]
            
            if args.trigger_command == "emit-registry-payload":
                # Special helper for registry payload emission
                from project.research.trigger_discovery.proposal_emission import generate_suggested_registry_payload
                import pandas as pd
                parquet_path = Path(args.proposal_dir) / "candidate_trigger_scored.parquet"
                if not parquet_path.exists():
                    print(f"Error: Missing {parquet_path}")
                    return 1
                df = pd.read_parquet(parquet_path)
                match = df[df["candidate_trigger_id"] == args.candidate_id]
                if match.empty:
                    print(f"Error: Candidate {args.candidate_id} not found in {parquet_path}")
                    return 1
                payload = generate_suggested_registry_payload(match.iloc[0])
                import yaml
                print(yaml.dump(payload, sort_keys=False))
                return 0

            if args.trigger_command in ["list", "inspect", "review", "approve", "reject", "mark-adopted"]:
                from project.research.trigger_discovery import adoption_store
                out_dir = Path(args.proposal_dir)
                
                if args.trigger_command == "list":
                    proposals = adoption_store.list_proposals(out_dir)
                    if not proposals:
                        print("No proposals found.")
                        return 0
                    print(f"{'CANDIDATE ID':<45} | {'STATUS':<15} | {'LANE'}")
                    print("-" * 80)
                    for p in proposals:
                        print(f"{p['candidate_id']:<45} | {p['status']:<15} | {p['source_lane']}")
                    return 0
                    
                if args.trigger_command == "inspect":
                    p = adoption_store.get_proposal(args.candidate_id, out_dir)
                    if not p:
                        print(f"Proposal {args.candidate_id} not found.")
                        return 1
                    import yaml
                    print(yaml.dump(p, sort_keys=False))
                    return 0
                    
                status_map = {
                    "review": "under_review",
                    "approve": "approved",
                    "reject": "rejected",
                    "mark-adopted": "adopted"
                }
                new_status = status_map[args.trigger_command]
                reason = getattr(args, "reason", None)
                
                success = adoption_store.transition_state(
                    args.candidate_id, 
                    new_status, 
                    out_dir,
                    reason=reason
                )
                return 0 if success else 1

            # Normal discovery dispatch
            sys.argv = [sys.argv[0]]
            if args.trigger_command == "parameter-sweep":
                sys.argv.extend(["--mode", "parameter_sweep", "--family", args.family])
            elif args.trigger_command == "feature-cluster":
                sys.argv.extend(["--mode", "feature_cluster"])
            
            if getattr(args, "symbol", None): sys.argv.extend(["--symbol", args.symbol])
            if getattr(args, "timeframe", None): sys.argv.extend(["--timeframe", args.timeframe])
            if getattr(args, "data_root", None): sys.argv.extend(["--data_root", args.data_root])
            if getattr(args, "out_dir", None): sys.argv.extend(["--out_dir", args.out_dir])
            
            return int(trigger_main() or 0)

    if args.command == "validate":
        from project import validate
        if args.subcommand == "run":
            try:
                bundle = validate.run(args.run_id, data_root=Path(args.data_root) if args.data_root else None)
                print(f"Validation completed. Validated: {len(bundle.validated_candidates)}, Rejected: {len(bundle.rejected_candidates)}")
            except ValueError as exc:
                if "No candidates found" in str(exc):
                    print(f"Validation completed. No candidates found for run {args.run_id}. Validated: 0, Rejected: 0")
                else:
                    raise
            return 0
        if args.subcommand == "report":
            result = validate.report(
                run_id=args.run_id,
                data_root=Path(args.data_root) if args.data_root else None,
            )
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if args.subcommand == "diagnose":
            result = validate.diagnose(
                run_id=args.run_id,
                program_id=args.program_id,
                data_root=Path(args.data_root) if args.data_root else None,
            )
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if args.subcommand == "list-artifacts":
            from project.core.config import get_data_root
            data_root = Path(args.data_root) if args.data_root else get_data_root()
            val_dir = data_root / "reports" / "validation" / args.run_id
            if val_dir.exists():
                print(f"Artifacts for validation run {args.run_id}:")
                for f in val_dir.glob("*"):
                    print(f"  - {f.relative_to(data_root)}")
            else:
                print(f"No validation artifacts found for run {args.run_id}")
            return 0

    if args.command == "promote":
        from project import promote
        if args.subcommand == "run":
            result = promote.run(
                run_id=args.run_id,
                symbols=args.symbols,
                out_dir=Path(args.out_dir) if args.out_dir else None,
                retail_profile=args.retail_profile,
            )
            if result.exit_code != 0:
                err = str(result.diagnostics.get("error", ""))
                if "missing validation bundle" in err or "No candidates found" in err:
                    print(f"Promotion completed. No validated candidates for run {args.run_id}. Promoted: 0")
                    return 0
            print(f"Promotion completed with exit code: {result.exit_code}")
            return result.exit_code
        if args.subcommand == "export":
            result = promote.export(
                args.run_id,
                data_root=Path(args.data_root) if args.data_root else None,
                allow_bundle_only_export=True,
            )
            print(f"Exported {result.thesis_count} theses to {result.output_path}")
            return 0
        if args.subcommand == "list-artifacts":
            from project.core.config import get_data_root
            data_root = Path(args.data_root) if args.data_root else get_data_root()
            promo_dir = data_root / "reports" / "promotions" / args.run_id
            if promo_dir.exists():
                print(f"Artifacts for promotion run {args.run_id}:")
                for f in promo_dir.glob("*"):
                    print(f"  - {f.relative_to(data_root)}")
            else:
                print(f"No promotion artifacts found for run {args.run_id}")
            return 0

    if args.command == "deploy":
        from project.artifacts import promoted_theses_path
        from project.core.config import get_data_root
        data_root = Path(args.data_root) if args.data_root else get_data_root()

        if args.subcommand == "list-theses":
            theses_dir = data_root / "live" / "theses"
            if theses_dir.exists():
                print("Available Promoted Theses:")
                for d in theses_dir.iterdir():
                    if d.is_dir() and (d / "promoted_theses.json").exists():
                        print(f"  - {d.name}")
            else:
                print("No promoted theses found in inventory.")
            return 0

        if args.subcommand in {"inspect-thesis", "paper", "live"}:
            path = promoted_theses_path(args.run_id, data_root)
            if not path.exists():
                print(f"Error: No promoted thesis found for run {args.run_id}")
                print("Deploy stage requires a completed 'promote' stage.")
                return 1

            if args.subcommand == "inspect-thesis":
                from project.live.thesis_store import ThesisStore
                store = ThesisStore.from_path(path)
                print(f"Thesis Inspection: {args.run_id}")
                print(f"  - Thesis Count: {len(store.all())}")
                for t in store.all():
                    deployment_state = getattr(t, 'deployment_state', 'N/A')
                    print(f"  - [{t.thesis_id}] Status: {t.status}, Deployment State: {deployment_state}, Class: {t.promotion_class}")
                return 0

            if args.subcommand == "paper":
                print(f"Launching Paper Deployment for {args.run_id}...")
                from project.live.thesis_store import ThesisStore

                store = ThesisStore.from_path(path)
                if not any(getattr(t, 'deployment_state', None) in ("paper_only", "live_enabled") for t in store.all()):
                    print("  - Status: BLOCKED")
                    print("  - Reason: Batch does not contain any theses with deployment_state 'paper_only' or 'live_enabled'.")
                    return 1

                config_path = args.config
                if not config_path:
                    config_path = str(PROJECT_ROOT / "configs" / "live_paper.yaml")

                from project.scripts.run_live_engine import main as run_live_engine_main
                run_argv = ["--config", config_path]
                return run_live_engine_main(run_argv)

            if args.subcommand == "live":
                from project.live.thesis_store import ThesisStore
                store = ThesisStore.from_path(path)
                if not any(getattr(t, 'deployment_state', None) == "live_enabled" for t in store.all()):
                    print("  - Status: BLOCKED")
                    print("  - Reason: Batch does not contain any theses with deployment_state 'live_enabled'.")
                    return 1

                config_path = args.config
                if not config_path:
                    config_path = str(PROJECT_ROOT / "configs" / "live_production.yaml")

                from project.scripts.run_live_engine import main as run_live_engine_main
                run_argv = ["--config", config_path]
                return run_live_engine_main(run_argv)

        if args.subcommand == "status":
            print("Deployment Status: Monitoring active sessions via explicit catalog integration.")
            return 0

    if args.command == "ingest":
        if args.exchange == "binance":
            if args.data_type == "ohlcv":
                from project.pipelines.ingest import ingest_binance_um_ohlcv as _ingest_ohlcv
                ingest_script = "ingest_binance_um_ohlcv.py"
            else:
                # We could add more binance types here if needed, but for now focusing on bybit
                raise ValueError(f"Binance ingestion for {args.data_type} not yet integrated in this unified command")
        elif args.exchange == "bybit":
            if args.data_type in ["ohlcv", "mark_price", "index_price"]:
                from project.pipelines.ingest import ingest_bybit_derivatives_ohlcv as _ingest_ohlcv
                ingest_script = "ingest_bybit_derivatives_ohlcv.py"
            elif args.data_type == "funding":
                from project.pipelines.ingest import ingest_bybit_derivatives_funding as _ingest_ohlcv
                ingest_script = "ingest_bybit_derivatives_funding.py"
            elif args.data_type == "oi":
                from project.pipelines.ingest import ingest_bybit_derivatives_open_interest as _ingest_ohlcv
                ingest_script = "ingest_bybit_derivatives_open_interest.py"
            else:
                raise ValueError(f"Unsupported data_type: {args.data_type}")



        else:
            raise ValueError(f"Unsupported exchange: {args.exchange}")

        ingest_argv = [
            ingest_script,
            f"--run_id={args.run_id}",
            f"--symbols={args.symbols}",
            f"--start={args.start}",
            f"--end={args.end}",
            f"--timeframe={args.timeframe}",
            f"--data_type={args.data_type}",
            f"--concurrency={args.concurrency}",
            f"--max_retries={args.max_retries}",
            f"--retry_backoff_sec={args.retry_backoff_sec}",
            f"--force={args.force}",
        ]
        if args.out_root:
            ingest_argv.append(f"--out_root={args.out_root}")
        if args.log_path:
            ingest_argv.append(f"--log_path={args.log_path}")
        ingest_argv.extend(unknown)
        orig_argv = list(sys.argv)
        sys.argv = ingest_argv
        try:
            return _ingest_ohlcv.main()
        finally:
            sys.argv = orig_argv

    if args.command == "catalog":
        from project.research.services import run_catalog_service
        data_root = Path(args.data_root) if args.data_root else None
        if args.subcommand == "list":
            runs = run_catalog_service.list_runs(stage=args.stage, data_root=data_root)
            print(f"{'Run ID':<40} | {'Stage':<10} | {'Created At':<25}")
            print("-" * 80)
            for r in runs:
                print(f"{r['run_id']:<40} | {r['stage']:<10} | {r['created_at']:<25}")
            return 0
        if args.subcommand == "compare":
            diff = run_catalog_service.compare_manifests(
                args.run_id_a, args.run_id_b, stage=args.stage, data_root=data_root
            )
            print(json.dumps(diff, indent=2, sort_keys=True))
            return 0
        if args.subcommand == "audit-artifacts":
            from project.research.audit_historical_artifacts import (
                scan_historical_artifacts,
                write_audit_inventory,
                rewrite_audit_stamp_sidecars,
            )
            result = scan_historical_artifacts(
                data_root=data_root,
                run_id=args.run_id,
                since=args.since,
            )
            print(f"Scanned {len(result.scanned_artifact_paths)} artifacts")
            print(f"Total rows: {len(result.rows)}")
            print(f"Historical trust: {dict(result.trust_status_counts)}")
            print(f"Stat regimes: {dict(result.stat_regime_counts)}")
            print(f"Audit statuses: {dict(result.audit_status_counts)}")
            print(f"Requires repromotion: {result.requires_repromotion_count}")
            print(f"Requires manual review: {result.requires_manual_review_count}")
            print(f"Canonical reuse blocked: {result.canonical_reuse_blocked_count}")
            print(f"Compat reuse blocked: {result.compat_reuse_blocked_count}")
            if bool(args.emit_inventory):
                output_dir = (data_root or Path("data")) / "reports" / "audit"
                paths = write_audit_inventory(result, output_dir)
                print(f"Inventory written:")
                for name, path in paths.items():
                    print(f"  - {name}: {path}")
            if bool(args.rewrite_stamps):
                rewrite_result = rewrite_audit_stamp_sidecars(result)
                print(f"Audit sidecars written: {rewrite_result['sidecars_written']}")
                print(f"Artifacts processed: {rewrite_result['artifacts_processed']}")
            if result.errors:
                print(f"Errors: {len(result.errors)}")
                for err in result.errors[:5]:
                    print(f"  - {err}")
            return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
