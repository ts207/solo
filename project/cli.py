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
DEFAULT_CONFIG_OUTPUT_DIR = PROJECT_ROOT / "configs"


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


def _run_data_preflight(args: argparse.Namespace) -> int:
    from project.research.data_preflight import build_data_preflight_report

    payload = build_data_preflight_report(
        proposal_path=args.proposal,
        data_root=_path_or_none(args.data_root),
        strict_columns=bool(getattr(args, "strict_columns", False)),
    )
    _emit_json(payload)
    return 0 if payload.get("status") == "pass" else 1


def _run_run_status(args: argparse.Namespace) -> int:
    from project.research.run_diagnostics import build_run_status_report

    _emit_json(
        build_run_status_report(
            run_id=args.run_id,
            data_root=_path_or_none(args.data_root),
            top_k=int(args.top_k),
        )
    )
    return 0


def _run_explain_rejection(args: argparse.Namespace) -> int:
    from project.research.run_diagnostics import build_rejection_explanation

    _emit_json(
        build_rejection_explanation(
            run_id=args.run_id,
            data_root=_path_or_none(args.data_root),
            top_k=int(args.top_k),
        )
    )
    return 0


def _run_run_context_audit(args: argparse.Namespace) -> int:
    from project.research.context_audit import build_context_audit_report

    _emit_json(
        build_context_audit_report(
            run_id=args.run_id,
            data_root=_path_or_none(args.data_root),
            write=bool(getattr(args, "write", False)),
        )
    )
    return 0


def _run_hypothesis_check(args: argparse.Namespace) -> int:
    from project.research.predeclared import validate_predeclared_hypotheses

    payload = validate_predeclared_hypotheses(args.registry)
    _emit_json(payload)
    return 0 if payload.get("status") == "pass" else 1



def _run_run_id_new(args: argparse.Namespace) -> int:
    from project.core.run_id import new_run_id

    print(new_run_id(prefix=args.prefix))
    return 0


def _run_repo_lock_check(args: argparse.Namespace) -> int:
    from project.core.dependency_lock import build_dependency_lock_report

    payload = build_dependency_lock_report(project_root=Path(args.project_root))
    _emit_json(payload)
    return 0 if payload.get("status") == "pass" else 1


def _run_hypothesis_proposal_check(args: argparse.Namespace) -> int:
    from project.research.predeclared import check_proposal_against_registry

    payload = check_proposal_against_registry(
        registry_path=args.registry,
        proposal_path=args.proposal,
        hypothesis_id=getattr(args, "hypothesis_id", None),
    )
    _emit_json(payload)
    return 0 if payload.get("status") == "pass" else 1


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _split_int_csv(value: str | None) -> list[int]:
    out: list[int] = []
    for item in _split_csv(value):
        out.append(int(item))
    return out


def _run_campaign_multiplicity_append(args: argparse.Namespace) -> int:
    from project.research.multiplicity_ledger import append_multiplicity_record, build_multiplicity_record

    record = build_multiplicity_record(
        campaign_id=args.campaign_id,
        run_id=args.run_id,
        proposal_path=args.proposal,
        symbols=_split_csv(args.symbols),
        horizons=_split_int_csv(args.horizons),
        directions=_split_csv(args.directions),
        filters=_split_csv(args.filters),
        templates=_split_csv(args.templates),
    )
    payload = append_multiplicity_record(args.ledger, record)
    _emit_json(payload)
    return 0


def _run_campaign_multiplicity_report(args: argparse.Namespace) -> int:
    from project.research.multiplicity_ledger import build_multiplicity_report

    payload = build_multiplicity_report(args.ledger, campaign_id=getattr(args, "campaign_id", None))
    _emit_json(payload)
    return 0 if payload.get("status") != "missing" else 1


def _run_discover(args: argparse.Namespace) -> int:
    from project import discover
    from project.core.run_guard import assert_run_id_available

    kwargs: dict[str, Any] = {
        "registry_root": Path(args.registry_root),
        "data_root": _path_or_none(args.data_root),
        "run_id": args.run_id,
        "plan_only": args.discover_action == "plan",
        "dry_run": args.dry_run,
        "check": args.check,
    }
    promotion_profile = str(getattr(args, "promotion_profile", "") or "").strip().lower()
    if promotion_profile:
        kwargs["promotion_profile"] = promotion_profile

    if args.discover_action == "run":
        assert_run_id_available(
            run_id=args.run_id,
            data_root=args.data_root,
            stages=["discovery"],
            overwrite=bool(getattr(args, "overwrite", False)),
        )

    result = discover.run(args.proposal, **kwargs)
    if isinstance(result, dict):
        _emit_json(result)
    return _result_exit_code(result)


def _run_discover_list_artifacts(args: argparse.Namespace) -> int:
    from project.artifacts.discovery import discover_run_artifacts

    payload = discover_run_artifacts(run_id=args.run_id, data_root=_path_or_none(args.data_root))
    artifact_paths = payload.get("artifact_paths", [])
    if not isinstance(artifact_paths, list) or not artifact_paths:
        print(f"No discovery artifacts found for run {args.run_id}")
        return 0

    print(f"Artifacts for discovery run {args.run_id}:")
    for path in artifact_paths:
        print(str(path))
    return 0


def _run_discover_cells(args: argparse.Namespace) -> int:
    from project.core.run_guard import assert_run_id_available
    from project.research.cell_discovery.cells_cli import run_from_namespace

    if str(getattr(args, "cells_action", "") or "") == "run":
        assert_run_id_available(
            run_id=getattr(args, "run_id", None),
            data_root=getattr(args, "data_root", None),
            stages=["discovery"],
            overwrite=bool(getattr(args, "overwrite", False)),
        )

    result = run_from_namespace(args)
    _emit_json(result)
    return int(result.get("exit_code", 0))


def _run_discover_summarize(args: argparse.Namespace) -> int:
    from project.discover.reporting import build_discover_summary, format_discover_summary_text

    payload = build_discover_summary(
        run_id=args.run_id,
        data_root=_path_or_none(args.data_root),
        top_k=int(args.top_k),
    )
    sys.stdout.write(format_discover_summary_text(payload))
    return 0


def _run_discover_explain_empty(args: argparse.Namespace) -> int:
    from project.discover.reporting import explain_empty_discovery, format_explain_empty_text

    payload = explain_empty_discovery(run_id=args.run_id, data_root=_path_or_none(args.data_root))
    sys.stdout.write(format_explain_empty_text(payload))
    classification = str(payload.get("classification", "") or "")
    if classification in {"hypotheses_rejected_pre_metrics", "zero_feasible_hypotheses"}:
        return 1
    feasibility = payload.get("feasibility_summary", {})
    if isinstance(feasibility, dict) and int(feasibility.get("feasible", 1) or 0) <= 0:
        return 1
    return 0


def _run_discover_funnel(args: argparse.Namespace) -> int:
    data_root = _path_or_none(args.data_root) or PROJECT_ROOT.parent / "data"
    path = data_root / "reports" / "phase2" / args.run_id / "funnel.json"
    if not path.exists():
        _emit_json({"status": "missing", "run_id": args.run_id, "path": str(path)})
        return 1
    _emit_json(json.loads(path.read_text(encoding="utf-8")))
    return 0


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
    result = subprocess.run(cmd)  # noqa: S603 - command is assembled from fixed CLI entrypoint.
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


def _run_proposal_inspect(args: argparse.Namespace) -> int:
    from project.research.agent_io.proposal_schema import load_normalized_operator_proposal
    from project.research.knowledge.memory import memory_paths

    proposal_path = Path(args.proposal)
    payload = load_normalized_operator_proposal(proposal_path).to_dict()

    program_id = str(payload.get("program_id", "") or "").strip()
    symbols = payload.get("symbols", [])
    timeframe = str(payload.get("timeframe", "") or "")
    start = str(payload.get("start", "") or "")
    end = str(payload.get("end", "") or "")
    promotion_profile = str(
        payload.get("promotion_profile", "") or payload.get("promotion_mode", "") or ""
    )
    search_spec = payload.get("search_spec")
    hypothesis = (
        payload.get("hypothesis", {}) if isinstance(payload.get("hypothesis"), dict) else {}
    )
    anchor = hypothesis.get("anchor", {}) if isinstance(hypothesis.get("anchor"), dict) else {}
    template = (
        hypothesis.get("template", {}) if isinstance(hypothesis.get("template"), dict) else {}
    )

    resolved_root = _path_or_none(args.data_root) or (PROJECT_ROOT.parent / "data")
    phase2_dir = resolved_root / "reports" / "phase2" / str(args.run_id or "<run_id>")
    proposal_memory_dir = (
        memory_paths(program_id, data_root=resolved_root).proposals_dir
        / str(args.run_id or "<run_id>")
        if program_id
        else None
    )

    lines: list[str] = []
    lines.append(f"Proposal: {proposal_path}")
    if program_id:
        lines.append(f"program_id: {program_id}")
    lines.append(f"symbols: {symbols}")
    lines.append(f"timeframe: {timeframe}")
    lines.append(f"date_range: {start} -> {end}")
    if promotion_profile:
        lines.append(f"promotion_profile: {promotion_profile}")
    if (isinstance(search_spec, str) and search_spec) or isinstance(search_spec, dict):
        lines.append(f"search_spec: {search_spec}")
    lines.append(f"anchor: {anchor}")
    lines.append(f"template: {template}")
    if "direction" in hypothesis:
        lines.append(f"direction: {hypothesis.get('direction')}")
    if "horizon_bars" in hypothesis:
        lines.append(f"horizon_bars: {hypothesis.get('horizon_bars')}")

    lines.append("expected_outputs:")
    lines.append(f"  - phase2_dir: {phase2_dir}")
    if proposal_memory_dir is not None:
        lines.append(f"  - proposal_memory_dir: {proposal_memory_dir}")

    sys.stdout.write("\n".join(lines) + "\n")
    return 0


def _run_validate(args: argparse.Namespace) -> int:
    from project import run as run_validation
    from project.core.run_guard import assert_run_id_available

    assert_run_id_available(
        run_id=args.run_id,
        data_root=args.data_root,
        stages=["validation"],
        overwrite=bool(getattr(args, "overwrite", False)),
    )

    result = run_validation(args.run_id, data_root=_path_or_none(args.data_root))
    _emit_json(result)
    return _result_exit_code(result)


def _run_validate_specs(args: argparse.Namespace) -> int:
    from project.spec_validation.cli import run_all_validations

    return int(run_all_validations(root=Path(args.root), verbose=args.verbose))


def _run_validate_forward_confirm(args: argparse.Namespace) -> int:
    from project.validate.forward_confirm import forward_confirm

    try:
        payload = forward_confirm(
            run_id=args.run_id,
            window=args.window,
            data_root=_path_or_none(args.data_root),
            proposal_path=_path_or_none(args.proposal),
            candidate_id=args.candidate_id,
        )
        _emit_json(payload)
        return 0
    except (RuntimeError, ValueError, FileNotFoundError) as exc:
        _emit_json({"status": "error", "message": str(exc)})
        return 1


def _run_promote(args: argparse.Namespace) -> int:
    from project import promote
    from project.core.run_guard import assert_run_id_available

    assert_run_id_available(
        run_id=args.run_id,
        data_root=None,
        stages=["promotion", "thesis"],
        overwrite=bool(getattr(args, "overwrite", False)),
    )

    result = promote.run(
        run_id=args.run_id,
        symbols=args.symbols,
        out_dir=_path_or_none(args.out_dir),
        retail_profile=args.retail_profile,
        promotion_profile=getattr(args, "promotion_profile", "auto"),
        require_forward_confirmation=getattr(args, "require_forward_confirmation", None),
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
    data_root = _path_or_none(args.data_root) or PROJECT_ROOT.parent / "data"
    thesis_path = _thesis_path_for_run(data_root=data_root, run_id=args.run_id)
    if not thesis_path.exists():
        phase2_dir = data_root / "reports" / "phase2" / args.run_id
        if phase2_dir.exists():
            _emit_json(
                {"status": "error", "message": "Deploy stage requires a completed 'promote' stage"}
            )
        else:
            _emit_json(
                {
                    "status": "error",
                    "message": f"Error: No promoted thesis found for run {args.run_id}",
                }
            )
        return 1
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
    from project.live.deploy_admission import assert_deploy_admission

    data_root = _path_or_none(args.data_root) or PROJECT_ROOT.parent / "data"
    thesis_path_override = _path_or_none(args.thesis_path)
    thesis_path = thesis_path_override or _thesis_path_for_run(
        data_root=data_root,
        run_id=args.run_id,
    )
    if not thesis_path.exists():
        raise FileNotFoundError(f"thesis artifact not found: {thesis_path}")

    monitor_report_path = _path_or_none(getattr(args, "monitor_report", None))
    runtime_mode = str(args.runtime_mode).strip().lower() or "monitor_only"

    # Assert admission
    try:
        assert_deploy_admission(
            thesis_path=thesis_path,
            runtime_mode=runtime_mode,
            monitor_report_path=monitor_report_path,
            data_root=data_root,
        )
    except (PermissionError, RuntimeError, ValueError) as e:
        _emit_json({"status": "error", "message": str(e)})
        return 1

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    runtime_mode = str(args.runtime_mode).strip().lower() or "monitor_only"
    mode_label = str(args.profile or "").strip().lower() or (
        "paper"
        if runtime_mode == "simulation"
        else "trading"
        if runtime_mode == "trading"
        else "monitor"
    )
    filename = (
        f"live_{mode_label}_{args.run_id}.yaml"
        if args.config_template == "run_id"
        else f"live_{mode_label}.yaml"
    )
    out_path = out_dir / filename

    strategy_runtime = {
        "implemented": True,
        "event_detector": {
            "adapter": "governed_runtime_core",
            "legacy_heuristic_enabled": False,
        },
    }
    if thesis_path_override is not None:
        strategy_runtime["thesis_path"] = str(thesis_path)
    else:
        strategy_runtime["thesis_run_id"] = args.run_id

    payload = {
        "runtime_mode": runtime_mode,
        "strategy_runtime": strategy_runtime,
        "freshness_streams": [
            {"symbol": symbol.strip().lower(), "stream": "kline_5m"}
            for symbol in args.symbols.split(",")
            if symbol.strip()
        ],
    }
    out_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    _emit_json(
        {
            "config_path": str(out_path),
            "thesis_path": str(thesis_path),
            "thesis_source": "thesis_path" if thesis_path_override is not None else "thesis_run_id",
        }
    )
    return 0


def _run_live_engine_entry(config: Path) -> int:
    script = PROJECT_ROOT / "scripts" / "run_live_engine.py"
    result = subprocess.run(  # noqa: S603 - invokes repo-owned live engine with explicit argv.
        [sys.executable, str(script), "--config", str(config)]
    )
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
    parser = argparse.ArgumentParser(prog="edge")
    sub = parser.add_subparsers(dest="command")

    data = sub.add_parser("data", help="data coverage and preflight utilities")
    data_sub = data.add_subparsers(dest="data_action")
    preflight = data_sub.add_parser("preflight")
    preflight.add_argument("--proposal", required=True)
    preflight.add_argument("--data_root")
    preflight.add_argument("--strict_columns", action="store_true")
    preflight.set_defaults(func=_run_data_preflight)


    run_id = sub.add_parser("run-id", help="run identifier utilities")
    run_id_sub = run_id.add_subparsers(dest="run_id_action")
    run_id_new = run_id_sub.add_parser("new")
    run_id_new.add_argument("--prefix", required=True)
    run_id_new.set_defaults(func=_run_run_id_new)

    repo = sub.add_parser("repo", help="repository hygiene utilities")
    repo_sub = repo.add_subparsers(dest="repo_action")
    lock_check = repo_sub.add_parser("lock-check")
    lock_check.add_argument("--project_root", default=".")
    lock_check.set_defaults(func=_run_repo_lock_check)

    run_cmd = sub.add_parser("run", help="run diagnostics and status")
    run_sub = run_cmd.add_subparsers(dest="run_action")
    run_status = run_sub.add_parser("status")
    run_status.add_argument("--run_id", required=True)
    run_status.add_argument("--data_root")
    run_status.add_argument("--top_k", type=int, default=10)
    run_status.set_defaults(func=_run_run_status)
    context_audit = run_sub.add_parser("context-audit")
    context_audit.add_argument("--run_id", required=True)
    context_audit.add_argument("--data_root")
    context_audit.add_argument("--write", action="store_true")
    context_audit.set_defaults(func=_run_run_context_audit)

    hypothesis = sub.add_parser("hypothesis", help="predeclared hypothesis registry utilities")
    hypothesis_sub = hypothesis.add_subparsers(dest="hypothesis_action")
    hypothesis_check = hypothesis_sub.add_parser("check")
    hypothesis_check.add_argument("--registry", default="research/predeclared_hypotheses.yaml")
    hypothesis_check.set_defaults(func=_run_hypothesis_check)

    proposal_check = hypothesis_sub.add_parser("proposal-check")
    proposal_check.add_argument("--registry", default="research/predeclared_hypotheses.yaml")
    proposal_check.add_argument("--proposal", required=True)
    proposal_check.add_argument("--hypothesis_id")
    proposal_check.set_defaults(func=_run_hypothesis_proposal_check)


    campaign = sub.add_parser("campaign", help="campaign-level research utilities")
    campaign_sub = campaign.add_subparsers(dest="campaign_action")
    multiplicity = campaign_sub.add_parser("multiplicity")
    multiplicity_sub = multiplicity.add_subparsers(dest="multiplicity_action")
    multiplicity_append = multiplicity_sub.add_parser("append")
    multiplicity_append.add_argument("--ledger", default="research/multiplicity_ledger.jsonl")
    multiplicity_append.add_argument("--campaign_id", required=True)
    multiplicity_append.add_argument("--run_id", required=True)
    multiplicity_append.add_argument("--proposal", required=True)
    multiplicity_append.add_argument("--symbols", default="")
    multiplicity_append.add_argument("--horizons", default="")
    multiplicity_append.add_argument("--directions", default="")
    multiplicity_append.add_argument("--filters", default="")
    multiplicity_append.add_argument("--templates", default="")
    multiplicity_append.set_defaults(func=_run_campaign_multiplicity_append)
    multiplicity_report = multiplicity_sub.add_parser("report")
    multiplicity_report.add_argument("--ledger", default="research/multiplicity_ledger.jsonl")
    multiplicity_report.add_argument("--campaign_id")
    multiplicity_report.set_defaults(func=_run_campaign_multiplicity_report)

    explain = sub.add_parser("explain", help="explain run outcomes and failures")
    explain_sub = explain.add_subparsers(dest="explain_action")
    rejection = explain_sub.add_parser("rejection")
    rejection.add_argument("--run_id", required=True)
    rejection.add_argument("--data_root")
    rejection.add_argument("--top_k", type=int, default=10)
    rejection.set_defaults(func=_run_explain_rejection)

    discover = sub.add_parser("discover", help="canonical discovery stage")
    discover_sub = discover.add_subparsers(dest="discover_action")
    for action in ("plan", "run"):
        stage = discover_sub.add_parser(action)
        stage.add_argument("--proposal", required=True)
        stage.add_argument("--registry_root", default=str(DEFAULT_REGISTRY_ROOT))
        stage.add_argument("--data_root")
        stage.add_argument("--run_id")
        stage.add_argument(
            "--promotion_profile",
            default="",
            help="Optional override for proposal promotion_profile (research|deploy|disabled).",
        )
        stage.add_argument("--dry_run", action="store_true")
        stage.add_argument("--check", action="store_true")
        stage.add_argument("--overwrite", action="store_true", help="Allow replacing existing artifacts for this run_id.")
        stage.set_defaults(func=_run_discover, discover_action=action)
    list_artifacts = discover_sub.add_parser("list-artifacts")
    list_artifacts.add_argument("--run_id", required=True)
    list_artifacts.add_argument("--data_root")
    list_artifacts.set_defaults(func=_run_discover_list_artifacts)
    summarize = discover_sub.add_parser("summarize")
    summarize.add_argument("--run_id", required=True)
    summarize.add_argument("--data_root")
    summarize.add_argument("--top_k", type=int, default=10)
    summarize.set_defaults(func=_run_discover_summarize)
    explain_empty = discover_sub.add_parser("explain-empty")
    explain_empty.add_argument("--run_id", required=True)
    explain_empty.add_argument("--data_root")
    explain_empty.set_defaults(func=_run_discover_explain_empty)
    funnel = discover_sub.add_parser("funnel")
    funnel.add_argument("--run_id", required=True)
    funnel.add_argument("--data_root")
    funnel.set_defaults(func=_run_discover_funnel)

    cells = discover_sub.add_parser("cells", help="cell-first discovery lane")
    cells_sub = cells.add_subparsers(dest="cells_action")
    coverage_audit = cells_sub.add_parser("coverage-audit")
    coverage_audit.add_argument("--spec_root", default="spec/discovery")
    coverage_audit.add_argument("--search_spec", default="spec/search_space.yaml")
    coverage_audit.add_argument(
        "--event_registry",
        default="spec/events/event_registry_unified.yaml",
    )
    coverage_audit.set_defaults(func=_run_discover_cells, cells_action="coverage-audit")
    spec_audit = cells_sub.add_parser("spec-audit")
    spec_audit.add_argument("--spec_dir", required=True)
    spec_audit.add_argument(
        "--template_registry",
        default="spec/templates/event_template_registry.yaml",
    )
    spec_audit.add_argument("--verify_report")
    spec_audit.set_defaults(func=_run_discover_cells, cells_action="spec-audit")
    for action in ("verify-data", "plan", "run"):
        cell_parser = cells_sub.add_parser(action)
        _add_cell_common_args(cell_parser)
        cell_parser.add_argument("--overwrite", action="store_true", help="Allow replacing existing artifacts for this run_id.")
        cell_parser.set_defaults(func=_run_discover_cells, cells_action=action)
    summarize = cells_sub.add_parser("summarize")
    summarize.add_argument("--run_id", required=True)
    summarize.add_argument("--data_root")
    summarize.set_defaults(func=_run_discover_cells, cells_action="summarize")
    assemble = cells_sub.add_parser("assemble-theses")
    assemble.add_argument("--run_id", required=True)
    assemble.add_argument("--data_root")
    assemble.add_argument("--limit", type=int, default=20)
    assemble.add_argument(
        "--per-cell",
        action="store_true",
        help=(
            "Assemble from rankable scoreboard rows instead of only redundancy-cluster "
            "representatives."
        ),
    )
    assemble.set_defaults(func=_run_discover_cells, cells_action="assemble-theses")

    triggers = discover_sub.add_parser(
        "triggers",
        help="advanced/internal research trigger discovery lanes; adapter-only, not canonical discovery",
        description=(
            "Advanced/Internal trigger discovery lanes for proposal-generating research. "
            "These subcommands operate in an internal research lane. "
            "No runtime effect — outputs are candidate proposals only. "
            "Manual review required before promoting any trigger discovery output."
        ),
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
    validate_run.add_argument("--overwrite", action="store_true", help="Allow replacing existing validation artifacts for this run_id.")
    validate_run.set_defaults(func=_run_validate)
    validate_specs = validate_sub.add_parser("specs")
    validate_specs.add_argument("--root", default=".")
    validate_specs.add_argument("--verbose", action="store_true")
    validate_specs.set_defaults(func=_run_validate_specs)
    forward_confirm = validate_sub.add_parser("forward-confirm")
    forward_confirm.add_argument("--run_id", required=True)
    forward_confirm.add_argument("--window", required=True)
    forward_confirm.add_argument("--data_root")
    forward_confirm.add_argument("--proposal")
    forward_confirm.add_argument("--candidate_id")
    forward_confirm.set_defaults(func=_run_validate_forward_confirm)

    promote = sub.add_parser("promote", help="canonical promotion/export stage")
    promote_sub = promote.add_subparsers(dest="promote_action")
    promote_run = promote_sub.add_parser("run")
    promote_run.add_argument("--run_id", required=True)
    promote_run.add_argument("--symbols", required=True)
    promote_run.add_argument("--out_dir")
    promote_run.add_argument("--retail_profile", default="capital_constrained")
    promote_run.add_argument(
        "--promotion_profile", choices=["auto", "research", "deploy"], default="auto"
    )
    promote_run.add_argument("--require_forward_confirmation", type=int, default=None)
    promote_run.add_argument("--overwrite", action="store_true", help="Allow replacing existing promotion/thesis artifacts for this run_id.")
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
    bind_config.add_argument("--monitor_report")
    bind_config.add_argument("--out_dir", default=str(DEFAULT_CONFIG_OUTPUT_DIR))
    bind_config.add_argument("--runtime_mode", default="monitor_only")
    bind_config.add_argument("--profile", choices=["paper", "monitor", "trading", "production"], default=None)
    bind_config.add_argument(
        "--config_template",
        choices=["run_id", "profile"],
        default="run_id",
    )
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

    proposal = sub.add_parser(
        "proposal", help="inspect and sanity-check a proposal before running it"
    )
    proposal_sub = proposal.add_subparsers(dest="proposal_action")
    inspect = proposal_sub.add_parser("inspect")
    inspect.add_argument("--proposal", required=True)
    inspect.add_argument("--run_id", default="")
    inspect.add_argument("--data_root")
    inspect.set_defaults(func=_run_proposal_inspect)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 0
    try:
        return int(args.func(args))
    except FileExistsError as exc:
        _emit_json({"status": "error", "message": str(exc)})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
