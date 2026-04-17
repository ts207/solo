"""Campaign controller for autonomous research sequencing."""

import argparse
import hashlib
import json
import logging
import re
import subprocess
import sys
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set

import pandas as pd
import yaml

from project.core.config import get_data_root
from project.io.utils import read_parquet
from project.domain.compiled_registry import get_domain_registry
from project.research import campaign_controller_scan_support as _scan_support
from project.research.experiment_engine import build_experiment_plan, RegistryBundle
from project.research.campaign_contract import controller_contract_view
from project.research.knowledge.schemas import canonical_json
from project.research.update_campaign_memory import _scope_already_tested
from project.research.search_intelligence import update_search_intelligence
from project.research.knowledge.memory import memory_paths, read_memory_table, write_memory_table
from project.research.reports.operator_reporting import write_operator_outputs_for_run
from project.spec_registry.search_space import (
    load_event_priority_weights,
    QUALITY_SCORES as _QUALITY_SCORES_MAP,
    DEFAULT_EVENT_PRIORITY_WEIGHT as _DEFAULT_QUALITY,
)

_LOG = logging.getLogger(__name__)

_QUALITY_SCORES: Dict[str, float] = _QUALITY_SCORES_MAP
_CANONICAL_REPAIR_STAGE_ALIASES: Dict[str, str] = {
    "phase2_candidate_discovery": "phase2_search_engine",
    "phase2_conditional_hypotheses": "phase2_search_engine",
    "bridge_evaluate_phase2": "phase2_search_engine",
}

_REPAIR_STAGE_DEFAULT_EVENTS: Dict[str, str] = {
    "build_event_registry": "VOL_SHOCK",
    "analyze_events": "VOL_SHOCK",
    "phase2_search_engine": "VOL_SHOCK",
}


class CampaignMemoryIntegrityError(RuntimeError):
    """Raised when persisted campaign memory exists but cannot be trusted."""


def _merge_proposal_rows(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return incoming.copy()
    if incoming.empty:
        return existing.copy()
    out = pd.concat([existing, incoming], ignore_index=True)
    return out.drop_duplicates(subset=["proposal_id"], keep="last").reset_index(drop=True)


def _load_event_quality_weights(search_space_path: Path) -> Dict[str, float]:
    """Backward-compatible shim for quality weight loading."""
    return load_event_priority_weights(search_space_path)


@dataclass
class CampaignConfig:
    program_id: str
    max_runs: int = 50
    max_hypotheses_total: int = 5000
    max_consecutive_no_signal: int = 2
    halt_on_empty_share: float = 0.8
    halt_on_unsupported_share: float = 0.5
    # Phase 2.1/2.3: research mode controls proposal strategy
    # "scan"    — quality-weighted frontier (default)
    # "exploit" — only propose from promising_regions in belief_state
    # "explore" — cross-family batches from explore_adjacent queue
    research_mode: Literal["scan", "exploit", "explore"] = "scan"
    # Phase 3.1: trigger types to activate in sequence.
    # ["EVENT"] → after event frontier exhausted → adds STATE → then TRANSITION.
    # ["EVENT", "STATE", "TRANSITION", "FEATURE_PREDICATE"] activates all four on init.
    scan_trigger_types: List[str] = None  # type: ignore[assignment]
    # Phase 3.2: enable vol_regime context conditioning on proposals.
    # False  — unconditional (default, safe for initial event scan).
    # True   — adds vol_regime: [low, high] to every Step 4 proposal, tripling
    #           the regime-conditional hypothesis count per run.
    enable_context_conditioning: bool = True
    proposal_context_dimensions: List[str] = None  # type: ignore[assignment]

    # Phase 4.4: optional live portfolio snapshot consumed by downstream
    # blueprint/allocation compilation for portfolio-aware sizing.
    portfolio_state_path: str | None = None
    # Phase 4.1: automatically run feature_mi_scan before the first proposal
    # cycle so the controller always has fresh MI-derived predicate candidates.
    # Set to False to skip (e.g. when features are unavailable or for speed).
    auto_run_mi_scan: bool = True
    # Symbols and timeframe used for the auto MI scan — must match the feature
    # table available for this program.
    mi_scan_symbols: str = "BTCUSDT"
    mi_scan_timeframe: str = "5m"
    repair_date_scope: tuple[str, str] = ("2024-01-01", "2024-01-07")
    exploit_date_scope: tuple[str, str] = ("2023-10-01", "2024-03-31")
    explore_date_scope: tuple[str, str] = ("2024-01-01", "2024-06-30")
    scan_event_date_scope: tuple[str, str] = ("2024-01-01", "2024-01-31")
    scan_general_date_scope: tuple[str, str] = ("2024-01-01", "2024-03-31")
    strict_memory_integrity: bool = True

    def __post_init__(self) -> None:
        # Default to the full trigger sequence
        if self.scan_trigger_types is None:
            self.scan_trigger_types = [
                "EVENT",
                "STATE",
                "TRANSITION",
                "FEATURE_PREDICATE",
                "SEQUENCE",
                "INTERACTION",
            ]
        if self.proposal_context_dimensions is None:
            self.proposal_context_dimensions = ["vol_regime", "carry_state"]
        else:
            self.proposal_context_dimensions = [
                str(dim).strip() for dim in self.proposal_context_dimensions if str(dim).strip()
            ]


@dataclass
class CampaignSummary:
    program_id: str
    total_runs: int = 0
    total_generated: int = 0
    total_evaluated: int = 0
    total_empty_sample: int = 0
    total_insufficient_sample: int = 0
    total_unsupported: int = 0
    total_skipped: int = 0
    top_hypotheses: List[Dict[str, Any]] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(self.__dict__, indent=2)


def _event_from_failure_detail(failure_detail: Any, enabled_events: List[str]) -> str:
    detail = str(failure_detail or "").upper()
    if not detail:
        return ""
    for event_id in sorted(enabled_events, key=len, reverse=True):
        pattern = rf"(?<![A-Z0-9_]){re.escape(event_id.upper())}(?![A-Z0-9_])"
        if re.search(pattern, detail):
            return event_id
    return ""


def _normalize_repair_stage_name(stage: str) -> str:
    raw = str(stage or "").strip().lower()
    if not raw:
        return ""
    base = raw.split("__", 1)[0]
    return _CANONICAL_REPAIR_STAGE_ALIASES.get(base, base)


def _preferred_default_repair_event(enabled_events: List[str]) -> str:
    if "VOL_SHOCK" in enabled_events:
        return "VOL_SHOCK"
    return sorted(enabled_events)[0] if enabled_events else ""


def _repair_event_for_stage(stage: str, enabled_events: List[str]) -> str:
    candidate = _REPAIR_STAGE_DEFAULT_EVENTS.get(_normalize_repair_stage_name(stage), "")
    if candidate and candidate in enabled_events:
        return candidate
    return ""


# ---------------------------------------------------------------------------
# Controller
# ---------------------------------------------------------------------------


class CampaignController:
    def __init__(self, config: CampaignConfig, data_root: Path, registry_root: Path):
        self.config = config
        self.data_root = data_root
        self.registry_root = registry_root
        self.campaign_dir = data_root / "artifacts" / "experiments" / config.program_id
        self.campaign_dir.mkdir(parents=True, exist_ok=True)
        self.ledger_path = self.campaign_dir / "tested_ledger.parquet"
        self.summary_path = self.campaign_dir / "campaign_summary.json"
        self.frontier_path = self.campaign_dir / "search_frontier.json"
        self.contract_path = self.campaign_dir / "campaign_contract.json"
        self.registries = RegistryBundle(registry_root)
        self._write_campaign_contract_artifact()

        # Phase 2.2: quality weights now loaded via the centralised
        # spec_registry.search_space loader, which also captures raw IG values
        # from comment annotations as fractional tiebreakers.
        _candidates = [
            Path("spec/search_space.yaml"),
            Path(__file__).parent.parent.parent.parent / "spec" / "search_space.yaml",
        ]
        self._search_space_path: Path = next(
            (p for p in _candidates if p.exists()), Path("spec/search_space.yaml")
        )
        self._quality_weights: Dict[str, float] = load_event_priority_weights(
            self._search_space_path
        )

    def _write_campaign_contract_artifact(self) -> None:
        payload = controller_contract_view(
            program_id=self.config.program_id,
            registry_root=str(self.registry_root),
            max_runs=self.config.max_runs,
            max_consecutive_no_signal=self.config.max_consecutive_no_signal,
            research_mode=self.config.research_mode,
        ).to_dict()
        self.contract_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    def _plan_payload(self, plan: Any) -> dict[str, Any]:
        return {
            "program_id": str(getattr(plan, "program_id", "") or ""),
            "estimated_hypothesis_count": int(getattr(plan, "estimated_hypothesis_count", 0) or 0),
            "required_detectors": list(getattr(plan, "required_detectors", []) or []),
            "required_features": list(getattr(plan, "required_features", []) or []),
            "required_states": list(getattr(plan, "required_states", []) or []),
        }

    def _pipeline_command(self, config_path: Path, run_id: str) -> list[str]:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        instrument_scope = payload.get("instrument_scope", {}) if isinstance(payload, dict) else {}
        symbols = instrument_scope.get("symbols", []) if isinstance(instrument_scope, dict) else []
        start = (
            str(instrument_scope.get("start", "")).strip()
            if isinstance(instrument_scope, dict)
            else ""
        )
        end = (
            str(instrument_scope.get("end", "")).strip()
            if isinstance(instrument_scope, dict)
            else ""
        )
        timeframe = (
            str(instrument_scope.get("timeframe", "")).strip()
            if isinstance(instrument_scope, dict)
            else ""
        )
        cmd = [
            sys.executable,
            "-m",
            "project.pipelines.run_all",
            "--mode",
            "research",
            "--run_id",
            run_id,
            "--experiment_config",
            str(config_path),
            "--registry_root",
            str(self.registry_root),
            "--run_campaign_memory_update",
            "1",
            "--program_id",
            self.config.program_id,
        ]
        if symbols:
            cmd.extend(
                [
                    "--symbols",
                    ",".join(str(symbol).strip() for symbol in symbols if str(symbol).strip()),
                ]
            )
        if start:
            cmd.extend(["--start", start])
        if end:
            cmd.extend(["--end", end])
        if timeframe:
            cmd.extend(["--timeframes", timeframe])
        return cmd

    def _persist_frontier_proposal_record(
        self,
        *,
        run_id: str,
        config_path: Path,
        plan: Any,
        command: list[str],
        status: str,
        returncode: int,
    ) -> None:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        instrument_scope = payload.get("instrument_scope", {}) if isinstance(payload, dict) else {}
        promotion = payload.get("promotion", {}) if isinstance(payload, dict) else {}
        request_copy_path = self.campaign_dir / run_id / "request.yaml"
        proposal_path = request_copy_path if request_copy_path.exists() else config_path
        proposal_row = {
            "proposal_id": f"proposal::{run_id}",
            "program_id": self.config.program_id,
            "run_id": run_id,
            "issued_at": datetime.now(timezone.utc).isoformat(),
            "proposal_path": str(proposal_path),
            "experiment_config_path": str(config_path),
            "run_all_overrides_path": "",
            "status": status,
            "plan_only": False,
            "dry_run": False,
            "returncode": int(returncode),
            "objective_name": str(payload.get("objective_name", "") or ""),
            "promotion_profile": "research"
            if bool(promotion.get("enabled", False))
            else "exploratory_only",
            "symbols": ",".join(
                str(symbol).strip()
                for symbol in list(instrument_scope.get("symbols", []) or [])
                if str(symbol).strip()
            ),
            "command_json": canonical_json(command),
            "validated_plan_json": canonical_json(self._plan_payload(plan)),
            "bounded_json": "",
            "baseline_run_id": "",
            "experiment_type": "discovery",
            "allowed_change_field": "",
            "campaign_id": "",
            "cycle_number": 0,
            "branch_id": "",
            "parent_run_id": "",
            "mutation_type": "",
            "branch_depth": 0,
            "decision": "",
        }
        existing = read_memory_table(self.config.program_id, "proposals", data_root=self.data_root)
        proposals = _merge_proposal_rows(existing, pd.DataFrame([proposal_row]))
        write_memory_table(self.config.program_id, "proposals", proposals, data_root=self.data_root)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run_campaign(self):
        _LOG.info(
            "Starting campaign: %s (mode=%s)", self.config.program_id, self.config.research_mode
        )

        # Phase 4.1 — Run MI scan once before the first proposal cycle so the
        # controller has fresh data-driven predicate candidates from the start.
        if self.config.auto_run_mi_scan:
            self._run_mi_scan_pre_step()

        for run_idx in range(self.config.max_runs):
            _LOG.info("Iteration %d/%d", run_idx + 1, self.config.max_runs)

            request_dict = self._propose_next_request()
            if not request_dict:
                _LOG.info("No more search frontier. Campaign complete.")
                break

            run_id = (
                f"run_{run_idx + 1}_"
                f"{hashlib.md5(json.dumps(request_dict, sort_keys=True).encode()).hexdigest()[:8]}"
            )
            config_path = self.campaign_dir / f"{run_id}_config.yaml"
            config_path.write_text(yaml.dump(request_dict))

            try:
                plan = build_experiment_plan(
                    config_path,
                    self.registry_root,
                    out_dir=self.campaign_dir / run_id,
                    data_root=self.data_root,
                )
            except Exception as exc:
                _LOG.error("Failed to build plan for %s: %s", run_id, exc)
                continue

            command = self._pipeline_command(config_path, run_id)
            self._persist_frontier_proposal_record(
                run_id=run_id,
                config_path=config_path,
                plan=plan,
                command=command,
                status="planned",
                returncode=0,
            )

            pipeline_failed = False
            returncode = 0
            try:
                self._execute_pipeline(config_path, run_id, command=command)
            except subprocess.CalledProcessError as exc:
                pipeline_failed = True
                returncode = int(exc.returncode or 1)
                _LOG.error("Pipeline execution failed for %s: %s", run_id, exc)
            except Exception as exc:
                pipeline_failed = True
                returncode = 1
                _LOG.error("Pipeline execution failed for %s: %s", run_id, exc)

            self._persist_frontier_proposal_record(
                run_id=run_id,
                config_path=config_path,
                plan=plan,
                command=command,
                status="failed" if pipeline_failed else "executed",
                returncode=returncode,
            )

            try:
                write_operator_outputs_for_run(
                    run_id=run_id,
                    program_id=self.config.program_id,
                    data_root=self.data_root,
                )
            except Exception as exc:
                _LOG.warning("write_operator_outputs_for_run failed for %s: %s", run_id, exc)

            summary = self._update_campaign_stats()
            if pipeline_failed:
                continue
            if self._should_halt(summary):
                _LOG.warning("Halt criteria met. Ending campaign.")
                break

        _LOG.info("Campaign %s finished.", self.config.program_id)

    # ------------------------------------------------------------------
    # Phase 2.1 — Memory-driven proposal (the core change)
    # Phase 2.3 — research_mode routes Step 4 to appropriate scan variant
    # ------------------------------------------------------------------

    def _propose_next_request(self) -> Optional[Dict[str, Any]]:
        """Select the next experiment by reading the memory system first.

        Priority order:
          1. Repair  — resolve open mechanical failures before any new work.
          2. Exploit — confirmatory run when last reflection says to exploit.
          3. Explore — dimension-varying run from the explore_adjacent queue.
          4. Scan    — quality-weighted untested frontier (filtered by avoidance).

        research_mode modifies Step 4 behaviour:
          - "exploit" — never reaches Step 4; returns None when promising_regions empty.
          - "explore" — Step 4 uses cross-family batching (all families at once).
          - "scan"    — Step 4 restricts each batch to the highest-quality untested
                        family, keeping attribution unambiguous (default).
        """
        mem = self._read_memory()

        # ── Step 1: REPAIR ────────────────────────────────────────────────────
        repair_proposal = self._step_repair(mem)
        if repair_proposal is not None:
            return repair_proposal
        current_focus = str(mem.get("belief_state", {}).get("current_focus", "")).strip()
        if current_focus == "repair_pipeline":
            message = (
                "belief_state.json current_focus=repair_pipeline but next_actions.json "
                "has no actionable repair entry; refusing to issue unrelated proposal"
            )
            if self.config.strict_memory_integrity:
                raise CampaignMemoryIntegrityError(message)
            _LOG.warning("Campaign memory consistency warning: %s", message)

        # ── Step 2: EXPLOIT ───────────────────────────────────────────────────
        # In exploit mode only propose from promising_regions; otherwise check
        # if the last reflection recommends an exploit run.
        if self.config.research_mode == "exploit":
            exploit_proposal = self._step_exploit_from_promising(mem)
            if exploit_proposal is not None:
                return exploit_proposal
            _LOG.info("Exploit mode: promising_regions exhausted, nothing to propose.")
            return None

        exploit_proposal = self._step_exploit_from_reflection(mem)
        if exploit_proposal is not None:
            return exploit_proposal

        # ── Step 3: EXPLORE ───────────────────────────────────────────────────
        explore_proposal = self._step_explore_adjacent(mem)
        if explore_proposal is not None:
            return explore_proposal

        # ── Step 4: SCAN ──────────────────────────────────────────────────────
        # explore mode: cross-family batches (wider surface, weaker attribution).
        # scan mode (default): single-family batches (narrow attribution first).
        if self.config.research_mode == "explore":
            return self._step_scan_frontier_cross_family(mem)
        return self._step_scan_frontier(mem)

    # ------------------------------------------------------------------
    # Memory reader — loads all relevant artefacts once per cycle
    # ------------------------------------------------------------------

    def _read_memory(self) -> Dict[str, Any]:
        """Read the full memory state for this program into a single dict."""
        paths = memory_paths(self.config.program_id, data_root=self.data_root)
        integrity_errors: List[str] = []

        def _record_memory_error(path: Path, message: str, *, exc_info: bool = False) -> None:
            detail = f"{path}: {message}"
            integrity_errors.append(detail)
            _LOG.error(
                "Campaign memory integrity failure at %s: %s", path, message, exc_info=exc_info
            )

        def _json(path: Path) -> Dict[str, Any]:
            if path.exists():
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(payload, dict):
                        return payload
                    _record_memory_error(path, "expected JSON object payload, got non-object")
                except Exception:
                    _record_memory_error(
                        path, "failed to parse JSON memory artifact", exc_info=True
                    )
                    return {}
            return {}

        def _parquet(path: Path) -> pd.DataFrame:
            if path.exists():
                try:
                    return read_parquet(path)
                except Exception:
                    _record_memory_error(
                        path, "failed to read Parquet memory artifact", exc_info=True
                    )
                    return pd.DataFrame()
            return pd.DataFrame()

        belief_state = _json(paths.belief_state)
        next_actions = _json(paths.next_actions)
        reflections = _parquet(paths.reflections)

        # Latest reflection row as a dict (empty dict if none yet)
        latest_reflection: Dict[str, Any] = {}
        if not reflections.empty:
            if "created_at" not in reflections.columns:
                _record_memory_error(
                    paths.reflections,
                    "reflections memory artifact is missing required column created_at",
                )
            else:
                latest_reflection = (
                    reflections.sort_values("created_at", ascending=False).iloc[0].to_dict()
                )

        # Avoid regions from belief_state: list of dicts with region metadata
        avoid_region_keys: Set[str] = {
            str(r.get("region_key", ""))
            for r in belief_state.get("avoid_regions", [])
            if r.get("region_key")
        }
        # Also collect avoided event_types for Step 4 frontier filtering
        avoid_event_types: Set[str] = {
            str(r.get("event_type", ""))
            for r in belief_state.get("avoid_regions", [])
            if r.get("event_type")
        }

        # Phase 2.4 — collect stages whose failures have already been superseded
        # so _step_repair can skip re-queuing them.
        superseded_stages: Set[str] = set()
        try:
            failures_df = read_memory_table(
                self.config.program_id, "failures", data_root=self.data_root
            )
            if not failures_df.empty and "superseded_by_run_id" in failures_df.columns:
                superseded_stages = set(
                    failures_df[failures_df["superseded_by_run_id"].astype(str).str.strip() != ""][
                        "stage"
                    ]
                    .astype(str)
                    .unique()
                )
        except Exception:
            _record_memory_error(
                paths.failures,
                "failed to read failures memory artifact for superseded-stage lookup",
                exc_info=True,
            )

        if integrity_errors and self.config.strict_memory_integrity:
            raise CampaignMemoryIntegrityError(
                "Campaign memory integrity check failed: " + " | ".join(integrity_errors)
            )

        return {
            "belief_state": belief_state,
            "next_actions": next_actions,
            "latest_reflection": latest_reflection,
            "avoid_region_keys": avoid_region_keys,
            "avoid_event_types": avoid_event_types,
            "promising_regions": belief_state.get("promising_regions", []),
            "superseded_stages": superseded_stages,
        }

    # ------------------------------------------------------------------
    # Step 1 — Repair
    # ------------------------------------------------------------------

    def _step_repair(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """If there are open mechanical failures, propose a targeted diagnostic run.

        A repair run uses a minimal 1-event, 1-template scope so attribution
        is unambiguous. It targets the event family from the latest failed region
        or falls back to a safe known event.

        Phase 2.4: Entries in the repair queue whose stage is already superseded
        (superseded_by_run_id is populated in the failures table) are skipped so
        the controller does not re-propose repairs for stages that have recovered.
        """
        repair_queue: List[Dict[str, Any]] = mem["next_actions"].get("repair", [])
        if not repair_queue:
            return None

        # Phase 2.4 — filter superseded repairs from the queue before acting
        superseded_stages: Set[str] = mem.get("superseded_stages", set())
        open_repairs = [
            r
            for r in repair_queue
            if str(r.get("proposed_scope", {}).get("stage", "")) not in superseded_stages
        ]
        if not open_repairs:
            _LOG.info("STEP 1 REPAIR: all queued repairs are superseded — skipping.")
            return None

        top_repair = open_repairs[0]
        stage = str(top_repair.get("proposed_scope", {}).get("stage", "unknown"))
        _LOG.info("STEP 1 REPAIR: open failure in stage=%s — proposing diagnostic run", stage)

        events_registry = self.registries.events.get("events", {})
        enabled = [e for e, m in events_registry.items() if m.get("enabled", True)]
        failure_detail = str(top_repair.get("failure_detail", "")).strip()
        event_type = _event_from_failure_detail(failure_detail, enabled)
        if not event_type:
            event_type = _repair_event_for_stage(stage, enabled)
        if not event_type:
            event_type = _preferred_default_repair_event(enabled)

        return self._build_proposal(
            events=[event_type],
            templates=["mean_reversion"],
            horizons=[12],
            description=f"Repair diagnostic — stage={stage}",
            promotion_enabled=False,
            date_scope=self.config.repair_date_scope,
        )

    # ------------------------------------------------------------------
    # Step 2a — Exploit from reflection (scan/explore modes)
    # ------------------------------------------------------------------

    def _step_exploit_from_reflection(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """If the latest reflection recommends exploiting a region, do it.

        Constructs a confirmatory run with promotion enabled, broader date scope,
        and all templates valid for the event's family.
        """
        reflection = mem["latest_reflection"]
        action = str(reflection.get("recommended_next_action", "")).strip()
        if action != "exploit_promising_region":
            return None

        try:
            experiment = json.loads(
                str(reflection.get("recommended_next_experiment", "{}") or "{}")
            )
        except Exception:
            return None

        event_type = str(experiment.get("event_type", "")).strip()
        if not event_type:
            return None

        _LOG.info("STEP 2 EXPLOIT (reflection): event=%s", event_type)

        templates = self._templates_for_event(event_type)
        return self._build_proposal(
            events=[event_type],
            templates=templates,
            horizons=[12, 24, 48],
            description=f"Exploit confirmatory — {event_type}",
            promotion_enabled=True,
            date_scope=self.config.exploit_date_scope,
        )

    # ------------------------------------------------------------------
    # Step 2b — Exploit from promising_regions (exploit mode only)
    # ------------------------------------------------------------------

    def _step_exploit_from_promising(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Propose from the top promising region in belief_state.

        Used when research_mode == "exploit".
        """
        for region in mem["promising_regions"]:
            event_type = str(region.get("event_type", "")).strip()
            if not event_type:
                continue
            _LOG.info(
                "STEP 2 EXPLOIT (promising): trigger_type=%s event=%s",
                str(region.get("trigger_type", "EVENT")).strip().upper() or "EVENT",
                event_type,
            )
            scope = dict(region)
            if "context_json" in scope and "contexts" not in scope:
                try:
                    parsed_contexts = json.loads(str(scope.get("context_json", "{}")))
                except Exception:
                    parsed_contexts = {}
                scope["contexts"] = parsed_contexts if isinstance(parsed_contexts, dict) else {}
            proposal = _scan_support.build_proposal_from_memory_scope(
                self,
                scope,
                description=f"Exploit promising region — {event_type}",
                promotion_enabled=True,
                date_scope=self.config.exploit_date_scope,
                default_horizons=[12, 24, 48],
            )
            if proposal is not None:
                return proposal
        return None

    # ------------------------------------------------------------------
    # Step 3 — Explore adjacent
    # ------------------------------------------------------------------

    def _step_explore_adjacent(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Vary one dimension of a near-miss region from the explore_adjacent queue.

        Tries a broader horizon set and an adjacent template to identify
        the dimension that was limiting the prior result.
        """
        explore_queue: List[Dict[str, Any]] = mem["next_actions"].get("explore_adjacent", [])
        if not explore_queue:
            return None
        tested_regions = read_memory_table(
            self.config.program_id, "tested_regions", data_root=self.data_root
        )
        for entry in explore_queue:
            try:
                scope = entry.get("proposed_scope", {})
                if isinstance(scope, str):
                    scope = json.loads(scope)
            except Exception:
                scope = {}
            if not isinstance(scope, dict) or not scope:
                continue

            # Propagate any context conditioning embedded in the explore scope.
            # Regime-conditional explore entries (from Phase 4.2 regime signal injection)
            # carry a contexts dict so the follow-up run targets the specific regime.
            raw_contexts = scope.get("contexts", {})
            contexts = raw_contexts if isinstance(raw_contexts, dict) else {}
            if "context_json" in scope and not contexts:
                try:
                    parsed_contexts = json.loads(str(scope.get("context_json", "{}")))
                except Exception:
                    parsed_contexts = {}
                contexts = parsed_contexts if isinstance(parsed_contexts, dict) else {}
            # Merge with config-level context conditioning (config wins on conflict)
            if self.config.enable_context_conditioning and not contexts:
                contexts = self._context_for_proposal()

            scope["contexts"] = contexts
            if _scope_already_tested(tested_regions, scope):
                _LOG.info(
                    "STEP 3 EXPLORE ADJACENT: skip already-tested scope %s",
                    canonical_json(scope),
                )
                continue

            scope_label = (
                str(scope.get("event_type", "")).strip()
                or str(scope.get("state_id", "")).strip()
                or str(scope.get("trigger_type", "scope")).strip().upper()
            )
            _LOG.info("STEP 3 EXPLORE ADJACENT: target=%s", scope_label)

            proposal = _scan_support.build_proposal_from_memory_scope(
                self,
                scope,
                description=f"Explore adjacent — {scope_label}",
                promotion_enabled=False,
                date_scope=self.config.explore_date_scope,
                default_horizons=[6, 12, 24, 48],
            )
            if proposal is not None:
                return proposal
        return None

    # ------------------------------------------------------------------
    # Step 4 — Quality-weighted frontier scan
    # ------------------------------------------------------------------

    def _step_scan_frontier(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _scan_support.step_scan_frontier(self, mem)

    def _step_scan_for_type(
        self, trigger_type: str, mem: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        return _scan_support.step_scan_for_type(self, trigger_type, mem)

    # ---- EVENT scan (Phase 3.1 + single-family constraint from Phase 2.3) ----

    def _step_scan_events(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _scan_support.step_scan_events(self, mem)

    # ---- STATE scan (Phase 3.1) ----

    def _step_scan_states(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _scan_support.step_scan_states(self, mem)

    # ---- TRANSITION scan (Phase 3.1) ----

    def _step_scan_transitions(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _scan_support.step_scan_transitions(self, mem)

    # ---- FEATURE_PREDICATE scan (Phase 3.5) ----

    def _step_scan_feature_predicates(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _scan_support.step_scan_feature_predicates(self, mem)

    # ---- SEQUENCE scan (Phase 3.4) ----

    def _step_scan_sequences(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _scan_support.step_scan_sequences(self, mem)

    # ---- INTERACTION scan (sixth trigger type — cross-dimensional motifs) ----

    def _step_scan_interactions(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _scan_support.step_scan_interactions(self, mem)

    def _load_interaction_motifs(self) -> List[Dict[str, Any]]:
        return _scan_support.load_interaction_motifs(self)

    # ---- Cross-family explore (Phase 2.3, updated for trigger types) ----

    def _step_scan_frontier_cross_family(self, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _scan_support.step_scan_frontier_cross_family(self, mem)

    # ---- Search-space YAML helpers (Phase 3.1/3.4/3.5) ----

    def _load_search_space_states(self) -> List[str]:
        return _scan_support.load_search_space_states(self)

    def _load_search_space_transitions(self) -> List[Dict[str, str]]:
        return _scan_support.load_search_space_transitions(self)

    def _load_search_space_predicates(self) -> List[Dict[str, Any]]:
        return _scan_support.load_search_space_predicates(self)

    def _load_mi_candidate_predicates(self) -> List[Dict[str, Any]]:
        return _scan_support.load_mi_candidate_predicates(self)

    def _find_weak_signal_event_pairs(self) -> List[tuple]:
        return _scan_support.find_weak_signal_event_pairs(self)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _templates_for_event(self, event_id: str) -> List[str]:
        return _scan_support.templates_for_event(self, event_id)

    def _executable_regime_event_fanout(self) -> Dict[str, List[str]]:
        registry = get_domain_registry()
        return {
            regime: list(registry.get_event_ids_for_regime(regime, executable_only=True))
            for regime in registry.canonical_regime_rows()
            if registry.get_event_ids_for_regime(regime, executable_only=True)
        }

    def _event_to_regime_map(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for regime, event_ids in self._executable_regime_event_fanout().items():
            for event_id in event_ids:
                out[event_id] = regime
        return out

    def _build_proposal(
        self,
        *,
        events: List[str],
        templates: List[str],
        horizons: List[int],
        directions: Optional[List[str]] = None,
        entry_lags: Optional[List[int]] = None,
        description: str,
        promotion_enabled: bool,
        date_scope: tuple[str, str],
        # Phase 3.1 — trigger type (default EVENT for backward compat)
        trigger_type: str = "EVENT",
        # Phase 3.1 — non-event trigger payload (states/transitions/predicates)
        states: Optional[List[str]] = None,
        transitions: Optional[List[Dict[str, str]]] = None,
        feature_predicates: Optional[List[Dict[str, Any]]] = None,
        sequences: Optional[Dict[str, Any]] = None,
        interactions: Optional[List[Dict[str, Any]]] = None,
        # Phase 3.2 — context conditioning
        contexts: Optional[Dict[str, List[str]]] = None,
        canonical_regimes: Optional[List[str]] = None,
        subtypes: Optional[List[str]] = None,
        phases: Optional[List[str]] = None,
        evidence_modes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        return _scan_support.build_proposal(
            self,
            events=events,
            templates=templates,
            horizons=horizons,
            directions=directions,
            entry_lags=entry_lags,
            description=description,
            promotion_enabled=promotion_enabled,
            date_scope=date_scope,
            trigger_type=trigger_type,
            states=states,
            transitions=transitions,
            feature_predicates=feature_predicates,
            sequences=sequences,
            interactions=interactions,
            contexts=contexts,
            canonical_regimes=canonical_regimes,
            subtypes=subtypes,
            phases=phases,
            evidence_modes=evidence_modes,
        )

    def _context_for_proposal(self) -> Dict[str, List[str]]:
        return _scan_support.context_for_proposal(self)

    # ------------------------------------------------------------------
    # Pipeline execution
    # ------------------------------------------------------------------

    def _run_mi_scan_pre_step(self) -> None:
        """Phase 4.1 — Run feature MI scan once before the first proposal cycle.

        Loads features for the configured symbols/timeframe, runs
        run_feature_mi_scan(), and writes candidate_predicates.json to
        data/reports/feature_mi/<program_id>/ so _load_mi_candidate_predicates()
        picks it up on the first Step-4 proposal.

        Failures are logged and swallowed — a missing MI scan should never
        block a campaign from starting.
        """
        try:
            from project.research.feature_mi_scan import run_feature_mi_scan
            from project.research.phase2 import load_features

            symbols = [
                s.strip().upper() for s in self.config.mi_scan_symbols.split(",") if s.strip()
            ]
            parts = []
            for sym in symbols:
                df = load_features(
                    self.data_root,
                    self.config.program_id,
                    sym,
                    timeframe=self.config.mi_scan_timeframe,
                )
                if not df.empty:
                    df = df.copy()
                    df["symbol"] = sym
                    parts.append(df)

            if not parts:
                _LOG.info("MI scan pre-step: no features found for %s — skipping.", symbols)
                return

            import pandas as _pd

            features = _pd.concat(parts, ignore_index=True)

            out_dir = self.data_root / "reports" / "feature_mi" / self.config.program_id
            result = run_feature_mi_scan(features, out_dir=out_dir)
            _LOG.info(
                "MI scan pre-step complete: %d MI rows, %d candidate predicates → %s",
                result["mi_rows"],
                result["candidate_predicates"],
                result["out_dir"],
            )
        except Exception as exc:
            _LOG.warning("MI scan pre-step failed (non-fatal): %s", exc)

    def _execute_pipeline(
        self, config_path: Path, run_id: str, *, command: list[str] | None = None
    ):
        _LOG.info("Executing pipeline for %s...", run_id)
        cmd = command or self._pipeline_command(config_path, run_id)
        _LOG.info("Command: %s", " ".join(cmd))
        subprocess.run(cmd, check=True, cwd=str(Path.cwd()))

    # ------------------------------------------------------------------
    # Stats update — now delegates to update_search_intelligence
    # ------------------------------------------------------------------

    def _update_campaign_stats(self) -> CampaignSummary:
        """Update campaign summary and frontier via the unified search_intelligence.

        Phase 2.1: replaces the dual _update_frontier + ledger-based system with
        a single call to update_search_intelligence(), which writes the richer
        campaign_summary.json and search_frontier.json (with candidate_next_moves).
        """
        # Always run search intelligence update regardless of ledger state
        try:
            update_search_intelligence(
                self.data_root,
                self.registry_root,
                self.config.program_id,
            )
        except Exception as exc:
            raise RuntimeError(
                f"update_search_intelligence failed for program {self.config.program_id}"
            ) from exc

        if not self.ledger_path.exists():
            return CampaignSummary(self.config.program_id)

        df = read_parquet(self.ledger_path)
        summary = CampaignSummary(
            program_id=self.config.program_id,
            total_runs=int(df["run_id"].nunique()) if "run_id" in df.columns else 0,
            total_generated=len(df),
            total_evaluated=int((df["eval_status"] == "evaluated").sum())
            if "eval_status" in df.columns
            else 0,
            total_empty_sample=int((df["eval_status"] == "empty_sample").sum())
            if "eval_status" in df.columns
            else 0,
            total_insufficient_sample=int((df["eval_status"] == "insufficient_sample").sum())
            if "eval_status" in df.columns
            else 0,
            total_unsupported=int((df["eval_status"] == "unsupported_trigger_evaluator").sum())
            if "eval_status" in df.columns
            else 0,
            total_skipped=int((df["eval_status"] == "not_executed_or_missing_data").sum())
            if "eval_status" in df.columns
            else 0,
        )

        if not df.empty and "expectancy" in df.columns and "eval_status" in df.columns:
            top = (
                df[df["eval_status"] == "evaluated"]
                .sort_values("expectancy", ascending=False)
                .head(5)
            )
            summary.top_hypotheses = top.to_dict(orient="records")

        self.summary_path.write_text(summary.to_json())
        self._write_frontier_compat_from_ledger(df)
        return summary

    def _write_frontier_compat_from_ledger(self, df: pd.DataFrame) -> None:
        frontier: Dict[str, Any] = {}
        if self.frontier_path.exists():
            try:
                frontier = json.loads(self.frontier_path.read_text(encoding="utf-8"))
            except Exception:
                frontier = {}

        events_registry = self.registries.events.get("events", {})
        enabled_events = sorted(
            eid for eid, meta in events_registry.items() if bool(meta.get("enabled", True))
        )
        tested_events: Set[str] = set()

        if "event_type" in df.columns:
            tested_events |= set(df["event_type"].dropna().astype(str))

        if "trigger_payload" in df.columns:

            def _payload_event_id(payload: object) -> Optional[str]:
                try:
                    parsed = json.loads(str(payload))
                except Exception:
                    return None
                value = str(parsed.get("event_id", "")).strip()
                return value or None

            tested_events |= set(
                df["trigger_payload"].apply(_payload_event_id).dropna().astype(str)
            )

        untested_events = [eid for eid in enabled_events if eid not in tested_events]
        partially_explored_families: List[str] = []
        family_to_events: Dict[str, List[str]] = {}
        for eid in enabled_events:
            family = str(events_registry.get(eid, {}).get("family", "")).strip()
            if family:
                family_to_events.setdefault(family, []).append(eid)
        for family, family_events in family_to_events.items():
            tested_count = sum(1 for eid in family_events if eid in tested_events)
            if 0 < tested_count < len(family_events):
                partially_explored_families.append(family)

        frontier["untested_events"] = untested_events
        frontier["untested_registry_events"] = untested_events
        frontier["partially_explored_families"] = sorted(partially_explored_families)
        self.frontier_path.write_text(
            json.dumps(frontier, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    # ------------------------------------------------------------------
    # Halt check
    # ------------------------------------------------------------------

    def _should_halt(self, summary: CampaignSummary) -> bool:
        if summary.total_generated == 0:
            return False

        empty_share = summary.total_empty_sample / summary.total_generated
        if empty_share > self.config.halt_on_empty_share:
            _LOG.warning("High empty sample share: %.1f%%", empty_share * 100)
            return True

        unsupported_share = summary.total_unsupported / summary.total_generated
        if unsupported_share > self.config.halt_on_unsupported_share:
            _LOG.warning("High unsupported trigger share: %.1f%%", unsupported_share * 100)
            return True

        return False


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Run an autonomous EDGE research campaign.")
    parser.add_argument("--program_id", required=True)
    parser.add_argument("--max_runs", type=int, default=50)
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument(
        "--research_mode",
        choices=["scan", "exploit", "explore"],
        default="scan",
        help="Proposal strategy: scan=frontier, exploit=promising regions, explore=adjacent",
    )
    parser.add_argument(
        "--report", action="store_true", help="Print campaign health report and exit"
    )
    args = parser.parse_args()

    if args.report:
        from project.research.services.campaign_memory_rollup_service import (
            build_campaign_memory_rollup,
        )

        rollup = build_campaign_memory_rollup(
            program_id=args.program_id,
            data_root=get_data_root(),
        )
        print(json.dumps(rollup, indent=2))
        sys.exit(0)

    data_root = get_data_root()
    config = CampaignConfig(
        program_id=args.program_id,
        max_runs=args.max_runs,
        research_mode=args.research_mode,
    )
    controller = CampaignController(config, data_root, Path(args.registry_root))
    controller.run_campaign()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
