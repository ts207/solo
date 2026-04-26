"""
Closed-loop autonomous research planner.

Orchestrates the full research cycle:
1. Read campaign memory and belief state
2. Generate ranked proposals using CampaignPlanner
3. Execute top proposal
4. Build reflection for the completed run
5. Record next action to memory
6. Update belief state

This enables fully autonomous nightly research at zero marginal researcher effort.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError
from project.io.utils import atomic_write_json, atomic_write_text
from project.research.agent_io.campaign_planner import (
    CampaignPlanner,
    CampaignPlannerConfig,
)
from project.research.agent_io.issue_proposal import generate_run_id, issue_proposal
from project.research.knowledge.memory import (
    build_tested_regions_snapshot,
    compute_context_statistics,
    compute_event_statistics,
    compute_region_statistics,
    compute_template_statistics,
    ensure_memory_store,
    read_memory_table,
    write_memory_table,
)
from project.research.knowledge.reflection import build_run_reflection
from project.research.update_campaign_memory import _build_next_actions

_LOG = logging.getLogger(__name__)


@dataclass
class CycleConfig:
    program_id: str
    registry_root: Path
    data_root: Path | None = None
    symbols: tuple[str, ...] = ("BTCUSDT",)
    lookback_days: int = 90
    max_proposals: int = 10
    execute: bool = True
    dry_run: bool = False
    plan_only: bool = False
    wait_for_completion: bool = False
    poll_interval_seconds: int = 60
    max_wait_minutes: int = 120
    max_retries: int = 3


@dataclass
class CycleResult:
    program_id: str
    cycle_id: str
    timestamp: str
    proposals_planned: int
    execution_run_id: str | None
    execution_status: str
    reflection: dict[str, Any] | None
    next_action: str
    belief_state_updated: bool
    summary: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "program_id": self.program_id,
            "cycle_id": self.cycle_id,
            "timestamp": self.timestamp,
            "proposals_planned": self.proposals_planned,
            "execution_run_id": self.execution_run_id,
            "execution_status": self.execution_status,
            "reflection": self.reflection,
            "next_action": self.next_action,
            "belief_state_updated": self.belief_state_updated,
            "summary": self.summary,
            "errors": self.errors,
        }


class CampaignCycleRunner:
    """Runs a complete research cycle: plan -> execute -> reflect -> update."""

    def __init__(self, config: CycleConfig):
        self.config = config
        self.data_root = Path(config.data_root) if config.data_root else get_data_root()
        self.registry_root = Path(config.registry_root)
        self.paths = ensure_memory_store(config.program_id, data_root=self.data_root)
        self.cycle_id = self._generate_cycle_id()

    def _generate_cycle_id(self) -> str:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"{self.config.program_id}_cycle_{stamp}"

    def _load_belief_state(self) -> dict[str, Any]:
        path = self.paths.belief_state
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception as e:
                raise DataIntegrityError(f"Failed to load belief state {path}: {e}") from e
        return {
            "current_focus": "",
            "avoid_regions": [],
            "promising_regions": [],
            "open_repairs": [],
            "last_reflection_run_id": "",
        }

    def _save_belief_state(self, belief_state: dict[str, Any]) -> None:
        atomic_write_json(
            self.paths.belief_state,
            belief_state,
        )

    def _load_next_actions(self) -> dict[str, list[str]]:
        path = self.paths.next_actions
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception as e:
                raise DataIntegrityError(f"Failed to load next actions {path}: {e}") from e
        return {"repair": [], "exploit": [], "retest": [], "explore_adjacent": [], "hold": []}

    def _save_next_actions(self, next_actions: dict[str, list[str]]) -> None:
        atomic_write_json(
            self.paths.next_actions,
            next_actions,
        )

    def _build_reflection(self, run_id: str) -> dict[str, Any] | None:
        try:
            return build_run_reflection(
                run_id=run_id,
                program_id=self.config.program_id,
                data_root=self.data_root,
            )
        except Exception as e:
            _LOG.error("Failed to build reflection for %s: %s", run_id, e)
            return None

    def _update_memory_from_run(self, run_id: str) -> None:
        """Build tested regions snapshot and update all statistics."""
        try:
            tested = build_tested_regions_snapshot(
                run_id=run_id,
                program_id=self.config.program_id,
                data_root=self.data_root,
            )
            if not tested.empty:
                existing = read_memory_table(
                    self.config.program_id, "tested_regions", data_root=self.data_root
                )
                if existing.empty:
                    merged = tested.copy()
                elif tested.empty:
                    merged = existing.copy()
                else:
                    merged = pd.concat([existing, tested], ignore_index=True)
                merged = merged.drop_duplicates(subset=["region_key"], keep="last")
                write_memory_table(
                    self.config.program_id, "tested_regions", merged, data_root=self.data_root
                )
                region_stats = compute_region_statistics(merged)
                write_memory_table(
                    self.config.program_id, "region_statistics", region_stats, data_root=self.data_root
                )
                event_stats = compute_event_statistics(merged)
                write_memory_table(
                    self.config.program_id, "event_statistics", event_stats, data_root=self.data_root
                )
                template_stats = compute_template_statistics(merged)
                write_memory_table(
                    self.config.program_id, "template_statistics", template_stats, data_root=self.data_root
                )
                context_stats = compute_context_statistics(merged)
                write_memory_table(
                    self.config.program_id, "context_statistics", context_stats, data_root=self.data_root
                )
                _LOG.info("Updated memory with %d regions from run %s", len(tested), run_id)
        except Exception as e:
            _LOG.error("Failed to update memory from run %s: %s", run_id, e)

    def _update_belief_state(
        self,
        belief_state: dict[str, Any],
        reflection: dict[str, Any],
    ) -> dict[str, Any]:
        """Update belief state based on reflection findings."""
        next_action = reflection.get("recommended_next_action", "hold")
        statistical_outcome = reflection.get("statistical_outcome", "")
        mechanical_outcome = reflection.get("mechanical_outcome", "")
        top_event = reflection.get("primary_fail_gate", "")

        if mechanical_outcome in {"mechanical_failure", "artifact_contract_failure"}:
            belief_state["open_repairs"].append(reflection.get("run_id", ""))
            belief_state["open_repairs"] = belief_state["open_repairs"][-10:]
        elif statistical_outcome == "deploy_promising":
            promising = {
                "event": top_event,
                "run_id": reflection.get("run_id", ""),
                "timestamp": reflection.get("created_at", ""),
            }
            belief_state["promising_regions"] = [
                p for p in belief_state.get("promising_regions", [])
                if p.get("event") != top_event
            ] + [promising]
            belief_state["promising_regions"] = belief_state["promising_regions"][-20:]
        elif next_action == "repair_pipeline":
            belief_state["open_repairs"].append(reflection.get("run_id", ""))
            belief_state["open_repairs"] = belief_state["open_repairs"][-10:]

        belief_state["last_reflection_run_id"] = reflection.get("run_id", "")
        return belief_state

    def _update_next_actions(
        self,
        next_actions: dict[str, list[str]],
        reflection: dict[str, Any],
    ) -> dict[str, list[str]]:
        """Rebuild next actions from current memory using the shared action policy."""
        tested_regions = read_memory_table(
            self.config.program_id, "tested_regions", data_root=self.data_root
        )
        failures = read_memory_table(
            self.config.program_id, "failures", data_root=self.data_root
        )
        rebuilt = _build_next_actions(
            reflection=reflection,
            tested_regions=tested_regions,
            failures=failures,
            regime_conditional_candidates=pd.DataFrame(),
            exploit_top_k=3,
            repair_top_k=3,
        )
        for key in list(rebuilt.keys()):
            rebuilt[key] = list(rebuilt.get(key, []))[:50]
        return rebuilt

    def _record_reflection(self, reflection: dict[str, Any]) -> None:
        """Record reflection to memory."""
        if not reflection:
            return
        try:
            existing = read_memory_table(
                self.config.program_id, "reflections", data_root=self.data_root
            )
            reflection_df = pd.DataFrame([reflection])
            merged = pd.concat([existing, reflection_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["run_id"], keep="last")
            write_memory_table(
                self.config.program_id, "reflections", merged, data_root=self.data_root
            )
            _LOG.info("Recorded reflection for run %s", reflection.get("run_id"))
        except Exception as e:
            _LOG.error("Failed to record reflection: %s", e)

    def _wait_for_run(self, run_id: str) -> bool:
        """Poll for run completion."""
        if not self.config.wait_for_completion:
            return True

        manifest_path = self.data_root / "runs" / run_id / "run_manifest.json"
        max_wait = self.config.max_wait_minutes * 60
        elapsed = 0

        while elapsed < max_wait:
            if manifest_path.exists():
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    status = manifest.get("status", "").strip().lower()
                    if status in {"success", "failed", "warning"}:
                        return True
                except Exception:
                    pass
            time.sleep(self.config.poll_interval_seconds)
            elapsed += self.config.poll_interval_seconds

        _LOG.warning("Run %s did not complete within %d minutes", run_id, self.config.max_wait_minutes)
        return False

    def run(self) -> CycleResult:
        """Execute a complete research cycle."""
        errors: list[str] = []
        execution_run_id: str | None = None
        execution_status = "skipped"
        reflection: dict[str, Any] | None = None

        try:
            planner_config = CampaignPlannerConfig(
                program_id=self.config.program_id,
                registry_root=self.registry_root,
                data_root=self.data_root,
                symbols=self.config.symbols,
                lookback_days=self.config.lookback_days,
                max_proposals=self.config.max_proposals,
            )
            planner = CampaignPlanner(planner_config)
            plan = planner.plan()

            if not plan.ranked_proposals:
                execution_status = "no_candidates"
                return CycleResult(
                    program_id=self.config.program_id,
                    cycle_id=self.cycle_id,
                    timestamp=datetime.now(UTC).isoformat(),
                    proposals_planned=0,
                    execution_run_id=None,
                    execution_status=execution_status,
                    reflection=None,
                    next_action="hold",
                    belief_state_updated=False,
                    summary=plan.summary,
                    errors=["No candidate proposals available"],
                )

            top_proposal = plan.ranked_proposals[0]

            if self.config.plan_only:
                execution_status = "planned_only"
                return CycleResult(
                    program_id=self.config.program_id,
                    cycle_id=self.cycle_id,
                    timestamp=datetime.now(UTC).isoformat(),
                    proposals_planned=len(plan.ranked_proposals),
                    execution_run_id=None,
                    execution_status=execution_status,
                    reflection=None,
                    next_action="hold",
                    belief_state_updated=False,
                    summary={
                        **plan.summary,
                        "top_event": top_proposal.event_type,
                        "top_score": top_proposal.score,
                    },
                )

            if not self.config.execute:
                execution_status = "skipped"
                return CycleResult(
                    program_id=self.config.program_id,
                    cycle_id=self.cycle_id,
                    timestamp=datetime.now(UTC).isoformat(),
                    proposals_planned=len(plan.ranked_proposals),
                    execution_run_id=None,
                    execution_status=execution_status,
                    reflection=None,
                    next_action="hold",
                    belief_state_updated=False,
                    summary=plan.summary,
                )

            proposal_dict = top_proposal.proposal
            run_id = generate_run_id(self.config.program_id, proposal_dict)

            proposal_path = self.paths.proposals_dir / self.cycle_id / "proposal.yaml"
            proposal_path.parent.mkdir(parents=True, exist_ok=True)
            import yaml
            atomic_write_text(proposal_path, yaml.safe_dump(proposal_dict, sort_keys=False))

            _LOG.info("Executing proposal for %s (run_id=%s)", top_proposal.event_type, run_id)

            result = issue_proposal(
                proposal_path,
                registry_root=self.registry_root,
                data_root=self.data_root,
                run_id=run_id,
                plan_only=False,
                dry_run=self.config.dry_run,
            )

            execution_run_id = result.get("run_id", run_id)
            execution_status = "executed" if not self.config.dry_run else "dry_run"

            if self.config.dry_run:
                return CycleResult(
                    program_id=self.config.program_id,
                    cycle_id=self.cycle_id,
                    timestamp=datetime.now(UTC).isoformat(),
                    proposals_planned=len(plan.ranked_proposals),
                    execution_run_id=execution_run_id,
                    execution_status=execution_status,
                    reflection=None,
                    next_action="hold",
                    belief_state_updated=False,
                    summary={
                        **plan.summary,
                        "top_event": top_proposal.event_type,
                        "top_score": top_proposal.score,
                    },
                )

            completed = self._wait_for_run(execution_run_id)
            if not completed:
                errors.append(f"Run {execution_run_id} did not complete in time")

            self._update_memory_from_run(execution_run_id)

            reflection = self._build_reflection(execution_run_id)
            if reflection:
                self._record_reflection(reflection)

                belief_state = self._load_belief_state()
                updated_belief = self._update_belief_state(belief_state, reflection)
                self._save_belief_state(updated_belief)

                next_actions = self._load_next_actions()
                updated_actions = self._update_next_actions(next_actions, reflection)
                self._save_next_actions(updated_actions)

        except Exception as e:
            _LOG.error("Cycle %s failed: %s", self.cycle_id, e)
            errors.append(str(e))
            execution_status = "failed"

        return CycleResult(
            program_id=self.config.program_id,
            cycle_id=self.cycle_id,
            timestamp=datetime.now(UTC).isoformat(),
            proposals_planned=0,
            execution_run_id=execution_run_id,
            execution_status=execution_status,
            reflection=reflection,
            next_action=reflection.get("recommended_next_action", "hold") if reflection else "hold",
            belief_state_updated=reflection is not None,
            errors=errors,
        )


def run_autonomous_cycle(config: CycleConfig) -> CycleResult:
    """Convenience function to run a complete autonomous research cycle."""
    runner = CampaignCycleRunner(config)
    return runner.run()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an autonomous research cycle: plan -> execute -> reflect -> update."
    )
    parser.add_argument("--program_id", required=True, help="Campaign program identifier")
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--symbols", default="BTCUSDT", help="Comma-separated symbols")
    parser.add_argument("--lookback_days", type=int, default=90)
    parser.add_argument("--max_proposals", type=int, default=10)
    parser.add_argument(
        "--execute", type=int, default=1, help="Execute proposal (0=plan only)"
    )
    parser.add_argument(
        "--dry_run", type=int, default=0, help="Dry run (don't actually execute)"
    )
    parser.add_argument(
        "--plan_only", type=int, default=0, help="Plan only, don't execute"
    )
    parser.add_argument(
        "--wait", type=int, default=0, help="Wait for run completion"
    )
    parser.add_argument(
        "--poll_interval", type=int, default=60, help="Poll interval in seconds"
    )
    parser.add_argument(
        "--max_wait", type=int, default=120, help="Max wait time in minutes"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    config = CycleConfig(
        program_id=args.program_id,
        registry_root=Path(args.registry_root),
        data_root=Path(args.data_root) if args.data_root else None,
        symbols=tuple(s.strip().upper() for s in str(args.symbols).split(",") if s.strip()),
        lookback_days=args.lookback_days,
        max_proposals=args.max_proposals,
        execute=bool(args.execute),
        dry_run=bool(args.dry_run),
        plan_only=bool(args.plan_only),
        wait_for_completion=bool(args.wait),
        poll_interval_seconds=args.poll_interval,
        max_wait_minutes=args.max_wait,
    )

    result = run_autonomous_cycle(config)
    print(json.dumps(result.to_dict(), indent=2, default=str))

    if result.errors:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
