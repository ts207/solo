from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping

import yaml

CampaignMode = Literal["autonomous", "operator_guided", "repair_only", "validation_only"]


@dataclass(frozen=True)
class CampaignStopConditions:
    max_cycles: int = 1
    max_fail_streak: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_cycles": int(self.max_cycles),
            "max_fail_streak": int(self.max_fail_streak),
        }


@dataclass(frozen=True)
class CampaignLineage:
    controller_module: str = "project.research.campaign_controller"
    controller_name: str = "CampaignController"
    orchestration_surface: str = "canonical"
    operator_adapter_module: str = "project.operator.campaign_engine"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CampaignContract:
    campaign_id: str
    program_id: str = ""
    initial_proposal: str = ""
    mode: CampaignMode = "operator_guided"
    registry_root: str = "project/configs/registries"
    stop_conditions: CampaignStopConditions = field(default_factory=CampaignStopConditions)
    lineage: CampaignLineage = field(default_factory=CampaignLineage)
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: str = "campaign_contract_v1"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["stop_conditions"] = self.stop_conditions.to_dict()
        payload["lineage"] = self.lineage.to_dict()
        payload["metadata"] = dict(self.metadata)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


@dataclass(frozen=True)
class ControllerContractView:
    program_id: str
    mode: CampaignMode
    registry_root: str
    max_runs: int
    max_consecutive_no_signal: int
    research_mode: str
    stop_conditions: dict[str, Any]
    lineage: CampaignLineage = field(default_factory=CampaignLineage)
    schema_version: str = "campaign_controller_contract_v1"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "program_id": self.program_id,
            "mode": self.mode,
            "registry_root": self.registry_root,
            "max_runs": int(self.max_runs),
            "max_consecutive_no_signal": int(self.max_consecutive_no_signal),
            "research_mode": self.research_mode,
            "stop_conditions": dict(self.stop_conditions),
            "lineage": self.lineage.to_dict(),
        }


def _read_payload(path_or_payload: str | Path | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(path_or_payload, Mapping):
        return dict(path_or_payload)
    path = Path(path_or_payload)
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        payload = json.loads(text)
    else:
        payload = yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError("Campaign contract must decode to an object payload")
    return dict(payload)


def _normalize_mode(raw: Any) -> CampaignMode:
    value = str(raw or "operator_guided").strip().lower()
    if value not in {"autonomous", "operator_guided", "repair_only", "validation_only"}:
        raise ValueError(f"Unsupported campaign mode: {raw}")
    return value  # type: ignore[return-value]


def load_campaign_contract(path_or_payload: str | Path | Mapping[str, Any]) -> CampaignContract:
    raw = _read_payload(path_or_payload)
    stop_payload = raw.get("stop_conditions", {}) or {}
    if not isinstance(stop_payload, Mapping):
        raise ValueError("campaign.stop_conditions must be an object")
    lineage_payload = raw.get("lineage", {}) or {}
    if not isinstance(lineage_payload, Mapping):
        raise ValueError("campaign.lineage must be an object")
    metadata = raw.get("metadata", {}) or {}
    if not isinstance(metadata, Mapping):
        raise ValueError("campaign.metadata must be an object")
    return CampaignContract(
        campaign_id=str(raw.get("campaign_id", "") or "").strip(),
        program_id=str(raw.get("program_id", "") or "").strip(),
        initial_proposal=str(raw.get("initial_proposal", "") or "").strip(),
        mode=_normalize_mode(raw.get("mode", raw.get("campaign_mode", "operator_guided"))),
        registry_root=str(raw.get("registry_root", "project/configs/registries") or "project/configs/registries"),
        stop_conditions=CampaignStopConditions(
            max_cycles=int(raw.get("max_cycles", stop_payload.get("max_cycles", 1)) or 1),
            max_fail_streak=int(stop_payload.get("max_fail_streak", raw.get("max_fail_streak", 1)) or 1),
        ),
        lineage=CampaignLineage(
            controller_module=str(lineage_payload.get("controller_module", "project.research.campaign_controller") or "project.research.campaign_controller"),
            controller_name=str(lineage_payload.get("controller_name", "CampaignController") or "CampaignController"),
            orchestration_surface=str(lineage_payload.get("orchestration_surface", "canonical") or "canonical"),
            operator_adapter_module=str(lineage_payload.get("operator_adapter_module", "project.operator.campaign_engine") or "project.operator.campaign_engine"),
        ),
        metadata=dict(metadata),
        schema_version=str(raw.get("schema_version", "campaign_contract_v1") or "campaign_contract_v1"),
    )


def controller_contract_view(*, program_id: str, registry_root: str, max_runs: int, max_consecutive_no_signal: int, research_mode: str) -> ControllerContractView:
    return ControllerContractView(
        program_id=str(program_id).strip(),
        mode="autonomous",
        registry_root=str(registry_root),
        max_runs=int(max_runs),
        max_consecutive_no_signal=int(max_consecutive_no_signal),
        research_mode=str(research_mode).strip() or "scan",
        stop_conditions={
            "max_runs": int(max_runs),
            "max_consecutive_no_signal": int(max_consecutive_no_signal),
        },
    )
