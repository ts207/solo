from __future__ import annotations

import functools
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator

from project.spec_registry.loaders import load_yaml_relative, repo_root


class EpisodeContract(BaseModel):
    model_config = ConfigDict(frozen=True)

    episode_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    causal_mechanism: str = ""
    required_events: list[str] = Field(default_factory=list)
    optional_confirmation_events: list[str] = Field(default_factory=list)
    invalidation_events: list[str] = Field(default_factory=list)
    disallowed_regimes: list[str] = Field(default_factory=list)
    tier: str = "D"
    operational_role: str = "sequence_component"
    deployment_disposition: str = "research_only"
    sequence_mode: str = "ordered_strict"
    minimum_required_events: int = 1
    runtime_min_required_events: int = 1
    runtime_hint: str = ""

    @field_validator(
        "episode_id",
        "title",
        "description",
        "causal_mechanism",
        "tier",
        "operational_role",
        "deployment_disposition",
        "sequence_mode",
        "runtime_hint",
        mode="before",
    )
    @classmethod
    def _strip_scalar(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator(
        "required_events",
        "optional_confirmation_events",
        "invalidation_events",
        "disallowed_regimes",
        mode="before",
    )
    @classmethod
    def _normalize_list(cls, value: Any) -> list[str]:
        if value in (None, ""):
            return []
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, (list, tuple, set)):
            return []
        out: list[str] = []
        seen: set[str] = set()
        for item in value:
            token = str(item or "").strip().upper()
            if token and token not in seen:
                out.append(token)
                seen.add(token)
        return out


@dataclass(frozen=True)
class EpisodeRuntimeMatch:
    episode_id: str
    observed_events: tuple[str, ...]
    matched_required_events: int
    runtime_hint: str


@functools.lru_cache(maxsize=1)
def _load_episode_registry_cached() -> dict[str, EpisodeContract]:
    payload = load_yaml_relative("spec/episodes/episode_registry.yaml")
    rows = payload.get("episodes", {}) if isinstance(payload, dict) else {}
    if not isinstance(rows, dict):
        return {}
    out: dict[str, EpisodeContract] = {}
    for episode_id, raw in rows.items():
        if not isinstance(raw, dict):
            continue
        contract = EpisodeContract.model_validate({"episode_id": str(episode_id), **raw})
        out[contract.episode_id] = contract
    return out


def load_episode_registry() -> dict[str, EpisodeContract]:
    return dict(_load_episode_registry_cached())


def _normalize_events(active_event_families: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in active_event_families:
        token = str(item or "").strip().upper()
        if token and token not in seen:
            out.append(token)
            seen.add(token)
    return out


def _regime_tokens(regime_snapshot: Mapping[str, Any] | None) -> set[str]:
    regime = dict(regime_snapshot or {})
    out: set[str] = set()
    for key in ("canonical_regime", "microstructure_regime", "state_family"):
        token = str(regime.get(key, "") or "").strip().upper()
        if token:
            out.add(token)
    return out


def _runtime_hint_satisfied(runtime_hint: str, live_features: Mapping[str, Any] | None, regimes: set[str]) -> bool:
    features = dict(live_features or {})
    spread_bps = float(features.get("spread_bps", 0.0) or 0.0)
    depth_usd = float(features.get("depth_usd", 0.0) or 0.0)
    move_bps = abs(float(features.get("move_bps", 0.0) or 0.0))
    desync_score = float(features.get("desync_score", 0.0) or 0.0)
    cross_venue_spread_bps = abs(float(features.get("cross_venue_spread_bps", 0.0) or 0.0))

    token = str(runtime_hint or "").strip().lower()
    if not token:
        return True
    if token == "wide_spread_and_thin_depth":
        return spread_bps >= 5.0 and depth_usd <= 25_000.0
    if token == "transition_or_volatility_move":
        return move_bps >= 35.0 and bool(regimes.intersection({"TRANSITION", "VOLATILITY"}))
    if token == "desync_reversion":
        return desync_score >= 1.0 or cross_venue_spread_bps >= 10.0
    return True


def infer_live_episode_matches(
    active_event_families: Iterable[str],
    *,
    regime_snapshot: Mapping[str, Any] | None = None,
    live_features: Mapping[str, Any] | None = None,
) -> list[EpisodeRuntimeMatch]:
    active_events = set(_normalize_events(active_event_families))
    features = dict(live_features or {})
    regimes = _regime_tokens(regime_snapshot)

    # Derive likely latent event states from live microstructure when the runtime
    # only has a single detected event family in hand.
    spread_bps = float(features.get("spread_bps", 0.0) or 0.0)
    depth_usd = float(features.get("depth_usd", 0.0) or 0.0)
    move_bps = abs(float(features.get("move_bps", 0.0) or 0.0))
    volume = float(features.get("volume", 0.0) or 0.0)
    if spread_bps >= 5.0 and depth_usd <= 25_000.0:
        active_events.add("LIQUIDITY_VACUUM")
    if move_bps >= 35.0:
        active_events.add("VOL_SHOCK")
    if move_bps >= 55.0 and volume >= 50_000.0:
        active_events.add("VOL_SPIKE")

    matches: list[EpisodeRuntimeMatch] = []
    for contract in load_episode_registry().values():
        if regimes.intersection(set(contract.disallowed_regimes)):
            continue
        observed = tuple(sorted(active_events.intersection(set(contract.required_events))))
        min_required = max(1, int(contract.runtime_min_required_events or contract.minimum_required_events or 1))
        if len(observed) < min_required:
            continue
        if not _runtime_hint_satisfied(contract.runtime_hint, features, regimes):
            continue
        matches.append(
            EpisodeRuntimeMatch(
                episode_id=contract.episode_id,
                observed_events=observed,
                matched_required_events=len(observed),
                runtime_hint=contract.runtime_hint,
            )
        )
    return matches


def build_episode_artifacts(target_dir: str | Path | None = None) -> dict[str, Path]:
    contracts = load_episode_registry()
    out_dir = Path(target_dir) if target_dir is not None else repo_root() / "docs" / "generated"
    out_dir.mkdir(parents=True, exist_ok=True)

    catalog_path = out_dir / "episode_catalog.md"
    matrix_path = out_dir / "episode_maturity_matrix.csv"

    lines = ["# Episode catalog", ""]
    for contract in contracts.values():
        lines.append(f"## {contract.episode_id}")
        lines.append(f"- title: `{contract.title}`")
        lines.append(f"- tier: `{contract.tier}`")
        lines.append(f"- role: `{contract.operational_role}`")
        lines.append(f"- disposition: `{contract.deployment_disposition}`")
        lines.append(f"- required_events: `{', '.join(contract.required_events)}`")
        optional = ", ".join(contract.optional_confirmation_events) or ""
        lines.append(f"- optional_confirmation_events: `{optional}`")
        lines.append(f"- runtime_hint: `{contract.runtime_hint}`")
        lines.append(f"- description: {contract.description}")
        lines.append("")
    catalog_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    rows = ["episode_id,title,tier,operational_role,deployment_disposition,required_events_count,runtime_min_required_events"]
    for contract in contracts.values():
        rows.append(
            f"{contract.episode_id},{contract.title},{contract.tier},{contract.operational_role},"
            f"{contract.deployment_disposition},{len(contract.required_events)},{int(contract.runtime_min_required_events)}"
        )
    matrix_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return {"catalog_path": catalog_path, "matrix_path": matrix_path}
