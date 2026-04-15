from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from project.live.contracts import PromotedThesis
from project.research.artifact_hygiene import (
    build_artifact_refs,
    build_summary_metadata,
    infer_workspace_root,
    invalid_artifact_header,
    metadata_markdown_lines,
)


THESIS_OVERLAP_SCHEMA_VERSION = "thesis_overlap_graph_v1"


def _sorted_tokens(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = str(value or "").strip().upper()
        if token and token not in seen:
            out.append(token)
            seen.add(token)
    return sorted(out)


def thesis_overlap_signature(thesis: PromotedThesis) -> dict[str, Any]:
    event_contract_ids = _sorted_tokens(thesis.source.event_contract_ids)
    episode_contract_ids = _sorted_tokens(thesis.source.episode_contract_ids)
    required_episodes = _sorted_tokens(thesis.requirements.required_episodes)
    trigger_events = _sorted_tokens(thesis.requirements.trigger_events)
    confirmations = _sorted_tokens(thesis.requirements.confirmation_events)
    invalidation_metric = str((thesis.invalidation or {}).get("metric", "")).strip().lower()
    canonical_regime = str((thesis.supportive_context or {}).get("canonical_regime", "")).strip().upper()
    operational_role = str(thesis.governance.operational_role or "").strip().lower()
    tier = str(thesis.governance.tier or "").strip().upper()
    return {
        "thesis_id": thesis.thesis_id,
        "primary_event_id": str(thesis.primary_event_id or thesis.event_family).strip().upper(),
        "compat_event_family": str(thesis.event_family).strip().upper(),
        "event_contract_ids": event_contract_ids,
        "episode_contract_ids": episode_contract_ids,
        "required_episodes": required_episodes,
        "trigger_events": trigger_events,
        "confirmation_events": confirmations,
        "canonical_regime": canonical_regime,
        "operational_role": operational_role,
        "tier": tier,
        "invalidation_metric": invalidation_metric,
    }


def overlap_group_id_for_thesis(thesis: PromotedThesis) -> str:
    sig = thesis_overlap_signature(thesis)
    primary_episode = (sig["episode_contract_ids"] or sig["required_episodes"] or [""])[0]
    regime = sig["canonical_regime"] or "ANY"
    role = sig["operational_role"] or "unknown"
    primary_event_id = sig["primary_event_id"] or "UNKNOWN"
    return f"{primary_event_id}::{primary_episode or 'NO_EPISODE'}::{regime}::{role}"


def _edge_dimensions(left: dict[str, Any], right: dict[str, Any]) -> tuple[float, list[str]]:
    shared: list[str] = []
    score = 0.0
    if left["primary_event_id"] and left["primary_event_id"] == right["primary_event_id"]:
        shared.append(f"primary_event_id:{left['primary_event_id']}")
        score += 0.35
    shared_events = set(left["event_contract_ids"]).intersection(right["event_contract_ids"])
    if shared_events:
        shared.append("event_contract_ids:" + ",".join(sorted(shared_events)))
        score += 0.25
    shared_episodes = set(left["episode_contract_ids"] + left["required_episodes"]).intersection(
        right["episode_contract_ids"] + right["required_episodes"]
    )
    if shared_episodes:
        shared.append("episodes:" + ",".join(sorted(shared_episodes)))
        score += 0.20
    if left["canonical_regime"] and left["canonical_regime"] == right["canonical_regime"]:
        shared.append(f"canonical_regime:{left['canonical_regime']}")
        score += 0.10
    if left["invalidation_metric"] and left["invalidation_metric"] == right["invalidation_metric"]:
        shared.append(f"invalidation_metric:{left['invalidation_metric']}")
        score += 0.10
    return min(1.0, score), shared


def build_thesis_overlap_graph(theses: Iterable[PromotedThesis]) -> dict[str, Any]:
    thesis_list = list(theses)
    signatures = {thesis.thesis_id: thesis_overlap_signature(thesis) for thesis in thesis_list}
    nodes = []
    overlap_groups: dict[str, list[str]] = {}
    for thesis in thesis_list:
        group_id = overlap_group_id_for_thesis(thesis)
        overlap_groups.setdefault(group_id, []).append(thesis.thesis_id)
        sig = signatures[thesis.thesis_id]
        nodes.append(
            {
                "thesis_id": thesis.thesis_id,
                "primary_event_id": sig["primary_event_id"],
                "compat_event_family": sig["compat_event_family"],
                "event_contract_ids": sig["event_contract_ids"],
                "episode_contract_ids": sig["episode_contract_ids"],
                "canonical_regime": sig["canonical_regime"],
                "operational_role": sig["operational_role"],
                "tier": sig["tier"],
                "overlap_group_id": group_id,
                "meta_rank_score": thesis.evidence.rank_score,
                "sample_size": int(thesis.evidence.sample_size),
            }
        )

    edges = []
    ordered = thesis_list
    for idx, left in enumerate(ordered):
        left_sig = signatures[left.thesis_id]
        for right in ordered[idx + 1 :]:
            right_sig = signatures[right.thesis_id]
            score, shared = _edge_dimensions(left_sig, right_sig)
            if score <= 0.0:
                continue
            edges.append(
                {
                    "source": left.thesis_id,
                    "target": right.thesis_id,
                    "overlap_score": round(score, 4),
                    "shared_dimensions": shared,
                }
            )

    return {
        "schema_version": THESIS_OVERLAP_SCHEMA_VERSION,
        "thesis_count": len(thesis_list),
        "overlap_group_count": len(overlap_groups),
        "groups": [
            {
                "overlap_group_id": group_id,
                "member_count": len(members),
                "members": sorted(members),
            }
            for group_id, members in sorted(overlap_groups.items(), key=lambda item: item[0])
        ],
        "nodes": sorted(nodes, key=lambda item: item["thesis_id"]),
        "edges": sorted(edges, key=lambda item: (item["source"], item["target"])),
    }


def render_overlap_graph_markdown(payload: dict[str, Any], *, metadata: dict[str, Any], invalid_refs: list[str]) -> str:
    lines = invalid_artifact_header(invalid_refs) + ["# Thesis Overlap Graph", ""]
    lines.extend(metadata_markdown_lines(metadata))
    lines.append(f"- Thesis count: {int(payload.get('thesis_count', 0) or 0)}")
    lines.append(f"- Overlap groups: {int(payload.get('overlap_group_count', 0) or 0)}")
    lines.append("")
    lines.append("## Groups")
    lines.append("")
    groups = payload.get("groups", []) if isinstance(payload.get("groups", []), list) else []
    if not groups:
        lines.append("_No groups available._")
    else:
        for group in groups:
            group_id = str(group.get("overlap_group_id", "")).strip()
            members = ", ".join(group.get("members", []))
            lines.append(f"- `{group_id}` — {int(group.get('member_count', 0) or 0)} member(s): {members}")
    lines.append("")
    lines.append("## Highest-overlap edges")
    lines.append("")
    edges = sorted(payload.get("edges", []), key=lambda item: float(item.get("overlap_score", 0.0)), reverse=True)
    if not edges:
        lines.append("_No overlap edges available._")
    else:
        for edge in edges[:20]:
            lines.append(
                f"- `{edge.get('source', '')}` ↔ `{edge.get('target', '')}` — score {float(edge.get('overlap_score', 0.0)):.2f}; "
                + ", ".join(edge.get("shared_dimensions", []))
            )
    return "\n".join(lines).rstrip() + "\n"


def write_thesis_overlap_artifacts(
    theses: Iterable[PromotedThesis],
    out_dir: str | Path,
    *,
    source_run_id: str | None = None,
    workspace_root: str | Path | None = None,
) -> dict[str, Any]:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    resolved_workspace_root = infer_workspace_root(workspace_root, out_path)
    payload = build_thesis_overlap_graph(theses)
    json_path = out_path / "thesis_overlap_graph.json"
    md_path = out_path / "thesis_overlap_graph.md"

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text("# Thesis Overlap Graph\n", encoding="utf-8")

    artifact_refs, invalid_refs = build_artifact_refs(
        {
            "overlap_json": json_path,
            "overlap_md": md_path,
        },
        workspace_root=resolved_workspace_root,
    )
    metadata = build_summary_metadata(
        schema_version=THESIS_OVERLAP_SCHEMA_VERSION,
        artifact_root=out_path,
        source_run_id=source_run_id,
        workspace_root=resolved_workspace_root,
        invalid_artifact_refs=invalid_refs,
    )
    payload.update(metadata)
    payload["artifact_refs"] = artifact_refs
    payload["invalid_artifact_refs"] = invalid_refs
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_overlap_graph_markdown(payload, metadata=metadata, invalid_refs=invalid_refs), encoding="utf-8")
    return payload
