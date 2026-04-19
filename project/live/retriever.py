from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List

from project.domain.models import ThesisDefinition
from project.live.contracts import PromotedThesis
from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.thesis_specs import resolve_promoted_thesis_definition
from project.live.thesis_store import ThesisStore
from project.portfolio.admission_policy import PortfolioAdmissionPolicy
from project.research.meta_ranking import thesis_meta_quality_score


def _normalized_tokens(items: list[str] | set[str] | tuple[str, ...] | None) -> set[str]:
    if not items:
        return set()
    return {
        str(item).strip().upper()
        for item in items
        if str(item).strip()
    }


@dataclass(frozen=True)
class ThesisMatch:
    thesis: PromotedThesis
    eligibility_passed: bool
    support_score: float
    contradiction_penalty: float
    reasons_for: list[str]
    reasons_against: list[str]


@dataclass(frozen=True)
class MatchingRequirements:
    trigger_events: set[str]
    confirmation_events: set[str]
    required_episodes: set[str]
    disallowed_regimes: set[str]
    thesis_event_ids: set[str]
    compatibility_event_families: set[str]
    canonical_regime: str


def _context_events(context: LiveTradeContext) -> set[str]:
    event_ids = _normalized_tokens([context.primary_event_id])
    event_ids.update(_normalized_tokens(list(context.active_event_ids or [])))
    if event_ids:
        return event_ids
    compat_families = _normalized_tokens([context.event_family])
    compat_families.update(_normalized_tokens(list(context.active_event_families or [])))
    return compat_families


def _context_event_families(context: LiveTradeContext) -> set[str]:
    compat_families = _normalized_tokens([context.event_family])
    compat_families.update(_normalized_tokens(list(context.active_event_families or [])))
    return compat_families


def _context_episodes(context: LiveTradeContext) -> set[str]:
    out = _normalized_tokens(list(context.active_episode_ids or []))
    snapshot = context.episode_snapshot or {}
    out.update(_normalized_tokens(snapshot.get("episode_ids", [])))
    return out


def _context_states(context: LiveTradeContext) -> set[str]:
    out = _normalized_tokens(list(getattr(context, "active_state_ids", []) or []))
    snapshot = context.episode_snapshot or {}
    out.update(_normalized_tokens(snapshot.get("state_ids", [])))
    out.update(_normalized_tokens(snapshot.get("active_state_ids", [])))
    return out


def _context_contradictions(context: LiveTradeContext) -> set[str]:
    return _normalized_tokens(list(context.contradiction_event_ids or []))


def _context_contradiction_families(context: LiveTradeContext) -> set[str]:
    return _normalized_tokens(list(context.contradiction_event_families or []))


def _evaluate_invalidation(thesis: PromotedThesis, context: LiveTradeContext) -> bool:
    invalidation = thesis.invalidation or {}
    metric = str(invalidation.get("metric", "")).strip()
    operator = str(invalidation.get("operator", "")).strip()
    value = invalidation.get("value")
    if not metric or not operator:
        return False
    current = None
    if metric in context.live_features:
        current = context.live_features.get(metric)
    elif metric in context.regime_snapshot:
        current = context.regime_snapshot.get(metric)
    if current is None or value is None:
        return False
    try:
        left = float(current)
        right = float(value)
    except (TypeError, ValueError):
        return False
    if operator == ">":
        return left > right
    if operator == ">=":
        return left >= right
    if operator == "<":
        return left < right
    if operator == "<=":
        return left <= right
    return False


def _score_supportive_context(
    thesis: PromotedThesis,
    context: LiveTradeContext,
) -> tuple[float, list[str], list[str]]:
    reasons_for: list[str] = []
    reasons_against: list[str] = []
    score = 0.0
    support = thesis.supportive_context or {}

    bridge_certified = bool(support.get("bridge_certified", False))
    if bridge_certified:
        score += 0.05
        reasons_for.append("supportive_context:bridge_certified")

    if bool(support.get("has_realized_oos_path", False)):
        score += 0.05
        reasons_for.append("supportive_context:realized_oos_path")
    else:
        reasons_against.append("supportive_context_missing:realized_oos_path")

    return score, reasons_for, reasons_against


def _merge_mapping(primary: dict | None, fallback: dict | None) -> dict:
    out: dict = {}
    if isinstance(fallback, dict):
        out.update(fallback)
    if isinstance(primary, dict):
        out.update(primary)
    return out


def _requirements_for_matching(
    thesis: PromotedThesis,
    definition: ThesisDefinition | None,
) -> MatchingRequirements:
    thesis_requirements = thesis.requirements
    if definition is not None:
        trigger_events = _normalized_tokens(
            thesis_requirements.trigger_events or definition.trigger_events
        )
        confirmation_events = _normalized_tokens(
            thesis_requirements.confirmation_events or definition.confirmation_events
        )
        required_episodes = _normalized_tokens(
            thesis_requirements.required_episodes or definition.required_episodes
        )
        disallowed_regimes = _normalized_tokens(
            thesis_requirements.disallowed_regimes or definition.disallowed_regimes
        )
        thesis_event_ids = set(trigger_events | confirmation_events)
        thesis_event_ids.update(
            _normalized_tokens(
                thesis.source.event_contract_ids or definition.source_event_contract_ids
            )
        )
        thesis_event_ids.update(
            _normalized_tokens([thesis.primary_event_id or definition.primary_event_id])
        )
        # TODO: thread required/supportive state ids through runtime retrieval once the
        # canonical thesis schema exposes them in live payloads.
        return MatchingRequirements(
            trigger_events=trigger_events,
            confirmation_events=confirmation_events,
            required_episodes=required_episodes,
            disallowed_regimes=disallowed_regimes,
            thesis_event_ids=thesis_event_ids,
            compatibility_event_families=_normalized_tokens(
                [thesis.event_family or definition.event_family]
            ),
            canonical_regime=str(
                thesis.canonical_regime
                or (thesis.supportive_context or {}).get("canonical_regime", "")
                or definition.canonical_regime
                or definition.supportive_context.get("canonical_regime", "")
            ).strip().upper(),
        )

    thesis_event_ids = _normalized_tokens(thesis_requirements.trigger_events)
    thesis_event_ids.update(_normalized_tokens(thesis_requirements.confirmation_events))
    thesis_event_ids.update(_normalized_tokens(thesis.source.event_contract_ids))
    thesis_event_ids.update(_normalized_tokens([thesis.primary_event_id]))
    # TODO: thread required/supportive state ids through runtime retrieval once the
    # canonical thesis schema exposes them in live payloads.
    return MatchingRequirements(
        trigger_events=_normalized_tokens(thesis_requirements.trigger_events),
        confirmation_events=_normalized_tokens(thesis_requirements.confirmation_events),
        required_episodes=_normalized_tokens(thesis_requirements.required_episodes),
        disallowed_regimes=_normalized_tokens(thesis_requirements.disallowed_regimes),
        thesis_event_ids=thesis_event_ids,
        compatibility_event_families=_normalized_tokens([thesis.event_family]),
        canonical_regime=str(
            thesis.canonical_regime or (thesis.supportive_context or {}).get("canonical_regime", "")
        ).strip().upper(),
    )


def _invalidation_for_matching(
    thesis: PromotedThesis,
    definition: ThesisDefinition | None,
) -> dict:
    fallback = definition.invalidation if definition is not None else {}
    return _merge_mapping(thesis.invalidation or {}, fallback)


def _supportive_context_for_matching(
    thesis: PromotedThesis,
    definition: ThesisDefinition | None,
) -> dict:
    fallback = definition.supportive_context if definition is not None else {}
    return _merge_mapping(thesis.supportive_context or {}, fallback)


def _required_context_for_matching(
    thesis: PromotedThesis,
    definition: ThesisDefinition | None,
) -> dict:
    fallback = definition.required_context if definition is not None else {}
    return _merge_mapping(thesis.required_context or {}, fallback)


def _freshness_policy_for_matching(
    thesis: PromotedThesis,
    definition: ThesisDefinition | None,
) -> dict:
    fallback = definition.freshness_policy if definition is not None else {}
    return _merge_mapping(thesis.freshness_policy or {}, fallback)


def _deployment_state_for_matching(
    thesis: PromotedThesis,
    definition: ThesisDefinition | None,
) -> str:
    thesis_state = str(thesis.deployment_state or "").strip().lower()
    if thesis_state:
        return thesis_state
    if definition is not None and str(definition.deployment_state or "").strip():
        return str(definition.deployment_state).strip().lower()
    return ""


def _overlap_group_for_matching(
    thesis: PromotedThesis,
    definition: ThesisDefinition | None,
) -> str:
    if str(thesis.governance.overlap_group_id or "").strip():
        return str(thesis.governance.overlap_group_id).strip()
    if definition is not None:
        return str(definition.governance.get("overlap_group_id", "")).strip()
    return ""


def _evaluate_required_context(
    context_clause: dict,
    context: LiveTradeContext,
) -> tuple[bool, list[str], list[str]]:
    reasons_for: list[str] = []
    reasons_against: list[str] = []
    if not context_clause:
        return True, reasons_for, reasons_against
    sources = [
        context.regime_snapshot or {},
        context.live_features or {},
        context.execution_env or {},
        context.portfolio_state or {},
        {
            "symbol": context.symbol,
            "timeframe": context.timeframe,
            "primary_event_id": context.primary_event_id,
            "event_type": context.primary_event_id,
            "event_family": context.event_family,
            "canonical_regime": context.canonical_regime,
            "event_side": context.event_side,
        },
    ]
    passed = True
    for key, expected in context_clause.items():
        found = None
        for source in sources:
            if key in source:
                found = source.get(key)
                break
        if found is None:
            passed = False
            reasons_against.append(f"required_context_missing:{key}")
            continue
        if str(found).strip().upper() != str(expected).strip().upper():
            passed = False
            reasons_against.append(f"required_context_mismatch:{key}")
            continue
        reasons_for.append(f"required_context_match:{key}")
    return passed, reasons_for, reasons_against


def _evaluate_freshness(
    thesis: PromotedThesis,
    freshness_policy: dict,
) -> tuple[bool, list[str], list[str]]:
    reasons_for: list[str] = []
    reasons_against: list[str] = []
    allowed = freshness_policy.get("allowed_staleness_classes", ["fresh", "watch"])
    allowed_tokens = {
        str(item).strip().lower()
        for item in allowed
        if str(item).strip()
    }
    staleness = str(thesis.staleness_class or "unknown").strip().lower()
    if (
        allowed_tokens
        and staleness
        and staleness not in {"", "unknown"}
        and staleness not in allowed_tokens
    ):
        reasons_against.append(f"freshness_disallowed:{staleness}")
        return False, reasons_for, reasons_against

    require_review_due = bool(freshness_policy.get("require_review_due_date", False))
    review_due_date = str(thesis.review_due_date or "").strip()
    if require_review_due and not review_due_date:
        reasons_against.append("review_due_date_missing")
        return False, reasons_for, reasons_against
    if review_due_date:
        try:
            due = date.fromisoformat(review_due_date)
            if due >= date.today():
                reasons_for.append("review_window_open")
            else:
                reasons_against.append("review_overdue")
                return False, reasons_for, reasons_against
        except ValueError:
            reasons_against.append("review_due_date_invalid")
            return False, reasons_for, reasons_against
    if staleness and staleness != "unknown":
        reasons_for.append(f"freshness:{staleness}")
    return True, reasons_for, reasons_against


def _evaluate_deployment_state(
    deployment_state: str,
    context: LiveTradeContext,
) -> tuple[bool, list[str], list[str]]:
    reasons_for: list[str] = []
    reasons_against: list[str] = []
    token = str(deployment_state or "").strip().lower()
    if not token:
        return True, reasons_for, reasons_against
    runtime_mode = str((context.execution_env or {}).get("runtime_mode", "")).strip().lower()
    if token == "retired":
        reasons_against.append("deployment_state_retired")
        return False, reasons_for, reasons_against
    if runtime_mode == "trading" and token != "live_enabled":
        reasons_against.append(f"deployment_state_blocked:{token}")
        return False, reasons_for, reasons_against
    reasons_for.append(f"deployment_state:{token}")
    return True, reasons_for, reasons_against


def _governance_declared(thesis: PromotedThesis, definition: ThesisDefinition | None) -> bool:
    if any(
        [
            str(thesis.governance.tier).strip(),
            str(thesis.governance.operational_role).strip(),
            str(thesis.governance.deployment_disposition).strip(),
            str(thesis.governance.evidence_mode).strip(),
        ]
    ):
        return True
    if definition is None:
        return False
    governance = definition.governance
    return any(
        [
            str(governance.get("tier", "")).strip(),
            str(governance.get("operational_role", "")).strip(),
            str(governance.get("deployment_disposition", "")).strip(),
            str(governance.get("evidence_mode", "")).strip(),
        ]
    )


def _trade_trigger_eligible(thesis: PromotedThesis, definition: ThesisDefinition | None) -> bool:
    if thesis.governance.trade_trigger_eligible:
        return True
    if definition is None:
        return bool(thesis.governance.trade_trigger_eligible)
    return bool(definition.governance.get("trade_trigger_eligible", False))


def retrieve_ranked_theses(
    *,
    thesis_store: ThesisStore,
    context: LiveTradeContext,
    include_pending: bool = True,
    limit: int = 5,
) -> List[ThesisMatch]:
    candidates = thesis_store.filter(symbol=context.symbol, timeframe=context.timeframe)
    context_events = _context_events(context)
    context_event_families = _context_event_families(context)
    context_episodes = _context_episodes(context)
    context_states = _context_states(context)
    contradiction_events = _context_contradictions(context)
    contradiction_event_families = _context_contradiction_families(context)
    current_regime = str(
        context.canonical_regime or (context.regime_snapshot or {}).get("canonical_regime", "")
    ).strip().upper()

    results: list[ThesisMatch] = []
    policy = PortfolioAdmissionPolicy(family_budgets=context.family_budgets)

    for thesis in candidates:
        if thesis.status == "pending_blueprint" and not include_pending:
            continue
        if thesis.status in {"paused", "retired"}:
            continue

        reasons_for: list[str] = []
        reasons_against: list[str] = []
        support_score = 0.10
        contradiction_penalty = 0.0
        eligibility_passed = True
        definition = resolve_promoted_thesis_definition(thesis)

        governance_declared = _governance_declared(thesis, definition)
        if governance_declared and not _trade_trigger_eligible(thesis, definition):
            reasons_against.append("thesis_not_trade_trigger_eligible")
            eligibility_passed = False

        # Sprint 6: Family Budget Admissibility
        family = thesis.event_family or thesis.primary_event_id
        family_result = policy.is_family_admissible(
            family, context.portfolio_state.get("family_exposures", {})
        )
        if not family_result.admissible:
            reasons_against.append(family_result.reason)
            eligibility_passed = False
        else:
            reasons_for.append(family_result.reason)

        requirements = _requirements_for_matching(thesis, definition)

        clause_triggers = requirements.trigger_events or requirements.thesis_event_ids
        matched_triggers = clause_triggers.intersection(context_events)
        matched_trigger_families: set[str] = set()
        if (
            not matched_triggers
            and not clause_triggers
            and requirements.compatibility_event_families
        ):
            matched_trigger_families = requirements.compatibility_event_families.intersection(
                context_event_families
            )
        if matched_triggers:
            support_score += 0.20
            reasons_for.append(f"trigger_clause_match:{','.join(sorted(matched_triggers))}")
        elif matched_trigger_families:
            support_score += 0.12
            reasons_for.append(
                f"trigger_clause_match_compat_family:{','.join(sorted(matched_trigger_families))}"
            )
        else:
            required_trigger_tokens = clause_triggers or requirements.compatibility_event_families
            reasons_against.append(
                f"trigger_clause_missing:{','.join(sorted(required_trigger_tokens))}"
            )
            eligibility_passed = False

        if matched_triggers:
            support_score += min(0.10, 0.05 * len(matched_triggers))

        if requirements.confirmation_events:
            matched_confirmations = requirements.confirmation_events.intersection(context_events)
            if matched_confirmations:
                support_score += min(0.10, 0.05 * len(matched_confirmations))
                reasons_for.append(f"confirmation_match:{','.join(sorted(matched_confirmations))}")
            else:
                contradiction_penalty += 0.10
                reasons_against.append(
                    f"confirmation_missing:{','.join(sorted(requirements.confirmation_events))}"
                )

        if requirements.required_episodes:
            matched_episodes = requirements.required_episodes.intersection(context_episodes)
            if not matched_episodes:
                reasons_against.append(
                    f"required_episode_missing:{','.join(sorted(requirements.required_episodes))}"
                )
                eligibility_passed = False
            else:
                support_score += min(0.15, 0.08 * len(matched_episodes))
                reasons_for.append(f"required_episode_match:{','.join(sorted(matched_episodes))}")

        required_states = _normalized_tokens(list(thesis.required_state_ids or []))
        if definition is not None:
            required_states.update(
                _normalized_tokens(list(getattr(definition, "required_state_ids", []) or []))
            )
        if required_states:
            matched_states = required_states.intersection(context_states)
            if not matched_states:
                reasons_against.append(
                    f"required_state_missing:{','.join(sorted(required_states))}"
                )
                eligibility_passed = False
            else:
                support_score += min(0.15, 0.08 * len(matched_states))
                reasons_for.append(f"required_state_match:{','.join(sorted(matched_states))}")

        supportive_states = _normalized_tokens(list(thesis.supportive_state_ids or []))
        if definition is not None:
            supportive_states.update(
                _normalized_tokens(list(getattr(definition, "supportive_state_ids", []) or []))
            )
        if supportive_states:
            matched_supportive_states = supportive_states.intersection(context_states)
            if matched_supportive_states:
                support_score += min(0.10, 0.05 * len(matched_supportive_states))
                reasons_for.append(
                    f"supportive_state_match:{','.join(sorted(matched_supportive_states))}"
                )
            else:
                reasons_against.append(
                    f"supportive_state_missing:{','.join(sorted(supportive_states))}"
                )

        if _evaluate_invalidation(
            thesis.model_copy(
                update={"invalidation": _invalidation_for_matching(thesis, definition)}
            ),
            context,
        ):
            contradiction_penalty += 0.60
            reasons_against.append("invalidation_triggered")
            eligibility_passed = False

        if current_regime and current_regime in requirements.disallowed_regimes:
            reasons_against.append(f"regime_disallowed:{current_regime}")
            eligibility_passed = False

        required_context_passed, context_for, context_against = _evaluate_required_context(
            _required_context_for_matching(thesis, definition),
            context,
        )
        reasons_for.extend(context_for)
        reasons_against.extend(context_against)
        if not required_context_passed:
            eligibility_passed = False

        if requirements.canonical_regime:
            if requirements.canonical_regime == current_regime:
                support_score += 0.05
                reasons_for.append(f"canonical_regime_match:{requirements.canonical_regime}")
            elif current_regime:
                reasons_against.append(
                    f"canonical_regime_mismatch:{requirements.canonical_regime}->{current_regime}"
                )

        extra_score, extra_for, extra_against = _score_supportive_context(
            thesis.model_copy(
                update={"supportive_context": _supportive_context_for_matching(thesis, definition)}
            ),
            context,
        )
        support_score += extra_score
        reasons_for.extend(extra_for)
        reasons_against.extend(extra_against)

        freshness_passed, freshness_for, freshness_against = _evaluate_freshness(
            thesis,
            _freshness_policy_for_matching(thesis, definition),
        )
        reasons_for.extend(freshness_for)
        reasons_against.extend(freshness_against)
        if not freshness_passed:
            eligibility_passed = False

        deployment_passed, deployment_for, deployment_against = _evaluate_deployment_state(
            _deployment_state_for_matching(thesis, definition),
            context,
        )
        reasons_for.extend(deployment_for)
        reasons_against.extend(deployment_against)
        if not deployment_passed:
            eligibility_passed = False

        contradiction_targets = requirements.trigger_events | requirements.confirmation_events
        contradiction_targets.update(requirements.thesis_event_ids)
        contradiction_overlap = contradiction_events.intersection(contradiction_targets)
        if not contradiction_overlap and not contradiction_targets:
            contradiction_overlap = contradiction_event_families.intersection(
                requirements.compatibility_event_families
            )
        if contradiction_overlap:
            contradiction_penalty += 0.25
            reasons_against.append(f"contradiction_event:{','.join(sorted(contradiction_overlap))}")

        if thesis.status == "active":
            support_score += 0.10
            reasons_for.append("blueprint_invalidation_available")
        elif thesis.status == "pending_blueprint":
            contradiction_penalty += 0.05
            reasons_against.append("pending_blueprint")

        meta_quality = thesis_meta_quality_score(thesis)
        if meta_quality > 0.0:
            support_score += min(0.10, 0.10 * float(meta_quality))
            reasons_for.append(f"meta_quality:{meta_quality:.2f}")

        results.append(
            ThesisMatch(
                thesis=thesis,
                eligibility_passed=eligibility_passed,
                support_score=min(1.0, max(0.0, support_score)),
                contradiction_penalty=min(1.0, max(0.0, contradiction_penalty)),
                reasons_for=reasons_for,
                reasons_against=reasons_against,
            )
        )

    # Replace legacy overlap suppression with PortfolioAdmissionPolicy().resolve_overlap_winners
    policy = PortfolioAdmissionPolicy(family_budgets=dict(context.family_budgets))
    eligible_dicts = []

    # Pre-filter by family budget
    family_exposures = context.portfolio_state.get("family_exposures", {})

    updated_results = []
    for m in results:
        if m.eligibility_passed:
            family = str(m.thesis.event_family or m.thesis.primary_event_id).strip().upper()
            admission = policy.is_family_admissible(family, family_exposures)

            if not admission.admissible:
                reasons_against = list(m.reasons_against)
                reasons_against.append(f"blocked_by_family_budget:{admission.reason}")
                m = ThesisMatch(
                    thesis=m.thesis,
                    eligibility_passed=False,
                    support_score=m.support_score,
                    contradiction_penalty=m.contradiction_penalty,
                    reasons_for=list(m.reasons_for),
                    reasons_against=reasons_against
                )

        updated_results.append(m)

        if m.eligibility_passed:
            eligible_dicts.append({
                "thesis_id": m.thesis.thesis_id,
                "support_score": m.support_score,
                "contradiction_penalty": m.contradiction_penalty,
                "sample_size": m.thesis.evidence.sample_size,
                "overlap_group_id": str(m.thesis.governance.overlap_group_id or "").strip(),
                "_match": m
            })

    results = updated_results

    winners = policy.resolve_overlap_winners(eligible_dicts, set(context.active_groups))
    winner_ids = {w["thesis_id"] for w in winners}

    # Identify winners per group for reason reporting
    group_winners: Dict[str, str] = {
        w["overlap_group_id"]: w["thesis_id"] for w in winners if w["overlap_group_id"]
    }

    final_results: list[ThesisMatch] = []
    for m in results:
        overlap_group_id = str(m.thesis.governance.overlap_group_id or "").strip()

        # If it was already ineligible, keep it
        if not m.eligibility_passed:
            final_results.append(m)
            continue

        # If it's a winner, keep it
        if m.thesis.thesis_id in winner_ids:
            final_results.append(m)
            continue

        # If it's not a winner, it's suppressed
        reasons_against = list(m.reasons_against)
        if overlap_group_id in context.active_groups:
            reasons_against.append(f"overlap_suppressed:{overlap_group_id}")
        elif overlap_group_id in group_winners:
            reasons_against.append(f"overlap_suppressed:{overlap_group_id}:{group_winners[overlap_group_id]}")
        else:
            # Should not happen given policy logic, but for safety:
            reasons_against.append(f"overlap_suppressed:{overlap_group_id}")

        final_results.append(
            ThesisMatch(
                thesis=m.thesis,
                eligibility_passed=False,
                support_score=m.support_score,
                contradiction_penalty=min(1.0, m.contradiction_penalty + 0.30),
                reasons_for=list(m.reasons_for),
                reasons_against=reasons_against,
            )
        )

    results = final_results
    results.sort(
        key=lambda match: (
            int(match.eligibility_passed),
            match.support_score - match.contradiction_penalty,
            match.thesis.evidence.sample_size,
        ),
        reverse=True,
    )
    return results[: max(1, int(limit))]
