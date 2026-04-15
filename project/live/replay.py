from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.contracts.trade_intent import TradeIntent
from project.live.decision import decide_trade_intent
from project.live.thesis_store import ThesisStore


@dataclass(frozen=True)
class ReplayResult:
    contexts_evaluated: int
    action_counts: dict[str, int]
    intents: List[TradeIntent]


def replay_contexts(
    *,
    thesis_store: ThesisStore,
    contexts: Iterable[LiveTradeContext],
    include_pending: bool = True,
) -> ReplayResult:
    intents: list[TradeIntent] = []
    action_counts: dict[str, int] = {}
    for context in contexts:
        outcome = decide_trade_intent(
            context=context,
            thesis_store=thesis_store,
            include_pending=include_pending,
        )
        intent = outcome.trade_intent
        intents.append(intent)
        action_counts[intent.action] = action_counts.get(intent.action, 0) + 1
    return ReplayResult(
        contexts_evaluated=len(intents),
        action_counts=action_counts,
        intents=intents,
    )
