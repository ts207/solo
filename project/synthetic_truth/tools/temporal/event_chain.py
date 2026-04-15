from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence

import pandas as pd


class ChainState(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


@dataclass
class ChainStep:
    event_type: str
    min_bar: Optional[int] = None
    max_bar: Optional[int] = None
    required: bool = True


@dataclass
class ChainProgress:
    chain_id: str
    state: ChainState
    current_step: int = 0
    triggered_events: list[str] = field(default_factory=list)
    triggered_bars: list[int] = field(default_factory=list)
    started_at: Optional[int] = None
    completed_at: Optional[int] = None
    failure_reason: Optional[str] = None

    @property
    def is_complete(self) -> bool:
        return self.state == ChainState.COMPLETED

    @property
    def progress_pct(self) -> float:
        if self.state == ChainState.COMPLETED:
            return 100.0
        return (self.current_step / (self.current_step + 1)) * 100.0


@dataclass
class EventChain:
    name: str
    chain_id: str
    steps: list[ChainStep]
    max_chain_duration_bars: int = 50
    min_chain_duration_bars: int = 5
    allow_overlap: bool = False

    def validate(self) -> tuple[bool, str]:
        if not self.steps:
            return False, "Chain has no steps"
        if len(self.steps) < 2:
            return False, "Chain must have at least 2 steps"
        if self.max_chain_duration_bars < self.min_chain_duration_bars:
            return False, "max_duration must be >= min_duration"
        return True, "valid"


class EventChainEngine:
    def __init__(self, chains: list[EventChain]):
        self.chains = {chain.chain_id: chain for chain in chains}
        self._progress: dict[str, list[ChainProgress]] = {}

    def process_event(
        self,
        event_type: str,
        bar_index: int,
        chain_ids: Optional[list[str]] = None,
    ) -> list[ChainProgress]:
        updates = []
        
        if chain_ids is None:
            chain_ids = list(self.chains.keys())
        
        for chain_id in chain_ids:
            if chain_id not in self.chains:
                continue
                
            chain = self.chains[chain_id]
            progress_list = self._progress.setdefault(chain_id, [])
            
            progress = self._process_chain(chain, progress_list, event_type, bar_index)
            if progress:
                updates.append(progress)
        
        return updates

    def _process_chain(
        self,
        chain: EventChain,
        progress_list: list[ChainProgress],
        event_type: str,
        bar_index: int,
    ) -> Optional[ChainProgress]:
        if not progress_list:
            progress = ChainProgress(
                chain_id=chain.chain_id,
                state=ChainState.PENDING,
            )
            progress_list.append(progress)
        else:
            progress = progress_list[-1]
            if progress.is_complete:
                return None

        expected_step = chain.steps[progress.current_step]
        
        if event_type != expected_step.event_type:
            return None
        
        if expected_step.min_bar is not None and bar_index < expected_step.min_bar:
            return None
        
        if expected_step.max_bar is not None and bar_index > expected_step.max_bar:
            progress.state = ChainState.TIMEOUT
            progress.failure_reason = f"Step {progress.current_step} exceeded max_bar"
            return progress

        if progress.started_at is None:
            progress.started_at = bar_index

        progress.triggered_events.append(event_type)
        progress.triggered_bars.append(bar_index)
        progress.state = ChainState.IN_PROGRESS
        progress.current_step += 1

        if progress.current_step >= len(chain.steps):
            if bar_index - progress.started_at >= chain.min_chain_duration_bars:
                progress.state = ChainState.COMPLETED
                progress.completed_at = bar_index
            else:
                progress.state = ChainState.FAILED
                progress.failure_reason = "Chain completed too quickly"

        return progress

    def get_chain_progress(self, chain_id: str) -> list[ChainProgress]:
        return self._progress.get(chain_id, [])

    def get_completed_chains(self) -> list[ChainProgress]:
        completed = []
        for progress_list in self._progress.values():
            for progress in progress_list:
                if progress.is_complete:
                    completed.append(progress)
        return completed

    def get_failed_chains(self) -> list[ChainProgress]:
        failed = []
        for progress_list in self._progress.values():
            for progress in progress_list:
                if progress.state in (ChainState.FAILED, ChainState.TIMEOUT):
                    failed.append(progress)
        return failed


PRECEDENCE_CHAINS = [
    EventChain(
        name="liquidity_vacuum_sequence",
        chain_id="seq_liq_vacuum_then_depth_recovery",
        steps=[
            ChainStep(event_type="LIQUIDITY_VACUUM", max_bar=50),
            ChainStep(event_type="LIQUIDITY_STRESS_PROXY", max_bar=100),
        ],
    ),
    EventChain(
        name="funding_extreme_then_breakout",
        chain_id="seq_fnd_extreme_then_breakout",
        steps=[
            ChainStep(event_type="FUNDING_EXTREME_ONSET", max_bar=50),
            ChainStep(event_type="BREAKOUT_TRIGGER", max_bar=100),
        ],
    ),
]
