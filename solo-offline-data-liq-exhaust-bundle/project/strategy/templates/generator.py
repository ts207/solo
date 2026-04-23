import random
from typing import List, Dict, Any, Optional
from project.strategy.templates.spec import StrategySpec
from project.schemas.control_spec import ControlSpec
from itertools import product


def generate_from_concept(concept: ControlSpec) -> List[StrategySpec]:
    """Generate strategy specs directly from a Unified ControlSpec."""
    candidates = []

    # Extract grids
    grids: Dict[str, List[Any]] = {}

    if concept.parameters.risk.stop_loss_bps:
        grids["stop_loss_bps"] = concept.parameters.risk.stop_loss_bps
    if concept.parameters.risk.take_profit_bps:
        grids["take_profit_bps"] = concept.parameters.risk.take_profit_bps
    if concept.parameters.risk.stop_loss_atr_multipliers:
        grids["stop_loss_atr_multipliers"] = concept.parameters.risk.stop_loss_atr_multipliers
    if concept.parameters.risk.take_profit_atr_multipliers:
        grids["take_profit_atr_multipliers"] = concept.parameters.risk.take_profit_atr_multipliers

    for k, v in concept.parameters.extra_grid.items():
        grids[k] = v

    horizons = concept.parameters.horizons_bars

    keys = list(grids.keys())
    pools = [grids[k] for k in keys]
    perms = list(product(*pools)) if pools else [()]

    for horizon in horizons:
        for perm in perms:
            params = dict(zip(keys, perm))
            primary_event_id = str(
                concept.event_definition.event_type or concept.event_definition.canonical_family
            ).strip().upper()

            s = StrategySpec(
                event_family=primary_event_id,
                entry_signal=concept.templates.base,
                exit_signal="exit",
                position_cap=1.0,
                cooldown_bars=12,
                stop_loss_bps=params.pop("stop_loss_bps", None),
                take_profit_bps=params.pop("take_profit_bps", None),
                stop_loss_atr_multipliers=params.pop("stop_loss_atr_multipliers", None),
                take_profit_atr_multipliers=params.pop("take_profit_atr_multipliers", None),
                params=params,
            )
            # Annotate with overlays and execution config for downstream compiler
            s.params["_overlays"] = concept.templates.overlays
            s.params["_execution_style"] = concept.parameters.execution.style
            s.params["_post_only"] = concept.parameters.execution.post_only_preference
            s.params["horizon_bars"] = horizon

            candidates.append(s)

    return candidates


def generate_candidates(
    event_family: str, priors: Dict[str, Any], grids: Dict[str, List[float]], n: int, seed: int
) -> List[StrategySpec]:
    random.seed(seed)
    # Restrict to <= 6 free parameters by design
    if len(grids) > 6:
        raise ValueError("Cannot search discrete grid with > 6 free parameters")

    candidates = []
    # Cartesian product sample
    keys = list(grids.keys())
    pools = [grids[k] for k in keys]

    perms = list(product(*pools))
    random.shuffle(perms)
    subset = perms[:n]
    primary_event_id = str(event_family).strip().upper()

    for perm in subset:
        params = dict(zip(keys, perm))
        params.update(priors.get("params", {}))

        s = StrategySpec(
            event_family=primary_event_id,
            entry_signal=priors.get("entry_signal", "enter"),
            exit_signal=priors.get("exit_signal", "exit"),
            position_cap=priors.get("position_cap", 1.0),
            cooldown_bars=priors.get("cooldown_bars", 12),
            stop_loss_bps=priors.get("stop_loss_bps"),
            take_profit_bps=priors.get("take_profit_bps"),
            stop_loss_atr_multipliers=priors.get("stop_loss_atr_multipliers"),
            take_profit_atr_multipliers=priors.get("take_profit_atr_multipliers"),
            params=params,
        )
        candidates.append(s)

    return candidates
