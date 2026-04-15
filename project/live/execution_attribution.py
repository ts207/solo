from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from project.io.utils import write_parquet


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ExecutionAttributionRecord:
    client_order_id: str
    symbol: str
    strategy: str
    thesis_id: str
    overlap_group_id: str
    governance_tier: str
    operational_role: str
    active_episode_ids: List[str]
    volatility_regime: str
    microstructure_regime: str
    side: str
    quantity: float
    signal_timestamp: str
    expected_entry_price: float
    expected_return_bps: float
    expected_adverse_bps: float
    expected_cost_bps: float
    expected_net_edge_bps: float
    realized_fill_price: float
    realized_fee_bps: float
    realized_slippage_bps: float
    realized_total_cost_bps: float
    realized_net_edge_bps: float
    edge_decay_bps: float
    created_at: str


def build_execution_attribution_record(
    *,
    client_order_id: str,
    symbol: str,
    strategy: str,
    thesis_id: str = "",
    overlap_group_id: str = "",
    governance_tier: str = "",
    operational_role: str = "",
    active_episode_ids: List[str] | None = None,
    volatility_regime: str,
    microstructure_regime: str,
    side: str,
    quantity: float,
    signal_timestamp: str,
    expected_entry_price: float,
    realized_fill_price: float,
    expected_return_bps: float,
    expected_adverse_bps: float,
    expected_cost_bps: float,
    realized_fee_bps: float,
) -> ExecutionAttributionRecord:
    entry_price = max(float(expected_entry_price), 1e-12)
    fill_price = float(realized_fill_price)
    signed_slippage_bps = ((fill_price - entry_price) / entry_price) * 10000.0
    side_norm = str(side).strip().upper()
    realized_slippage_bps = signed_slippage_bps if side_norm == "BUY" else -signed_slippage_bps
    realized_total_cost_bps = float(realized_fee_bps) + float(realized_slippage_bps)
    expected_net_edge_bps = (
        float(expected_return_bps) - float(expected_adverse_bps) - float(expected_cost_bps)
    )
    realized_net_edge_bps = (
        float(expected_return_bps) - float(expected_adverse_bps) - realized_total_cost_bps
    )
    edge_decay_bps = realized_net_edge_bps - expected_net_edge_bps
    return ExecutionAttributionRecord(
        client_order_id=str(client_order_id),
        symbol=str(symbol).upper(),
        strategy=str(strategy),
        thesis_id=str(thesis_id),
        overlap_group_id=str(overlap_group_id),
        governance_tier=str(governance_tier),
        operational_role=str(operational_role),
        active_episode_ids=[str(item).strip().upper() for item in (active_episode_ids or []) if str(item).strip()],
        volatility_regime=str(volatility_regime),
        microstructure_regime=str(microstructure_regime),
        side=side_norm,
        quantity=float(quantity),
        signal_timestamp=str(signal_timestamp),
        expected_entry_price=float(expected_entry_price),
        expected_return_bps=float(expected_return_bps),
        expected_adverse_bps=float(expected_adverse_bps),
        expected_cost_bps=float(expected_cost_bps),
        expected_net_edge_bps=float(expected_net_edge_bps),
        realized_fill_price=fill_price,
        realized_fee_bps=float(realized_fee_bps),
        realized_slippage_bps=float(realized_slippage_bps),
        realized_total_cost_bps=float(realized_total_cost_bps),
        realized_net_edge_bps=float(realized_net_edge_bps),
        edge_decay_bps=float(edge_decay_bps),
        created_at=_utcnow().isoformat(),
    )


def summarize_execution_attribution(records: List[ExecutionAttributionRecord]) -> Dict[str, float]:
    if not records:
        return {
            "fills": 0.0,
            "avg_expected_net_edge_bps": 0.0,
            "avg_realized_net_edge_bps": 0.0,
            "avg_edge_decay_bps": 0.0,
            "avg_realized_fee_bps": 0.0,
            "avg_realized_slippage_bps": 0.0,
            "win_rate_vs_expected_edge": 0.0,
            "overlap_group_count": 0.0,
            "episode_count": 0.0,
        }

    count = float(len(records))
    expected_net = [float(item.expected_net_edge_bps) for item in records]
    realized_net = [float(item.realized_net_edge_bps) for item in records]
    edge_decay = [float(item.edge_decay_bps) for item in records]
    fees = [float(item.realized_fee_bps) for item in records]
    slippage = [float(item.realized_slippage_bps) for item in records]
    wins = sum(1 for item in records if item.realized_net_edge_bps >= 0.0)
    overlap_group_count = len({str(item.overlap_group_id).strip() for item in records if str(item.overlap_group_id).strip()})
    episode_count = len({episode for item in records for episode in item.active_episode_ids})
    return {
        "fills": count,
        "avg_expected_net_edge_bps": sum(expected_net) / count,
        "avg_realized_net_edge_bps": sum(realized_net) / count,
        "avg_edge_decay_bps": sum(edge_decay) / count,
        "avg_realized_fee_bps": sum(fees) / count,
        "avg_realized_slippage_bps": sum(slippage) / count,
        "win_rate_vs_expected_edge": wins / count,
        "overlap_group_count": float(overlap_group_count),
        "episode_count": float(episode_count),
    }


def summarize_execution_attribution_by(
    records: List[ExecutionAttributionRecord], key: str
) -> Dict[str, Dict[str, float]]:
    grouped: Dict[str, List[ExecutionAttributionRecord]] = {}
    for record in records:
        group_value = str(getattr(record, key, "") or "")
        grouped.setdefault(group_value, []).append(record)
    return {
        group: summarize_execution_attribution(items)
        for group, items in sorted(grouped.items(), key=lambda item: item[0])
    }


def record_to_dict(record: ExecutionAttributionRecord) -> Dict[str, Any]:
    return asdict(record)


def write_attribution_summary(
    records: List[ExecutionAttributionRecord],
    output_path: str,
) -> None:
    """Write aggregated attribution summaries to parquet for research feedback loop."""
    import pandas as pd

    if not records:
        return

    by_key = {}
    for key in ["symbol", "thesis_id", "overlap_group_id", "governance_tier", "operational_role", "volatility_regime", "microstructure_regime"]:
        by_key[key] = summarize_execution_attribution_by(records, key)

    summaries = []
    for key, group_dict in by_key.items():
        for group_value, stats in group_dict.items():
            row = {"group_key": key, "group_value": group_value}
            row.update(stats)
            summaries.append(row)

    if summaries:
        summary_df = pd.DataFrame(summaries)
        write_parquet(summary_df, Path(output_path))
