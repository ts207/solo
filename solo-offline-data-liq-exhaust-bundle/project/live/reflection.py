from __future__ import annotations

from typing import Any, Dict

from project.live.memory import load_live_episodes


def build_live_reflection(root: str) -> Dict[str, Any]:
    episodes = load_live_episodes(root)
    if not episodes:
        return {
            "episode_count": 0,
            "action_counts": {},
            "pause_recommendations": [],
        }
    action_counts: dict[str, int] = {}
    negative_count = 0
    for row in episodes:
        action = str(row.get("action", "")).strip().lower()
        if action:
            action_counts[action] = action_counts.get(action, 0) + 1
        realized = float(row.get("realized_net_edge_bps", 0.0) or 0.0)
        if realized < 0.0:
            negative_count += 1
    recommendations: list[str] = []
    if negative_count >= 3:
        recommendations.append("consider_pause_after_negative_streak")
    return {
        "episode_count": len(episodes),
        "action_counts": action_counts,
        "negative_episode_count": negative_count,
        "pause_recommendations": recommendations,
    }
