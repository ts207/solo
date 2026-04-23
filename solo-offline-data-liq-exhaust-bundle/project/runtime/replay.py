from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping

from project.runtime.hashing import hash_records


def determinism_replay_check(
    ticks: Iterable[Mapping[str, Any]],
    *,
    hashing_spec: Mapping[str, Any],
) -> Dict[str, Any]:
    rows = [dict(row) for row in ticks]
    if not rows:
        return {
            "status": "no_runtime_events",
            "replay_digest": "",
            "variant_digests": {},
            "tick_count": 0,
        }

    canonical_digest = hash_records(rows, hashing_spec=hashing_spec)
    reverse_digest = hash_records(list(reversed(rows)), hashing_spec=hashing_spec)
    seq_sorted_digest = hash_records(
        sorted(rows, key=lambda r: (int(r.get("source_seq", 0)), str(r.get("event_id", "")))),
        hashing_spec=hashing_spec,
    )
    all_equal = canonical_digest == reverse_digest == seq_sorted_digest
    return {
        "status": "pass" if all_equal else "failed",
        "replay_digest": canonical_digest,
        "variant_digests": {
            "canonical": canonical_digest,
            "reverse": reverse_digest,
            "source_seq_sorted": seq_sorted_digest,
        },
        "tick_count": int(len(rows)),
    }
