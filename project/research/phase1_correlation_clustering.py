"""
Phase 1 Feature Orthogonalization

Loads Phase 1 events across all families, computes a binary occurrence matrix
over a 5m grid, applies Agglomerative Clustering (Jaccard/Pearson), and
identifies the centroid event per cluster to preserve FDR power in Phase 2.
"""

from __future__ import annotations
from project.core.config import get_data_root

import argparse
import json
import sys

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering

from project.specs.manifest import start_manifest, finalize_manifest
from project.events.registry import collect_registry_events
from project.io.utils import ensure_dir


def main() -> int:
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser("Cluster phase 1 events by correlation")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--correlation_threshold", type=float, default=0.85)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    manifest = start_manifest("phase1_correlation_clustering", args.run_id, vars(args), [], [])

    try:
        # Collect all phase 1 events generated so far for this run
        df = collect_registry_events(DATA_ROOT, args.run_id)
        if df.empty:
            clusters = {}
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df["present"] = 1
            pivot = df.pivot_table(
                index="timestamp", columns="event_type", values="present", fill_value=0
            )

            event_types = list(pivot.columns)

            if len(event_types) <= 1:
                clusters = {et: et for et in event_types}
            else:
                corr_matrix = pivot.corr(method="pearson").fillna(0)
                # Distance = 1 - corr
                dist_matrix = 1.0 - corr_matrix.values
                np.clip(dist_matrix, 0, 2, out=dist_matrix)

                # We need a proper distance matrix for AgglomerativeClustering
                # "precomputed" works fine if provided as square matrix
                dist_thresh = 1.0 - args.correlation_threshold

                clustering = AgglomerativeClustering(
                    n_clusters=None,
                    metric="precomputed",
                    linkage="average",
                    distance_threshold=dist_thresh,
                )
                labels = clustering.fit_predict(dist_matrix)

                clusters = {}
                for cluster_id in np.unique(labels):
                    members = np.where(labels == cluster_id)[0]
                    if len(members) == 1:
                        clusters[event_types[members[0]]] = event_types[members[0]]
                    else:
                        sub_corr = corr_matrix.iloc[members, members]
                        # centroid is the member with highest average correlation to others in cluster
                        centroid_idx = members[np.argmax(sub_corr.sum(axis=1).values)]
                        centroid = event_types[centroid_idx]
                        for m in members:
                            clusters[event_types[m]] = centroid

        out_dir = DATA_ROOT / "reports" / "phase1_orthogonalization" / args.run_id
        ensure_dir(out_dir)

        out_path = out_dir / "event_clusters.json"

        reverse_map = {}
        for k, v in clusters.items():
            reverse_map.setdefault(v, []).append(k)

        report = {
            "run_id": args.run_id,
            "correlation_threshold": args.correlation_threshold,
            "total_events": len(clusters),
            "total_clusters": len(reverse_map),
            "event_to_centroid": clusters,
            "cluster_members": reverse_map,
        }

        with open(out_path, "w") as f:
            json.dump(report, f, indent=2)

        print(
            f"Clustered {len(clusters)} events into {len(reverse_map)} clusters. Output -> {out_path}"
        )
        outputs = [{"path": str(out_path), "rows": 1, "start_ts": None, "end_ts": None}]

        stats = {
            "total_events": len(clusters),
            "total_clusters": len(reverse_map),
            "outputs": outputs,
        }
        finalize_manifest(manifest, "success", stats=stats)
        return 0

    except Exception as e:
        finalize_manifest(manifest, "failed", error=str(e), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
