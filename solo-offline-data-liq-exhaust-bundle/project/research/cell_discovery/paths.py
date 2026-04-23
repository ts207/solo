from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from project.research.services.pathing import phase2_run_dir


@dataclass(frozen=True)
class CellDiscoveryPaths:
    run_dir: Path
    generated_dir: Path
    data_contract_path: Path
    search_spec_path: Path
    experiment_path: Path
    lineage_path: Path
    skipped_cells_path: Path
    candidate_universe_path: Path
    raw_cells_path: Path
    contrast_path: Path
    scoreboard_path: Path
    summary_path: Path
    clusters_path: Path
    cluster_representatives_path: Path
    pnl_traces_path: Path
    trigger_traces_path: Path
    generated_proposals_dir: Path
    thesis_assembly_report_path: Path


def paths_for_run(*, data_root: Path, run_id: str) -> CellDiscoveryPaths:
    run_dir = phase2_run_dir(data_root=data_root, run_id=run_id)
    generated_dir = run_dir / "generated"
    return CellDiscoveryPaths(
        run_dir=run_dir,
        generated_dir=generated_dir,
        data_contract_path=run_dir / "edge_cell_data_contract.json",
        search_spec_path=generated_dir / "generated_edge_cell_search_space.yaml",
        experiment_path=generated_dir / "generated_edge_cell_experiment.json",
        lineage_path=generated_dir / "edge_cell_lineage.parquet",
        skipped_cells_path=generated_dir / "edge_cell_skipped_cells.json",
        candidate_universe_path=run_dir / "phase2_candidate_universe.parquet",
        raw_cells_path=run_dir / "edge_cells_raw.parquet",
        contrast_path=run_dir / "edge_cells_contrast.parquet",
        scoreboard_path=run_dir / "edge_scoreboard.parquet",
        summary_path=run_dir / "edge_scoreboard_summary.json",
        clusters_path=run_dir / "edge_clusters.parquet",
        cluster_representatives_path=run_dir / "edge_cluster_representatives.parquet",
        pnl_traces_path=run_dir / "edge_cell_pnl_traces.parquet",
        trigger_traces_path=run_dir / "edge_cell_trigger_traces.parquet",
        generated_proposals_dir=run_dir / "generated_proposals",
        thesis_assembly_report_path=run_dir / "thesis_assembly_report.json",
    )
