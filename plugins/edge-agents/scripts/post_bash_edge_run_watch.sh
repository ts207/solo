#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
cd "$repo_root"

fresh_runs=()
while IFS= read -r manifest; do
  [ -n "$manifest" ] || continue
  run_dir="$(dirname "$manifest")"
  run_id="$(basename "$run_dir")"
  fresh_runs+=("$run_id")
done < <(find "$repo_root/data/runs" -maxdepth 2 -name run_manifest.json -mmin -10 -type f 2>/dev/null | sort)

if [ "${#fresh_runs[@]}" -eq 0 ]; then
  exit 0
fi

echo "[edge-hook] Fresh run artifacts detected:"
printf '  - %s\n' "${fresh_runs[@]}"

for run_id in "${fresh_runs[@]}"; do
  echo "[edge-hook] Inspect:"
  echo "  ./plugins/edge-agents/scripts/edge_show_run_artifacts.sh $run_id"
  echo "[edge-hook] Verify:"
  echo "  ./plugins/edge-agents/scripts/edge_verify_run.sh $run_id"
  echo "[edge-hook] Diagnose:"
  echo "  ./plugins/edge-agents/scripts/edge_diagnose_run.sh $run_id"
  echo "[edge-hook] Regime stability:"
  echo "  ./plugins/edge-agents/scripts/edge_regime_report.sh $run_id"
  echo "[edge-hook] Export thesis batch if the run is promotable:"
  echo "  ./plugins/edge-agents/scripts/edge_export_theses.sh $run_id"
done
