#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 RUN_ID [BASELINE_RUN_ID]" >&2
  exit 2
fi

run_id="$1"
baseline_run_id="${2:-}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
ensure_edge_env "$repo_root"
cd "$repo_root"

cmd=(
  "$repo_root/.venv/bin/python" -m project.scripts.run_researcher_verification
  --mode experiment
  --run-id "$run_id"
)

if [ -n "$baseline_run_id" ]; then
  cmd+=(--baseline-run-id "$baseline_run_id")
fi

"${cmd[@]}"
