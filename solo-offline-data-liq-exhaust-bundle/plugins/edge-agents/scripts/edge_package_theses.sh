#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 RUN_ID" >&2
  exit 2
fi

run_id="$1"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
ensure_edge_env "$repo_root"
cd "$repo_root"

"$repo_root/.venv/bin/edge" promote export --run_id "$run_id"
"$repo_root/.venv/bin/python" -m project.scripts.build_thesis_overlap_artifacts --run_id "$run_id"
