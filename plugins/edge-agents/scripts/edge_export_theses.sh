#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 RUN_ID [extra export_promoted_theses args...]" >&2
  echo "example: $0 my_run --register-runtime paper-btc --set-deployment-state thesis_1=paper_only" >&2
  exit 2
fi

run_id="$1"
shift

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
ensure_edge_env "$repo_root"
cd "$repo_root"

"$repo_root/.venv/bin/python" -m project.research.export_promoted_theses \
  --run_id "$run_id" \
  "$@"
