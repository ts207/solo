#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 PROPOSAL_PATH [JSON_OUTPUT] [REGISTRY_ROOT]" >&2
  exit 2
fi

proposal_path="$1"
json_output="${2:-}"
registry_root="${3:-project/configs/registries}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
ensure_edge_env "$repo_root"
cd "$repo_root"

if [ -z "$json_output" ]; then
  proposal_name="$(basename "$proposal_path")"
  proposal_stem="${proposal_name%.*}"
  json_output="data/reports/operator_preflight/${proposal_stem}.json"
fi

mkdir -p "$(dirname "$json_output")"

"$repo_root/.venv/bin/python" -m project.operator.preflight \
  --proposal "$proposal_path" \
  --registry_root "$registry_root" \
  --json_output "$json_output"
