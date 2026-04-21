#!/usr/bin/env bash
set -euo pipefail

mode="${1:-contracts}"

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [contracts|minimum-green|all]" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
ensure_edge_env "$repo_root"
cd "$repo_root"

case "$mode" in
  contracts)
    "$repo_root/.venv/bin/python" -m project.scripts.run_researcher_verification --mode contracts
    ;;
  minimum-green)
    make minimum-green-gate
    ;;
  all)
    "$repo_root/.venv/bin/python" -m project.scripts.run_researcher_verification --mode contracts
    make minimum-green-gate
    ;;
  *)
    echo "usage: $0 [contracts|minimum-green|all]" >&2
    exit 2
    ;;
esac
