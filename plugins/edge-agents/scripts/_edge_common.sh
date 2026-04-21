#!/usr/bin/env bash
set -euo pipefail

edge_repo_root() {
  cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
  pwd
}

ensure_edge_env() {
  local repo_root="$1"
  if [ ! -x "$repo_root/.venv/bin/python" ]; then
    echo "missing virtualenv python: $repo_root/.venv/bin/python" >&2
    exit 1
  fi
  if [ ! -x "$repo_root/.venv/bin/edge" ]; then
    echo "missing edge CLI: $repo_root/.venv/bin/edge" >&2
    exit 1
  fi
}
