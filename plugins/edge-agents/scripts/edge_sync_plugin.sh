#!/usr/bin/env bash
set -euo pipefail

mode="${1:-sync}"
target_dir="${2:-${CODEX_HOME:-$HOME/.codex}/plugins/cache/edge-local/edge-agents/local}"

if [ "$mode" != "targets" ] && [ "$mode" != "sync" ] && [ "$mode" != "check" ]; then
  echo "usage: $0 [targets|sync|check] [target_dir]" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
source_dir="$repo_root/plugins/edge-agents"

if [ ! -d "$source_dir" ]; then
  echo "missing plugin source: $source_dir" >&2
  exit 1
fi

discover_targets() {
  printf '%s\n' "${CODEX_HOME:-$HOME/.codex}/plugins/cache/edge-local/edge-agents/local"
}

if [ "$mode" = "targets" ]; then
  discover_targets
  exit 0
fi

if [ "$mode" = "check" ]; then
  if [ ! -d "$target_dir" ]; then
    echo "missing plugin target: $target_dir" >&2
    exit 1
  fi
  if diff -qr --exclude='*.Zone.Identifier' "$source_dir" "$target_dir"; then
    echo "plugin source and installed target match: $target_dir"
  else
    exit 1
  fi
  exit 0
fi

mkdir -p "$(dirname "$target_dir")"
rm -rf "$target_dir"
mkdir -p "$target_dir"
cp -R "$source_dir"/. "$target_dir"/
find "$target_dir" -name '*.Zone.Identifier' -delete

echo "synced plugin source to: $target_dir"
