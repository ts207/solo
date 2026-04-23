#!/usr/bin/env bash
# clean_data.sh — comprehensive artifact and data cleanup
# Usage: clean_data.sh [runtime|all|repo|hygiene]
set -euo pipefail

MODE="${1:-runtime}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

# Wipe contents only, keeping the directory itself (for top-level dirs tracked by git)
wipe_contents() {
  local dir="$1"
  if [[ -d "$dir" ]]; then
    echo "  - Wiping $dir"
    # Remove everything inside except .gitkeep
    find "$dir" -mindepth 1 -maxdepth 1 ! -name '.gitkeep' -exec rm -rf {} +
  fi
  mkdir -p "$dir"
  [[ -f "$dir/.gitkeep" ]] || touch "$dir/.gitkeep"
}

case "$MODE" in
  runtime)
    echo "Cleaning runtime artifacts (runs, reports, lake/runs, lake/trades, events, synthetic)..."
    wipe_contents "data/runs"
    wipe_contents "data/reports"
    wipe_contents "data/events"
    wipe_contents "data/synthetic"
    if [[ -d "data/lake/runs" ]]; then rm -rf data/lake/runs; mkdir -p data/lake/runs; fi
    if [[ -d "data/lake/trades" ]]; then rm -rf data/lake/trades; mkdir -p data/lake/trades; fi
    ;;
  data)
    echo "Cleaning run data and synthetic data..."
    wipe_contents "data/runs"
    wipe_contents "data/synthetic"
    ;;
  all)
    echo "Cleaning ALL data and research artifacts..."
    for subdir in artifacts events knowledge lake reports research runs synthetic; do
      wipe_contents "data/$subdir"
    done
    if [[ -d "artifacts" ]]; then
      wipe_contents "artifacts"
    fi
    ;;
  repo)
    echo "Cleaning repository caches and temporary files..."
    find . -type d -name "__pycache__" -exec rm -rf {} +
    find . -type d -name ".pytest_cache" -exec rm -rf {} +
    find . -type d -name ".ruff_cache" -exec rm -rf {} +
    find . -type d -name ".mypy_cache" -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete
    find . -type f -name "*.pyo" -delete
    find . -type f -name "*.pyd" -delete
    find . -type f -name ".coverage" -delete
    find . -type d -name "htmlcov" -exec rm -rf {} +
    rm -rf /tmp/edgee_smoke_out
    ;;
  hygiene)
    echo "Cleaning hygiene artifacts (Zone.Identifier files)..."
    find . -not -path './.git/*' -not -path './.venv/*' -type f \
      \( -name '*:Zone.Identifier' -o -name '*#Uf03aZone.Identifier' -o -name '*#Uf03aZone.Identifier:Zone.Identifier' \) \
      -print -delete
    ;;
  *)
    echo "Usage: $0 [runtime|all|repo|hygiene]" >&2
    exit 1
    ;;
esac

echo "Clean completed: mode=$MODE"
