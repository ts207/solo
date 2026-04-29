#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

MAX_BYTES=$((5 * 1024 * 1024))
if [[ -n "${MAX_TRACKED_FILE_BYTES:-}" ]]; then
  MAX_BYTES="${MAX_TRACKED_FILE_BYTES}"
fi

fail=0

echo "[hygiene] checking forbidden tracked path patterns..."
present_tracked="$(mktemp)"
git ls-files >"$present_tracked"

blocked_patterns=(
  '^data/reports/.+'
  '^data/runs/.+'
  '^data/lake/(cleaned|features|runs)/.+'
  '^tmp/.+'
  '^\.tmp/.+'
  '^live/persist/.+'
  '^artifacts/.+'
  '^logs/.+'
  '^project/(context_entropy_report\.json|extraction_output\.txt|ontology_dedup_map\.json|ontology_dedup_summary\.csv)$'
  '^(analyze_phase2_candidates\.py|check_blocked\.py|check_missing_binance\.py|find_all_raw\.py|find_blocked_conditions\.py|run_and_analyze\.py)$'
  '^debug\.log$'
  '^debug.*\.log$'
  '^debug.*\.txt$'
  '^diag_out\.txt$'
  '^ingest_run\.log$'
  '^nohup\.out$'
)
for pattern in "${blocked_patterns[@]}"; do
  if grep -E "$pattern" "$present_tracked" >/tmp/hygiene_blocked.txt; then
    filtered="$(mktemp)"
    grep -v '\.gitkeep$' /tmp/hygiene_blocked.txt >"$filtered" || true
    if [[ -s "$filtered" ]]; then
      echo "[hygiene] blocked tracked files matched pattern: $pattern"
      cat "$filtered"
      fail=1
    fi
    rm -f "$filtered"
  fi
done

rm -f /tmp/hygiene_blocked.txt "$present_tracked"

echo "[hygiene] checking Zone.Identifier sidecar files..."
zone_files="$(find . \
  -not -path './.git/*' \
  -not -path './.venv/*' \
  -type f \( \
    -name '*:Zone.Identifier' \
    -o -name '*#Uf03aZone.Identifier' \
    -o -name '*#Uf03aZone.Identifier:Zone.Identifier' \
  \) | sed 's#^\./##' | sort)"
if [[ -n "$zone_files" ]]; then
  echo "[hygiene] Zone.Identifier sidecar files detected ($(echo "$zone_files" | wc -l) files):"
  echo "$zone_files" | head -20 || true
  echo "[hygiene] Fix: make clean-hygiene"
  fail=1
fi

tracked_zone="$(git ls-files -- '*:Zone.Identifier' '*#Uf03aZone.Identifier' 2>/dev/null || true)"
if [[ -n "$tracked_zone" ]]; then
  echo "[hygiene] Zone.Identifier files are tracked by git ($(echo "$tracked_zone" | wc -l) files):"
  echo "$tracked_zone" | head -20 || true
  echo "[hygiene] Fix: git rm --cached the tracked sidecar files"
  fail=1
fi

echo "[hygiene] checking tracked file size limits..."
while IFS= read -r path; do
  [[ -f "$path" ]] || continue
  bytes=$(wc -c <"$path")
  if [[ "$bytes" -gt "$MAX_BYTES" ]]; then
    echo "[hygiene] tracked file exceeds max size (${MAX_BYTES} bytes): $path ($bytes bytes)"
    fail=1
  fi
done < <(git ls-files)

echo "[hygiene] checking root directory clutter..."
# Allowed files in root
allowed_root=(
  "AGENTS.md" "CLAUDE.md" "GEMINI.md" "README.md" "CONTRIBUTING.md" "Makefile"
  "PATCH_NOTES.md" "pyproject.toml" "pyrightconfig.json" "pytest.ini" "requirements-dev.txt" "constraints.lock"
  "research_backlog.csv" ".gitignore" ".dockerignore" ".editorconfig" "LICENSE" "LICENSE.md"
)
# Check for unexpected files in root (non-directories)
for f in *; do
  if [[ -f "$f" ]]; then
    found=0
    for a in "${allowed_root[@]}"; do
      if [[ "$f" == "$a" ]]; then
        found=1
        break
      fi
    done
    if [[ "$found" -eq 0 ]]; then
      echo "[hygiene] unexpected file in root: $f"
      fail=1
    fi
  fi
done

echo "[hygiene] checking for large untracked data..."
untracked_data="$(find data -type f ! -name ".gitkeep" -print -quit)"
if [[ -n "$untracked_data" ]]; then
  echo "[hygiene] non-fatal: generated data artifacts are untracked in data/."
  echo "[hygiene] this does not block agent-check unless STRICT_HYGIENE=1 or protected paths are modified."
  echo "[hygiene] run 'make clean-all-data' to purge generated data artifacts."
  # We don't fail hard on untracked data, just warn, unless strict mode requested
  if [[ "${STRICT_HYGIENE:-0}" == "1" ]]; then
    fail=1
  fi
fi

echo "[hygiene] checking repo uses a single test root..."
if [[ -d "tests" ]] && find tests -type f | grep -q .; then
  echo "[hygiene] legacy repo-root tests/ tree still contains test files."
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  echo "[hygiene] FAILED"
  exit 1
fi

echo "[hygiene] OK"
