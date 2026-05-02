#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [[ -n "${PYTHON:-}" ]]; then
  PYTHON_BIN="$PYTHON"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

if ! "$PYTHON_BIN" -m ruff --version >/dev/null 2>&1; then
  echo "Ruff is not installed for $PYTHON_BIN. Install with: $PYTHON_BIN -m pip install -r requirements-dev.txt"
  exit 1
fi

echo "Running Ruff checks on staged Python files..."
staged_py="$(git diff --cached --name-only --diff-filter=ACMR -- '*.py')"
if [[ -n "$staged_py" ]]; then
  $PYTHON_BIN -m ruff check --select E9,F63,F7,F82 $staged_py
  $PYTHON_BIN -m ruff format --check $staged_py
else
  echo "No staged Python files to lint/format-check."
fi

# 1. Run Pipeline Governance Audit
echo "Running Pipeline Governance Audit..."
$PYTHON_BIN project/scripts/pipeline_governance.py --audit

# 2. Run Detector Coverage Audit
echo "Running Detector Coverage Audit..."
$PYTHON_BIN -m project.scripts.detector_coverage_audit

# 3. Run Repo Hygiene Check
echo "Running Repo Hygiene Check..."
bash project/scripts/check_repo_hygiene.sh

# 4. Sync Schemas (optional, can be part of commit if needed)
echo "Syncing Schemas..."
$PYTHON_BIN project/scripts/pipeline_governance.py --sync

echo "Pre-commit checks passed!"
