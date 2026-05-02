#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# This script is an internal maintenance tool, not part of canonical runtime pipelines.
# It regenerates approved tracked runtime and registry artifacts.
THESIS_RUN_ID="${THESIS_RUN_ID:-}"

echo "Regenerating repository artifacts..."

echo "[registry] Rebuilding unified event registry..."
PYTHONPATH="$REPO_ROOT" python3 "$REPO_ROOT/project/scripts/build_unified_event_registry.py"

echo "[templates] Regenerating template registry sidecars..."
PYTHONPATH="$REPO_ROOT" python3 "$REPO_ROOT/project/scripts/build_template_registry_sidecars.py"

echo "[states] Regenerating state registry sidecars..."
PYTHONPATH="$REPO_ROOT" python3 "$REPO_ROOT/project/scripts/build_state_registry_sidecars.py"

echo "[regimes] Regenerating regime registry sidecars..."
PYTHONPATH="$REPO_ROOT" python3 "$REPO_ROOT/project/scripts/build_regime_registry_sidecars.py"

echo "[domain] Rebuilding compiled domain graph..."
PYTHONPATH="$REPO_ROOT" python3 "$REPO_ROOT/project/scripts/build_domain_graph.py"
PYTHONPATH="$REPO_ROOT" python3 "$REPO_ROOT/project/scripts/check_domain_graph_freshness.py"

echo "[runtime] Regenerating runtime event registry..."
PYTHONPATH="$REPO_ROOT" python3 "$REPO_ROOT/project/scripts/build_runtime_event_registry.py"

echo "[sidecars] Regenerating compatibility sidecars..."
PYTHONPATH="$REPO_ROOT" python3 "$REPO_ROOT/project/scripts/build_canonical_registry_sidecars.py"

if [ -n "$THESIS_RUN_ID" ]; then
  echo "[0/2] Regenerating Thesis Overlap Artifacts for THESIS_RUN_ID=$THESIS_RUN_ID..."
  PYTHONPATH="$REPO_ROOT" python3 "$REPO_ROOT/project/scripts/build_thesis_overlap_artifacts.py" \
    --run_id "$THESIS_RUN_ID"
else
  echo "[0/2] Skipping thesis-overlap regeneration (set THESIS_RUN_ID to enable it)."
fi

if [ -n "$THESIS_RUN_ID" ]; then
  echo "[1/2] Verifying thesis store loads cleanly for THESIS_RUN_ID=$THESIS_RUN_ID..."
  PYTHONPATH="$REPO_ROOT" THESIS_RUN_ID="$THESIS_RUN_ID" python3 - <<'PY'
import os
from project.live.thesis_store import ThesisStore
run_id = os.environ["THESIS_RUN_ID"]
store = ThesisStore.from_run_id(run_id)
print(f"Loaded {len(store.all())} theses from explicit run_id={store.run_id or run_id}")
PY
else
  echo "[1/2] Skipping thesis-store verification (set THESIS_RUN_ID to enable it)."
fi

echo "Artifact regeneration complete."
