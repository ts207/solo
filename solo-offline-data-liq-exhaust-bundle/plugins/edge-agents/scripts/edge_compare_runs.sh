#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 4 ]; then
  echo "usage: $0 BASELINE_RUN_ID CANDIDATE_RUN_ID [PROGRAM_ID] [DATA_ROOT]" >&2
  exit 2
fi

baseline_run_id="$1"
candidate_run_id="$2"
program_id="${3:-}"
data_root="${4:-}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
ensure_edge_env "$repo_root"
cd "$repo_root"

"$repo_root/.venv/bin/python" - "$baseline_run_id" "$candidate_run_id" "$program_id" "$data_root" <<'PY'
import json
import sys
from pathlib import Path

from project.operator.stability import write_time_slice_report

baseline_run_id, candidate_run_id, program_id, data_root = sys.argv[1:5]
payload = write_time_slice_report(
    run_ids=[baseline_run_id, candidate_run_id],
    program_id=program_id or None,
    data_root=Path(data_root) if data_root else None,
)
print(json.dumps(payload, indent=2, sort_keys=True))
PY
