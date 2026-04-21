#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 RUN_ID [PROGRAM_ID] [DATA_ROOT]" >&2
  exit 2
fi

run_id="$1"
program_id="${2:-}"
data_root="${3:-}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
ensure_edge_env "$repo_root"
cd "$repo_root"

"$repo_root/.venv/bin/python" - "$run_id" "$program_id" "$data_root" <<'PY'
import json
import sys
from pathlib import Path

from project.operator.stability import write_negative_result_diagnostics

run_id, program_id, data_root = sys.argv[1:4]
payload = write_negative_result_diagnostics(
    run_id=run_id,
    program_id=program_id or None,
    data_root=Path(data_root) if data_root else None,
)
print(json.dumps(payload, indent=2, sort_keys=True))
PY
