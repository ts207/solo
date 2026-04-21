#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 RUN_ID [DATA_ROOT]" >&2
  exit 2
fi

run_id="$1"
data_root="${2:-}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
ensure_edge_env "$repo_root"
cd "$repo_root"

"$repo_root/.venv/bin/python" - "$run_id" "$data_root" <<'PY'
import json
import sys
from pathlib import Path

from project.operator.stability import write_regime_split_report

run_id, data_root = sys.argv[1:3]
payload = write_regime_split_report(
    run_id=run_id,
    data_root=Path(data_root) if data_root else None,
)
print(json.dumps(payload, indent=2, sort_keys=True))
PY
