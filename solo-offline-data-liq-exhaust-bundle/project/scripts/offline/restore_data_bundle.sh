#!/usr/bin/env bash
set -euo pipefail

bundle_id="${1:-single_event_liq_exhaust_down_bounce_20260421T0521Z}"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../../.." && pwd)"
bundle_dir="${repo_root}/offline-data/${bundle_id}"
manifest="${bundle_dir}/manifest.json"
archive="${bundle_dir}/bundle.tar.gz"
python_bin="${PYTHON:-}"

if [[ -z "${python_bin}" ]]; then
  if command -v python3 > /dev/null 2>&1; then
    python_bin="python3"
  elif command -v python > /dev/null 2>&1; then
    python_bin="python"
  else
    echo "Missing Python interpreter. Set PYTHON=/path/to/python and retry." >&2
    exit 1
  fi
fi

if [[ ! -f "${manifest}" ]]; then
  echo "Missing manifest: ${manifest}" >&2
  exit 1
fi

if ! compgen -G "${bundle_dir}/bundle.tar.gz.part-*" > /dev/null; then
  echo "Missing bundle parts under ${bundle_dir}" >&2
  exit 1
fi

cd "${repo_root}"

"${python_bin}" - "${manifest}" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
repo_root = manifest_path.parents[2]
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

for part in manifest["parts"]:
    path = repo_root / part["path"]
    if not path.exists():
        raise SystemExit(f"Missing bundle part: {path}")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != part["sha256"]:
        raise SystemExit(f"Checksum mismatch for {path}: {digest} != {part['sha256']}")
PY

cat "${bundle_dir}"/bundle.tar.gz.part-* > "${archive}"

"${python_bin}" - "${manifest}" "${archive}" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
archive = Path(sys.argv[2])
digest = hashlib.sha256(archive.read_bytes()).hexdigest()
expected = manifest["archive_sha256"]
if digest != expected:
    raise SystemExit(f"Archive checksum mismatch: {digest} != {expected}")
PY

tar -xzf "${archive}" -C "${repo_root}"
echo "Restored offline data bundle: ${bundle_id}"
