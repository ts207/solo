from __future__ import annotations

import re
import secrets
from datetime import datetime, timezone

_SAFE_TOKEN = re.compile(r"[^A-Za-z0-9_]+")


def normalize_run_id_prefix(prefix: str) -> str:
    token = _SAFE_TOKEN.sub("_", str(prefix or "").strip()).strip("_").lower()
    token = re.sub(r"_+", "_", token)
    if not token:
        raise ValueError("run id prefix must contain at least one alphanumeric character")
    return token[:80]


def new_run_id(*, prefix: str, now: datetime | None = None, entropy_bytes: int = 3) -> str:
    base = normalize_run_id_prefix(prefix)
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%d_%H%M%S")
    suffix = secrets.token_hex(max(1, int(entropy_bytes)))
    return f"{base}_{stamp}_{suffix}"
