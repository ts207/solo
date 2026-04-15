from __future__ import annotations

import threading

_STAGE_OUTPUT_LOCK = threading.Lock()


def emit_buffered_stage_output(stage_instance_id: str, stage: str, text: str) -> None:
    payload = str(text or "").rstrip()
    if not payload:
        return
    prefix = f"[{stage_instance_id}]"
    with _STAGE_OUTPUT_LOCK:
        print(f"{prefix} buffered output ({stage})")
        for line in payload.splitlines():
            print(f"{prefix} {line}")
